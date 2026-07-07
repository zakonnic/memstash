package integration

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
	"github.com/zakonnic/memstash/l2/gomemcache_adapter"
	"github.com/zakonnic/memstash/l2/goredis_adapter"
	"github.com/zakonnic/memstash/l2/rueidis_adapter"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/redis/go-redis/v9"
	rueidislib "github.com/redis/rueidis"
)

// The benchmarks emulate realistic mixed L1+L2 workloads against the canonical client of each backend (go-redis and
// bradfitz/gomemcache); the adapters differ only in wire code, so backend-level numbers carry over. Sizing: the L1
// holds ~10% of the keyspace, so the long tail is served by the network - exactly the regime a two-level cache is
// built for.
const (
	benchKeySpace = 10_000
	benchL1Cap    = 1_000
	benchValueLen = 512
)

var benchValue = strings.Repeat("x", benchValueLen)

func benchKey(i int) string { return fmt.Sprintf("k%06d", i) }

// benchBackend is one L2 backend under test: factory builds a fresh two-level cache over the shared client, prefix
// pins the L2 namespace.
type benchBackend struct {
	name    string
	factory func(b *testing.B, prefix string, opts ...memstash.Option) *memstash.Cache[string, string]
}

// benchBackends returns the available backends, skipping the ones whose server is down (b.Skip inside sub-benchmarks
// keeps partial environments useful). Clients are created once per process.
func benchBackends(b *testing.B) []benchBackend {
	b.Helper()
	var backends []benchBackend

	redisClientsOnce.Do(func() {
		goredisClient = redis.NewClient(&redis.Options{Addr: redisAddr(), PoolSize: 64})
		c, err := rueidislib.NewClient(rueidislib.ClientOption{InitAddress: []string{redisAddr()}})
		if err != nil {
			b.Fatalf("rueidislib.NewClient: %v", err)
		}
		rueidisClient = c
	})
	backends = append(backends, benchBackend{
		name: "redis-goredis",
		factory: func(b *testing.B, prefix string, opts ...memstash.Option) *memstash.Cache[string, string] {
			requireServer(b, redisAddr())
			opts = append(opts, l2.WithKeyFunc(l2.PrefixedString(prefix)))
			c, err := goredis_adapter.NewCache[string, string](goredisClient, l2.StringCodec(), opts...)
			if err != nil {
				b.Fatalf("NewCache: %v", err)
			}
			b.Cleanup(c.Close)
			return c
		},
	})
	backends = append(backends, benchBackend{
		name: "redis-rueidis",
		factory: func(b *testing.B, prefix string, opts ...memstash.Option) *memstash.Cache[string, string] {
			requireServer(b, redisAddr())
			opts = append(opts, l2.WithKeyFunc(l2.PrefixedString(prefix)))
			c, err := rueidis_adapter.NewCache[string, string](rueidisClient, l2.StringCodec(), opts...)
			if err != nil {
				b.Fatalf("NewCache: %v", err)
			}
			b.Cleanup(c.Close)
			return c
		},
	})

	memcachedClientOnce.Do(func() {
		memcachedClient = memcache.New(memcachedAddr())
		memcachedClient.MaxIdleConns = 64
	})
	backends = append(backends, benchBackend{
		name: "memcached",
		factory: func(b *testing.B, prefix string, opts ...memstash.Option) *memstash.Cache[string, string] {
			requireServer(b, memcachedAddr())
			opts = append(opts, l2.WithKeyFunc(l2.PrefixedString(prefix)))
			c, err := gomemcache_adapter.NewCache[string, string](memcachedClient, l2.StringCodec(), opts...)
			if err != nil {
				b.Fatalf("NewCache: %v", err)
			}
			b.Cleanup(c.Close)
			return c
		},
	})
	return backends
}

var (
	redisClientsOnce    sync.Once
	goredisClient       *redis.Client
	rueidisClient       rueidislib.Client
	memcachedClientOnce sync.Once
	memcachedClient     *memcache.Client

	// benchRunPrefix isolates one process run; seeded namespaces are reused across benchmarks within the run.
	benchRunPrefix = fmt.Sprintf("membench|%d|", time.Now().UnixNano())
	seededOnce     sync.Map // backend name -> *sync.Once
)

// seedBackend fills the backend's read-only namespace with the whole keyspace once per process (write-through via a
// throwaway cache) and returns the prefix.
func seedBackend(b *testing.B, backend benchBackend) string {
	b.Helper()
	prefix := benchRunPrefix + backend.name + "|ro|"
	onceAny, _ := seededOnce.LoadOrStore(backend.name, &sync.Once{})
	onceAny.(*sync.Once).Do(func() {
		ctx := context.Background()
		seeder := backend.factory(b, prefix)
		for i := 0; i < benchKeySpace; i++ {
			if err := seeder.Set(ctx, benchKey(i), benchValue); err != nil {
				b.Fatalf("seeding %s: %v", backend.name, err)
			}
		}
		seeder.Close()
	})
	return prefix
}

// zipfSource returns a per-goroutine Zipf generator (rand sources are not thread-safe): a hot head with a long tail,
// the canonical shape of content and entity caches.
func zipfSource(seed int64) *rand.Zipf {
	return rand.NewZipf(rand.New(rand.NewSource(seed)), 1.1, 1, benchKeySpace-1)
}

// BenchmarkZipfReadThrough is the classic read-through cache in front of a slow store: Zipf-distributed reads, the
// hot head lives in L1, the tail is fetched from L2 and promoted. Reports the achieved L1 hit ratio next to ns/op.
func BenchmarkZipfReadThrough(b *testing.B) {
	for _, backend := range benchBackends(b) {
		b.Run(backend.name, func(b *testing.B) {
			prefix := seedBackend(b, backend)
			c := backend.factory(b, prefix, memstash.WithMemoryCapacity(benchL1Cap))
			ctx := context.Background()

			var l1Hits, lookups atomic.Int64
			var seed atomic.Int64
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				zipf := zipfSource(seed.Add(1))
				for pb.Next() {
					key := benchKey(int(zipf.Uint64()))
					lookups.Add(1)
					if _, ok := c.GetFromMemory(key); ok {
						l1Hits.Add(1)
						continue
					}
					if _, ok, err := c.Get(ctx, key); err != nil || !ok {
						b.Errorf("Get %s: ok=%v err=%v", key, ok, err)
						return
					}
				}
			})
			b.ReportMetric(float64(l1Hits.Load())/float64(lookups.Load()), "l1hit-ratio")
		})
	}
}

// BenchmarkSessionReadMostly is a session-store profile: 90% reads / 10% write-through updates over a uniformly
// accessed keyspace ten times larger than L1. Uniform access is the worst case for L1 - most reads go to the network.
func BenchmarkSessionReadMostly(b *testing.B) {
	for _, backend := range benchBackends(b) {
		b.Run(backend.name, func(b *testing.B) {
			prefix := seedBackend(b, backend)
			c := backend.factory(b, prefix,
				memstash.WithMemoryCapacity(benchL1Cap),
				memstash.WithWritePolicy(memstash.WriteThrough), // a session update must be durable in L2 when Set returns
			)
			ctx := context.Background()

			var seed atomic.Int64
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(seed.Add(1)))
				for pb.Next() {
					key := benchKey(rng.Intn(benchKeySpace))
					if rng.Intn(10) == 0 {
						if err := c.Set(ctx, key, benchValue); err != nil {
							b.Errorf("Set %s: %v", key, err)
							return
						}
						continue
					}
					if _, ok, err := c.Get(ctx, key); err != nil || !ok {
						b.Errorf("Get %s: ok=%v err=%v", key, ok, err)
						return
					}
				}
			})
		})
	}
}

// BenchmarkWriteBackIngest is an ingestion profile (metrics, counters, session touches): 100% writes with the
// asynchronous WriteBack policy - the hot path pays only the L1 insert plus a channel send, the network is drained by
// the background worker. Compare with BenchmarkWriteThroughIngest to see what the synchronous L2 write costs.
func BenchmarkWriteBackIngest(b *testing.B) {
	benchIngest(b, memstash.WriteBack)
}

// BenchmarkWriteThroughIngest is the same ingestion profile with synchronous write-through: every Set pays the full
// network round trip. The baseline for the write-back comparison.
func BenchmarkWriteThroughIngest(b *testing.B) {
	benchIngest(b, memstash.WriteThrough)
}

func benchIngest(b *testing.B, policy memstash.WritePolicy) {
	for _, backend := range benchBackends(b) {
		b.Run(backend.name, func(b *testing.B) {
			prefix := benchRunPrefix + backend.name + fmt.Sprintf("|ingest%d|", policy)
			c := backend.factory(b, prefix,
				memstash.WithMemoryCapacity(benchL1Cap),
				memstash.WithWritePolicy(policy),
				memstash.WithWriteBackBuffer(4096),
			)
			ctx := context.Background()

			var seed atomic.Int64
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(seed.Add(1)))
				for pb.Next() {
					if err := c.Set(ctx, benchKey(rng.Intn(benchKeySpace)), benchValue); err != nil {
						b.Errorf("Set: %v", err)
						return
					}
				}
			})
			b.StopTimer()
			c.Close() // drain the write-back buffer outside the timed section
		})
	}
}

// BenchmarkGetOrLoadZipf is the read-through profile expressed via GetOrLoad: Zipf reads with singleflight collapsing
// concurrent misses; L2 holds the whole keyspace, so the loader (the "database") is the last resort and stays cold.
func BenchmarkGetOrLoadZipf(b *testing.B) {
	for _, backend := range benchBackends(b) {
		b.Run(backend.name, func(b *testing.B) {
			prefix := seedBackend(b, backend)
			c := backend.factory(b, prefix, memstash.WithMemoryCapacity(benchL1Cap))
			ctx := context.Background()

			var loaderCalls atomic.Int64
			load := func(_ context.Context, key string) (string, error) {
				loaderCalls.Add(1)
				return benchValue, nil
			}

			var seed atomic.Int64
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				zipf := zipfSource(seed.Add(1))
				for pb.Next() {
					if _, err := c.GetOrLoad(ctx, benchKey(int(zipf.Uint64())), load); err != nil {
						b.Errorf("GetOrLoad: %v", err)
						return
					}
				}
			})
			b.ReportMetric(float64(loaderCalls.Load()), "loader-calls")
		})
	}
}
