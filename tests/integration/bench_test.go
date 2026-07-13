package integration

// Realistic L1+L2 benchmarks over every adapter whose server lives in docker/docker-compose.yml.
//
// The replay is cache-aside: L1 lookup, then L2, a miss stores the deterministic value for the key (write-back
// drains in the background). L2 namespaces are stable within one process run, so the ramp-up runs of one benchmark
// reuse the L2 data they left behind, like a service restarting next to a warm cache.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	nethttp "net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v7"
	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamotypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/bradfitz/gomemcache/memcache"
	_ "github.com/go-sql-driver/mysql"
	redigolib "github.com/gomodule/redigo/redis"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/joomcode/redispipe/redisconn"
	mclib "github.com/memcachier/mc/v3"
	rainycape "github.com/rainycape/memcache"
	goredislib "github.com/redis/go-redis/v9"
	rueidislib "github.com/redis/rueidis"
	tarantoollib "github.com/tarantool/go-tarantool/v2"
	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
	aerospike_adapter "github.com/zakonnic/memstash/l2/aerospike_adapter"
	dynamo_adapter "github.com/zakonnic/memstash/l2/dynamo_adapter"
	gomemcache_adapter "github.com/zakonnic/memstash/l2/gomemcache_adapter"
	goredis_adapter "github.com/zakonnic/memstash/l2/goredis_adapter"
	mc_adapter "github.com/zakonnic/memstash/l2/mc_adapter"
	mongo_adapter "github.com/zakonnic/memstash/l2/mongo_adapter"
	pgx_adapter "github.com/zakonnic/memstash/l2/pgx_adapter"
	rainycape_adapter "github.com/zakonnic/memstash/l2/rainycape_adapter"
	redigo_adapter "github.com/zakonnic/memstash/l2/redigo_adapter"
	redispipe_adapter "github.com/zakonnic/memstash/l2/redispipe_adapter"
	rueidis_adapter "github.com/zakonnic/memstash/l2/rueidis_adapter"
	sql_adapter "github.com/zakonnic/memstash/l2/sql_adapter"
	tarantool_adapter "github.com/zakonnic/memstash/l2/tarantool_adapter"
	"github.com/zakonnic/memstash/tests/workload"
	"go.mongodb.org/mongo-driver/mongo"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"
)

// --- workloads: the realistic package's scenarios, scaled down from tests/realistic_values_test.go's sizes ---

const benchTraceLen = 100_000

// benchBlob is a shared pool of deterministic printable bytes; values are sliced out of it.
var benchBlob = workload.NewBlob(99, workload.DefaultBlobSize)

// benchScenario is one workload: a key trace, the deterministic value per key and the L1 byte budget (sized to hold
// a fraction of the keyspace, so the tail is served by L2).
type benchScenario struct {
	name       string
	buildTrace func() []string
	value      func(key string) []byte
	l1Budget   int64

	traceOnce sync.Once
	trace     []string
}

var (
	// web session store: Zipf over 20k session tokens, ~350-650 byte JSON documents.
	benchSessions = workload.SessionScenario{Catalog: 20_000, TraceLen: benchTraceLen}
	// CDN / static assets: 75% Zipf over an 8k catalogue + 25% one-hit wonders, bimodal 0.6-32 KB values.
	benchCDN = workload.CDNScenario{Catalog: 8_000, TraceLen: benchTraceLen, LargeSpan: 24 * 1024}
	// DB row cache: Zipf point lookups over 20k rows with a recurring 2k-row sequential scan.
	benchDBRows = workload.DBScenario{Rows: 20_000, ScanRows: 2_000, ChunkSize: 10_000, TraceLen: benchTraceLen}
)

var benchScenarios = []*benchScenario{
	{name: "WebSessions", buildTrace: benchSessions.Trace,
		value: func(key string) []byte { return benchSessions.Value(benchBlob, key) }, l1Budget: 2 << 20},
	{name: "CDNAssets", buildTrace: benchCDN.Trace,
		value: func(key string) []byte { return benchCDN.Value(benchBlob, key) }, l1Budget: 8 << 20},
	{name: "DBRows", buildTrace: benchDBRows.Trace,
		value: func(key string) []byte { return benchDBRows.Value(benchBlob, key) }, l1Budget: 2 << 20},
}

// --- backends: lazily dialed shared clients over every server in the compose file ---

// benchBackend is one L2 backend: addr gates the run on server availability, factory builds a two-level cache over
// the process-wide client.
type benchBackend struct {
	name    string
	addr    string
	factory func(b *testing.B, prefix string, opts ...memstash.Option) *memstash.Cache[string, []byte]
}

// lazyClient builds a client (and its schema) once per process; every later use shares it.
type lazyClient[T any] struct {
	once sync.Once
	v    T
	err  error
}

func (l *lazyClient[T]) get(mk func() (T, error)) (T, error) {
	l.once.Do(func() { l.v, l.err = mk() })
	return l.v, l.err
}

// serverUp memoizes the availability probe per address: a down server costs one dial timeout per process, not one
// per benchmark.
var serverUp sync.Map // addr -> bool

func benchRequireServer(b *testing.B, addr string) {
	b.Helper()
	up, ok := serverUp.Load(addr)
	if !ok {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		up = err == nil
		if err == nil {
			_ = conn.Close()
		}
		serverUp.Store(addr, up)
	}
	if !up.(bool) {
		b.Skipf("no server at %s (make up)", addr)
	}
}

// adapterCache wraps NewCache boilerplate: the key prefix pins the namespace, the []byte codec passes values
// through. The cache is not closed per benchmark - benchReplay keeps one live cache per scenario+backend for the
// whole process, so the ramp-up runs neither recool L1 nor pay a write-back drain each.
func adapterCache(b *testing.B, store memstash.L2Cache[string, []byte], err error, opts []memstash.Option) *memstash.Cache[string, []byte] {
	b.Helper()
	if err != nil {
		b.Fatalf("adapter: %v", err)
	}
	cacheOpts := append(opts, memstash.WithL2Cache[string, []byte](store))
	c, err := memstash.New[string, []byte](cacheOpts...)
	if err != nil {
		b.Fatalf("memstash.New: %v", err)
	}
	return c
}

var (
	lazyGoredis       lazyClient[*goredislib.Client]
	lazyRueidis       lazyClient[rueidislib.Client]
	lazyRedigo        lazyClient[*redigolib.Pool]
	lazyRedispipe     lazyClient[*redisconn.Connection]
	lazyRueidisCl     lazyClient[rueidislib.Client]
	lazyGomemcache    lazyClient[*memcache.Client]
	lazyMc            lazyClient[*mclib.Client]
	lazyRainycape     lazyClient[*rainycape.Client]
	lazyPgxPool       lazyClient[*pgxpool.Pool]
	lazySQLPg         lazyClient[*sql.DB]
	lazySQLMy         lazyClient[*sql.DB]
	lazyMongo         lazyClient[*mongo.Collection]
	lazyDynamo        lazyClient[*dynamodb.Client]
	lazyAerospike     lazyClient[*as.Client]
	lazyTarantool     lazyClient[*tarantoollib.Connection]
	extraBenchBackend []benchBackend // cgo-only backends (valyala) register themselves here from init()
)

func benchBackends(b *testing.B) []benchBackend {
	b.Helper()
	backends := []benchBackend{
		{name: "redis/goredis", addr: redisAddr(), factory: func(b *testing.B, prefix string, opts ...memstash.Option) *memstash.Cache[string, []byte] {
			client, err := lazyGoredis.get(func() (*goredislib.Client, error) {
				return goredislib.NewClient(&goredislib.Options{Addr: redisAddr(), PoolSize: 64}), nil
			})
			if err != nil {
				b.Fatal(err)
			}
			store, aerr := goredis_adapter.New[string, []byte](client, l2.BytesCodec(), l2.WithKeyFunc(l2.PrefixedString(prefix)))
			return adapterCache(b, store, aerr, opts)
		}},
		{name: "redis/rueidis", addr: redisAddr(), factory: func(b *testing.B, prefix string, opts ...memstash.Option) *memstash.Cache[string, []byte] {
			client, err := lazyRueidis.get(func() (rueidislib.Client, error) {
				return rueidislib.NewClient(rueidislib.ClientOption{InitAddress: []string{redisAddr()}})
			})
			if err != nil {
				b.Fatal(err)
			}
			store, aerr := rueidis_adapter.New[string, []byte](client, l2.BytesCodec(), l2.WithKeyFunc(l2.PrefixedString(prefix)))
			return adapterCache(b, store, aerr, opts)
		}},
		{name: "redis/redigo", addr: redisAddr(), factory: func(b *testing.B, prefix string, opts ...memstash.Option) *memstash.Cache[string, []byte] {
			pool, err := lazyRedigo.get(func() (*redigolib.Pool, error) {
				return &redigolib.Pool{
					MaxIdle: 64,
					Dial:    func() (redigolib.Conn, error) { return redigolib.Dial("tcp", redisAddr()) },
				}, nil
			})
			if err != nil {
				b.Fatal(err)
			}
			store, aerr := redigo_adapter.New[string, []byte](pool, l2.BytesCodec(), l2.WithKeyFunc(l2.PrefixedString(prefix)))
			return adapterCache(b, store, aerr, opts)
		}},
		{name: "redis/redispipe", addr: redisAddr(), factory: func(b *testing.B, prefix string, opts ...memstash.Option) *memstash.Cache[string, []byte] {
			conn, err := lazyRedispipe.get(func() (*redisconn.Connection, error) {
				return redisconn.Connect(context.Background(), redisAddr(), redisconn.Opts{})
			})
			if err != nil {
				b.Fatal(err)
			}
			store, aerr := redispipe_adapter.New[string, []byte](conn, l2.BytesCodec(), l2.WithKeyFunc(l2.PrefixedString(prefix)))
			return adapterCache(b, store, aerr, opts)
		}},
		{name: "redis-cluster/rueidis", addr: redisClusterAddrs()[0], factory: func(b *testing.B, prefix string, opts ...memstash.Option) *memstash.Cache[string, []byte] {
			client, err := lazyRueidisCl.get(func() (rueidislib.Client, error) {
				return rueidislib.NewClient(rueidislib.ClientOption{InitAddress: redisClusterAddrs()})
			})
			if err != nil {
				b.Fatal(err)
			}
			store, aerr := rueidis_adapter.New[string, []byte](client, l2.BytesCodec(), l2.WithKeyFunc(l2.PrefixedString(prefix)))
			return adapterCache(b, store, aerr, opts)
		}},
		{name: "memcached/gomemcache", addr: memcachedAddr(), factory: func(b *testing.B, prefix string, opts ...memstash.Option) *memstash.Cache[string, []byte] {
			client, err := lazyGomemcache.get(func() (*memcache.Client, error) {
				c := memcache.New(memcachedAddr())
				c.MaxIdleConns = 64
				return c, nil
			})
			if err != nil {
				b.Fatal(err)
			}
			store, aerr := gomemcache_adapter.New[string, []byte](client, l2.BytesCodec(), l2.WithKeyFunc(l2.PrefixedString(prefix)))
			return adapterCache(b, store, aerr, opts)
		}},
		{name: "memcached/mc", addr: memcachedAddr(), factory: func(b *testing.B, prefix string, opts ...memstash.Option) *memstash.Cache[string, []byte] {
			client, err := lazyMc.get(func() (*mclib.Client, error) {
				cfg := mclib.DefaultConfig()
				cfg.PoolSize = 16
				return mclib.NewMCwithConfig(memcachedAddr(), "", "", cfg), nil
			})
			if err != nil {
				b.Fatal(err)
			}
			store, aerr := mc_adapter.New[string, []byte](client, l2.BytesCodec(), l2.WithKeyFunc(l2.PrefixedString(prefix)))
			return adapterCache(b, store, aerr, opts)
		}},
		{name: "memcached/rainycape", addr: memcachedAddr(), factory: func(b *testing.B, prefix string, opts ...memstash.Option) *memstash.Cache[string, []byte] {
			client, err := lazyRainycape.get(func() (*rainycape.Client, error) {
				c, err := rainycape.New(memcachedAddr())
				if err == nil {
					c.SetMaxIdleConnsPerAddr(64)
				}
				return c, err
			})
			if err != nil {
				b.Fatal(err)
			}
			store, aerr := rainycape_adapter.New[string, []byte](client, l2.BytesCodec(), l2.WithKeyFunc(l2.PrefixedString(prefix)))
			return adapterCache(b, store, aerr, opts)
		}},
		{name: "postgres/pgx", addr: postgresAddr(), factory: func(b *testing.B, prefix string, opts ...memstash.Option) *memstash.Cache[string, []byte] {
			pool, err := lazyPgxPool.get(func() (*pgxpool.Pool, error) {
				pool, err := pgxpool.New(context.Background(), postgresDSN()+"?pool_max_conns=32")
				if err != nil {
					return nil, err
				}
				_, err = pool.Exec(context.Background(), pgx_adapter.CreateTableSQL("membench_pgx"))
				return pool, err
			})
			if err != nil {
				b.Fatal(err)
			}
			store, aerr := pgx_adapter.New[string, []byte](pool, l2.BytesCodec(), "membench_pgx", l2.WithKeyFunc(l2.PrefixedString(prefix)))
			return adapterCache(b, store, aerr, opts)
		}},
		{name: "postgres/sql", addr: postgresAddr(), factory: func(b *testing.B, prefix string, opts ...memstash.Option) *memstash.Cache[string, []byte] {
			db, err := lazySQLPg.get(func() (*sql.DB, error) {
				db, err := sql.Open("pgx", postgresDSN())
				if err != nil {
					return nil, err
				}
				db.SetMaxOpenConns(32)
				db.SetMaxIdleConns(32)
				_, err = db.Exec("CREATE TABLE IF NOT EXISTS membench_sql (cache_key TEXT PRIMARY KEY, value BYTEA, expires_at BIGINT NOT NULL DEFAULT 0)")
				return db, err
			})
			if err != nil {
				b.Fatal(err)
			}
			store, aerr := sql_adapter.New[string, []byte](db, l2.BytesCodec(), "membench_sql", sql_adapter.Postgres, l2.WithKeyFunc(l2.PrefixedString(prefix)))
			return adapterCache(b, store, aerr, opts)
		}},
		{name: "mysql/sql", addr: mysqlAddr(), factory: func(b *testing.B, prefix string, opts ...memstash.Option) *memstash.Cache[string, []byte] {
			db, err := lazySQLMy.get(func() (*sql.DB, error) {
				db, err := sql.Open("mysql", "memstash:memstash@tcp("+mysqlAddr()+")/memstash?interpolateParams=true")
				if err != nil {
					return nil, err
				}
				db.SetMaxOpenConns(32)
				db.SetMaxIdleConns(32)
				// MEDIUMBLOB: the CDN scenario stores values beyond BLOB's 64 KB cap once framing is included.
				_, err = db.Exec("CREATE TABLE IF NOT EXISTS membench_sql (cache_key VARCHAR(255) PRIMARY KEY, value MEDIUMBLOB, expires_at BIGINT NOT NULL DEFAULT 0)")
				return db, err
			})
			if err != nil {
				b.Fatal(err)
			}
			store, aerr := sql_adapter.New[string, []byte](db, l2.BytesCodec(), "membench_sql", sql_adapter.MySQL, l2.WithKeyFunc(l2.PrefixedString(prefix)))
			return adapterCache(b, store, aerr, opts)
		}},
		{name: "mongo", addr: mongoAddr(), factory: func(b *testing.B, prefix string, opts ...memstash.Option) *memstash.Cache[string, []byte] {
			coll, err := lazyMongo.get(func() (*mongo.Collection, error) {
				client, err := mongo.Connect(context.Background(),
					mongooptions.Client().ApplyURI("mongodb://memstash:memstash@"+mongoAddr()).SetMaxPoolSize(64))
				if err != nil {
					return nil, err
				}
				return client.Database("memstash").Collection("bench"), nil
			})
			if err != nil {
				b.Fatal(err)
			}
			store, aerr := mongo_adapter.New[string, []byte](coll, l2.BytesCodec(), l2.WithKeyFunc(l2.PrefixedString(prefix)))
			return adapterCache(b, store, aerr, opts)
		}},
		{name: "dynamodb", addr: dynamoAddr(), factory: func(b *testing.B, prefix string, opts ...memstash.Option) *memstash.Cache[string, []byte] {
			client, err := lazyDynamo.get(func() (*dynamodb.Client, error) {
				// The default transport keeps only 10 idle conns per host; concurrent readers plus the write-back
				// drain churn through that, and every fresh dial pays the Docker relay. The usual Go+DynamoDB tuning.
				httpClient := awshttp.NewBuildableClient().WithTransportOptions(func(tr *nethttp.Transport) {
					tr.MaxIdleConnsPerHost = 64
				})
				cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
					awsconfig.WithRegion("us-east-1"),
					awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("local", "local", "")),
					awsconfig.WithHTTPClient(httpClient),
				)
				if err != nil {
					return nil, err
				}
				client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
					o.BaseEndpoint = aws.String("http://" + dynamoAddr())
				})
				_, err = client.CreateTable(context.Background(), &dynamodb.CreateTableInput{
					TableName: aws.String("membench"),
					AttributeDefinitions: []dynamotypes.AttributeDefinition{
						{AttributeName: aws.String("cache_key"), AttributeType: dynamotypes.ScalarAttributeTypeS},
					},
					KeySchema: []dynamotypes.KeySchemaElement{
						{AttributeName: aws.String("cache_key"), KeyType: dynamotypes.KeyTypeHash},
					},
					BillingMode: dynamotypes.BillingModePayPerRequest,
				})
				var exists *dynamotypes.ResourceInUseException
				if err != nil && !errors.As(err, &exists) {
					return nil, err
				}
				return client, nil
			})
			if err != nil {
				b.Fatal(err)
			}
			store, aerr := dynamo_adapter.New[string, []byte](client, l2.BytesCodec(), "membench", l2.WithKeyFunc(l2.PrefixedString(prefix)))
			return adapterCache(b, store, aerr, opts)
		}},
		{name: "aerospike", addr: aerospikeAddr(), factory: func(b *testing.B, prefix string, opts ...memstash.Option) *memstash.Cache[string, []byte] {
			client, err := lazyAerospike.get(func() (*as.Client, error) {
				host, portStr, err := net.SplitHostPort(aerospikeAddr())
				if err != nil {
					return nil, err
				}
				port, err := strconv.Atoi(portStr)
				if err != nil {
					return nil, err
				}
				client, aerr := as.NewClient(host, port)
				if aerr != nil {
					return nil, aerr
				}
				return client, nil
			})
			if err != nil {
				b.Fatal(err)
			}
			store, aerr := aerospike_adapter.New[string, []byte](client, l2.BytesCodec(), "memstash", "bench", l2.WithKeyFunc(l2.PrefixedString(prefix)))
			return adapterCache(b, store, aerr, opts)
		}},
		{name: "tarantool", addr: tarantoolAddr(), factory: func(b *testing.B, prefix string, opts ...memstash.Option) *memstash.Cache[string, []byte] {
			conn, err := lazyTarantool.get(func() (*tarantoollib.Connection, error) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				return tarantoollib.Connect(ctx, tarantoollib.NetDialer{
					Address: tarantoolAddr(), User: "memstash", Password: "memstash",
				}, tarantoollib.Opts{Timeout: 5 * time.Second})
			})
			if err != nil {
				b.Fatal(err)
			}
			store, aerr := tarantool_adapter.New[string, []byte](conn, l2.BytesCodec(), "memstash_cache", l2.WithKeyFunc(l2.PrefixedString(prefix)))
			return adapterCache(b, store, aerr, opts)
		}},
	}
	return append(backends, extraBenchBackend...)
}

// benchRunPrefix isolates one process run inside the shared backends.
var benchRunPrefix = fmt.Sprintf("membench|%d|", time.Now().UnixNano())

// replayCaches keeps one live cache per scenario+backend for the whole process: the ramp-up runs of one benchmark
// then measure a converging warm cache instead of paying a cold L1 and a write-back drain per run.
var replayCaches sync.Map // "scenario|backend" -> *memstash.Cache[string, []byte]

// benchReplay replays the scenario trace through a two-level cache: L1, then L2, a full miss stores the value
// (write-back, drained in the background).
func benchReplay(b *testing.B, backend benchBackend, sc *benchScenario) {
	benchRequireServer(b, backend.addr)
	sc.traceOnce.Do(func() { sc.trace = sc.buildTrace() })
	cacheKey := sc.name + "|" + backend.name
	cached, ok := replayCaches.Load(cacheKey)
	if !ok {
		cached = backend.factory(b, benchRunPrefix+cacheKey+"|",
			memstash.WithMemoryCapacity(sc.l1Budget),
			memstash.WithCostFunc(func(key string, value []byte) uint32 { return uint32(len(key) + len(value)) }),
			memstash.WithWriteBackBuffer(8192),
		)
		replayCaches.Store(cacheKey, cached)
	}
	c := cached.(*memstash.Cache[string, []byte])
	ctx := context.Background()

	var pos, l1Hits, l2Hits, lookups atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := sc.trace[int(pos.Add(1)-1)%len(sc.trace)]
			lookups.Add(1)
			if _, ok := c.GetFromMemory(key); ok {
				l1Hits.Add(1)
				continue
			}
			_, ok, err := c.Get(ctx, key)
			if err != nil {
				b.Errorf("Get %q: %v", key, err)
				return
			}
			if ok {
				l2Hits.Add(1)
				continue
			}
			if err := c.Set(ctx, key, sc.value(key)); err != nil { // cache-aside fill, drained by write-back
				b.Errorf("Set %q: %v", key, err)
				return
			}
		}
	})
	b.StopTimer()
	if n := lookups.Load(); n > 0 {
		b.ReportMetric(float64(l1Hits.Load())/float64(n), "l1hit-ratio")
		b.ReportMetric(float64(l2Hits.Load())/float64(n), "l2hit-ratio")
	}
}

func BenchmarkWebSessions(b *testing.B) { benchScenarioAll(b, benchScenarios[0]) }
func BenchmarkCDNAssets(b *testing.B)   { benchScenarioAll(b, benchScenarios[1]) }
func BenchmarkDBRows(b *testing.B)      { benchScenarioAll(b, benchScenarios[2]) }

func benchScenarioAll(b *testing.B, sc *benchScenario) {
	for _, backend := range benchBackends(b) {
		b.Run(backend.name, func(b *testing.B) { benchReplay(b, backend, sc) })
	}
}
