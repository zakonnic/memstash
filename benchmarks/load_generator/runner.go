package load_generator

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	rueidislib "github.com/redis/rueidis"
	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
	rueidis_adapter "github.com/zakonnic/memstash/l2/rueidis_adapter"
	"github.com/zakonnic/memstash/tests/workload"
)

// runner is a Scenario plus everything the run needs: its cache, its log file and its counters. All runners of an app
// run in parallel, each with its own goroutines and log file.
type runner[K comparable, V any] struct {
	Scenario[K, V]

	cache       *memstash.Cache[K, V]
	redisClient rueidislib.Client // nil when L1-only

	errLog *errorLog

	console *console
	slot    int

	logPath       string
	logFile       *os.File
	statsInterval time.Duration
	verify        bool

	ops, errs atomic.Int64
}

// cacheOptions are the options the app sets on every scenario's cache; the scenario's own come after them.
func (r *runner[K, V]) cacheOptions() []memstash.Option {
	return []memstash.Option{
		memstash.WithStats(), // the monitor reports the cache's own counters
		memstash.WithMemoryCapacity(r.CacheSize),
		memstash.WithOnL2Error[K, V](func(key K, err error) {
			r.errs.Add(1)
			r.errLog.l2Error(r.Name, key, err)
		}),
		memstash.WithPanicHandler(func(recovered any, handled bool) {
			r.errs.Add(1)
			r.errLog.cachePanic(r.Name, recovered, handled)
		}),
	}
}

// open builds the cache (dialing Redis when the scenario has an L2) and opens the scenario's log file.
func (r *runner[K, V]) open() error {
	f, err := os.OpenFile(r.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("%s: open log file %s: %w", r.Name, r.logPath, err)
	}
	r.logFile = f

	opts := append(r.cacheOptions(), r.CacheOptions...)
	if len(r.RedisAddress) == 0 {
		cache, err := memstash.New[K, V](opts...)
		if err != nil {
			return fmt.Errorf("%s: %w", r.Name, err)
		}
		r.cache = cache
		return nil
	}

	client, err := rueidislib.NewClient(rueidislib.ClientOption{InitAddress: r.RedisAddress})
	if err != nil {
		return fmt.Errorf("%s: dial redis %v: %w", r.Name, r.RedisAddress, err)
	}
	codec := r.Codec
	if codec == nil {
		codec = defaultCodec[V]()
	}
	cache, err := rueidis_adapter.NewCache[K, V](client, codec, opts...)
	if err != nil {
		client.Close()
		return fmt.Errorf("%s: %w", r.Name, err)
	}
	r.cache, r.redisClient = cache, client
	return nil
}

// defaultCodec keeps the values that already are bytes on the wire as they are, and falls back to JSON for value
// types the L2 cannot store on its own.
func defaultCodec[V any]() memstash.Codec[V] {
	switch any(*new(V)).(type) {
	case []byte:
		if codec, ok := any(l2.BytesCodec()).(memstash.Codec[V]); ok {
			return codec
		}
	case string:
		if codec, ok := any(l2.StringCodec()).(memstash.Codec[V]); ok {
			return codec
		}
	}
	return l2.JSONCodec[V]()
}

// close flushes the write-back buffer to L2 and releases the cache, the Redis client and the log file.
func (r *runner[K, V]) close() {
	defer r.errLog.recoverPanic(r.Name, "shutdown")
	if r.cache != nil {
		r.cache.Close() // flush write-back to L2 before closing the client
	}
	if r.redisClient != nil {
		r.redisClient.Close()
	}
	if r.logFile != nil {
		r.logFile.Close()
	}
}

// run starts the worker goroutines and blocks in the monitor loop until ctx is canceled.
func (r *runner[K, V]) run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	var workers sync.WaitGroup
	for i := 0; i < r.Goroutines; i++ {
		workers.Add(1)
		go r.worker(ctx, &workers, i)
	}

	r.monitor(ctx)
	workers.Wait()
}

// workerTick paces each worker by running its owed ops in a batch per tick; one timer tick per op can't sustain
// high rps.
const workerTick = 10 * time.Millisecond

func (r *runner[K, V]) worker(ctx context.Context, wg *sync.WaitGroup, idx int) {
	defer wg.Done()
	defer r.errLog.recoverPanic(r.Name, "worker")

	rps := r.RPS[idx]
	if rps <= 0 {
		return
	}
	rng := workload.Random()
	reads := newZipf(rng, r.KeySpace, r.ZipfS, r.ZipfV)
	writes := newZipf(rng, r.WriteKeySpace, r.ZipfS, r.ZipfV)
	opsPerTick := rps * workerTick.Seconds()

	ticker := time.NewTicker(workerTick)
	defer ticker.Stop()

	var owed float64 // fractional operations carried over from the previous tick, so non-integer rps stays accurate
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			owed += opsPerTick
			n := int(owed)
			owed -= float64(n)
			for i := 0; i < n; i++ {
				r.doOp(rng, reads, writes)
			}
		}
	}
}

// newZipf builds a Zipf generator over [0, n) with skew s and shift v (index 0 hottest).
func newZipf(rng *rand.Rand, n int, s, v float64) *rand.Zipf {
	if s <= 1 {
		s = 1.001
	}
	if v < 1 {
		v = 1
	}
	if n < 2 {
		n = 2
	}
	return rand.NewZipf(rng, s, v, uint64(n-1))
}

func nextKey(rng *rand.Rand, z *rand.Zipf, n int, random bool) int {
	if random {
		return rng.Intn(n)
	}
	return int(z.Uint64())
}

func (r *runner[K, V]) doOp(rng *rand.Rand, reads, writes *rand.Zipf) {
	ctx := context.Background()
	random := r.RandomPercent > 0 && rng.Intn(100) < r.RandomPercent
	if rng.Intn(100) < r.ReadPercent {
		r.doGet(ctx, nextKey(rng, reads, r.WriteKeySpace, random))
	} else {
		r.doSet(ctx, nextKey(rng, writes, r.WriteKeySpace, random))
	}
	r.ops.Add(1)
}

// doGet reads key n and checks any returned value against what Value returns for it, unless verification is off.
// Cache/Redis errors and verification failures both count as errors and are logged.
func (r *runner[K, V]) doGet(ctx context.Context, n int) {
	key := r.Key(n)
	got, ok, err := r.cache.Get(ctx, key)
	switch {
	case err != nil:
		r.errs.Add(1)
		r.errLog.opError(r.Name, "get", key, nil, err)
	case ok && r.verify:
		// Reads run over KeySpace but writes only over WriteKeySpace, so a hit above it is a value nobody wrote.
		if n >= r.WriteKeySpace {
			r.errs.Add(1)
			r.errLog.badKey(r.Name, key, got)
		} else if want := r.Value(key); !r.Equal(got, want) {
			r.errs.Add(1)
			r.errLog.badValue(r.Name, key, got, want)
		}
	}
}

// doSet writes what Value returns for key n, so a later Get has the right bytes to verify against.
func (r *runner[K, V]) doSet(ctx context.Context, n int) {
	key := r.Key(n)
	value := r.Value(key)
	if err := r.cache.Set(ctx, key, value); err != nil {
		r.errs.Add(1)
		r.errLog.opError(r.Name, "set", key, value, err)
	}
}
