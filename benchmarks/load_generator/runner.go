package load_generator

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/l2"
	"github.com/zakonnic/memstash/tests/workload"
)

// runner is a Scenario plus everything the run needs: its cache, its log file and its counters. All runners of an app
// run in parallel, each with its own goroutines and log file.
type runner[K comparable, V any] struct {
	Scenario[K, V]

	cache   *memstash.Cache[K, V]
	closeL2 func() // releases the dialed client; nil when L1-only

	errLog *errorLog

	console *console
	slot    int

	logPath       string
	logFile       *os.File
	statsInterval time.Duration
	verify        bool

	probe *probe[K, V] // built on the first stats line; only the monitor goroutine touches it

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
	if len(r.Address) == 0 {
		cache, err := memstash.New[K, V](opts...)
		if err != nil {
			return fmt.Errorf("%s: %w", r.Name, err)
		}
		r.cache = cache
		return nil
	}

	codec := r.Codec
	if codec == nil {
		codec = defaultCodec[V]()
	}
	cache, closeL2, err := openL2[K, V](r.L2ClientType, r.Address, codec, opts)
	if err != nil {
		return fmt.Errorf("%s: dial %s %v: %w", r.Name, r.L2ClientType, r.Address, err)
	}
	r.cache, r.closeL2 = cache, closeL2
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
	if r.closeL2 != nil {
		r.closeL2()
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

// maxBacklog is how far behind schedule a worker may fall and still catch up: a GC pause or an L2 hiccup, not a
// backend that cannot keep up. Older debt is dropped, so that shows as ops_per_sec below target rather than a burst.
const maxBacklog = time.Second

// pacer turns a worker's rate into the ops each tick owes. The debt grows with the time that passed, not per tick: a
// ticker drops the ticks a busy worker misses, and their ops would be lost without a trace.
type pacer struct {
	rps  float64
	owed float64 // the fraction carries over, so non-integer rps stays accurate
	last time.Time
}

func (p *pacer) due(now time.Time) int {
	// The cap holds at least one op, or a worker under 1 rps would never reach one.
	p.owed = min(p.owed+p.rps*now.Sub(p.last).Seconds(), max(p.rps*maxBacklog.Seconds(), 1))
	p.last = now
	run := int(p.owed)
	p.owed -= float64(run)
	return run
}

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

	pace := pacer{rps: rps, last: time.Now()}
	ticker := time.NewTicker(workerTick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			// A worker catching up runs up to a second of ops in one go, so it checks for shutdown between them.
			for run := pace.due(now); run > 0 && ctx.Err() == nil; run-- {
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
		r.doGet(ctx, nextKey(rng, reads, r.KeySpace, random))
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
