package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"log/slog"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v3"
	rueidislib "github.com/redis/rueidis"
	"github.com/zakonnic/memstash"
	rueidis_adapter "github.com/zakonnic/memstash/l2/rueidis_adapter"
	"github.com/zakonnic/memstash/tests/workload"
)

// scenario drives one cache with its own goroutines, key-space shape, and log file; all scenarios run in parallel.
type scenario struct {
	name        string
	description string
	cache       *memstash.Cache[string, []byte]
	cacheSize   int64 // for display only

	redisClient rueidislib.Client // nil when L1-only
	redisAddr   []string

	goroutines int
	rps        []float64 // target requests/sec per worker, len == goroutines

	readPercent int // 0-100: chance an op is a Get rather than a Set

	// Keys follow a Zipf distribution (skew zipfS, index 0 hottest): Gets over [0, keySpace), Sets over
	// [0, writeKeySpace). Zipf never reaches the tail of the keyspace. To cover it, a share of operations
	// uses uniformly random keys instead (randomPercent).
	keySpace      int
	writeKeySpace int
	zipfS         float64
	zipfV         float64
	randomPercent int

	// value derives a key's deterministic bytes (see values.go); truth holds them for every write key and is the
	// oracle a Get is checked against.
	value  valueFunc
	truth  *xsync.MapOf[string, []byte]
	errLog *errorLog

	logPath   string
	console   *console
	slot      int
	truthHeap int64

	ops, errs atomic.Int64
}

// cacheOptions are the options every scenario's cache shares.
func (s *scenario) cacheOptions() []memstash.Option {
	return []memstash.Option{
		memstash.WithStats(), // the monitor reports the cache's own counters
		memstash.WithOnL2Error[string, []byte](func(key string, err error) {
			s.errs.Add(1)
			s.errLog.l2Error(s.name, key, err)
		}),
		memstash.WithPanicHandler(func(recovered any, handled bool) {
			s.errs.Add(1)
			s.errLog.cachePanic(s.name, recovered, handled)
		}),
	}
}

func (s *scenario) buildCache(redisHosts []string, opts ...memstash.Option) error {
	s.redisAddr = redisHosts
	opts = append(s.cacheOptions(), opts...)
	if len(redisHosts) == 0 {
		cache, err := memstash.New[string, []byte](opts...)
		s.cache = cache
		return err
	}
	client, err := rueidislib.NewClient(rueidislib.ClientOption{InitAddress: redisHosts})
	if err != nil {
		return fmt.Errorf("dial redis %v: %w", redisHosts, err)
	}
	cache, err := rueidis_adapter.NewBytesCache[string](client, opts...)
	if err != nil {
		client.Close()
		return err
	}
	s.cache, s.redisClient = cache, client
	return nil
}

// randomRate is how many uniform draws per second the scenario makes; 0 when it makes none.
func (s *scenario) randomRate() float64 {
	var total float64
	for _, r := range s.rps {
		total += r
	}
	return total * float64(s.randomPercent) / 100
}

// randomPeriod is how long a given key waits between uniform draws.
func (s *scenario) randomPeriod() time.Duration {
	rate := s.randomRate()
	if rate <= 0 {
		return 0
	}
	return time.Duration(float64(s.writeKeySpace) / rate * float64(time.Second))
}

// randomCover is how long until every key has come up at least once: N*ln(N) draws, not N - the last few keys are
// the ones that keep the collector waiting.
func (s *scenario) randomCover() time.Duration {
	rate := s.randomRate()
	if rate <= 0 || s.writeKeySpace < 2 {
		return 0
	}
	n := float64(s.writeKeySpace)
	return time.Duration(n * math.Log(n) / rate * float64(time.Second))
}

// keyFor builds the key for index n. The scenario-name prefix keeps scenarios that share a Redis L2 from colliding
// (which would overwrite each other's values and trip the verification).
func (s *scenario) keyFor(n int) string { return fmt.Sprintf("%s:key-%d", s.name, n) }

// fillTruth populates the source of truth for every write key, in parallel, before any worker runs.
func (s *scenario) fillTruth() {
	s.truth = xsync.NewMapOf[string, []byte]()
	workers := runtime.NumCPU()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			defer s.errLog.recoverPanic(s.name, "fill-truth")
			for n := start; n < s.writeKeySpace; n += workers {
				key := s.keyFor(n)
				s.truth.Store(key, s.value(key))
			}
		}(w)
	}
	wg.Wait()
}

// run starts the worker goroutines and blocks in the monitor loop until ctx is canceled.
func (s *scenario) run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer func() {
		defer s.errLog.recoverPanic(s.name, "shutdown")
		s.cache.Close() // flush write-back to L2 before closing the client
		if s.redisClient != nil {
			s.redisClient.Close()
		}
	}()

	var workers sync.WaitGroup
	for i := 0; i < s.goroutines; i++ {
		workers.Add(1)
		go s.worker(ctx, &workers, i)
	}

	s.monitor(ctx)
	workers.Wait()
}

// workerTick paces each worker by running its owed ops in a batch per tick; one timer tick per op can't sustain
// high rps.
const workerTick = 10 * time.Millisecond

func (s *scenario) worker(ctx context.Context, wg *sync.WaitGroup, idx int) {
	defer wg.Done()
	defer s.errLog.recoverPanic(s.name, "worker")

	rps := s.rps[idx]
	if rps <= 0 {
		return
	}
	rng := workload.Random()
	reads := newZipf(rng, s.keySpace, s.zipfS, s.zipfV)
	writes := newZipf(rng, s.writeKeySpace, s.zipfS, s.zipfV)
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
				s.doOp(rng, reads, writes)
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

func (s *scenario) doOp(rng *rand.Rand, reads, writes *rand.Zipf) {
	ctx := context.Background()
	random := s.randomPercent > 0 && rng.Intn(100) < s.randomPercent
	if rng.Intn(100) < s.readPercent {
		s.doGet(ctx, s.keyFor(nextKey(rng, reads, s.writeKeySpace, random)))
	} else {
		s.doSet(ctx, s.keyFor(nextKey(rng, writes, s.writeKeySpace, random)))
	}
	s.ops.Add(1)
}

// doGet reads the key and checks any returned value against the source of truth. Cache/Redis errors and
// verification failures both count as errors and are logged.
func (s *scenario) doGet(ctx context.Context, key string) {
	got, ok, err := s.cache.Get(ctx, key)
	switch {
	case err != nil:
		s.errs.Add(1)
		s.errLog.opError(s.name, "get", key, 0, err)
	case ok:
		if want, known := s.truth.Load(key); !known {
			s.errs.Add(1)
			s.errLog.anomaly(s.name, key, got) // a value for a key we never wrote
		} else if !bytes.Equal(got, want) {
			s.errs.Add(1)
			s.errLog.mismatch(s.name, key, got, want)
		}
	}
}

// doSet writes the key's source-of-truth value, so a later Get always has the right bytes to verify against.
func (s *scenario) doSet(ctx context.Context, key string) {
	value, ok := s.truth.Load(key)
	if !ok { // pre-filled for every write key; defensive fallback
		value = s.value(key)
		s.truth.Store(key, value)
	}
	if err := s.cache.Set(ctx, key, value); err != nil {
		s.errs.Add(1)
		s.errLog.opError(s.name, "set", key, len(value), err)
	}
}

// monitor logs a stats snapshot once a minute (and once more on shutdown) to the scenario's own log file, mirroring
// every line into the scenario's console slot.
func (s *scenario) monitor(ctx context.Context) {
	defer s.errLog.recoverPanic(s.name, "monitor")

	logFile, err := os.OpenFile(s.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Printf("scenario %s: cannot open log file %s: %v", s.name, s.logPath, err)
		return
	}
	defer logFile.Close()

	handler := fanout{
		slog.NewJSONHandler(logFile, nil),
		slog.NewTextHandler(statusWriter{con: s.console, slot: s.slot}, statusOptions),
	}
	logger := slog.New(handler).With("scenario", s.name)
	logger.Info("scenario started",
		"description", s.description,
		"goroutines", s.goroutines,
		"read_percent", s.readPercent,
		"key_space", s.keySpace,
		"write_key_space", s.writeKeySpace,
		"random_percent", s.randomPercent,
		"random_key_sec", s.randomPeriod().Seconds(),
		"random_cover_sec", s.randomCover().Seconds(),
		"redis_address", s.redisAddr, // empty when the scenario runs L1-only
		"truth_map_heap_bytes", s.truthHeap,
	)

	cpu, cpuErr := processCPUTime()
	if cpuErr != nil {
		logger.Warn("cpu time unavailable", "error", cpuErr.Error())
	}
	start := time.Now()
	prev := meter{t: start, cpu: cpu}

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			prev = s.logStats(logger, start, prev)
		case <-ctx.Done():
			s.logStats(logger, start, prev)
			logger.Info("scenario stopped")
			return
		}
	}
}

// meter is the previous snapshot the windowed rates (ops/sec, hit rates, CPU) are measured against.
type meter struct {
	t                                        time.Time
	ops, gets, memHits, l2Gets, l2Hits, hits int64
	cpu                                      time.Duration
}

func (s *scenario) logStats(logger *slog.Logger, start time.Time, prev meter) meter {
	now := time.Now()
	wall := now.Sub(prev.t).Seconds()

	ops, errs := s.ops.Load(), s.errs.Load()
	st := s.cache.Stats()
	gets, sets := st.Gets(), st.Sets()
	memHits, l2Gets, l2Hits, hits, misses := st.MemoryHits(), st.L2Gets(), st.L2Hits(), st.Hits(), st.Misses()
	hitRate, memHitRate, l2HitRate := st.HitRate()*100, st.MemoryHitRate()*100, st.L2HitRate()*100

	opsPerSec := float64(0)
	if wall > 0 {
		opsPerSec = float64(ops-prev.ops) / wall
	}

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	cpu, cpuErr := processCPUTime()
	cpuCores, cpuPercent := float64(0), float64(0)
	if cpuErr == nil && wall > 0 {
		cpuCores = (cpu - prev.cpu).Seconds() / wall
		cpuPercent = cpuCores / float64(runtime.NumCPU()) * 100
	}

	// Field order matters: the console wraps this line across the terminal, so the live numbers come first and the
	// running totals after.
	logger.Info("stats",
		"uptime_sec", now.Sub(start).Seconds(),
		"ops_per_sec", opsPerSec,
		"errors_total", errs,
		"hit_rate_pct", hitRate,
		"mem_hit_rate_pct", memHitRate,
		"l2_hit_rate_pct", l2HitRate,
		"cache_len", s.cache.Len(),
		"cache_weight", s.cache.Weight(),
		"cache_total_size", s.cache.TotalSize(),
		"ops_total", ops,
		"gets_total", gets,
		"sets_total", sets,
		"hits_total", hits,
		"mem_hits_total", memHits,
		"l2_gets_total", l2Gets,
		"l2_hits_total", l2Hits,
		"misses_total", misses,
		"heap_alloc_bytes", max(int64(mem.HeapAlloc)-s.truthHeap, 0),
		"process_sys_bytes", mem.Sys,
		"process_goroutines", runtime.NumGoroutine(),
		"process_cpu_cores", cpuCores,
		"process_cpu_percent", cpuPercent,
	)

	if cpuErr != nil {
		cpu = prev.cpu // no fresh reading; keep the old baseline so the next interval measures from it
	}
	return meter{t: now, ops: ops, gets: gets, memHits: memHits, l2Gets: l2Gets, l2Hits: l2Hits, hits: hits, cpu: cpu}
}
