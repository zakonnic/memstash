package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"log/slog"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v3"
	rueidislib "github.com/redis/rueidis"
	"github.com/zakonnic/memstash"
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
	// [0, writeKeySpace). keySpace is several times L1 capacity, so L1 holds only the hot head and the tail leans on
	// L2 or misses.
	keySpace      int
	writeKeySpace int
	zipfS         float64

	// value derives a key's deterministic bytes (see values.go); truth holds them for every write key and is the
	// oracle a Get is checked against.
	value  valueFunc
	truth  *xsync.MapOf[string, []byte]
	errLog *errorLog

	logPath string

	ops, errs atomic.Int64
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

	rps := s.rps[idx]
	if rps <= 0 {
		return
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(idx)<<32 ^ int64(len(s.name))))
	reads := newZipf(rng, s.keySpace, s.zipfS)
	writes := newZipf(rng, s.writeKeySpace, s.zipfS)
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

// newZipf builds a Zipf generator over [0, n) with skew s (index 0 hottest). s is clamped above 1, which NewZipf
// requires.
func newZipf(rng *rand.Rand, n int, s float64) *rand.Zipf {
	if s <= 1 {
		s = 1.001
	}
	if n < 2 {
		n = 2
	}
	return rand.NewZipf(rng, s, 1, uint64(n-1))
}

func (s *scenario) doOp(rng *rand.Rand, reads, writes *rand.Zipf) {
	ctx := context.Background()
	if rng.Intn(100) < s.readPercent {
		s.doGet(ctx, s.keyFor(int(reads.Uint64())))
	} else {
		s.doSet(ctx, s.keyFor(int(writes.Uint64())))
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

// monitor logs a stats snapshot once a minute (and once more on shutdown) to the scenario's own log file.
func (s *scenario) monitor(ctx context.Context) {
	logFile, err := os.OpenFile(s.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Printf("scenario %s: cannot open log file %s: %v", s.name, s.logPath, err)
		return
	}
	defer logFile.Close()

	logger := slog.New(slog.NewJSONHandler(logFile, nil)).With("scenario", s.name)
	logger.Info("scenario started",
		"description", s.description,
		"goroutines", s.goroutines,
		"read_percent", s.readPercent,
		"key_space", s.keySpace,
		"write_key_space", s.writeKeySpace,
		"redis_address", s.redisAddr, // empty when the scenario runs L1-only
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

	opsPerSec := float64(0)
	if wall > 0 {
		opsPerSec = float64(ops-prev.ops) / wall
	}
	// Rates over this interval, not since boot, so they reflect the current steady state rather than being dragged
	// down forever by cold-start misses.
	hitRate, memHitRate, l2HitRate := float64(0), float64(0), float64(0)
	if dg := gets - prev.gets; dg > 0 {
		hitRate = float64(hits-prev.hits) / float64(dg) * 100
		memHitRate = float64(memHits-prev.memHits) / float64(dg) * 100
	}
	if dl2 := l2Gets - prev.l2Gets; dl2 > 0 {
		l2HitRate = float64(l2Hits-prev.l2Hits) / float64(dl2) * 100
	}

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	cpu, cpuErr := processCPUTime()
	cpuCores, cpuPercent := float64(0), float64(0)
	if cpuErr == nil && wall > 0 {
		cpuCores = (cpu - prev.cpu).Seconds() / wall
		cpuPercent = cpuCores / float64(runtime.NumCPU()) * 100
	}

	logger.Info("stats",
		"uptime_sec", now.Sub(start).Seconds(),
		"cache_len", s.cache.Len(),
		"cache_weight_bytes", s.cache.TotalWeight(),
		"ops_total", ops,
		"gets_total", gets,
		"sets_total", sets,
		"hits_total", hits,
		"mem_hits_total", memHits,
		"l2_gets_total", l2Gets,
		"l2_hits_total", l2Hits,
		"misses_total", misses,
		"errors_total", errs,
		"hit_rate_pct", hitRate,
		"mem_hit_rate_pct", memHitRate,
		"l2_hit_rate_pct", l2HitRate,
		"ops_per_sec", opsPerSec,
		// Process-wide (shared with the other scenarios running in this client), not scenario-isolated.
		"process_heap_alloc_bytes", mem.HeapAlloc,
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
