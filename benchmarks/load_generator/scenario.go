package main

import (
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

	"github.com/zakonnic/memstash"
)

// scenario drives one cache with its own goroutines, its own request-rate/key-space shape, and its own log file.
// All scenarios run in the same process, in parallel and independently of each other.
type scenario struct {
	name        string
	description string // human-readable summary printed to the console at startup
	cache       *memstash.Cache[string, []byte]
	cacheSize   int64 // capacity passed to WithMemoryCapacity when the cache was built; for display only

	goroutines int       // number of worker goroutines
	rps        []float64 // target requests/sec per worker, len(rps) == goroutines

	readPercent int // chance (0-100) that an operation is a Get rather than a Set

	// Get keys are drawn uniformly from [0, keySpace); Set keys are drawn uniformly from [0, writeKeySpace).
	// writeKeySpace < keySpace means some fraction of Gets can never hit (those keys were never written).
	keySpace      int
	writeKeySpace int

	newValue func(rng *rand.Rand) []byte // value generator for Set

	logPath string

	ops, gets, sets, hits, misses, errs atomic.Int64
}

// run starts the worker goroutines and blocks in the monitor loop until ctx is canceled.
func (s *scenario) run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer s.cache.Close()

	var workers sync.WaitGroup
	for i := 0; i < s.goroutines; i++ {
		workers.Add(1)
		go s.worker(ctx, &workers, i)
	}

	s.monitor(ctx)
	workers.Wait()
}

const workerTick = 10 * time.Millisecond

func (s *scenario) worker(ctx context.Context, wg *sync.WaitGroup, idx int) {
	defer wg.Done()

	rng := rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(idx)<<32 ^ int64(len(s.name))))
	rps := s.rps[idx]
	if rps <= 0 {
		return
	}
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
				s.doOp(rng)
			}
		}
	}
}

func (s *scenario) doOp(rng *rand.Rand) {
	ctx := context.Background()
	if rng.Intn(100) < s.readPercent {
		key := fmt.Sprintf("key-%d", rng.Intn(s.keySpace))
		_, ok, err := s.cache.Get(ctx, key)
		s.gets.Add(1)
		switch {
		case err != nil:
			s.errs.Add(1)
		case ok:
			s.hits.Add(1)
		default:
			s.misses.Add(1)
		}
	} else {
		key := fmt.Sprintf("key-%d", rng.Intn(s.writeKeySpace))
		if err := s.cache.Set(ctx, key, s.newValue(rng)); err != nil {
			s.errs.Add(1)
		}
		s.sets.Add(1)
	}
	s.ops.Add(1)
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
	)

	start := time.Now()
	lastTime := start
	var lastOps int64
	lastCPU, cpuErr := processCPUTime()
	if cpuErr != nil {
		logger.Warn("cpu time unavailable", "error", cpuErr.Error())
	}

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			lastTime, lastOps, lastCPU = s.logStats(logger, start, lastTime, lastOps, lastCPU)
		case <-ctx.Done():
			s.logStats(logger, start, lastTime, lastOps, lastCPU)
			logger.Info("scenario stopped")
			return
		}
	}
}

func (s *scenario) logStats(logger *slog.Logger, start, lastTime time.Time, lastOps int64, lastCPU time.Duration) (time.Time, int64, time.Duration) {
	now := time.Now()
	wallDelta := now.Sub(lastTime)

	ops, gets, sets, hits, misses, errs := s.ops.Load(), s.gets.Load(), s.sets.Load(), s.hits.Load(), s.misses.Load(), s.errs.Load()
	opsDelta := ops - lastOps
	opsPerSec := float64(0)
	if wallDelta > 0 {
		opsPerSec = float64(opsDelta) / wallDelta.Seconds()
	}

	hitRate := float64(0)
	if gets > 0 {
		hitRate = float64(hits) / float64(gets) * 100
	}

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	cpuNow, cpuErr := processCPUTime()
	cpuCores, cpuPercent := float64(0), float64(0)
	if cpuErr == nil && wallDelta > 0 {
		cpuCores = (cpuNow - lastCPU).Seconds() / wallDelta.Seconds()
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
		"misses_total", misses,
		"errors_total", errs,
		"hit_rate_pct", hitRate,
		"ops_per_sec", opsPerSec,
		// Process-wide (shared with the other scenarios running in this client), not scenario-isolated.
		"process_heap_alloc_bytes", mem.HeapAlloc,
		"process_sys_bytes", mem.Sys,
		"process_goroutines", runtime.NumGoroutine(),
		"process_cpu_cores", cpuCores,
		"process_cpu_percent", cpuPercent,
	)

	if cpuErr == nil {
		lastCPU = cpuNow
	}
	return now, ops, lastCPU
}
