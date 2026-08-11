package load_generator

import (
	"context"
	"log/slog"
	"runtime"
	"time"
)

// monitor logs a stats snapshot every statsInterval (and once more on shutdown) to the scenario's own log file,
// mirroring every line into the scenario's console slot.
func (r *runner[K, V]) monitor(ctx context.Context) {
	defer r.errLog.recoverPanic(r.Name, "monitor")

	handler := fanout{
		slog.NewJSONHandler(r.logFile, nil),
		slog.NewTextHandler(statusWriter{con: r.console, slot: r.slot}, statusOptions),
	}
	logger := slog.New(handler).With("scenario", r.Name)
	logger.Info("scenario started",
		"description", r.Description,
		"goroutines", r.Goroutines,
		"read_percent", r.ReadPercent,
		"key_space", r.KeySpace,
		"write_key_space", r.WriteKeySpace,
		"random_percent", r.RandomPercent,
		"random_key_sec", r.randomPeriod().Seconds(),
		"random_cover_sec", r.randomCover().Seconds(),
		"redis_address", r.RedisAddress, // empty when the scenario runs L1-only
		"verification", r.verify,
	)

	cpu, cpuErr := processCPUTime()
	if cpuErr != nil {
		logger.Warn("cpu time unavailable", "error", cpuErr.Error())
	}
	start := time.Now()
	prev := meter{t: start, cpu: cpu}

	ticker := time.NewTicker(r.statsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			prev = r.logStats(logger, start, prev)
		case <-ctx.Done():
			r.logStats(logger, start, prev)
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

func (r *runner[K, V]) logStats(logger *slog.Logger, start time.Time, prev meter) meter {
	now := time.Now()
	wall := now.Sub(prev.t).Seconds()

	ops, errs := r.ops.Load(), r.errs.Load()
	st := r.cache.Stats()
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
		"cache_len", r.cache.Len(),
		"cache_weight", r.cache.Weight(),
		"cache_total_size", r.cache.TotalSize(),
		"ops_total", ops,
		"gets_total", gets,
		"sets_total", sets,
		"hits_total", hits,
		"mem_hits_total", memHits,
		"l2_gets_total", l2Gets,
		"l2_hits_total", l2Hits,
		"misses_total", misses,
		"heap_alloc_bytes", mem.HeapAlloc,
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
