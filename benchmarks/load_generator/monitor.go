package load_generator

import (
	"context"
	"log/slog"
	"runtime"
	"time"

	"github.com/zakonnic/memstash"
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
		"target_rps", r.totalRPS(),
		"read_percent", r.ReadPercent,
		"key_space", r.KeySpace,
		"write_key_space", r.WriteKeySpace,
		"random_percent", r.RandomPercent,
		"address", r.Address, // empty when the scenario runs L1-only
		"l2_client", r.L2ClientType,
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
	t      time.Time
	ops    int64
	cpu    time.Duration
	cache  counts
	probed counts
}

// counts are the cache counters a stats line reports.
type counts struct{ gets, sets, hits, misses, memHits, l2Gets, l2Hits int64 }

func countsOf(st memstash.Stats) counts {
	return counts{gets: st.Gets(), sets: st.Sets(), hits: st.Hits(), misses: st.Misses(), memHits: st.MemoryHits(),
		l2Gets: st.L2Gets(), l2Hits: st.L2Hits()}
}

func (c counts) sub(o counts) counts {
	return counts{gets: c.gets - o.gets, sets: c.sets - o.sets, hits: c.hits - o.hits, misses: c.misses - o.misses,
		memHits: c.memHits - o.memHits, l2Gets: c.l2Gets - o.l2Gets, l2Hits: c.l2Hits - o.l2Hits}
}

// percent is part of whole in percent, 0 for an empty whole.
func percent(part, whole int64) float64 {
	if whole <= 0 {
		return 0
	}
	return float64(part) / float64(whole) * 100
}

func (r *runner[K, V]) logStats(logger *slog.Logger, start time.Time, prev meter) meter {
	now := time.Now()
	wall := now.Sub(prev.t).Seconds()

	ops, errs := r.ops.Load(), r.errs.Load()
	cache := countsOf(r.cache.Stats()).sub(prev.probed)
	window := cache.sub(prev.cache)
	hitRate, memHitRate := percent(window.hits, window.gets), percent(window.memHits, window.gets)
	l2HitRate := percent(window.l2Hits, window.l2Gets)

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

	getMemNs, getNs, setNs := r.runProbe(context.Background())
	probed := countsOf(r.cache.Stats()).sub(cache)

	// Field order matters: the console wraps this line across the terminal, so the live numbers come first and the
	// running totals after.
	logger.Info("stats",
		"uptime_sec", now.Sub(start).Seconds(),
		"ops_per_sec", opsPerSec,
		"errors_total", errs,
		"hit_rate_pct", hitRate,
		"mem_hit_rate_pct", memHitRate,
		"l2_hit_rate_pct", l2HitRate,
		"get_mem_ns", getMemNs,
		"get_ns", getNs,
		"set_ns", setNs,
		"cache_len", r.cache.Len(),
		"cache_weight", r.cache.Weight(),
		"cache_total_size", r.cache.TotalSize(),
		"ops_total", ops,
		"gets_total", cache.gets,
		"sets_total", cache.sets,
		"hits_total", cache.hits,
		"mem_hits_total", cache.memHits,
		"l2_gets_total", cache.l2Gets,
		"l2_hits_total", cache.l2Hits,
		"misses_total", cache.misses,
		"heap_alloc_bytes", mem.HeapAlloc,
		"process_sys_bytes", mem.Sys,
		"process_goroutines", runtime.NumGoroutine(),
		"process_cpu_cores", cpuCores,
		"process_cpu_percent", cpuPercent,
	)

	if cpuErr != nil {
		cpu = prev.cpu // no fresh reading; keep the old baseline so the next interval measures from it
	}
	return meter{t: now, ops: ops, cpu: cpu, cache: cache, probed: probed}
}
