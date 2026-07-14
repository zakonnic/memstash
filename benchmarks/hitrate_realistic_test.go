package benchmarks

import (
	"testing"

	"github.com/dustin/go-humanize"
	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/tests/workload"
)

// Realistic-workload hit-rate comparison: string keys and []byte values shaped like real payloads (JSON sessions,
// static assets, serialized DB rows) instead of uint64/uint64. Caches are bounded by a byte budget, not an item
// count, so variable value sizes matter: byte-aware caches must balance "many small" against "few large" entries.
// Every generator is deterministic (fixed seeds), so each cache receives exactly the same stream.
//
// Budgets are chosen well below the working-set footprint of each trace (roughly 15-30% of it), otherwise every
// policy converges to the same compulsory-miss ceiling and the comparison degenerates (see hitrate_test.go).

// contentBlob is a shared pool of deterministic printable bytes; values are sliced/copied out of it instead of
// being generated per key, which keeps value construction cheap on the miss path.
var contentBlob = workload.NewBlob(99, workload.DefaultBlobSize)

// --- scenario 1: web session store ---
// Zipf popularity over 3M session tokens, ~2M requests: a session-cookie lookup cache in front of an auth service.
// Values are ~350-650 byte JSON session documents.
var hitrateSessions = workload.SessionScenario{Catalog: 3_000_000, TraceLen: 2_000_000}

// --- scenario 2: CDN / static-asset cache ---
// 75% Zipf (s=1.05) over a 1M-object catalogue plus 25% one-hit wonders (cache-busting URLs, crawlers), ~1.5M
// requests. Value sizes are bimodal like real assets: ~90% small files (0.6-8 KB) and ~10% large ones (8-64 KB),
// so the byte budget forces a "many small vs few large" trade-off.
var hitrateCDN = workload.CDNScenario{Catalog: 1_000_000, TraceLen: 1_500_000}

// --- scenario 3: DB row cache ---
// Zipf point lookups over a 2M-row table, with a recurring sequential scan of the first 200k rows (a report job)
// injected every 500k requests; ~2M requests total. Values are ~250-380 byte serialized rows.
var hitrateDBRows = workload.DBScenario{Rows: 2_000_000, ScanRows: 200_000, ChunkSize: 500_000, TraceLen: 2_000_000}

// TestHitRateRealistic prints a hit-rate table per scenario at an equal byte budget per cache.
func TestHitRateRealistic(t *testing.T) {
	if testing.Short() {
		t.Skip("long comparative run")
	}
	scenarios := []struct {
		name string
		// budget is the byte budget every cache gets; avgEntry is the estimated key+value+overhead bytes per entry
		// used to translate the budget into an item count for count-bounded caches (hashicorp-lru) and into
		// counter/window sizing hints (ristretto, bigcache).
		buildTrace func() []string
		value      func(key string) []byte
		budget     int64
		avgEntry   int
	}{
		{name: "web-sessions", buildTrace: hitrateSessions.Trace,
			value:  func(key string) []byte { return hitrateSessions.Value(contentBlob, key) },
			budget: 64 << 20, avgEntry: 37 + 500 + 48},
		{name: "cdn-assets", buildTrace: hitrateCDN.Trace,
			value:  func(key string) []byte { return hitrateCDN.Value(contentBlob, key) },
			budget: 256 << 20, avgEntry: 34 + 7_300 + 48},
		{name: "db-rows", buildTrace: hitrateDBRows.Trace,
			value:  func(key string) []byte { return hitrateDBRows.Value(contentBlob, key) },
			budget: 48 << 20, avgEntry: 13 + 300 + 48},
	}

	for _, sc := range scenarios {
		trace := sc.buildTrace()
		builders := []func() benchCacheBytes{
			func() benchCacheBytes { return newMemstashBytes(sc.budget, memstash.PolicyS3FIFO, "memstash-s3fifo") },
			func() benchCacheBytes { return newMemstashBytes(sc.budget, memstash.PolicyClock, "memstash-clock") },
			func() benchCacheBytes {
				return newMemstashBytes(sc.budget, memstash.PolicyWTinyLFU, "memstash-wtinylfu")
			},
			func() benchCacheBytes { return newMemstashBytes(sc.budget, memstash.PolicySIEVE, "memstash-sieve") },
			func() benchCacheBytes { return newRistrettoBytes(sc.budget, sc.avgEntry) },
			func() benchCacheBytes { return newOtterBytes(sc.budget) },
			func() benchCacheBytes { return newTheineBytes(sc.budget) },
			func() benchCacheBytes { return newBigcacheBytes(sc.budget, sc.avgEntry) },
			func() benchCacheBytes { return newFreecacheBytes(sc.budget) },
			func() benchCacheBytes { return newLRUBytes(sc.budget, sc.avgEntry) },
		}
		t.Logf("---- %s: budget %s, %s requests ----", sc.name, humanize.IBytes(uint64(sc.budget)), humanize.Comma(int64(len(trace))))
		t.Logf("%-18s %8s %12s", "cache", "hit rate", "size estimate")
		for _, build := range builders {
			cache := build()
			hitRate := runTraceBytes(cache, trace, sc.value)
			sizeBytes := cache.GetSize()
			t.Logf("%-18s %7.2f%% %12s", cache.Name(), hitRate, humanize.Bytes(sizeBytes))
			cache.Close()
		}
	}
}

// runTraceBytes replays the stream through the cache: a miss is counted and triggers a Set with the key's
// deterministic value.
func runTraceBytes(c benchCacheBytes, trace []string, value func(key string) []byte) float64 {
	hits := 0
	for _, key := range trace {
		if _, ok := c.Get(key); ok {
			hits++
			continue
		}
		c.Set(key, value(key), true)
	}
	return 100 * float64(hits) / float64(len(trace))
}
