package benchmarks

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/dustin/go-humanize"
	"github.com/zakonnic/memstash"
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
var contentBlob = func() []byte {
	rng := rand.New(rand.NewSource(99))
	b := make([]byte, 256*1024)
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+/"
	for i := range b {
		b[i] = alphabet[rng.Intn(len(alphabet))]
	}
	return b
}()

// fnv64 gives a deterministic per-key hash used to derive stable value sizes and contents.
func fnv64(s string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

// splitmix scrambles sequential ids into token-looking values.
func splitmix(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// --- scenario 1: web session store ---
// Zipf popularity over 3M session tokens, ~2M requests: a session-cookie lookup cache in front of an auth service.
// Values are ~350-650 byte JSON session documents.

func sessionKey(id uint64) string { return fmt.Sprintf("sess:%032x", splitmix(id)) }

func sessionTrace() []string {
	rng := rand.New(rand.NewSource(11))
	zipf := rand.NewZipf(rng, 1.001, 10, 3_000_000-1)
	trace := make([]string, 2_000_000)
	for i := range trace {
		trace[i] = sessionKey(zipf.Uint64())
	}
	return trace
}

func sessionValue(key string) []byte {
	h := fnv64(key)
	pad := int(h % 300)
	v := make([]byte, 0, 420+pad)
	v = fmt.Appendf(v,
		`{"sid":%q,"uid":%d,"login":"user%07d","roles":["user","editor"],"created_at":%d,"expires_at":%d,"csrf":"%016x","data":"`,
		key[5:], h%9_000_000, h%9_000_000, 1_752_000_000-h%86_400, 1_752_000_000+3_600, splitmix(h))
	off := int(h % uint64(len(contentBlob)-300))
	v = append(v, contentBlob[off:off+pad]...)
	return append(v, '"', '}')
}

// --- scenario 2: CDN / static-asset cache ---
// 75% Zipf (s=1.05) over a 1M-object catalogue plus 25% one-hit wonders (cache-busting URLs, crawlers), ~1.5M
// requests. Value sizes are bimodal like real assets: ~90% small files (0.6-8 KB) and ~10% large ones (8-64 KB),
// so the byte budget forces a "many small vs few large" trade-off.

func cdnKey(id uint64) string {
	dirs := [...]string{"img", "js", "css", "media"}
	exts := [...]string{"webp", "js", "css", "mp4"}
	d := id % 4
	return fmt.Sprintf("/static/%s/%016x.%s", dirs[d], splitmix(id), exts[d])
}

func cdnTrace() []string {
	const catalog = 1_000_000
	rng := rand.New(rand.NewSource(12))
	zipf := rand.NewZipf(rng, 1.05, 10, catalog-1)
	trace := make([]string, 1_500_000)
	oneHit := uint64(catalog)
	for i := range trace {
		if rng.Intn(4) == 0 {
			trace[i] = cdnKey(oneHit)
			oneHit++
		} else {
			trace[i] = cdnKey(zipf.Uint64())
		}
	}
	return trace
}

func cdnValue(key string) []byte {
	h := fnv64(key)
	var size int
	if h%10 == 0 {
		size = 8*1024 + int(h%(56*1024)) // large asset: 8-64 KB
	} else {
		size = 600 + int(h%(7*1024)) // small asset: 0.6-8 KB
	}
	off := int(h % uint64(len(contentBlob)-64*1024))
	return contentBlob[off : off+size]
}

// --- scenario 3: DB row cache ---
// Zipf point lookups over a 2M-row table, with a recurring sequential scan of the first 200k rows (a report job)
// injected every 500k requests; ~2M requests total. Values are ~250-380 byte serialized rows.

func dbKey(id uint64) string { return fmt.Sprintf("user:%08d", id) }

func dbTrace() []string {
	const (
		rows     = 2_000_000
		total    = 2_000_000
		scanRows = 200_000
	)
	rng := rand.New(rand.NewSource(13))
	zipf := rand.NewZipf(rng, 1.001, 10, rows-1)
	trace := make([]string, 0, total)
	for len(trace) < total {
		for i := 0; i < 500_000 && len(trace) < total; i++ {
			trace = append(trace, dbKey(zipf.Uint64()))
		}
		for i := uint64(0); i < scanRows && len(trace) < total; i++ {
			trace = append(trace, dbKey(i))
		}
	}
	return trace
}

func dbValue(key string) []byte {
	h := fnv64(key)
	v := make([]byte, 0, 320)
	v = fmt.Appendf(v,
		"id=%s;name=user_%07d;email=user%07d@example.com;plan=%s;balance=%d.%02d;created=%d;flags=%08b;bio=",
		key[5:], h%9_000_000, h%9_000_000, [...]string{"free", "pro", "team"}[h%3], h%100_000, h%100,
		1_600_000_000+h%100_000_000, h%256)
	off := int(h % uint64(len(contentBlob)-160))
	return append(v, contentBlob[off:off+120]...)
}

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
		{name: "web-sessions", buildTrace: sessionTrace, value: sessionValue, budget: 64 << 20, avgEntry: 37 + 500 + 48},
		{name: "cdn-assets", buildTrace: cdnTrace, value: cdnValue, budget: 256 << 20, avgEntry: 34 + 7_300 + 48},
		{name: "db-rows", buildTrace: dbTrace, value: dbValue, budget: 48 << 20, avgEntry: 13 + 300 + 48},
	}

	for _, sc := range scenarios {
		trace := sc.buildTrace()
		builders := []func() benchCacheBytes{
			func() benchCacheBytes { return newMemstashBytes(sc.budget, memstash.PolicyS3FIFO, "memstash-s3fifo") },
			func() benchCacheBytes { return newMemstashBytes(sc.budget, memstash.PolicyClock, "memstash-clock") },
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
