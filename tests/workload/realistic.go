// Package workload generates deterministic, realistic-looking cache workloads - web sessions, CDN assets, and a DB
// row cache with a recurring sequential scan - shared by the root test suite, the standalone benchmarks module, and
// the integration benchmarks. Every trace and value is a pure function of its scenario parameters and the key, so
// the same workload can be regenerated and value-checked independently at any scale, in any of those modules.
package workload

import (
	"fmt"
	"math/rand"
)

// DefaultBlobSize is large enough for NewBlob to back every scenario's largest value (a full-span CDNScenario asset,
// up to 64 KiB) at any offset.
const DefaultBlobSize = 256 * 1024

// NewBlob returns a deterministic pool of printable bytes that scenario Value methods slice their payloads from; the
// same seed and size always yield the same bytes.
func NewBlob(seed int64, size int) []byte {
	rng := rand.New(rand.NewSource(seed))
	b := make([]byte, size)
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+/"
	for i := range b {
		b[i] = alphabet[rng.Intn(len(alphabet))]
	}
	return b
}

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

// --- web session store: Zipf over a token catalogue, ~350-650 byte JSON documents ---

// SessionScenario is a session-cookie lookup cache in front of an auth service: Zipf-popular requests over Catalog
// distinct tokens. ZipfS and Seed default to 1.001 and 11 when zero.
type SessionScenario struct {
	Catalog  uint64 // distinct session tokens
	TraceLen int
	ZipfS    float64
	Seed     int64
}

func (s SessionScenario) withDefaults() SessionScenario {
	if s.ZipfS == 0 {
		s.ZipfS = 1.001
	}
	if s.Seed == 0 {
		s.Seed = Seed()
	}
	return s
}

func sessionKey(id uint64) string { return fmt.Sprintf("sess:%032x", splitmix(id)) }

// Trace builds the Zipf-distributed request sequence.
func (s SessionScenario) Trace() []string {
	s = s.withDefaults()
	rng := rand.New(rand.NewSource(s.Seed))
	zipf := rand.NewZipf(rng, s.ZipfS, 10, s.Catalog-1)
	trace := make([]string, s.TraceLen)
	for i := range trace {
		trace[i] = sessionKey(zipf.Uint64())
	}
	return trace
}

// Value returns the key's deterministic ~350-650 byte JSON session document, sliced out of blob.
func (SessionScenario) Value(blob []byte, key string) []byte {
	h := fnv64(key)
	pad := int(h % 300)
	v := make([]byte, 0, 420+pad)
	v = fmt.Appendf(v,
		`{"sid":%q,"uid":%d,"login":"user%07d","roles":["user","editor"],"created_at":%d,"expires_at":%d,"csrf":"%016x","data":"`,
		key[5:], h%9_000_000, h%9_000_000, 1_752_000_000-h%86_400, 1_752_000_000+3_600, splitmix(h))
	off := int(h % uint64(len(blob)-300))
	v = append(v, blob[off:off+pad]...)
	return append(v, '"', '}')
}

// --- CDN / static assets: mostly-Zipf catalogue + one-hit wonders, bimodal small/large values ---

// CDNScenario is a static-asset cache: Zipf-popular requests over Catalog objects, plus one-hit-wonder requests
// (cache-busting URLs, crawlers) minting a fresh key beyond Catalog every OneHitEvery-th request. Values are bimodal
// like real assets: ~90% small (0.6-8 KiB), ~10% large (8 KiB + up to LargeSpan). ZipfS, Seed, OneHitEvery and
// LargeSpan default to 1.05, 12, 4 and 56*1024 (a 64 KiB large-asset ceiling) when zero.
type CDNScenario struct {
	Catalog     uint64
	TraceLen    int
	ZipfS       float64
	Seed        int64
	OneHitEvery int
	LargeSpan   int
}

func (s CDNScenario) withDefaults() CDNScenario {
	if s.ZipfS == 0 {
		s.ZipfS = 1.05
	}
	if s.Seed == 0 {
		s.Seed = 12
	}
	if s.OneHitEvery == 0 {
		s.OneHitEvery = 4
	}
	if s.LargeSpan == 0 {
		s.LargeSpan = 56 * 1024
	}
	return s
}

func cdnKey(id uint64) string {
	dirs := [...]string{"img", "js", "css", "media"}
	exts := [...]string{"webp", "js", "css", "mp4"}
	d := id % 4
	return fmt.Sprintf("/static/%s/%016x.%s", dirs[d], splitmix(id), exts[d])
}

// Trace builds the request sequence: 1 in OneHitEvery requests mints a fresh one-hit-wonder key, the rest are
// Zipf-popular over Catalog.
func (s CDNScenario) Trace() []string {
	s = s.withDefaults()
	rng := rand.New(rand.NewSource(s.Seed))
	zipf := rand.NewZipf(rng, s.ZipfS, 10, s.Catalog-1)
	trace := make([]string, s.TraceLen)
	oneHit := s.Catalog
	for i := range trace {
		if rng.Intn(s.OneHitEvery) == 0 {
			trace[i] = cdnKey(oneHit)
			oneHit++
		} else {
			trace[i] = cdnKey(zipf.Uint64())
		}
	}
	return trace
}

// Value returns the key's deterministic asset bytes, sliced out of blob.
func (s CDNScenario) Value(blob []byte, key string) []byte {
	s = s.withDefaults()
	h := fnv64(key)
	var size int
	if h%10 == 0 {
		size = 8*1024 + int(h%uint64(s.LargeSpan)) // large asset
	} else {
		size = 600 + int(h%(7*1024)) // small asset
	}
	off := int(h % uint64(len(blob)-8*1024-s.LargeSpan))
	return blob[off : off+size]
}

// --- DB row cache: Zipf point lookups with a recurring sequential scan ---

// DBScenario is a DB-row cache: Zipf point lookups over Rows, interrupted every ChunkSize requests by a full
// sequential scan of the first ScanRows rows (a recurring report job). ZipfS and Seed default to 1.001 and 13 when
// zero.
type DBScenario struct {
	Rows      uint64
	ScanRows  uint64
	ChunkSize int // Zipf lookups between each sequential scan
	TraceLen  int
	ZipfS     float64
	Seed      int64
}

func (s DBScenario) withDefaults() DBScenario {
	if s.ZipfS == 0 {
		s.ZipfS = 1.001
	}
	if s.Seed == 0 {
		s.Seed = 13
	}
	return s
}

func dbKey(id uint64) string { return fmt.Sprintf("user:%08d", id) }

// Trace builds the request sequence: ChunkSize Zipf lookups, then a full scan of ScanRows, repeated until TraceLen
// is reached.
func (s DBScenario) Trace() []string {
	s = s.withDefaults()
	rng := rand.New(rand.NewSource(s.Seed))
	zipf := rand.NewZipf(rng, s.ZipfS, 10, s.Rows-1)
	trace := make([]string, 0, s.TraceLen)
	for len(trace) < s.TraceLen {
		for i := 0; i < s.ChunkSize && len(trace) < s.TraceLen; i++ {
			trace = append(trace, dbKey(zipf.Uint64()))
		}
		for i := uint64(0); i < s.ScanRows && len(trace) < s.TraceLen; i++ {
			trace = append(trace, dbKey(i))
		}
	}
	return trace
}

// Value returns the key's deterministic ~300-380 byte serialized row, sliced out of blob.
func (DBScenario) Value(blob []byte, key string) []byte {
	h := fnv64(key)
	v := make([]byte, 0, 320)
	v = fmt.Appendf(v,
		"id=%s;name=user_%07d;email=user%07d@example.com;plan=%s;balance=%d.%02d;created=%d;flags=%08b;bio=",
		key[5:], h%9_000_000, h%9_000_000, [...]string{"free", "pro", "team"}[h%3], h%100_000, h%100,
		1_600_000_000+h%100_000_000, h%256)
	off := int(h % uint64(len(blob)-160))
	return append(v, blob[off:off+120]...)
}
