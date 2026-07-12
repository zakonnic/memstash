package tests

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
)

// Value-integrity check for some realistic scenarios (copied from TestHitRateRealistic benchmark).

// realisticBlob is a shared pool of deterministic printable bytes; values are sliced/copied out of it.
var realisticBlob = func() []byte {
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

// --- web session store: Zipf over 3M session tokens, ~350-650 byte JSON documents ---

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
	off := int(h % uint64(len(realisticBlob)-300))
	v = append(v, realisticBlob[off:off+pad]...)
	return append(v, '"', '}')
}

// --- CDN / static assets: 75% Zipf over a 1M catalogue + 25% one-hit wonders, bimodal 0.6-64 KB values ---

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
	off := int(h % uint64(len(realisticBlob)-64*1024))
	return realisticBlob[off : off+size]
}

// --- DB row cache: Zipf point lookups over 2M rows with a recurring 200k-row sequential scan ---

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
	off := int(h % uint64(len(realisticBlob)-160))
	return append(v, realisticBlob[off:off+120]...)
}

// TestRealisticTraceValues replays every scenario and verifies each hit byte-for-byte against the key's deterministic
// value. It also checks the weight invariant: the live weight never settles above the configured byte budget.
func TestRealisticTraceValues(t *testing.T) {
	if testing.Short() {
		t.Skip("long trace replay")
	}
	scenarios := []struct {
		name       string
		buildTrace func() []string
		value      func(key string) []byte
		budget     int64
	}{
		{name: "web-sessions", buildTrace: sessionTrace, value: sessionValue, budget: 64 << 20},
		{name: "cdn-assets", buildTrace: cdnTrace, value: cdnValue, budget: 256 << 20},
		{name: "db-rows", buildTrace: dbTrace, value: dbValue, budget: 48 << 20},
	}

	for _, sc := range scenarios {
		trace := sc.buildTrace()
		for _, tc := range policies {
			t.Run(sc.name+"/"+tc.name, func(t *testing.T) {
				c, err := memstash.New[string, []byte](
					memstash.WithMemoryCapacity(sc.budget),
					memstash.WithPolicy(tc.policy),
					memstash.WithCostFunc(func(key string, value []byte) uint32 { return uint32(len(key) + len(value)) }),
				)
				require.NoError(t, err)
				defer c.Close()

				ctx := context.Background()
				hits := 0
				for i, key := range trace {
					got, ok := c.GetFromMemory(key)
					if !ok {
						require.NoError(t, c.Set(ctx, key, sc.value(key)))
						continue
					}
					hits++
					if want := sc.value(key); !bytes.Equal(got, want) {
						t.Fatalf("request %d: hit on key %q returned a wrong value (got %d bytes, want %d bytes)",
							i, key, len(got), len(want))
					}
				}

				// The traces are built so a working cache serves roughly half the requests from memory; a hit count
				// this low means the replay did not really exercise the eviction path.
				require.Greater(t, hits, len(trace)/4, "suspiciously few hits: the trace no longer stresses the cache")
				assert.LessOrEqual(t, c.Weight(), sc.budget, "live weight exceeds the byte budget")
			})
		}
	}
}
