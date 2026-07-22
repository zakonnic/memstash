package benchmarks

import (
	"math/rand"
	"testing"

	"github.com/dustin/go-humanize"
	"github.com/zakonnic/memstash"
)

// Workloads for comparing hit rate. Seeded fixed, so the table is reproducible and every cache sees the same stream.
const (
	keyspace  = 4_000_000
	requests  = 4_000_000
	traceSeed = 0x5eed

	zipfS = 1.005
	zipfV = keyspace / 1000
)

func zipfStream(rng *rand.Rand) *rand.Zipf { return rand.NewZipf(rng, zipfS, zipfV, keyspace-1) }

// zipfTrace is a stationary Zipfian popularity distribution: a broad hot head and a long cold tail.
func zipfTrace() []uint64 {
	rng := rand.New(rand.NewSource(traceSeed))
	zipf := zipfStream(rng)
	trace := make([]uint64, requests)
	for i := range trace {
		trace[i] = zipf.Uint64()
	}
	return trace
}

// scanTrace injects a sequential scan of 100k unique keys into a Zipfian stream every 200k requests (analytics,
// migration).
func scanTrace() []uint64 {
	rng := rand.New(rand.NewSource(traceSeed))
	zipf := zipfStream(rng)
	trace := make([]uint64, 0, requests)
	scanKey := uint64(10_000_000)
	for len(trace) < requests {
		for i := 0; i < 200_000 && len(trace) < requests; i++ {
			trace = append(trace, zipf.Uint64())
		}
		for i := 0; i < 100_000 && len(trace) < requests; i++ {
			trace = append(trace, scanKey)
			scanKey++
		}
	}
	return trace
}

// oneHitTrace is 70% Zipf over a hot core plus 30% practically unique keys (one-hit wonders): a CDN/web-cache profile.
func oneHitTrace() []uint64 {
	rng := rand.New(rand.NewSource(traceSeed))
	zipf := zipfStream(rng)
	trace := make([]uint64, requests)
	unique := uint64(20_000_000)
	for i := range trace {
		if rng.Intn(10) < 3 {
			trace[i] = unique
			unique++
		} else {
			trace[i] = zipf.Uint64()
		}
	}
	return trace
}

// runTrace replays the stream through the cache: a miss is counted and triggers a Set.
func runTrace(c benchCache, trace []uint64) float64 {
	hits := 0
	for _, key := range trace {
		if _, ok := c.Get(key); ok {
			hits++
			continue
		}
		c.Set(key, key)
	}
	c.Settle()
	return 100 * float64(hits) / float64(len(trace))
}

func distinctKeys(trace []uint64) int {
	seen := make(map[uint64]struct{}, len(trace)/4)
	for _, k := range trace {
		seen[k] = struct{}{}
	}
	return len(seen)
}

func runHitRateSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("long comparative run")
	}
	traces := []struct {
		name  string
		trace []uint64
	}{
		{"zipf", zipfTrace()},
		{"zipf+scan", scanTrace()},
		{"one-hit-30%", oneHitTrace()},
	}
	working := distinctKeys(traces[0].trace)
	t.Logf("zipf trace: %d requests over %d distinct keys", requests, working)
	capacities := []int64{10_000, 100_000, 500_000} // ~1%, ~9% and ~44% of the zipf working set

	for _, capacity := range capacities {
		t.Logf("---- capacity %d items (~%.0f%% of working set) ----", capacity, 100*float64(capacity)/float64(working))
		t.Logf("%-18s %12s %12s %12s %12s", "cache", traces[0].name, traces[1].name, traces[2].name, "size estimate")
		builders := []func() benchCache{
			func() benchCache { return newMemstash(capacity, memstash.PolicyS3FIFO, "memstash-s3fifo") },
			func() benchCache { return newMemstash(capacity, memstash.PolicyClock, "memstash-clock") },
			func() benchCache { return newMemstash(capacity, memstash.PolicyWTinyLFU, "memstash-wtinylfu") },
			func() benchCache { return newMemstash(capacity, memstash.PolicySIEVE, "memstash-sieve") },
			func() benchCache { return newRistretto(capacity) },
			func() benchCache { return newOtter(capacity) },
			func() benchCache { return newTheine(capacity) },
			func() benchCache { return newBigcache(capacity) },
			func() benchCache { return newFreecache(capacity, 8, 8) },
			func() benchCache { return newLRU(capacity) },
		}
		for _, build := range builders {
			var name string
			var sizeBytes uint64
			row := make([]float64, len(traces))
			for i, traceCase := range traces {
				cache := build()
				name = cache.Name()
				row[i] = runTrace(cache, traceCase.trace)
				if i == len(traces)-1 {
					sizeBytes = cache.GetSize()
				}
				cache.Close()
			}

			t.Logf("%-18s %11.2f%% %11.2f%% %11.2f%% %12s", name, row[0], row[1], row[2], humanize.Bytes(sizeBytes))
		}
	}
}

// TestHitRate prints a comparative hit-rate table at equal nominal capacity (item count) per cache.
func TestHitRate(t *testing.T) {
	runHitRateSuite(t)
}
