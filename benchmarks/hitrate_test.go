package benchmarks

import (
	"math/rand"
	"testing"

	"github.com/dustin/go-humanize"
	"github.com/zakonnic/memstash"
)

// Workloads for comparing hit rate. Every generator is deterministic (the seed is fixed), so each cache receives
// exactly the same stream.
const (
	keyspace = 1_000_000 // cardinality of the key space
	requests = 1_000_000 // length of the request stream
)

// zipfTrace is a classic Zipfian popularity distribution (s close to 1): a small hot core and a long tail.
func zipfTrace() []uint64 {
	rng := rand.New(rand.NewSource(1))
	zipf := rand.NewZipf(rng, 1.001, 10, keyspace-1)
	trace := make([]uint64, requests)
	for i := range trace {
		trace[i] = zipf.Uint64()
	}
	return trace
}

// scanTrace is a Zipfian stream into which a sequential scan of 100k unique keys is injected every 200k requests
// (analytics, migration).
func scanTrace() []uint64 {
	rng := rand.New(rand.NewSource(2))
	zipf := rand.NewZipf(rng, 1.001, 10, keyspace-1)
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
	rng := rand.New(rand.NewSource(3))
	zipf := rand.NewZipf(rng, 1.2, 10, 100_000)
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
		c.Set(key, key, true)
	}
	return 100 * float64(hits) / float64(len(trace))
}

func runHitRateSuite(t *testing.T, withSizeCoef bool) {
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
	capacities := []int64{10_000, 100_000, 500_000} // 1%, 10% and 50% of the key space

	for _, capacity := range capacities {
		if withSizeCoef {
			t.Logf("---- virtual capacity %d items (real depends from lib) ----", capacity)
		} else {
			t.Logf("---- capacity %d items ----", capacity)
		}
		t.Logf("%-18s %12s %12s %12s %12s", "cache", traces[0].name, traces[1].name, traces[2].name, "size estimate")
		builders := []func() benchCache{
			func() benchCache {
				return newMemstash(capacity, memstash.PolicyS3FIFO, "memstash-s3fifo", withSizeCoef)
			},
			func() benchCache { return newMemstash(capacity, memstash.PolicyClock, "memstash-clock", withSizeCoef) },
			func() benchCache { return newRistretto(capacity, withSizeCoef) },
			func() benchCache { return newOtter(capacity, withSizeCoef) },
			func() benchCache { return newTheine(capacity, withSizeCoef) },
			func() benchCache { return newBigcache(capacity, withSizeCoef) },
			func() benchCache { return newFreecache(capacity, 8, 8, withSizeCoef) },
			func() benchCache { return newLRU(capacity, withSizeCoef) },
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
	runHitRateSuite(t, false)
}

// TestHitRateNormalized prints the same comparative hit-rate table, but with each adapter's capacity adjusted via
// GetSizeCoeff first, so the caches are compared at roughly equal memory footprint rather than equal item count.
func TestHitRateNormalized(t *testing.T) {
	runHitRateSuite(t, true)
}
