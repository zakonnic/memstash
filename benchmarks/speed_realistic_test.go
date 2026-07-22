package benchmarks

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/tests/workload"
)

// Speed under a realistic random load, the honest counterpart of speed_test.go. That file's Zipf s=1.1 concentrates
// almost every access on a few dozen keys, so whatever the cache capacity, the measured lines live in L1/L2 and the
// numbers read sub-nanosecond. Here the workload is shaped like a high-load service instead: a near-flat Zipf
// (s=1.001) over a 10M-row catalog with the periodic report scan. The trace touches ~950k distinct rows, ~320 MiB of
// payload, against a 192 MiB budget: several times L3, so most hits pay real memory latency, and the budget is well
// under the working set, so eviction runs and misses stay in the stream (hit% reports their share).
//
// DBScenario is the smallest-entry scenario of the three (13-byte keys, ~300-380 byte rows, vs ~537 and ~7.3 KiB for
// sessions and CDN assets). Speed here is meant to be the cache's own cost, so the payload is kept as small as the
// realistic set allows: less of each op goes into copying and hashing bytes, more into lookup, eviction and
// contention.
const (
	randomBudget    = 192 << 20
	randomRows      = 10_000_000
	randomScanRows  = 200_000
	randomChunkSize = 500_000
	randomTraceLen  = 1 << 22 // power of two: workers walk it with a mask
	randomAvgEntry  = 13 + 340 + 48

	// randomValsLen bounds the precomputed value pool: value bytes are subslices of contentBlob, so the pool costs
	// slice headers only. Precomputing keeps value construction off the measured Set path.
	randomValsLen = 1 << 20
)

var randomTrace = sync.OnceValue(func() []string {
	return workload.DBScenario{
		Rows: randomRows, ScanRows: randomScanRows, ChunkSize: randomChunkSize, TraceLen: randomTraceLen,
	}.Trace()
})

// randomVals reproduces DBScenario.Value's size distribution, not its bytes: only the length reaches the cache, and
// slicing the blob keeps the pool free of ~360 MiB of generated rows.
var randomVals = sync.OnceValue(func() [][]byte {
	trace := randomTrace()
	vals := make([][]byte, randomValsLen)
	for i := range vals {
		h := fnvHash(trace[i])
		size := 300 + int(h%80)
		off := int(h % uint64(len(contentBlob)-380))
		vals[i] = contentBlob[off : off+size]
	}
	return vals
})

// fnvHash mirrors the workload package's per-key hash (unexported there) for deriving stable value sizes.
func fnvHash(s string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

func randomContenders() []benchCacheBytes {
	return []benchCacheBytes{
		newMemstashBytes(randomBudget, memstash.PolicyS3FIFO, "memstash-s3fifo"),
		newMemstashBytes(randomBudget, memstash.PolicyClock, "memstash-clock"),
		newMemstashBytes(randomBudget, memstash.PolicyWTinyLFU, "memstash-wtinylfu"),
		newMemstashBytes(randomBudget, memstash.PolicySIEVE, "memstash-sieve"),
		newRistrettoBytes(randomBudget, randomAvgEntry),
		newOtterBytes(randomBudget),
		newTheineBytes(randomBudget),
		newBigcacheBytes(randomBudget, randomAvgEntry),
		newFreecacheBytes(randomBudget),
		newLRUBytes(randomBudget, randomAvgEntry),
	}
}

// warmRandom replays the trace once, setting every miss, so the cache reaches its steady working set before the
// timer starts.
func warmRandom(c benchCacheBytes) {
	trace, vals := randomTrace(), randomVals()
	for i, key := range trace {
		if _, ok := c.Get(key); !ok {
			c.Set(key, vals[i&(randomValsLen-1)], false)
		}
	}
}

// BenchmarkRandomGet measures reads over the realistic stream. hit% is part of the result: a fast contender that
// holds less of the working set is answering cheaper questions.
func BenchmarkRandomGet(b *testing.B) {
	trace := randomTrace()
	mask := len(trace) - 1
	for _, c := range randomContenders() {
		b.Run(c.Name(), func(b *testing.B) {
			warmRandom(c)
			var ops, hits atomic.Int64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := rand.Int()
				localOps, localHits := int64(0), int64(0)
				for pb.Next() {
					if _, ok := c.Get(trace[i&mask]); ok {
						localHits++
					}
					i++
					localOps++
				}
				ops.Add(localOps)
				hits.Add(localHits)
			})
			b.StopTimer()
			b.ReportMetric(100*float64(hits.Load())/float64(ops.Load()), "hit%")
		})
		c.Close()
	}
}

// BenchmarkRandomSet measures writes over the realistic stream: mostly overwrites of resident keys, with eviction
// running (the budget holds only part of the catalog).
func BenchmarkRandomSet(b *testing.B) {
	trace, vals := randomTrace(), randomVals()
	mask := len(trace) - 1
	for _, c := range randomContenders() {
		b.Run(c.Name(), func(b *testing.B) {
			warmRandom(c)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := rand.Int()
				for pb.Next() {
					c.Set(trace[i&mask], vals[i&(randomValsLen-1)], false)
					i++
				}
			})
		})
		c.Close()
	}
}

// BenchmarkRandomMixed90_10 is the realistic service mix: 90% reads, 10% writes, same stream.
func BenchmarkRandomMixed90_10(b *testing.B) {
	trace, vals := randomTrace(), randomVals()
	mask := len(trace) - 1
	for _, c := range randomContenders() {
		b.Run(c.Name(), func(b *testing.B) {
			warmRandom(c)
			var ops, hits atomic.Int64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := rand.Int()
				localOps, localHits := int64(0), int64(0)
				for pb.Next() {
					key := trace[i&mask]
					if i%10 == 0 {
						c.Set(key, vals[i&(randomValsLen-1)], false)
					} else {
						if _, ok := c.Get(key); ok {
							localHits++
						}
						localOps++
					}
					i++
				}
				ops.Add(localOps)
				hits.Add(localHits)
			})
			b.StopTimer()
			b.ReportMetric(100*float64(hits.Load())/float64(ops.Load()), "hit%")
		})
		c.Close()
	}
}
