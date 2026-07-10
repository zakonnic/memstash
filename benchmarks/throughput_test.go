package benchmarks

import (
	"fmt"
	"math/rand"
	"testing"
)

// BenchmarkThroughput is modeled on otter's benchmarks/throughput
// (https://github.com/maypok86/otter/tree/main/benchmarks/throughput): it measures the aggregate parallel throughput
// (ops/s) of purely in-memory caches under a Zipfian key distribution at several read/write ratios. Every contender
// is pre-warmed to capacity and shares the same precomputed key stream (keySeq), so the number reflects the cost of
// the cache operations under contention, not key generation.
//
// All comparison libraries participate: memstash (clock and s3fifo), ristretto, otter (W-TinyLFU), theine (W-TinyLFU),
// hashicorp/lru, freecache, bigcache, and sync.Map as the no-eviction baseline.
//
// Run: go -C benchmarks test -run xxx -bench BenchmarkThroughput
func BenchmarkThroughput(b *testing.B) {
	// readPercent = share of Get operations; the rest are Set. 100 is read-only, 0 is write-only.
	for _, readPercent := range []int{100, 75, 50, 25, 0} {
		b.Run(fmt.Sprintf("reads=%d%%", readPercent), func(b *testing.B) {
			for _, c := range speedContenders() {
				warmUp(c)
				b.Run(c.Name(), func(b *testing.B) {
					b.RunParallel(func(pb *testing.PB) {
						i := rand.Int()
						for pb.Next() {
							key := keySeq[i&seqMask]
							if i%100 < readPercent {
								c.Get(key)
							} else {
								c.Set(key, key, false)
							}
							i++
						}
					})
					// Report throughput the way otter's benchmark does, in addition to the default ns/op.
					b.ReportMetric(float64(b.N)/b.Elapsed().Seconds()/1e6, "Mops/s")
				})
				c.Close()
			}
		})
	}
}
