package benchmarks

import (
	"fmt"
	"math/rand"
	"testing"
)

// BenchmarkThroughput measures aggregate parallel throughput under a Zipfian key distribution at several read/write
// ratios. Modeled on otter's benchmarks/throughput (https://github.com/maypok86/otter/tree/main/benchmarks/throughput).
//
// Run: go -C benchmarks test -run xxx -bench BenchmarkThroughput
func BenchmarkThroughput(b *testing.B) {
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
					b.ReportMetric(float64(b.N)/b.Elapsed().Seconds()/1e6, "Mops/s")
				})
				c.Close()
			}
		})
	}
}
