package benchmarks

import (
	"context"
	"testing"

	"github.com/zakonnic/memstash"
)

// BenchmarkMemstashGetHitSerial is the single-threaded twin of BenchmarkGetHit for memstash only: it isolates the latency of
// the lock-free memory-hit path without RunParallel scheduling noise.
func BenchmarkMemstashGetHitSerial(b *testing.B) {
	for _, tc := range []struct {
		name   string
		policy memstash.Policy
	}{
		{"memstash-s3fifo", memstash.PolicyS3FIFO},
		{"memstash-clock", memstash.PolicyClock},
	} {
		c, err := memstash.New[uint64, uint64](
			memstash.WithMemoryCapacity(speedCapacity),
			memstash.WithPolicy(tc.policy),
		)
		if err != nil {
			b.Fatal(err)
		}
		ctx := context.Background()
		for key := uint64(0); key < speedHotKeys; key++ {
			_ = c.Set(ctx, key, key)
		}
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				c.GetFromMemory(keySeq[i&seqMask])
			}
		})
		c.Close()
	}
}
