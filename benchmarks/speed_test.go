package benchmarks

import (
	"math/rand"
	"testing"
)

// Speed benchmarks of in-memory operations. The key sequence (Zipf over a hot set) is precomputed so the random number
// generator does not distort the measurement. sync.Map and xsync.MapOf are the no-eviction baselines.
const (
	speedCapacity = 1 << 17 // cache capacity
	speedHotKeys  = 1 << 16 // warmed-up keys (they fit entirely within the capacity)
	seqLen        = 1 << 20
	seqMask       = seqLen - 1
)

var keySeq = func() []uint64 {
	rng := rand.New(rand.NewSource(42))
	zipf := rand.NewZipf(rng, 1.1, 10, speedHotKeys-1)
	seq := make([]uint64, seqLen)
	for i := range seq {
		seq[i] = zipf.Uint64()
	}
	return seq
}()

func speedContenders() []benchCache {
	return []benchCache{
		newMemstash(speedCapacity, 0, "memstash-s3fifo"),
		newMemstash(speedCapacity, 1, "memstash-clock"),
		newRistretto(speedCapacity),
		newOtter(speedCapacity),
		newTheine(speedCapacity),
		newLRU(speedCapacity),
		newSyncMap(),
	}
}

func warmUp(c benchCache) {
	for key := uint64(0); key < speedHotKeys; key++ {
		c.Set(key, key, true)
	}
}

// BenchmarkGetHit measures reads at 100% hits (the main hot path).
func BenchmarkGetHit(b *testing.B) {
	for _, c := range speedContenders() {
		warmUp(c)
		b.Run(c.Name(), func(b *testing.B) {
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				i := rand.Int()
				for pb.Next() {
					c.Get(keySeq[i&seqMask])
					i++
				}
			})
		})
		c.Close()
	}
}

// BenchmarkSet writes into a key space twice the capacity: eviction runs constantly in the evicting caches.
func BenchmarkSet(b *testing.B) {
	const setSpace = speedCapacity * 2
	for _, c := range speedContenders() {
		b.Run(c.Name(), func(b *testing.B) {
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				i := uint64(rand.Int63())
				for pb.Next() {
					key := (i * 0x9E3779B97F4A7C15) % setSpace
					c.Set(key, key, false)
					i++
				}
			})
		})
		c.Close()
	}
}

// BenchmarkMixed90_10 is a realistic mix: 90% reads, 10% writes.
func BenchmarkMixed90_10(b *testing.B) {
	for _, c := range speedContenders() {
		warmUp(c)
		b.Run(c.Name(), func(b *testing.B) {
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				i := rand.Int()
				for pb.Next() {
					key := keySeq[i&seqMask]
					if i%10 == 0 {
						c.Set(key, key, false)
					} else {
						c.Get(key)
					}
					i++
				}
			})
		})
		c.Close()
	}
}
