package benchmarks

import (
	"context"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/tests/workload"
)

// Speed benchmarks of in-memory operations. The key sequence (Zipf over a hot set) is precomputed so the RNG does not
// distort the measurement.
const (
	speedCapacity = 1 << 17
	seqHitMax     = speedCapacity * 3 / 4 // lower than capacity to make 100% hits
	seqLen        = 1 << 20
	seqMask       = seqLen - 1
)

func zipfSeq(len, max uint64) []uint64 {
	rng := workload.Random()
	zipf := rand.NewZipf(rng, 1.1, 10, max)
	seq := make([]uint64, len)
	for i := range seq {
		seq[i] = zipf.Uint64()
	}
	return seq
}

func speedContenders() []benchCache {
	return []benchCache{
		newMemstash(speedCapacity, memstash.PolicyS3FIFO, "memstash-s3fifo"),
		newMemstash(speedCapacity, memstash.PolicyClock, "memstash-clock"),
		newMemstash(speedCapacity, memstash.PolicyWTinyLFU, "memstash-wtinylfu"),
		newMemstash(speedCapacity, memstash.PolicySIEVE, "memstash-sieve"),
		newRistretto(speedCapacity),
		newOtter(speedCapacity),
		newTheine(speedCapacity),
		newBigcache(speedCapacity),
		newFreecache(speedCapacity, 8, 8),
		newLRU(speedCapacity),
		newSyncMap(),
	}
}

func warmUp(c benchCache, keyCount uint64) {
	for key := uint64(0); key < keyCount; key++ {
		c.Set(key, key)
	}
	c.Settle() // Must finish warm-up writes.
}

var sinkU64 atomic.Uint64

// BenchmarkGetHit measures reads at 100% hits (the main hot path).
func BenchmarkGetHit(b *testing.B) {
	// seqLen keys from 0 to seqHitMax-1, with speedCapacity cache size
	keys := zipfSeq(seqLen, seqHitMax-1) // seqHitMax is less than capacity
	for _, c := range speedContenders() {
		warmUp(c, seqHitMax)
		b.Run(c.Name(), func(b *testing.B) {
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				i := rand.Int()
				local := uint64(0)
				for pb.Next() {
					v, _ := c.Get(keys[i&seqMask])
					local += v
					i++
				}
				sinkU64.Store(local)
			})
		})
		c.Close()
	}
}

func BenchmarkGet(b *testing.B) {
	keys := zipfSeq(seqLen, seqHitMax-1)
	for _, c := range speedContenders() {
		warmUp(c, seqHitMax)
		b.Run(c.Name(), func(b *testing.B) {
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				i := rand.Int()
				local := uint64(0)
				for pb.Next() {
					key := keys[i&seqMask]
					if i%10 < 5 {
						key += seqHitMax // hitrate 50%
					}
					v, _ := c.Get(key)
					local += v
					i++
				}
				sinkU64.Store(local)
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
				local := uint64(0)
				for pb.Next() {
					key := (i * 0x9E3779B97F4A7C15) % setSpace
					c.Set(key, key)
					i++
				}
				sinkU64.Add(local)
			})
		})
		c.Close()
	}
}

func BenchmarkShortCheckup(b *testing.B) {
	ctx := context.Background()

	const nKeys = 1 << 16
	const keyMask = nKeys - 1
	var sink atomic.Int64
	strKeys := make([]string, nKeys)
	for i := range strKeys {
		strKeys[i] = "long-key:" + strconv.Itoa(i)
	}

	cStr, _ := memstash.New[string, int]()
	mStr := sync.Map{}
	for i, k := range strKeys {
		_ = cStr.Set(ctx, k, i)
		mStr.Store(k, i)
	}
	defer cStr.Close()

	cUuid, _ := memstash.New[uuid.UUID, int]()
	uids := make([]uuid.UUID, nKeys)
	mUuid := sync.Map{}
	for i := range nKeys {
		uids[i] = uuid.Must(uuid.NewRandom())
		_ = cUuid.Set(ctx, uids[i], i)
		mUuid.Store(uids[i], i)
	}
	defer cUuid.Close()

	b.Run("memstash-string", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			i := rand.Int()
			var local int
			for pb.Next() {
				v, _, _ := cStr.Get(ctx, strKeys[i&keyMask])
				local += v
				i++
			}
			sink.Add(int64(local))
		})
	})

	b.Run("memstash-uuid", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			i := rand.Int()
			var local int
			for pb.Next() {
				v, _, _ := cUuid.Get(ctx, uids[i&keyMask])
				local += v
				i++
			}
			sink.Add(int64(local))
		})
	})

	b.Run("sync.Map-string", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			i := rand.Int()
			var local int
			for pb.Next() {
				v, _ := mStr.Load(strKeys[i&keyMask])
				local += v.(int)
				i++
			}
			sink.Add(int64(local))
		})
	})

	b.Run("sync.Map-uuid", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			i := rand.Int()
			var local int
			for pb.Next() {
				v, _ := mUuid.Load(uids[i&keyMask])
				local += v.(int)
				i++
			}
			sink.Add(int64(local))
		})
	})

	b.Logf("output: %d", sink.Load())
}

// BenchmarkMixed90_10 is the realistic mix: 90% reads, 10% writes.
func BenchmarkMixed90_10(b *testing.B) {
	keys := zipfSeq(seqLen, seqHitMax-1)
	for _, c := range speedContenders() {
		warmUp(c, seqHitMax)
		b.Run(c.Name(), func(b *testing.B) {
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				i := rand.Int()
				for pb.Next() {
					key := keys[i&seqMask]
					if i%10 == 0 {
						c.Set(key, key)
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
