package benchmarks

import (
	"context"
	"testing"

	"github.com/zakonnic/memstash"
)

// BenchmarkMemstashGetHitSerial is the single-threaded twin of BenchmarkGetHit: the lock-free hit path without
// RunParallel scheduling noise.
func BenchmarkMemstashGetHitSerial(b *testing.B) {
	keys := zipfSeq(seqLen, seqHitMax-1)
	for _, tc := range []struct {
		name   string
		policy memstash.Policy
	}{
		{"memstash-s3fifo", memstash.PolicyS3FIFO},
		{"memstash-clock", memstash.PolicyClock},
		{"memstash-wtinylfu", memstash.PolicyWTinyLFU},
		{"memstash-sieve", memstash.PolicySIEVE},
	} {
		c, err := memstash.New[uint64, uint64](
			memstash.WithMemoryCapacity(speedCapacity),
			memstash.WithPolicy(tc.policy),
		)
		if err != nil {
			b.Fatal(err)
		}
		ctx := context.Background()
		for key := uint64(0); key < seqHitMax; key++ {
			_ = c.Set(ctx, key, key)
		}
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				c.GetFromMemory(keys[i&seqMask])
			}
		})
		c.Close()
	}
}

// BenchmarkMemstashSetHitSerial overwrites keys the cache holds, single-threaded: the in-place write path without
// lock contention.
func BenchmarkMemstashSetHitSerial(b *testing.B) {
	keys := zipfSeq(seqLen, seqHitMax-1)
	c, err := memstash.New[uint64, uint64](memstash.WithMemoryCapacity(speedCapacity))
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	for key := uint64(0); key < seqHitMax; key++ {
		_ = c.Set(ctx, key, key)
	}
	b.Run("memstash-s3fifo", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = c.Set(ctx, keys[i&seqMask], uint64(i))
		}
	})
}

var serialPolicies = []struct {
	name   string
	policy memstash.Policy
}{
	{"memstash-s3fifo", memstash.PolicyS3FIFO},
	{"memstash-clock", memstash.PolicyClock},
	{"memstash-wtinylfu", memstash.PolicyWTinyLFU},
	{"memstash-sieve", memstash.PolicySIEVE},
}

// serialCache is a preallocated cache, like the speed contenders, holding the keys below warm.
func serialCache(b *testing.B, policy memstash.Policy, warm uint64) *memstash.Cache[uint64, uint64] {
	c, err := memstash.New[uint64, uint64](
		memstash.WithMemoryCapacity(speedCapacity),
		memstash.WithPolicy(policy),
		memstash.WithPreallocatedSize(),
	)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	for key := uint64(0); key < warm; key++ {
		_ = c.Set(ctx, key, key)
	}
	return c
}

// BenchmarkMemstashGetMissSerial is the single-threaded twin of BenchmarkGetMiss: most reads miss and walk probe
// chains that a long eviction churn has shaped.
func BenchmarkMemstashGetMissSerial(b *testing.B) {
	const keySpace = speedCapacity * 8
	keys := zipfSeq(seqLen, keySpace-1)
	for _, tc := range serialPolicies {
		c := serialCache(b, tc.policy, keySpace)
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				c.GetFromMemory(keys[i&seqMask])
			}
		})
		c.Close()
	}
}

// BenchmarkMemstashSetEvictSerial writes keys a full cache has never held, single-threaded: every Set claims a slot
// and evicts a victim. BenchmarkSet has this path under lock contention, mixed with overwrites.
func BenchmarkMemstashSetEvictSerial(b *testing.B) {
	ctx := context.Background()
	for _, tc := range serialPolicies {
		c := serialCache(b, tc.policy, speedCapacity)
		next := uint64(speedCapacity) // outlives the reruns b.Run makes while it sizes b.N: no key comes twice
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = c.Set(ctx, next, next)
				next++
			}
		})
		c.Close()
	}
}

// BenchmarkMemstashDeleteSerial is the single-threaded twin of BenchmarkDelete: the cost of a Delete that finds its
// key plus the Set that puts the key back, without lock contention.
func BenchmarkMemstashDeleteSerial(b *testing.B) {
	keys := zipfSeq(seqLen, seqHitMax-1)
	ctx := context.Background()
	for _, tc := range serialPolicies {
		c := serialCache(b, tc.policy, seqHitMax)
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				key := keys[i&seqMask]
				_ = c.Delete(ctx, key)
				_ = c.Set(ctx, key, key)
			}
		})
		c.Close()
	}
}

// BenchmarkMemstashFillSerial fills caches that were not preallocated, single-threaded: every Set is an insert, and
// the tables grow under it from their first slots. The cost of growth is spread over the keys.
func BenchmarkMemstashFillSerial(b *testing.B) {
	const fillKeys = 1 << 20
	ctx := context.Background()
	for _, tc := range serialPolicies {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			var c *memstash.Cache[uint64, uint64]
			for i := 0; i < b.N; i++ {
				key := uint64(i % fillKeys)
				if key == 0 { // the previous cache is full: start an empty one
					if c != nil {
						c.Close()
					}
					var err error
					c, err = memstash.New[uint64, uint64](
						memstash.WithMemoryCapacity(fillKeys),
						memstash.WithPolicy(tc.policy),
					)
					if err != nil {
						b.Fatal(err)
					}
				}
				_ = c.Set(ctx, key, key)
			}
			c.Close()
		})
	}
}
