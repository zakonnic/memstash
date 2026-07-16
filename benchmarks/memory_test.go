//go:build long
// +build long

package benchmarks

import (
	"context"
	"runtime"
	"testing"

	"github.com/dustin/go-humanize"
	"github.com/zakonnic/memstash"
)

// Real heap footprint of a full cache, measured from the runtime rather than the built-in TotalWeight estimator:
// GC to a quiescent live set, snapshot HeapAlloc, build and fill, GC again, snapshot again. uint64/uint64 entries
// are pointer-free and keys come from a counter, so the growth between snapshots is the cache and nothing else.

const memstashCapacity = 100_000_000

func BenchmarkMemoryFootprintMemstash(b *testing.B) {
	ctx := context.Background()

	var c *memstash.Cache[uint64, uint64]
	b.Run(humanize.Comma(memstashCapacity)+"-entries", func(b *testing.B) {
		c = measureFootprint(b, memstashCapacity)
	})

	get := func(key uint64) bool { _, ok := c.GetFromMemory(key); return ok }
	set := func(key uint64) { _ = c.Set(ctx, key, key) }

	b.Run("GetFromMemory/hot", func(b *testing.B) { runRead(b, hotWindow(), get) })
	b.Run("GetFromMemory/full", func(b *testing.B) { runRead(b, fullRange(memstashCapacity), get) })
	b.Run("Set/hot", func(b *testing.B) { runWrite(b, hotWindow(), set) })
	b.Run("Set/full", func(b *testing.B) { runWrite(b, fullRange(memstashCapacity), set) })

	c.Close()
}

func measureFootprint(b *testing.B, capacity int64) *memstash.Cache[uint64, uint64] {
	ctx := context.Background()

	// Quiescent baseline: HeapAlloc reflects only the live set, no floating garbage.
	var before, after runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&before)

	c, err := memstash.New[uint64, uint64](
		memstash.WithMemoryCapacity(capacity),
		memstash.WithPolicy(memstash.PolicyClock),
	)
	if err != nil {
		b.Fatal(err)
	}

	for i := int64(0); i < capacity; i++ {
		k := uint64(i) * 0x9E3779B97F4A7C15
		_ = c.Set(ctx, k, k)
	}

	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)

	live := c.Len()
	heapBytes := after.HeapAlloc - before.HeapAlloc
	estimate := c.TotalWeight()
	runtime.KeepAlive(c) // the cache must be alive at the second snapshot, or the GC reclaims what we are measuring

	perEntryHeap := float64(heapBytes) / float64(live)
	b.ReportMetric(perEntryHeap, "B/entry-heap")
	b.ReportMetric(float64(estimate)/float64(live), "B/entry-estimate")
	b.ReportMetric(perEntryHeap/16.0, "x-payload") // overhead relative to the 16-byte raw key+value
	b.ReportMetric(float64(heapBytes)/(1<<20), "MiB-heap-total")
	b.Logf("capacity=%d live=%d heap=%s size-estimate=%s",
		capacity, live, humanize.IBytes(heapBytes), humanize.IBytes(uint64(estimate)))

	return c
}
