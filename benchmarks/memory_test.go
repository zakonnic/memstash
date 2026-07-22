//go:build long
// +build long

package benchmarks

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
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

	b.Run("GetFromMemory/hot", func(b *testing.B) { runRead(b, hotWindow(memstashCapacity), get) })
	b.Run("GetFromMemory/full", func(b *testing.B) { runRead(b, fullRange(memstashCapacity), get) })
	b.Run("BatchGetFromMemory/full", func(b *testing.B) { runBatchRead(b, fullRange(memstashCapacity), c) })
	b.Run("Set/hot", func(b *testing.B) { runWrite(b, hotWindow(memstashCapacity), set) })
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

// runBatchRead is runRead over BatchGetFromMemory: the same key stream drained in reused fixed-size batches. Its gap
// against GetFromMemory/full is what pipelining the dependent misses buys.
func runBatchRead(b *testing.B, src keySource, c *memstash.Cache[uint64, uint64]) {
	const batchLen = 256
	workers := runtime.GOMAXPROCS(0)
	perWorker := latencyIters / workers / batchLen * batchLen
	ops := perWorker * workers

	var hitCount atomic.Int64
	var wg sync.WaitGroup
	b.ResetTimer()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			next := src(seed)
			keys := make([]uint64, batchLen)
			dst := make(memstash.List[uint64, uint64], 0, batchLen)
			local := int64(0)
			for n := 0; n < perWorker; n += batchLen {
				for j := range keys {
					keys[j] = next()
				}
				dst = c.BatchGetFromMemory(keys, dst[:0])
				local += int64(len(dst))
			}
			hitCount.Add(local)
		}(uint64(w+1) * 0x9E3779B97F4A7C15)
	}
	wg.Wait()
	b.StopTimer()

	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(ops), "ns/op")
	b.ReportMetric(float64(ops)/b.Elapsed().Seconds()/1e6, "Mops/s")
	b.ReportMetric(100*float64(hitCount.Load())/float64(ops), "hit%")
}
