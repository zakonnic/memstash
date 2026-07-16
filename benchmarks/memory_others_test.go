//go:build long || others
// +build long others

package benchmarks

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	theine "github.com/Yiling-J/theine-go"
	"github.com/allegro/bigcache/v3"
	"github.com/coocood/freecache"
	"github.com/dgraph-io/ristretto/v2"
	"github.com/dustin/go-humanize"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/maypok86/otter/v2"
	"github.com/puzpuzpuz/xsync/v3"
)

// Shared harness for the BenchmarkMemoryFootprint family, plus the non-memstash contenders.
const (
	// At 100M pointer-free entries a contender costs several GiB resident.
	bigBenchCapacity = 100_000_000

	// benchWorkingSet is the key range the hot half of each latency pair touches: small enough to stay CPU-cache
	// friendly. Must be a power of two (hotWindow masks with it).
	benchWorkingSet = 1 << 20

	// latencyIters is the fixed op count of the latency sub-benchmarks (they ignore b.N, like the footprint pass).
	latencyIters = 10_000_000

	// payloadBytes is the raw uint64 key + uint64 value, the baseline the per-entry overhead is expressed against.
	payloadBytes = 16
)

// The BenchmarkMemoryFootprint contenders, measured the same way memstash is (see memory_test.go). Each is a separate
// top-level benchmark because a single 100M-entry run costs several GiB - run them one at a time.

func BenchmarkMemoryFootprintOtter(b *testing.B) {
	runFootprint(b, footprintCase{
		build: func() benchCache { return newOtter(bigBenchCapacity) },
		live: func(c benchCache) int {
			return c.Expose().(*otter.Cache[uint64, uint64]).EstimatedSize()
		},
		// heap figure is the only measurement available
	})
}

func BenchmarkMemoryFootprintBigcache(b *testing.B) {
	runFootprint(b, footprintCase{
		build: func() benchCache { return newBigcache(bigBenchCapacity) },
		live: func(c benchCache) int {
			return c.Expose().(*bigcache.BigCache).Len()
		},
		// Capacity() counts only the ring buffers and misses the hash index, but it is native and cheap.
		sizeEstimate: func(c benchCache) uint64 {
			return uint64(c.Expose().(*bigcache.BigCache).Capacity())
		},
	})
}

// BenchmarkMemoryFootprintXsyncMap measures xsync.MapOf. It performs no eviction, so it holds the full fill and serves
// as the lower-bound baseline: what a bare concurrent map costs per entry before any cache bookkeeping is added.
func BenchmarkMemoryFootprintXsyncMap(b *testing.B) {
	runFootprint(b, footprintCase{
		build: func() benchCache { return newXsyncMap() },
		live: func(c benchCache) int {
			return c.Expose().(*xsync.MapOf[uint64, uint64]).Size()
		},
		// Stats() gives exact bucket and entry counts.
		sizeEstimate: func(c benchCache) uint64 {
			return xsyncMapOfBytes(c.Expose().(*xsync.MapOf[uint64, uint64]))
		},
	})
}

// BenchmarkMemoryFootprintRistretto measures ristretto. It is cost-bounded rather than item-bounded, but every Set
// here costs 1, so MaxCost-RemainingCost is the live count. Its Set is asynchronous and drops under pressure, so live
// falls short of the nominal capacity.
func BenchmarkMemoryFootprintRistretto(b *testing.B) {
	runFootprint(b, footprintCase{
		build: func() benchCache { return asyncFill{newRistretto(bigBenchCapacity)} },
		live: func(c benchCache) int {
			rc := c.Expose().(*ristretto.Cache[uint64, uint64])
			rc.Wait()
			return int(rc.MaxCost() - rc.RemainingCost())
		},
		// No sizeEstimate: GetSize()'s SizeOf would walk every entry of the store map - minutes of reflection plus a
		// visited map larger than the cache.
	})
}

// asyncFill drops the per-Set Wait of a contender whose adapter honours the sync flag: waiting 100M times never
// finishes. Whatever the buffer still holds is drained by the live count before it is read.
type asyncFill struct{ benchCache }

func (c asyncFill) Set(key, value uint64, _ bool) { c.benchCache.Set(key, value, false) }

// BenchmarkMemoryFootprintTheine measures theine (W-TinyLFU). Its Set is asynchronous too, hence the Wait before Len.
func BenchmarkMemoryFootprintTheine(b *testing.B) {
	runFootprint(b, footprintCase{
		build: func() benchCache { return newTheine(bigBenchCapacity) },
		live: func(c benchCache) int {
			tc := c.Expose().(*theine.Cache[uint64, uint64])
			tc.Wait()
			return tc.Len()
		},
		// No sizeEstimate, same as ristretto: SizeOf would walk 100M map entries.
	})
}

// BenchmarkMemoryFootprintFreecache measures freecache. Like bigcache it is bounded by bytes, not item count, so live
// falls short of the nominal capacity and the per-entry math uses the actual live count.
func BenchmarkMemoryFootprintFreecache(b *testing.B) {
	runFootprint(b, footprintCase{
		build: func() benchCache { return newFreecache(bigBenchCapacity, 8, 8) },
		live: func(c benchCache) int {
			return int(c.Expose().(*freecache.Cache).EntryCount())
		},
		// The one contender whose SizeOf is usable here: entries are raw bytes inside per-segment ring buffers, so it
		// sizes the slices and never walks an entry.
		sizeEstimate: func(c benchCache) uint64 {
			return SizeOf(c.Expose().(*freecache.Cache))
		},
	})
}

// BenchmarkMemoryFootprintLRU measures hashicorp/golang-lru: one map plus one container/list behind a mutex, the
// textbook layout the others are trying to beat.
func BenchmarkMemoryFootprintLRU(b *testing.B) {
	runFootprint(b, footprintCase{
		build: func() benchCache { return newLRU(bigBenchCapacity) },
		live: func(c benchCache) int {
			return c.Expose().(*lru.Cache[uint64, uint64]).Len()
		},
		// No sizeEstimate: SizeOf would walk the map and every list element.
	})
}

// fillKey maps a dense index to a distinct, well-spread uint64 (multiplying by an odd constant is a bijection over
// uint64), so keys reach every shard without materializing a key slice.
func fillKey(i int) uint64 { return uint64(i) * 0x9E3779B97F4A7C15 }

// keySource makes one worker's key generator. Every worker gets its own, so no state is shared across cores.
type keySource func(seed uint64) func() uint64

// hotWindow confines lookups to benchWorkingSet keys, which stay resident in the CPU caches. Workers start at
// independent offsets so they do not march in lockstep over the same line.
func hotWindow() keySource {
	return func(seed uint64) func() uint64 {
		i := seed
		return func() uint64 {
			i++
			return fillKey(int(i & (benchWorkingSet - 1)))
		}
	}
}

// fullRange draws uniformly over the whole fill, so nearly every op is a random probe into a multi-GiB heap - an LLC
// miss and a TLB miss. Its gap against hotWindow is what memory stalls cost per op at this cardinality.
func fullRange(fillSize int) keySource {
	return func(seed uint64) func() uint64 {
		x := seed | 1 // xorshift degenerates to zero if seeded with zero
		return func() uint64 {
			x ^= x << 13
			x ^= x >> 7
			x ^= x << 17
			// Lemire's range reduction: a multiply-shift instead of a division, cheap enough not to distort an op
			// measured in tens of nanoseconds.
			return fillKey(int(uint64(uint32(x)) * uint64(fillSize) >> 32))
		}
	}
}

// runLatencyOps runs latencyIters ops spread over GOMAXPROCS workers and reports throughput plus ns/op. ns/op is wall
// time over total ops, the same aggregate RunParallel prints, so these numbers compare against bench-speed rather
// than against a single-threaded latency. Returns the op count and how many of them reported true.
//
// It drives the workers itself because RunParallel takes its op count from b.N, and this family runs at -benchtime=1x.
func runLatencyOps(b *testing.B, src keySource, op func(key uint64) bool) (ops int, hits int64) {
	workers := runtime.GOMAXPROCS(0)
	perWorker := latencyIters / workers
	ops = perWorker * workers

	var hitCount atomic.Int64
	var wg sync.WaitGroup
	b.ResetTimer()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			next := src(seed)
			local := int64(0)
			for n := 0; n < perWorker; n++ {
				if op(next()) {
					local++
				}
			}
			hitCount.Add(local)
		}(uint64(w+1) * 0x9E3779B97F4A7C15)
	}
	wg.Wait()
	b.StopTimer()

	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(ops), "ns/op")
	b.ReportMetric(float64(ops)/b.Elapsed().Seconds()/1e6, "Mops/s")
	return ops, hitCount.Load()
}

// runRead times reads and guards the result with hit%: a contender that evicted the working set is timing its miss
// path, not a lookup, and the metric makes that visible instead of silently wrong.
func runRead(b *testing.B, src keySource, get func(key uint64) bool) {
	ops, hits := runLatencyOps(b, src, get)
	b.ReportMetric(100*float64(hits)/float64(ops), "hit%")
}

// runWrite times overwrites: every key drawn is already resident, so the write lands in place without growing or
// evicting.
func runWrite(b *testing.B, src keySource, set func(key uint64)) {
	runLatencyOps(b, src, func(key uint64) bool { set(key); return true })
}

// heapBaseline GCs to a quiescent live set and snapshots HeapAlloc. The returned func snapshots again the same way
// and returns the bytes resident since.
func heapBaseline() func() uint64 {
	var before runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&before)

	return func() uint64 {
		var after runtime.MemStats
		runtime.GC()
		runtime.GC()
		runtime.ReadMemStats(&after)
		return after.HeapAlloc - before.HeapAlloc
	}
}

// reportFootprint prints the memory footprint size. sizeEstimate is the contender's own byte accounting; 0 means it has none
// and only the heap figure is reported.
func reportFootprint(b *testing.B, heapBytes uint64, live int, sizeEstimate uint64) {
	perEntryHeap := float64(heapBytes) / float64(live)
	b.ReportMetric(perEntryHeap, "B/entry-heap")
	if sizeEstimate > 0 {
		b.ReportMetric(float64(sizeEstimate)/float64(live), "B/entry-estimate")
	}
	b.ReportMetric(perEntryHeap/payloadBytes, "x-payload")
	b.ReportMetric(float64(heapBytes)/(1<<20), "MiB-heap-total")
	b.Logf("capacity=%d live=%d heap=%s size-estimate=%s",
		bigBenchCapacity, live, humanize.IBytes(heapBytes), humanize.IBytes(sizeEstimate))
}

// footprintCase adapts one contender to the shared runner: build it, count its live entries, and read its own byte
// accounting. Leave sizeEstimate nil unless it is cheap at 100M entries - the reflection-based SizeOf walks every entry
// and blows up the heap it is measuring.
type footprintCase struct {
	build        func() benchCache
	live         func(benchCache) int
	sizeEstimate func(benchCache) uint64
}

// runFootprint runs a footprint pass over a cache filled to bigBenchCapacity, then Get and Set latency against that same
// filled cache.
func runFootprint(b *testing.B, tc footprintCase) {
	var c benchCache
	b.Run(humanize.Comma(bigBenchCapacity)+"-entries", func(b *testing.B) {
		c = fillAndMeasure(b, tc)
	})

	get := func(key uint64) bool { _, ok := c.Get(key); return ok }
	set := func(key uint64) { c.Set(key, key, false) }

	// Each op is measured twice against the same filled cache: over a CPU-cache-resident window, then over the whole
	// fill. Hot is the cache's own work; full adds the memory stalls of reaching into a multi-GiB heap.
	b.Run("Get/hot", func(b *testing.B) { runRead(b, hotWindow(), get) })
	b.Run("Get/full", func(b *testing.B) { runRead(b, fullRange(bigBenchCapacity), get) })
	b.Run("Set/hot", func(b *testing.B) { runWrite(b, hotWindow(), set) })
	b.Run("Set/full", func(b *testing.B) { runWrite(b, fullRange(bigBenchCapacity), set) })

	c.Close()
}

func fillAndMeasure(b *testing.B, tc footprintCase) benchCache {
	resident := heapBaseline()

	c := tc.build()
	for i := 0; i < bigBenchCapacity; i++ {
		k := fillKey(i)
		c.Set(k, k, true)
	}

	heapBytes := resident()
	live := tc.live(c)
	var sizeEstimate uint64
	if tc.sizeEstimate != nil {
		sizeEstimate = tc.sizeEstimate(c)
	}
	runtime.KeepAlive(c) // the cache must be alive at the second snapshot, or the GC reclaims what we are measuring

	reportFootprint(b, heapBytes, live, sizeEstimate)
	return c
}
