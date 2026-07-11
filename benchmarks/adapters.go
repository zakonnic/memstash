// Package benchmarks compares memstash against popular in-memory caches by hit rate and operation speed.
package benchmarks

import (
	"context"
	"encoding/binary"
	"math"
	"strconv"
	"sync"
	"time"
	"unsafe"

	theine "github.com/Yiling-J/theine-go"
	"github.com/allegro/bigcache/v3"
	"github.com/coocood/freecache"
	"github.com/dgraph-io/ristretto/v2"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/maypok86/otter/v2"
	"github.com/puzpuzpuz/xsync/v3"
	"github.com/zakonnic/memstash"
)

type benchCache interface {
	Name() string
	Get(key uint64) (uint64, bool)
	Set(key uint64, value uint64, sync bool)
	Close()
	Expose() any
	GetSize() uint64
}

// --- memstash ---

type memstashAdapter struct {
	c    *memstash.Cache[uint64, uint64]
	name string
}

func newMemstash(capacity int64, policy memstash.Policy, name string, withSizeCoef bool) benchCache {
	if withSizeCoef {
		capacity = AjustCapForMemstash(capacity, policy)
	}
	c, err := memstash.New[uint64, uint64](
		memstash.WithMemoryCapacity(capacity),
		memstash.WithPolicy(policy),
	)
	if err != nil {
		panic(err)
	}
	return &memstashAdapter{c: c, name: name}
}

// AjustCapForMemstash adjusts capacity so memory footprint lands close to others
func AjustCapForMemstash(capacity int64, policy memstash.Policy) int64 {
	coeff := 1.0
	if policy == memstash.PolicyClock {
		coeff = 0.8
	}
	return int64(math.Round(float64(capacity) / coeff))
}

func (a *memstashAdapter) Name() string                  { return a.name }
func (a *memstashAdapter) Get(key uint64) (uint64, bool) { return a.c.GetFromMemory(key) }
func (a *memstashAdapter) Set(key, value uint64, _ bool) {
	_ = a.c.Set(context.Background(), key, value)
}
func (a *memstashAdapter) Close()          { a.c.Close() }
func (a *memstashAdapter) Expose() any     { return a.c }
func (a *memstashAdapter) GetSize() uint64 { return uint64(a.c.TotalWeight()) }

// --- ristretto ---

type ristrettoAdapter struct {
	c *ristretto.Cache[uint64, uint64]
}

func newRistretto(capacity int64, withSizeCoef bool) benchCache {
	if withSizeCoef {
		capacity = AjustCapForRistretto(capacity)
	}
	c, err := ristretto.NewCache(&ristretto.Config[uint64, uint64]{
		NumCounters: capacity * 10,
		MaxCost:     capacity,
		BufferItems: 64,
	})
	if err != nil {
		panic(err)
	}
	return &ristrettoAdapter{c: c}
}

// AjustCapForRistretto adjusts capacity so memory footprint lands close to others
func AjustCapForRistretto(capacity int64) int64 {
	const coeff = 0.39
	return int64(math.Round(float64(capacity) / coeff))
}

func (a *ristrettoAdapter) Name() string                  { return "ristretto" }
func (a *ristrettoAdapter) Get(key uint64) (uint64, bool) { return a.c.Get(key) }
func (a *ristrettoAdapter) Set(key, value uint64, sync bool) {
	a.c.Set(key, value, 1)
	if sync {
		a.c.Wait() // ristretto's Set is asynchronous: without Wait the hit-rate measurement is unfair
	}
}
func (a *ristrettoAdapter) Close()      { a.c.Close() }
func (a *ristrettoAdapter) Expose() any { return a.c }

// GetSize ristretto has no byte-accounting API (Metrics tracks Set's cost units, which the benchmark always calls
// with 1 - an item count, not bytes), and its store is a plain mutex-guarded Go map, so reflection sees all of it.
func (a *ristrettoAdapter) GetSize() uint64 {
	a.c.Wait()
	return SizeOf(a.c)
}

// --- otter (W-TinyLFU) ---
//
// otter v2 has a single eviction policy - adaptive W-TinyLFU (a count-min frequency sketch in front of an LRU/SLRU) -
// so the label makes the policy explicit; there is no other otter mode to select.

type otterAdapter struct{ c *otter.Cache[uint64, uint64] }

func newOtter(capacity int64, withSizeCoef bool) benchCache {
	if withSizeCoef {
		capacity = AjustCapForOtter(capacity)
	}
	c := otter.Must(&otter.Options[uint64, uint64]{
		MaximumSize: int(capacity),
	})
	return &otterAdapter{c: c}
}

func (a *otterAdapter) Name() string                  { return "otter-wtinylfu" }
func (a *otterAdapter) Get(key uint64) (uint64, bool) { return a.c.GetIfPresent(key) }
func (a *otterAdapter) Set(key, value uint64, _ bool) { a.c.Set(key, value) }
func (a *otterAdapter) Close()                        { a.c.StopAllGoroutines() }
func (a *otterAdapter) Expose() any                   { return a.c }

// AjustCapForOtter adjusts capacity so memory footprint lands close to others
func AjustCapForOtter(capacity int64) int64 {
	const coeff = 0.60
	return int64(math.Round(float64(capacity) / coeff))
}

type otterNode[K comparable, V any] struct {
	key       K
	value     V
	prev      unsafe.Pointer
	next      unsafe.Pointer
	state     uint32
	queueType uint8
}

// GetSize otter reports no byte total, and its hash table is a fork of xsync.MapOf living in an unexported
// internal package, so we can't call an equivalent of Stats() directly on it. But can simulate this map.
func (a *otterAdapter) GetSize() uint64 {
	a.c.StopAllGoroutines()
	estimatedSize := SizeOf(a.c)
	count := a.c.EstimatedSize()
	return estimatedSize + simulateMapBucketBytes[uint64, otterNode[uint64, uint64]](count, func(i int) uint64 { return uint64(i) })
}

// --- theine (W-TinyLFU) ---

type theineAdapter struct{ c *theine.Cache[uint64, uint64] }

func newTheine(capacity int64, withSizeCoef bool) benchCache {
	if withSizeCoef {
		capacity = AjustCapForTheine(capacity)
	}
	c, err := theine.NewBuilder[uint64, uint64](capacity).Build()
	if err != nil {
		panic(err)
	}
	return &theineAdapter{c: c}
}

// AjustCapForTheine adjusts capacity so memory footprint lands close to others
func AjustCapForTheine(capacity int64) int64 {
	const coeff = 0.96
	return int64(math.Round(float64(capacity) / coeff))
}

func (a *theineAdapter) Name() string                  { return "theine-wtinylfu" }
func (a *theineAdapter) Get(key uint64) (uint64, bool) { return a.c.Get(key) }
func (a *theineAdapter) Set(key, value uint64, _ bool) { a.c.Set(key, value, 1) }
func (a *theineAdapter) Close()                        { a.c.Close() }
func (a *theineAdapter) Expose() any                   { return a.c }

// GetSize theine has no byte-accounting API either, but its store is plain sharded maps (map[K]*Entry) guarded by
// mutexes, no unsafe.Pointer indirection, so reflection can walk all of it.
func (a *theineAdapter) GetSize() uint64 {
	a.c.Wait()
	return SizeOf(a.c)
}

// --- hashicorp/golang-lru (classic LRU behind a mutex) ---

type lruAdapter struct{ c *lru.Cache[uint64, uint64] }

func newLRU(capacity int64, withSizeCoef bool) benchCache {
	if withSizeCoef {
		capacity = AjustCapForHashicorpLRU(capacity)
	}
	c, err := lru.New[uint64, uint64](int(capacity))
	if err != nil {
		panic(err)
	}
	return &lruAdapter{c: c}
}

// AjustCapForHashicorpLRU adjusts capacity so memory footprint lands close to others
func AjustCapForHashicorpLRU(capacity int64) int64 {
	const coeff = 0.72
	return int64(math.Round(float64(capacity) / coeff))
}

func (a *lruAdapter) Name() string                  { return "hashicorp-lru" }
func (a *lruAdapter) Get(key uint64) (uint64, bool) { return a.c.Get(key) }
func (a *lruAdapter) Set(key, value uint64, _ bool) { a.c.Add(key, value) }
func (a *lruAdapter) Close()                        {}
func (a *lruAdapter) Expose() any                   { return a.c }

// GetSize a plain container/list + map, no native size accounting; reflection covers it fully (no unsafe.Pointer).
func (a *lruAdapter) GetSize() uint64 { return SizeOf(a.c) }

// --- freecache (shard-local LRU-ish ring buffers over []byte) ---
//
// freecache sizes itself in bytes rather than item count, so capacity is converted using a generous per-entry
// overhead estimate (header + 8-byte int key + 8-byte value) to make its resident size comparable to the other
// caches under the same `capacity` items.

type freecacheAdapter struct{ c *freecache.Cache }

func newFreecache(capacity int64, avgKeySize, avgValueSize int, withSizeCoef bool) benchCache {
	if withSizeCoef {
		capacity = AjustCapForFreecache(capacity)
	}
	const entryOverhead = 24 // bytes, overhead estimate
	itemSize := entryOverhead + avgKeySize + avgValueSize
	totalBytes := int(capacity) * itemSize
	c := freecache.NewCache(totalBytes)
	return &freecacheAdapter{c: c}
}

// AjustCapForFreecache adjusts capacity so memory footprint lands close to others
func AjustCapForFreecache(capacity int64) int64 {
	const coeff = 1.05
	return int64(math.Round(float64(capacity) / coeff))
}

func (a *freecacheAdapter) Name() string { return "freecache" }
func (a *freecacheAdapter) Get(key uint64) (uint64, bool) {
	v, err := a.c.GetInt(int64(key))
	if err != nil {
		return 0, false
	}
	return binary.LittleEndian.Uint64(v), true
}
func (a *freecacheAdapter) Set(key, value uint64, _ bool) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], value)
	_ = a.c.SetInt(int64(key), buf[:], 0) // expireSeconds=0: never expire, only size-based eviction
}
func (a *freecacheAdapter) Close()      {}
func (a *freecacheAdapter) Expose() any { return a.c }

// GetSize freecache pre-allocates one fixed []byte ring buffer per segment at construction and manages entries as
// raw bytes within it - so reflection already reports the real, mostly load-independent footprint;
func (a *freecacheAdapter) GetSize() uint64 { return SizeOf(a.c) }

// --- bigcache (sharded, size-bounded, string-keyed) ---
//
// bigcache has no item-count limit, only a byte budget (HardMaxCacheSize) and a shard count; both are derived from
// capacity with a generous per-entry overhead estimate so its resident size is comparable to the other caches.

type bigcacheAdapter struct{ c *bigcache.BigCache }

func newBigcache(capacity int64, withSizeCoef bool) benchCache {
	if withSizeCoef {
		capacity = AjustCapForBigcache(capacity)
	}
	config := bigcache.DefaultConfig(100 * 365 * 24 * time.Hour) // effectively no time-based expiry
	config.Shards = 64
	config.MaxEntriesInWindow = int(capacity)
	config.MaxEntrySize = 64
	config.HardMaxCacheSize = getBigCacheHardMaxCacheSize(capacity, 8, 8)
	config.Verbose = false
	c, err := bigcache.New(context.Background(), config)
	if err != nil {
		panic(err)
	}
	return &bigcacheAdapter{c: c}
}

// AjustCapForBigcache adjusts capacity so memory footprint lands close to others
func AjustCapForBigcache(capacity int64) int64 {
	if capacity < 200_000 {
		return int64(math.Round(float64(capacity) / 0.3))
	}
	return int64(math.Round(float64(capacity) / 0.4))
}

func getBigCacheHardMaxCacheSize(capacity int64, avgKeySize, avgValueSize int) int {
	const bigcacheEntryOverhead = 8 + 8 + 2 // timestamp + hash + key length

	bytesPerEntry := int64(avgKeySize + avgValueSize + bigcacheEntryOverhead)
	totalBytes := capacity * bytesPerEntry
	hardMaxMB := (totalBytes + 1024*1024 - 1) / (1024 * 1024) // MiB
	if hardMaxMB < 1 {
		hardMaxMB = 1
	}
	return int(hardMaxMB)
}

func (a *bigcacheAdapter) Name() string { return "bigcache" }
func (a *bigcacheAdapter) Get(key uint64) (uint64, bool) {
	v, err := a.c.Get(strconv.FormatUint(key, 10))
	if err != nil {
		return 0, false
	}
	return binary.LittleEndian.Uint64(v), true
}
func (a *bigcacheAdapter) Set(key, value uint64, _ bool) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], value)
	_ = a.c.Set(strconv.FormatUint(key, 10), buf[:])
}
func (a *bigcacheAdapter) Close()      { _ = a.c.Close() }
func (a *bigcacheAdapter) Expose() any { return a.c }

// GetSize bigcache's own Capacity() misses inner hash index, SizeOf walks both.
func (a *bigcacheAdapter) GetSize() uint64 { return SizeOf(a.c) }

// --- sync.Map (no eviction; speed measurements only) ---

type syncMapAdapter struct{ m sync.Map }

func newSyncMap() benchCache { return &syncMapAdapter{} }

func (a *syncMapAdapter) Name() string { return "sync.Map" }
func (a *syncMapAdapter) Get(key uint64) (uint64, bool) {
	value, ok := a.m.Load(key)
	if !ok {
		return 0, false
	}
	return value.(uint64), true
}
func (a *syncMapAdapter) Set(key, value uint64, _ bool) { a.m.Store(key, value) }
func (a *syncMapAdapter) Close()                        {}
func (a *syncMapAdapter) Expose() any                   { return &a.m }

// GetSize sync.Map has no size accounting of its own, and its internal read/dirty maps use interface{}-boxed
// entries reached through regular pointers (no unsafe.Pointer), so reflection can walk it.
func (a *syncMapAdapter) GetSize() uint64 { return SizeOf(&a.m) }

// --- xsync.MapOf (no eviction; lower bound on the cost of map operations) ---

type xsyncMapAdapter struct{ m *xsync.MapOf[uint64, uint64] }

func newXsyncMap() benchCache { return &xsyncMapAdapter{m: xsync.NewMapOf[uint64, uint64]()} }

func (a *xsyncMapAdapter) Name() string                  { return "xsync.MapOf" }
func (a *xsyncMapAdapter) Get(key uint64) (uint64, bool) { return a.m.Load(key) }
func (a *xsyncMapAdapter) Set(key, value uint64, _ bool) { a.m.Store(key, value) }
func (a *xsyncMapAdapter) Close()                        {}
func (a *xsyncMapAdapter) Expose() any                   { return a.m }

// GetSize xsync.MapOf's own buckets sit behind unsafe.Pointer (invisible to reflection), but it exposes Stats(),
// which gives the exact bucket/entry counts needed to size it properly - same approach memstash's TotalWeight uses.
func (a *xsyncMapAdapter) GetSize() uint64 { return xsyncMapOfBytes(a.m) }
