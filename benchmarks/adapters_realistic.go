package benchmarks

import (
	"context"
	"strconv"
	"time"

	theine "github.com/Yiling-J/theine-go"
	"github.com/allegro/bigcache/v3"
	"github.com/coocood/freecache"
	"github.com/dgraph-io/ristretto/v2"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/maypok86/otter/v2"
	"github.com/zakonnic/memstash"
)

// benchCacheBytes is the realistic-workload counterpart of benchCache: string keys, []byte values, and a byte budget
// instead of an item count. Weight-aware and byte-bounded caches take the budget directly with cost =
// len(key)+len(value); the count-only hashicorp LRU approximates it as budget/avgEntry items.
type benchCacheBytes interface {
	Name() string
	Get(key string) ([]byte, bool)
	Set(key string, value []byte, sync bool)
	Close()
	GetSize() uint64
	Len() int
}

func entryCost(key string, value []byte) uint32 { return uint32(len(key) + len(value)) }

// --- memstash ---

type memstashBytesAdapter struct {
	c    *memstash.Cache[string, []byte]
	name string
}

func newMemstashBytes(budget int64, policy memstash.Policy, name string) benchCacheBytes {
	c, err := memstash.New[string, []byte](
		memstash.WithMemoryBudget(budget),
		memstash.WithCostFunc(entryCost),
		memstash.WithPolicy(policy),
	)
	if err != nil {
		panic(err)
	}
	return &memstashBytesAdapter{c: c, name: name}
}

func (a *memstashBytesAdapter) Name() string                  { return a.name }
func (a *memstashBytesAdapter) Get(key string) ([]byte, bool) { return a.c.GetFromMemory(key) }
func (a *memstashBytesAdapter) Set(key string, value []byte, _ bool) {
	_ = a.c.Set(context.Background(), key, value)
}
func (a *memstashBytesAdapter) Close() { a.c.Close() }

// GetSize adds Weight() to the structural total: the payload lives in heap allocations outside the cache, and the
// cost function here is exactly len(key)+len(value), so Weight() is the payload byte total.
func (a *memstashBytesAdapter) GetSize() uint64 { return uint64(a.c.TotalWeight() + a.c.Weight()) }
func (a *memstashBytesAdapter) Len() int        { return a.c.Len() }

// --- ristretto ---

type ristrettoBytesAdapter struct {
	c *ristretto.Cache[string, []byte]
}

func newRistrettoBytes(budget int64, avgEntry int) benchCacheBytes {
	c, err := ristretto.NewCache(&ristretto.Config[string, []byte]{
		NumCounters: budget / int64(avgEntry) * 10,
		Cost: func(value []byte) int64 {
			return int64(len(value))
		},
		MaxCost:     budget,
		BufferItems: 64,
	})
	if err != nil {
		panic(err)
	}
	return &ristrettoBytesAdapter{c: c}
}

func (a *ristrettoBytesAdapter) Name() string                  { return "ristretto" }
func (a *ristrettoBytesAdapter) Get(key string) ([]byte, bool) { return a.c.Get(key) }
func (a *ristrettoBytesAdapter) Set(key string, value []byte, sync bool) {
	a.c.Set(key, value, int64(entryCost(key, value)))
	if sync {
		a.c.Wait() // ristretto's Set is asynchronous: without Wait the hit-rate measurement is unfair
	}
}
func (a *ristrettoBytesAdapter) Close() { a.c.Close() }

// GetSize: see ristrettoAdapter.GetSize.
func (a *ristrettoBytesAdapter) GetSize() uint64 {
	a.c.Wait()
	return SizeOf(a.c)
}

func (a *ristrettoBytesAdapter) Len() int {
	a.c.Wait()
	count := 0
	a.c.IterValues(func([]byte) bool {
		count++
		return false
	})
	return count
}

// --- otter (W-TinyLFU) ---

type otterBytesAdapter struct{ c *otter.Cache[string, []byte] }

func newOtterBytes(budget int64) benchCacheBytes {
	c := otter.Must(&otter.Options[string, []byte]{
		MaximumWeight: uint64(budget),
		Weigher:       entryCost,
	})
	return &otterBytesAdapter{c: c}
}

func (a *otterBytesAdapter) Name() string                         { return "otter-wtinylfu" }
func (a *otterBytesAdapter) Get(key string) ([]byte, bool)        { return a.c.GetIfPresent(key) }
func (a *otterBytesAdapter) Set(key string, value []byte, _ bool) { a.c.Set(key, value) }
func (a *otterBytesAdapter) Close()                               { a.c.StopAllGoroutines() }

// GetSize: see otterAdapter.GetSize.
func (a *otterBytesAdapter) GetSize() uint64 {
	a.c.StopAllGoroutines()
	estimatedSize := SizeOf(a.c)
	count := a.c.EstimatedSize()
	return estimatedSize + simulateMapBucketBytes[string, otterNode[string, []byte]](count, func(i int) string { return strconv.Itoa(i) })
}

func (a *otterBytesAdapter) Len() int { return a.c.EstimatedSize() }

// --- theine (W-TinyLFU) ---

type theineBytesAdapter struct{ c *theine.Cache[string, []byte] }

func newTheineBytes(budget int64) benchCacheBytes {
	c, err := theine.NewBuilder[string, []byte](budget).Build()
	if err != nil {
		panic(err)
	}
	return &theineBytesAdapter{c: c}
}

func (a *theineBytesAdapter) Name() string                  { return "theine-wtinylfu" }
func (a *theineBytesAdapter) Get(key string) ([]byte, bool) { return a.c.Get(key) }
func (a *theineBytesAdapter) Set(key string, value []byte, _ bool) {
	a.c.Set(key, value, int64(entryCost(key, value)))
}
func (a *theineBytesAdapter) Close() { a.c.Close() }

// GetSize: see theineAdapter.GetSize.
func (a *theineBytesAdapter) GetSize() uint64 {
	a.c.Wait()
	return SizeOf(a.c)
}

func (a *theineBytesAdapter) Len() int {
	a.c.Wait() // Set is asynchronous
	return a.c.Len()
}

// --- hashicorp/golang-lru ---

type lruBytesAdapter struct{ c *lru.Cache[string, []byte] }

func newLRUBytes(budget int64, avgEntry int) benchCacheBytes {
	c, err := lru.New[string, []byte](int(budget / int64(avgEntry)))
	if err != nil {
		panic(err)
	}
	return &lruBytesAdapter{c: c}
}

func (a *lruBytesAdapter) Name() string                         { return "hashicorp-lru" }
func (a *lruBytesAdapter) Get(key string) ([]byte, bool)        { return a.c.Get(key) }
func (a *lruBytesAdapter) Set(key string, value []byte, _ bool) { a.c.Add(key, value) }
func (a *lruBytesAdapter) Close()                               {}
func (a *lruBytesAdapter) GetSize() uint64                      { return SizeOf(a.c) }
func (a *lruBytesAdapter) Len() int                             { return a.c.Len() }

// --- freecache ---

type freecacheBytesAdapter struct{ c *freecache.Cache }

func newFreecacheBytes(budget int64) benchCacheBytes {
	return &freecacheBytesAdapter{c: freecache.NewCache(int(budget))}
}

func (a *freecacheBytesAdapter) Name() string { return "freecache" }
func (a *freecacheBytesAdapter) Get(key string) ([]byte, bool) {
	v, err := a.c.Get([]byte(key))
	if err != nil {
		return nil, false
	}
	return v, true
}
func (a *freecacheBytesAdapter) Set(key string, value []byte, _ bool) {
	_ = a.c.Set([]byte(key), value, 0) // expireSeconds=0: never expire, only size-based eviction
}
func (a *freecacheBytesAdapter) Close()          {}
func (a *freecacheBytesAdapter) GetSize() uint64 { return SizeOf(a.c) }
func (a *freecacheBytesAdapter) Len() int        { return int(a.c.EntryCount()) }

// --- bigcache ---

type bigcacheBytesAdapter struct{ c *bigcache.BigCache }

func newBigcacheBytes(budget int64, avgEntry int) benchCacheBytes {
	config := bigcache.DefaultConfig(100 * 365 * 24 * time.Hour) // effectively no time-based expiry
	config.Shards = 64
	config.MaxEntriesInWindow = int(budget / int64(avgEntry))
	config.MaxEntrySize = avgEntry
	config.HardMaxCacheSize = int((budget + 1<<20 - 1) >> 20) // MiB
	config.Verbose = false
	c, err := bigcache.New(context.Background(), config)
	if err != nil {
		panic(err)
	}
	return &bigcacheBytesAdapter{c: c}
}

func (a *bigcacheBytesAdapter) Name() string { return "bigcache" }
func (a *bigcacheBytesAdapter) Get(key string) ([]byte, bool) {
	v, err := a.c.Get(key)
	if err != nil {
		return nil, false
	}
	return v, true
}
func (a *bigcacheBytesAdapter) Set(key string, value []byte, _ bool) { _ = a.c.Set(key, value) }
func (a *bigcacheBytesAdapter) Close()                               { _ = a.c.Close() }
func (a *bigcacheBytesAdapter) GetSize() uint64                      { return SizeOf(a.c) }
func (a *bigcacheBytesAdapter) Len() int                             { return a.c.Len() }
