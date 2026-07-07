// Package benchmarks compares memstash against popular in-memory caches by hit rate and operation speed.
package benchmarks

import (
	"context"
	"sync"

	theine "github.com/Yiling-J/theine-go"
	"github.com/dgraph-io/ristretto/v2"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/maypok86/otter/v2"
	"github.com/puzpuzpuz/xsync/v3"

	"github.com/zakonnic/memstash"
)

// benchCache is the single contract shared by all comparison participants.
type benchCache interface {
	Name() string
	Get(key uint64) (uint64, bool)
	// Set stores the value; sync=true asks it to wait until the write is applied (needed only for a fair hit-rate
	// measurement of caches with an asynchronous Set).
	Set(key uint64, value uint64, sync bool)
	Close()
}

// --- memstash ---

type memstashAdapter struct {
	c    *memstash.Cache[uint64, uint64]
	name string
}

func newMemstash(capacity int64, policy memstash.Policy, name string) benchCache {
	c, err := memstash.New[uint64, uint64](
		memstash.WithMemoryCapacity(capacity),
		memstash.WithPolicy(policy),
	)
	if err != nil {
		panic(err)
	}
	return &memstashAdapter{c: c, name: name}
}

func (a *memstashAdapter) Name() string                  { return a.name }
func (a *memstashAdapter) Get(key uint64) (uint64, bool) { return a.c.GetFromMemory(key) }
func (a *memstashAdapter) Set(key, value uint64, _ bool) {
	_ = a.c.Set(context.Background(), key, value)
}
func (a *memstashAdapter) Close() { a.c.Close() }

// --- ristretto ---

type ristrettoAdapter struct {
	c *ristretto.Cache[uint64, uint64]
}

func newRistretto(capacity int64) benchCache {
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

func (a *ristrettoAdapter) Name() string                  { return "ristretto" }
func (a *ristrettoAdapter) Get(key uint64) (uint64, bool) { return a.c.Get(key) }
func (a *ristrettoAdapter) Set(key, value uint64, sync bool) {
	a.c.Set(key, value, 1)
	if sync {
		a.c.Wait() // ristretto's Set is asynchronous: without Wait the hit-rate measurement is unfair
	}
}
func (a *ristrettoAdapter) Close() { a.c.Close() }

// --- otter (W-TinyLFU) ---
//
// otter v2 has a single eviction policy - adaptive W-TinyLFU (a count-min frequency sketch in front of an LRU/SLRU) -
// so the label makes the policy explicit; there is no other otter mode to select.

type otterAdapter struct{ c *otter.Cache[uint64, uint64] }

func newOtter(capacity int64) benchCache {
	c := otter.Must(&otter.Options[uint64, uint64]{
		MaximumSize: int(capacity),
	})
	return &otterAdapter{c: c}
}

func (a *otterAdapter) Name() string                  { return "otter-wtinylfu" }
func (a *otterAdapter) Get(key uint64) (uint64, bool) { return a.c.GetIfPresent(key) }
func (a *otterAdapter) Set(key, value uint64, _ bool) { a.c.Set(key, value) }
func (a *otterAdapter) Close()                        { a.c.StopAllGoroutines() }

// --- theine (W-TinyLFU) ---

type theineAdapter struct{ c *theine.Cache[uint64, uint64] }

func newTheine(capacity int64) benchCache {
	c, err := theine.NewBuilder[uint64, uint64](capacity).Build()
	if err != nil {
		panic(err)
	}
	return &theineAdapter{c: c}
}

func (a *theineAdapter) Name() string                  { return "theine-wtinylfu" }
func (a *theineAdapter) Get(key uint64) (uint64, bool) { return a.c.Get(key) }
func (a *theineAdapter) Set(key, value uint64, _ bool) { a.c.Set(key, value, 1) }
func (a *theineAdapter) Close()                        { a.c.Close() }

// --- hashicorp/golang-lru (classic LRU behind a mutex) ---

type lruAdapter struct{ c *lru.Cache[uint64, uint64] }

func newLRU(capacity int64) benchCache {
	c, err := lru.New[uint64, uint64](int(capacity))
	if err != nil {
		panic(err)
	}
	return &lruAdapter{c: c}
}

func (a *lruAdapter) Name() string                  { return "hashicorp-lru" }
func (a *lruAdapter) Get(key uint64) (uint64, bool) { return a.c.Get(key) }
func (a *lruAdapter) Set(key, value uint64, _ bool) { a.c.Add(key, value) }
func (a *lruAdapter) Close()                        {}

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

// --- xsync.MapOf (no eviction; lower bound on the cost of map operations) ---

type xsyncMapAdapter struct{ m *xsync.MapOf[uint64, uint64] }

func newXsyncMap() benchCache { return &xsyncMapAdapter{m: xsync.NewMapOf[uint64, uint64]()} }

func (a *xsyncMapAdapter) Name() string                  { return "xsync.MapOf" }
func (a *xsyncMapAdapter) Get(key uint64) (uint64, bool) { return a.m.Load(key) }
func (a *xsyncMapAdapter) Set(key, value uint64, _ bool) { a.m.Store(key, value) }
func (a *xsyncMapAdapter) Close()                        {}
