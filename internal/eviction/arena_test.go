package eviction

import "github.com/zakonnic/memstash/internal/itemstore"

// arena is the tests' stand-in for a shard's slot table: items are published at sequential positions, mimicking
// what Cache.setMemory does through the probe.
type arena struct {
	items itemstore.StorageProxy[string, string]
	next  uint32
}

func newArena() *arena {
	a := &arena{}
	a.items.Store(itemstore.NewFlatHashMap[string, string](1 << 10))
	return a
}

func (a *arena) claim(key string) uint32 {
	idx := a.next
	a.next++
	a.items.At(idx).Publish(itemstore.Entry[string, string]{Key: key, Value: "v"}, 0, 0)
	return idx
}

// release mirrors the cache after an eviction: kill (idempotent) and hand the slot back.
func (a *arena) release(idx uint32) {
	item := a.items.At(idx)
	item.Kill()
	item.MakeFree()
}

// addFromPool registers a fresh item's node with the policy, mirroring what Cache.setMemory does. Returns the
// node's slot index.
func addFromPool(p interface{ Add(itemstore.QNode) }, a *arena, key string) uint32 {
	idx := a.claim(key)
	p.Add(itemstore.QNode{Idx: idx, Cost: 1})
	return idx
}

// touch sets one reference-counter bit on the item, as a lock-free reader would.
func touch(a *arena, idx uint32) {
	item := a.items.At(idx)
	item.TouchWith(item.Load())
}
