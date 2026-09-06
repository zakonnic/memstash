package eviction

import (
	"hash/maphash"
	"sync/atomic"

	"github.com/zakonnic/memstash/internal/itemstore"
)

// arena is the tests' stand-in for a shard's slot table, driven the way Cache.setMemory and Cache.killAt drive it:
// items are inserted by the probe from their home, and a release kills the item and reclaims its slot. Items move
// when a run is compacted, so the helpers address them by key.
type arena struct {
	items   itemstore.StorageProxy[string, string]
	seed    maphash.Seed
	scratch []itemstore.ShiftEntry
	moves   atomic.Uint32
}

func newArena() *arena {
	a := &arena{seed: maphash.MakeSeed()}
	a.items.Store(itemstore.NewFlatHashMap[string, string](1<<10, a.seed))
	return a
}

func (a *arena) hash(key string) uint64 { return maphash.Comparable(a.seed, key) }

// node is the queue node Cache.setMemory would register for the key.
func (a *arena) node(key string) itemstore.QNode { return itemstore.NewQNode(a.hash(key), 1) }

// claim inserts the key and returns its slot.
func (a *arena) claim(key string) uint32 {
	t := a.items.GetStorage()
	hash := a.hash(key)
	home := t.Home(hash)
	for pos := home; ; pos++ {
		item := t.At(pos)
		if itemstore.Empty(item.Metadata()) {
			item.Publish(itemstore.Entry[string, string]{Key: key, Value: "v"}, itemstore.TagOf(hash), 0, t.Dist(home, t.Wrap(pos)), false)
			t.NoteDisplaced(hash, pos)
			t.MarkChain(home, t.Wrap(pos))
			return t.Wrap(pos)
		}
	}
}

// release mirrors the cache after an eviction: kill the item at idx and reclaim its slot.
func (a *arena) release(idx uint32) {
	t := a.items.GetStorage()
	item := t.At(idx)
	hash := a.hash(item.Entry().Key)
	item.Kill()
	t.ForgetDisplaced(hash, idx)
	t.Reclaim(idx, &a.scratch, &a.moves)
}

// kill removes the key the way Delete does.
func (a *arena) kill(key string) {
	_, idx, ok := a.items.Find(a.node(key))
	if !ok {
		panic("kill: " + key + " is not in the arena")
	}
	a.release(idx)
}

// alive reports whether the key is in the arena.
func (a *arena) alive(key string) bool {
	_, _, ok := a.items.Find(a.node(key))
	return ok
}

// keyAt is the key living in the slot.
func (a *arena) keyAt(idx uint32) string { return a.items.At(idx).Entry().Key }

// keyOf resolves a queue node to its key.
func (a *arena) keyOf(node itemstore.QNode) string {
	item, _, ok := a.items.Find(node)
	if !ok {
		return ""
	}
	return item.Entry().Key
}

// addFromPool registers a fresh item's node with the policy, mirroring what Cache.setMemory does.
func addFromPool(p interface{ Add(itemstore.QNode) }, a *arena, key string) {
	a.claim(key)
	p.Add(a.node(key))
}

// touch sets one reference-counter bit on the item, as a lock-free reader would.
func touch(a *arena, key string) {
	item, _, ok := a.items.Find(a.node(key))
	if !ok {
		panic("touch: " + key + " is not in the arena")
	}
	item.TouchWith(item.Metadata())
}
