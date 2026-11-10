package eviction

import (
	"hash/maphash"
	"sync/atomic"

	"github.com/zakonnic/memstash/internal/itemstore"
)

type policy interface {
	Add(itemstore.QNode)
	Remove(itemstore.QNode)
	Evict(nowOff uint32) (uint32, bool)
}

// arena is the tests' stand-in for a shard's slot table, driven the way Cache.setMemory and Cache.killAt drive it.
// Items move when a run is compacted, so the helpers address them by key.
type arena struct {
	items   itemstore.StorageProxy[string, string]
	h       maphash.Seed
	scratch []int64
	moves   atomic.Uint32
}

func newArena() *arena {
	a := &arena{h: maphash.MakeSeed()}
	a.items.Store(itemstore.NewFlatHashMap[string, string](1<<10, a.h))
	return a
}

func (a *arena) hash(key string) uint64 { return maphash.Comparable(a.h, key) }

// claim places the key and enqueues it at the back of queue 0, the cache's half of an insert.
func (a *arena) claim(key string) uint32 {
	t := a.items.GetStorage()
	hash := a.hash(key)
	home := t.Home(hash)
	for pos := home; ; pos++ {
		item := t.At(pos)
		if itemstore.Empty(item.Metadata()) {
			item.Publish(itemstore.Entry[string, string]{Key: key, Value: "v"}, itemstore.TagOf(hash), 0, t.Dist(home, t.Wrap(pos)))
			t.NoteDisplaced(hash, pos)
			t.MarkChain(home, t.Wrap(pos))
			t.Enqueue(0, t.Wrap(pos))
			return t.Wrap(pos)
		}
	}
}

func (a *arena) add(p policy, key string) {
	p.Add(itemstore.QNode{Idx: a.claim(key), Cost: 1})
}

// release removes the item at idx the way Cache.killAt does.
func (a *arena) release(p policy, idx uint32) {
	t := a.items.GetStorage()
	item := t.At(idx)
	p.Remove(itemstore.QNode{Idx: idx, Cost: a.items.Cost(idx)})
	t.Remove(idx)
	t.ForgetDisplacedOf(idx, item)
	item.Kill()
	t.Reclaim(idx, &a.scratch, &a.moves)
}

// evict runs one eviction to its end and returns the victim's key.
func (a *arena) evict(p policy) (string, bool) {
	idx, ok := p.Evict(0)
	if !ok {
		return "", false
	}
	key := a.keyAt(idx)
	a.release(p, idx)
	return key, true
}

func (a *arena) slotOf(key string) (uint32, bool) {
	t := a.items.GetStorage()
	for idx := range uint32(t.Len()) {
		if item := t.At(idx); !itemstore.Empty(item.Metadata()) && item.Entry().Key == key {
			return idx, true
		}
	}
	return 0, false
}

func (a *arena) keyAt(idx uint32) string { return a.items.At(idx).Entry().Key }

// queued returns the keys of a queue, oldest first.
func (a *arena) queued(queue uint) []string {
	var keys []string
	for idx, ok := a.items.Oldest(queue); ok; idx, ok = a.items.Newer(idx) {
		keys = append(keys, a.keyAt(idx))
	}
	return keys
}

// touch sets one reference-counter bit on the item, as a lock-free reader would.
func (a *arena) touch(key string) {
	idx, ok := a.slotOf(key)
	if !ok {
		panic("touch: " + key + " is not in the arena")
	}
	item := a.items.At(idx)
	item.TouchWith(item.Metadata())
}
