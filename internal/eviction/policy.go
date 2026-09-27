// Package eviction implements the shard's eviction policies (Clock, S3-FIFO, W-TinyLFU and SIEVE) over the eviction
// queues the shard's table threads through its slots. Every method runs under the owning shard's mutex.
//
// The policy contract itself is public: memstash.EvictionPolicy. The types here implement it, and custom policies
// plugged in through memstash.WithCustomEvictionPolicy implement the same interface.
package eviction

import "github.com/zakonnic/memstash/internal/itemstore"

// The cache enqueues every new item on queue 0; a two-queue policy moves the items it protects to mainQueue.
const mainQueue = 1

// chanced reports whether an item still has a chance on its reference counter and has not expired. Such an item gets
// another round instead of eviction.
func chanced(metaWord uint64, nowOff uint32) bool {
	return metaWord&itemstore.ChanceMask != 0 && !itemstore.Expired(metaWord, nowOff)
}

// admission is queue 0 of a two-queue policy, kept to a weight: S3-FIFO's small, W-TinyLFU's window.
type admission[K comparable, V any] struct {
	items  *itemstore.StorageProxy[K, V]
	weight int64
	cap    int64
}

func (a *admission[K, V]) Add(node itemstore.QNode) { a.weight += int64(node.Cost) }

func (a *admission[K, V]) Remove(node itemstore.QNode) {
	if a.items.At(node.Idx).Metadata()&itemstore.InMainQueue == 0 {
		a.weight -= int64(node.Cost)
	}
}

func (a *admission[K, V]) Reweigh(idx uint32, delta int64) {
	if a.items.At(idx).Metadata()&itemstore.InMainQueue == 0 {
		a.weight += delta
	}
}

// next is the item eviction examines: admission's oldest while the queue is over its weight, main's otherwise, and
// admission's again when main is empty.
func (a *admission[K, V]) next(t *itemstore.FlatHashMap[K, V]) (idx uint32, fromAdmission, ok bool) {
	if idx, ok = t.Oldest(0); ok && a.weight > a.cap {
		return idx, true, true
	}
	if idx, ok := t.Oldest(mainQueue); ok {
		return idx, false, true
	}
	idx, ok = t.Oldest(0)
	return idx, true, ok
}

// promote moves an item of the admission queue to the back of main.
func (a *admission[K, V]) promote(t *itemstore.FlatHashMap[K, V], idx uint32) {
	a.weight -= int64(a.items.Cost(idx))
	t.Requeue(mainQueue, idx)
}
