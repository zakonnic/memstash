package eviction

import (
	"unsafe"

	"github.com/zakonnic/memstash/internal/itemstore"
)

// ClockPolicy is GCLOCK: one FIFO queue where an item with a non-zero reference counter loses one chance and moves to
// the back, and an item with a zero counter is evicted.
type ClockPolicy[K comparable, V any] struct {
	items *itemstore.StorageProxy[K, V]
}

// NewClockPolicy creates a ClockPolicy policy.
func NewClockPolicy[K comparable, V any](items *itemstore.StorageProxy[K, V]) *ClockPolicy[K, V] {
	return &ClockPolicy[K, V]{items: items}
}

func (p *ClockPolicy[K, V]) Add(itemstore.QNode)    {}
func (p *ClockPolicy[K, V]) Remove(itemstore.QNode) {}
func (p *ClockPolicy[K, V]) Reweigh(uint32, int64)  {}
func (p *ClockPolicy[K, V]) Bytes() int64           { return int64(unsafe.Sizeof(*p)) }

func (p *ClockPolicy[K, V]) Evict(nowOff uint32) (uint32, bool) {
	t := p.items.GetStorage()
	// Finite: each step spends one of an item's at most two chances or returns.
	for {
		idx, ok := t.Oldest(0)
		if !ok {
			return 0, false
		}
		item := t.At(idx)
		if metaWord := item.Metadata(); chanced(metaWord, nowOff) {
			item.RevokeChance(metaWord)
			t.Requeue(0, idx)
			continue
		}
		return idx, true
	}
}
