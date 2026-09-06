package eviction

import (
	"unsafe"

	"github.com/zakonnic/memstash/internal/itemstore"
)

// ClockPolicy is GCLOCK: a single FIFO queue where a node with a non-zero reference counter loses one chance and
// moves to the tail, and a node with a zero counter is evicted.
type ClockPolicy[K comparable, V any] struct {
	items *itemstore.StorageProxy[K, V]
	q     itemstore.EvictQueue
}

// NewClockPolicy creates a ClockPolicy policy.
func NewClockPolicy[K comparable, V any](items *itemstore.StorageProxy[K, V]) *ClockPolicy[K, V] {
	return &ClockPolicy[K, V]{items: items}
}

func (p *ClockPolicy[K, V]) Add(node itemstore.QNode) {
	p.q.Push(node)
}

func (p *ClockPolicy[K, V]) Len() int { return p.q.Len() }

func (p *ClockPolicy[K, V]) Bytes() int64 { return int64(unsafe.Sizeof(*p)) + p.q.Bytes() }

func (p *ClockPolicy[K, V]) Sweep(limit int, parity bool) bool {
	if !p.q.Sweeping() {
		p.q.BeginSweep()
	}
	return itemstore.SweepStep(&p.q, p.items, limit, parity, nil)
}

// Evict pops the head until it finds a victim. Every candidate is resolved exactly: examining an item changes its
// reference counter, and that must never land on a neighbor.
func (p *ClockPolicy[K, V]) Evict(nowOff uint32) (uint32, bool) {
	// Finite: each step removes a node for good or spends one of its at most two chances.
	for {
		candidate, ok := p.q.Pop()
		if !ok {
			return 0, false
		}
		item, idx, found := p.items.Find(candidate)
		if !found {
			continue // the key left the cache; its node goes with it
		}
		if metaWord := item.Metadata(); chanced(metaWord, nowOff) {
			item.RevokeChance(metaWord)
			p.q.Push(candidate)
			continue
		}
		return idx, true
	}
}
