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

func (p *ClockPolicy[K, V]) Sweep(release func(idx uint32)) {
	itemstore.SweepQueue(&p.q, p.items.GetStorage(), func(node itemstore.QNode) { release(node.Idx) })
}

func (p *ClockPolicy[K, V]) Rebuild(remap func(oldIdx uint32) (uint32, bool)) {
	itemstore.RebuildQueue(&p.q, remap, nil)
}

func (p *ClockPolicy[K, V]) Evict(nowOff uint32) (uint32, bool) {
	// Finite: each step removes a node for good or spends one of its at most two chances.
	for {
		candidate, ok := p.q.Pop()
		if !ok {
			return 0, false
		}
		item := p.items.At(candidate.Idx)
		metaWord := item.Load()
		switch {
		case metaWord&itemstore.Dead != 0 || itemstore.Expired(metaWord, nowOff):
			return candidate.Idx, true
		case metaWord&itemstore.ChanceMask != 0:
			item.RevokeChance(metaWord)
			p.q.Push(candidate)
		default:
			return candidate.Idx, true
		}
	}
}
