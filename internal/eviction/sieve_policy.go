package eviction

import (
	"unsafe"

	"github.com/zakonnic/memstash/internal/itemstore"
)

// SievePolicy implements SIEVE (NSDI'24): one insertion-ordered queue and a hand that scans it from the oldest item
// toward the newest. A visited item, one with a non-zero reference counter, stays in place with its counter cleared;
// an unvisited one is evicted. Unlike GCLOCK nothing is re-queued, so retained items keep their insertion order. New
// items are examined, and evicted if unused, before the retained ones come up again: that is SIEVE's quick demotion
// of one-hit wonders at FIFO cost.
type SievePolicy[K comparable, V any] struct {
	items *itemstore.StorageProxy[K, V]
}

// NewSieve creates a SIEVE policy.
func NewSieve[K comparable, V any](items *itemstore.StorageProxy[K, V]) *SievePolicy[K, V] {
	return &SievePolicy[K, V]{items: items}
}

func (p *SievePolicy[K, V]) Add(itemstore.QNode)    {}
func (p *SievePolicy[K, V]) Remove(itemstore.QNode) {}
func (p *SievePolicy[K, V]) Reweigh(uint32, int64)  {}
func (p *SievePolicy[K, V]) Bytes() int64           { return int64(unsafe.Sizeof(*p)) }

func (p *SievePolicy[K, V]) Evict(nowOff uint32) (uint32, bool) {
	t := p.items.GetStorage()
	// Finite as GCLOCK's loop is: a step either returns or clears a reference counter.
	for {
		idx, ok := t.Hand()
		if !ok {
			return 0, false
		}
		item := t.At(idx)
		if !chanced(item.Metadata(), nowOff) {
			return idx, true // removing it moves the hand on
		}
		item.ResetChances()
		t.AdvanceHand()
	}
}
