package eviction

import (
	"unsafe"

	"github.com/zakonnic/memstash/internal/itemstate"
)

// ClockPolicy is GCLOCK: a single FIFO queue where a node with a non-zero reference counter loses one chance and
// moves to the tail, and a node with a zero counter is evicted.
type ClockPolicy[K comparable, V any] struct {
	pool *itemstate.Pool[K, V]
	q    itemstate.EvictQueue
}

// NewClockPolicy creates a ClockPolicy policy.
func NewClockPolicy[K comparable, V any](pool *itemstate.Pool[K, V]) *ClockPolicy[K, V] {
	return &ClockPolicy[K, V]{pool: pool}
}

func (p *ClockPolicy[K, V]) Add(node itemstate.QNode) {
	p.q.Push(node)
}

func (p *ClockPolicy[K, V]) Len() int { return p.q.Len() }

func (p *ClockPolicy[K, V]) Bytes() int64 { return int64(unsafe.Sizeof(*p)) + p.q.Bytes() }

func (p *ClockPolicy[K, V]) Sweep(release func(idx uint32)) {
	itemstate.SweepQueue(&p.q, p.pool, func(node itemstate.QNode) { release(node.Idx) })
}

func (p *ClockPolicy[K, V]) Range(f func(itemstate.QNode)) { p.q.Range(f) }

func (p *ClockPolicy[K, V]) Evict(nowOff uint32) (uint32, bool) {
	// Finite: each step removes a node for good or spends one of its at most two chances.
	for {
		candidate, ok := p.q.Pop()
		if !ok {
			return 0, false
		}
		state := p.pool.At(candidate.Idx)
		metaWord := state.Load()
		switch {
		case metaWord&itemstate.Dead != 0:
			return candidate.Idx, true // died earlier (Delete/TTL); weight already subtracted
		case itemstate.Expired(metaWord, nowOff):
			state.Kill()
			return candidate.Idx, true
		case metaWord&itemstate.ChanceMask != 0:
			state.RevokeChance(metaWord)
			p.q.Push(candidate)
		default:
			state.Kill()
			return candidate.Idx, true
		}
	}
}
