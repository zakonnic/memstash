package eviction

import (
	"unsafe"

	"github.com/zakonnic/memstash/internal/itemstate"
)

// ClockPolicy is GCLOCK: a single FIFO queue. When the hand passes a node whose reference counter is non-zero, the node
// loses one chance and is moved to the tail; a node with a zero counter is evicted.
type ClockPolicy[K comparable] struct {
	pool *itemstate.Pool[K]
	q    itemstate.EvictQueue
}

// NewClockPolicy creates a ClockPolicy policy.
func NewClockPolicy[K comparable](pool *itemstate.Pool[K]) *ClockPolicy[K] {
	return &ClockPolicy[K]{pool: pool}
}

func (p *ClockPolicy[K]) Add(node itemstate.QNode) {
	p.q.Push(node)
}

func (p *ClockPolicy[K]) Len() int { return p.q.Len() }

func (p *ClockPolicy[K]) Bytes() int64 { return int64(unsafe.Sizeof(*p)) + p.q.Bytes() }

func (p *ClockPolicy[K]) Sweep(release func(idx uint32)) {
	itemstate.SweepQueue(&p.q, p.pool, func(node itemstate.QNode) { release(node.Idx) })
}

func (p *ClockPolicy[K]) Evict(nowOff uint32) (uint32, bool) {
	// The loop is finite: each step either removes a node from the queue for good or decrements the chance counter of
	// its item (at most 2 per item).
	for {
		candidate, ok := p.q.Pop()
		if !ok {
			return 0, false
		}
		state := p.pool.At(candidate.Idx)
		metaWord := state.Load()
		switch {
		case metaWord&itemstate.Dead != 0:
			// Died earlier (Delete/TTL): its weight is already subtracted, the record goes back to the pool.
			return candidate.Idx, true
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
