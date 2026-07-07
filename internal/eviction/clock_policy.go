package eviction

import "github.com/zakonnic/memstash/internal/itemstate"

// ClockPolicy is GCLOCK: a single FIFO queue. When the hand passes a node whose reference counter is non-zero, the node
// loses one chance and is moved to the tail; a node with a zero counter is evicted.
type ClockPolicy[K comparable] struct {
	q itemstate.EvictQueue[K]
}

// NewClockPolicy creates a ClockPolicy policy.
func NewClockPolicy[K comparable]() *ClockPolicy[K] {
	return &ClockPolicy[K]{}
}

func (p *ClockPolicy[K]) Add(item itemstate.QNode[K]) {
	p.q.Push(item)
}

func (p *ClockPolicy[K]) Len() int { return p.q.Len() }

func (p *ClockPolicy[K]) Bytes() int64 { return p.q.Bytes() }

func (p *ClockPolicy[K]) Sweep(release func(*itemstate.State[K])) {
	p.q.Sweep(func(node itemstate.QNode[K]) { release(node.State) })
}

func (p *ClockPolicy[K]) Evict(nowOff uint32) (*itemstate.State[K], bool) {
	// The loop is finite: each step either removes a node from the queue for good or decrements the chance counter of
	// its item (at most 2 per item).
	for {
		candidate, ok := p.q.Pop()
		if !ok {
			return nil, false
		}
		metaWord := candidate.State.Load()
		switch {
		case metaWord&itemstate.Dead != 0:
			// Died earlier (Delete/TTL): its weight is already subtracted, the record goes back to the pool.
			return candidate.State, true
		case itemstate.Expired(metaWord, nowOff):
			candidate.State.Kill()
			return candidate.State, true
		case metaWord&itemstate.ChanceMask != 0:
			candidate.State.RevokeChance(metaWord)
			p.q.Push(candidate)
		default:
			candidate.State.Kill()
			return candidate.State, true
		}
	}
}
