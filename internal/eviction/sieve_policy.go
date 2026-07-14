package eviction

import (
	"unsafe"

	"github.com/zakonnic/memstash/internal/itemstate"
)

// noLink marks an absent list neighbor / an unset hand.
const noLink int32 = -1

// sieveNode is one element of the SIEVE list: the queue node plus doubly-linked neighbors. prev points toward the
// newer end (head), next toward the older end (tail); next doubles as the freelist link for removed slots.
type sieveNode struct {
	item       itemstate.QNode
	prev, next int32
}

// SievePolicy implements SIEVE (NSDI'24): a single insertion-ordered list with a hand that scans from the oldest
// item toward the newest. A visited item (a non-zero reference counter) survives in place with its counter cleared;
// an unvisited one is evicted where it stands. Unlike GCLOCK nothing is re-queued, so retained items keep their
// insertion order - new items land between the hand and the head and are examined (and, if unused, evicted) first,
// which gives SIEVE its quick demotion of one-hit wonders at plain FIFO cost.
//
// The list lives in one slice of 16-byte nodes with an intrusive freelist, so eviction's mid-list removal is O(1)
// and allocations amortize to one slice growth per capacity doubling.
type SievePolicy[K comparable, V any] struct {
	pool  *itemstate.Pool[K, V]
	nodes []sieveNode
	free  int32 // freelist head through sieveNode.next; noLink when empty
	head  int32 // newest item; noLink when the list is empty
	tail  int32 // oldest item
	hand  int32 // next scan position; noLink wraps to tail
	size  int
}

// NewSieve creates a SIEVE policy.
func NewSieve[K comparable, V any](pool *itemstate.Pool[K, V]) *SievePolicy[K, V] {
	return &SievePolicy[K, V]{pool: pool, free: noLink, head: noLink, tail: noLink, hand: noLink}
}

func (p *SievePolicy[K, V]) Add(node itemstate.QNode) {
	cur := p.alloc()
	p.nodes[cur] = sieveNode{item: node, prev: noLink, next: p.head}
	if p.head != noLink {
		p.nodes[p.head].prev = cur
	} else {
		p.tail = cur
	}
	p.head = cur
	p.size++
}

func (p *SievePolicy[K, V]) Len() int { return p.size }

func (p *SievePolicy[K, V]) Bytes() int64 {
	return int64(unsafe.Sizeof(*p)) + int64(cap(p.nodes))*int64(unsafe.Sizeof(sieveNode{}))
}

func (p *SievePolicy[K, V]) Evict(nowOff uint32) (uint32, bool) {
	// Finite as GCLOCK's loop is: each step removes a node for good or clears its reference counter.
	for p.size > 0 {
		cur := p.hand
		if cur == noLink {
			cur = p.tail // wrap: resume from the oldest item
		}
		node := &p.nodes[cur]
		state := p.pool.At(node.item.Idx)
		metaWord := state.Load()
		switch {
		case metaWord&itemstate.Dead != 0:
			return p.removeAt(cur, node), true // died earlier (Delete/TTL); weight already subtracted
		case itemstate.Expired(metaWord, nowOff):
			state.Kill()
			return p.removeAt(cur, node), true
		case metaWord&itemstate.ChanceMask != 0:
			state.ResetChances()
			p.hand = node.prev // survived in place; the hand moves on toward the newer end
		default:
			state.Kill()
			return p.removeAt(cur, node), true
		}
	}
	return 0, false
}

// removeAt advances the hand past cur, unlinks it and returns its record's pool index.
func (p *SievePolicy[K, V]) removeAt(cur int32, node *sieveNode) uint32 {
	idx := node.item.Idx
	p.hand = node.prev
	p.unlink(cur, node)
	return idx
}

func (p *SievePolicy[K, V]) Sweep(release func(idx uint32)) {
	for cur := p.head; cur != noLink; {
		node := &p.nodes[cur]
		next := node.next
		if p.pool.At(node.item.Idx).Load()&itemstate.Dead != 0 {
			if p.hand == cur {
				// The walk goes head to tail, so prev nodes are already swept: the hand lands on a live node.
				p.hand = node.prev
			}
			release(node.item.Idx)
			p.unlink(cur, node)
		}
		cur = next
	}
}

func (p *SievePolicy[K, V]) Range(f func(itemstate.QNode)) {
	// Oldest first, like the queue-based policies: table rebuilds insert in Range order, so ranging the long-lived
	// (usually hot) items first gives them the short probe chains.
	for cur := p.tail; cur != noLink; cur = p.nodes[cur].prev {
		f(p.nodes[cur].item)
	}
}

// alloc hands out a node slot: from the freelist, or by growing the slice.
func (p *SievePolicy[K, V]) alloc() int32 {
	if p.free != noLink {
		cur := p.free
		p.free = p.nodes[cur].next
		return cur
	}
	p.nodes = append(p.nodes, sieveNode{})
	return int32(len(p.nodes) - 1)
}

// unlink removes cur from the list and pushes its slot onto the freelist. The caller keeps the hand off cur.
func (p *SievePolicy[K, V]) unlink(cur int32, node *sieveNode) {
	if node.prev != noLink {
		p.nodes[node.prev].next = node.next
	} else {
		p.head = node.next
	}
	if node.next != noLink {
		p.nodes[node.next].prev = node.prev
	} else {
		p.tail = node.prev
	}
	node.item = itemstate.QNode{}
	node.next = p.free
	p.free = cur
	p.size--
}
