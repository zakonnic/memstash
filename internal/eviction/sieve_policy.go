package eviction

import (
	"unsafe"

	"github.com/zakonnic/memstash/internal/itemstore"
)

// noLink marks an absent list neighbor / an unset hand.
const noLink int32 = -1

// sieveNode is one element of the SIEVE list: the queue node plus doubly-linked neighbors. prev points toward the
// newer end (head), next toward the older end (tail); next doubles as the freelist link for removed slots.
type sieveNode struct {
	item       itemstore.QNode
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
	slots *itemstore.StorageProxy[K, V]
	nodes []sieveNode
	free  int32 // freelist head through sieveNode.next; noLink when empty
	head  int32 // newest item; noLink when the list is empty
	tail  int32 // oldest item
	hand  int32 // next scan position; noLink wraps to tail
	sweep int32 // where the running sweep pass resumes; noLink between passes
	size  int
}

// NewSieve creates a SIEVE policy.
func NewSieve[K comparable, V any](slots *itemstore.StorageProxy[K, V]) *SievePolicy[K, V] {
	return &SievePolicy[K, V]{slots: slots, free: noLink, head: noLink, tail: noLink, hand: noLink, sweep: noLink}
}

func (p *SievePolicy[K, V]) Add(node itemstore.QNode) {
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
		item, idx, found := p.slots.Find(node.item)
		if !found {
			p.removeAt(cur, node) // the key left the cache; its node goes with it
			continue
		}
		if chanced(item.Metadata(), nowOff) {
			item.ResetChances()
			p.hand = node.prev // survived in place; the hand moves on toward the newer end
			continue
		}
		p.removeAt(cur, node)
		return idx, true
	}
	return 0, false
}

// removeAt advances the hand past cur and unlinks it.
func (p *SievePolicy[K, V]) removeAt(cur int32, node *sieveNode) {
	p.hand = node.prev
	p.unlink(cur, node)
}

// Sweep walks up to limit nodes from where the last call stopped (the head on a fresh pass) toward the tail, dropping
// the nodes whose keys are gone; eviction keeps the cursor valid when it unlinks the node under it (see unlink).
func (p *SievePolicy[K, V]) Sweep(limit int) bool {
	if p.sweep == noLink {
		p.sweep = p.head
	}
	for ; limit > 0 && p.sweep != noLink; limit-- {
		cur := p.sweep
		node := &p.nodes[cur]
		p.sweep = node.next
		if _, _, found := p.slots.Find(node.item); !found {
			if p.hand == cur {
				// The walk goes head to tail, so prev nodes are already swept: the hand lands on a live node.
				p.hand = node.prev
			}
			p.unlink(cur, node)
		}
	}
	return p.sweep != noLink
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

// unlink removes cur from the list and pushes its slot onto the freelist. The caller keeps the hand off cur; the
// sweep cursor steps over it here.
func (p *SievePolicy[K, V]) unlink(cur int32, node *sieveNode) {
	if p.sweep == cur {
		p.sweep = node.next
	}
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
	node.item = itemstore.QNode{}
	node.next = p.free
	p.free = cur
	p.size--
}
