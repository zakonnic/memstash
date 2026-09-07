package itemstore

import "unsafe"

// queueChunkSize is the number of nodes in a single queue chunk: 127 8-byte nodes plus the next pointer make the
// chunk exactly 1 KiB.
const queueChunkSize = 127

// QNode is an eviction-queue element: a claim on a key, not on a slot. It carries the 40 hash bits Find needs to
// locate the item again - the 32 bits Home scales into the slot range and the 8 bits a probe compares first - plus the
// item's weight when it was enqueued. Items may move between slots (deletion shifts their neighbors toward home),
// so a node never remembers a position; it is resolved through Items.Find whenever the policy needs the item.
//
// The weight is kept to 20 bits with a small floating-point encoding: exact below 128 Ki, 2^-14 relative error above.
// It only balances S3-FIFO's small queue and W-TinyLFU's window, so that precision is plenty; it may drift after an
// overwrite anyway - the global accounting is recomputed from the live value.
type QNode struct {
	home    uint32 // keyHash >> 32, the input of FlatHashMap.Home
	tagCost uint32 // keyHash bits 14..25 in the top twelve bits - the tag as the meta word keeps it - the encoded weight below
}

// nodeTagShift is where the tag sits in tagCost; the weight has the bits below it.
const nodeTagShift = 20

// NewQNode builds the node for an item with the given key hash and weight.
func NewQNode(keyHash uint64, cost uint32) QNode {
	return QNode{home: uint32(keyHash >> 32), tagCost: uint32(keyHash>>14)&(1<<12-1)<<nodeTagShift | encodeCost(cost)}
}

// Cost is the item's weight when it was enqueued, decoded.
func (n QNode) Cost() uint32 { return decodeCost(n.tagCost & (1<<nodeTagShift - 1)) }

// Hash lays the node's 40 identity bits out where the key hash carried them, so a frequency sketch or a ghost table
// keyed by the hash sees the same key across the item's residencies.
func (n QNode) Hash() uint64 { return uint64(n.home)<<32 | uint64(n.tag12())<<14 }

func (n QNode) tag12() uint32 { return n.tagCost >> nodeTagShift }

// tagBits is the node's tag as the meta word carries it: what TagOf gives for the node's key.
func (n QNode) tagBits() uint64 { return uint64(n.tag12())<<TagShift | TagForced }

// bucket is the node's OverflowBucket: the low three tag bits.
func (n QNode) bucket() uint32 { return n.tag12() & (overflowBuckets - 1) }

// The weight encoding: 3 exponent bits over a 17-bit mantissa, the value being mantissa << 3*exponent.
const (
	costMantissaBits = 17
	costMantissaMax  = 1<<costMantissaBits - 1
)

func encodeCost(cost uint32) uint32 {
	exp := uint32(0)
	for cost>>(3*exp) > costMantissaMax {
		exp++
	}
	return exp<<costMantissaBits | cost>>(3*exp)
}

func decodeCost(encoded uint32) uint32 {
	exp := encoded >> costMantissaBits
	return (encoded & costMantissaMax) << (3 * exp)
}

// queueChunk is a single link of the unrolled queue.
type queueChunk struct {
	items [queueChunkSize]QNode
	next  *queueChunk
}

// EvictQueue is a FIFO queue built on chunks. Nodes whose keys have left the cache are not spliced out - they are
// lazily skipped during eviction, or dropped by SweepQueue. Not thread-safe: guard with the shard mutex.
type EvictQueue struct {
	head      *queueChunk
	tail      *queueChunk
	spare     *queueChunk // the last chunk the head drained, kept for the next one the tail needs
	headIdx   int
	tailIdx   int
	size      int
	sweepLeft int // nodes of the running sweep pass still to examine; 0 between passes
}

// Len returns the number of nodes currently queued.
func (q *EvictQueue) Len() int { return q.size }

// Bytes returns the footprint of the chunks the queue holds, the spare one included.
func (q *EvictQueue) Bytes() int64 {
	var chunks int64
	for c := q.head; c != nil; c = c.next {
		chunks++
	}
	if q.spare != nil {
		chunks++
	}
	return chunks * int64(unsafe.Sizeof(queueChunk{}))
}

// Push appends a node to the tail, taking a fresh chunk when the current tail is full: the one the head drained
// last, if any, so that a rotation - a re-queue, a sweep - allocates nothing.
func (q *EvictQueue) Push(item QNode) {
	if q.tail == nil {
		newChunk := q.freshChunk()
		q.head, q.tail = newChunk, newChunk
	} else if q.tailIdx == queueChunkSize {
		newChunk := q.freshChunk()
		q.tail.next = newChunk
		q.tail = newChunk
		q.tailIdx = 0
	}
	q.tail.items[q.tailIdx] = item
	q.tailIdx++
	q.size++
}

// freshChunk hands out the spare chunk (Pop zeroed its nodes on the way out) or allocates one.
func (q *EvictQueue) freshChunk() *queueChunk {
	if c := q.spare; c != nil {
		q.spare = nil
		c.next = nil
		return c
	}
	return &queueChunk{}
}

// BeginSweep starts a sweep pass over the nodes queued right now; SweepStep works through it.
func (q *EvictQueue) BeginSweep() { q.sweepLeft = q.size }

// Sweeping reports whether the current sweep pass has nodes left.
func (q *EvictQueue) Sweeping() bool { return q.sweepLeft > 0 }

// SweepStep is one slice of the sweep pass BeginSweep started: it examines up to limit nodes from the head and pushes
// back the one node each live item is entitled to, dropping the rest - the nodes whose keys left the cache, and the
// duplicates a delete-then-set of one key leaves behind. The item carries which pass has accounted for it (Queued),
// so parity must be the same for every call of one pass and the opposite of the last one's.
//
// A pass covers the nodes queued when it began; it is spread over calls so that no single write stalls on a queue of
// millions, at the price of the nodes pushed meanwhile ending up a few places ahead of the survivors rotated after
// them. Returns whether the pass has nodes left. onDrop, when not nil, sees every dropped node.
func SweepStep[K comparable, V any](q *EvictQueue, items *StorageProxy[K, V], limit int, parity bool,
	onDrop func(QNode)) bool {
	q.sweepLeft = min(q.sweepLeft, q.size) // evictions may have popped part of the pass already
	for ; limit > 0 && q.sweepLeft > 0; limit, q.sweepLeft = limit-1, q.sweepLeft-1 {
		node, ok := q.Pop()
		if !ok {
			q.sweepLeft = 0
			break
		}
		item, _, found := items.Find(node)
		if found && QueuedIn(item.Metadata(), parity) {
			// A node was kept for this item already - or for a twin of the node's own item, which Find cannot tell
			// apart. Dropping on a twin would leave the item without a node, so only an exact match is a duplicate.
			if item, _, found = items.FindExact(node); found && QueuedIn(item.Metadata(), parity) {
				found = false
			}
		}
		if !found {
			if onDrop != nil {
				onDrop(node)
			}
			continue
		}
		item.SetQueued(parity)
		q.Push(node)
	}
	return q.sweepLeft > 0
}

// Range calls f for every queued node in FIFO order without disturbing the queue.
func (q *EvictQueue) Range(f func(QNode)) {
	start := q.headIdx
	for chunk := q.head; chunk != nil; chunk = chunk.next {
		end := queueChunkSize
		if chunk == q.tail {
			end = q.tailIdx
		}
		for i := start; i < end; i++ {
			f(chunk.items[i])
		}
		start = 0
	}
}

// Peek returns the head node without removing it.
func (q *EvictQueue) Peek() (QNode, bool) {
	if q.size == 0 {
		return QNode{}, false
	}
	return q.head.items[q.headIdx], true
}

// Pop removes a node from the head. A fully consumed chunk becomes the spare for the next Push that needs one (one is
// kept; further ones go to the GC); a drained last chunk is reused from the beginning.
func (q *EvictQueue) Pop() (QNode, bool) {
	if q.size == 0 {
		return QNode{}, false
	}
	popped := q.head.items[q.headIdx]
	q.head.items[q.headIdx] = QNode{}
	q.headIdx++
	q.size--

	if q.headIdx == queueChunkSize {
		drained := q.head
		q.head = q.head.next
		q.headIdx = 0
		if q.head == nil {
			q.tail = nil
			q.tailIdx = 0
		}
		if q.spare == nil {
			q.spare = drained
		}
	}
	if q.size == 0 && q.head != nil {
		q.headIdx = 0
		q.tailIdx = 0
	}
	return popped, true
}
