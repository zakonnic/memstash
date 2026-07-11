package itemstate

import "unsafe"

// poolChunkSize is the number of state records in a single pool chunk. A chunk of 128 records at 16-24 bytes each
// occupies 2-3 KiB and is allocated as a single object: allocations and GC pressure are amortized to 1/128 of an
// operation.
const poolChunkSize = 128

// queueChunkSize is the number of nodes in a single queue chunk. 127 for optimization, total struct size becomes 1024.
const queueChunkSize = 127

// QNode is an eviction-queue element: the pool index of a stable state record plus the item's weight at the time it
// was enqueued. Indices instead of pointers keep the node at 8 bytes; the owning shard's Pool resolves them. The
// weight kept in the node lets S3-FIFO balance its small queue without touching the map; after a value is overwritten
// it may drift slightly, but that drift only affects queue selection - the global weight accounting is always exact
// (it is recomputed from the live value).
type QNode struct {
	Idx  uint32
	Cost uint32
}

// queueChunk is a single link of the unrolled queue.
type queueChunk struct {
	items [queueChunkSize]QNode
	next  *queueChunk
}

// EvictQueue is a FIFO queue built on chunks. NOT thread-safe: access exclusively under the shard mutex. Dead nodes
// are not spliced out - they are lazily skipped during eviction.
type EvictQueue struct {
	head    *queueChunk
	tail    *queueChunk
	headIdx int
	tailIdx int
	size    int
}

// Len returns the number of nodes currently queued.
func (q *EvictQueue) Len() int { return q.size }

// Bytes returns the memory footprint of the queue's currently allocated chunks (dead nodes included - a chunk is
// only released back to the GC once fully drained by Pop). Not thread-safe: call under the shard mutex, same as
// every other EvictQueue method.
func (q *EvictQueue) Bytes() int64 {
	var chunks int64
	for c := q.head; c != nil; c = c.next {
		chunks++
	}
	return chunks * int64(unsafe.Sizeof(queueChunk{}))
}

// Push appends a node to the tail, allocating a new chunk when the current tail is full.
func (q *EvictQueue) Push(item QNode) {
	if q.tail == nil {
		newChunk := &queueChunk{}
		q.head, q.tail = newChunk, newChunk
	} else if q.tailIdx == queueChunkSize {
		newChunk := &queueChunk{}
		q.tail.next = newChunk
		q.tail = newChunk
		q.tailIdx = 0
	}
	q.tail.items[q.tailIdx] = item
	q.tailIdx++
	q.size++
}

// SweepQueue rotates the queue exactly once, dropping the nodes of dead (tombstoned) items and handing each dropped
// node to onDrop; live nodes are pushed back, so their FIFO order and reference counters are fully preserved. This is
// the same reclamation the eviction pass performs lazily, just done in bulk: it exists so that tombstones from Delete
// and TTL expiry do not pile up when the cache stays below capacity and eviction never runs. The pool is needed to
// resolve node indices into state records. O(len) pops and pushes; fully drained chunks are released to the GC by Pop
// as usual. NOT thread-safe: call under the shard mutex.
func SweepQueue[K comparable](q *EvictQueue, pool *Pool[K], onDrop func(QNode)) {
	for n := q.size; n > 0; n-- {
		node, ok := q.Pop()
		if !ok {
			return
		}
		if pool.At(node.Idx).Load()&Dead != 0 {
			onDrop(node)
		} else {
			q.Push(node)
		}
	}
}

// Pop removes a node from the head. Fully consumed chunks are detached and handed to the GC as a whole; a drained
// last chunk is reused from the beginning.
func (q *EvictQueue) Pop() (QNode, bool) {
	if q.size == 0 {
		return QNode{}, false
	}
	popped := q.head.items[q.headIdx]
	q.head.items[q.headIdx] = QNode{}
	q.headIdx++
	q.size--

	if q.headIdx == queueChunkSize {
		q.head = q.head.next
		q.headIdx = 0
		if q.head == nil {
			q.tail = nil
			q.tailIdx = 0
		}
	}
	if q.size == 0 && q.head != nil {
		q.headIdx = 0
		q.tailIdx = 0
	}
	return popped, true
}
