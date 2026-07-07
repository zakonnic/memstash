package itemstate

import "unsafe"

// chunkSize is the number of elements in a single chunk (queue or pool). A chunk of 128 nodes at 16 bytes each
// occupies 2 KiB and is allocated as a single object: allocations and GC pressure are amortized to 1/128 of an
// operation.
const chunkSize = 128

// QNode is an eviction-queue element: a pointer to a stable state record plus the item's weight at the time it was
// enqueued. The weight kept in the node lets S3-FIFO balance its small queue without touching the map; after a value
// is overwritten it may drift slightly, but that drift only affects queue selection - the global weight accounting is
// always exact (it is recomputed from the live value).
type QNode[K comparable] struct {
	State *State[K]
	Cost  uint32
}

// queueChunk is a single link of the unrolled queue.
type queueChunk[K comparable] struct {
	items [chunkSize]QNode[K]
	next  *queueChunk[K]
}

// EvictQueue is a FIFO queue built on chunks. NOT thread-safe: access exclusively under the shard mutex. Dead nodes
// are not spliced out - they are lazily skipped during eviction.
type EvictQueue[K comparable] struct {
	head    *queueChunk[K]
	tail    *queueChunk[K]
	headIdx int
	tailIdx int
	size    int
}

// Len returns the number of nodes currently queued.
func (q *EvictQueue[K]) Len() int { return q.size }

// Bytes returns the memory footprint of the queue's currently allocated chunks (dead nodes included - a chunk is
// only released back to the GC once fully drained by Pop). Not thread-safe: call under the shard mutex, same as
// every other EvictQueue method.
func (q *EvictQueue[K]) Bytes() int64 {
	var chunks int64
	for c := q.head; c != nil; c = c.next {
		chunks++
	}
	return chunks * int64(unsafe.Sizeof(queueChunk[K]{}))
}

// Push appends a node to the tail, allocating a new chunk when the current tail is full.
func (q *EvictQueue[K]) Push(item QNode[K]) {
	if q.tail == nil {
		newChunk := &queueChunk[K]{}
		q.head, q.tail = newChunk, newChunk
	} else if q.tailIdx == chunkSize {
		newChunk := &queueChunk[K]{}
		q.tail.next = newChunk
		q.tail = newChunk
		q.tailIdx = 0
	}
	q.tail.items[q.tailIdx] = item
	q.tailIdx++
	q.size++
}

// Sweep rotates the queue exactly once, dropping the nodes of dead (tombstoned) items and handing each dropped node
// to onDrop; live nodes are pushed back, so their FIFO order and reference counters are fully preserved. This is the
// same reclamation the eviction pass performs lazily, just done in bulk: it exists so that tombstones from Delete and
// TTL expiry do not pile up when the cache stays below capacity and eviction never runs. O(len) pops and pushes;
// fully drained chunks are released to the GC by Pop as usual. NOT thread-safe: call under the shard mutex.
func (q *EvictQueue[K]) Sweep(onDrop func(QNode[K])) {
	for n := q.size; n > 0; n-- {
		node, ok := q.Pop()
		if !ok {
			return
		}
		if node.State.Load()&Dead != 0 {
			onDrop(node)
		} else {
			q.Push(node)
		}
	}
}

// Pop removes a node from the head. Fully consumed chunks are detached and handed to the GC as a whole; a drained
// last chunk is reused from the beginning.
func (q *EvictQueue[K]) Pop() (QNode[K], bool) {
	if q.size == 0 {
		return QNode[K]{}, false
	}
	popped := q.head.items[q.headIdx]
	q.head.items[q.headIdx] = QNode[K]{}
	q.headIdx++
	q.size--

	if q.headIdx == chunkSize {
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
