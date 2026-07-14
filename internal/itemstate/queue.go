package itemstate

import "unsafe"

// poolChunkSize is the number of state records in a single pool chunk; allocations are amortized to 1/128 of an
// insert.
const poolChunkSize = 128

// queueChunkSize is the number of nodes in a single queue chunk: 127 8-byte nodes plus the next pointer make the
// chunk exactly 1 KiB.
const queueChunkSize = 127

// QNode is an eviction-queue element: the pool index of a state record plus the item's weight when enqueued (the
// index instead of a pointer keeps the node at 8 bytes). The weight lets S3-FIFO balance its small queue without
// touching the record; it may drift after an overwrite, but only queue selection depends on it - the global
// accounting is recomputed from the live value.
type QNode struct {
	Idx  uint32
	Cost uint32
}

// queueChunk is a single link of the unrolled queue.
type queueChunk struct {
	items [queueChunkSize]QNode
	next  *queueChunk
}

// EvictQueue is a FIFO queue built on chunks. Dead nodes are not spliced out - they are lazily skipped during
// eviction. Not thread-safe: guard with the shard mutex.
type EvictQueue struct {
	head    *queueChunk
	tail    *queueChunk
	headIdx int
	tailIdx int
	size    int
}

// Len returns the number of nodes currently queued.
func (q *EvictQueue) Len() int { return q.size }

// Bytes returns the footprint of the currently allocated chunks (a chunk is only released once fully drained).
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

// SweepQueue rotates the queue once, handing the nodes of dead items to onDrop and pushing live ones back, so their
// FIFO order and reference counters are preserved. It reclaims in bulk the tombstones that eviction would otherwise
// only reach under capacity pressure. The pool resolves node indices.
func SweepQueue[K comparable, V any](q *EvictQueue, pool *Pool[K, V], onDrop func(QNode)) {
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

// Range calls f for every queued node in FIFO order without disturbing the queue. Every claimed record has exactly
// one node across its shard's queues, so ranging a policy's queues visits each record once - which is what table
// rebuilds rely on.
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

// Pop removes a node from the head. Fully consumed chunks are handed to the GC as a whole; a drained last chunk is
// reused from the beginning.
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
