package itemstate

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEvictQueueBytes verifies the queue's byte accounting through a full grow/drain cycle: one chunk per
// queueChunkSize pushes, and on drain every fully consumed chunk is detached except the last, which Pop keeps around
// for reuse (see Pop's doc comment).
func TestEvictQueueBytes(t *testing.T) {
	var q EvictQueue
	assert.Zero(t, q.Bytes(), "an empty queue has no chunks")

	chunkBytes := int64(unsafe.Sizeof(queueChunk{}))

	for i := 0; i < queueChunkSize; i++ {
		q.Push(QNode{})
	}
	assert.Equal(t, chunkBytes, q.Bytes(), "exactly one chunk after queueChunkSize pushes")

	q.Push(QNode{})
	assert.Equal(t, 2*chunkBytes, q.Bytes(), "a push beyond queueChunkSize allocates a second chunk")

	for i := 0; i < queueChunkSize+1; i++ {
		_, ok := q.Pop()
		require.True(t, ok)
	}
	assert.Equal(t, chunkBytes, q.Bytes(), "a fully drained chunk is detached, but the last one is kept for reuse")
}

// TestSweepQueue verifies that SweepQueue drops exactly the tombstoned nodes, preserves the FIFO order of the
// survivors, and hands every dropped node to onDrop.
func TestSweepQueue(t *testing.T) {
	var p Pool[int, int]
	var q EvictQueue
	for i := 0; i < 10; i++ {
		_, _, idx := p.Claim(i, i, 0)
		q.Push(QNode{Idx: idx})
		if i%2 == 0 {
			p.At(idx).Kill()
		}
	}

	var dropped []int
	SweepQueue(&q, &p, func(node QNode) { dropped = append(dropped, p.At(node.Idx).Entry().Key) })
	assert.Equal(t, []int{0, 2, 4, 6, 8}, dropped, "every tombstoned node is dropped")

	survivors := make([]int, 0, q.Len())
	for {
		node, ok := q.Pop()
		if !ok {
			break
		}
		survivors = append(survivors, p.At(node.Idx).Entry().Key)
	}
	assert.Equal(t, []int{1, 3, 5, 7, 9}, survivors, "live nodes keep their FIFO order")
}

// TestEvictQueueRange verifies that Range visits every queued node in FIFO order without disturbing the queue,
// across chunk boundaries and after partial drains.
func TestEvictQueueRange(t *testing.T) {
	var q EvictQueue
	total := queueChunkSize + 10
	for i := 0; i < total; i++ {
		q.Push(QNode{Idx: uint32(i)})
	}
	for i := 0; i < 5; i++ { // shift the head into the chunk
		_, ok := q.Pop()
		assert.True(t, ok)
	}

	var seen []uint32
	q.Range(func(node QNode) { seen = append(seen, node.Idx) })
	assert.Len(t, seen, total-5)
	assert.EqualValues(t, 5, seen[0], "Range must start at the queue head")
	assert.EqualValues(t, total-1, seen[len(seen)-1], "Range must end at the queue tail")
	assert.Equal(t, total-5, q.Len(), "Range must not consume the queue")
}
