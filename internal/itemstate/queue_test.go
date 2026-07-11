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
	var p Pool[int]
	var q EvictQueue
	for i := 0; i < 10; i++ {
		_, _, idx := p.Claim(i, 0)
		q.Push(QNode{Idx: idx})
		if i%2 == 0 {
			p.At(idx).Kill()
		}
	}

	var dropped []int
	SweepQueue(&q, &p, func(node QNode) { dropped = append(dropped, p.At(node.Idx).Key) })
	assert.Equal(t, []int{0, 2, 4, 6, 8}, dropped, "every tombstoned node is dropped")

	survivors := make([]int, 0, q.Len())
	for {
		node, ok := q.Pop()
		if !ok {
			break
		}
		survivors = append(survivors, p.At(node.Idx).Key)
	}
	assert.Equal(t, []int{1, 3, 5, 7, 9}, survivors, "live nodes keep their FIFO order")
}
