package itemstate

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEvictQueueBytes verifies the queue's byte accounting through a full grow/drain cycle: one chunk per
// chunkSize pushes, and on drain every fully consumed chunk is detached except the last, which Pop keeps around
// for reuse (see Pop's doc comment).
func TestEvictQueueBytes(t *testing.T) {
	var q EvictQueue[string]
	assert.Zero(t, q.Bytes(), "an empty queue has no chunks")

	chunkBytes := int64(unsafe.Sizeof(queueChunk[string]{}))

	for i := 0; i < chunkSize; i++ {
		q.Push(QNode[string]{})
	}
	assert.Equal(t, chunkBytes, q.Bytes(), "exactly one chunk after chunkSize pushes")

	q.Push(QNode[string]{})
	assert.Equal(t, 2*chunkBytes, q.Bytes(), "a push beyond chunkSize allocates a second chunk")

	for i := 0; i < chunkSize+1; i++ {
		_, ok := q.Pop()
		require.True(t, ok)
	}
	assert.Equal(t, chunkBytes, q.Bytes(), "a fully drained chunk is detached, but the last one is kept for reuse")
}
