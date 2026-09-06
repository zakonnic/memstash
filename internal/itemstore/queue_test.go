package itemstore

import (
	"hash/maphash"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEvictQueueBytes verifies the queue's byte accounting through a full grow/drain cycle: one chunk per
// queueChunkSize pushes, and on drain the first consumed chunk stays as the spare while the last one is reused from
// the beginning (see Pop's doc comment).
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
	assert.Equal(t, 2*chunkBytes, q.Bytes(), "the drained chunk is kept as the spare, the last one for reuse")
}

// TestSweepStep verifies that a sweep pass drops exactly the nodes whose keys are gone, preserves the FIFO order of
// the survivors, hands every dropped node to onDrop, and spreads over calls when given a budget.
func TestSweepStep(t *testing.T) {
	m := newModel(64, 11)
	var items StorageProxy[uint64, uint64]
	items.Store(m.t)
	var q EvictQueue
	var scratch []ShiftEntry
	for key := uint64(1); key <= 10; key++ {
		m.insert(key, key)
		q.Push(NewQNode(m.hash(key), 1))
	}
	for key := uint64(2); key <= 10; key += 2 {
		m.remove(key, &scratch)
	}

	var dropped []uint64
	onDrop := func(node QNode) {
		for key := uint64(1); key <= 10; key++ {
			if NewQNode(m.hash(key), 1) == node {
				dropped = append(dropped, key)
			}
		}
	}
	// A budget of 3 needs four calls for ten nodes; the pass reports more work until the last one.
	q.BeginSweep()
	calls := 0
	for more := true; more; calls++ {
		more = SweepStep(&q, &items, 3, true, onDrop)
	}
	assert.Equal(t, 4, calls, "ten nodes at three per call")
	assert.Equal(t, []uint64{2, 4, 6, 8, 10}, dropped, "every node of a gone key is dropped, in order")

	survivors := make([]uint64, 0, q.Len())
	for {
		node, ok := q.Pop()
		if !ok {
			break
		}
		item, _, found := items.Find(node)
		require.True(t, found)
		survivors = append(survivors, item.Entry().Key)
	}
	assert.Equal(t, []uint64{1, 3, 5, 7, 9}, survivors, "live nodes keep their FIFO order")
}

// TestEvictQueueRange verifies that Range visits every queued node in FIFO order without disturbing the queue,
// across chunk boundaries and after partial drains.
func TestEvictQueueRange(t *testing.T) {
	var q EvictQueue
	total := queueChunkSize + 10
	for i := 0; i < total; i++ {
		q.Push(NewQNode(uint64(i)<<32, 1))
	}
	for i := 0; i < 5; i++ { // shift the head into the chunk
		_, ok := q.Pop()
		assert.True(t, ok)
	}

	var seen []uint32
	q.Range(func(node QNode) { seen = append(seen, node.home) })
	assert.Len(t, seen, total-5)
	assert.EqualValues(t, 5, seen[0], "Range must start at the queue head")
	assert.EqualValues(t, total-1, seen[len(seen)-1], "Range must end at the queue tail")
	assert.Equal(t, total-5, q.Len(), "Range must not consume the queue")
}

// TestFindResolvesByIdentity pins Find: a node resolves its own key wherever the item sits, and a node of a key that
// left resolves nothing - even when another key with the same tag byte sits on its walk.
func TestFindResolvesByIdentity(t *testing.T) {
	m := newModel(64, 5)
	var scratch []ShiftEntry
	for key := uint64(1); key <= 40; key++ {
		m.insert(key, key)
	}
	for key := uint64(1); key <= 40; key++ {
		item, _, ok := m.t.Find(NewQNode(m.hash(key), 1))
		require.True(t, ok, "key %d", key)
		require.Equal(t, key, item.Entry().Key)
	}
	for key := uint64(1); key <= 40; key += 3 {
		m.remove(key, &scratch)
		_, _, ok := m.t.Find(NewQNode(m.hash(key), 1))
		require.False(t, ok, "key %d is gone but Find still resolves its node", key)
	}
	m.check(t)
	_ = maphash.MakeSeed
}
