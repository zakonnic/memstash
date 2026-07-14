package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
)

func TestBatchDeleteWriteThrough(t *testing.T) {
	ctx := context.Background()
	l2 := newL2Stub()
	c := newCache(t, memstash.Config[string, string]{
		MemoryCapacity: 100, L2Cache: l2, WritePolicy: memstash.WriteThrough,
	})

	items := memstash.List[string, string]{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}, {Key: "c", Value: "3"}}
	require.NoError(t, c.BatchSet(ctx, items))
	require.NoError(t, c.BatchDelete(ctx, []string{"a", "b"}))

	for _, key := range []string{"a", "b"} {
		_, ok := c.GetFromMemory(key)
		assert.False(t, ok, "key %s survived BatchDelete in memory", key)
		_, ok = l2.snapshot(key)
		assert.False(t, ok, "key %s survived BatchDelete in L2", key)
	}
	v, ok := l2.snapshot("c")
	require.True(t, ok, "BatchDelete removed a key it was not asked to")
	assert.Equal(t, "3", v)
}

func TestBatchDeleteWriteDisabledLeavesL2(t *testing.T) {
	ctx := context.Background()
	l2 := newL2Stub()
	require.NoError(t, l2.Set(ctx, "a", "1", 0))
	c := newCache(t, memstash.Config[string, string]{
		MemoryCapacity: 100, L2Cache: l2, WritePolicy: memstash.WriteDisabled,
	})

	require.NoError(t, c.BatchDelete(ctx, []string{"a"}))
	_, ok := l2.snapshot("a")
	assert.True(t, ok, "WriteDisabled BatchDelete must not touch L2")
}

// TestBatchDeleteWriteBack drives the write-back worker through a queue of sets and deletes: the deletes drain as
// one BatchDelete, and the set/delete order of the queue is preserved per key.
func TestBatchDeleteWriteBack(t *testing.T) {
	ctx := context.Background()
	l2 := newGatedL2()
	c := newWriteBackCache(t, l2)

	stall(t, c, l2)
	require.NoError(t, c.Set(ctx, "a", "v"))
	require.NoError(t, c.Set(ctx, "b", "v"))
	require.NoError(t, c.BatchDelete(ctx, []string{"a", "b"}))
	require.NoError(t, c.Set(ctx, "c", "v"))

	l2.release <- struct{}{} // the stalled single completes
	l2.pass()                // run 1: BatchSet{a, b}
	l2.pass()                // run 2: BatchDelete{a, b}
	l2.pass()                // run 3: Set{c}
	c.Wait()

	sets, batches, sizes := l2.counters()
	assert.Equal(t, 2, sets, "the stalled first write and the trailing single set")
	assert.Equal(t, 1, batches)
	assert.Equal(t, []int{2}, sizes)

	deletes, deleteBatches, deleteSizes := l2.deleteCounters()
	assert.Equal(t, 0, deletes)
	assert.Equal(t, 1, deleteBatches, "queued deletes must drain as one BatchDelete")
	assert.Equal(t, []int{2}, deleteSizes)

	l2.mu.Lock()
	_, aOk := l2.m["a"]
	_, bOk := l2.m["b"]
	_, cOk := l2.m["c"]
	l2.mu.Unlock()
	assert.False(t, aOk, "the delete queued after the set must win")
	assert.False(t, bOk, "the delete queued after the set must win")
	assert.True(t, cOk)
}
