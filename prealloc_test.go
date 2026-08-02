package memstash

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func slotCounts[K comparable, V any](c *Cache[K, V]) []int {
	counts := make([]int, len(c.shards))
	for i := range c.shards {
		counts[i] = c.shards[i].items.GetStorage().Len()
	}
	return counts
}

// TestPreallocatedTableFillsWithoutRebuild fills both caches to capacity: the preallocated one must not touch its
// tables, and the one growing on demand must arrive at exactly the same size - preallocation buys away the rebuilds,
// not memory.
func TestPreallocatedTableFillsWithoutRebuild(t *testing.T) {
	ctx := context.Background()
	const capacity = 4096

	c, err := New[int, int](WithMemoryCapacity(capacity), WithShardsCount(4), WithPreallocatedSize())
	require.NoError(t, err)
	defer c.Close()

	initial := slotCounts(c)
	for i := range c.shards {
		require.Equal(t, preallocSlots(c.shards[i].cap), initial[i], "shard %d was not sized for its capacity", i)
	}
	for i := range capacity {
		require.NoError(t, c.Set(ctx, i, i))
	}
	assert.Equal(t, initial, slotCounts(c), "a preallocated table must survive the fill untouched")

	grown, err := New[int, int](WithMemoryCapacity(capacity), WithShardsCount(4))
	require.NoError(t, err)
	defer grown.Close()

	for i := range capacity {
		require.NoError(t, grown.Set(ctx, i, i))
	}
	assert.Equal(t, initial, slotCounts(grown), "preallocation must not hand out more slots than growth would")
}

// TestPreallocSlots pins the sizing rule: the smallest power of two that keeps cap+1 items under the 3/4 occupancy
// that trips a rebuild, never below the initial table.
func TestPreallocSlots(t *testing.T) {
	assert.Equal(t, minTableSlots, preallocSlots(1))
	assert.Equal(t, minTableSlots, preallocSlots(40))
	assert.Equal(t, 128, preallocSlots(60))
	assert.Equal(t, 2048, preallocSlots(1000))
	assert.Equal(t, 2048, preallocSlots(1024))
	// One shard of the 100M-entry memory benchmark, whose 32 shards must stay at 4Mi slots (~3 GiB of tables).
	assert.Equal(t, 1<<22, preallocSlots(100_000_000/32))
	assert.Equal(t, int(maxTableSlots), preallocSlots(3<<30), "the table is capped by the uint32 index space")

	for _, shardCap := range []int64{100, 1000, 1024, 65_536, 3_125_000} {
		slots := int64(preallocSlots(shardCap))
		assert.Less(t, (shardCap+1)*4, slots*3, "cap %d: a full shard must stay under the rebuild threshold", shardCap)
		assert.GreaterOrEqual(t, (shardCap+1)*4, slots*3/2,
			"cap %d: half that table would already do", shardCap)
	}
}
