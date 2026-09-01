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

// TestPreallocatedTableDoublesOnChurn: every eviction after fill leaves a tombstone, and live+dirty is exactly the
// number of slots ever touched. Capacity 12000 preallocates 16384 slots, so the fill already sits at 0.73 - a few
// hundred tombstones cross the 3/4 rebuild line, and maybeRebuild doubles instead of purging because live is over
// half the table.
func TestPreallocatedTableDoublesOnChurn(t *testing.T) {
	const capacity = 12000

	newCache := func(t *testing.T) (*Cache[int, int], int) {
		t.Helper()
		c, err := New[int, int](WithMemoryCapacity(capacity), WithShardsCount(1), WithPreallocatedSize())
		require.NoError(t, err)
		t.Cleanup(func() { c.Close() })

		prealloc := slotCounts(c)[0]
		require.Equal(t, preallocSlots(c.shards[0].cap), prealloc)
		return c, prealloc
	}

	// Steady state at capacity: each Set evicts one item, so live stays put and only the tombstone count grows.
	t.Run("eviction churn at capacity", func(t *testing.T) {
		ctx := context.Background()
		c, prealloc := newCache(t)

		for i := range capacity {
			require.NoError(t, c.Set(ctx, i, i))
		}
		require.Equal(t, prealloc, slotCounts(c)[0], "the fill alone must not rebuild")

		for i := capacity; i < capacity+capacity/32; i++ { // 375 sets, 375 evictions, 375 tombstones
			require.NoError(t, c.Set(ctx, i, i))
		}

		assert.Equal(t, prealloc, slotCounts(c)[0],
			"the preallocated table doubled after %d sets past a full cache: %d live items in %d slots is over half, "+
				"so the tombstone purge promoted itself to a growth rebuild", capacity/32, c.Len(), prealloc)
	})

	// Below capacity nothing is ever evicted, yet the same doubling arrives: Delete tombstones are touched slots too.
	t.Run("delete churn below capacity", func(t *testing.T) {
		ctx := context.Background()
		c, prealloc := newCache(t)

		const live = capacity - 1000 // stays under capacity for the whole run: the eviction path never fires
		for i := range live {
			require.NoError(t, c.Set(ctx, i, i))
		}
		require.Equal(t, prealloc, slotCounts(c)[0], "the fill alone must not rebuild")

		for i := live; i < live+capacity/8; i++ {
			require.NoError(t, c.Set(ctx, i, i))
			require.NoError(t, c.Delete(ctx, i-live))
		}
		require.Equal(t, live, c.Len(), "the item count must not have moved")

		assert.Equal(t, prealloc, slotCounts(c)[0],
			"the preallocated table doubled on a cache that never reached capacity: %d live items in %d slots",
			c.Len(), prealloc)
	})
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
