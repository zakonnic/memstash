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

// TestPreallocatedTableFillsWithoutRebuild fills both caches to capacity and then churns them: the preallocated one
// must not touch its tables at any point, and the one growing on demand must arrive at exactly the same size -
// preallocation buys away the rebuilds, not memory.
func TestPreallocatedTableFillsWithoutRebuild(t *testing.T) {
	ctx := context.Background()
	const capacity = 4096

	c, err := New[int, int](WithMemoryCapacity(capacity), WithShardsCount(4), WithPreallocatedSize(), WithStats())
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

	for i := capacity; i < 20*capacity; i++ {
		require.NoError(t, c.Set(ctx, i, i))
	}
	assert.Equal(t, initial, slotCounts(c), "a preallocated table must survive eviction churn untouched")
	stats := c.Stats()
	assert.Zero(t, stats.TableGrowths(), "a preallocated table must never be enlarged")

	grown, err := New[int, int](WithMemoryCapacity(capacity), WithShardsCount(4))
	require.NoError(t, err)
	defer grown.Close()

	for i := range 20 * capacity {
		require.NoError(t, grown.Set(ctx, i, i))
	}
	assert.Equal(t, initial, slotCounts(grown), "preallocation must hand out exactly what growth settles at")
}

// TestPreallocatedTableDoublesOnChurn is the regression that forced preallocSlots to size for churn rather than for
// the fill: every eviction leaves a tombstone, and live+dirty is exactly the number of slots ever touched. Sized for
// the fill alone, capacity 12000 sat at load 0.73, so a few hundred tombstones crossed the 3/4 rebuild line and the
// purge promoted itself to a growth rebuild because live was over half the table.
func TestPreallocatedTableDoublesOnChurn(t *testing.T) {
	const capacity = 12000

	newCache := func(t *testing.T) (*Cache[int, int], int) {
		t.Helper()
		c, err := New[int, int](WithMemoryCapacity(capacity), WithShardsCount(1), WithPreallocatedSize())
		require.NoError(t, err)
		t.Cleanup(func() { c.Close() })

		prealloc := slotCounts(c)[0]
		require.Equal(t, preallocSlots(c.shards[0].cap), prealloc)
		require.Greater(t, prealloc, 2*(capacity+1), "the peak live count must stay under half the table")
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

// TestPreallocSlots pins the sizing rule: a power of two that keeps the peak live count of cap+1 items under half the
// table, so no rebuild it ever trips can double it, and never below the initial table.
func TestPreallocSlots(t *testing.T) {
	assert.Equal(t, minTableSlots, preallocSlots(1))
	assert.Equal(t, 128, preallocSlots(40))
	assert.Equal(t, 128, preallocSlots(60))
	assert.Equal(t, 2048, preallocSlots(1000))
	assert.Equal(t, 4096, preallocSlots(1024))
	// One shard of the 100M-entry memory benchmark: 32 shards of 8Mi slots, ~6.4 GiB of tables and no growth from
	// there. Sized for the fill alone this was 4Mi, and churn doubled it to the same 8Mi within a percent of a full
	// cache - the same memory, arrived at by surprise.
	assert.Equal(t, 1<<23, preallocSlots(100_000_000/32))
	assert.Equal(t, int(maxTableSlots), preallocSlots(3<<30), "the table is capped by the uint32 index space")

	for _, shardCap := range []int64{100, 1000, 1024, 65_536, 3_125_000} {
		slots := int64(preallocSlots(shardCap))
		assert.Less(t, (shardCap+1)*2, slots, "cap %d: a rebuild would still double this table", shardCap)
		assert.GreaterOrEqual(t, (shardCap+1)*4, slots, "cap %d: half that table would already do", shardCap)
	}
}
