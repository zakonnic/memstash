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

// TestPreallocatedTableFillsWithoutRebuild fills a preallocated cache and then churns it. The fill itself must never
// rebuild - that is what preallocation is for. Churn is a different matter at the target load: tombstones push the
// table over the rebuild line once, it grows a step, and from there it holds. One step, not a cycle of them.
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

	for i := capacity; i < 5*capacity; i++ {
		require.NoError(t, c.Set(ctx, i, i))
	}
	settled := slotCounts(c)
	stats := c.Stats()
	growths := stats.TableGrowths()

	for i := 5 * capacity; i < 40*capacity; i++ {
		require.NoError(t, c.Set(ctx, i, i))
	}
	after := c.Stats()
	assert.Equal(t, settled, slotCounts(c), "the table must hold its size once churn has settled")
	assert.Equal(t, growths, after.TableGrowths(), "a settled table must not keep growing")
	assert.LessOrEqual(t, after.TableGrowths(), int64(len(c.shards)), "one growth per shard at most")
	assert.Zero(t, after.TablePurges(), "releasing slots in place must keep the table off the rebuild line")
}

// TestPreallocatedTableSurvivesEarlyChurn is the regression that first forced preallocSlots to account for
// tombstones: every eviction leaves one, live+dirty is exactly the number of slots ever touched, and a few hundred
// of them used to carry a just-filled table over the rebuild line and double it. Releasing slots in place is what
// keeps that from happening now, so a cache barely past its fill must still be sitting on the table it was given.
func TestPreallocatedTableSurvivesEarlyChurn(t *testing.T) {
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
			"the preallocated table grew after only %d sets past a full cache: %d live items in %d slots",
			capacity/32, c.Len(), prealloc)
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
			"the preallocated table grew on a cache that never reached capacity: %d live items in %d slots",
			c.Len(), prealloc)
	})
}

// TestPreallocSlots pins the sizing rule: whole chunks, enough that a full shard stays under the rebuild line and
// packs no denser than the target load, and never below the initial table.
func TestPreallocSlots(t *testing.T) {
	assert.Equal(t, minTableSlots, preallocSlots(1))
	assert.Equal(t, 72, preallocSlots(40))
	assert.Equal(t, 104, preallocSlots(60))
	assert.Equal(t, 1672, preallocSlots(1000))
	assert.Equal(t, 1712, preallocSlots(1024))
	// One shard of the 100M-entry memory benchmark: 32 shards of 80 chunks, ~4.0 GiB of tables against the 6.4 GiB
	// a power-of-two table took for the same capacity.
	assert.Equal(t, 80<<16, preallocSlots(100_000_000/32))
	assert.Equal(t, int(maxTableSlots), preallocSlots(3<<30), "the table is capped by the uint32 index space")

	for _, shardCap := range []int64{100, 1000, 1024, 65_536, 3_125_000} {
		slots := int64(preallocSlots(shardCap))
		assert.Less(t, (shardCap+1)*4, slots*3, "cap %d: a full shard must not trip the rebuild line", shardCap)
		assert.LessOrEqual(t, (shardCap+1)*preallocLoadDen, slots*preallocLoadNum,
			"cap %d: the table is packed past the target load", shardCap)
	}
}
