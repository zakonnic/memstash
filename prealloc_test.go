package memstash

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash/internal/itemstore"
)

func slotCounts[K comparable, V any](c *Cache[K, V]) []int {
	counts := make([]int, len(c.shards))
	for i := range c.shards {
		counts[i] = c.shards[i].items.GetStorage().Len()
	}
	return counts
}

// TestPreallocatedTableFillsWithoutRebuild fills a preallocated cache and then churns it. Neither the fill nor the
// churn may rebuild: that is what preallocation is for, and a deletion reclaims its slot on the spot, so churn leaves
// no tombstones to purge.
func TestPreallocatedTableFillsWithoutRebuild(t *testing.T) {
	ctx := context.Background()
	const capacity = 4096

	c, err := New[int, int](WithMemoryCapacity(capacity), WithShardsCount(4), WithPreallocatedSize(), WithStats())
	require.NoError(t, err)
	defer c.Close()

	initial := slotCounts(c)
	for i := range c.shards {
		require.Equal(t, preallocSlots(c.shards[i].cap, DefaultPreallocationRatio), initial[i],
			"shard %d was not sized for its capacity", i)
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
	assert.Zero(t, after.TableGrowths(), "the target load is below the growth line: a preallocated table never grows")
	assert.Zero(t, after.TablePurges(), "a deletion compacts its run on the spot: nothing is ever purged")
}

// TestPreallocatedTableSurvivesEarlyChurn is the regression that first forced preallocSlots to account for
// tombstones: every eviction used to leave one, and a few hundred of them carried a just-filled table over the
// rebuild line and doubled it. Slots are reclaimed on the spot now, so a cache barely past its fill must still be
// sitting on the table it was given.
func TestPreallocatedTableSurvivesEarlyChurn(t *testing.T) {
	const capacity = 12000

	newCache := func(t *testing.T) (*Cache[int, int], int) {
		t.Helper()
		c, err := New[int, int](WithMemoryCapacity(capacity), WithShardsCount(1), WithPreallocatedSize())
		require.NoError(t, err)
		t.Cleanup(func() { c.Close() })

		prealloc := slotCounts(c)[0]
		require.Equal(t, preallocSlots(c.shards[0].cap, DefaultPreallocationRatio), prealloc)
		return c, prealloc
	}

	// Steady state at capacity: each Set evicts one item, so live stays put while the slots churn.
	t.Run("eviction churn at capacity", func(t *testing.T) {
		ctx := context.Background()
		c, prealloc := newCache(t)

		for i := range capacity {
			require.NoError(t, c.Set(ctx, i, i))
		}
		require.Equal(t, prealloc, slotCounts(c)[0], "the fill alone must not rebuild")

		for i := capacity; i < capacity+capacity/32; i++ { // 375 sets, 375 evictions
			require.NoError(t, c.Set(ctx, i, i))
		}

		assert.Equal(t, prealloc, slotCounts(c)[0],
			"the preallocated table grew after only %d sets past a full cache: %d live items in %d slots",
			capacity/32, c.Len(), prealloc)
	})

	// Below capacity nothing is ever evicted; the deletes reclaim their slots and the table must stay put all the same.
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

// TestPreallocatedTableNeverGrows drives the growth decision with a live count no eviction path would leave behind:
// a preallocated table stays put exactly where a grown one doubles.
func TestPreallocatedTableNeverGrows(t *testing.T) {
	const capacity = 4096

	rebuildAtFullLive := func(t *testing.T, opts ...Option) (before, after int, growths, purges int64) {
		t.Helper()
		ctx := context.Background()
		c, err := New[int, int](append(
			[]Option{WithMemoryCapacity(capacity), WithShardsCount(1), WithStats()}, opts...)...)
		require.NoError(t, err)
		t.Cleanup(func() { c.Close() })
		for i := range capacity {
			require.NoError(t, c.Set(ctx, i, i))
		}

		// Stats counters share their storage with the snapshot they came from, so the numbers have to be read out
		// on the spot rather than kept as two snapshots to diff.
		counts := func() (int64, int64) {
			s := c.Stats()
			return s.TableGrowths(), s.TablePurges()
		}
		grownBefore, purgedBefore := counts()

		sh := &c.shards[0]
		sh.mu.Lock()
		storage := sh.items.GetStorage()
		before = storage.Len()
		sh.live = before // over the line, so the rebuild has to choose between growing and purging
		c.maybeGrow(sh, storage)
		after = sh.items.GetStorage().Len()
		sh.mu.Unlock()

		grownAfter, purgedAfter := counts()
		return before, after, grownAfter - grownBefore, purgedAfter - purgedBefore
	}

	t.Run("preallocated", func(t *testing.T) {
		before, after, growths, purges := rebuildAtFullLive(t, WithPreallocatedSize())
		assert.Equal(t, before, after, "a preallocated table is never grown, whatever its live count")
		assert.Zero(t, growths)
		assert.Zero(t, purges)
	})

	t.Run("grown", func(t *testing.T) {
		before, after, growths, purges := rebuildAtFullLive(t)
		assert.Greater(t, after, before, "without preallocation the same live count takes the table up a step")
		assert.Equal(t, int64(1), growths)
		assert.Zero(t, purges)
	})
}

// TestGrowthThresholdMovesTheRebuildLine pins the other knob: seven tenths occupancy is over a 0.6 line and under a
// 0.8 one, and the growth decision follows it.
func TestGrowthThresholdMovesTheRebuildLine(t *testing.T) {
	const capacity = 4096

	rebuildAtSevenTenths := func(t *testing.T, threshold float64) (before, after int, growths, purges int64) {
		t.Helper()
		ctx := context.Background()
		c, err := New[int, int](WithMemoryCapacity(capacity), WithShardsCount(1), WithStats(),
			WithGrowthThreshold(threshold))
		require.NoError(t, err)
		t.Cleanup(func() { c.Close() })
		for i := range capacity {
			require.NoError(t, c.Set(ctx, i, i))
		}
		counts := func() (int64, int64) {
			s := c.Stats()
			return s.TableGrowths(), s.TablePurges()
		}
		grownBefore, purgedBefore := counts()

		sh := &c.shards[0]
		sh.mu.Lock()
		storage := sh.items.GetStorage()
		before = storage.Len()
		sh.live = before * 7 / 10
		c.maybeGrow(sh, storage)
		after = sh.items.GetStorage().Len()
		sh.mu.Unlock()

		grownAfter, purgedAfter := counts()
		return before, after, grownAfter - grownBefore, purgedAfter - purgedBefore
	}

	t.Run("over a 0.6 line", func(t *testing.T) {
		before, after, growths, purges := rebuildAtSevenTenths(t, 0.6)
		assert.Greater(t, after, before, "live past the line has to take the table up a step")
		assert.Equal(t, int64(1), growths)
		assert.Zero(t, purges)
	})

	t.Run("under a 0.8 line", func(t *testing.T) {
		before, after, growths, purges := rebuildAtSevenTenths(t, 0.8)
		assert.Equal(t, before, after, "seven tenths is below the line: no rebuild at all")
		assert.Zero(t, growths)
		assert.Zero(t, purges)
	})
}

// TestGrowthLineOnLargeTables covers tables past 2^31/growthNum slots. With a 32-bit int, live<<growthShift and
// Len*growthNum overflowed there, and a half-full shard grew on every insert until it ran out of memory.
func TestGrowthLineOnLargeTables(t *testing.T) {
	const slots = 12 << 16 // 786432: Len*growthNum is 2.68e9 at the default threshold
	c, err := New[int, int](WithShardsCount(1))
	require.NoError(t, err)
	defer c.Close()
	sh := &c.shards[0]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	grows := func(live int) bool {
		storage := itemstore.NewFlatHashMap[int, int](slots, c.seed)
		sh.items.Store(storage)
		sh.live = live
		c.maybeGrow(sh, storage)
		return sh.items.GetStorage() != storage
	}
	assert.False(t, grows(slots/2), "half full is under the line")
	assert.True(t, grows(slots*9/10), "nine tenths is over the line")
	sh.live = 0
}

// TestPreallocationRatioSizesTheTable pins what the ratio dial does: it preallocates on its own, and the shard gets
// the table that ratio names.
func TestPreallocationRatioSizesTheTable(t *testing.T) {
	const capacity = 4096

	for _, tc := range []struct {
		name  string
		ratio float64
	}{
		{"quarter", 0.25},
		{"half", 0.5},
		{"nine tenths", 0.9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := New[int, int](WithMemoryCapacity(capacity), WithShardsCount(1), WithPreallocationRatio(tc.ratio))
			require.NoError(t, err)
			defer c.Close()
			assert.Equal(t, preallocSlots(capacity, tc.ratio), slotCounts(c)[0],
				"the ratio must preallocate on its own, at the size it names")
		})
	}

	t.Run("the default ratio is what WithPreallocatedSize gives", func(t *testing.T) {
		tuned, err := New[int, int](WithMemoryCapacity(capacity), WithShardsCount(1), WithPreallocationRatio(0.75))
		require.NoError(t, err)
		defer tuned.Close()
		plain, err := New[int, int](WithMemoryCapacity(capacity), WithShardsCount(1), WithPreallocatedSize())
		require.NoError(t, err)
		defer plain.Close()
		assert.Equal(t, slotCounts(plain), slotCounts(tuned))
	})
}

// TestPreallocSlots pins the sizing rule: whole chunks, enough that a full shard stays under the rebuild line and
// packs no denser than the target load, and never below the initial table.
func TestPreallocSlots(t *testing.T) {
	assert.Equal(t, minTableSlots, preallocSlots(1, DefaultPreallocationRatio))
	assert.Equal(t, 64, preallocSlots(40, DefaultPreallocationRatio))
	assert.Equal(t, 88, preallocSlots(60, DefaultPreallocationRatio))
	assert.Equal(t, 1336, preallocSlots(1000, DefaultPreallocationRatio))
	assert.Equal(t, 1368, preallocSlots(1024, DefaultPreallocationRatio))
	// One shard of the 100M-entry memory benchmark: 32 shards of 64 chunks, ~3.1 GiB of tables, and it stays there -
	// the target load sits below the growth line, so churn never grows this table.
	assert.Equal(t, 64<<16, preallocSlots(100_000_000/32, DefaultPreallocationRatio))
	assert.Equal(t, int(maxTableSlots), preallocSlots(3<<30, DefaultPreallocationRatio),
		"the table is capped by the uint32 index space")
	// The slot count is a uint32 too: a table of 2^32 slots would have size 0.
	assert.EqualValues(t, maxTableSlots, uint32(maxTableSlots))
	assert.EqualValues(t, maxTableSlots, itemstore.TableSlots(int(maxTableSlots)), "the cap is whole chunks")
	assert.Equal(t, int(maxTableSlots), itemstore.GrowSlots(int(maxTableSlots)-1<<16), "growth stops at the cap")

	// The ratio is the dial: the same shard packs into whatever share of its table the caller asked for.
	assert.Equal(t, 2056, preallocSlots(1024, 0.5))
	assert.Equal(t, 1144, preallocSlots(1024, 0.9))
	assert.Equal(t, 10256, preallocSlots(1024, 0.1))

	for _, shardCap := range []int64{100, 1000, 1024, 65_536, 3_125_000} {
		slots := int64(preallocSlots(shardCap, DefaultPreallocationRatio))
		assert.Less(t, float64(shardCap+1), float64(slots)*DefaultGrowthThreshold,
			"cap %d: a full shard must not trip the rebuild line", shardCap)
		assert.LessOrEqual(t, float64(shardCap+1), float64(slots)*DefaultPreallocationRatio,
			"cap %d: the table is packed past the target load", shardCap)
	}
}
