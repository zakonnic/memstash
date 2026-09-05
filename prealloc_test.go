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
// rebuild - that is what preallocation is for. Churn is a different matter: the target load leaves room for a limited
// number of tombstones, so they reach the rebuild line and are purged in place. The table never grows, it just pays
// for that in periodic O(live) purges.
func TestPreallocatedTableFillsWithoutRebuild(t *testing.T) {
	ctx := context.Background()
	const capacity = 4096

	c, err := New[int, int](WithMemoryCapacity(capacity), WithShardsCount(4), WithPreallocatedSize(), WithStats())
	require.NoError(t, err)
	defer c.Close()

	initial := slotCounts(c)
	for i := range c.shards {
		require.Equal(t, preallocSlots(c.shards[i].cap, defaultPreallocRatio), initial[i],
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
	assert.Positive(t, after.TablePurges(), "the tombstones this churn leaves have to be reclaimed by a purge")
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
		require.Equal(t, preallocSlots(c.shards[0].cap, defaultPreallocRatio), prealloc)
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

// TestPreallocatedTableNeverGrows drives the rebuild decision with a live count no eviction path would leave behind:
// a preallocated table has to purge exactly where a grown one doubles.
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
		c.maybeRebuild(sh, storage)
		after = sh.items.GetStorage().Len()
		sh.mu.Unlock()

		grownAfter, purgedAfter := counts()
		return before, after, grownAfter - grownBefore, purgedAfter - purgedBefore
	}

	t.Run("preallocated", func(t *testing.T) {
		before, after, growths, purges := rebuildAtFullLive(t, WithPreallocatedSize())
		assert.Equal(t, before, after, "a preallocated table must be purged in place, never grown")
		assert.Zero(t, growths)
		assert.Equal(t, int64(1), purges)
	})

	t.Run("grown", func(t *testing.T) {
		before, after, growths, purges := rebuildAtFullLive(t)
		assert.Greater(t, after, before, "without preallocation the same live count takes the table up a step")
		assert.Equal(t, int64(1), growths)
		assert.Zero(t, purges)
	})
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
	assert.Equal(t, minTableSlots, preallocSlots(1, defaultPreallocRatio))
	assert.Equal(t, 64, preallocSlots(40, defaultPreallocRatio))
	assert.Equal(t, 88, preallocSlots(60, defaultPreallocRatio))
	assert.Equal(t, 1336, preallocSlots(1000, defaultPreallocRatio))
	assert.Equal(t, 1368, preallocSlots(1024, defaultPreallocRatio))
	// One shard of the 100M-entry memory benchmark: 32 shards of 64 chunks, ~3.1 GiB of tables, and it stays there -
	// the target load sits below the growth line, so churn can only purge this table, never grow it.
	assert.Equal(t, 64<<16, preallocSlots(100_000_000/32, defaultPreallocRatio))
	assert.Equal(t, int(maxTableSlots), preallocSlots(3<<30, defaultPreallocRatio),
		"the table is capped by the uint32 index space")

	// The ratio is the dial: the same shard packs into whatever share of its table the caller asked for.
	assert.Equal(t, 2056, preallocSlots(1024, 0.5))
	assert.Equal(t, 1144, preallocSlots(1024, 0.9))
	assert.Equal(t, 10256, preallocSlots(1024, 0.1))

	for _, shardCap := range []int64{100, 1000, 1024, 65_536, 3_125_000} {
		slots := int64(preallocSlots(shardCap, defaultPreallocRatio))
		assert.Less(t, (shardCap+1)*6, slots*5, "cap %d: a full shard must not trip the rebuild line", shardCap)
		assert.LessOrEqual(t, float64(shardCap+1), float64(slots)*defaultPreallocRatio,
			"cap %d: the table is packed past the target load", shardCap)
	}
}
