package memstash

import (
	"context"
	"maps"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zakonnic/memstash/internal/itemstore"
)

// keepAll evicts nothing, which the EvictionPolicy contract allows.
type keepAll[K comparable, V any] struct{}

func (keepAll[K, V]) Add(QNode)                   {}
func (keepAll[K, V]) Remove(QNode)                {}
func (keepAll[K, V]) Reweigh(uint32, int64)       {}
func (keepAll[K, V]) Evict(uint32) (uint32, bool) { return 0, false }
func (keepAll[K, V]) Bytes() int64                { return 0 }

// A table that cannot grow fills up under a policy that evicts nothing. Its last free slot must stay free: a probe
// for an absent key, and an insert, need an empty slot to end at.
func TestFullTableKeepsASlotFree(t *testing.T) {
	ctx := context.Background()
	c, err := New[int, int](WithMemoryCapacity(100), WithShardsCount(1), WithPreallocatedSize(), WithStats(),
		WithCustomEvictionPolicy(func(Items[int, int], int64) EvictionPolicy[int, int] { return keepAll[int, int]{} }))
	require.NoError(t, err)
	defer c.Close()
	slots := c.shards[0].items.GetStorage().Len()

	for k := range 2 * slots {
		require.NoError(t, c.Set(ctx, k, k))
	}
	require.Equal(t, slots-1, c.Len())
	require.EqualValues(t, slots+1, c.Stats().Rejected())
	for k := range slots - 1 {
		v, ok := c.GetFromMemory(k)
		require.True(t, ok, k)
		require.Equal(t, k, v)
	}
	_, ok := c.GetFromMemory(2*slots - 1)
	require.False(t, ok)

	require.NoError(t, c.Set(ctx, 0, -1), "an overwrite needs no free slot")
	v, _ := c.GetFromMemory(0)
	require.Equal(t, -1, v)
	require.NoError(t, c.Delete(ctx, 1))
	require.NoError(t, c.Set(ctx, 2*slots, 1), "a delete frees a slot for a new key")
	_, ok = c.GetFromMemory(2 * slots)
	require.True(t, ok)
}

// A table that can grow does so before its last free slot is needed, whatever the growth threshold.
func TestHighGrowthThresholdStillGrows(t *testing.T) {
	ctx := context.Background()
	c, err := New[int, int](WithMemoryCapacity(1000), WithShardsCount(1), WithGrowthThreshold(0.999))
	require.NoError(t, err)
	defer c.Close()
	for k := range 1000 {
		require.NoError(t, c.Set(ctx, k, k))
	}
	require.Equal(t, 1000, c.Len())
}

// badVictim names a victim no live item sits at: past the table, or an empty slot.
type badVictim struct {
	keepAll[int, int]
	items Items[int, int]
	idx   func(Items[int, int]) uint32
}

func (p badVictim) Evict(uint32) (uint32, bool) { return p.idx(p.items), true }

// An eviction policy that names no live item ends the eviction round: the shard stays over capacity, but nothing is
// written outside the table, killed twice or miscounted.
func TestBadVictimIndexLeavesTheShardIntact(t *testing.T) {
	ctx := context.Background()
	emptySlot := func(items Items[int, int]) uint32 {
		for idx := uint32(0); ; idx++ {
			if itemstore.Empty(items.At(idx).Metadata()) {
				return idx
			}
		}
	}
	for name, idx := range map[string]func(Items[int, int]) uint32{
		"far past the table":  func(Items[int, int]) uint32 { return 4_000_000 },
		"just past the table": func(Items[int, int]) uint32 { return 64 },
		"an empty slot":       emptySlot,
	} {
		c, err := New[int, int](WithMemoryCapacity(8), WithShardsCount(1),
			WithCustomEvictionPolicy(func(items Items[int, int], _ int64) EvictionPolicy[int, int] {
				return badVictim{items: items, idx: idx}
			}))
		require.NoError(t, err, name)
		for k := range 12 {
			require.NoError(t, c.Set(ctx, k, k), name)
		}
		require.Equal(t, 12, c.Len(), name)
		require.Len(t, maps.Collect(c.Iterator()), 12, name)
		c.Close()
	}
}
