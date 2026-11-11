package tests

import (
	"context"
	"maps"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/internal/itemstore"
)

// keepAll evicts nothing, which the EvictionPolicy contract allows.
type keepAll[K comparable, V any] struct{}

func (keepAll[K, V]) Add(memstash.QNode)          {}
func (keepAll[K, V]) Remove(memstash.QNode)       {}
func (keepAll[K, V]) Reweigh(uint32, int64)       {}
func (keepAll[K, V]) Evict(uint32) (uint32, bool) { return 0, false }
func (keepAll[K, V]) Bytes() int64                { return 0 }

// A table that can grow does so before its last free slot is needed, whatever the growth threshold.
func TestHighGrowthThresholdStillGrows(t *testing.T) {
	ctx := context.Background()
	c, err := memstash.New[int, int](memstash.WithMemoryCapacity(1000), memstash.WithShardsCount(1),
		memstash.WithGrowthThreshold(0.999))
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
	items memstash.Items[int, int]
	idx   func(memstash.Items[int, int]) uint32
}

func (p badVictim) Evict(uint32) (uint32, bool) { return p.idx(p.items), true }

// An eviction policy that names no live item ends the eviction round: the shard stays over capacity, but nothing is
// written outside the table, killed twice or miscounted.
func TestBadVictimIndexLeavesTheShardIntact(t *testing.T) {
	ctx := context.Background()
	emptySlot := func(items memstash.Items[int, int]) uint32 {
		for idx := uint32(0); ; idx++ {
			if itemstore.Empty(items.At(idx).Metadata()) {
				return idx
			}
		}
	}
	for name, idx := range map[string]func(memstash.Items[int, int]) uint32{
		"far past the table":  func(memstash.Items[int, int]) uint32 { return 4_000_000 },
		"just past the table": func(memstash.Items[int, int]) uint32 { return 64 },
		"an empty slot":       emptySlot,
	} {
		c, err := memstash.New[int, int](memstash.WithMemoryCapacity(8), memstash.WithShardsCount(1),
			memstash.WithCustomEvictionPolicy(
				func(items memstash.Items[int, int], _ int64) memstash.EvictionPolicy[int, int] {
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
