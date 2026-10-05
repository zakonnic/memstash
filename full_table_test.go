package memstash

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
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
	c, err := New[int, int](WithMemoryCapacity(100), WithShardsCount(1), WithPreallocatedSize(),
		WithCustomEvictionPolicy(func(Items[int, int], int64) EvictionPolicy[int, int] { return keepAll[int, int]{} }))
	require.NoError(t, err)
	defer c.Close()
	slots := c.shards[0].items.GetStorage().Len()

	for k := range 2 * slots {
		require.NoError(t, c.Set(ctx, k, k))
	}
	require.Equal(t, slots-1, c.Len())
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
