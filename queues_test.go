package memstash

import (
	"context"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash/internal/itemstore"
)

// checkQueues verifies what eviction rests on: every live item sits in exactly one eviction queue, every queued item is
// live, InMainQueue says which queue it is in, and the weights add up.
func checkQueues[K comparable, V any](t *testing.T, c *Cache[K, V]) {
	t.Helper()
	for i := range c.shards {
		sh := &c.shards[i]
		storage := sh.items.GetStorage()
		queued := map[uint32]uint{}
		for queue := range uint(itemstore.EvictionQueues) {
			for idx, ok := storage.Oldest(queue); ok; idx, ok = storage.Newer(idx) {
				_, dup := queued[idx]
				require.False(t, dup, "shard %d: slot %d is queued twice", i, idx)
				queued[idx] = queue
				metaWord := storage.At(idx).Metadata()
				require.False(t, empty(metaWord) || metaWord&itemstore.Dead != 0,
					"shard %d: queue %d holds a dead slot %d", i, queue, idx)
				require.Equal(t, queue == 1, metaWord&itemstore.InMainQueue != 0, "shard %d: slot %d in queue %d", i, idx, queue)
			}
		}
		require.Len(t, queued, sh.live, "shard %d: queued items vs live count", i)
		var weight int64
		for idx := range uint32(storage.Len()) {
			item := storage.At(idx)
			if empty(item.Metadata()) {
				continue
			}
			_, ok := queued[idx]
			require.True(t, ok, "shard %d: live slot %d is in no queue - nothing can evict it", i, idx)
			weight += c.rawCost(item.Entry().Key, item.Entry().Value)
		}
		require.Equal(t, weight, sh.weight.Load(), "shard %d: weight", i)
	}
}

// TestQueuesUnderChurn drives every policy through inserts, weight-changing overwrites, deletes, reads, growth and
// eviction, and checks the queues after each round.
func TestQueuesUnderChurn(t *testing.T) {
	ctx := context.Background()
	for _, policy := range []Policy{PolicyS3FIFO, PolicyClock, PolicyWTinyLFU, PolicySIEVE} {
		for _, prealloc := range []bool{false, true} {
			opts := []Option{WithMemoryCapacity(3000), WithShardsCount(2), WithPolicy(policy),
				WithCostFunc(func(_ uint64, v uint64) uint32 { return uint32(v%4) + 1 })}
			if prealloc {
				opts = append(opts[:3], WithPreallocatedSize()) // a preallocated cache takes no cost function
			}
			c, err := New[uint64, uint64](opts...)
			require.NoError(t, err)
			rng := rand.New(rand.NewPCG(uint64(policy), 7))
			for round := range 20 {
				for range 2000 {
					key := rng.Uint64N(uint64(1000 + 150*round)) // below capacity first, past it later
					switch rng.IntN(4) {
					case 0:
						_ = c.Delete(ctx, key)
					case 1:
						c.GetFromMemory(key)
					default:
						_ = c.Set(ctx, key, rng.Uint64())
					}
				}
				checkQueues(t, c)
			}
			c.Close()
		}
	}
}

// TestDeletedTwinStaysEvictable is the scenario that orphaned an item while queue nodes named keys by hash: a key
// replaced in its home slot by a new key with the same tag, which then turns hot. Now the item's place in
// its queue is its own, so it is evicted like any other.
func TestDeletedTwinStaysEvictable(t *testing.T) {
	ctx := context.Background()
	c, err := New[uint64, uint64](WithMemoryCapacity(4096), WithShardsCount(1), WithPolicy(PolicyClock),
		WithPreallocatedSize())
	require.NoError(t, err)
	defer c.Close()
	storage := c.shards[0].items.GetStorage()
	x, hx := uint64(1), c.keyHash(1)
	y := uint64(2)
	for hy := c.keyHash(y); storage.Home(hy) != storage.Home(hx) || tagOf(hy) != tagOf(hx); hy = c.keyHash(y) {
		y++
	}
	require.NoError(t, c.Set(ctx, x, x))
	require.NoError(t, c.Delete(ctx, x))
	require.NoError(t, c.Set(ctx, y, y))
	for key := uint64(1 << 40); key < 1<<40+1000; key++ {
		require.NoError(t, c.Set(ctx, key, key))
		require.NoError(t, c.Delete(ctx, key))
	}
	c.GetFromMemory(y)
	for key := uint64(1 << 50); key < 1<<50+10*4096; key++ {
		require.NoError(t, c.Set(ctx, key, key))
	}
	_, alive := c.GetFromMemory(y)
	require.False(t, alive, "y outlived ten capacities of fresh keys")
	checkQueues(t, c)
}
