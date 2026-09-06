package memstash

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestProbeFindsEverythingUnderChurn is the property HasNext has to keep: a key the cache holds must always be
// findable. The probe stops on a chain bit, not on an empty slot, so a chain cut anywhere (a missed mark on insert,
// a mark lost to Publish, a stale bit carried across growth) shows up here as a key that is present and not found. A
// plain map is the oracle.
func TestProbeFindsEverythingUnderChurn(t *testing.T) {
	ctx := context.Background()
	const capacity = 2000

	c, err := New[uint64, uint64](WithMemoryCapacity(capacity), WithShardsCount(1))
	require.NoError(t, err)
	defer c.Close()

	// The oracle can only track what the cache is not allowed to drop on its own, so stay under capacity.
	live := map[uint64]uint64{}
	rng := rand.New(rand.NewSource(7))
	for step := range 400_000 {
		switch {
		case len(live) < capacity*3/4:
			k := uint64(rng.Intn(capacity * 4))
			v := uint64(step)
			require.NoError(t, c.Set(ctx, k, v))
			live[k] = v
		default:
			for k := range live { // delete an arbitrary live key
				require.NoError(t, c.Delete(ctx, k))
				delete(live, k)
				break
			}
		}
		if step%5000 == 0 {
			for k, v := range live {
				got, ok := c.GetFromMemory(k)
				require.True(t, ok, "step %d: key %d is in the cache but the probe stopped short", step, k)
				require.Equal(t, v, got, "step %d: key %d", step, k)
			}
		}
	}
	for k, v := range live {
		got, ok := c.GetFromMemory(k)
		require.True(t, ok, "final: key %d is in the cache but the probe stopped short", k)
		require.Equal(t, v, got)
	}
	require.Equal(t, len(live), c.Len())
}

// TestNoFalseMissUnderConcurrentChurn is the property every lock-free probe has to keep once slots can be reclaimed
// while readers walk them: a key that is in the cache and never removed must be found by every read, whatever the
// writers do to its neighbors. Pinned keys are never deleted and the churn stays under capacity, so no eviction can
// touch them; the churn keys share the single shard and are inserted and deleted as fast as the writers can go.
func TestNoFalseMissUnderConcurrentChurn(t *testing.T) {
	ctx := context.Background()
	const (
		capacity = 20_000
		pinned   = 10_000
		writers  = 4
		readers  = 4
		churnOps = 300_000
	)
	c, err := New[uint64, uint64](WithMemoryCapacity(capacity), WithShardsCount(1))
	require.NoError(t, err)
	defer c.Close()

	for k := uint64(0); k < pinned; k++ {
		require.NoError(t, c.Set(ctx, k, k+1))
	}

	var writersWG, readersWG sync.WaitGroup
	for w := range writers {
		writersWG.Add(1)
		go func(base uint64) {
			defer writersWG.Done()
			rng := rand.New(rand.NewSource(int64(base >> 32)))
			for i := 0; i < churnOps; i++ {
				k := base + uint64(rng.Intn(capacity))
				_ = c.Set(ctx, k, k)
				if rng.Intn(4) != 0 {
					_ = c.Delete(ctx, k)
				}
			}
		}(uint64(w+1) << 32)
	}
	stop := make(chan struct{})
	var falseMisses, wrongValues atomic.Int64
	for r := range readers {
		readersWG.Add(1)
		go func(seed int64) {
			defer readersWG.Done()
			rng := rand.New(rand.NewSource(seed))
			batch := make([]uint64, 16)
			for {
				select {
				case <-stop:
					return
				default:
				}
				k := uint64(rng.Intn(pinned))
				if v, ok := c.GetFromMemory(k); !ok {
					falseMisses.Add(1)
				} else if v != k+1 {
					wrongValues.Add(1)
				}
				for i := range batch {
					batch[i] = uint64(rng.Intn(pinned))
				}
				if got := c.BatchGetFromMemory(batch, nil); len(got) != len(batch) {
					falseMisses.Add(int64(len(batch) - len(got)))
				}
			}
		}(int64(r) + 100)
	}
	writersWG.Wait()
	close(stop)
	readersWG.Wait()
	require.Zero(t, falseMisses.Load(), "a pinned key was not found by a concurrent read")
	require.Zero(t, wrongValues.Load(), "a pinned key was read with a wrong value")
}
