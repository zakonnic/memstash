package memstash

import (
	"context"
	"math/rand"
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
