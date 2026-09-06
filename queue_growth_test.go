package memstash

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestQueueGrowthOnDeleteThenSet drives the invalidate-then-reload pattern below capacity on one key: no eviction
// runs, so nothing pops the eviction queue, and the node every reinsertion leaves behind would grow it for good.
// What bounds it is the sweep: it keeps the one node the item is entitled to and drops the duplicates.
func TestQueueGrowthOnDeleteThenSet(t *testing.T) {
	ctx := context.Background()
	c, err := New[int, int](WithMemoryCapacity(4096), WithShardsCount(1))
	require.NoError(t, err)
	defer c.Close()

	for i := range 1000 { // a resident set well below capacity
		require.NoError(t, c.Set(ctx, i, i))
	}
	sh := &c.shards[0]
	sh.mu.Lock()
	before := sh.policy.Len()
	sh.mu.Unlock()

	const cycles = 100_000
	for range cycles {
		require.NoError(t, c.Delete(ctx, 7))
		require.NoError(t, c.Set(ctx, 7, 7))
	}
	sh.mu.Lock()
	after, live := sh.policy.Len(), sh.live
	sh.mu.Unlock()
	// The sweep starts at twice the live count and runs a few nodes per removal, so the queue settles a little past
	// its trigger and stays there: proportional to the resident set, not to the number of reinsertions.
	t.Logf("queued nodes: %d -> %d for %d live items over %d cycles", before, after, live, cycles)
	require.Less(t, after, 3*live+sweepMinStale, "the queue grew with the reinsertions instead of settling")
}
