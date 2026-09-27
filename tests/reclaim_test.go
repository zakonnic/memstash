package tests

import (
	"context"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
)

func heapAlloc() int64 {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return int64(ms.HeapAlloc)
}

// TestDeleteBelowCapacityDoesNotLeak runs a delete-heavy workload that never reaches the capacity, so eviction never
// runs: whatever a deletion leaves behind stays for good, and the footprint must stay flat. The subtests measure
// process heap, so they cannot run in parallel with each other.
func TestDeleteBelowCapacityDoesNotLeak(t *testing.T) {
	ctx := context.Background()
	for _, tc := range policies {
		t.Run(tc.name, func(t *testing.T) {
			c, err := memstash.New[int, int](
				memstash.WithMemoryCapacity(1024),
				memstash.WithPolicy(tc.policy),
				memstash.WithShardsCount(1),
			)
			require.NoError(t, err)
			defer c.Close()

			before := heapAlloc()
			const cycles = 250_000
			for i := 0; i < cycles; i++ {
				require.NoError(t, c.Set(ctx, i, i))
				require.NoError(t, c.Delete(ctx, i))
			}
			growth := heapAlloc() - before

			// Without reclamation these cycles leak ~8 MB; with it the growth is a rounding error - measured at
			// -0.04 MB. The 1 MB bound leaves room for GC noise while still failing hard on the unbounded behavior.
			t.Logf("heap growth over %d cycles: %.2f MB", cycles, float64(growth)/(1<<20))
			assert.LessOrEqualf(t, growth, int64(1<<20),
				"heap grew by %.1f MB over %d set+delete cycles below capacity: tombstones are not reclaimed",
				float64(growth)/(1<<20), cycles)

			// The cache must stay fully functional after the churn.
			require.NoError(t, c.Set(ctx, -1, 42))
			v, ok := c.GetFromMemory(-1)
			require.True(t, ok, "GetFromMemory after churn")
			assert.Equal(t, 42, v)
		})
	}
}

// TestChurnKeepsLiveItems verifies that a delete-heavy churn of other keys, and the compactions it causes, leave the
// hot keys and their values alone.
func TestChurnKeepsLiveItems(t *testing.T) {
	ctx := context.Background()
	c, err := memstash.New[int, int](
		memstash.WithMemoryCapacity(1024),
		memstash.WithShardsCount(1),
	)
	require.NoError(t, err)
	defer c.Close()

	// 100 long-lived keys, then a churn of 100k short-lived ones interleaved with reads of the hot set.
	for i := 0; i < 100; i++ {
		require.NoError(t, c.Set(ctx, i, i*7))
	}
	for i := 0; i < 100_000; i++ {
		key := 1_000 + i
		require.NoError(t, c.Set(ctx, key, key))
		require.NoError(t, c.Delete(ctx, key))
		if i%1000 == 0 {
			c.GetFromMemory(i % 100)
		}
	}

	for i := 0; i < 100; i++ {
		v, ok := c.GetFromMemory(i)
		require.True(t, ok, "live key %d lost after the churn", i)
		assert.Equal(t, i*7, v, "live key %d corrupted after the churn", i)
	}
	// Every churn key was deleted right away - only the hot set remains.
	assert.EqualValues(t, 100, c.Weight())
}
