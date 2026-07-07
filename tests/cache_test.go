package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
)

// policies is the eviction-policy axis shared by the table-driven tests.
var policies = []struct {
	name   string
	policy memstash.Policy
}{
	{name: "clock", policy: memstash.PolicyClock},
	{name: "s3fifo", policy: memstash.PolicyS3FIFO},
}

func newCache(t *testing.T, cfg memstash.Config[string, string]) *memstash.Cache[string, string] {
	t.Helper()
	c, err := memstash.NewWithConfig[string, string](&cfg)
	require.NoError(t, err, "NewWithConfig")
	t.Cleanup(c.Close)
	return c
}

func TestSetGetDelete(t *testing.T) {
	ctx := context.Background()
	for _, tc := range policies {
		t.Run(tc.name, func(t *testing.T) {
			c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100, Policy: tc.policy})

			_, ok, err := c.Get(ctx, "k")
			require.NoError(t, err)
			require.False(t, ok, "empty cache returned a value")

			require.NoError(t, c.Set(ctx, "k", "v"))

			v, ok, err := c.Get(ctx, "k")
			require.NoError(t, err)
			require.True(t, ok, "Get after Set")
			assert.Equal(t, "v", v)

			v, ok = c.GetFromMemory("k")
			require.True(t, ok, "GetFromMemory after Set")
			assert.Equal(t, "v", v)

			require.NoError(t, c.Delete(ctx, "k"))

			_, ok, err = c.Get(ctx, "k")
			require.NoError(t, err)
			assert.False(t, ok, "value survived Delete")
			assert.EqualValues(t, 0, c.Weight(), "weight after Delete")
		})
	}
}

func TestOverwriteKeepsWeight(t *testing.T) {
	ctx := context.Background()
	for _, tc := range policies {
		t.Run(tc.name, func(t *testing.T) {
			c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100, Policy: tc.policy})

			for i := 0; i < 10; i++ {
				require.NoError(t, c.Set(ctx, "k", fmt.Sprintf("v%d", i)))
			}
			assert.EqualValues(t, 1, c.Weight(), "weight after overwrites")

			v, ok, err := c.Get(ctx, "k")
			require.NoError(t, err)
			require.True(t, ok)
			assert.Equal(t, "v9", v, "the last written value must win")
		})
	}
}

func TestCapacityIsRespected(t *testing.T) {
	ctx := context.Background()
	const capacity = 128
	for _, tc := range policies {
		t.Run(tc.name, func(t *testing.T) {
			c := newCache(t, memstash.Config[string, string]{MemoryCapacity: capacity, Policy: tc.policy})

			for i := 0; i < 10*capacity; i++ {
				require.NoError(t, c.Set(ctx, fmt.Sprintf("k%d", i), "v"))
			}
			assert.LessOrEqual(t, c.Weight(), int64(capacity), "weight exceeds capacity")
			assert.LessOrEqual(t, c.Len(), capacity, "item count exceeds capacity")
		})
	}
}

// TestClockSecondChance verifies a deterministic GCLOCK scenario: keys that were accessed survive a pass of the hand,
// while untouched keys are evicted in insertion order.
func TestClockSecondChance(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{
		MemoryCapacity: 100,
		Policy:         memstash.PolicyClock,
		Shards:         1, // deterministic eviction order
	})

	for i := 0; i < 100; i++ {
		require.NoError(t, c.Set(ctx, fmt.Sprintf("k%d", i), "v"))
	}
	// Warm up k0..k9 (two accesses are enough to saturate the counter).
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("k%d", i)
		c.GetFromMemory(key)
		c.GetFromMemory(key)
	}
	// 50 new inserts => 50 evictions.
	for i := 100; i < 150; i++ {
		require.NoError(t, c.Set(ctx, fmt.Sprintf("k%d", i), "v"))
	}

	ranges := []struct {
		name      string
		from, to  int
		wantAlive bool
	}{
		{name: "hot keys survive the hand", from: 0, to: 10, wantAlive: true},
		{name: "cold keys are evicted in insertion order", from: 10, to: 60, wantAlive: false},
		{name: "the rest is untouched", from: 60, to: 150, wantAlive: true},
	}
	for _, tc := range ranges {
		t.Run(tc.name, func(t *testing.T) {
			for i := tc.from; i < tc.to; i++ {
				_, ok := c.GetFromMemory(fmt.Sprintf("k%d", i))
				assert.Equal(t, tc.wantAlive, ok, "key k%d", i)
			}
		})
	}
}

func TestCostFunctionAccounting(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{
		MemoryCapacity: 1000,
		CostFunc:       func(_ string, v string) uint32 { return uint32(len(v)) },
		Shards:         1, // the exact "does not fit" boundary equals the whole capacity
	})

	// Ordered pipeline: every step changes the state the next one depends on.
	steps := []struct {
		name       string
		op         func() error
		wantWeight int64
	}{
		{name: "insert a (weight 10)", op: func() error { return c.Set(ctx, "a", "0123456789") }, wantWeight: 10},
		{name: "insert b (weight 5)", op: func() error { return c.Set(ctx, "b", "01234") }, wantWeight: 15},
		{name: "overwrite a (weight 2)", op: func() error { return c.Set(ctx, "a", "01") }, wantWeight: 7},
		{name: "delete b", op: func() error { return c.Delete(ctx, "b") }, wantWeight: 2},
	}
	for _, step := range steps {
		require.NoError(t, step.op(), step.name)
		require.EqualValues(t, step.wantWeight, c.Weight(), step.name)
	}

	// An item that is too heavy must not wreck the cache.
	require.NoError(t, c.Set(ctx, "huge", string(make([]byte, 2000))))
	_, ok := c.GetFromMemory("huge")
	assert.False(t, ok, "an item heavier than the capacity got into the cache")
	_, ok = c.GetFromMemory("a")
	assert.True(t, ok, "a light item was harmed by an attempt to insert a heavy one")
}

func TestTTLExpiry(t *testing.T) {
	if testing.Short() {
		t.Skip("slow TTL test")
	}
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{
		MemoryCapacity: 100,
		TTL:            time.Second,
	})

	require.NoError(t, c.Set(ctx, "k", "v"))
	_, ok, err := c.Get(ctx, "k")
	require.NoError(t, err)
	require.True(t, ok, "value is unavailable right after Set")

	time.Sleep(3 * time.Second)

	_, ok, err = c.Get(ctx, "k")
	require.NoError(t, err)
	assert.False(t, ok, "value is still alive after the TTL elapsed")
	assert.EqualValues(t, 0, c.Weight(), "weight of the expired item was not subtracted")
}

func TestOverwriteRefreshesTTL(t *testing.T) {
	if testing.Short() {
		t.Skip("slow TTL test")
	}
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{
		MemoryCapacity: 100,
		TTL:            time.Second,
	})

	require.NoError(t, c.Set(ctx, "k", "v1"))
	time.Sleep(3 * time.Second) // past the original TTL, same margin TestTTLExpiry relies on to call it expired
	require.NoError(t, c.Set(ctx, "k", "v2"))

	v, ok, err := c.Get(ctx, "k")
	require.NoError(t, err)
	require.True(t, ok, "overwrite did not refresh the TTL: the item was already treated as expired")
	assert.Equal(t, "v2", v)
}

func TestTotalWeight(t *testing.T) {
	ctx := context.Background()
	for _, tc := range policies {
		t.Run(tc.name, func(t *testing.T) {
			c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 1000, Policy: tc.policy})

			empty := c.TotalWeight()
			assert.GreaterOrEqual(t, empty, c.Weight(), "TotalWeight must be at least the logical Weight")

			for i := 0; i < 50; i++ {
				require.NoError(t, c.Set(ctx, fmt.Sprintf("k%d", i), "v"))
			}
			assert.Greater(t, c.TotalWeight(), empty, "TotalWeight must grow as items are added")
			assert.GreaterOrEqual(t, c.TotalWeight(), c.Weight(), "TotalWeight must stay at least the logical Weight")
		})
	}
}

func TestNewValidation(t *testing.T) {
	cases := []struct {
		name    string
		opts    []memstash.Option
		wantErr error // nil means the constructor must succeed
	}{
		{
			name: "zero capacity falls back to DefaultMemoryCapacity",
			opts: nil,
		},
		{
			name:    "negative capacity",
			opts:    []memstash.Option{memstash.WithMemoryCapacity(-1)},
			wantErr: memstash.ErrBadCapacity,
		},
		{
			name:    "zero capacity with a cost function",
			opts:    []memstash.Option{memstash.WithCostFunc(func(string, string) uint32 { return 1 })},
			wantErr: memstash.ErrBadCapacity,
		},
		{
			name:    "unknown eviction policy",
			opts:    []memstash.Option{memstash.WithMemoryCapacity(1), memstash.WithPolicy(memstash.Policy(42))},
			wantErr: memstash.ErrUnknownPolicy,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := memstash.New[string, string](tc.opts...)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			c.Close()
		})
	}

	t.Run("nil loader", func(t *testing.T) {
		_, err := memstash.NewLoadable[string, string](nil, memstash.WithMemoryCapacity(1))
		require.ErrorIs(t, err, memstash.ErrNilLoader)
	})
}

// TestShardedCapacityAndConsistency exercises the sharded mode: the total weight stays within the capacity, values are
// not corrupted, and concurrent access is safe.
func TestShardedCapacityAndConsistency(t *testing.T) {
	ctx := context.Background()
	c, err := memstash.New[int, int](
		memstash.WithMemoryCapacity(1024),
		memstash.WithShards(8),
	)
	require.NoError(t, err)
	defer c.Close()

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				key := (seed*5000 + i) % 3000
				assert.NoError(t, c.Set(ctx, key, key*7))
				if value, ok := c.GetFromMemory(key); ok {
					assert.Equal(t, key*7, value, "corrupted value for key %d", key)
				}
				if i%10 == 0 {
					assert.NoError(t, c.Delete(ctx, key))
				}
			}
		}(g)
	}
	wg.Wait()

	require.LessOrEqual(t, c.Weight(), int64(1024), "weight exceeds capacity after the concurrent load")
	require.Positive(t, c.Weight(), "cache is empty after the load")
	// Every remaining value must be consistent.
	for key := 0; key < 3000; key++ {
		if value, ok := c.GetFromMemory(key); ok {
			assert.Equal(t, key*7, value, "corrupted value for key %d", key)
		}
	}
}
