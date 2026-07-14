package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
)

func TestStatsCounters(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100, StatsEnabled: true})

	require.NoError(t, c.Set(ctx, "a", "1"))
	require.NoError(t, c.BatchSet(ctx, memstash.List[string, string]{{Key: "b", Value: "2"}, {Key: "c", Value: "3"}}))

	_, ok, err := c.Get(ctx, "a") // hit
	require.NoError(t, err)
	require.True(t, ok)
	_, ok, err = c.Get(ctx, "absent") // miss
	require.NoError(t, err)
	require.False(t, ok)
	c.GetFromMemory("b")      // hit
	c.GetFromMemory("absent") // miss

	got, err := c.BatchGet(ctx, []string{"a", "b", "c", "absent"}) // 3 hits + 1 miss
	require.NoError(t, err)
	require.Len(t, got, 3)

	require.NoError(t, c.Delete(ctx, "a"))
	require.NoError(t, c.BatchDelete(ctx, []string{"b", "c"}))

	s := c.Stats()
	assert.Equal(t, int64(5), s.Hits())
	assert.Equal(t, int64(3), s.Misses())
	assert.Equal(t, int64(3), s.Sets())
	assert.Equal(t, int64(3), s.Deletes())
	assert.Equal(t, int64(8), s.Gets())
	assert.InDelta(t, 5.0/8.0, s.HitRate(), 1e-9)
	assert.InDelta(t, 3.0/8.0, s.MissRate(), 1e-9)
}

// TestStatsDisabledByDefault verifies that without StatsEnabled/WithStats, Stats() stays a zero value regardless of
// activity - the counters are never allocated or touched.
func TestStatsDisabledByDefault(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})

	require.NoError(t, c.Set(ctx, "a", "1"))
	_, _, err := c.Get(ctx, "a")
	require.NoError(t, err)
	_, _, err = c.Get(ctx, "absent")
	require.NoError(t, err)
	require.NoError(t, c.Delete(ctx, "a"))

	assert.Equal(t, memstash.Stats{}, c.Stats(), "Stats() must stay zero when collection was never enabled")
}

// TestWithStatsOption verifies the New(...) option form (as opposed to Config.StatsEnabled used elsewhere in this
// file) turns collection on the same way.
func TestWithStatsOption(t *testing.T) {
	ctx := context.Background()
	c, err := memstash.New[string, string](memstash.WithMemoryCapacity(100), memstash.WithStats())
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.Set(ctx, "a", "1"))
	_, ok, err := c.Get(ctx, "a")
	require.NoError(t, err)
	require.True(t, ok)

	s := c.Stats()
	assert.Equal(t, int64(1), s.Hits())
	assert.Equal(t, int64(1), s.Sets())
}

func TestStatsZeroRates(t *testing.T) {
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100, StatsEnabled: true})
	s := c.Stats()
	assert.Zero(t, s.Gets())
	assert.Zero(t, s.HitRate())
	assert.Zero(t, s.MissRate())
}

func TestStatsGetOrLoad(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100, StatsEnabled: true})

	loader := func(context.Context, string) (string, error) { return "v", nil }
	_, err := c.GetOrLoad(ctx, "k", loader) // miss + set
	require.NoError(t, err)
	_, err = c.GetOrLoad(ctx, "k", loader) // hit
	require.NoError(t, err)

	s := c.Stats()
	assert.Equal(t, int64(1), s.Hits())
	assert.Equal(t, int64(1), s.Misses())
	assert.Equal(t, int64(1), s.Sets(), "the stored loader result counts as a set")
}

func TestStatsCountsL2HitAsHit(t *testing.T) {
	ctx := context.Background()
	l2 := newL2Stub()
	require.NoError(t, l2.Set(ctx, "only-l2", "v", 0))
	c := newCache(t, memstash.Config[string, string]{
		MemoryCapacity: 100, L2Cache: l2, WritePolicy: memstash.WriteThrough, StatsEnabled: true,
	})

	_, ok, err := c.Get(ctx, "only-l2")
	require.NoError(t, err)
	require.True(t, ok)

	s := c.Stats()
	assert.Equal(t, int64(1), s.Hits(), "a value found in L2 is a hit")
	assert.Zero(t, s.Misses())
	assert.Zero(t, s.Sets(), "the L2-to-memory promotion is not a set")

	// The batch loader path: one key from L2 (hit), one from the loader (miss + set).
	require.NoError(t, l2.Set(ctx, "batch-l2", "v", 0))
	loader := func(_ context.Context, keys []string) (memstash.List[string, string], error) {
		loaded := make(memstash.List[string, string], 0, len(keys))
		for _, key := range keys {
			loaded = append(loaded, memstash.KeyVal[string, string]{Key: key, Value: "v"})
		}
		return loaded, nil
	}
	_, err = c.BatchGetOrLoad(ctx, []string{"batch-l2", "loaded"}, loader)
	require.NoError(t, err)

	s = c.Stats()
	assert.Equal(t, int64(2), s.Hits())
	assert.Equal(t, int64(1), s.Misses())
	assert.Equal(t, int64(1), s.Sets())
}
