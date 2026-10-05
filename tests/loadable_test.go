package tests

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zakonnic/memstash"
)

// TestLoadableCacheDirectAccess covers the plain passthrough methods of LoadableCache (Get/Set/Delete/BatchGet/
// BatchSet/Wait): they bypass the loader entirely and just forward to the underlying Cache.
func TestLoadableCacheDirectAccess(t *testing.T) {
	ctx := context.Background()
	lc, err := memstash.NewLoadable(
		func(_ context.Context, key string) (string, error) { return "loaded:" + key, nil },
		memstash.WithMemoryCapacity(10),
	)
	require.NoError(t, err)
	defer lc.Close()

	require.NoError(t, lc.Set(ctx, "k", "v"))
	v, ok, err := lc.Get(ctx, "k")
	require.NoError(t, err)
	require.True(t, ok, "Get after Set")
	assert.Equal(t, "v", v)

	require.NoError(t, lc.Delete(ctx, "k"))
	_, ok, err = lc.Get(ctx, "k")
	require.NoError(t, err)
	assert.False(t, ok, "value survived Delete")

	require.NoError(t, lc.BatchSet(ctx, memstash.List[string, string]{
		{Key: "a", Value: "1"}, {Key: "b", Value: "2"},
	}))
	found, err := lc.BatchGet(ctx, []string{"a", "b", "missing"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "1", "b": "2"}, found.ToMap())

	lc.Wait(context.Background()) // no L2/WriteBack configured: must return immediately, not hang
}

// TestNewBatchLoadable covers the batch-loader constructor: GetOrLoad and BatchGetOrLoad both go through the
// user-supplied BatchLoaderFunc directly (no per-key loader is synthesized, unlike NewLoadable).
func TestNewBatchLoadable(t *testing.T) {
	ctx := context.Background()
	var calls atomic.Int32
	lc, err := memstash.NewBatchLoadable(
		func(_ context.Context, keys []string) (memstash.List[string, string], error) {
			calls.Add(1)
			found := make(memstash.List[string, string], 0, len(keys))
			for _, k := range keys {
				found = append(found, memstash.KeyVal[string, string]{Key: k, Value: "batch:" + k})
			}
			return found, nil
		},
		memstash.WithMemoryCapacity(10),
	)
	require.NoError(t, err)
	defer lc.Close()

	v, err := lc.GetOrLoad(ctx, "x")
	require.NoError(t, err)
	assert.Equal(t, "batch:x", v)
	assert.EqualValues(t, 1, calls.Load())

	// "x" is now cached; only "y" is actually missing and reaches the batch loader.
	found, err := lc.BatchGetOrLoad(ctx, []string{"x", "y"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"x": "batch:x", "y": "batch:y"}, found.ToMap())
	assert.EqualValues(t, 2, calls.Load())

	t.Run("nil loader", func(t *testing.T) {
		_, err := memstash.NewBatchLoadable[string, string](nil, memstash.WithMemoryCapacity(1))
		require.ErrorIs(t, err, memstash.ErrNilLoader)
	})

	t.Run("loader error propagates", func(t *testing.T) {
		boom := errors.New("boom")
		failing, err := memstash.NewBatchLoadable(
			func(_ context.Context, _ []string) (memstash.List[string, string], error) { return nil, boom },
			memstash.WithMemoryCapacity(1),
		)
		require.NoError(t, err)
		defer failing.Close()

		_, err = failing.GetOrLoad(ctx, "z")
		require.ErrorIs(t, err, boom)
	})
}

// A key the source does not have is ErrKeyNotFound whichever loader shape says so, and it is cached in neither level:
// a zero value written for it would outlive a restart in L2.
func TestLoadersNotFound(t *testing.T) {
	ctx := context.Background()
	known := map[string]string{"a": "va"}

	t.Run("batch loader behind GetOrLoad", func(t *testing.T) {
		l2 := newL2Stub()
		lc, err := memstash.NewBatchLoadable(
			func(_ context.Context, keys []string) (memstash.List[string, string], error) {
				found := memstash.List[string, string]{}
				for _, key := range keys {
					if v, ok := known[key]; ok {
						found = append(found, memstash.KeyVal[string, string]{Key: key, Value: v})
					}
				}
				return found, nil
			},
			memstash.WithL2Cache[string, string](l2), memstash.WithWritePolicy(memstash.WriteThrough),
		)
		require.NoError(t, err)
		defer lc.Close()

		_, err = lc.GetOrLoad(ctx, "ghost")
		require.ErrorIs(t, err, memstash.ErrKeyNotFound)
		_, ok := lc.GetFromMemory("ghost")
		assert.False(t, ok, "a missing key must not be cached")
		_, ok = l2.snapshot("ghost")
		assert.False(t, ok, "a missing key must not be written to L2")
	})

	t.Run("single loader behind BatchGetOrLoad", func(t *testing.T) {
		lc, err := memstash.NewLoadable(func(_ context.Context, key string) (string, error) {
			if v, ok := known[key]; ok {
				return v, nil
			}
			return "", memstash.ErrKeyNotFound
		})
		require.NoError(t, err)
		defer lc.Close()

		found, err := lc.BatchGetOrLoad(ctx, []string{"ghost", "a"})
		require.NoError(t, err, "a missing key is absent from the result, not a failed batch")
		assert.Equal(t, map[string]string{"a": "va"}, found.ToMap())
		_, ok := lc.GetFromMemory("ghost")
		assert.False(t, ok)
	})

	t.Run("GetOrLoad waiting on a batch that omits its key", func(t *testing.T) {
		c, err := memstash.New[string, string](memstash.WithStats())
		require.NoError(t, err)
		defer c.Close()

		entered, gate := make(chan struct{}), make(chan struct{})
		done := make(chan error, 1)
		go func() {
			_, err := c.BatchGetOrLoad(ctx, []string{"ghost"},
				func(context.Context, []string) (memstash.List[string, string], error) {
					close(entered)
					<-gate
					return nil, nil
				})
			done <- err
		}()
		<-entered
		misses := func() int64 { stats := c.Stats(); return stats.MemoryMisses() }
		base := misses()
		waiter := make(chan error, 1)
		go func() {
			_, err := c.GetOrLoad(ctx, "ghost", func(context.Context, string) (string, error) {
				return "", errors.New("a flight was running: this call had to join it")
			})
			waiter <- err
		}()
		require.Eventually(t, func() bool { return misses() > base }, 5*time.Second, time.Millisecond)
		close(gate)
		require.NoError(t, <-done)
		assert.ErrorIs(t, <-waiter, memstash.ErrKeyNotFound)
	})
}
