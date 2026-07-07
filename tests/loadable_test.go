package tests

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

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

	lc.Wait() // no L2/WriteBack configured: must return immediately, not hang
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
