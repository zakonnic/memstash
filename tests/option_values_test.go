package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
)

// A negative size or count is rejected, as a negative worker count already was, instead of quietly meaning the
// default.
func TestNegativeOptionValuesAreRejected(t *testing.T) {
	for opt, want := range map[string]struct {
		opt memstash.Option
		err error
	}{
		"ShardsCount":      {memstash.WithShardsCount(-5), memstash.ErrBadShardsCount},
		"GhostSize":        {memstash.WithGhostSize(-1), memstash.ErrBadGhostSize},
		"WriteBackBuffer":  {memstash.WithWriteBackBuffer(-1), memstash.ErrBadBufferSize},
		"GetBatchedBuffer": {memstash.WithGetBatchedBuffer(-1), memstash.ErrBadBufferSize},
	} {
		_, err := memstash.New[string, string](want.opt)
		assert.ErrorIs(t, err, want.err, opt)
	}
}

// WithPreallocationRatio turns preallocation on by itself, 0 included: 0 only picks the default ratio.
func TestPreallocationRatioZeroStillPreallocates(t *testing.T) {
	ctx := context.Background()
	c, err := memstash.New[int, int](memstash.WithMemoryCapacity(10_000), memstash.WithShardsCount(1),
		memstash.WithPreallocationRatio(0), memstash.WithStats())
	require.NoError(t, err)
	defer c.Close()
	for k := range 10_000 {
		require.NoError(t, c.Set(ctx, k, k))
	}
	assert.Zero(t, c.Stats().TableGrowths())
}

// A threshold below the growth line's resolution (1/4096) puts the line at its floor: the table grows as it doubles,
// not on every insert.
func TestTinyGrowthThresholdDoesNotGrowOnEveryInsert(t *testing.T) {
	ctx := context.Background()
	c, err := memstash.New[int, int](memstash.WithMemoryCapacity(1000), memstash.WithShardsCount(1),
		memstash.WithGrowthThreshold(0.0001), memstash.WithStats())
	require.NoError(t, err)
	defer c.Close()
	for k := range 24 {
		require.NoError(t, c.Set(ctx, k, k))
	}
	assert.Less(t, c.Stats().TableGrowths(), int64(24))
}
