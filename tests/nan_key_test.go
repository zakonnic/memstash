package tests

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
)

// A NaN key hashes differently every time: stored, it could never be found, and the table's bookkeeping, which rehashes
// a far-displaced item to find its home, would charge it to a random group and lose a live key there.
func TestNaNKeyStaysOutOfMemory(t *testing.T) {
	ctx := context.Background()
	c, err := memstash.New[float64, int](memstash.WithMemoryCapacity(1000), memstash.WithShardsCount(1))
	require.NoError(t, err)
	defer c.Close()

	for k := range 500 {
		require.NoError(t, c.Set(ctx, float64(k), k))
		require.NoError(t, c.Set(ctx, math.NaN(), k))
	}
	require.Equal(t, 500, c.Len())
	for k := range 500 {
		v, ok := c.GetFromMemory(float64(k))
		require.True(t, ok, k)
		require.Equal(t, k, v)
	}
}
