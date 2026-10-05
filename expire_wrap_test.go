package memstash

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zakonnic/memstash/internal/itemstore"
)

// A new offset that wraps onto 0 ("no TTL") moves one unit earlier. One unit later puts a lifetime at the cap
// (ExpireMax-1 units) past ExpireMax: the item would be expired at birth.
func TestLongestTTLSurvivesTheOffsetWrap(t *testing.T) {
	ctx := context.Background()
	c, err := New[string, int](WithMemoryCapacity(10), WithTTL(itemstore.ExpireMax*time.Second), WithShardsCount(1))
	require.NoError(t, err)
	c.Close() // stops the clock: the test sets it
	require.EqualValues(t, itemstore.ExpireMax-1, c.ttlOff)

	c.nowOff.Store(itemstore.ExpireWrapMask - c.ttlOff) // nowOff + ttlOff + 1 wraps onto 0
	require.NoError(t, c.Set(ctx, "set", 1))
	require.NoError(t, c.SetWithTTL(ctx, "setWithTTL", 1, 48*time.Hour))
	for _, key := range []string{"set", "setWithTTL"} {
		_, ok := c.GetFromMemory(key)
		require.True(t, ok, "%s: expired at birth", key)
	}
}
