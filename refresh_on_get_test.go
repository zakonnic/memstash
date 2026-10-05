package memstash

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A hit extends the TTL under RefreshTTLOnGet whichever path served it - the fallbacks a read racing a reclaim takes
// included.
func TestRefreshTTLOnGetOnEveryHitPath(t *testing.T) {
	ctx := context.Background()
	c, err := New[string, int](WithMemoryCapacity(10), WithTTL(time.Hour), WithRefreshTTLOnGet(), WithShardsCount(1))
	require.NoError(t, err)
	c.Close() // stops the clock: the test moves it

	paths := map[string]func(key string) (int, bool){
		"getMemory": c.getMemory,
		"getMoved": func(key string) (int, bool) {
			sh, keyHash := c.shardAndHash(key)
			return c.getMoved(sh, keyHash, key)
		},
		"getLocked": func(key string) (int, bool) {
			sh, keyHash := c.shardAndHash(key)
			return c.getLocked(sh, keyHash, key)
		},
	}
	for path, get := range paths {
		c.nowOff.Store(1000)
		require.NoError(t, c.Set(ctx, path, 1))
		c.nowOff.Store(1000 + c.ttlOff/2)
		_, ok := get(path)
		require.True(t, ok, path)
		c.nowOff.Store(1000 + c.ttlOff + 2) // past the deadline the Set gave, short of the one the hit gave
		_, ok = c.getMemory(path)
		require.True(t, ok, "%s: the hit did not extend the TTL", path)
	}
}
