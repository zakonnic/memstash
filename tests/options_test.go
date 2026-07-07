package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zakonnic/memstash"
)

// TestFunctionalOptions exercises the With* option constructors that no other test happens to call directly - most
// tests configure Config fields directly via NewWithConfig instead of going through New(opts...).
func TestFunctionalOptions(t *testing.T) {
	ctx := context.Background()

	t.Run("WithTTL", func(t *testing.T) {
		if testing.Short() {
			t.Skip("slow TTL test")
		}
		c, err := memstash.New[string, string](memstash.WithMemoryCapacity(10), memstash.WithTTL(time.Second))
		require.NoError(t, err)
		defer c.Close()

		require.NoError(t, c.Set(ctx, "k", "v"))
		_, ok, err := c.Get(ctx, "k")
		require.NoError(t, err)
		require.True(t, ok, "value must be present right after Set")

		time.Sleep(3 * time.Second)
		_, ok, err = c.Get(ctx, "k")
		require.NoError(t, err)
		assert.False(t, ok, "WithTTL had no effect: the value did not expire")
	})

	t.Run("WithGhostSize and WithWriteBackBuffer do not break construction or normal use", func(t *testing.T) {
		l2 := newL2Stub()
		c, err := memstash.New[string, string](
			memstash.WithMemoryCapacity(10),
			memstash.WithPolicy(memstash.PolicyS3FIFO),
			memstash.WithGhostSize(4),
			memstash.WithL2Cache[string, string](l2),
			memstash.WithWriteBackBuffer(1),
		)
		require.NoError(t, err)
		defer c.Close()

		require.NoError(t, c.Set(ctx, "k", "v"))
		v, ok, err := c.Get(ctx, "k")
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, "v", v)
	})
}
