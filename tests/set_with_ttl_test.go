package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
)

// TestSetWithTTLNeedsConfiguredTTL: the expiry scale is fixed at construction and the clock that ages items only
// runs when a TTL is configured, so without one the call must fail instead of silently storing forever.
func TestSetWithTTLNeedsConfiguredTTL(t *testing.T) {
	ctx := context.Background()
	c, err := memstash.New[string, string]()
	require.NoError(t, err)
	defer c.Close()

	err = c.SetWithTTL(ctx, "k", "v", time.Minute)

	require.ErrorIs(t, err, memstash.ErrTTLDisabled)
	_, ok := c.GetFromMemory("k")
	assert.False(t, ok, "a rejected write must not reach memory")
}

// TestSetWithTTLRejectsNonPositive: Set is how you ask for the cache's own TTL; a zero or negative lifetime here is
// a mistake, not a shorthand.
func TestSetWithTTLRejectsNonPositive(t *testing.T) {
	ctx := context.Background()
	c, err := memstash.New[string, string](memstash.WithTTL(time.Hour))
	require.NoError(t, err)
	defer c.Close()

	for _, ttl := range []time.Duration{0, -time.Second} {
		assert.ErrorIs(t, c.SetWithTTL(ctx, "k", "v", ttl), memstash.ErrBadTTL)
	}
	_, ok := c.GetFromMemory("k")
	assert.False(t, ok)
}

// TestSetWithTTLExpiresOnItsOwnSchedule: the entry outlives neither its own lifetime nor, in the other direction,
// the cache's - the point of the method is that the two are independent.
func TestSetWithTTLExpiresOnItsOwnSchedule(t *testing.T) {
	ctx := context.Background()
	c, err := memstash.New[string, string](memstash.WithTTL(time.Hour))
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.SetWithTTL(ctx, "short", "v", time.Second))
	require.NoError(t, c.Set(ctx, "cache-ttl", "v"))

	_, ok := c.GetFromMemory("short")
	require.True(t, ok, "still fresh right after the write")

	require.True(t, waitFor(t, func() bool {
		_, ok := c.GetFromMemory("short")
		return !ok
	}), "the one-second entry must expire while the cache's own hour is still running")

	_, ok = c.GetFromMemory("cache-ttl")
	assert.True(t, ok, "an entry written with Set keeps the cache's TTL")
}

// TestSetWithTTLOutlivesTheCacheTTL: a per-entry lifetime longer than the configured one is allowed - the scale, not
// the cache's TTL, is the ceiling.
func TestSetWithTTLOutlivesTheCacheTTL(t *testing.T) {
	ctx := context.Background()
	c, err := memstash.New[string, string](memstash.WithTTL(time.Second))
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.Set(ctx, "plain", "v"))
	require.NoError(t, c.SetWithTTL(ctx, "long", "v", time.Hour))

	require.True(t, waitFor(t, func() bool {
		_, ok := c.GetFromMemory("plain")
		return !ok
	}), "the cache's own one-second TTL must expire the plain entry")

	_, ok := c.GetFromMemory("long")
	assert.True(t, ok, "the hour-long entry must still be there")
}

// TestSetWithTTLRefreshOnGetUsesCacheTTL: documented behaviour - a read under WithRefreshTTLOnGet extends the entry
// by the cache's TTL, so the custom lifetime holds only until the first refresh.
func TestSetWithTTLRefreshOnGetUsesCacheTTL(t *testing.T) {
	ctx := context.Background()
	c, err := memstash.New[string, string](
		memstash.WithTTL(time.Hour),
		memstash.WithRefreshTTLOnGet(),
	)
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.SetWithTTL(ctx, "k", "v", 2*time.Second))
	_, ok := c.GetFromMemory("k") // the refresh happens here
	require.True(t, ok)

	time.Sleep(3 * time.Second) // past the custom lifetime, far short of the cache's hour

	_, ok = c.GetFromMemory("k")
	assert.True(t, ok, "the read extended the entry by the cache's TTL, not by its own")
}
