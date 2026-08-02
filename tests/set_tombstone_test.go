package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
)

// TestSetReusesTombstoneOnlyWhenKeyAbsent drives the exact shape setMemory's tombstone reuse depends on: a freed
// tombstone sitting earlier in a key's probe chain than the key's own live slot.
func TestSetReusesTombstoneOnlyWhenKeyAbsent(t *testing.T) {
	ctx := context.Background()
	const capacity = 1024
	c, err := memstash.New[int, int](memstash.WithMemoryCapacity(capacity), memstash.WithShardsCount(1))
	require.NoError(t, err)
	defer c.Close()

	live := map[int]bool{}
	for i := range capacity { // fill to capacity
		require.NoError(t, c.Set(ctx, i, i))
		live[i] = true
	}
	for i := 0; i < capacity/2; i++ { // tombstones in the low half of the key space
		c.Delete(ctx, i)
		delete(live, i)
	}
	for i := 2000; i < 2000+capacity/2; i++ { // churn: the queue walks past the tombstones and frees their slots
		require.NoError(t, c.Set(ctx, i, i))
		live[i] = true
	}

	before := c.Len()
	for k := range live { // rewrite every surviving key: each may now meet a freed tombstone before its own slot
		require.NoError(t, c.Set(ctx, k, k*10))
	}
	t.Logf("live keys %d, Len before rewrite %d, after %d", len(live), before, c.Len())
	assert.LessOrEqual(t, c.Len(), before, "rewriting existing keys must not add items")

	// The user-visible consequence: Delete removes the first copy, a second one keeps serving the old value.
	resurrected := 0
	for k := range live {
		c.Delete(ctx, k)
		if v, ok, err := c.Get(ctx, k); err == nil && ok {
			t.Logf("key %d survived Delete with value %d", k, v)
			resurrected++
		}
	}
	assert.Zero(t, resurrected, "a deleted key must not be served from a second slot")
}
