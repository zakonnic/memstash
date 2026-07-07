package eviction

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zakonnic/memstash/internal/itemstate"
)

// TestS3FIFOPolicyBytes verifies that Bytes is the sum of its three parts (small, main and the ghost ring) by
// comparing it against those unexported fields directly, then forces evictions so the ghost ring actually holds
// something - otherwise the ghost term of the sum would never be exercised.
func TestS3FIFOPolicyBytes(t *testing.T) {
	p := NewS3FIFO[string](1000, 100)
	// The ghost ring pre-allocates its full ring capacity at construction (unlike the queues, which grow chunks
	// lazily), so a freshly built policy already reports its ring size, not zero.
	assert.Equal(t, p.ghost.bytes(), p.Bytes(), "empty policy: only the pre-allocated ghost ring counts")

	for i := 0; i < 200; i++ {
		p.Add(itemstate.QNode[string]{State: newState(fmt.Sprintf("k%d", i)), Cost: 1})
	}

	// None of these fresh states were ever touched (no second chance), so every eviction from small kills the item
	// and sends its key to ghost rather than promoting it to main.
	for i := 0; i < 150; i++ {
		_, ok := p.Evict(0)
		require.True(t, ok, "eviction %d: small must still have items to evict", i)
	}
	require.Positive(t, p.ghost.size, "test setup: expected evictions to have populated the ghost ring")

	assert.Equal(t, p.small.Bytes()+p.main.Bytes()+p.ghost.bytes(), p.Bytes(),
		"S3FIFOPolicy.Bytes must be the sum of small, main and ghost")
	assert.Positive(t, p.Bytes())
}
