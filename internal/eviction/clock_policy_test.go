package eviction

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zakonnic/memstash/internal/itemstate"
)

// addFromPool claims a record for the key from the pool and registers its node with the policy, mirroring what
// Cache.setMemory does. Returns the node's pool index.
func addFromPool(p Policy[string, string], pool *itemstate.Pool[string, string], key string) uint32 {
	_, _, idx := pool.Claim(key, "v", 0)
	p.Add(itemstate.QNode{Idx: idx, Cost: 1})
	return idx
}

func TestClockPolicyBytes(t *testing.T) {
	var pool itemstate.Pool[string, string]
	p := NewClockPolicy(&pool)
	emptyBytes := p.Bytes()

	for i := 0; i < 200; i++ {
		addFromPool(p, &pool, fmt.Sprintf("k%d", i))
	}

	assert.Equal(t, emptyBytes+p.q.Bytes(), p.Bytes(), "ClockPolicy.Bytes must be its fixed size plus its queue")
	assert.Positive(t, p.q.Bytes(), "the queue must have allocated at least one chunk")
}
