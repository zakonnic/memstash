package eviction

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClockPolicyBytes(t *testing.T) {
	pool := newArena()
	p := NewClockPolicy(&pool.items)
	emptyBytes := p.Bytes()

	for i := 0; i < 200; i++ {
		addFromPool(p, pool, fmt.Sprintf("k%d", i))
	}

	assert.Equal(t, emptyBytes+p.q.Bytes(), p.Bytes(), "ClockPolicy.Bytes must be its fixed size plus its queue")
	assert.Positive(t, p.q.Bytes(), "the queue must have allocated at least one chunk")
}
