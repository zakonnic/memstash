package eviction

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zakonnic/memstash/internal/itemstate"
)

// newState is a bare state record for the policy tests: a fresh generation/chance/expiry-free record is all Add and
// Evict need to exercise the queues.
func newState(key string) *itemstate.State[string] { return &itemstate.State[string]{Key: key} }

func TestClockPolicyBytes(t *testing.T) {
	p := NewClockPolicy[string]()
	assert.Zero(t, p.Bytes(), "empty policy has no queued chunks")

	for i := 0; i < 200; i++ {
		p.Add(itemstate.QNode[string]{State: newState(fmt.Sprintf("k%d", i))})
	}

	assert.Equal(t, p.q.Bytes(), p.Bytes(), "ClockPolicy.Bytes must delegate to its queue")
	assert.Positive(t, p.Bytes(), "the queue must have allocated at least one chunk")
}
