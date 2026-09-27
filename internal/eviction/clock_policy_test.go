package eviction

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClockSecondChance: a touched item loses its chance and goes to the back instead of being evicted.
func TestClockSecondChance(t *testing.T) {
	pool := newArena()
	p := NewClockPolicy(&pool.items)
	for _, key := range []string{"a", "b", "c"} {
		pool.add(p, key)
	}
	pool.touch("a")

	var victims []string
	for range 3 {
		key, ok := pool.evict(p)
		require.True(t, ok)
		victims = append(victims, key)
	}
	assert.Equal(t, []string{"b", "c", "a"}, victims)
	_, ok := pool.evict(p)
	assert.False(t, ok, "an empty policy must report nothing to evict")
}
