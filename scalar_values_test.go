package memstash

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestScalarValuesFollowsV pins that New classifies the value type, not the key type: a cache that lets a two-store
// value skip the seqlock bracket returns torn values, and the stress tests catch that only now and then.
func TestScalarValuesFollowsV(t *testing.T) {
	type paddedHalves struct {
		_    [0]uint64
		A, B uint32
	}
	assert.True(t, scalarValues[string, uintptr](t))
	assert.False(t, scalarValues[uintptr, string](t))
	assert.False(t, scalarValues[int, paddedHalves](t))
}

func scalarValues[K comparable, V any](t *testing.T) bool {
	c, err := New[K, V]()
	require.NoError(t, err)
	defer c.Close()
	return c.scalarValues
}
