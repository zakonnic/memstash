package tests

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
)

type paddedPair struct {
	_    [0]uint64 // word-sized and word-aligned, yet two stores
	A, B uint32
}

// TestGetConcurrentOverwriteNotTorn overwrites one key in place while readers Get it: every read must be one of the
// two values whole. A string is two words, and on a 32-bit platform those are 8 bytes, which up to 0.9.8 skipped the
// seqlock bracket: a reader paired one string's pointer with the other's length. The padded pair is the case size and
// alignment cannot tell from a uint64.
func TestGetConcurrentOverwriteNotTorn(t *testing.T) {
	t.Run("string", func(t *testing.T) {
		overwriteNotTorn(t, "a", strings.Repeat("b", 64))
	})
	t.Run("padded pair", func(t *testing.T) {
		overwriteNotTorn(t, paddedPair{A: 1, B: 1}, paddedPair{A: 2, B: 2})
	})
}

func overwriteNotTorn[V comparable](t *testing.T, a, b V) {
	ctx := context.Background()
	c, err := memstash.New[int, V](memstash.WithShardsCount(1))
	require.NoError(t, err)
	t.Cleanup(c.Close)
	require.NoError(t, c.Set(ctx, 1, a))

	var stop atomic.Bool
	var reads, torn atomic.Int64
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				v, ok, _ := c.Get(ctx, 1)
				reads.Add(1)
				if ok && v != a && v != b {
					torn.Add(1)
				}
			}
		}()
	}
	for i := range 500_000 {
		value := a
		if i&1 == 0 {
			value = b
		}
		_ = c.Set(ctx, 1, value)
	}
	stop.Store(true)
	wg.Wait()

	assert.Zero(t, torn.Load(), "a read mixed two writes")
	assert.Greater(t, reads.Load(), int64(1000), "readers never got going: the test proves nothing")
}
