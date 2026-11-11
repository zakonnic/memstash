package memstash

import (
	"context"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCanBeNaN(t *testing.T) {
	type plain struct {
		A, B string
		N    int
	}
	type withFloat struct {
		A string
		F [2]float64
	}
	for _, tc := range []struct {
		t    reflect.Type
		want bool
	}{
		{reflect.TypeFor[string](), false},
		{reflect.TypeFor[int](), false},
		{reflect.TypeFor[plain](), false},
		{reflect.TypeFor[[4]byte](), false},
		{reflect.TypeFor[float64](), true},
		{reflect.TypeFor[complex64](), true},
		{reflect.TypeFor[withFloat](), true},
		{reflect.TypeFor[any](), true},
	} {
		assert.Equal(t, tc.want, canBeNaN(tc.t), tc.t.String())
	}
}

// A NaN key's load is never registered: a bucket's overflow map could never delete it.
func TestNaNKeyLeavesNoFlightBehind(t *testing.T) {
	ctx := context.Background()
	c, err := New[float64, int](WithMemoryCapacity(1000), WithShardsCount(1))
	require.NoError(t, err)
	defer c.Close()

	release := make(chan struct{})
	var held sync.WaitGroup
	for k := range flightSlots { // fill the only bucket's inline slots, so the next load would spill
		held.Add(1)
		go func() {
			defer held.Done()
			_, _ = c.GetOrLoad(ctx, float64(k), func(context.Context, float64) (int, error) { <-release; return 0, nil })
		}()
	}
	bucket := &c.flights[0]
	require.Eventually(t, func() bool {
		bucket.mu.Lock()
		defer bucket.mu.Unlock()
		return bucket.live == 1<<flightSlots-1
	}, 5*time.Second, time.Millisecond)

	for range 100 {
		v, err := c.GetOrLoad(ctx, math.NaN(), func(context.Context, float64) (int, error) { return 1, nil })
		require.NoError(t, err)
		require.Equal(t, 1, v)
	}
	bucket.mu.Lock()
	spilled := len(bucket.spill)
	bucket.mu.Unlock()
	close(release)
	held.Wait()
	require.Zero(t, spilled)
}
