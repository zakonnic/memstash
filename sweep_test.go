package memstash

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/zakonnic/memstash/internal/itemstore"
)

// An expired item that nothing reads or evicts must be gone before its offset wraps: ExpireMax+2 units past the
// deadline it would read as live again.
func TestSweepRemovesExpiredBeforeTheOffsetWraps(t *testing.T) {
	ctx := context.Background()
	c, err := New[string, int](WithMemoryCapacity(100), WithTTL(24*time.Hour), WithShardsCount(1))
	require.NoError(t, err)
	c.Close() // stops the clock: the test moves it

	c.nowOff.Store(1000)
	require.NoError(t, c.Set(ctx, "k", 42))
	deadline := c.expireOffset()
	c.nowOff.Store(deadline - 1)
	require.NoError(t, c.Set(ctx, "live", 1))

	for off := deadline; off < deadline+sweepUnits; off++ { // one pass, a unit at a time
		c.nowOff.Store(off)
		c.sweep(off, 1)
	}
	assert.Equal(t, 1, c.Len(), "a pass left the expired item, or took the live one")
	_, ok := c.GetFromMemory("live")
	assert.True(t, ok)

	c.nowOff.Store((deadline + itemstore.ExpireMax + 2) & itemstore.ExpireWrapMask)
	_, ok = c.GetFromMemory("k")
	assert.False(t, ok, "an item expired ExpireMax+2 units ago is served again")
}

// The sweep kills and compacts under the mutex like any delete: readers racing it must still find every live key.
func TestSweepUnderConcurrentChurn(t *testing.T) {
	ctx := context.Background()
	const (
		pinned    = 10_000
		writers   = 4
		churnKeys = 20_000 // per writer
		churnOps  = 100_000
		capacity  = pinned + writers*churnKeys // room for every key: an evicted pinned key would read as a false miss
	)
	var swept atomic.Int64
	c, err := New[uint64, uint64](WithMemoryCapacity(capacity), WithTTL(24*time.Hour), WithShardsCount(1),
		WithOnDeletion(func(_, _ uint64, cause DeletionCause) {
			if cause == CauseExpiration {
				swept.Add(1)
			}
		}))
	require.NoError(t, err)
	c.Close() // stops the clock: the sweeper below moves it

	for k := uint64(0); k < pinned; k++ {
		require.NoError(t, c.Set(ctx, k, k+1))
	}
	stop := make(chan struct{})
	var background sync.WaitGroup
	background.Add(1)
	go func() { // a pass per 64 units, short of the pinned keys' deadline
		defer background.Done()
		start := c.nowOff.Load()
		for off := start; ; off = min(off+1, start+c.ttlOff/2) {
			select {
			case <-stop:
				return
			default:
			}
			c.nowOff.Store(off)
			c.sweep(off, sweepUnits/64)
			time.Sleep(100 * time.Microsecond) // leave the writers the mutex most of the time
		}
	}()
	var falseMisses, wrongValues atomic.Int64
	for r := range 4 {
		background.Add(1)
		go func(seed int64) {
			defer background.Done()
			rng := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-stop:
					return
				default:
				}
				k := uint64(rng.Intn(pinned))
				if v, ok := c.GetFromMemory(k); !ok {
					falseMisses.Add(1)
				} else if v != k+1 {
					wrongValues.Add(1)
				}
			}
		}(int64(r) + 100)
	}
	var writersWG sync.WaitGroup
	for w := range writers {
		writersWG.Add(1)
		go func(base uint64) {
			defer writersWG.Done()
			rng := rand.New(rand.NewSource(int64(base >> 32)))
			for range churnOps {
				k := base + uint64(rng.Intn(churnKeys))
				_ = c.SetWithTTL(ctx, k, k, 2*time.Second)
				if rng.Intn(4) == 0 {
					_ = c.Delete(ctx, k)
				}
			}
		}(uint64(w+1) << 32)
	}
	writersWG.Wait()
	// The churn keys left behind expire as the sweeper moves the clock; the readers race those kills too.
	require.Eventually(t, func() bool { return swept.Load() > 0 }, 10*time.Second, time.Millisecond,
		"the sweep removed nothing: the race was never run")
	close(stop)
	background.Wait()
	require.Zero(t, falseMisses.Load(), "a pinned key was not found by a read racing the sweep")
	require.Zero(t, wrongValues.Load(), "a pinned key was read with a wrong value")
}

// A CostFunc that panics under the sweep costs that sweep, not the shard: the mutex comes back.
func TestSweepSurvivesAPanickingCostFunc(t *testing.T) {
	ctx := context.Background()
	var armed atomic.Bool
	var panics atomic.Int32
	c, err := New[string, int](WithMemoryCapacity(10), WithTTL(time.Hour), WithShardsCount(1),
		WithCostFunc(func(string, int) uint32 {
			if armed.Load() {
				panic("cost")
			}
			return 1
		}),
		WithPanicHandler(func(any, bool) { panics.Add(1) }))
	require.NoError(t, err)
	c.Close() // stops the clock: the test moves it

	c.nowOff.Store(10)
	require.NoError(t, c.Set(ctx, "k", 1))
	armed.Store(true)
	off := 10 + c.ttlOff + 5
	c.nowOff.Store(off)
	c.sweep(off, sweepUnits)
	armed.Store(false)
	require.EqualValues(t, 1, panics.Load())

	done := make(chan struct{})
	go func() {
		_ = c.Set(ctx, "other", 2)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the sweep left the shard locked")
	}
}
