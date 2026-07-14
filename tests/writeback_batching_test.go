package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
)

// gatedL2 blocks inside every Set/BatchSet until the test permits it, so the test controls exactly what is queued in
// the write-back buffer when the worker drains it. Each call signals entered and waits for one release token.
type gatedL2 struct {
	mu               sync.Mutex
	m                map[string]string
	setCalls         int
	batchCalls       int
	batchSizes       []int
	deleteCalls      int
	batchDeleteCalls int
	batchDeleteSizes []int
	entered          chan struct{}
	release          chan struct{}
}

func newGatedL2() *gatedL2 {
	return &gatedL2{m: map[string]string{}, entered: make(chan struct{}), release: make(chan struct{})}
}

func (g *gatedL2) pass() { // wait for the worker to enter a write and let it finish
	<-g.entered
	g.release <- struct{}{}
}

func (g *gatedL2) Get(_ context.Context, key string) (string, bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	v, ok := g.m[key]
	return v, ok, nil
}

func (g *gatedL2) BatchGet(_ context.Context, keys []string) (memstash.List[string, string], error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	found := make(memstash.List[string, string], 0, len(keys))
	for _, key := range keys {
		if v, ok := g.m[key]; ok {
			found = append(found, memstash.KeyVal[string, string]{Key: key, Value: v})
		}
	}
	return found, nil
}

func (g *gatedL2) Set(_ context.Context, key, value string, _ time.Duration) error {
	g.entered <- struct{}{}
	<-g.release
	g.mu.Lock()
	defer g.mu.Unlock()
	g.setCalls++
	g.m[key] = value
	return nil
}

func (g *gatedL2) BatchSet(_ context.Context, items memstash.List[string, string], _ time.Duration) error {
	g.entered <- struct{}{}
	<-g.release
	g.mu.Lock()
	defer g.mu.Unlock()
	g.batchCalls++
	g.batchSizes = append(g.batchSizes, len(items))
	for _, item := range items {
		g.m[item.Key] = item.Value
	}
	return nil
}

func (g *gatedL2) Delete(_ context.Context, key string) error {
	g.entered <- struct{}{}
	<-g.release
	g.mu.Lock()
	defer g.mu.Unlock()
	g.deleteCalls++
	delete(g.m, key)
	return nil
}

func (g *gatedL2) BatchDelete(_ context.Context, keys []string) error {
	g.entered <- struct{}{}
	<-g.release
	g.mu.Lock()
	defer g.mu.Unlock()
	g.batchDeleteCalls++
	g.batchDeleteSizes = append(g.batchDeleteSizes, len(keys))
	for _, key := range keys {
		delete(g.m, key)
	}
	return nil
}

func (g *gatedL2) counters() (sets, batches int, sizes []int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.setCalls, g.batchCalls, append([]int(nil), g.batchSizes...)
}

func (g *gatedL2) deleteCounters() (deletes, batches int, sizes []int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.deleteCalls, g.batchDeleteCalls, append([]int(nil), g.batchDeleteSizes...)
}

// newWriteBackCache builds a WriteBack cache with an 8-slot buffer over the gated stub.
func newWriteBackCache(t *testing.T, l2 *gatedL2, opts ...memstash.Option) *memstash.Cache[string, string] {
	t.Helper()
	opts = append(opts,
		memstash.WithMemoryCapacity(100),
		memstash.WithL2Cache[string, string](l2),
		memstash.WithWritePolicy(memstash.WriteBack),
		memstash.WithWriteBackBuffer(8),
	)
	c, err := memstash.New[string, string](opts...)
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}

// stall enqueues the first write and waits until the worker is blocked inside the stub on it, so everything the
// test enqueues next stays in the buffer.
func stall(t *testing.T, c *memstash.Cache[string, string], l2 *gatedL2) {
	t.Helper()
	require.NoError(t, c.Set(context.Background(), "k-stall", "v"))
	<-l2.entered
}

// TestWriteBackBatchingModes drives the write-back worker through a controlled queue and checks which L2 calls each
// WriteBackBatching mode produces.
func TestWriteBackBatchingModes(t *testing.T) {
	ctx := context.Background()

	t.Run("full batching is the default and coalesces the queue", func(t *testing.T) {
		l2 := newGatedL2()
		c := newWriteBackCache(t, l2)

		stall(t, c, l2)
		for _, key := range []string{"a", "b", "c", "d", "e", "f"} {
			require.NoError(t, c.Set(ctx, key, "v"))
		}
		l2.release <- struct{}{} // the stalled single completes
		l2.pass()                // the queued six drain as one BatchSet
		c.Wait()

		sets, batches, sizes := l2.counters()
		assert.Equal(t, 1, sets, "only the stalled first write goes through Set")
		assert.Equal(t, 1, batches)
		assert.Equal(t, []int{6}, sizes)
	})

	t.Run("no batching sends one Set per write", func(t *testing.T) {
		l2 := newGatedL2()
		c := newWriteBackCache(t, l2, memstash.WithNoBatchingForWriteBack())

		stall(t, c, l2)
		for _, key := range []string{"a", "b", "c", "d", "e", "f"} {
			require.NoError(t, c.Set(ctx, key, "v"))
		}
		l2.release <- struct{}{}
		for i := 0; i < 6; i++ {
			l2.pass()
		}
		c.Wait()

		sets, batches, _ := l2.counters()
		assert.Equal(t, 7, sets)
		assert.Equal(t, 0, batches)
	})

	t.Run("adaptive batches over half the buffer and goes per-item under it", func(t *testing.T) {
		l2 := newGatedL2()
		c := newWriteBackCache(t, l2, memstash.WithAdaptiveBatchingForWriteBack())

		// Six queued: after the worker pops the first, five remain - more than half of the 8-slot buffer.
		stall(t, c, l2)
		for _, key := range []string{"a", "b", "c", "d", "e", "f"} {
			require.NoError(t, c.Set(ctx, key, "v"))
		}
		l2.release <- struct{}{}
		l2.pass() // one BatchSet of six
		c.Wait()

		// Two queued: at most half the buffer stays after the pop - per-item Sets.
		stall(t, c, l2)
		require.NoError(t, c.Set(ctx, "g", "v"))
		require.NoError(t, c.Set(ctx, "h", "v"))
		l2.release <- struct{}{}
		l2.pass()
		l2.pass()
		c.Wait()

		sets, batches, sizes := l2.counters()
		assert.Equal(t, 4, sets, "the two stalled firsts plus the two under-half writes")
		assert.Equal(t, 1, batches)
		assert.Equal(t, []int{6}, sizes)
	})
}
