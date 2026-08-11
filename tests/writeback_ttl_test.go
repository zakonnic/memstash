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

// ttlL2 is gatedL2 for lifetimes: it records the ttl every write-back call arrives with, so a test can see how the
// worker split its batches.
type ttlL2 struct {
	gatedL2
	mu    sync.Mutex
	calls []ttlCall
}

// ttlCall is one L2 write: how many items it carried and the lifetime they were given.
type ttlCall struct {
	size int
	ttl  time.Duration
}

func newTTLL2() *ttlL2 {
	return &ttlL2{gatedL2: gatedL2{m: map[string]string{}, entered: make(chan struct{}), release: make(chan struct{})}}
}

func (g *ttlL2) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	err := g.gatedL2.Set(ctx, key, value, ttl)
	g.mu.Lock()
	defer g.mu.Unlock()
	g.calls = append(g.calls, ttlCall{size: 1, ttl: ttl})
	return err
}

func (g *ttlL2) BatchSet(ctx context.Context, items memstash.List[string, string], ttl time.Duration) error {
	err := g.gatedL2.BatchSet(ctx, items, ttl)
	g.mu.Lock()
	defer g.mu.Unlock()
	g.calls = append(g.calls, ttlCall{size: len(items), ttl: ttl})
	return err
}

func (g *ttlL2) recorded() []ttlCall {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]ttlCall(nil), g.calls...)
}

func newTTLWriteBackCache(t *testing.T, l2 *ttlL2) *memstash.Cache[string, string] {
	t.Helper()
	c, err := memstash.New[string, string](
		memstash.WithMemoryCapacity(100),
		memstash.WithTTL(time.Hour),
		memstash.WithL2Cache[string, string](l2),
		memstash.WithWritePolicy(memstash.WriteBack),
		memstash.WithWriteBackBuffer(16),
		memstash.WithWriteBackWorkers(1), // one queue, so the writes below stay in the order the test enqueues them
	)
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}

// TestWriteBackCarriesPerEntryTTL: a lifetime set by SetWithTTL must reach L2 as that lifetime, not as the cache's.
func TestWriteBackCarriesPerEntryTTL(t *testing.T) {
	ctx := context.Background()
	l2 := newTTLL2()
	c := newTTLWriteBackCache(t, l2)

	require.NoError(t, c.SetWithTTL(ctx, "k", "v", 5*time.Minute))
	<-l2.entered
	l2.release <- struct{}{}
	c.Wait()

	require.Len(t, l2.recorded(), 1)
	assert.Equal(t, ttlCall{size: 1, ttl: 5 * time.Minute}, l2.recorded()[0])
}

// TestWriteBackSplitsBatchesByTTL: BatchSet takes one lifetime for the whole batch, so a run of queued writes has to
// end wherever the ttl changes - and each piece must arrive with its own.
func TestWriteBackSplitsBatchesByTTL(t *testing.T) {
	ctx := context.Background()
	l2 := newTTLL2()
	c := newTTLWriteBackCache(t, l2)

	// Stall the worker inside the first write so everything below piles up in the buffer behind it.
	require.NoError(t, c.Set(ctx, "stall", "v"))
	<-l2.entered

	require.NoError(t, c.Set(ctx, "a", "v"))
	require.NoError(t, c.Set(ctx, "b", "v"))
	require.NoError(t, c.SetWithTTL(ctx, "c", "v", time.Minute))
	require.NoError(t, c.SetWithTTL(ctx, "d", "v", time.Minute))
	require.NoError(t, c.SetWithTTL(ctx, "e", "v", 2*time.Minute))
	require.NoError(t, c.Set(ctx, "f", "v"))

	l2.release <- struct{}{} // the stalled write completes
	for range 4 {            // the queue drains as four runs, one per lifetime stretch
		l2.pass()
	}
	c.Wait()

	assert.Equal(t, []ttlCall{
		{size: 1, ttl: time.Hour},       // the stalled write, on the cache's own TTL
		{size: 2, ttl: time.Hour},       // a, b
		{size: 2, ttl: time.Minute},     // c, d
		{size: 1, ttl: 2 * time.Minute}, // e
		{size: 1, ttl: time.Hour},       // f
	}, l2.recorded())
}
