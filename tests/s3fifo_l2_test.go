package tests

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
)

// TestS3FIFOGhostPromotion: a key evicted from small and inserted again lands in main (via ghost) and survives further
// evictions from small.
func TestS3FIFOGhostPromotion(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{
		MemoryCapacity: 100,
		Policy:         memstash.PolicyS3FIFO,
		Shards:         1, // deterministic eviction order
	})

	require.NoError(t, c.Set(ctx, "A", "v"))
	// Flood of one-hit keys: A (never accessed) is evicted from small into ghost.
	for i := 0; i < 200; i++ {
		require.NoError(t, c.Set(ctx, fmt.Sprintf("f%d", i), "v"))
	}
	_, ok := c.GetFromMemory("A")
	require.False(t, ok, "A should have been evicted from small")

	// Re-insertion: a ghost hit sends it straight to main.
	require.NoError(t, c.Set(ctx, "A", "v"))
	// More flooding: evictions come from the overflowing small, main is left untouched.
	for i := 200; i < 260; i++ {
		require.NoError(t, c.Set(ctx, fmt.Sprintf("f%d", i), "v"))
	}
	_, ok = c.GetFromMemory("A")
	assert.True(t, ok, "A in main must not be evicted by a flood of one-hit keys")
}

// TestS3FIFOHotSurvivesFlood: hot keys that receive accesses are promoted to main and survive a long stream of one-hit
// wonders.
func TestS3FIFOHotSurvivesFlood(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{
		MemoryCapacity: 100,
		Policy:         memstash.PolicyS3FIFO,
		Shards:         1, // deterministic eviction order
	})

	hot := make([]string, 10)
	for i := range hot {
		hot[i] = fmt.Sprintf("hot%d", i)
		require.NoError(t, c.Set(ctx, hot[i], "v"))
	}
	for i := 0; i < 2000; i++ {
		require.NoError(t, c.Set(ctx, fmt.Sprintf("cold%d", i), "v"))
		for _, hotKey := range hot {
			c.GetFromMemory(hotKey)
		}
	}
	for _, hotKey := range hot {
		_, ok := c.GetFromMemory(hotKey)
		assert.True(t, ok, "hot key %s was evicted by the one-hit stream", hotKey)
	}
}

func TestGetOrLoadSingleflight(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})

	var calls atomic.Int32
	loader := func(_ context.Context, key string) (string, error) {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond)
		return "loaded:" + key, nil
	}

	const goroutines = 64
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := c.GetOrLoad(ctx, "key", loader)
			assert.NoError(t, err)
			assert.Equal(t, "loaded:key", v)
		}()
	}
	wg.Wait()
	require.EqualValues(t, 1, calls.Load(), "concurrent misses must be coalesced into one load")

	// The value is cached: the loader is not called anymore.
	_, err := c.GetOrLoad(ctx, "key", loader)
	require.NoError(t, err)
	assert.EqualValues(t, 1, calls.Load(), "loader was called again on a hit")
}

func TestGetOrLoadErrorNotCached(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})

	boom := errors.New("boom")
	var calls atomic.Int32
	failing := func(context.Context, string) (string, error) {
		calls.Add(1)
		return "", boom
	}

	_, err := c.GetOrLoad(ctx, "k", failing)
	require.ErrorIs(t, err, boom)
	_, err = c.GetOrLoad(ctx, "k", failing)
	require.ErrorIs(t, err, boom)
	assert.EqualValues(t, 2, calls.Load(), "the error was cached: the second call must invoke the loader again")
}

// l2Stub is a thread-safe reference implementation of L2Cache for the tests.
type l2Stub struct {
	mu   sync.Mutex
	m    map[string]string
	gets int
	sets int
}

func newL2Stub() *l2Stub { return &l2Stub{m: map[string]string{}} }

func (f *l2Stub) Get(_ context.Context, key string) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	v, ok := f.m[key]
	return v, ok, nil
}

func (f *l2Stub) Set(_ context.Context, key, value string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sets++
	f.m[key] = value
	return nil
}

func (f *l2Stub) BatchGet(_ context.Context, keys []string) (memstash.List[string, string], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	found := make(memstash.List[string, string], 0, len(keys))
	for _, key := range keys {
		if v, ok := f.m[key]; ok {
			found = append(found, memstash.KeyVal[string, string]{Key: key, Value: v})
		}
	}
	return found, nil
}

func (f *l2Stub) BatchSet(_ context.Context, items memstash.List[string, string], _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sets++
	for _, item := range items {
		f.m[item.Key] = item.Value
	}
	return nil
}

func (f *l2Stub) Delete(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.m, key)
	return nil
}

func (f *l2Stub) BatchDelete(_ context.Context, keys []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, key := range keys {
		delete(f.m, key)
	}
	return nil
}

func (f *l2Stub) snapshot(key string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.m[key]
	return v, ok
}

func (f *l2Stub) getCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gets
}

func TestL2WriteThroughAndPromotion(t *testing.T) {
	ctx := context.Background()
	l2 := newL2Stub()

	c1 := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100, L2Cache: l2, WritePolicy: memstash.WriteThrough})
	require.NoError(t, c1.Set(ctx, "k", "v"))
	v, ok := l2.snapshot("k")
	require.True(t, ok, "write-through did not persist the value to L2")
	assert.Equal(t, "v", v)

	// A second instance with the same L2: memory miss, L2 hit, promotion.
	c2 := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100, L2Cache: l2, WritePolicy: memstash.WriteThrough})
	v, ok, err := c2.Get(ctx, "k")
	require.NoError(t, err)
	require.True(t, ok, "Get through L2")
	assert.Equal(t, "v", v)

	before := l2.getCount()
	v, ok, err = c2.Get(ctx, "k")
	require.NoError(t, err)
	require.True(t, ok, "repeated Get")
	assert.Equal(t, "v", v)
	assert.Equal(t, before, l2.getCount(), "value was not promoted into memory: the repeated Get went to L2")

	require.NoError(t, c2.Delete(ctx, "k"))
	_, ok = l2.snapshot("k")
	assert.False(t, ok, "Delete did not reach L2")
}

func TestL2WriteBackFlushOnClose(t *testing.T) {
	ctx := context.Background()
	l2 := newL2Stub()
	c, err := memstash.New[string, string](
		memstash.WithMemoryCapacity(1000),
		memstash.WithL2Cache[string, string](l2),
		memstash.WithWritePolicy(memstash.WriteBack),
	)
	require.NoError(t, err)

	const keys = 500
	for i := 0; i < keys; i++ {
		require.NoError(t, c.Set(ctx, fmt.Sprintf("k%d", i), "v"))
	}
	c.Close() // must flush the buffer

	for i := 0; i < keys; i++ {
		_, ok := l2.snapshot(fmt.Sprintf("k%d", i))
		assert.True(t, ok, "write-back lost key k%d", i)
	}
}

// TestWriteBackWait: Wait is a checkpoint of the asynchronous write-back stream - after it returns, every earlier Set
// is visible in L2, while the cache keeps running (unlike Close).
func TestWriteBackWait(t *testing.T) {
	ctx := context.Background()
	l2 := newL2Stub()
	c, err := memstash.New[string, string](
		memstash.WithMemoryCapacity(1000),
		memstash.WithL2Cache[string, string](l2),
		memstash.WithWritePolicy(memstash.WriteBack),
	)
	require.NoError(t, err)
	defer c.Close()

	const keys = 500
	for i := 0; i < keys; i++ {
		require.NoError(t, c.Set(ctx, fmt.Sprintf("k%d", i), "v"))
	}

	// Wait and verify from a separate goroutine: it must not need the test goroutine or a stopped cache.
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Wait()
		for i := 0; i < keys; i++ {
			_, ok := l2.snapshot(fmt.Sprintf("k%d", i))
			assert.True(t, ok, "write k%d is not in L2 after Wait", i)
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Wait did not return")
	}

	// The cache keeps accepting writes after Wait, and a second Wait flushes them too.
	require.NoError(t, c.Set(ctx, "later", "v"))
	c.Wait()
	_, ok := l2.snapshot("later")
	assert.True(t, ok, "the write issued after the first Wait was not flushed by the second one")
}

// TestWaitIsNoopWithoutWriteBack: with no write-back worker (WriteThrough, no L2, or a closed cache) Wait must return
// immediately instead of hanging.
func TestWaitIsNoopWithoutWriteBack(t *testing.T) {
	noL2 := newCache(t, memstash.Config[string, string]{MemoryCapacity: 10})
	noL2.Wait()

	writeThrough := newCache(t, memstash.Config[string, string]{
		MemoryCapacity: 10, L2Cache: newL2Stub(), WritePolicy: memstash.WriteThrough,
	})
	writeThrough.Wait()

	closed, err := memstash.New[string, string](
		memstash.WithMemoryCapacity(10),
		memstash.WithL2Cache[string, string](newL2Stub()),
		memstash.WithWritePolicy(memstash.WriteBack),
	)
	require.NoError(t, err)
	closed.Close()
	closed.Wait() // must not hang after Close
}

// failingL2 wraps l2Stub but Set always fails.
type failingL2 struct {
	*l2Stub
	err error
}

func (f *failingL2) Set(_ context.Context, _, _ string, _ time.Duration) error { return f.err }

// TestOnL2ErrorCallback verifies that an L2 error on the asynchronous write-back path reaches OnL2Error, since Set
// itself has already returned successfully by the time the background worker discovers the failure.
func TestOnL2ErrorCallback(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("boom")
	l2 := &failingL2{l2Stub: newL2Stub(), err: boom}

	errCh := make(chan error, 1)
	c, err := memstash.New[string, string](
		memstash.WithMemoryCapacity(10),
		memstash.WithL2Cache[string, string](l2),
		memstash.WithWritePolicy(memstash.WriteBack),
		memstash.WithOnL2Error[string, string](func(_ string, err error) { errCh <- err }),
	)
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.Set(ctx, "k", "v"), "Set itself must still succeed - the L2 write fails in the background")

	select {
	case gotErr := <-errCh:
		assert.ErrorIs(t, gotErr, boom)
	case <-time.After(5 * time.Second):
		t.Fatal("OnL2Error was not called")
	}
}

func TestGetOrLoadUsesL2BeforeLoader(t *testing.T) {
	ctx := context.Background()
	l2 := newL2Stub()
	require.NoError(t, l2.Set(ctx, "k", "from-l2", 0))

	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100, L2Cache: l2})
	v, err := c.GetOrLoad(ctx, "k", func(context.Context, string) (string, error) {
		t.Error("loader must not be called when the value is present in L2")
		return "", nil
	})
	require.NoError(t, err)
	assert.Equal(t, "from-l2", v)
}

func TestLoadableCache(t *testing.T) {
	ctx := context.Background()
	var calls atomic.Int32
	lc, err := memstash.NewLoadable(
		func(_ context.Context, key int) (string, error) {
			calls.Add(1)
			return fmt.Sprintf("v%d", key), nil
		},
		memstash.WithMemoryCapacity(10))
	require.NoError(t, err)
	defer lc.Close()

	for i := 0; i < 3; i++ {
		v, err := lc.GetOrLoad(ctx, 7)
		require.NoError(t, err)
		assert.Equal(t, "v7", v)
	}
	assert.EqualValues(t, 1, calls.Load(), "loader calls")
}

// TestConcurrentMixed runs a mixed workload from several goroutines; together with -race it verifies correct
// synchronization and the capacity invariant.
func TestConcurrentMixed(t *testing.T) {
	ctx := context.Background()
	for _, tc := range policies {
		t.Run(tc.name, func(t *testing.T) {
			c, err := memstash.New[int, int](memstash.WithMemoryCapacity(256), memstash.WithPolicy(tc.policy))
			require.NoError(t, err)
			defer c.Close()

			var wg sync.WaitGroup
			for g := 0; g < 4; g++ {
				wg.Add(1)
				go func(seed int64) {
					defer wg.Done()
					rng := rand.New(rand.NewSource(seed))
					for i := 0; i < 20000; i++ {
						key := rng.Intn(1024)
						switch rng.Intn(10) {
						case 0:
							assert.NoError(t, c.Delete(ctx, key))
						case 1, 2:
							assert.NoError(t, c.Set(ctx, key, key))
						default:
							if value, ok := c.GetFromMemory(key); ok {
								assert.Equal(t, key, value, "corrupted value for key %d", key)
							}
						}
					}
				}(int64(g))
			}
			wg.Wait()

			require.LessOrEqual(t, c.Weight(), int64(256), "weight exceeded the capacity after the concurrent load")
			require.GreaterOrEqual(t, c.Weight(), int64(0), "negative weight - double subtraction")
		})
	}
}
