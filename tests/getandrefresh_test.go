package tests

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
)

func eventually(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	require.Eventually(t, cond, 3*time.Second, 2*time.Millisecond, msg)
}

func TestGetAndRefreshServesCurrentAndReloads(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	require.NoError(t, c.Set(ctx, "k", "old"))

	value, ok := c.GetAndRefresh(ctx, "k", func(context.Context, string) (string, error) { return "new", nil })
	require.True(t, ok)
	assert.Equal(t, "old", value)

	eventually(t, func() bool {
		v, _ := c.GetFromMemory("k")
		return v == "new"
	}, "the background load did not replace the value")
}

func TestGetAndRefreshServesExpiredValue(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100, TTL: time.Second})
	require.NoError(t, c.Set(ctx, "k", "old"))

	// No read in between: a read past the TTL reclaims the item.
	time.Sleep(3 * time.Second)

	value, ok := c.GetAndRefresh(ctx, "k", func(context.Context, string) (string, error) { return "new", nil })
	require.True(t, ok, "an expired item that no read reclaimed is still served")
	assert.Equal(t, "old", value)

	eventually(t, func() bool {
		v, ok := c.GetFromMemory("k")
		return ok && v == "new"
	}, "the reloaded value must be live again")
}

func TestGetAndRefreshAbsentKeyIsStillLoaded(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})

	_, ok := c.GetAndRefresh(ctx, "k", func(context.Context, string) (string, error) { return "loaded", nil })
	assert.False(t, ok)

	eventually(t, func() bool {
		v, ok := c.GetFromMemory("k")
		return ok && v == "loaded"
	}, "the background load did not populate the key")
}

func TestGetAndRefreshDeletedKeyIsNotStale(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	require.NoError(t, c.Set(ctx, "k", "v"))
	require.NoError(t, c.Delete(ctx, "k"))

	_, ok := c.GetAndRefresh(ctx, "k", func(context.Context, string) (string, error) { return "new", nil })
	assert.False(t, ok, "a deleted item is gone, not stale")
}

func TestGetAndRefreshNilLoader(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	require.NoError(t, c.Set(ctx, "k", "v"))

	value, ok := c.GetAndRefresh(ctx, "k", nil)
	require.True(t, ok)
	assert.Equal(t, "v", value)
	assert.Empty(t, c.BatchGetAndRefresh(ctx, []string{"absent"}, nil))
}

func TestGetAndRefreshCoalescesConcurrentCalls(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	require.NoError(t, c.Set(ctx, "k", "old"))

	var calls atomic.Int64
	release := make(chan struct{})
	load := func(context.Context, string) (string, error) {
		calls.Add(1)
		<-release
		return "new", nil
	}

	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, ok := c.GetAndRefresh(ctx, "k", load)
			assert.True(t, ok)
			assert.Equal(t, "old", v)
		}()
	}
	wg.Wait()

	close(release)
	eventually(t, func() bool {
		v, _ := c.GetFromMemory("k")
		return v == "new"
	}, "the load never completed")
	assert.EqualValues(t, 1, calls.Load(), "32 calls on one key must run the loader once")
}

func TestGetAndRefreshErrorKeepsCurrentValue(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	require.NoError(t, c.Set(ctx, "k", "old"))

	boom := errors.New("boom")
	var called atomic.Bool
	_, _ = c.GetAndRefresh(ctx, "k", func(context.Context, string) (string, error) {
		called.Store(true)
		return "", boom
	})

	eventually(t, func() bool { return called.Load() }, "the loader never ran")
	v, ok := c.GetFromMemory("k")
	assert.True(t, ok)
	assert.Equal(t, "old", v, "a failed load must not drop the value it could not replace")
}

// A GetOrLoad joining an in-flight background load gets its result instead of starting a second one.
func TestGetAndRefreshFlightIsSharedWithGetOrLoad(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})

	var calls atomic.Int64
	release := make(chan struct{})
	load := func(context.Context, string) (string, error) {
		calls.Add(1)
		<-release
		return "loaded", nil
	}

	_, _ = c.GetAndRefresh(ctx, "k", load)
	eventually(t, func() bool { return calls.Load() == 1 }, "the background load never started")

	joined := make(chan string, 1)
	go func() {
		v, err := c.GetOrLoad(ctx, "k", load)
		assert.NoError(t, err)
		joined <- v
	}()

	close(release)
	select {
	case v := <-joined:
		assert.Equal(t, "loaded", v)
	case <-time.After(3 * time.Second):
		t.Fatal("GetOrLoad did not join the in-flight load")
	}
	assert.EqualValues(t, 1, calls.Load(), "GetOrLoad must join, not start a second load")
}

func TestGetAndRefreshWritesToL2(t *testing.T) {
	ctx := context.Background()
	l2 := newL2Stub()
	c := newCache(t, memstash.Config[string, string]{
		MemoryCapacity: 100, L2Cache: l2, WritePolicy: memstash.WriteThrough,
	})
	require.NoError(t, c.Set(ctx, "k", "old"))

	_, _ = c.GetAndRefresh(ctx, "k", func(context.Context, string) (string, error) { return "new", nil })
	eventually(t, func() bool {
		v, ok := l2.snapshot("k")
		return ok && v == "new"
	}, "the loaded value did not reach L2")
}

func TestBatchGetAndRefresh(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	require.NoError(t, c.Set(ctx, "a", "old-a"))
	require.NoError(t, c.Set(ctx, "c", "old-c"))

	var asked atomic.Int64
	load := func(_ context.Context, keys []string) (memstash.List[string, string], error) {
		asked.Add(1)
		out := make(memstash.List[string, string], 0, len(keys))
		for _, k := range keys {
			out = append(out, memstash.KeyVal[string, string]{Key: k, Value: "new-" + k})
		}
		return out, nil
	}

	found := c.BatchGetAndRefresh(ctx, []string{"a", "b", "c"}, load)
	assert.Equal(t, map[string]string{"a": "old-a", "c": "old-c"}, found.ToMap())

	eventually(t, func() bool {
		a, _ := c.GetFromMemory("a")
		b, _ := c.GetFromMemory("b")
		cc, _ := c.GetFromMemory("c")
		return a == "new-a" && b == "new-b" && cc == "new-c"
	}, "the background batch load did not land")
	assert.EqualValues(t, 1, asked.Load(), "the whole set must cost one loader call")
}

func TestBatchGetAndRefreshEdgeCases(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	require.NoError(t, c.Set(ctx, "k", "v"))

	var calls atomic.Int64
	load := func(_ context.Context, keys []string) (memstash.List[string, string], error) {
		calls.Add(1)
		return memstash.List[string, string]{{Key: keys[0], Value: "new"}}, nil
	}

	assert.Empty(t, c.BatchGetAndRefresh(ctx, nil, load))
	assert.Zero(t, calls.Load(), "an empty key set must not call the loader")

	keys := []string{"k"}
	found := c.BatchGetAndRefresh(ctx, keys, load)
	keys[0] = "mutated"
	require.Len(t, found, 1)
	eventually(t, func() bool {
		v, _ := c.GetFromMemory("k")
		return v == "new"
	}, "mutating the caller's slice must not change what was loaded")
}

func TestBatchGetAndRefreshErrorKeepsCurrentValues(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	require.NoError(t, c.Set(ctx, "k", "old"))

	boom := errors.New("boom")
	var called atomic.Bool
	found := c.BatchGetAndRefresh(ctx, []string{"k"}, func(context.Context, []string) (memstash.List[string, string], error) {
		called.Store(true)
		return nil, boom
	})
	assert.Equal(t, map[string]string{"k": "old"}, found.ToMap())

	eventually(t, func() bool { return called.Load() }, "the loader never ran")
	v, _ := c.GetFromMemory("k")
	assert.Equal(t, "old", v)
}

func TestGetAndRefreshOutlivesCallerContext(t *testing.T) {
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	require.NoError(t, c.Set(context.Background(), "k", "old"))

	ctx, cancel := context.WithCancel(context.Background())
	_, _ = c.GetAndRefresh(ctx, "k", func(ctx context.Context, _ string) (string, error) {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		return "new", nil
	})
	cancel()

	eventually(t, func() bool {
		v, _ := c.GetFromMemory("k")
		return v == "new"
	}, "the background load died with the caller's context")
}

func TestGetAndRefreshAfterCloseIsSafe(t *testing.T) {
	ctx := context.Background()
	c, err := memstash.New[string, string](memstash.WithMemoryCapacity(100))
	require.NoError(t, err)
	require.NoError(t, c.Set(ctx, "k", "v"))
	c.Close()

	load := func(context.Context, string) (string, error) { return "new", nil }
	value, ok := c.GetAndRefresh(ctx, "k", load)
	assert.True(t, ok)
	assert.Equal(t, "v", value)
	_, _ = c.GetAndRefresh(ctx, "k", load) // an abandoned flight must not block the next call
}

func TestLoadableCacheGetAndRefresh(t *testing.T) {
	ctx := context.Background()
	var calls atomic.Int64
	lc, err := memstash.NewLoadable(
		func(_ context.Context, key string) (string, error) {
			calls.Add(1)
			return "loaded-" + key, nil
		},
		memstash.WithMemoryCapacity(100),
	)
	require.NoError(t, err)
	defer lc.Close()

	require.NoError(t, lc.Set(ctx, "k", "old"))
	value, ok := lc.GetAndRefresh(ctx, "k")
	require.True(t, ok)
	assert.Equal(t, "old", value)
	eventually(t, func() bool {
		v, _ := lc.GetFromMemory("k")
		return v == "loaded-k"
	}, "the constructor's loader did not run")

	require.NoError(t, lc.Set(ctx, "a", "old-a"))
	found := lc.BatchGetAndRefresh(ctx, []string{"a", "b"})
	assert.Equal(t, map[string]string{"a": "old-a"}, found.ToMap())
	eventually(t, func() bool {
		a, _ := lc.GetFromMemory("a")
		b, _ := lc.GetFromMemory("b")
		return a == "loaded-a" && b == "loaded-b"
	}, "the synthesized batch loader did not run")
}

func TestGetAndRefreshConcurrentWithGetsAndSets(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 200, TTL: time.Second})
	load := func(_ context.Context, key string) (string, error) { return "loaded", nil }
	batchLoad := func(_ context.Context, keys []string) (memstash.List[string, string], error) {
		out := make(memstash.List[string, string], 0, len(keys))
		for _, k := range keys {
			out = append(out, memstash.KeyVal[string, string]{Key: k, Value: "loaded"})
		}
		return out, nil
	}

	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 200 {
				key := fmt.Sprintf("k%d", (w*200+i)%50)
				switch i % 4 {
				case 0:
					_ = c.Set(ctx, key, "mem")
				case 1:
					_, _ = c.GetFromMemory(key)
				case 2:
					_, _ = c.GetAndRefresh(ctx, key, load)
				default:
					_ = c.BatchGetAndRefresh(ctx, []string{key}, batchLoad)
				}
			}
		}()
	}
	wg.Wait()
}

// Keys already being loaded are dropped from the batch: the in-flight load publishes for them.
func TestBatchGetAndRefreshCoalescesWithInFlightLoads(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})

	release := make(chan struct{})
	var singleCalls atomic.Int64
	_, _ = c.GetAndRefresh(ctx, "a", func(context.Context, string) (string, error) {
		singleCalls.Add(1)
		<-release
		return "from-single", nil
	})
	eventually(t, func() bool { return singleCalls.Load() == 1 }, "the single load never started")

	askedFor := make(chan []string, 1)
	c.BatchGetAndRefresh(ctx, []string{"a", "b", "c"}, func(_ context.Context, keys []string) (memstash.List[string, string], error) {
		askedFor <- append([]string(nil), keys...)
		out := make(memstash.List[string, string], 0, len(keys))
		for _, k := range keys {
			out = append(out, memstash.KeyVal[string, string]{Key: k, Value: "from-batch"})
		}
		return out, nil
	})

	select {
	case keys := <-askedFor:
		assert.ElementsMatch(t, []string{"b", "c"}, keys, "the key already in flight must not be asked for twice")
	case <-time.After(3 * time.Second):
		t.Fatal("the batch loader never ran")
	}

	close(release)
	eventually(t, func() bool {
		a, _ := c.GetFromMemory("a")
		b, _ := c.GetFromMemory("b")
		return a == "from-single" && b == "from-batch"
	}, "both loads must land")
	assert.EqualValues(t, 1, singleCalls.Load())
}

// Two overlapping batches must not ask the loader for the same key twice.
func TestBatchGetAndRefreshCoalescesOverlappingBatches(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})

	inLoader := make(chan struct{})
	release := make(chan struct{})
	firstAsked := make(chan []string, 1)
	first := func(_ context.Context, keys []string) (memstash.List[string, string], error) {
		firstAsked <- append([]string(nil), keys...)
		close(inLoader)
		<-release
		return loadedList(keys, "first"), nil
	}

	secondAsked := make(chan []string, 1)
	second := func(_ context.Context, keys []string) (memstash.List[string, string], error) {
		secondAsked <- append([]string(nil), keys...)
		return loadedList(keys, "second"), nil
	}

	c.BatchGetAndRefresh(ctx, []string{"a", "b"}, first)
	<-inLoader // the first batch owns a and b for as long as it is in the loader

	c.BatchGetAndRefresh(ctx, []string{"b", "c"}, second)
	select {
	case keys := <-secondAsked:
		assert.Equal(t, []string{"c"}, keys, "b is already in flight and must be left to the first batch")
	case <-time.After(3 * time.Second):
		t.Fatal("the second batch loader never ran")
	}

	close(release)
	assert.ElementsMatch(t, []string{"a", "b"}, <-firstAsked)
	eventually(t, func() bool {
		a, _ := c.GetFromMemory("a")
		b, _ := c.GetFromMemory("b")
		cc, _ := c.GetFromMemory("c")
		return a == "first" && b == "first" && cc == "second"
	}, "every key must be resolved by exactly one of the two batches")
}

func loadedList(keys []string, value string) memstash.List[string, string] {
	out := make(memstash.List[string, string], 0, len(keys))
	for _, k := range keys {
		out = append(out, memstash.KeyVal[string, string]{Key: k, Value: value})
	}
	return out
}
