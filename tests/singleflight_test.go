package tests

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
)

// A single shard gives the in-flight registry a single bucket, so more simultaneous loads than its inline slots hold
// have to spill into the overflow map. Coalescing must survive that.
func TestSingleflightOverflowsOneBucket(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 1000, ShardsCount: 1})

	const keys, perKey = 24, 3
	var calls [keys]atomic.Int32
	gate := make(chan struct{})
	entered := make(chan struct{}, keys*perKey)

	load := func(_ context.Context, key string) (string, error) {
		i, err := strconv.Atoi(strings.TrimPrefix(key, "k"))
		require.NoError(t, err)
		calls[i].Add(1)
		entered <- struct{}{}
		<-gate // hold every flight open so all of them are registered at once
		return "v" + key, nil
	}

	var wg sync.WaitGroup
	for i := range keys {
		for range perKey {
			wg.Add(1)
			go func() {
				defer wg.Done()
				value, err := c.GetOrLoad(ctx, "k"+strconv.Itoa(i), load)
				assert.NoError(t, err)
				assert.Equal(t, "vk"+strconv.Itoa(i), value)
			}()
		}
	}

	deadline := time.After(10 * time.Second)
	for range keys { // every key must reach the loader before any of them may finish
		select {
		case <-entered:
		case <-deadline:
			t.Fatal("not every key reached the loader: a flight was lost")
		}
	}
	close(gate)
	wg.Wait()

	for i := range keys {
		assert.EqualValues(t, 1, calls[i].Load(), "key k%d loaded more than once", i)
	}
}

func TestBatchGetOrLoadDuplicateKeys(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})

	var asked [][]string
	got, err := c.BatchGetOrLoad(ctx, []string{"a", "b", "a", "b"},
		func(_ context.Context, keys []string) (memstash.List[string, string], error) {
			asked = append(asked, append([]string(nil), keys...))
			out := make(memstash.List[string, string], 0, len(keys))
			for _, key := range keys {
				out = append(out, memstash.KeyVal[string, string]{Key: key, Value: "v" + key})
			}
			return out, nil
		})

	require.NoError(t, err)
	require.Len(t, asked, 1)
	assert.Equal(t, []string{"a", "b"}, asked[0], "a repeated key must be loaded once")
	assert.Equal(t, map[string]string{"a": "va", "b": "vb"}, got.ToMap())
}

// The loader is free to answer in any order; above ownedScanMax the cache switches from a rescan to an index, so both
// sides of that threshold are exercised.
func TestBatchGetOrLoadOutOfOrderAnswer(t *testing.T) {
	ctx := context.Background()
	for _, n := range []int{5, 100} {
		t.Run(fmt.Sprintf("keys=%d", n), func(t *testing.T) {
			c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 1000})
			keys := make([]string, n)
			for i := range keys {
				keys[i] = "k" + strconv.Itoa(i)
			}

			got, err := c.BatchGetOrLoad(ctx, keys,
				func(_ context.Context, asked []string) (memstash.List[string, string], error) {
					out := make(memstash.List[string, string], 0, len(asked))
					for i := len(asked) - 1; i >= 0; i-- { // reversed
						out = append(out, memstash.KeyVal[string, string]{Key: asked[i], Value: "v" + asked[i]})
					}
					return out, nil
				})

			require.NoError(t, err)
			require.Len(t, got, n)
			for _, key := range keys {
				assert.Equal(t, "v"+key, got.ToMap()[key])
			}
			// Every value must have landed in memory under its own key, not a neighbour's.
			for _, key := range keys {
				value, ok := c.GetFromMemory(key)
				assert.True(t, ok, "%s missing from memory", key)
				assert.Equal(t, "v"+key, value)
			}
		})
	}
}

func TestBatchGetOrLoadPartialAnswerWithError(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	boom := errors.New("boom")

	got, err := c.BatchGetOrLoad(ctx, []string{"a", "b", "c"},
		func(context.Context, []string) (memstash.List[string, string], error) {
			return memstash.List[string, string]{{Key: "a", Value: "va"}}, boom
		})

	require.ErrorIs(t, err, boom)
	assert.Equal(t, map[string]string{"a": "va"}, got.ToMap(), "what the loader did answer must come back")

	value, ok := c.GetFromMemory("a")
	assert.True(t, ok, "a key the loader resolved must be cached even though the call errored")
	assert.Equal(t, "va", value)
	_, ok = c.GetFromMemory("b")
	assert.False(t, ok)
}

// A key the loader drops resolves as "not found" for the caller and for anyone waiting on that flight.
func TestBatchGetOrLoadDroppedKeyIsNotAnError(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})

	got, err := c.BatchGetOrLoad(ctx, []string{"a", "b"},
		func(context.Context, []string) (memstash.List[string, string], error) {
			return memstash.List[string, string]{{Key: "a", Value: "va"}}, nil
		})

	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "va"}, got.ToMap())
}

// noisyL2 answers a BatchGet with duplicates and with a key nobody asked for - the shapes a third-party adapter can
// get wrong. Nothing may panic and no requested key may be skipped.
type noisyL2 struct {
	memstash.L2Cache[string, string]
	value string
}

func (n noisyL2) BatchGet(_ context.Context, keys []string) (memstash.List[string, string], error) {
	out := make(memstash.List[string, string], 0, len(keys)*2+1)
	for _, key := range keys {
		out = append(out, memstash.KeyVal[string, string]{Key: key, Value: n.value})
		out = append(out, memstash.KeyVal[string, string]{Key: key, Value: n.value})
	}
	return append(out, memstash.KeyVal[string, string]{Key: "stranger", Value: n.value}), nil
}

func TestBatchGetOrLoadSurvivesNoisyL2(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{
		MemoryCapacity: 100,
		L2Cache:        noisyL2{L2Cache: newL2Stub(), value: "fromL2"},
		WritePolicy:    memstash.WriteDisabled,
	})

	got, err := c.BatchGetOrLoad(ctx, []string{"a", "b"},
		func(context.Context, []string) (memstash.List[string, string], error) {
			return nil, errors.New("the loader must not be reached")
		})

	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "fromL2", "b": "fromL2"}, got.ToMap())
}

func TestBatchOpsWithNoKeysSkipL2(t *testing.T) {
	ctx := context.Background()
	l2 := newL2Stub()
	c := newCache(t, memstash.Config[string, string]{
		MemoryCapacity: 100, L2Cache: l2, WritePolicy: memstash.WriteThrough,
	})

	require.NoError(t, c.BatchSet(ctx, nil))
	require.NoError(t, c.BatchDelete(ctx, nil))
	got, err := c.BatchGetOrLoad(ctx, nil,
		func(context.Context, []string) (memstash.List[string, string], error) {
			t.Fatal("an empty batch must not reach the loader")
			return nil, nil
		})
	require.NoError(t, err)
	assert.Empty(t, got)
	l2.mu.Lock()
	defer l2.mu.Unlock()
	assert.Zero(t, l2.sets, "an empty batch must not reach L2")
}

// A refresh started after Close must not register a flight: an abandoned one would hand the next caller a zero value
// instead of letting it load.
func TestGetAndRefreshAfterCloseLeavesNoFlight(t *testing.T) {
	ctx := context.Background()
	c, err := memstash.New[string, string](memstash.WithMemoryCapacity(100))
	require.NoError(t, err)
	require.NoError(t, c.Set(ctx, "k", "v"))
	c.Close()

	_, _ = c.GetAndRefresh(ctx, "k", func(context.Context, string) (string, error) { return "refreshed", nil })

	var calls atomic.Int32
	value, err := c.GetOrLoad(ctx, "other", func(context.Context, string) (string, error) {
		calls.Add(1)
		return "loaded", nil
	})
	require.NoError(t, err)
	assert.Equal(t, "loaded", value)
	assert.EqualValues(t, 1, calls.Load())
}

// A background refresh has no caller to unwind into, so its loader's panic must be caught rather than end the
// process - and delivered to whoever waits on that flight.
func TestGetAndRefreshLoaderPanicIsContained(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	require.NoError(t, c.Set(ctx, "k", "v"))

	var ran atomic.Bool
	value, ok := c.GetAndRefresh(ctx, "k", func(context.Context, string) (string, error) {
		ran.Store(true)
		panic("refresh loader exploded")
	})
	assert.True(t, ok)
	assert.Equal(t, "v", value, "the cached value is still served")
	eventually(t, ran.Load, "the refresh goroutine never ran")

	// The key must not stay wedged: once the panicked flight is released, a load of it goes through again. Until
	// then a caller joins that flight and gets the panic, so this retries.
	eventually(t, func() bool {
		got, err := c.GetOrLoad(ctx, "k", func(context.Context, string) (string, error) { return "reloaded", nil })
		return err == nil && got == "v"
	}, "the key stayed wedged after its refresh panicked")
}

func TestBatchGetAndRefreshLoaderPanicIsContained(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	require.NoError(t, c.Set(ctx, "a", "va"))

	var ran atomic.Bool
	found := c.BatchGetAndRefresh(ctx, []string{"a", "b"},
		func(context.Context, []string) (memstash.List[string, string], error) {
			ran.Store(true)
			panic("batch refresh loader exploded")
		})
	assert.Equal(t, map[string]string{"a": "va"}, found.ToMap())
	eventually(t, ran.Load, "the refresh goroutine never ran")

	eventually(t, func() bool {
		got, err := c.BatchGetOrLoad(ctx, []string{"b"},
			func(context.Context, []string) (memstash.List[string, string], error) {
				return memstash.List[string, string]{{Key: "b", Value: "vb"}}, nil
			})
		return err == nil && got.ToMap()["b"] == "vb"
	}, "the keys stayed wedged after their refresh panicked")
}

// A waiter that joined a background refresh gets the panic as ErrLoaderPanic instead of hanging.
func TestGetOrLoadWaitingOnPanickedRefresh(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100, StatsEnabled: true})

	entered, release := make(chan struct{}), make(chan struct{})
	_, _ = c.GetAndRefresh(ctx, "k", func(context.Context, string) (string, error) {
		close(entered)
		<-release
		panic("refresh loader exploded")
	})
	<-entered // the refresh owns the flight; its peek already counted one memory miss

	joined := make(chan error, 1)
	go func() {
		_, err := c.GetOrLoad(ctx, "k", func(context.Context, string) (string, error) {
			return "", errors.New("this call started its own load instead of joining")
		})
		joined <- err
	}()
	// GetOrLoad counts its miss the moment it joins, so the second one says the waiter is on the flight.
	eventually(t, func() bool { s := c.Stats(); return s.MemoryMisses() >= 2 }, "GetOrLoad never joined the flight")
	close(release)

	select {
	case err := <-joined:
		require.ErrorIs(t, err, memstash.ErrLoaderPanic)
		assert.Contains(t, err.Error(), "refresh loader exploded", "the panic value must survive")
	case <-time.After(5 * time.Second):
		t.Fatal("the waiter hung on the panicked flight")
	}
}

// A panicking L2 adapter must not take the write-back worker down: later writes still get through.
func TestWriteBackSurvivesPanickingL2(t *testing.T) {
	ctx := context.Background()
	l2 := &panicOnceL2{l2Stub: newL2Stub()}
	var reported []error
	var mu sync.Mutex
	c := newCache(t, memstash.Config[string, string]{
		MemoryCapacity: 100,
		L2Cache:        l2,
		WritePolicy:    memstash.WriteBack,
		OnL2Error: func(_ string, err error) {
			mu.Lock()
			defer mu.Unlock()
			reported = append(reported, err)
		},
	})

	require.NoError(t, c.Set(ctx, "boom", "1"))
	c.Wait()
	require.NoError(t, c.Set(ctx, "after", "2"))
	c.Wait()

	l2.mu.Lock()
	_, stored := l2.m["after"]
	l2.mu.Unlock()
	assert.True(t, stored, "the worker died with the panicking adapter")

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, reported, "the panic must reach OnL2Error")
	assert.ErrorIs(t, reported[0], memstash.ErrPanic)
}

// panicOnceL2 blows up on the first write and behaves afterwards.
type panicOnceL2 struct {
	*l2Stub
	panicked atomic.Bool
}

func (p *panicOnceL2) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	if p.panicked.CompareAndSwap(false, true) {
		panic("adapter exploded")
	}
	return p.l2Stub.Set(ctx, key, value, ttl)
}

func (p *panicOnceL2) BatchSet(ctx context.Context, items memstash.List[string, string], ttl time.Duration) error {
	if p.panicked.CompareAndSwap(false, true) {
		panic("adapter exploded")
	}
	return p.l2Stub.BatchSet(ctx, items, ttl)
}

func TestStatsCountGetAndRefresh(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100, StatsEnabled: true})
	require.NoError(t, c.Set(ctx, "k", "v"))

	_, _ = c.GetAndRefresh(ctx, "k", nil)
	_, _ = c.GetAndRefresh(ctx, "absent", nil)
	c.BatchGetAndRefresh(ctx, []string{"k", "absent"}, nil)

	s := c.Stats()
	assert.EqualValues(t, 2, s.MemoryHits())
	assert.EqualValues(t, 2, s.MemoryMisses())
}
