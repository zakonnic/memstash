package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
)

// heldReadL2 is l2Stub whose reads of one key take their answer, then wait at a gate: the answer is as of the read,
// whatever lands in the map while it waits.
type heldReadL2 struct {
	*l2Stub
	key      string
	entered  chan struct{}
	gate     chan struct{}
	once     sync.Once
	openOnce sync.Once
}

func newHeldReadL2(key string) *heldReadL2 {
	return &heldReadL2{l2Stub: newL2Stub(), key: key, entered: make(chan struct{}), gate: make(chan struct{})}
}

func (h *heldReadL2) open() { h.openOnce.Do(func() { close(h.gate) }) }

func (h *heldReadL2) hold() {
	h.once.Do(func() {
		close(h.entered)
		<-h.gate
	})
}

func (h *heldReadL2) Get(ctx context.Context, key string) (string, bool, error) {
	v, ok, err := h.l2Stub.Get(ctx, key)
	if key == h.key {
		h.hold()
	}
	return v, ok, err
}

func (h *heldReadL2) BatchGet(ctx context.Context, keys []string) (memstash.List[string, string], error) {
	found, err := h.l2Stub.BatchGet(ctx, keys)
	for _, key := range keys {
		if key == h.key {
			h.hold()
		}
	}
	return found, err
}

// A value read from L2 or loaded answers a miss; a Set that lands while the read is out is newer, and the fill must not
// put the older value back over it - in memory, nor, for a loaded value, in L2.
func TestFillNeverOverwritesALaterSet(t *testing.T) {
	ctx := context.Background()
	fromL2 := map[string]func(*memstash.Cache[string, string]){
		"Get":        func(c *memstash.Cache[string, string]) { _, _, _ = c.Get(ctx, "k") },
		"BatchGet":   func(c *memstash.Cache[string, string]) { _, _ = c.BatchGet(ctx, []string{"k"}) },
		"GetBatched": func(c *memstash.Cache[string, string]) { _, _, _ = c.GetBatched(ctx, "k") },
		"GetOrLoad":  func(c *memstash.Cache[string, string]) { _, _ = c.GetOrLoad(ctx, "k", unusedLoader) },
		"BatchGetOrLoad": func(c *memstash.Cache[string, string]) {
			_, _ = c.BatchGetOrLoad(ctx, []string{"k"}, unusedBatchLoader)
		},
	}
	for name, read := range fromL2 {
		t.Run("L2 promotion via "+name, func(t *testing.T) {
			l2 := newHeldReadL2("k")
			require.NoError(t, l2.Set(ctx, "k", "v1", 0))
			c := newCache(t, memstash.Config[string, string]{
				MemoryCapacity: 100, L2Cache: l2, WritePolicy: memstash.WriteThrough,
			})
			defer l2.open()

			done := make(chan struct{})
			go func() { read(c); close(done) }()
			<-l2.entered // the read has its answer, v1, and waits
			require.NoError(t, c.Set(ctx, "k", "v2"))
			l2.open()
			<-done

			v, ok := c.GetFromMemory("k")
			assert.True(t, ok)
			assert.Equal(t, "v2", v, "the promotion put back the value the Set replaced")
		})
	}

	loaders := map[string]func(*memstash.Cache[string, string], func() string){
		"GetOrLoad": func(c *memstash.Cache[string, string], load func() string) {
			_, _ = c.GetOrLoad(ctx, "k", func(context.Context, string) (string, error) { return load(), nil })
		},
		"BatchGetOrLoad": func(c *memstash.Cache[string, string], load func() string) {
			_, _ = c.BatchGetOrLoad(ctx, []string{"k"}, func(context.Context, []string) (memstash.List[string, string], error) {
				return memstash.List[string, string]{{Key: "k", Value: load()}}, nil
			})
		},
	}
	for name, read := range loaders {
		t.Run("loaded value via "+name, func(t *testing.T) {
			l2 := newL2Stub()
			c := newCache(t, memstash.Config[string, string]{
				MemoryCapacity: 100, L2Cache: l2, WritePolicy: memstash.WriteThrough,
			})
			entered, gate := make(chan struct{}), make(chan struct{})
			done := make(chan struct{})
			go func() {
				read(c, func() string {
					close(entered)
					<-gate
					return "v1" // read from the source before the Set below
				})
				close(done)
			}()
			<-entered
			require.NoError(t, c.Set(ctx, "k", "v2"))
			close(gate)
			<-done

			v, ok := c.GetFromMemory("k")
			assert.True(t, ok)
			assert.Equal(t, "v2", v, "the loaded value replaced the Set in memory")
			v, _ = l2.snapshot("k")
			assert.Equal(t, "v2", v, "the loaded value replaced the Set in L2")
		})
	}
}

func unusedLoader(context.Context, string) (string, error) {
	return "", memstash.ErrKeyNotFound
}

func unusedBatchLoader(context.Context, []string) (memstash.List[string, string], error) {
	return nil, nil
}

// A Delete that lands while a read of the key is out removed what that read brings back, or its source's copy: the
// value must not come back into memory, nor, when loaded, into L2 - whatever memory held when the read began.
func TestFillNeverResurrectsALaterDelete(t *testing.T) {
	ctx := context.Background()
	fromL2 := map[string]func(*memstash.Cache[string, string]){
		"Get":        func(c *memstash.Cache[string, string]) { _, _, _ = c.Get(ctx, "k") },
		"BatchGet":   func(c *memstash.Cache[string, string]) { _, _ = c.BatchGet(ctx, []string{"k"}) },
		"GetBatched": func(c *memstash.Cache[string, string]) { _, _, _ = c.GetBatched(ctx, "k") },
		"GetOrLoad":  func(c *memstash.Cache[string, string]) { _, _ = c.GetOrLoad(ctx, "k", unusedLoader) },
		"BatchGetOrLoad": func(c *memstash.Cache[string, string]) {
			_, _ = c.BatchGetOrLoad(ctx, []string{"k"}, unusedBatchLoader)
		},
	}
	for name, read := range fromL2 {
		t.Run("L2 promotion via "+name, func(t *testing.T) {
			l2 := newHeldReadL2("k")
			require.NoError(t, l2.Set(ctx, "k", "v1", 0))
			c := newCache(t, memstash.Config[string, string]{
				MemoryCapacity: 100, L2Cache: l2, WritePolicy: memstash.WriteThrough,
			})
			defer l2.open()

			done := make(chan struct{})
			go func() { read(c); close(done) }()
			<-l2.entered // the read has its answer, v1, and waits; memory does not hold the key
			require.NoError(t, c.Delete(ctx, "k"))
			l2.open()
			<-done

			_, ok := c.GetFromMemory("k")
			assert.False(t, ok, "the promotion brought back a key deleted after it read L2")
		})
	}

	loaders := map[string]func(*memstash.Cache[string, string], func() string){
		"GetOrLoad": func(c *memstash.Cache[string, string], load func() string) {
			_, _ = c.GetOrLoad(ctx, "k", func(context.Context, string) (string, error) { return load(), nil })
		},
		"BatchGetOrLoad": func(c *memstash.Cache[string, string], load func() string) {
			_, _ = c.BatchGetOrLoad(ctx, []string{"k"}, func(context.Context, []string) (memstash.List[string, string], error) {
				return memstash.List[string, string]{{Key: "k", Value: load()}}, nil
			})
		},
		"GetAndRefresh": func(c *memstash.Cache[string, string], load func() string) {
			require.NoError(t, c.Set(ctx, "k", "v0"))
			done := make(chan struct{})
			_, _ = c.GetAndRefresh(ctx, "k", func(context.Context, string) (string, error) {
				defer close(done)
				return load(), nil
			})
			<-done
			require.NoError(t, c.Wait(context.Background()))
		},
	}
	for name, read := range loaders {
		t.Run("loaded value via "+name, func(t *testing.T) {
			l2 := newL2Stub()
			c := newCache(t, memstash.Config[string, string]{
				MemoryCapacity: 100, L2Cache: l2, WritePolicy: memstash.WriteThrough,
			})
			entered, gate := make(chan struct{}), make(chan struct{})
			done := make(chan struct{})
			go func() {
				read(c, func() string {
					close(entered)
					<-gate
					return "v1" // read from the source before the Delete below
				})
				close(done)
			}()
			<-entered
			require.NoError(t, c.Delete(ctx, "k"))
			close(gate)
			<-done

			_, ok := c.GetFromMemory("k")
			assert.False(t, ok, "the loaded value brought back a key deleted while it loaded")
			_, ok = l2.snapshot("k")
			assert.False(t, ok, "the loaded value went to L2 after the Delete")
		})
	}
}

// Writes to other keys say nothing about this one: they never keep a promotion out, however many land meanwhile.
func TestFillSurvivesUnrelatedWrites(t *testing.T) {
	ctx := context.Background()
	l2 := newHeldReadL2("k")
	require.NoError(t, l2.Set(ctx, "k", "v1", 0))
	c := newCache(t, memstash.Config[string, string]{
		MemoryCapacity: 100_000, ShardsCount: 1, L2Cache: l2, WritePolicy: memstash.WriteDisabled,
	})
	defer l2.open()

	done := make(chan struct{})
	go func() { _, _, _ = c.Get(ctx, "k"); close(done) }()
	<-l2.entered
	for i := range 10_000 {
		require.NoError(t, c.Set(ctx, fmt.Sprintf("other-%d", i), "v"))
	}
	l2.open()
	<-done

	v, ok := c.GetFromMemory("k")
	assert.True(t, ok)
	assert.Equal(t, "v1", v)
}

// heldDeleteL2 is l2Stub whose Deletes wait at a gate.
type heldDeleteL2 struct {
	*l2Stub
	entered, gate chan struct{}
}

func (h *heldDeleteL2) Delete(ctx context.Context, key string) error {
	close(h.entered)
	<-h.gate
	return h.l2Stub.Delete(ctx, key)
}

// A write-through Delete clears L2 before memory: a read that misses memory afterwards finds the key gone from L2
// too, and until then memory still answers with what L2 still has.
func TestWriteThroughDeleteClearsL2First(t *testing.T) {
	ctx := context.Background()
	l2 := &heldDeleteL2{l2Stub: newL2Stub(), entered: make(chan struct{}), gate: make(chan struct{})}
	c := newCache(t, memstash.Config[string, string]{
		MemoryCapacity: 100, L2Cache: l2, WritePolicy: memstash.WriteThrough,
	})
	require.NoError(t, c.Set(ctx, "k", "v"))

	done := make(chan error, 1)
	go func() { done <- c.Delete(ctx, "k") }()
	<-l2.entered
	_, ok := c.GetFromMemory("k")
	assert.True(t, ok, "memory dropped the key before L2 did")
	close(l2.gate)
	require.NoError(t, <-done)
	_, ok = c.GetFromMemory("k")
	assert.False(t, ok)
}

// A write-through Delete that finds nothing in memory takes no lock: a fill publishing right then must still not bring
// the deleted value back. The window is a few instructions wide, so the race runs many times.
func TestFillLosesToALockFreeDelete(t *testing.T) {
	ctx := context.Background()
	l2 := newL2Stub()
	c := newCache(t, memstash.Config[string, string]{
		MemoryCapacity: 1000, ShardsCount: 1, L2Cache: l2, WritePolicy: memstash.WriteThrough,
	})
	iters := 200_000
	if testing.Short() {
		iters = 20_000
	}
	resurrected := 0
	done := make(chan struct{})
	for range iters {
		_ = c.Delete(ctx, "k")
		_ = l2.Set(ctx, "k", "v1", 0)
		go func() { _, _, _ = c.Get(ctx, "k"); done <- struct{}{} }()
		_ = c.Delete(ctx, "k")
		<-done
		if _, ok := c.GetFromMemory("k"); ok {
			resurrected++
		}
	}
	require.Zero(t, resurrected, "a deleted key came back")
}
