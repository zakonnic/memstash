package tests

import (
	"context"
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
