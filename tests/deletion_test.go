package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
)

type removal struct {
	key   string
	value string
	cause memstash.DeletionCause
}

// recorder collects the handler's calls; it runs outside the shard mutex but on arbitrary goroutines.
type recorder struct {
	mu        sync.Mutex
	deletions []removal
}

func (r *recorder) onDeletion(key, value string, cause memstash.DeletionCause) {
	r.mu.Lock()
	r.deletions = append(r.deletions, removal{key, value, cause})
	r.mu.Unlock()
}

func (r *recorder) snapshot() []removal {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]removal(nil), r.deletions...)
}

func newWatchedCache(t *testing.T, cfg memstash.Config[string, string]) (*memstash.Cache[string, string], *recorder) {
	t.Helper()
	rec := &recorder{}
	cfg.OnDeletion = rec.onDeletion
	c, err := memstash.NewWithConfig[string, string](&cfg)
	require.NoError(t, err, "NewWithConfig")
	t.Cleanup(c.Close)
	return c, rec
}

func TestDeletionCauseInvalidation(t *testing.T) {
	ctx := context.Background()
	c, rec := newWatchedCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	require.NoError(t, c.Set(ctx, "k", "v"))
	require.NoError(t, c.Delete(ctx, "k"))

	assert.Equal(t, []removal{{"k", "v", memstash.CauseInvalidation}}, rec.snapshot())
}

func TestDeletionMissingKeyIsSilent(t *testing.T) {
	ctx := context.Background()
	c, rec := newWatchedCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	require.NoError(t, c.Delete(ctx, "absent"))

	assert.Empty(t, rec.snapshot())
}

func TestDeletionCauseReplacement(t *testing.T) {
	ctx := context.Background()
	c, rec := newWatchedCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	require.NoError(t, c.Set(ctx, "k", "old"))
	require.NoError(t, c.Set(ctx, "k", "new"))

	assert.Equal(t, []removal{{"k", "old", memstash.CauseReplacement}}, rec.snapshot(),
		"the handler must receive the value that was replaced")

	v, ok := c.GetFromMemory("k")
	assert.True(t, ok)
	assert.Equal(t, "new", v)
}

func TestDeletionCauseEviction(t *testing.T) {
	ctx := context.Background()
	for _, tc := range policies {
		t.Run(tc.name, func(t *testing.T) {
			c, rec := newWatchedCache(t, memstash.Config[string, string]{
				MemoryCapacity: 20, ShardsCount: 1, Policy: tc.policy,
			})
			for i := range 200 {
				require.NoError(t, c.Set(ctx, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)))
			}

			evicted := rec.snapshot()
			require.NotEmpty(t, evicted, "filling 10x past capacity must evict")
			for _, r := range evicted {
				assert.Equal(t, memstash.CauseEviction, r.cause)
				assert.Equal(t, "v"+r.key[1:], r.value, "key and value must belong to the same item")
			}
			assert.Equal(t, 200-c.Len(), len(evicted), "every item that left must be reported exactly once")
		})
	}
}

func TestDeletionCauseOverflow(t *testing.T) {
	ctx := context.Background()
	c, rec := newWatchedCache(t, memstash.Config[string, string]{
		MemoryCapacity: 100,
		ShardsCount:    1,
		CostFunc:       func(k string, v string) uint32 { return uint32(len(v)) },
	})
	require.NoError(t, c.Set(ctx, "k", "small"))
	require.NoError(t, c.Set(ctx, "k", string(make([]byte, 200)))) // alone outweighs the shard

	assert.Equal(t, []removal{{"k", "small", memstash.CauseOverflow}}, rec.snapshot())

	_, ok := c.GetFromMemory("k")
	assert.False(t, ok, "neither the old nor the oversized value may stay")
}

func TestDeletionCauseExpiration(t *testing.T) {
	ctx := context.Background()
	c, rec := newWatchedCache(t, memstash.Config[string, string]{MemoryCapacity: 100, TTL: time.Second})
	require.NoError(t, c.Set(ctx, "k", "v"))

	time.Sleep(3 * time.Second) // same margin the other TTL tests rely on
	_, ok := c.GetFromMemory("k")
	require.False(t, ok, "the item must be expired by now")

	assert.Equal(t, []removal{{"k", "v", memstash.CauseExpiration}}, rec.snapshot())
}

func TestDeletionBatchDelete(t *testing.T) {
	ctx := context.Background()
	c, rec := newWatchedCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	for _, k := range []string{"a", "b", "c"} {
		require.NoError(t, c.Set(ctx, k, "v"+k))
	}
	require.NoError(t, c.BatchDelete(ctx, []string{"a", "absent", "c"}))

	assert.Equal(t, []removal{
		{"a", "va", memstash.CauseInvalidation},
		{"c", "vc", memstash.CauseInvalidation},
	}, rec.snapshot(), "only the keys that were present are reported, in call order")
}

// TestDeletionAutomaticFilter covers the handler wired through the option and narrowed with DeletionCause.Automatic,
// the way a caller interested only in the cache's own decisions filters.
func TestDeletionAutomaticFilter(t *testing.T) {
	ctx := context.Background()
	var evicted []removal
	c, err := memstash.New[string, string](
		memstash.WithMemoryCapacity(20),
		memstash.WithShardsCount(1),
		memstash.WithOnDeletion(func(key, value string, cause memstash.DeletionCause) {
			if cause.Automatic() {
				evicted = append(evicted, removal{key, value, cause})
			}
		}),
	)
	require.NoError(t, err)
	t.Cleanup(c.Close)

	require.NoError(t, c.Set(ctx, "k", "v1"))
	require.NoError(t, c.Set(ctx, "k", "v2")) // a replacement is not automatic: filtered out
	assert.Empty(t, evicted)

	for i := range 200 {
		require.NoError(t, c.Set(ctx, fmt.Sprintf("k%d", i), "v"))
	}
	assert.NotEmpty(t, evicted)
	for _, r := range evicted {
		assert.True(t, r.cause.Automatic(), "the filter let through %s", r.cause)
	}
}

// TestDeletionHandlerReentrant pins the contract that handlers run with no shard mutex held: one that calls back into
// the cache must not deadlock. Sharding is off so the callback lands on the shard that just fired.
func TestDeletionHandlerReentrant(t *testing.T) {
	ctx := context.Background()
	done := make(chan struct{})
	var c *memstash.Cache[string, string]
	c, err := memstash.New[string, string](
		memstash.WithMemoryCapacity(100),
		memstash.WithShardsCount(1),
		memstash.WithOnDeletion(func(key, value string, cause memstash.DeletionCause) {
			if key == "trigger" {
				_ = c.Set(ctx, "from-handler", value)
				_, _ = c.GetFromMemory("from-handler")
				_ = c.Delete(ctx, "other")
				close(done)
			}
		}),
	)
	require.NoError(t, err)
	t.Cleanup(c.Close)

	require.NoError(t, c.Set(ctx, "other", "v"))
	require.NoError(t, c.Set(ctx, "trigger", "v"))
	require.NoError(t, c.Delete(ctx, "trigger"))

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler calling back into the cache deadlocked")
	}
	v, ok := c.GetFromMemory("from-handler")
	assert.True(t, ok)
	assert.Equal(t, "v", v)
}

func TestDeletionCauseString(t *testing.T) {
	assert.Equal(t, "invalidation", memstash.CauseInvalidation.String())
	assert.Equal(t, "replacement", memstash.CauseReplacement.String())
	assert.Equal(t, "expiration", memstash.CauseExpiration.String())
	assert.Equal(t, "eviction", memstash.CauseEviction.String())
	assert.Equal(t, "overflow", memstash.CauseOverflow.String())
	assert.Equal(t, "unknown", memstash.DeletionCause(200).String())

	assert.False(t, memstash.CauseInvalidation.Automatic())
	assert.False(t, memstash.CauseReplacement.Automatic())
	assert.True(t, memstash.CauseExpiration.Automatic())
	assert.True(t, memstash.CauseEviction.Automatic())
	assert.True(t, memstash.CauseOverflow.Automatic())
}

// TestDeletionUnderConcurrency runs the handlers while several goroutines mutate the cache: every reported pair must be
// one that was actually written, and nothing may be reported twice.
func TestDeletionUnderConcurrency(t *testing.T) {
	ctx := context.Background()
	c, rec := newWatchedCache(t, memstash.Config[string, string]{MemoryCapacity: 200, ShardsCount: 4})

	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				key := fmt.Sprintf("k%d", (w*500+i)%400)
				_ = c.Set(ctx, key, "v"+key)
				if i%3 == 0 {
					_ = c.Delete(ctx, key)
				}
			}
		}()
	}
	wg.Wait()

	deletions := rec.snapshot()
	require.NotEmpty(t, deletions)
	for _, r := range deletions {
		assert.Equal(t, "v"+r.key, r.value, "a reported pair must never mix two items")
	}
}
