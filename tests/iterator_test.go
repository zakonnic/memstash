package tests

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
)

func TestIterator(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 1000})

	want := map[string]string{}
	for i := range 200 {
		key := fmt.Sprintf("k%03d", i)
		want[key] = "v" + key
		require.NoError(t, c.Set(ctx, key, "v"+key))
	}
	require.NoError(t, c.Delete(ctx, "k000"))
	delete(want, "k000")

	assert.Equal(t, want, maps.Collect(c.Iterator()))
}

func TestIteratorEarlyBreak(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	for i := range 50 {
		require.NoError(t, c.Set(ctx, fmt.Sprintf("k%d", i), "v"))
	}

	seen := 0
	for range c.Iterator() {
		seen++
		if seen == 10 {
			break
		}
	}
	assert.Equal(t, 10, seen)
}

func TestIteratorOverwriteYieldsLatestValue(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	require.NoError(t, c.Set(ctx, "k", "old"))
	require.NoError(t, c.Set(ctx, "k", "new"))

	assert.Equal(t, map[string]string{"k": "new"}, maps.Collect(c.Iterator()))
}

func TestIteratorNothingLive(t *testing.T) {
	ctx := context.Background()

	t.Run("empty", func(t *testing.T) {
		c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
		assert.Empty(t, maps.Collect(c.Iterator()))
	})

	t.Run("single", func(t *testing.T) {
		c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
		require.NoError(t, c.Set(ctx, "k", "v"))
		assert.Equal(t, map[string]string{"k": "v"}, maps.Collect(c.Iterator()))
	})

	t.Run("all deleted", func(t *testing.T) {
		c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
		for i := range 50 {
			require.NoError(t, c.Set(ctx, fmt.Sprintf("k%d", i), "v"))
		}
		for i := range 50 {
			require.NoError(t, c.Delete(ctx, fmt.Sprintf("k%d", i)))
		}
		assert.Empty(t, maps.Collect(c.Iterator()))
	})
}

func TestIteratorSkipsExpired(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 1000, TTL: time.Second})
	for i := range 50 {
		require.NoError(t, c.Set(ctx, fmt.Sprintf("old%d", i), "v"))
	}

	time.Sleep(3 * time.Second) // same margin TestTTLExpiry relies on

	assert.Empty(t, maps.Collect(c.Iterator()), "expired items must not be yielded")
	assert.Equal(t, 50, c.Len(), "Len counts expired items until they are swept - Iterator must not")

	want := map[string]string{}
	for i := range 10 {
		key := fmt.Sprintf("fresh%d", i)
		want[key] = "v"
		require.NoError(t, c.Set(ctx, key, "v"))
	}
	assert.Equal(t, want, maps.Collect(c.Iterator()), "only the fresh items survive alongside expired ones")
}

func TestIteratorSkipsEvictionTombstones(t *testing.T) {
	ctx := context.Background()
	const capacity = 100
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: capacity, ShardsCount: 1})

	written := map[string]bool{}
	for i := range 2000 {
		key := fmt.Sprintf("k%d", i)
		written[key] = true
		require.NoError(t, c.Set(ctx, key, "v"))
	}

	got := maps.Collect(c.Iterator())
	assert.Equal(t, c.Len(), len(got), "Iterator and Len must agree once tombstones are the only dead slots")
	assert.LessOrEqual(t, len(got), capacity)
	assert.NotEmpty(t, got)
	for key := range got {
		assert.True(t, written[key], "yielded a key that was never written: %q", key)
	}
}

func TestIteratorShardCounts(t *testing.T) {
	ctx := context.Background()
	for _, shards := range []int{1, 2, 8} {
		t.Run(fmt.Sprintf("shards=%d", shards), func(t *testing.T) {
			c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 4000, ShardsCount: shards})
			want := map[string]string{}
			for i := range 300 {
				key := fmt.Sprintf("k%03d", i)
				want[key] = "v" + key
				require.NoError(t, c.Set(ctx, key, "v"+key))
			}
			assert.Equal(t, want, maps.Collect(c.Iterator()))
		})
	}
}

func TestIteratorNoDuplicatesAndRepeatable(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 4000, ShardsCount: 8})
	for i := range 500 {
		require.NoError(t, c.Set(ctx, fmt.Sprintf("k%03d", i), "v"))
	}

	seq := c.Iterator()
	first := map[string]int{}
	for key := range seq {
		first[key]++
	}
	for key, n := range first {
		require.Equal(t, 1, n, "key %q yielded more than once in a single pass", key)
	}

	second := map[string]int{}
	for key := range seq {
		second[key]++
	}
	assert.Equal(t, first, second, "the same iter.Seq2 must be re-rangeable")
	assert.Len(t, first, 500)
}

func TestIteratorBreakBoundaries(t *testing.T) {
	ctx := context.Background()
	const total = 50
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 1000, ShardsCount: 4})
	for i := range total {
		require.NoError(t, c.Set(ctx, fmt.Sprintf("k%d", i), "v"))
	}

	for _, stopAt := range []int{1, total - 1, total} {
		t.Run(fmt.Sprintf("stop=%d", stopAt), func(t *testing.T) {
			seen := 0
			for range c.Iterator() {
				seen++
				if seen == stopAt {
					break
				}
			}
			assert.Equal(t, stopAt, seen)
		})
	}

	seen := 0
	for range c.Iterator() {
		seen++
	}
	assert.Equal(t, total, seen, "a full pass must yield every live item")
}

func TestIteratorAfterClose(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100})
	want := map[string]string{}
	for i := range 10 {
		key := fmt.Sprintf("k%d", i)
		want[key] = "v"
		require.NoError(t, c.Set(ctx, key, "v"))
	}
	c.Close()

	assert.Equal(t, want, maps.Collect(c.Iterator()))
}

func TestIteratorNonStringKeys(t *testing.T) {
	ctx := context.Background()
	c, err := memstash.NewWithConfig[int, int](&memstash.Config[int, int]{MemoryCapacity: 1000})
	require.NoError(t, err)
	t.Cleanup(c.Close)

	want := map[int]int{}
	for i := range 200 {
		want[i] = i * 2
		require.NoError(t, c.Set(ctx, i, i*2))
	}
	assert.Equal(t, want, maps.Collect(c.Iterator()))
}

type iterPair struct{ A, B int64 }

// A multi-word value is published under the seqlock, so a snapshot must never mix an old half with a new one.
func TestIteratorConcurrentWritesNotTorn(t *testing.T) {
	ctx := context.Background()
	c, err := memstash.NewWithConfig[int, iterPair](&memstash.Config[int, iterPair]{MemoryCapacity: 1000, ShardsCount: 1})
	require.NoError(t, err)
	t.Cleanup(c.Close)

	const keys = 64
	for i := range keys {
		require.NoError(t, c.Set(ctx, i, iterPair{A: 0, B: 0}))
	}

	var stop atomic.Bool
	var writes atomic.Int64
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for n := int64(seed); !stop.Load(); n++ {
				_ = c.Set(ctx, int(n)%keys, iterPair{A: n, B: n})
				writes.Add(1)
			}
		}(w)
	}

	yielded, torn := 0, iterPair{}
	for range 2000 {
		for _, v := range c.Iterator() {
			yielded++
			if v.A != v.B {
				torn = v
			}
		}
	}
	stop.Store(true)
	wg.Wait()

	assert.Equal(t, iterPair{}, torn, "snapshot mixed two writes")
	assert.Positive(t, yielded)
	assert.Greater(t, writes.Load(), int64(1000), "writers never got going: the test proves nothing")
}

// Growth reallocates the table mid-walk; the pass runs on the table it started with and must stay consistent.
func TestIteratorConcurrentMutation(t *testing.T) {
	ctx := context.Background()
	c := newCache(t, memstash.Config[string, string]{MemoryCapacity: 100_000, ShardsCount: 2})
	for i := range 500 {
		require.NoError(t, c.Set(ctx, fmt.Sprintf("k%05d", i), "v"))
	}

	var stop atomic.Bool
	var writes, deletes atomic.Int64
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { // grows the table, then wraps into overwrites
		defer wg.Done()
		for i := 500; !stop.Load(); i++ {
			_ = c.Set(ctx, fmt.Sprintf("k%05d", 500+i%5000), "v")
			writes.Add(1)
		}
	}()
	go func() { // leaves tombstones behind
		defer wg.Done()
		for i := 0; !stop.Load(); i++ {
			_ = c.Delete(ctx, fmt.Sprintf("k%05d", i%500))
			deletes.Add(1)
		}
	}()

	footprintBefore := c.TotalSize()
	badValue, dupKey := "", ""
	for range 100 {
		seen := make(map[string]struct{}, 8192)
		for key, value := range c.Iterator() {
			if value != "v" {
				badValue = value
			}
			if _, dup := seen[key]; dup {
				dupKey = key
			}
			seen[key] = struct{}{}
		}
	}
	stop.Store(true)
	wg.Wait()

	assert.Empty(t, badValue, "yielded a value never written")
	assert.Empty(t, dupKey, "key yielded twice in one pass")
	assert.Greater(t, writes.Load(), int64(1000), "writer never got going")
	assert.Greater(t, deletes.Load(), int64(1000), "deleter never got going")
	assert.Greater(t, c.TotalSize(), footprintBefore, "the table never grew: no rebuild raced the walk")
}
