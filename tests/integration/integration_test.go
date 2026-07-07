// Package integration tests the two-level cache against live Redis and memcached servers through every L2 adapter.
// Start the servers with `docker compose -f docker/docker-compose.yml up -d` (repo root); tests skip themselves when
// the corresponding server is not listening. One file per adapter builds a cacheFactory and runs the shared suite.
package integration

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/zakonnic/memstash"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func redisAddr() string {
	if addr := os.Getenv("MEMSTASH_TEST_REDIS_ADDR"); addr != "" {
		return addr
	}
	return "localhost:6379"
}

func memcachedAddr() string {
	if addr := os.Getenv("MEMSTASH_TEST_MEMCACHED_ADDR"); addr != "" {
		return addr
	}
	return "localhost:11211"
}

// requireServer skips the test when nothing listens on addr, so a partial environment (only Redis, only memcached)
// still runs the relevant part of the suite.
func requireServer(tb testing.TB, addr string) {
	tb.Helper()
	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		tb.Skipf("no server at %s (start docker/docker-compose.yml): %v", addr, err)
	}
	_ = conn.Close()
}

// uniquePrefix namespaces one subtest's keys inside the shared backend, so tests and reruns never see each other's
// data.
func uniquePrefix(tb testing.TB) string {
	return fmt.Sprintf("%s|%d|", tb.Name(), time.Now().UnixNano())
}

// cacheFactory builds a two-level cache over the adapter under test. Caches created with the same prefix share the
// same L2 namespace; the factory must register Close via t.Cleanup (double Close is safe).
type cacheFactory func(t *testing.T, prefix string, opts ...memstash.Option) *memstash.Cache[string, string]

// suiteScenarios is the table of L1+L2 contract scenarios executed for every adapter.
var suiteScenarios = []struct {
	name string
	run  func(t *testing.T, newCache cacheFactory)
}{
	{name: "SetGetDelete", run: suiteSetGetDelete},
	{name: "PromotionFromL2", run: suitePromotionFromL2},
	{name: "DeletePropagatesToL2", run: suiteDeletePropagatesToL2},
	{name: "WriteBackFlushOnClose", run: suiteWriteBackFlushOnClose},
	{name: "WriteBackVisibleAfterWait", run: suiteWriteBackVisibleAfterWait},
	{name: "BatchSetGet", run: suiteBatchSetGet},
	{name: "EvictedItemsServedFromL2", run: suiteEvictedItemsServedFromL2},
	{name: "GetOrLoadPrefersL2", run: suiteGetOrLoadPrefersL2},
	{name: "TTLReachesL2", run: suiteTTLReachesL2},
	{name: "ConcurrentMixed", run: suiteConcurrentMixed},
}

// runSuite exercises the full L1+L2 contract through one adapter.
func runSuite(t *testing.T, newCache cacheFactory) {
	for _, scenario := range suiteScenarios {
		t.Run(scenario.name, func(t *testing.T) {
			scenario.run(t, newCache)
		})
	}
}

func suiteSetGetDelete(t *testing.T, newCache cacheFactory) {
	ctx := context.Background()
	// WriteThrough: the scenario asserts that L2 observes Set and Delete synchronously; with the default WriteBack a
	// lagging background flush could resurrect the key right after Delete.
	c := newCache(t, uniquePrefix(t), memstash.WithWritePolicy(memstash.WriteThrough))

	require.NoError(t, c.Set(ctx, "k", "v"))
	v, ok, err := c.Get(ctx, "k")
	require.NoError(t, err)
	require.True(t, ok, "Get after Set")
	assert.Equal(t, "v", v)

	require.NoError(t, c.Delete(ctx, "k"))
	_, ok, err = c.Get(ctx, "k")
	require.NoError(t, err)
	assert.False(t, ok, "value survived Delete")
}

func suitePromotionFromL2(t *testing.T, newCache cacheFactory) {
	ctx := context.Background()
	prefix := uniquePrefix(t)
	// WriteThrough: the reader below must see the value in L2 immediately after Set returns.
	writer := newCache(t, prefix, memstash.WithWritePolicy(memstash.WriteThrough))
	require.NoError(t, writer.Set(ctx, "k", "v"))

	reader := newCache(t, prefix) // fresh L1, same L2 namespace
	v, ok, err := reader.Get(ctx, "k")
	require.NoError(t, err)
	require.True(t, ok, "Get through L2")
	assert.Equal(t, "v", v)

	// The value must have been promoted into the reader's memory.
	v, ok = reader.GetFromMemory("k")
	require.True(t, ok, "value was not promoted to L1")
	assert.Equal(t, "v", v)
}

func suiteDeletePropagatesToL2(t *testing.T, newCache cacheFactory) {
	ctx := context.Background()
	prefix := uniquePrefix(t)
	writer := newCache(t, prefix, memstash.WithWritePolicy(memstash.WriteThrough))
	require.NoError(t, writer.Set(ctx, "k", "v"))
	require.NoError(t, writer.Delete(ctx, "k"))

	reader := newCache(t, prefix)
	_, ok, err := reader.Get(ctx, "k")
	require.NoError(t, err)
	assert.False(t, ok, "value survived Delete in L2")
}

func suiteWriteBackFlushOnClose(t *testing.T, newCache cacheFactory) {
	ctx := context.Background()
	prefix := uniquePrefix(t)
	writer := newCache(t, prefix, memstash.WithWritePolicy(memstash.WriteBack))
	const keys = 200
	for i := 0; i < keys; i++ {
		require.NoError(t, writer.Set(ctx, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)))
	}
	writer.Close() // must drain the write-back buffer into L2

	reader := newCache(t, prefix)
	for i := 0; i < keys; i++ {
		v, ok, err := reader.Get(ctx, fmt.Sprintf("k%d", i))
		require.NoError(t, err)
		require.True(t, ok, "write-back lost k%d", i)
		assert.Equal(t, fmt.Sprintf("v%d", i), v)
	}
}

// suiteWriteBackVisibleAfterWait exercises the default policy (WriteBack): Sets return immediately, and Wait is the
// checkpoint after which the background worker is guaranteed to have delivered them to L2 - all without stopping the
// cache. The verification deliberately runs in a separate goroutine.
func suiteWriteBackVisibleAfterWait(t *testing.T, newCache cacheFactory) {
	ctx := context.Background()
	prefix := uniquePrefix(t)
	writer := newCache(t, prefix) // the default policy is WriteBack
	reader := newCache(t, prefix) // created up front: the factory must be called from the test goroutine

	const keys = 100
	for i := 0; i < keys; i++ {
		require.NoError(t, writer.Set(ctx, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)))
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		writer.Wait() // returns once every Set above has been handed to L2
		for i := 0; i < keys; i++ {
			v, ok, err := reader.Get(ctx, fmt.Sprintf("k%d", i))
			assert.NoError(t, err)
			assert.True(t, ok, "write-back write k%d is not visible in L2 after Wait", i)
			assert.Equal(t, fmt.Sprintf("v%d", i), v)
		}
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("Wait did not return")
	}

	// Wait is a checkpoint, not a shutdown: the cache keeps accepting writes afterwards.
	require.NoError(t, writer.Set(ctx, "after-wait", "v"))
	v, ok := writer.GetFromMemory("after-wait")
	require.True(t, ok, "the cache stopped serving after Wait")
	assert.Equal(t, "v", v)
}

// suiteBatchSetGet drives the adapter's native batch paths (pipelines, DoMulti, SendMany, GetMulti) against the live
// server: one BatchSet, then a fresh cache resolves a mixed BatchGet (L1 hit + L2 hits + a miss) with promotion.
func suiteBatchSetGet(t *testing.T, newCache cacheFactory) {
	ctx := context.Background()
	prefix := uniquePrefix(t)
	writer := newCache(t, prefix, memstash.WithWritePolicy(memstash.WriteThrough))

	const batch = 50
	items := make(memstash.List[string, string], 0, batch)
	keys := make([]string, 0, batch+2)
	for i := 0; i < batch; i++ {
		key := fmt.Sprintf("k%d", i)
		items = append(items, memstash.KeyVal[string, string]{Key: key, Value: fmt.Sprintf("v%d", i)})
		keys = append(keys, key)
	}
	require.NoError(t, writer.BatchSet(ctx, items))

	reader := newCache(t, prefix)
	require.NoError(t, reader.Set(ctx, "mem-only", "m")) // an L1 resident joins the same batch read
	keys = append(keys, "mem-only", "absent")

	got, err := reader.BatchGet(ctx, keys)
	require.NoError(t, err)
	assert.Len(t, got, batch+1, "expected every stored key plus the L1 resident, without the absent one")
	gotByKey := got.ToMap()
	for _, item := range items {
		assert.Equal(t, item.Value, gotByKey[item.Key], "key %s", item.Key)
	}
	assert.Equal(t, "m", gotByKey["mem-only"])
	_, ok := gotByKey["absent"]
	assert.False(t, ok, "a key never stored must be absent from the batch result")

	// The L2 part of the batch must have been promoted into the reader's memory.
	_, ok = reader.GetFromMemory("k0")
	assert.True(t, ok, "BatchGet did not promote L2 hits to L1")
}

// suiteEvictedItemsServedFromL2 is the intended default mode of the two-level cache: the hot set lives in L1, a long
// TTL keeps everything alive in L2, and keys evicted from the small memory level are transparently served (and
// re-promoted) from L2.
func suiteEvictedItemsServedFromL2(t *testing.T, newCache cacheFactory) {
	ctx := context.Background()
	prefix := uniquePrefix(t)
	c := newCache(t, prefix,
		memstash.WithMemoryCapacity(32),
		memstash.WithTTL(time.Hour), // long TTL: the values outlive their stay in L1
	)

	const keys = 256 // 8x the L1 capacity - most keys are evicted from memory
	for i := 0; i < keys; i++ {
		require.NoError(t, c.Set(ctx, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)))
	}
	c.Wait() // the write-back worker must have delivered every value to L2 before we start reading

	evicted := 0
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("k%d", i)
		if _, ok := c.GetFromMemory(key); !ok {
			evicted++
		}
		v, ok, err := c.Get(ctx, key) // an L1 miss falls through to L2 and promotes the value back
		require.NoError(t, err)
		require.True(t, ok, "key %s lost: neither in memory nor in L2", key)
		assert.Equal(t, fmt.Sprintf("v%d", i), v)
	}
	require.Positive(t, evicted, "nothing was evicted from L1 - the scenario did not exercise the L2 path")
}

func suiteGetOrLoadPrefersL2(t *testing.T, newCache cacheFactory) {
	ctx := context.Background()
	prefix := uniquePrefix(t)
	writer := newCache(t, prefix, memstash.WithWritePolicy(memstash.WriteThrough))
	require.NoError(t, writer.Set(ctx, "k", "from-l2"))

	reader := newCache(t, prefix)
	v, err := reader.GetOrLoad(ctx, "k", func(context.Context, string) (string, error) {
		t.Error("loader must not be called when the value is present in L2")
		return "", nil
	})
	require.NoError(t, err)
	assert.Equal(t, "from-l2", v)
}

func suiteTTLReachesL2(t *testing.T, newCache cacheFactory) {
	if testing.Short() {
		t.Skip("slow TTL test")
	}
	ctx := context.Background()
	prefix := uniquePrefix(t)
	writer := newCache(t, prefix, memstash.WithTTL(2*time.Second), memstash.WithWritePolicy(memstash.WriteThrough))
	require.NoError(t, writer.Set(ctx, "k", "v"))
	_, ok, err := writer.Get(ctx, "k")
	require.NoError(t, err)
	require.True(t, ok, "value is unavailable right after Set")

	time.Sleep(4 * time.Second)

	reader := newCache(t, prefix) // empty L1 - the answer comes from L2, where the TTL must have expired
	_, ok, err = reader.Get(ctx, "k")
	require.NoError(t, err)
	assert.False(t, ok, "value survived its TTL in L2")
}

func suiteConcurrentMixed(t *testing.T, newCache cacheFactory) {
	ctx := context.Background()
	c := newCache(t, uniquePrefix(t))
	const (
		goroutines = 4
		ops        = 2000
		keySpace   = 200
	)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				key := fmt.Sprintf("k%d", (seed*ops+i)%keySpace)
				switch i % 10 {
				case 0:
					assert.NoError(t, c.Delete(ctx, key), "Delete %s", key)
				case 1, 2, 3:
					assert.NoError(t, c.Set(ctx, key, "v-"+key), "Set %s", key)
				default:
					v, ok, err := c.Get(ctx, key)
					assert.NoError(t, err, "Get %s", key)
					if ok {
						assert.Equal(t, "v-"+key, v, "corrupted value for %s", key)
					}
				}
			}
		}(g)
	}
	wg.Wait()
}
