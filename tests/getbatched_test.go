package tests

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
)

// coalesceL2 records every read the pool makes and holds the calls until the test releases them, so the test
// controls exactly what piles up in the read queue while a worker is busy.
type coalesceL2 struct {
	mu      sync.Mutex
	m       map[string]string
	gets    []string
	batches [][]string

	hold        chan struct{}
	entered     chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once

	err    error
	panics atomic.Bool
}

func newCoalesceL2(items map[string]string) *coalesceL2 {
	if items == nil {
		items = map[string]string{}
	}
	return &coalesceL2{m: items, hold: make(chan struct{}), entered: make(chan struct{})}
}

func (l *coalesceL2) release() { l.releaseOnce.Do(func() { close(l.hold) }) }

func (l *coalesceL2) waitEntered(t *testing.T) {
	t.Helper()
	select {
	case <-l.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("no L2 read reached the adapter")
	}
}

func (l *coalesceL2) enter() {
	l.enterOnce.Do(func() { close(l.entered) })
	<-l.hold
	if l.panics.CompareAndSwap(true, false) {
		panic("l2 exploded")
	}
}

func (l *coalesceL2) readCalls() (gets []string, batches [][]string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.gets...), append([][]string(nil), l.batches...)
}

func (l *coalesceL2) Get(_ context.Context, key string) (string, bool, error) {
	l.enter()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.gets = append(l.gets, key)
	if l.err != nil {
		return "", false, l.err
	}
	value, ok := l.m[key]
	return value, ok, nil
}

func (l *coalesceL2) BatchGet(_ context.Context, keys []string) (memstash.List[string, string], error) {
	l.enter()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.batches = append(l.batches, append([]string(nil), keys...))
	if l.err != nil {
		return nil, l.err
	}
	found := make(memstash.List[string, string], 0, len(keys))
	for _, key := range keys {
		if value, ok := l.m[key]; ok {
			found = append(found, memstash.KeyVal[string, string]{Key: key, Value: value})
		}
	}
	return found, nil
}

func (l *coalesceL2) Set(_ context.Context, key, value string, _ time.Duration) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.m[key] = value
	return nil
}

func (l *coalesceL2) BatchSet(_ context.Context, items memstash.List[string, string], _ time.Duration) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, item := range items {
		l.m[item.Key] = item.Value
	}
	return nil
}

func (l *coalesceL2) Delete(_ context.Context, key string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.m, key)
	return nil
}

func (l *coalesceL2) BatchDelete(_ context.Context, keys []string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, key := range keys {
		delete(l.m, key)
	}
	return nil
}

func newBatchedCache(t *testing.T, l2 memstash.L2Cache[string, string], opts ...memstash.Option) *memstash.Cache[string, string] {
	t.Helper()
	base := []memstash.Option{
		memstash.WithMemoryCapacity(1000),
		memstash.WithL2Cache[string, string](l2),
		memstash.WithWritePolicy(memstash.WriteDisabled),
		memstash.WithStats(),
	}
	cache, err := memstash.New[string, string](append(base, opts...)...)
	require.NoError(t, err)
	return cache
}

// fireGets starts one GetBatched per key and reports the results back; it returns once every goroutine has reached
// the call, so a test holding the adapter knows the requests are on their way to the queue.
func fireGets(cache *memstash.Cache[string, string], keys []string) <-chan getResult {
	results := make(chan getResult, len(keys))
	var started sync.WaitGroup
	started.Add(len(keys))
	for _, key := range keys {
		go func() {
			started.Done()
			value, found, err := cache.GetBatched(context.Background(), key)
			results <- getResult{key: key, value: value, found: found, err: err}
		}()
	}
	started.Wait()
	time.Sleep(50 * time.Millisecond) // the sends land while the only worker is held inside the adapter
	return results
}

type getResult struct {
	key   string
	value string
	found bool
	err   error
}

func collectResults(t *testing.T, results <-chan getResult, count int) []getResult {
	t.Helper()
	got := make([]getResult, 0, count)
	for range count {
		select {
		case res := <-results:
			got = append(got, res)
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d reads came back", len(got), count)
		}
	}
	return got
}

func TestGetBatchedMemoryHitStaysLocal(t *testing.T) {
	l2 := newCoalesceL2(nil)
	l2.release()
	cache := newBatchedCache(t, l2)
	defer cache.Close()

	require.NoError(t, cache.Set(context.Background(), "k", "v"))

	value, found, err := cache.GetBatched(context.Background(), "k")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "v", value)

	gets, batches := l2.readCalls()
	assert.Empty(t, gets, "a memory hit must not reach L2")
	assert.Empty(t, batches)
	stats := cache.Stats()
	assert.Equal(t, int64(1), stats.MemoryHits())

	allocs := testing.AllocsPerRun(200, func() {
		_, _, _ = cache.GetBatched(context.Background(), "k")
	})
	assert.Zero(t, allocs, "a memory hit must not allocate")
}

// missL2 answers "not found" without allocating, so what AllocsPerRun sees on the queued path is the cache's own.
type missL2 struct{}

func (missL2) Get(context.Context, string) (string, bool, error) { return "", false, nil }
func (missL2) BatchGet(context.Context, []string) (memstash.List[string, string], error) {
	return nil, nil
}
func (missL2) Set(context.Context, string, string, time.Duration) error { return nil }
func (missL2) BatchSet(context.Context, memstash.List[string, string], time.Duration) error {
	return nil
}
func (missL2) Delete(context.Context, string) error        { return nil }
func (missL2) BatchDelete(context.Context, []string) error { return nil }

func TestGetBatchedQueuedPathAllocations(t *testing.T) {
	cache := newBatchedCache(t, missL2{}, memstash.WithGetBatchedWorkers(1))
	defer cache.Close()

	_, _, err := cache.GetBatched(context.Background(), "k") // start the pool and warm the slot pool
	require.NoError(t, err)

	ctx := context.Background()
	allocs := testing.AllocsPerRun(500, func() {
		_, _, _ = cache.GetBatched(ctx, "k")
	})
	assert.LessOrEqual(t, allocs, 0.05, "a queued read must recycle its slot, not allocate one")
}

func TestGetBatchedPromotesFromL2(t *testing.T) {
	l2 := newCoalesceL2(map[string]string{"k": "v"})
	l2.release()
	cache := newBatchedCache(t, l2)
	defer cache.Close()

	value, found, err := cache.GetBatched(context.Background(), "k")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "v", value)

	value, found = cache.GetFromMemory("k")
	assert.True(t, found, "an L2 hit must be promoted into memory")
	assert.Equal(t, "v", value)
	stats := cache.Stats()
	assert.Equal(t, int64(1), stats.L2Hits())
}

func TestGetBatchedMissingKey(t *testing.T) {
	l2 := newCoalesceL2(nil)
	l2.release()
	cache := newBatchedCache(t, l2)
	defer cache.Close()

	value, found, err := cache.GetBatched(context.Background(), "nope")
	require.NoError(t, err)
	assert.False(t, found)
	assert.Empty(t, value)
	stats := cache.Stats()
	assert.Equal(t, int64(1), stats.L2Misses())
}

func TestGetBatchedWithoutL2(t *testing.T) {
	cache, err := memstash.New[string, string](memstash.WithMemoryCapacity(100), memstash.WithStats())
	require.NoError(t, err)
	defer cache.Close()

	_, found, err := cache.GetBatched(context.Background(), "k")
	require.NoError(t, err)
	assert.False(t, found)
	stats := cache.Stats()
	assert.Equal(t, int64(1), stats.MemoryMisses())
}

func TestGetBatchedCoalescesConcurrentMisses(t *testing.T) {
	items := map[string]string{}
	keys := make([]string, 32)
	for i := range keys {
		keys[i] = string(rune('a' + i))
		items[keys[i]] = "v" + keys[i]
	}
	l2 := newCoalesceL2(items)
	cache := newBatchedCache(t, l2, memstash.WithGetBatchedWorkers(1))
	defer cache.Close()

	first := fireGets(cache, keys[:1])
	l2.waitEntered(t) // the only worker is now inside the adapter, so everything else queues up
	rest := fireGets(cache, keys[1:])
	l2.release()

	for _, res := range append(collectResults(t, first, 1), collectResults(t, rest, len(keys)-1)...) {
		require.NoError(t, res.err)
		assert.True(t, res.found, res.key)
		assert.Equal(t, items[res.key], res.value)
	}

	gets, batches := l2.readCalls()
	assert.Len(t, gets, 1, "only the read that started the batch goes out on its own")
	require.NotEmpty(t, batches)
	assert.Greater(t, len(batches[0]), 1, "the queued reads must be coalesced into one BatchGet")
	assert.Less(t, len(gets)+len(batches), len(keys), "L2 must see fewer calls than there were readers")
}

func TestGetBatchedDeduplicatesHotKey(t *testing.T) {
	l2 := newCoalesceL2(nil) // the key is missing, so nothing gets promoted between the two batches
	cache := newBatchedCache(t, l2, memstash.WithGetBatchedWorkers(1))
	defer cache.Close()

	keys := make([]string, 32)
	for i := range keys {
		keys[i] = "hot"
	}
	first := fireGets(cache, keys[:1])
	l2.waitEntered(t)
	rest := fireGets(cache, keys[1:])
	l2.release()

	for _, res := range append(collectResults(t, first, 1), collectResults(t, rest, len(keys)-1)...) {
		require.NoError(t, res.err)
		assert.False(t, res.found)
	}

	gets, batches := l2.readCalls()
	assert.Equal(t, []string{"hot", "hot"}, gets, "31 queued readers of one key cost one lookup")
	assert.Empty(t, batches, "a batch of one distinct key goes out as a plain Get")
}

func TestGetBatchedSkipsKeysPromotedWhileQueued(t *testing.T) {
	l2 := newCoalesceL2(map[string]string{"hot": "v"})
	cache := newBatchedCache(t, l2, memstash.WithGetBatchedWorkers(1))
	defer cache.Close()

	keys := make([]string, 16)
	for i := range keys {
		keys[i] = "hot"
	}
	first := fireGets(cache, keys[:1])
	l2.waitEntered(t)
	rest := fireGets(cache, keys[1:])
	l2.release()

	for _, res := range append(collectResults(t, first, 1), collectResults(t, rest, len(keys)-1)...) {
		require.NoError(t, res.err)
		assert.True(t, res.found)
		assert.Equal(t, "v", res.value)
	}

	gets, batches := l2.readCalls()
	assert.Equal(t, []string{"hot"}, gets, "the queued readers find the key in memory and never reach L2")
	assert.Empty(t, batches)
}

func TestGetBatchedContextCancelWhileQueued(t *testing.T) {
	l2 := newCoalesceL2(map[string]string{"a": "va", "b": "vb"})
	cache := newBatchedCache(t, l2, memstash.WithGetBatchedWorkers(1))
	defer cache.Close()

	busy := fireGets(cache, []string{"a"})
	l2.waitEntered(t)

	ctx, cancel := context.WithCancel(context.Background())
	queued := make(chan getResult, 1)
	go func() {
		value, found, err := cache.GetBatched(ctx, "b")
		queued <- getResult{value: value, found: found, err: err}
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case res := <-queued:
		require.ErrorIs(t, res.err, context.Canceled)
		assert.False(t, res.found)
	case <-time.After(3 * time.Second):
		t.Fatal("a canceled read must not wait for its batch")
	}

	l2.release()
	collectResults(t, busy, 1)
}

func TestGetBatchedReturnsL2ErrorToEveryCaller(t *testing.T) {
	l2 := newCoalesceL2(nil)
	l2.err = errors.New("l2 down")
	cache := newBatchedCache(t, l2, memstash.WithGetBatchedWorkers(1))
	defer cache.Close()

	keys := []string{"a", "b", "c", "d"}
	first := fireGets(cache, keys[:1])
	l2.waitEntered(t)
	rest := fireGets(cache, keys[1:])
	l2.release()

	for _, res := range append(collectResults(t, first, 1), collectResults(t, rest, len(keys)-1)...) {
		require.Error(t, res.err, res.key)
		assert.Contains(t, res.err.Error(), "l2 down")
		assert.False(t, res.found)
	}
}

func TestGetBatchedAdapterPanicWakesCallers(t *testing.T) {
	l2 := newCoalesceL2(map[string]string{"k": "v"})
	l2.panics.Store(true)
	var handled atomic.Bool
	cache := newBatchedCache(t, l2, memstash.WithPanicHandler(func(_ any, wasHandled bool) {
		handled.Store(wasHandled)
	}))
	defer cache.Close()
	l2.release()

	_, found, err := cache.GetBatched(context.Background(), "k")
	require.ErrorIs(t, err, memstash.ErrPanic)
	assert.False(t, found)
	assert.True(t, handled.Load(), "the panic reached the caller, so it counts as handled")

	value, found, err := cache.GetBatched(context.Background(), "k")
	require.NoError(t, err, "the pool must survive a panicking adapter")
	assert.True(t, found)
	assert.Equal(t, "v", value)
}

func TestGetBatchedAfterCloseReadsL2Directly(t *testing.T) {
	l2 := newCoalesceL2(map[string]string{"k": "v"})
	l2.release()
	cache := newBatchedCache(t, l2)

	_, _, err := cache.GetBatched(context.Background(), "k") // starts the pool
	require.NoError(t, err)
	require.NoError(t, cache.Delete(context.Background(), "k"))
	cache.Close()

	value, found, err := cache.GetBatched(context.Background(), "k")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "v", value)
}

func TestGetBatchedCloseReleasesWaitingCallers(t *testing.T) {
	l2 := newCoalesceL2(map[string]string{"a": "va", "b": "vb", "c": "vc"})
	cache := newBatchedCache(t, l2, memstash.WithGetBatchedWorkers(1))

	busy := fireGets(cache, []string{"a"})
	l2.waitEntered(t)
	waiting := fireGets(cache, []string{"b", "c"})

	closed := make(chan struct{})
	go func() {
		cache.Close()
		close(closed)
	}()
	time.Sleep(50 * time.Millisecond)
	l2.release()

	for _, res := range append(collectResults(t, busy, 1), collectResults(t, waiting, 2)...) {
		require.NoError(t, res.err)
		assert.True(t, res.found, res.key)
	}
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not return")
	}
}

func TestGetBatchedStartsWorkersLazily(t *testing.T) {
	l2 := newCoalesceL2(map[string]string{"k": "v"})
	l2.release()

	before := runtime.NumGoroutine()
	cache := newBatchedCache(t, l2, memstash.WithGetBatchedWorkers(3))
	require.Equal(t, before, runtime.NumGoroutine(), "an unused cache must run no goroutines")

	_, _, err := cache.GetBatched(context.Background(), "k")
	require.NoError(t, err)
	assert.Equal(t, before+3, runtime.NumGoroutine(), "the first read that misses memory starts the pool")

	cache.Close()
	deadline := time.Now().Add(3 * time.Second) // polled by hand: assert.Eventually runs its condition in a goroutine
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	assert.LessOrEqual(t, runtime.NumGoroutine(), before, "Close must stop the read pool")
}

// TestGetBatchedStress exercises the slot hand-off the way the race detector needs it: readers that take their
// answer, readers that walk away on ctx, and writers evicting the keys underneath them.
func TestGetBatchedStress(t *testing.T) {
	items := map[string]string{}
	for i := range 64 {
		items[strconv.Itoa(i)] = "v" + strconv.Itoa(i)
	}
	l2 := newCoalesceL2(items)
	l2.release()
	cache := newBatchedCache(t, l2, memstash.WithMemoryCapacity(16), memstash.WithGetBatchedBuffer(16),
		memstash.WithGetBatchedWorkers(3))
	defer cache.Close()

	var wg sync.WaitGroup
	for worker := range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 200 {
				key := strconv.Itoa((worker*7 + i) % 96) // half the keys are not in L2 at all
				ctx, cancel := context.Background(), context.CancelFunc(nil)
				if i%5 == 0 { // a deadline this tight leaves plenty of reads unclaimed
					ctx, cancel = context.WithTimeout(ctx, time.Microsecond)
				}
				value, found, err := cache.GetBatched(ctx, key)
				if cancel != nil {
					cancel()
				}
				if err != nil {
					require.ErrorIs(t, err, context.DeadlineExceeded)
					continue
				}
				if found {
					require.Equal(t, items[key], value)
				} else {
					require.NotContains(t, items, key)
				}
			}
		}()
	}
	wg.Wait()
}

func TestGetBatchedOptions(t *testing.T) {
	l2 := newCoalesceL2(map[string]string{"k": "v"})
	l2.release()

	cache := newBatchedCache(t, l2, memstash.WithGetBatchedBuffer(8), memstash.WithGetBatchedWorkers(2))
	defer cache.Close()
	_, found, err := cache.GetBatched(context.Background(), "k")
	require.NoError(t, err)
	assert.True(t, found)

	_, err = memstash.New[string, string](memstash.WithMemoryCapacity(10), memstash.WithGetBatchedWorkers(-1))
	assert.ErrorIs(t, err, memstash.ErrBadGetBatchedWorkers)
}

// slowL2 answers after a fixed round trip and counts what it was asked, so a benchmark can weigh coalescing by the
// only thing that matters to a real second level: how many calls it had to serve.
type slowL2 struct {
	rtt   time.Duration
	calls atomic.Int64
}

func (l *slowL2) Get(_ context.Context, key string) (string, bool, error) {
	l.calls.Add(1)
	time.Sleep(l.rtt)
	return "v" + key, true, nil
}

func (l *slowL2) BatchGet(_ context.Context, keys []string) (memstash.List[string, string], error) {
	l.calls.Add(1)
	time.Sleep(l.rtt)
	found := make(memstash.List[string, string], 0, len(keys))
	for _, key := range keys {
		found = append(found, memstash.KeyVal[string, string]{Key: key, Value: "v" + key})
	}
	return found, nil
}

func (l *slowL2) Set(context.Context, string, string, time.Duration) error { return nil }
func (l *slowL2) BatchSet(context.Context, memstash.List[string, string], time.Duration) error {
	return nil
}
func (l *slowL2) Delete(context.Context, string) error        { return nil }
func (l *slowL2) BatchDelete(context.Context, []string) error { return nil }

// BenchmarkL2BoundReads compares Get and GetBatched on a workload that misses memory almost every time: same
// latency budget per caller, but l2calls/op says how much of it the second level actually sees.
func BenchmarkL2BoundReads(b *testing.B) {
	keys := make([]string, 4096)
	for i := range keys {
		keys[i] = strconv.Itoa(i)
	}
	for _, mode := range []string{"Get", "GetBatched"} {
		b.Run(mode, func(b *testing.B) {
			l2 := &slowL2{rtt: time.Millisecond}
			cache, err := memstash.New[string, string](
				memstash.WithMemoryCapacity(64), // far below the key space, so nearly every read reaches L2
				memstash.WithL2Cache[string, string](l2),
				memstash.WithWritePolicy(memstash.WriteDisabled),
			)
			require.NoError(b, err)
			defer cache.Close()

			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := int(time.Now().UnixNano())
				for pb.Next() {
					i++
					key := keys[i&(len(keys)-1)]
					if mode == "Get" {
						_, _, _ = cache.Get(ctx, key)
						continue
					}
					_, _, _ = cache.GetBatched(ctx, key)
				}
			})
			b.StopTimer()
			b.ReportMetric(float64(l2.calls.Load())/float64(b.N), "l2calls/op")
		})
	}
}

func BenchmarkGetBatchedMemoryHit(b *testing.B) {
	l2 := newCoalesceL2(nil)
	l2.release()
	cache, err := memstash.New[string, string](
		memstash.WithMemoryCapacity(1000),
		memstash.WithL2Cache[string, string](l2),
		memstash.WithWritePolicy(memstash.WriteDisabled),
	)
	require.NoError(b, err)
	defer cache.Close()
	require.NoError(b, cache.Set(context.Background(), "k", "v"))

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, _, _ = cache.GetBatched(ctx, "k")
	}
}
