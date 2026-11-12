package tests

import (
	"context"
	"errors"
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

// blockingL2Stub holds every write inside L2 until releaseAt of them are in flight at once, or the wait times out.
// The peak it records is therefore the number of write-back workers that reached L2 together.
type blockingL2Stub struct {
	releaseAt int

	mu       sync.Mutex
	inFlight int
	peak     int
	largest  int                 // the biggest batch L2 received
	values   map[string][]string // per key, in the order L2 received them

	releaseOnce sync.Once
	released    chan struct{}
}

func newBlockingL2Stub(releaseAt int) *blockingL2Stub {
	return &blockingL2Stub{releaseAt: releaseAt, values: map[string][]string{}, released: make(chan struct{})}
}

func (b *blockingL2Stub) hold() {
	b.mu.Lock()
	b.inFlight++
	b.peak = max(b.peak, b.inFlight)
	full := b.inFlight >= b.releaseAt
	b.mu.Unlock()

	if full {
		b.releaseOnce.Do(func() { close(b.released) })
	}
	select {
	case <-b.released:
	case <-time.After(2 * time.Second): // let the test finish and fail on peak
	}

	b.mu.Lock()
	b.inFlight--
	b.mu.Unlock()
}

func (b *blockingL2Stub) record(key, value string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.values[key] = append(b.values[key], value)
}

func (b *blockingL2Stub) snapshot() (peak int, values map[string][]string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	clone := make(map[string][]string, len(b.values))
	for key, vals := range b.values {
		clone[key] = append([]string(nil), vals...)
	}
	return b.peak, clone
}

func (b *blockingL2Stub) Get(context.Context, string) (string, bool, error) { return "", false, nil }

func (b *blockingL2Stub) BatchGet(context.Context, []string) (memstash.List[string, string], error) {
	return nil, nil
}

func (b *blockingL2Stub) Set(_ context.Context, key, value string, _ time.Duration) error {
	b.hold()
	b.record(key, value)
	return nil
}

func (b *blockingL2Stub) BatchSet(_ context.Context, items memstash.List[string, string], _ time.Duration) error {
	b.hold()
	b.mu.Lock()
	b.largest = max(b.largest, len(items))
	b.mu.Unlock()
	for _, item := range items {
		b.record(item.Key, item.Value)
	}
	return nil
}

func (b *blockingL2Stub) Delete(context.Context, string) error { return nil }

func (b *blockingL2Stub) BatchDelete(context.Context, []string) error { return nil }

// TestWriteBackPoolRunsWorkersInParallel: the write-back pool must have as many workers writing to L2 at once as it
// was configured for - DefaultWriteBackWorkers when the option is not given - and no more while no queue holds more
// than a batch.
func TestWriteBackPoolRunsWorkersInParallel(t *testing.T) {
	for _, workers := range []int{memstash.DefaultWriteBackWorkers, 1, 2, 7, 13} {
		t.Run(fmt.Sprintf("%d workers", workers), func(t *testing.T) {
			l2 := newBlockingL2Stub(workers)
			opts := []memstash.Option{
				memstash.WithMemoryCapacity(1000),
				memstash.WithL2Cache[string, string](l2),
				memstash.WithWritePolicy(memstash.WriteBack),
				memstash.WithWriteBackBuffer(4096), // no overflow: a synchronous fallback would fake the concurrency
			}
			if workers != memstash.DefaultWriteBackWorkers {
				opts = append(opts, memstash.WithWriteBackWorkers(workers))
			}
			c, err := memstash.New[string, string](opts...)
			require.NoError(t, err)
			defer c.Close()

			// 32 distinct keys a worker: every queue gets work whatever the hash does, and none of them more than a
			// batch, which a worker would send beside the first.
			for i := range 32 * workers {
				require.NoError(t, c.Set(context.Background(), fmt.Sprintf("k-%d", i), "v"))
			}
			c.Wait(context.Background())

			peak, _ := l2.snapshot()
			assert.Equal(t, workers, peak)
		})
	}
}

// TestWriteBackPoolKeepsPerKeyOrder: a key belongs to one worker, so its overwrites must reach L2 in the order they
// were made - concurrent workers must not be able to land an older value last.
func TestWriteBackPoolKeepsPerKeyOrder(t *testing.T) {
	l2 := newBlockingL2Stub(1) // no barrier: every write goes straight through
	c, err := memstash.New[string, string](
		memstash.WithMemoryCapacity(1000),
		memstash.WithL2Cache[string, string](l2),
		memstash.WithWritePolicy(memstash.WriteBack),
		memstash.WithWriteBackBuffer(4096),
	)
	require.NoError(t, err)
	defer c.Close()

	const writes = 300
	for i := range writes {
		require.NoError(t, c.Set(context.Background(), "k", fmt.Sprintf("v-%04d", i)))
	}
	c.Wait(context.Background())

	_, values := l2.snapshot()
	got := values["k"]
	require.NotEmpty(t, got)
	assert.Equal(t, fmt.Sprintf("v-%04d", writes-1), got[len(got)-1], "the last write must land last")
	assert.IsIncreasing(t, got)
}

// backlogL2 is blockingL2Stub that keeps the worker inside the Set of the key "gate" until open is closed, so a test
// can queue a backlog behind it.
type backlogL2 struct {
	*blockingL2Stub
	entered, open chan struct{}
	failBatch     atomic.Bool // the next BatchSet fails
}

const deleted = "<deleted>" // what backlogL2 records for a Delete

func newBacklogL2(releaseAt int) *backlogL2 {
	stub := newBlockingL2Stub(releaseAt)
	return &backlogL2{blockingL2Stub: stub, entered: make(chan struct{}), open: make(chan struct{})}
}

func (g *backlogL2) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	if key == "gate" {
		close(g.entered)
		<-g.open
		return nil
	}
	return g.blockingL2Stub.Set(ctx, key, value, ttl)
}

func (g *backlogL2) BatchSet(ctx context.Context, items memstash.List[string, string], ttl time.Duration) error {
	if g.failBatch.CompareAndSwap(true, false) {
		return errors.New("l2 is down")
	}
	return g.blockingL2Stub.BatchSet(ctx, items, ttl)
}

func (g *backlogL2) Delete(_ context.Context, key string) error {
	g.record(key, deleted)
	return nil
}

// backlogFanout is writeBackFanout in cache.go: the batches one worker sends at once from a backlog.
const backlogFanout = 4

// TestWriteBackBacklogFansOut: a worker that finds more than a batch in its queue sends backlogFanout batches at
// once instead of one after another, none of them over WriteBackBatchMax.
func TestWriteBackBacklogFansOut(t *testing.T) {
	ctx := context.Background()
	l2 := newBacklogL2(backlogFanout)
	c, err := memstash.New[string, string](
		memstash.WithL2Cache[string, string](l2),
		memstash.WithWriteBackWorkers(1),
		memstash.WithWriteBackBuffer(4096),
	)
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.Set(ctx, "gate", ""))
	<-l2.entered
	const keys = 300 // more than a batch, less than one run takes: all of it goes out together
	for i := range keys {
		require.NoError(t, c.Set(ctx, fmt.Sprintf("k-%d", i), "v"))
	}
	close(l2.open)
	require.NoError(t, c.Wait(ctx))

	peak, values := l2.snapshot()
	assert.Equal(t, backlogFanout, peak)
	assert.Len(t, values, keys)
	assert.LessOrEqual(t, l2.largest, memstash.WriteBackBatchMax)
}

// TestWriteBackBacklogKeepsPerKeyOrder: the batches a backlog goes out in at once must not share a key, or its
// writes would race each other to L2. The hot key alone is more than a batch, so its part goes out in several,
// one after another. The delete queued behind the backlog must wait for all of it.
func TestWriteBackBacklogKeepsPerKeyOrder(t *testing.T) {
	ctx := context.Background()
	l2 := newBacklogL2(1) // no barrier: the batches run into L2 as they come
	c, err := memstash.New[string, string](
		memstash.WithL2Cache[string, string](l2),
		memstash.WithWriteBackWorkers(1),
		memstash.WithWriteBackBuffer(4096),
	)
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.Set(ctx, "gate", ""))
	<-l2.entered
	const rounds, keys = 15, 10
	for round := range rounds {
		for k := range keys {
			require.NoError(t, c.Set(ctx, fmt.Sprintf("k-%d", k), fmt.Sprintf("v-%04d", round)))
		}
		for i := range 10 {
			require.NoError(t, c.Set(ctx, "hot", fmt.Sprintf("v-%04d", round*10+i)))
		}
	}
	require.NoError(t, c.Delete(ctx, "hot"))
	close(l2.open)
	require.NoError(t, c.Wait(ctx))

	_, values := l2.snapshot()
	require.Len(t, values, keys+1)
	for key, got := range values {
		want := rounds
		if key == "hot" {
			want = rounds * 10
			require.Equal(t, deleted, got[len(got)-1], "the delete made last must land last")
			got = got[:len(got)-1]
		}
		assert.Len(t, got, want, "key %s", key)
		assert.IsIncreasing(t, got, "key %s", key)
	}
	assert.LessOrEqual(t, l2.largest, memstash.WriteBackBatchMax)
}

// TestWriteBackHandlerPanicCostsOnlyItsBatch: an OnL2Error that panics over the first batch of a backlog must not
// take along the batches the worker holds behind it. The hot key fills three, one after another in one part: with
// the last of them lost, L2 would keep a stale value.
func TestWriteBackHandlerPanicCostsOnlyItsBatch(t *testing.T) {
	ctx := context.Background()
	l2 := newBacklogL2(1)
	var panicked atomic.Bool
	c, err := memstash.New[string, string](
		memstash.WithL2Cache[string, string](l2),
		memstash.WithWriteBackWorkers(1),
		memstash.WithWriteBackBuffer(4096),
		memstash.WithOnL2Error[string, string](func(string, error) {
			if panicked.CompareAndSwap(false, true) {
				panic("the handler exploded")
			}
		}),
	)
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.Set(ctx, "gate", ""))
	<-l2.entered
	const writes = 300
	for i := range writes {
		require.NoError(t, c.Set(ctx, "hot", fmt.Sprintf("v-%04d", i)))
	}
	l2.failBatch.Store(true)
	close(l2.open)
	require.NoError(t, c.Wait(ctx))

	_, values := l2.snapshot()
	got := values["hot"]
	require.True(t, panicked.Load())
	require.Len(t, got, writes-memstash.WriteBackBatchMax, "all but the failed batch")
	assert.Equal(t, fmt.Sprintf("v-%04d", writes-1), got[len(got)-1])
}

// TestWriteBackWorkersValidation: a negative worker count is a configuration error, zero means the default.
func TestWriteBackWorkersValidation(t *testing.T) {
	_, err := memstash.New[string, string](memstash.WithWriteBackWorkers(-1))
	assert.ErrorIs(t, err, memstash.ErrBadWorkers)

	c, err := memstash.New[string, string](memstash.WithWriteBackWorkers(0))
	require.NoError(t, err)
	c.Close()
}

// holdL2 keeps what L2 received per key and holds the Set of one value until gate is closed.
type holdL2 struct {
	blockingL2Stub
	hold     string
	once     sync.Once
	openOnce sync.Once
	entered  chan struct{}
	gate     chan struct{}
	deleted  map[string]bool
}

func newHoldL2(hold string) *holdL2 {
	return &holdL2{
		blockingL2Stub: *newBlockingL2Stub(1),
		hold:           hold,
		entered:        make(chan struct{}),
		gate:           make(chan struct{}),
		deleted:        map[string]bool{},
	}
}

func (g *holdL2) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	if value == g.hold {
		g.once.Do(func() {
			close(g.entered)
			<-g.gate
		})
	}
	g.mu.Lock()
	g.deleted[key] = false
	g.mu.Unlock()
	return g.blockingL2Stub.Set(ctx, key, value, ttl)
}

func (g *holdL2) open() { g.openOnce.Do(func() { close(g.gate) }) }

func (g *holdL2) Delete(_ context.Context, key string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.deleted[key] = true
	return nil
}

func (g *holdL2) BatchDelete(ctx context.Context, keys []string) error {
	for _, key := range keys {
		_ = g.Delete(ctx, key)
	}
	return nil
}

// TestWriteBackOverflowKeepsPerKeyOrder: a write that finds its worker's buffer full waits for room instead of
// overtaking what the key already queued, so L2 ends with neither a stale value nor a resurrected key.
func TestWriteBackOverflowKeepsPerKeyOrder(t *testing.T) {
	ctx := context.Background()
	for name, last := range map[string]func(*memstash.Cache[string, string]) error{
		"set":    func(c *memstash.Cache[string, string]) error { return c.Set(ctx, "k", "v3") },
		"delete": func(c *memstash.Cache[string, string]) error { return c.Delete(ctx, "k") },
	} {
		t.Run(name, func(t *testing.T) {
			l2 := newHoldL2("v1")
			c, err := memstash.New[string, string](
				memstash.WithL2Cache[string, string](l2),
				memstash.WithWriteBackWorkers(1),
				memstash.WithWriteBackBuffer(1),
				memstash.WithNoBatchingForWriteBack(),
			)
			require.NoError(t, err)
			defer c.Close()
			defer l2.open() // before Close, which waits for the worker held inside L2

			require.NoError(t, c.Set(ctx, "k", "v1"))
			<-l2.entered                              // the worker holds v1 inside L2
			require.NoError(t, c.Set(ctx, "k", "v2")) // fills the buffer
			done := make(chan error, 1)
			go func() { done <- last(c) }()
			select {
			case <-done:
				t.Fatal("a write into a full buffer returned before the worker made room")
			case <-time.After(50 * time.Millisecond):
			}
			l2.open()
			require.NoError(t, <-done)
			c.Wait(context.Background())

			_, values := l2.snapshot()
			l2.mu.Lock()
			deleted := l2.deleted["k"]
			l2.mu.Unlock()
			if name == "delete" {
				assert.True(t, deleted, "the delete made last must land last")
				return
			}
			assert.Equal(t, []string{"v1", "v2", "v3"}, values["k"])
		})
	}
}

// explodingL2 is holdL2 whose next BatchSet or BatchDelete panics once armed.
type explodingL2 struct {
	*holdL2
	panicSet, panicDelete atomic.Bool
}

func (e *explodingL2) BatchSet(ctx context.Context, items memstash.List[string, string], ttl time.Duration) error {
	if e.panicSet.CompareAndSwap(true, false) {
		panic("batch set exploded")
	}
	return e.holdL2.BatchSet(ctx, items, ttl)
}

func (e *explodingL2) BatchDelete(ctx context.Context, keys []string) error {
	if e.panicDelete.CompareAndSwap(true, false) {
		panic("batch delete exploded")
	}
	return e.holdL2.BatchDelete(ctx, keys)
}

// TestWriteBackPanicCostsOnlyItsBatch: an adapter panicking inside a write-back batch is reported for every key of
// that batch and no other, and the task queued right behind it is still delivered.
func TestWriteBackPanicCostsOnlyItsBatch(t *testing.T) {
	ctx := context.Background()
	for name, arm := range map[string]func(*explodingL2){
		"batch set":    func(l2 *explodingL2) { l2.panicSet.Store(true) },
		"batch delete": func(l2 *explodingL2) { l2.panicDelete.Store(true) },
	} {
		t.Run(name, func(t *testing.T) {
			l2 := &explodingL2{holdL2: newHoldL2("hold")}
			var mu sync.Mutex
			reported := map[string]error{}
			c, err := memstash.New[string, string](
				memstash.WithL2Cache[string, string](l2),
				memstash.WithWriteBackWorkers(1),
				memstash.WithOnL2Error[string, string](func(key string, err error) {
					mu.Lock()
					defer mu.Unlock()
					reported[key] = err
				}),
			)
			require.NoError(t, err)
			defer c.Close()
			defer l2.open()

			require.NoError(t, c.Set(ctx, "w", "hold"))
			<-l2.entered // the worker is held, so the next four queue up: a run of sets, then a run of deletes
			require.NoError(t, c.Set(ctx, "a", "1"))
			require.NoError(t, c.Set(ctx, "b", "2"))
			require.NoError(t, c.Delete(ctx, "c"))
			require.NoError(t, c.Delete(ctx, "d"))
			arm(l2)
			l2.open()
			c.Wait(context.Background())

			failed, delivered := []string{"a", "b"}, []string{"c", "d"}
			if name == "batch delete" {
				failed, delivered = delivered, failed
			}
			_, values := l2.snapshot()
			l2.mu.Lock()
			deleted := maps.Clone(l2.deleted)
			l2.mu.Unlock()
			mu.Lock()
			defer mu.Unlock()
			for _, key := range failed {
				assert.ErrorIs(t, reported[key], memstash.ErrPanic, "key %s of the failed batch", key)
			}
			for _, key := range delivered {
				assert.NotContains(t, reported, key)
				assert.True(t, deleted[key] || len(values[key]) > 0, "key %s, queued behind the failed batch, was lost", key)
			}
		})
	}
}
