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

// blockingL2Stub holds every write inside L2 until releaseAt of them are in flight at once, or the wait times out.
// The peak it records is therefore the number of write-back workers that reached L2 together.
type blockingL2Stub struct {
	releaseAt int

	mu       sync.Mutex
	inFlight int
	peak     int
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
	for _, item := range items {
		b.record(item.Key, item.Value)
	}
	return nil
}

func (b *blockingL2Stub) Delete(context.Context, string) error { return nil }

func (b *blockingL2Stub) BatchDelete(context.Context, []string) error { return nil }

// TestWriteBackPoolRunsWorkersInParallel: the write-back pool must have as many workers writing to L2 at once as it
// was configured for - DefaultWriteBackWorkers when the option is not given.
func TestWriteBackPoolRunsWorkersInParallel(t *testing.T) {
	for _, workers := range []int{memstash.DefaultWriteBackWorkers, 1, 2, 7} {
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

			// Plenty of distinct keys, so every queue gets work whatever the hash does.
			for i := range 500 {
				require.NoError(t, c.Set(context.Background(), fmt.Sprintf("k-%d", i), "v"))
			}
			c.Wait()

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
	c.Wait()

	_, values := l2.snapshot()
	got := values["k"]
	require.NotEmpty(t, got)
	assert.Equal(t, fmt.Sprintf("v-%04d", writes-1), got[len(got)-1], "the last write must land last")
	assert.IsIncreasing(t, got)
}

// TestWriteBackWorkersValidation: a negative worker count is a configuration error, zero means the default.
func TestWriteBackWorkersValidation(t *testing.T) {
	_, err := memstash.New[string, string](memstash.WithWriteBackWorkers(-1))
	assert.ErrorIs(t, err, memstash.ErrBadWorkers)

	c, err := memstash.New[string, string](memstash.WithWriteBackWorkers(0))
	require.NoError(t, err)
	c.Close()
}
