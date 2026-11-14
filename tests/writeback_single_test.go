package tests

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
)

// singleCallStub is blockingL2Stub as a memstash.SingleCallL2 reporting calls, counting the batch calls the pool
// must not make.
type singleCallStub struct {
	*blockingL2Stub
	calls   int
	batches atomic.Int32
}

func (s *singleCallStub) ConcurrentCalls() int { return s.calls }

func (s *singleCallStub) BatchSet(ctx context.Context, items memstash.List[string, string], ttl time.Duration) error {
	s.batches.Add(1)
	return s.blockingL2Stub.BatchSet(ctx, items, ttl)
}

func (s *singleCallStub) BatchDelete(context.Context, []string) error {
	s.batches.Add(1)
	return nil
}

// TestSingleCallPoolSize: a SingleCallL2 gets one worker per call ConcurrentCalls allows, at least one; every worker
// reaches L2 at the same time, and no batch is sent.
func TestSingleCallPoolSize(t *testing.T) {
	for _, tc := range []struct{ calls, workers int }{{300, 300}, {2, 2}, {0, 1}} {
		t.Run(fmt.Sprintf("%d calls", tc.calls), func(t *testing.T) {
			l2 := &singleCallStub{blockingL2Stub: newBlockingL2Stub(tc.workers), calls: tc.calls}
			c, err := memstash.New[string, string](
				memstash.WithMemoryCapacity(1000),
				memstash.WithL2Cache[string, string](l2),
				memstash.WithWritePolicy(memstash.WriteBack),
				memstash.WithWriteBackBuffer(256), // a queue gets 32 keys on average: Set never waits for room
			)
			require.NoError(t, err)
			defer c.Close()

			for i := range 32 * tc.workers {
				require.NoError(t, c.Set(context.Background(), fmt.Sprintf("k-%d", i), "v"))
			}
			c.Wait(context.Background())

			peak, _ := l2.snapshot()
			assert.Equal(t, tc.workers, peak)
			assert.Zero(t, l2.batches.Load())
		})
	}
}

// TestSingleCallRejectsWriteBackWorkers: the adapter's constructor sizes a SingleCallL2's pool, so WriteBackWorkers
// next to one is a conflict New reports, with any write policy, rather than a setting it quietly ignores.
func TestSingleCallRejectsWriteBackWorkers(t *testing.T) {
	for _, policy := range []memstash.WritePolicy{memstash.WriteBack, memstash.WriteThrough} {
		_, err := memstash.New[string, string](
			memstash.WithL2Cache[string, string](&singleCallStub{blockingL2Stub: newBlockingL2Stub(1), calls: 8}),
			memstash.WithWritePolicy(policy),
			memstash.WithWriteBackWorkers(4),
		)
		require.ErrorIs(t, err, memstash.ErrWorkersSetByL2)
	}
}

// gatedSingleCall is a memstash.SingleCallL2 that records what L2 receives per key and holds the Set of the value
// "gate" until open is closed, so the tasks of that key queue up behind it.
type gatedSingleCall struct {
	blockingL2Stub
	entered, open chan struct{}
	batches       atomic.Int32
	once          sync.Once
}

func newGatedSingleCall() *gatedSingleCall {
	return &gatedSingleCall{
		blockingL2Stub: blockingL2Stub{values: map[string][]string{}},
		entered:        make(chan struct{}),
		open:           make(chan struct{}),
	}
}

func (g *gatedSingleCall) ConcurrentCalls() int { return 1 }

func (g *gatedSingleCall) Set(_ context.Context, key, value string, _ time.Duration) error {
	if value == "gate" {
		g.once.Do(func() { close(g.entered) })
		<-g.open
	}
	g.record(key, value)
	return nil
}

func (g *gatedSingleCall) Delete(_ context.Context, key string) error {
	g.record(key, deleted)
	return nil
}

func (g *gatedSingleCall) BatchSet(_ context.Context, items memstash.List[string, string], _ time.Duration) error {
	g.batches.Add(1)
	for _, item := range items {
		g.record(item.Key, item.Value)
	}
	return nil
}

func (g *gatedSingleCall) BatchDelete(_ context.Context, keys []string) error {
	g.batches.Add(1)
	for _, key := range keys {
		g.record(key, deleted)
	}
	return nil
}

// TestSingleCallCollapsesRepeats: tasks of one key queued behind a busy call reach L2 as one call per run, the last
// task of it, and the runs keep their order across writes and deletes. A pool that sent every task would record each
// value; one that reordered would end on the wrong one.
func TestSingleCallCollapsesRepeats(t *testing.T) {
	ctx := context.Background()
	l2 := newGatedSingleCall()
	c, err := memstash.New[string, string](
		memstash.WithMemoryCapacity(1000),
		memstash.WithL2Cache[string, string](l2),
		memstash.WithWritePolicy(memstash.WriteBack),
		memstash.WithWriteBackBuffer(512), // the whole backlog of the key fits
	)
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.Set(ctx, "k", "gate"))
	<-l2.entered
	for i := range 100 {
		require.NoError(t, c.Set(ctx, "k", fmt.Sprintf("v-%03d", i)))
	}
	require.NoError(t, c.Delete(ctx, "k"))
	require.NoError(t, c.Delete(ctx, "k"))
	require.NoError(t, c.Set(ctx, "k", "last"))
	close(l2.open)
	c.Wait(ctx)

	_, values := l2.snapshot()
	assert.Equal(t, []string{"gate", "v-099", deleted, "last"}, values["k"])
	assert.Zero(t, l2.batches.Load())
}
