package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
)

// panicRecord is one OnPanic call.
type panicRecord struct {
	recovered any
	handled   bool
}

// panicSink collects OnPanic calls from whichever goroutine recovered.
type panicSink struct {
	mu   sync.Mutex
	seen []panicRecord
}

func (s *panicSink) handler() memstash.PanicHandler {
	return func(recovered any, handled bool) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.seen = append(s.seen, panicRecord{recovered: recovered, handled: handled})
	}
}

func (s *panicSink) records() []panicRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]panicRecord(nil), s.seen...)
}

// waitFor polls until cond holds, so a background worker's panic has somewhere to land before the assert runs.
func waitFor(t *testing.T, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

// panicL2 is an L2 whose Set explodes, which is what drives the write-back worker into its recover.
type panicL2 struct {
	memstash.L2Cache[string, string]
}

func (panicL2) Get(context.Context, string) (string, bool, error) { return "", false, nil }

func (panicL2) BatchGet(context.Context, []string) (memstash.List[string, string], error) {
	return nil, nil
}

func (panicL2) Set(context.Context, string, string, time.Duration) error { panic("l2 set exploded") }

func (panicL2) BatchSet(context.Context, memstash.List[string, string], time.Duration) error {
	panic("l2 batch set exploded")
}

func (panicL2) Delete(context.Context, string) error        { return nil }
func (panicL2) BatchDelete(context.Context, []string) error { return nil }

// TestPanicHandlerWriteBackHandled: the write-back worker recovers the adapter's panic and passes it to OnL2Error, so
// OnPanic must hear about it as handled.
func TestPanicHandlerWriteBackHandled(t *testing.T) {
	ctx := context.Background()
	var sink panicSink
	var l2Errs int64
	var mu sync.Mutex

	c, err := memstash.New[string, string](
		memstash.WithMemoryCapacity(10),
		memstash.WithL2Cache[string, string](panicL2{}),
		memstash.WithWritePolicy(memstash.WriteBack),
		memstash.WithPanicRecover(sink.handler()),
		memstash.WithOnL2Error[string, string](func(string, error) {
			mu.Lock()
			l2Errs++
			mu.Unlock()
		}),
	)
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.Set(ctx, "k", "v"))
	require.True(t, waitFor(t, func() bool { return len(sink.records()) > 0 }), "OnPanic was never called")

	got := sink.records()[0]
	assert.Equal(t, "l2 set exploded", got.recovered)
	assert.True(t, got.handled, "the panic also went to OnL2Error, so it is handled")
}

// TestPanicHandlerWriteBackUnhandled: without an OnL2Error the same panic is swallowed and OnPanic is its only trace.
func TestPanicHandlerWriteBackUnhandled(t *testing.T) {
	ctx := context.Background()
	var sink panicSink

	c, err := memstash.New[string, string](
		memstash.WithMemoryCapacity(10),
		memstash.WithL2Cache[string, string](panicL2{}),
		memstash.WithWritePolicy(memstash.WriteBack),
		memstash.WithPanicRecover(sink.handler()),
	)
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.Set(ctx, "k", "v"))
	require.True(t, waitFor(t, func() bool { return len(sink.records()) > 0 }), "OnPanic was never called")

	got := sink.records()[0]
	assert.Equal(t, "l2 set exploded", got.recovered)
	assert.False(t, got.handled, "nothing else saw this panic, so it is unhandled")
}

// TestPanicHandlerBackgroundLoader: GetAndRefresh runs its loader on a goroutine nobody can recover on, and the panic
// reaches the waiters as ErrLoaderPanic - handled.
func TestPanicHandlerBackgroundLoader(t *testing.T) {
	ctx := context.Background()
	var sink panicSink

	c, err := memstash.New[string, string](
		memstash.WithMemoryCapacity(10),
		memstash.WithTTL(time.Millisecond),
		memstash.WithPanicRecover(sink.handler()),
	)
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.Set(ctx, "k", "v"))
	time.Sleep(5 * time.Millisecond) // let the value go stale so the refresh actually fires

	_, _ = c.GetAndRefresh(ctx, "k", func(context.Context, string) (string, error) {
		panic("refresh loader exploded")
	})
	require.True(t, waitFor(t, func() bool { return len(sink.records()) > 0 }), "OnPanic was never called")

	got := sink.records()[0]
	assert.Equal(t, "refresh loader exploded", got.recovered)
	assert.True(t, got.handled, "waiters get this panic as ErrLoaderPanic, so it is handled")
}

// TestPanicHandlerSurvivesPanickingHandler: an OnPanic that panics must not take the worker down with it - it runs
// behind its own recover.
func TestPanicHandlerSurvivesPanickingHandler(t *testing.T) {
	ctx := context.Background()
	var calls int64
	var mu sync.Mutex

	c, err := memstash.New[string, string](
		memstash.WithMemoryCapacity(10),
		memstash.WithL2Cache[string, string](panicL2{}),
		memstash.WithWritePolicy(memstash.WriteBack),
		memstash.WithPanicRecover(func(any, bool) {
			mu.Lock()
			calls++
			mu.Unlock()
			panic("handler exploded too")
		}),
	)
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.Set(ctx, "k", "v"))
	require.True(t, waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls > 0
	}), "OnPanic was never called")

	// The worker has to still be alive: a second write must reach it too.
	require.NoError(t, c.Set(ctx, "k2", "v2"))
	assert.True(t, waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls > 1
	}), "the write-back worker died with the panicking handler")
}

// TestPanicHandlerNotCalledForSyncLoader: a loader panicking under a synchronous GetOrLoad is not recovered at all -
// it reaches the caller, so OnPanic must stay quiet.
func TestPanicHandlerNotCalledForSyncLoader(t *testing.T) {
	ctx := context.Background()
	var sink panicSink

	c, err := memstash.New[string, string](
		memstash.WithMemoryCapacity(10),
		memstash.WithPanicRecover(sink.handler()),
	)
	require.NoError(t, err)
	defer c.Close()

	func() {
		defer func() { require.NotNil(t, recover(), "the loader's panic must still reach the caller") }()
		_, _ = c.GetOrLoad(ctx, "k", func(context.Context, string) (string, error) {
			panic("sync loader exploded")
		})
	}()

	assert.Empty(t, sink.records(), "a panic the caller receives must not also be reported to OnPanic")
}
