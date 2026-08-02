package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
)

var errL2Down = errors.New("l2 is down")

// failingL2 stands in for an L2 whose every call fails - a Redis that is down, or refusing the command.
type failingL2 struct{}

func (failingL2) Get(context.Context, string) ([]byte, bool, error) { return nil, false, errL2Down }

func (failingL2) BatchGet(context.Context, []string) (memstash.List[string, []byte], error) {
	return nil, errL2Down
}
func (failingL2) Set(context.Context, string, []byte, time.Duration) error { return errL2Down }

func (failingL2) BatchSet(context.Context, memstash.List[string, []byte], time.Duration) error {
	return errL2Down
}
func (failingL2) Delete(context.Context, string) error        { return errL2Down }
func (failingL2) BatchDelete(context.Context, []string) error { return errL2Down }

// panickingL2 is a failingL2 whose write path panics instead of returning an error.
type panickingL2 struct{ failingL2 }

func (panickingL2) BatchSet(context.Context, memstash.List[string, []byte], time.Duration) error {
	panic("l2 adapter exploded")
}
func (panickingL2) Set(context.Context, string, []byte, time.Duration) error {
	panic("l2 adapter exploded")
}

// logSink is an errorLog writing into a temp dir, plus the console copy it mirrors to.
type logSink struct {
	errLog  *errorLog
	path    string
	console *os.File
	t       *testing.T
}

func newLogSink(t *testing.T) *logSink {
	t.Helper()
	dir := t.TempDir()
	consoleFile, err := os.Create(filepath.Join(dir, "console.txt"))
	require.NoError(t, err)
	t.Cleanup(func() { consoleFile.Close() })

	path := filepath.Join(dir, "errors.log")
	errLog, err := newErrorLog(path, newConsole(consoleFile))
	require.NoError(t, err)
	t.Cleanup(func() { errLog.Close() })

	return &logSink{errLog: errLog, path: path, console: consoleFile, t: t}
}

// messages returns the "msg" of every record in errors.log, in order.
func (s *logSink) messages() []string {
	s.t.Helper()
	data, err := os.ReadFile(s.path)
	require.NoError(s.t, err)

	var msgs []string
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		var record map[string]any
		require.NoError(s.t, json.Unmarshal([]byte(line), &record), "errors.log must hold one JSON object per line")
		require.Equal(s.t, "ERROR", record["level"])
		msgs = append(msgs, record["msg"].(string))
	}
	return msgs
}

func (s *logSink) consoleText() string {
	s.t.Helper()
	data, err := os.ReadFile(s.console.Name())
	require.NoError(s.t, err)
	return string(data)
}

// TestErrorLogWritesEveryKind checks the plumbing itself: each kind of error reaches errors.log as a JSON line, is
// mirrored to the console, and is counted.
func TestErrorLogWritesEveryKind(t *testing.T) {
	sink := newLogSink(t)

	sink.errLog.opError("scenario-1", "get", "scenario-1:key-1", 0, errL2Down)
	sink.errLog.mismatch("scenario-1", "scenario-1:key-2", []byte("got"), []byte("want"))
	sink.errLog.anomaly("scenario-1", "scenario-1:key-3", []byte("got"))
	sink.errLog.l2Error("scenario-1", "scenario-1:key-4", errL2Down)
	sink.errLog.cachePanic("scenario-1", "boom", true)
	sink.errLog.panicked("scenario-1", "worker", "boom")

	assert.Equal(t, []string{
		"cache operation failed", "value mismatch", "hit on never-written key", "l2 error", "panic in cache",
		"panic recovered",
	}, sink.messages())
	assert.Equal(t, int64(6), sink.errLog.count.Load())

	console := sink.consoleText()
	assert.Equal(t, 6, strings.Count(console, "\n"), "every error must also reach the console, one line each")
	assert.Contains(t, console, "l2 is down")
}

// newTestScenario builds a scenario with the same handler wiring the real ones get; a nil l2 leaves it L1-only.
func newTestScenario(t *testing.T, sink *logSink, l2 memstash.L2Cache[string, []byte]) *scenario {
	t.Helper()
	s := &scenario{
		name:   "scenario-test",
		value:  sessionValue,
		truth:  xsync.NewMapOf[string, []byte](),
		errLog: sink.errLog,
	}
	opts := append(s.cacheOptions(), memstash.WithMemoryCapacity(64))
	if l2 != nil {
		opts = append(opts, memstash.WithL2Cache[string, []byte](l2))
	}
	cache, err := memstash.New[string, []byte](opts...)
	require.NoError(t, err)
	s.cache = cache
	return s
}

// TestFailingL2ReachesErrorLog is the end-to-end check that a broken L2 cannot pass unnoticed. The write-back path is
// the interesting one: Set returns nil there, so without OnL2Error the failure would have no trace at all.
func TestFailingL2ReachesErrorLog(t *testing.T) {
	sink := newLogSink(t)
	s := newTestScenario(t, sink, failingL2{})

	s.doGet(context.Background(), s.keyFor(1)) // L1 miss -> L2 read error, returned to the caller
	s.doSet(context.Background(), s.keyFor(2)) // write-back: the error surfaces only through OnL2Error
	s.cache.Close()                            // flushes the write-back buffer

	assert.Equal(t, []string{"cache operation failed", "l2 error"}, sink.messages())
	assert.Equal(t, int64(2), s.errs.Load(), "both errors must show up in the scenario's errors_total")
}

// TestPanickingL2ReachesErrorLog: a panic inside the adapter is recovered on the cache's own goroutine, so OnPanic is
// the only thing standing between it and silence.
func TestPanickingL2ReachesErrorLog(t *testing.T) {
	sink := newLogSink(t)
	s := newTestScenario(t, sink, panickingL2{})

	s.doSet(context.Background(), s.keyFor(1))
	s.cache.Close()

	assert.Contains(t, sink.messages(), "panic in cache")
	assert.Contains(t, sink.consoleText(), "l2 adapter exploded")
}

// TestRecoverPanicLogsTheStack: a panic in the generator's own goroutine is logged with its stack, and only that
// goroutine dies.
func TestRecoverPanicLogsTheStack(t *testing.T) {
	sink := newLogSink(t)

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer sink.errLog.recoverPanic("scenario-1", "worker")
		panic("worker exploded")
	}()
	<-done

	require.Equal(t, []string{"panic recovered"}, sink.messages())
	console := sink.consoleText()
	assert.Contains(t, console, "worker exploded")
	assert.Contains(t, console, "recoverPanic", "the stack of the goroutine that died must be in the record")
}
