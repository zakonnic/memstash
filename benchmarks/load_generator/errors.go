package load_generator

import (
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"sync/atomic"
)

// errorLog is the shared JSON-lines sink for every runtime error (timestamp, scenario, key/value, error text). slog
// handlers are concurrency-safe, so all workers share one instance. Keys and values arrive as any: one log serves
// every scenario whatever its K and V.
type errorLog struct {
	logger *slog.Logger
	file   *os.File
	count  atomic.Int64
}

func newErrorLog(path string, con *console) (*errorLog, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	handler := fanout{
		slog.NewJSONHandler(f, nil),
		slog.NewTextHandler(con.writer(), errorOptions),
	}
	return &errorLog{logger: slog.New(handler), file: f}, nil
}

func (e *errorLog) Close() error { return e.file.Close() }

// opError records a Get/Set error from the cache; value is nil on the read side.
func (e *errorLog) opError(scenario, op string, key, value any, err error) {
	e.count.Add(1)
	args := []any{"scenario", scenario, "op", op, "key", keyText(key), "error", err.Error()}
	if value != nil {
		args = append(args, "value", valueDigest(value))
	}
	e.logger.Error("cache operation failed", args...)
}

// badValue records a Get whose value didn't match what the scenario's Value returned for the key.
func (e *errorLog) badValue(scenario string, key, got, want any) {
	e.count.Add(1)
	e.logger.Error("value mismatch",
		"scenario", scenario, "key", keyText(key),
		"got", valueDigest(got), "want", valueDigest(want))
}

// badKey records a hit on a key the scenario never wrote - a sign of contamination in the shared L2.
func (e *errorLog) badKey(scenario string, key, got any) {
	e.count.Add(1)
	e.logger.Error("hit on never-written key",
		"scenario", scenario, "key", keyText(key), "got", valueDigest(got))
}

func (e *errorLog) l2Error(scenario string, key any, err error) {
	e.count.Add(1)
	e.logger.Error("l2 error", "scenario", scenario, "key", keyText(key), "error", err.Error())
}

func (e *errorLog) cachePanic(scenario string, recovered any, handled bool) {
	e.count.Add(1)
	e.logger.Error("panic in cache", "scenario", scenario, "panic", fmt.Sprint(recovered), "handled", handled)
}

func (e *errorLog) recoverPanic(scenario, where string) {
	if r := recover(); r != nil {
		e.panicked(scenario, where, r)
	}
}

func (e *errorLog) panicked(scenario, where string, recovered any) {
	e.count.Add(1)
	e.logger.Error("panic recovered",
		"scenario", scenario, "where", where, "panic", fmt.Sprint(recovered), "stack", string(debug.Stack()))
}

// keyText renders a key of any type as one string, so the JSON log and its console copy show the same thing - slog
// would otherwise write a struct key as an object in one and a Go literal in the other.
func keyText(key any) string {
	if s, ok := key.(string); ok {
		return s
	}
	return fmt.Sprint(key)
}

// valueDigest describes a value without dumping it: values run to tens of KiB, so bytes and strings are logged as a
// length plus a short prefix, and anything else as its %v cut to the same order of size.
func valueDigest(value any) string {
	const prefix = 16
	switch v := value.(type) {
	case []byte:
		return fmt.Sprintf("len=%d hex=%s", len(v), hex.EncodeToString(v[:min(prefix, len(v))]))
	case string:
		return fmt.Sprintf("len=%d str=%q", len(v), v[:min(prefix, len(v))])
	default:
		text := fmt.Sprint(value)
		if len(text) > 4*prefix {
			text = text[:4*prefix] + "..."
		}
		return text
	}
}
