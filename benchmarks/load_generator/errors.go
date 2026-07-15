package main

import (
	"encoding/hex"
	"log/slog"
	"os"
	"sync/atomic"
)

// errorLog is the shared JSON-lines sink for every runtime error (timestamp, scenario, key/value, error text). slog
// handlers are concurrency-safe, so all workers share one instance.
type errorLog struct {
	logger *slog.Logger
	file   *os.File
	count  atomic.Int64
}

func newErrorLog(path string) (*errorLog, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &errorLog{logger: slog.New(slog.NewJSONHandler(f, nil)), file: f}, nil
}

func (e *errorLog) Close() error { return e.file.Close() }

// opError records a Get/Set error from the cache.
func (e *errorLog) opError(scenario, op, key string, valueLen int, err error) {
	e.count.Add(1)
	e.logger.Error("cache operation failed",
		"scenario", scenario, "op", op, "key", key, "value_len", valueLen, "error", err.Error())
}

// mismatch records a Get whose value didn't match the source of truth. Values can be tens of KiB, so it logs
// lengths and hex prefixes rather than the full payloads.
func (e *errorLog) mismatch(scenario, key string, got, want []byte) {
	e.count.Add(1)
	e.logger.Error("value mismatch",
		"scenario", scenario, "key", key,
		"got_len", len(got), "want_len", len(want),
		"got_prefix", hexPrefix(got), "want_prefix", hexPrefix(want))
}

// anomaly records a hit on a key the scenario never wrote - a sign of contamination in the shared L2.
func (e *errorLog) anomaly(scenario, key string, got []byte) {
	e.count.Add(1)
	e.logger.Error("hit on never-written key",
		"scenario", scenario, "key", key, "got_len", len(got), "got_prefix", hexPrefix(got))
}

func hexPrefix(b []byte) string {
	const n = 16
	if len(b) > n {
		b = b[:n]
	}
	return hex.EncodeToString(b)
}
