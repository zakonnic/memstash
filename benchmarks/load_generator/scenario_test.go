package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// logOneStats runs a single stats snapshot through a logger of its own and returns the record it wrote.
func logOneStats(t *testing.T, s *scenario) map[string]any {
	t.Helper()
	var buf bytes.Buffer
	s.logStats(slog.New(slog.NewJSONHandler(&buf, nil)), time.Now(), meter{t: time.Now()})

	var record map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &record))
	return record
}

// TestStatsHeapIsNetOfTruthMap: the reported heap is what the caches cost, with the verification tax subtracted out.
func TestStatsHeapIsNetOfTruthMap(t *testing.T) {
	s := newTestScenario(t, newLogSink(t), nil)

	record := logOneStats(t, s)
	assert.Positive(t, record["heap_alloc_bytes"], "with no tax to subtract the whole heap is reported")

	const tax = 1 << 62 // more than the process can possibly hold: the subtraction must clamp, not go negative
	s.truthHeap = tax
	record = logOneStats(t, s)
	assert.Equal(t, float64(0), record["heap_alloc_bytes"])
}

// TestMeasureTruthHeapSharesOneBaseline: the maps live in one process heap, so every scenario reports the same tax.
func TestMeasureTruthHeapSharesOneBaseline(t *testing.T) {
	scenarios := []*scenario{{name: "scenario-1"}, {name: "scenario-2"}}

	truthHeap := measureTruthHeap(scenarios)

	assert.Positive(t, truthHeap)
	for _, s := range scenarios {
		assert.Equal(t, truthHeap, s.truthHeap)
	}
}
