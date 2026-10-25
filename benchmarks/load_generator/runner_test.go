package load_generator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestWorkerStopsMidBatch: a worker that is behind runs up to a second of ops in one batch, at L2 latency tens of
// seconds of them, and shutdown must not wait for the batch to end.
func TestWorkerStopsMidBatch(t *testing.T) {
	s := newTestScenario(t, newLogSink(t), nil)
	ctx, cancel := context.WithCancel(context.Background())
	s.RPS, s.ReadPercent = []float64{1e6}, 0 // ten thousand Sets on the first tick
	value := s.Value
	s.Value = func(key string) []byte {
		cancel()
		return value(key)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	s.worker(ctx, &wg, 0)

	assert.Equal(t, int64(1), s.ops.Load())
}

// TestPacerCatchesUpOnDroppedTicks: a ticker hands a worker that overran one tick for all the ones it missed, and the
// ops those carried must still run. Counted per tick, a 50 ms stall cost 40 ops for good.
func TestPacerCatchesUpOnDroppedTicks(t *testing.T) {
	start := time.Now()
	p := pacer{rps: 1000, last: start}

	assert.Equal(t, 10, p.due(start.Add(10*time.Millisecond)))
	assert.Equal(t, 50, p.due(start.Add(60*time.Millisecond)), "four ticks dropped")
}

// TestPacerDropsPastTheBacklog: a worker catches up at most maxBacklog, so a backend that stalled for seconds does
// not get the whole stall back as one burst.
func TestPacerDropsPastTheBacklog(t *testing.T) {
	start := time.Now()
	p := pacer{rps: 1000, last: start}

	assert.Equal(t, 1000, p.due(start.Add(5*time.Second)), "one second of backlog")
	assert.Equal(t, 10, p.due(start.Add(5*time.Second+workerTick)), "and nothing older")
}

// TestPacerRunsBelowOneRPS: a split like 30 rps over 39 workers leaves each under one op a second, less than a
// second's backlog can hold, and such a worker must still run.
func TestPacerRunsBelowOneRPS(t *testing.T) {
	start := time.Now()
	p := pacer{rps: 0.5, last: start}

	assert.Equal(t, 0, p.due(start.Add(time.Second)))
	assert.Equal(t, 1, p.due(start.Add(2*time.Second)))
}

// TestPacerKeepsFractions: scenario 3 splits its rate into 769.23 rps per worker, 7.69 ops a tick. Rounding each tick
// down would run 700 a second.
func TestPacerKeepsFractions(t *testing.T) {
	start := time.Now()
	p := pacer{rps: 30_000.0 / 39, last: start}

	total := 0
	for i := range 100 {
		total += p.due(start.Add(time.Duration(i+1) * workerTick))
	}
	assert.Equal(t, 769, total)
}
