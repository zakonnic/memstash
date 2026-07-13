package tests

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/tests/workload"
)

// Value-integrity check for some realistic scenarios (copied from TestHitRateRealistic benchmark).

var realisticBlob = workload.NewBlob(99, workload.DefaultBlobSize)

var (
	realisticSessions = workload.SessionScenario{Catalog: 3_000_000, TraceLen: 2_000_000}
	realisticCDN      = workload.CDNScenario{Catalog: 1_000_000, TraceLen: 1_500_000}
	realisticDBRows   = workload.DBScenario{Rows: 2_000_000, ScanRows: 200_000, ChunkSize: 500_000, TraceLen: 2_000_000}
)

// TestRealisticTraceValues replays every scenario and verifies each hit byte-for-byte against the key's deterministic
// value. It also checks the weight invariant: the live weight never settles above the configured byte budget.
func TestRealisticTraceValues(t *testing.T) {
	if testing.Short() {
		t.Skip("long trace replay")
	}
	scenarios := []struct {
		name       string
		buildTrace func() []string
		value      func(key string) []byte
		budget     int64
	}{
		{name: "web-sessions", buildTrace: realisticSessions.Trace,
			value: func(key string) []byte { return realisticSessions.Value(realisticBlob, key) }, budget: 64 << 20},
		{name: "cdn-assets", buildTrace: realisticCDN.Trace,
			value: func(key string) []byte { return realisticCDN.Value(realisticBlob, key) }, budget: 256 << 20},
		{name: "db-rows", buildTrace: realisticDBRows.Trace,
			value: func(key string) []byte { return realisticDBRows.Value(realisticBlob, key) }, budget: 48 << 20},
	}

	for _, sc := range scenarios {
		trace := sc.buildTrace()
		for _, tc := range policies {
			t.Run(sc.name+"/"+tc.name, func(t *testing.T) {
				c, err := memstash.New[string, []byte](
					memstash.WithMemoryCapacity(sc.budget),
					memstash.WithPolicy(tc.policy),
					memstash.WithCostFunc(func(key string, value []byte) uint32 { return uint32(len(key) + len(value)) }),
				)
				require.NoError(t, err)
				defer c.Close()

				ctx := context.Background()
				hits := 0
				for i, key := range trace {
					got, ok := c.GetFromMemory(key)
					if !ok {
						require.NoError(t, c.Set(ctx, key, sc.value(key)))
						continue
					}
					hits++
					if want := sc.value(key); !bytes.Equal(got, want) {
						t.Fatalf("request %d: hit on key %q returned a wrong value (got %d bytes, want %d bytes)",
							i, key, len(got), len(want))
					}
				}

				// The traces are built so a working cache serves roughly half the requests from memory; a hit count
				// this low means the replay did not really exercise the eviction path.
				require.Greater(t, hits, len(trace)/4, "suspiciously few hits: the trace no longer stresses the cache")
				assert.LessOrEqual(t, c.Weight(), sc.budget, "live weight exceeds the byte budget")
			})
		}
	}
}
