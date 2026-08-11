package load_generator

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// logOneStats runs a single stats snapshot through a logger of its own and returns the record it wrote.
func logOneStats(t *testing.T, r *runner[string, []byte]) map[string]any {
	t.Helper()
	var buf bytes.Buffer
	r.logStats(slog.New(slog.NewJSONHandler(&buf, nil)), time.Now(), meter{t: time.Now()})

	var record map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &record))
	return record
}

// TestStatsReportsHeap: verification holds nothing of its own, so the snapshot carries the process heap as it is.
func TestStatsReportsHeap(t *testing.T) {
	s := newTestScenario(t, newLogSink(t), nil)

	record := logOneStats(t, s)

	assert.Positive(t, record["heap_alloc_bytes"])
}

// TestGetIsVerifiedAgainstValue: a hit is checked against what Value returns for the key, and a hit above
// WriteKeySpace is a value nobody wrote - the shared L2 leaking between scenarios looks exactly like that.
func TestGetIsVerifiedAgainstValue(t *testing.T) {
	sink := newLogSink(t)
	s := newTestScenario(t, sink, nil)
	ctx := context.Background()

	s.doSet(ctx, 1)
	s.doGet(ctx, 1) // what doSet wrote is what Value returns

	require.NoError(t, s.cache.Set(ctx, s.Key(2), []byte("not what Value returns")))
	s.doGet(ctx, 2)

	never := s.WriteKeySpace
	require.NoError(t, s.cache.Set(ctx, s.Key(never), SessionValue(s.Key(never))))
	s.doGet(ctx, never) // right value, wrong key space

	assert.Equal(t, []string{"value mismatch", "hit on never-written key"}, sink.messages())
	assert.Equal(t, int64(2), s.errs.Load())
}

// TestNoVerificationLeavesGetsUnchecked: the same two Gets that fail verification pass unremarked once it is off.
func TestNoVerificationLeavesGetsUnchecked(t *testing.T) {
	sink := newLogSink(t)
	s := newTestScenario(t, sink, nil)
	s.verify = false
	ctx := context.Background()

	require.NoError(t, s.cache.Set(ctx, s.Key(2), []byte("not what Value returns")))
	s.doGet(ctx, 2)

	never := s.WriteKeySpace
	require.NoError(t, s.cache.Set(ctx, s.Key(never), SessionValue(s.Key(never))))
	s.doGet(ctx, never)

	assert.Empty(t, sink.messages())
	assert.Zero(t, s.errs.Load())
}

// baseScenario is a scenario that validates, so a case only has to set what it is about.
func baseScenario() Scenario[string, []byte] {
	return Scenario[string, []byte]{
		Name: "scenario-test", CacheSize: 64, WriteKeySpace: 100,
		Goroutines: 1, RPS: []float64{1}, ZipfS: 1.01, Value: SessionValue,
	}
}

// TestServerTypeValidation: the server type is checked before anything is dialed, and only where it will be.
func TestServerTypeValidation(t *testing.T) {
	t.Run("defaults to rueidis", func(t *testing.T) {
		s := baseScenario()
		s.Address = []string{"127.0.0.1:6379"}
		s = s.withDefaults()
		assert.Equal(t, Rueidis, s.L2ClientType)
		assert.NoError(t, s.validate())
	})

	t.Run("every listed type is accepted", func(t *testing.T) {
		for _, server := range ServerTypes() {
			s := baseScenario()
			s.Address, s.L2ClientType = []string{"127.0.0.1:6379"}, server
			assert.NoError(t, s.withDefaults().validate(), "%s", server)
		}
	})

	t.Run("unknown type", func(t *testing.T) {
		s := baseScenario()
		s.Address, s.L2ClientType = []string{"127.0.0.1:6379"}, "postgres"
		assert.ErrorContains(t, s.withDefaults().validate(), `unknown L2ClientType "postgres"`)
	})

	t.Run("unknown type without an address", func(t *testing.T) {
		s := baseScenario()
		s.L2ClientType = "postgres"
		assert.NoError(t, s.withDefaults().validate(), "nothing is dialed, so nothing to reject")
	})

	t.Run("single-node client given a cluster", func(t *testing.T) {
		s := baseScenario()
		s.Address, s.L2ClientType = []string{"127.0.0.1:1", "127.0.0.1:2"}, Redigo
		assert.ErrorContains(t, s.withDefaults().validate(), "takes one address, got 2")
	})
}
