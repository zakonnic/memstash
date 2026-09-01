package load_generator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStatsReportsProbeTimings: the three micro-benchmarks ride on every stats line, and none of them may come out at
// zero - that is what a run too short for the clock to resolve looks like.
func TestStatsReportsProbeTimings(t *testing.T) {
	s := newTestScenario(t, newLogSink(t), nil)

	record := logOneStats(t, s)

	for _, field := range []string{"get_mem_ns", "get_ns", "set_ns"} {
		value, ok := record[field]
		require.True(t, ok, "%s missing from the stats line", field)
		ns, ok := value.(float64)
		require.True(t, ok, "%s is %T, want a number", field, value)
		assert.Positive(t, ns, "%s came out at zero: the timed run fit inside one clock tick", field)
	}
}

// TestProbeSplitsReadsBetweenHitAndMiss: half of each read set has to target a key the workload writes and half one
// nothing ever writes, otherwise the Get figures would only ever measure one side of the path.
func TestProbeSplitsReadsBetweenHitAndMiss(t *testing.T) {
	s := newTestScenario(t, newLogSink(t), nil)
	written := writtenKeys(s)

	p := newProbe(s.Scenario)

	for name, keys := range map[string][]string{"memKeys": p.memKeys, "getKeys": p.getKeys} {
		var hits, misses int
		for i, key := range keys {
			if i&1 == 1 {
				require.False(t, written[key], "%s[%d]=%q is a key the workload writes", name, i, key)
				misses++
				continue
			}
			require.True(t, written[key], "%s[%d]=%q is a key the workload never writes", name, i, key)
			hits++
		}
		assert.Equal(t, probeKeys/2, hits, name)
		assert.Equal(t, probeKeys/2, misses, name)
	}
}

// TestProbeReadSetsShareNoKeys: whichever read runs first pays for pulling its slots into CPU cache, so a shared key
// would hand the other measurement a warm table and make the two figures incomparable.
func TestProbeReadSetsShareNoKeys(t *testing.T) {
	s := newTestScenario(t, newLogSink(t), nil)

	p := newProbe(s.Scenario)

	mem := make(map[string]bool, probeKeys/2)
	for i := 1; i < len(p.memKeys); i += 2 {
		mem[p.memKeys[i]] = true
	}
	for i := 1; i < len(p.getKeys); i += 2 {
		require.False(t, mem[p.getKeys[i]], "miss key %q is used by both reads", p.getKeys[i])
	}
}

// TestProbeMissKeysAreAllDistinct: a probe repeating one miss key would walk the same slots over and over and report
// the cost of a table that fits in CPU cache, not of the shard the scenario has actually been filling.
func TestProbeMissKeysAreAllDistinct(t *testing.T) {
	s := newTestScenario(t, newLogSink(t), nil)

	p := newProbe(s.Scenario)

	seen := make(map[string]bool, probeKeys/2)
	for i := 1; i < len(p.memKeys); i += 2 {
		require.False(t, seen[p.memKeys[i]], "miss key %q repeats", p.memKeys[i])
		seen[p.memKeys[i]] = true
	}
	assert.Len(t, seen, probeKeys/2)
}

// TestProbeSetsFollowTheWorkloadKeys keeps the timed Sets inside the write space: a Set outside it would introduce
// keys the workload never writes, and a later Get would report them as values from somewhere else.
func TestProbeSetsFollowTheWorkloadKeys(t *testing.T) {
	s := newTestScenario(t, newLogSink(t), nil)
	written := writtenKeys(s)

	p := newProbe(s.Scenario)

	for i, key := range p.setKeys {
		require.True(t, written[key], "setKeys[%d]=%q is outside WriteKeySpace", i, key)
	}
}

func writtenKeys(s *runner[string, []byte]) map[string]bool {
	written := make(map[string]bool, s.WriteKeySpace)
	for n := range s.WriteKeySpace {
		written[s.Key(n)] = true
	}
	return written
}
