package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
)

// flatStruct is a pointer-free struct: its full size is visible to unsafe.Sizeof, so the budget estimator supports it.
type flatStruct struct {
	A, B int64
	Tag  [8]byte
}

// stringyStruct references a heap allocation through its string field, which the fast estimator cannot size.
type stringyStruct struct {
	Name string
}

func TestMemoryBudgetValidation(t *testing.T) {
	t.Run("budget together with capacity", func(t *testing.T) {
		_, err := memstash.New[string, string](
			memstash.WithMemoryBudget(1<<20),
			memstash.WithMemoryCapacity(100),
		)
		require.ErrorIs(t, err, memstash.ErrBudgetAndCapacity)
	})

	t.Run("negative budget", func(t *testing.T) {
		_, err := memstash.New[string, string](memstash.WithMemoryBudget(-1))
		require.ErrorIs(t, err, memstash.ErrBadBudget)
	})

	t.Run("complex types demand an explicit CostFunc", func(t *testing.T) {
		_, err := memstash.New[string, map[string]int](memstash.WithMemoryBudget(1 << 20))
		require.ErrorIs(t, err, memstash.ErrBudgetNeedsCostFunc, "map value")

		_, err = memstash.New[string, stringyStruct](memstash.WithMemoryBudget(1 << 20))
		require.ErrorIs(t, err, memstash.ErrBudgetNeedsCostFunc, "struct with a string field")

		_, err = memstash.New[string, [][]byte](memstash.WithMemoryBudget(1 << 20))
		require.ErrorIs(t, err, memstash.ErrBudgetNeedsCostFunc, "slice of slices")

		_, err = memstash.New[string, *stringyStruct](memstash.WithMemoryBudget(1 << 20))
		require.ErrorIs(t, err, memstash.ErrBudgetNeedsCostFunc, "pointer to a string-bearing struct")
	})

	t.Run("complex type with explicit CostFunc is accepted", func(t *testing.T) {
		c, err := memstash.New[string, map[string]int](
			memstash.WithMemoryBudget(1<<20),
			memstash.WithCostFunc(func(key string, value map[string]int) uint32 {
				return uint32(len(key) + len(value)*64)
			}),
		)
		require.NoError(t, err)
		c.Close()
	})

	t.Run("simple types are accepted", func(t *testing.T) {
		requireBuilds := func(err error, c interface{ Close() }) {
			t.Helper()
			require.NoError(t, err)
			c.Close()
		}
		c1, err := memstash.New[uint64, uint64](memstash.WithMemoryBudget(1 << 20))
		requireBuilds(err, c1)
		c2, err := memstash.New[string, []byte](memstash.WithMemoryBudget(1 << 20))
		requireBuilds(err, c2)
		c3, err := memstash.New[string, *flatStruct](memstash.WithMemoryBudget(1 << 20))
		requireBuilds(err, c3)
		c4, err := memstash.New[[16]byte, flatStruct](memstash.WithMemoryBudget(1 << 20))
		requireBuilds(err, c4)
	})
}

// TestMemoryBudgetFixedTypes pins down the arithmetic on fixed-size types: every uint64/uint64 item costs exactly its
// 16-byte Entry, so a single-shard cache must settle at exactly budget/cost items.
func TestMemoryBudgetFixedTypes(t *testing.T) {
	const budget = 64 << 10
	c, err := memstash.NewWithConfig(&memstash.Config[uint64, uint64]{
		MemoryBudget: budget,
		Shards:       1,
		Policy:       memstash.PolicyClock,
	})
	require.NoError(t, err)
	defer c.Close()

	ctx := context.Background()
	for i := uint64(0); i < 4096; i++ {
		require.NoError(t, c.Set(ctx, i, i))
	}

	require.LessOrEqual(t, c.Weight(), int64(budget), "weight above the byte budget")
	require.Positive(t, c.Len())
	perItem := c.Weight() / int64(c.Len())
	assert.Equal(t, int64(budget/perItem), int64(c.Len()), "a full single-shard cache must sit exactly at the budget")
	// Data bytes only: the estimate for uint64/uint64 is the 16-byte Entry, no cache bookkeeping on top.
	assert.EqualValues(t, 16, perItem, "per-item byte estimate for uint64/uint64 drifted")
}

// TestMemoryBudgetVariablePayload checks byte accounting for string/[]byte: the cache under a byte budget must hold
// many small values or few large ones, keeping the weight within the budget either way.
func TestMemoryBudgetVariablePayload(t *testing.T) {
	const budget = 1 << 20
	for _, tc := range policies {
		t.Run(tc.name, func(t *testing.T) {
			c, err := memstash.New[string, []byte](
				memstash.WithMemoryBudget(budget),
				memstash.WithPolicy(tc.policy),
			)
			require.NoError(t, err)
			defer c.Close()

			ctx := context.Background()
			value := make([]byte, 1024)
			for i := 0; i < 4000; i++ {
				require.NoError(t, c.Set(ctx, fmt.Sprintf("key:%06d", i), value))
			}

			weight := c.Weight()
			require.LessOrEqual(t, weight, int64(budget), "weight above the byte budget")
			require.Greater(t, weight, int64(budget/2), "cache did not fill up to the budget")
			// ~1KB payload plus the Entry headers and key bytes: the retained count must reflect byte, not item, accounting.
			assert.InDelta(t, budget/1100, c.Len(), float64(budget)/1100/4, "item count inconsistent with ~1.1KB per-item cost")
		})
	}
}

// TestMemoryBudgetSetAllocations guards the hot path: the derived cost function must not allocate, so an overwrite
// Set stays at its usual single allocation (the new Entry box).
func TestMemoryBudgetSetAllocations(t *testing.T) {
	c, err := memstash.New[string, []byte](memstash.WithMemoryBudget(1 << 20))
	require.NoError(t, err)
	defer c.Close()

	ctx := context.Background()
	value := []byte("payload-payload-payload")
	require.NoError(t, c.Set(ctx, "key", value))

	allocs := testing.AllocsPerRun(1000, func() {
		_ = c.Set(ctx, "key", value)
	})
	assert.LessOrEqual(t, allocs, 1.0, "budget cost estimation allocates on the Set path")
}
