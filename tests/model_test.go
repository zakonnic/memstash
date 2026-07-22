package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
	"github.com/zakonnic/memstash/tests/workload"
)

// TestModelAgainstMap is a differential test of the first level's slot tables: a random stream of Set / overwrite /
// Delete / Get is mirrored into a plain map, and with the capacity above the keyspace (so eviction never runs) the
// cache must behave exactly like that map. The keyspace is small on purpose - probe chains stay dense, tombstones
// from deletes pile up inside chains, and the per-shard tables repeatedly grow from their minimal size and rebuild
// at the same size to purge tombstones. Any probe-chain break (a lost key, a duplicate slot, a value served for the
// wrong key) surfaces as a mismatch.
func TestModelAgainstMap(t *testing.T) {
	for _, tc := range policies {
		for _, shards := range []int{0, 1} { // default sharding and the single-shard deterministic layout
			t.Run(fmt.Sprintf("%s/shards=%d", tc.name, shards), func(t *testing.T) {
				ctx := context.Background()
				c, err := memstash.NewWithConfig(&memstash.Config[string, int]{
					MemoryCapacity: 10_000,
					Policy:         tc.policy,
					Shards:         shards,
				})
				require.NoError(t, err)
				defer c.Close()

				model := make(map[string]int)
				rng := workload.Random()
				const keyspace = 2000

				verifyKey := func(key string) {
					got, ok := c.GetFromMemory(key)
					want, exists := model[key]
					require.Equal(t, exists, ok, "presence mismatch for %q", key)
					if exists {
						require.Equal(t, want, got, "value mismatch for %q", key)
					}
				}
				verifyAll := func() {
					require.Equal(t, len(model), c.Len(), "Len must match the model")
					require.EqualValues(t, len(model), c.Weight(), "unweighted Weight must equal the item count")
					for i := 0; i < keyspace; i++ {
						verifyKey(fmt.Sprintf("key-%d", i))
					}
				}

				for op := 0; op < 200_000; op++ {
					key := fmt.Sprintf("key-%d", rng.Intn(keyspace))
					switch rng.Intn(10) {
					case 0, 1, 2: // delete (often - keeps tombstones churning)
						require.NoError(t, c.Delete(ctx, key))
						delete(model, key)
					case 3, 4, 5, 6: // set / overwrite
						value := rng.Int()
						require.NoError(t, c.Set(ctx, key, value))
						model[key] = value
					default: // read
					}
					verifyKey(key)
					if op%20_000 == 19_999 {
						verifyAll()
					}
				}
				verifyAll()
			})
		}
	}
}

// TestTombstoneChurnBounded drives the delete-heavy pattern that never triggers eviction: a fixed number of live
// keys is kept while fresh keys replace deleted ones, so every shard's table accumulates tombstones that only
// rebuilds can purge. The table growth must stay bounded by the live count, not by the number of operations.
func TestTombstoneChurnBounded(t *testing.T) {
	ctx := context.Background()
	for _, tc := range policies {
		t.Run(tc.name, func(t *testing.T) {
			c, err := memstash.NewWithConfig(&memstash.Config[int, int]{
				MemoryCapacity: 100_000,
				Policy:         tc.policy,
			})
			require.NoError(t, err)
			defer c.Close()

			const window = 1000 // live keys at any moment, far below capacity
			for i := 0; i < window; i++ {
				require.NoError(t, c.Set(ctx, i, i))
			}
			baseline := c.TotalWeight()
			for i := window; i < 60_000; i++ {
				require.NoError(t, c.Set(ctx, i, i))
				require.NoError(t, c.Delete(ctx, i-window))
			}

			require.Equal(t, window, c.Len(), "live count must stay at the window size")
			for i := 60_000 - window; i < 60_000; i++ {
				value, ok := c.GetFromMemory(i)
				require.True(t, ok, "live key %d lost", i)
				require.Equal(t, i, value)
			}
			// The structures may hold slack (pool freelist, queue chunks, tombstones between rebuilds), but must not
			// grow in proportion to the 60k operations.
			require.Less(t, c.TotalWeight(), baseline*8+1<<20,
				"first-level structures grew unboundedly under delete churn (baseline %d, now %d)", baseline, c.TotalWeight())
		})
	}
}

// TestEvictionTableConsistency overfills the cache twofold and checks the table, weight accounting and the eviction
// state stay mutually consistent: the live count matches the weight, never exceeds capacity, and every reported-live
// key still serves its exact value.
func TestEvictionTableConsistency(t *testing.T) {
	ctx := context.Background()
	for _, tc := range policies {
		t.Run(tc.name, func(t *testing.T) {
			const capacity = 4096
			c, err := memstash.NewWithConfig(&memstash.Config[int, int]{
				MemoryCapacity: capacity,
				Policy:         tc.policy,
			})
			require.NoError(t, err)
			defer c.Close()

			for i := 0; i < capacity*2; i++ {
				require.NoError(t, c.Set(ctx, i, i*3))
			}
			require.LessOrEqual(t, c.Weight(), int64(capacity), "weight must not exceed capacity")
			require.EqualValues(t, c.Len(), c.Weight(), "unweighted weight must equal the live count")

			hits := 0
			for i := 0; i < capacity*2; i++ {
				if value, ok := c.GetFromMemory(i); ok {
					require.Equal(t, i*3, value, "corrupted value for key %d", i)
					hits++
				}
			}
			require.Equal(t, c.Len(), hits, "every live slot must be reachable by its key")
		})
	}
}
