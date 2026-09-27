package memstash

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIteratorRunWrapsDuringWalk: the run that wraps past the table's end grows after the walk has started, so items
// land wrapped beyond where that run ended at the start, and a deletion slides one of them back across the end, into
// slots the walk has not reached - at two points of the walk. Every key must be yielded once.
func TestIteratorRunWrapsDuringWalk(t *testing.T) {
	for _, deleteOn := range []string{"w2", "d"} {
		t.Run("delete on "+deleteOn, func(t *testing.T) {
			ctx := context.Background()
			c, err := New[uint64, uint64](WithMemoryCapacity(47), WithShardsCount(1), WithPreallocatedSize(),
				WithPolicy(PolicyClock))
			require.NoError(t, err)
			defer c.Close()
			storage := c.shards[0].items.GetStorage()
			last := uint32(storage.Len()) - 1
			next := uint64(1)
			homedAt := func(home uint32) uint64 {
				for ; storage.Home(c.keyHash(next)) != home; next++ {
				}
				next++
				return next - 1
			}
			a, b, cKey, d := homedAt(last), homedAt(1), homedAt(2), homedAt(3)
			w1, w2 := homedAt(last), homedAt(last)
			trigger := map[string]uint64{"w2": w2, "d": d}[deleteOn]
			for _, key := range []uint64{a, b, cKey, d} { // slot 0 stays empty: no run wraps when the walk starts
				require.NoError(t, c.Set(ctx, key, key))
			}

			yielded := map[uint64]int{}
			for key := range c.Iterator() {
				yielded[key]++
				switch key {
				case b:
					require.NoError(t, c.Set(ctx, w1, w1)) // wraps into slot 0, behind the walk
					require.NoError(t, c.Set(ctx, w2, w2)) // wraps on into slot 4, ahead of it
				case trigger:
					require.NoError(t, c.Delete(ctx, a)) // the compaction slides w2 back into the last slot
				}
			}
			for key, n := range yielded {
				require.Equal(t, 1, n, "key %d yielded %d times", key, n)
			}
			require.Equal(t, 1, yielded[w1])
			require.Equal(t, 1, yielded[w2])
		})
	}
}
