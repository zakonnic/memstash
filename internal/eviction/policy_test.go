package eviction

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash/internal/itemstore"
)

// TestAdmissionWeightIsExact follows S3-FIFO's small and W-TinyLFU's window through weighted inserts, overwrites
// that change the weight, promotions, evictions and removals: the queue's weight must stay what its items weigh.
func TestAdmissionWeightIsExact(t *testing.T) {
	for name, build := range map[string]func(*arena) (policy, *admission[string, string]){
		"s3fifo": func(a *arena) (policy, *admission[string, string]) {
			p := NewS3FIFO(&a.items, 200, 64)
			return p, &p.admission
		},
		"wtinylfu": func(a *arena) (policy, *admission[string, string]) {
			p := NewWTinyLFU(&a.items, 2000, 64)
			return p, &p.admission
		},
	} {
		t.Run(name, func(t *testing.T) {
			pool := newArena()
			pool.items.SetCostFunc(func(_, value string) uint32 { return uint32(len(value)) })
			p, small := build(pool)
			rng := rand.New(rand.NewPCG(1, 2))
			var live []string
			for step := range 5000 {
				switch op := rng.IntN(10); {
				case op < 4 && len(live) < 400:
					key := fmt.Sprintf("k%d", step)
					idx := pool.claim(key)
					pool.items.At(idx).Entry().Value = strings.Repeat("v", 1+rng.IntN(8))
					p.Add(itemstore.QNode{Idx: idx, Cost: pool.items.Cost(idx)})
					live = append(live, key)
				case op < 6 && len(live) > 0:
					idx, _ := pool.slotOf(live[rng.IntN(len(live))])
					before := int64(pool.items.Cost(idx))
					pool.items.At(idx).Entry().Value = strings.Repeat("v", 1+rng.IntN(8))
					p.(interface{ Reweigh(uint32, int64) }).Reweigh(idx, int64(pool.items.Cost(idx))-before)
				case op < 7 && len(live) > 0:
					pool.touch(live[rng.IntN(len(live))])
				case op < 8 && len(live) > 0:
					i := rng.IntN(len(live))
					idx, _ := pool.slotOf(live[i])
					pool.release(p, idx)
					live = append(live[:i], live[i+1:]...)
				default:
					if idx, ok := p.Evict(0); ok {
						key := pool.keyAt(idx)
						pool.release(p, idx)
						live = slices.DeleteFunc(live, func(k string) bool { return k == key })
					}
				}
				var want int64
				for idx, ok := pool.items.Oldest(0); ok; idx, ok = pool.items.Newer(idx) {
					want += int64(pool.items.Cost(idx))
				}
				require.Equal(t, want, small.weight, "step %d", step)
			}
		})
	}
}
