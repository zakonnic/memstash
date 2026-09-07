//go:build probestats

// Probe-length instrumentation. It walks the real slot tables, so it lives in the package; behind a build tag
// because one run is tens of millions of operations.
package memstash

import (
	"context"
	"hash/maphash"
	"math/rand/v2"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash/internal/itemstore"
)

const (
	statsCapacity   = 100_000
	statsHotKeys    = 60_000 // the frequently accessed set
	statsHotShare   = 80     // percent of operations that go to the hot set
	statsDeleteRate = 30     // deletes per 100 insertions
	statsTurnovers  = 40     // insertions, in multiples of the capacity
	statsSample     = 200_000
	histMax         = 64
)

type tableStats struct {
	slots, live, dead, hasNext int
	shLive, shStale            int // live items, and the queue nodes beyond them
	probeSum                   int64
	probeMax                   int
	hotLive, coldLive          int
	hotSum, coldSum            int64
	hist                       []int
	keys                       []uint64
}

type accessStats struct {
	hits, misses      int
	hitSum, missSum   int64
	hitDead, missDead int64
	hitMax, missMax   int
	groupExits        int
	hitHist, missHist []int
}

// scanTables measures every live item distance from its home slot: the slots a hit for that key touches.
func scanTables(c *Cache[uint64, uint64], collect bool) tableStats {
	st := tableStats{hist: make([]int, histMax+1)}
	for i := range c.shards {
		sh := &c.shards[i]
		st.shLive, st.shStale = st.shLive+sh.live, st.shStale+sh.policy.Len()-sh.live
		storage := sh.items.GetStorage()
		slots := uint32(storage.Len())
		st.slots += int(slots)
		for pos := uint32(0); pos < slots; pos++ {
			item := storage.At(pos)
			meta := item.Metadata()
			if meta&itemstore.HasNext != 0 {
				st.hasNext++
			}
			switch {
			case itemstore.Empty(meta):
			case meta&itemstore.Dead != 0:
				st.dead++
			default:
				st.live++
				key := item.Entry().Key
				home := storage.Home(maphash.Comparable(c.seed, key))
				probes := int(storage.Dist(home, pos)) + 1
				st.probeSum += int64(probes)
				st.probeMax = max(st.probeMax, probes)
				st.hist[min(probes, histMax)]++
				if key < statsHotKeys { // survivors of the initial fill vs items the churn placed
					st.hotLive, st.hotSum = st.hotLive+1, st.hotSum+int64(probes)
				} else {
					st.coldLive, st.coldSum = st.coldLive+1, st.coldSum+int64(probes)
				}
				if collect {
					st.keys = append(st.keys, key)
				}
			}
		}
	}
	return st
}

// probeCost replays the getMemory slot walk: the slots it touches, how many of them are dead, and whether the
// group-edge overflow filter cut the walk short.
func probeCost(c *Cache[uint64, uint64], key uint64) (probes, dead int, hit, groupExit bool) {
	keyHash := maphash.Comparable(c.seed, key)
	sh := &c.shards[uint32(keyHash)&c.shardMask]
	storage := sh.items.GetStorage()
	tagged := itemstore.TagOf(keyHash)
	home := storage.Home(keyHash)
	groupEnd := (home | 7) + 1
	for pos := home; probes <= storage.Len(); pos++ {
		if pos == groupEnd && !storage.OverflowedTag(home, itemstore.OverflowBucket(keyHash)) {
			return probes, dead, false, true
		}
		item := storage.At(pos)
		meta := item.Metadata()
		probes++
		if meta&itemstore.Dead != 0 {
			dead++
		}
		if meta&itemstore.DeadOrTag == tagged && item.Entry().Key == key {
			return probes, dead, true, false
		}
		if meta&itemstore.HasNext == 0 {
			return probes, dead, false, false
		}
	}
	return probes, dead, false, false
}

func newAccessStats() accessStats {
	return accessStats{hitHist: make([]int, histMax+1), missHist: make([]int, histMax+1)}
}

func (as *accessStats) add(probes, dead int, hit, groupExit bool) {
	if groupExit {
		as.groupExits++
	}
	if hit {
		as.hits++
		as.hitSum += int64(probes)
		as.hitDead += int64(dead)
		as.hitMax = max(as.hitMax, probes)
		as.hitHist[min(probes, histMax)]++
		return
	}
	as.misses++
	as.missSum += int64(probes)
	as.missDead += int64(dead)
	as.missMax = max(as.missMax, probes)
	as.missHist[min(probes, histMax)]++
}

func percentile(hist []int, total int, p float64) int {
	want, seen := int(float64(total)*p), 0
	for i, n := range hist {
		seen += n
		if seen >= want {
			return i
		}
	}
	return len(hist) - 1
}

func mean(sum int64, n int) float64 {
	if n == 0 {
		return 0
	}
	return float64(sum) / float64(n)
}

// replay measures what the workload actually pays on the frozen table: hits walk to their key, misses walk to a
// terminator. Cold keys are fresh, so they are true misses.
func replay(c *Cache[uint64, uint64], rng *rand.Rand, cold *uint64) accessStats {
	as := newAccessStats()
	for range statsSample {
		var key uint64
		if rng.IntN(100) < statsHotShare {
			key = uint64(rng.IntN(statsHotKeys))
		} else {
			key = *cold
			*cold++
		}
		as.add(probeCost(c, key))
	}
	return as
}

func TestProbeChainLengthUnderChurn(t *testing.T) {
	t.Logf("GOMAXPROCS=%d capacity=%d hot=%d keys (%d%% of ops) deletes=%d%% of inserts target=%d insertions",
		runtime.GOMAXPROCS(0), statsCapacity, statsHotKeys, statsHotShare, statsDeleteRate,
		statsTurnovers*statsCapacity)
	for _, cfg := range []struct {
		name string
		opts []Option
	}{
		{"default-shards", nil},
		{"1-shard", []Option{WithShardsCount(1)}},
		{"prealloc", []Option{WithPreallocatedSize()}},
		{"prealloc-0.5", []Option{WithPreallocationRatio(0.5)}},
	} {
		t.Run(cfg.name, func(t *testing.T) { runChurn(t, cfg.opts) })
	}
}

func runChurn(t *testing.T, opts []Option) {
	ctx := context.Background()
	var evicted, invalidated int
	base := []Option{WithMemoryCapacity(statsCapacity), WithStats(), WithOnDeletion(
		func(_, _ uint64, cause DeletionCause) {
			if cause == CauseEviction {
				evicted++
				return
			}
			invalidated++
		})}
	c, err := New[uint64, uint64](append(base, opts...)...)
	require.NoError(t, err)
	defer c.Close()

	rng := rand.New(rand.NewPCG(1, 2))
	cold := uint64(1) << 32 // disjoint from the hot range
	hotOps, inserts, deletes, ops := 0, 0, 0, 0
	milestones := []int{1, 2, 5, 10, 20, 30, statsTurnovers}
	t.Log("turn |    ops |    live |   slots | load |   dead  dead% | hit(all) p50 p99 max | grow purge")
	for next := 0; next < len(milestones); {
		ops++
		var key uint64
		if rng.IntN(100) < statsHotShare {
			key = uint64(rng.IntN(statsHotKeys))
			hotOps++
		} else {
			key = cold
			cold++
		}
		if _, ok := c.GetFromMemory(key); !ok {
			require.NoError(t, c.Set(ctx, key, key))
			inserts++
			if rng.IntN(100) < statsDeleteRate { // deletes hit the resident set, so runs compact
				require.NoError(t, c.Delete(ctx, uint64(rng.IntN(statsHotKeys))))
				deletes++
			}
		}
		if inserts >= milestones[next]*statsCapacity {
			last := next == len(milestones)-1
			st := scanTables(c, last)
			as := replay(c, rng, &cold)
			s := c.Stats()
			t.Logf("%4d | %5.1fM | %7d | %7d | %.2f | %6d %5.1f%% | %8.2f %3d %3d %3d | %4d %5d",
				milestones[next], float64(ops)/1e6, st.live, st.slots,
				float64(st.live+st.dead)/float64(st.slots), st.dead, 100*float64(st.dead)/float64(st.slots),
				mean(st.probeSum, st.live),
				percentile(st.hist, st.live, 0.5), percentile(st.hist, st.live, 0.99), st.probeMax,
				s.TableGrowths(), s.TablePurges())
			t.Logf("     | structural: hot=%.2f (%d items) cold=%.2f (%d items)",
				mean(st.hotSum, st.hotLive), st.hotLive, mean(st.coldSum, st.coldLive), st.coldLive)
			t.Logf("     | walk: hit(hot)=%.2f miss=%.2f | dead per walk: hit=%.2f miss=%.2f | grpExit=%.0f%% "+
				"| hitRate=%.1f%%",
				mean(as.hitSum, as.hits), mean(as.missSum, as.misses),
				mean(as.hitDead, as.hits), mean(as.missDead, as.misses),
				100*float64(as.groupExits)/float64(max(as.misses, 1)),
				100*float64(as.hits)/float64(as.hits+as.misses))
			if last {
				t.Logf("  ops=%d inserts=%d deletes=%d (%.0f%% of inserts) hot mean accesses=%.0f",
					ops, inserts, deletes, 100*float64(deletes)/float64(inserts), float64(hotOps)/statsHotKeys)
				t.Logf("  evictions=%d other removals=%d", evicted, invalidated)
				t.Logf("  shard counters: live=%d stale nodes=%d | scanned dead=%d", st.shLive, st.shStale, st.dead)
				require.Zero(t, st.dead, "a deletion reclaims its slot on the spot: no dead slot may survive")
				require.Zero(t, s.TablePurges(), "nothing is ever purged")
				t.Logf("  hasNext set on %d slots (%.1f%%)", st.hasNext, 100*float64(st.hasNext)/float64(st.slots))
				t.Logf("  miss walk: p50=%d p99=%d max=%d | hit walk: p99=%d max=%d",
					percentile(as.missHist, as.misses, 0.5), percentile(as.missHist, as.misses, 0.99), as.missMax,
					percentile(as.hitHist, as.hits, 0.99), as.hitMax)
				reportFreshControl(t, st, opts)
			}
			next++
		}
	}
}

// reportFreshControl fills a second cache with the churned cache live keys and no eviction or delete pressure, so
// the churn contribution to the walk is the difference between the two.
func reportFreshControl(t *testing.T, churned tableStats, opts []Option) {
	ctx := context.Background()
	c, err := New[uint64, uint64](append([]Option{WithMemoryCapacity(statsCapacity), WithStats()}, opts...)...)
	require.NoError(t, err)
	defer c.Close()
	for _, k := range churned.keys {
		require.NoError(t, c.Set(ctx, k, k))
	}
	st := scanTables(c, false)
	t.Logf("  fresh table, same %d keys: live=%d slots=%d load=%.2f dead=%d hit(all)=%.2f p99=%d max=%d (churned %.2f)",
		len(churned.keys), st.live, st.slots, float64(st.live+st.dead)/float64(st.slots), st.dead,
		mean(st.probeSum, st.live), percentile(st.hist, st.live, 0.99), st.probeMax,
		mean(churned.probeSum, churned.live))
}

// TestPreallocatedGrowthAt100M answers whether a preallocated cache keeps the table it was given. It fills the
// 100M-entry configuration and then churns fresh keys through it, reporting the slot count as it goes.
// Around 4 GiB of live heap and a few minutes of Sets, which is what the build tag is for.
func TestPreallocatedGrowthAt100M(t *testing.T) {
	const capacity = 100_000_000
	const churn = 2 * capacity

	ctx := context.Background()
	c, err := New[uint64, uint64](WithMemoryCapacity(capacity), WithPreallocatedSize(), WithStats())
	require.NoError(t, err)
	defer c.Close()

	start := time.Now()
	report := func(stage string, inserts int64) {
		slots, live, dirty := 0, 0, 0
		for i := range c.shards {
			slots += c.shards[i].items.GetStorage().Len()
			live += c.shards[i].live
		}
		s := c.Stats()
		t.Logf("%-8s %4.0fM inserts | live %5.1fM | dirty %5.2fM (%4.1f%% of live) | slots %d (%d/shard) "+
			"| load %.2f | growths %d purges %d | %s",
			stage, float64(inserts)/1e6, float64(live)/1e6, float64(dirty)/1e6, 100*float64(dirty)/float64(live),
			slots, slots/len(c.shards), float64(live+dirty)/float64(slots),
			s.TableGrowths(), s.TablePurges(), time.Since(start).Round(time.Second))
	}

	key := func(i int64) uint64 { return uint64(i) * 0x9E3779B97F4A7C15 }
	t.Logf("%d shards x %d slots, shard cap %d, load at a full shard %.3f", len(c.shards),
		c.shards[0].items.GetStorage().Len(), c.shards[0].cap,
		float64(c.shards[0].cap+1)/float64(c.shards[0].items.GetStorage().Len()))

	for i := int64(0); i < capacity; i++ {
		if err := c.Set(ctx, key(i), 1); err != nil {
			t.Fatal(err)
		}
	}
	report("filled", capacity)

	for i := int64(capacity); i < capacity+churn; i++ {
		if err := c.Set(ctx, key(i), 1); err != nil {
			t.Fatal(err)
		}
		if (i-capacity+1)%10_000_000 == 0 {
			report("churn", i-capacity+1)
		}
	}
}

// TestPreallocatedFillTime100M splits the two costs of getting a 100M-entry cache warm: allocating the preallocated
// tables, and the fill itself, which runs from GOMAXPROCS goroutines the way a real warm-up would.
func TestPreallocatedFillTime100M(t *testing.T) {
	const capacity = 100_000_000
	ctx := context.Background()

	alloc := time.Now()
	c, err := New[uint64, uint64](WithMemoryCapacity(capacity), WithPreallocatedSize(), WithStats())
	require.NoError(t, err)
	defer c.Close()
	allocated := time.Since(alloc)

	slots := 0
	for i := range c.shards {
		slots += c.shards[i].items.GetStorage().Len()
	}
	t.Logf("New: %s for %d slots (%.2f GiB of tables)", allocated.Round(time.Millisecond), slots,
		float64(c.TotalSize())/(1<<30))

	workers := runtime.GOMAXPROCS(0)
	fill := time.Now()
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func(start int64) { // interleaved, so the workers spread over the shards instead of marching through one
			defer wg.Done()
			for i := start; i < capacity; i += int64(workers) {
				if err := c.Set(ctx, uint64(i)*0x9E3779B97F4A7C15, 1); err != nil {
					t.Errorf("set: %v", err)
					return
				}
			}
		}(int64(w))
	}
	wg.Wait()
	filled := time.Since(fill)
	s := c.Stats()
	t.Logf("fill: %s for %d items on %d workers (%.0f ns/set wall, %.1fM sets/s), live %d, growths %d purges %d",
		filled.Round(time.Millisecond), capacity, workers, float64(filled.Nanoseconds())/capacity,
		capacity/filled.Seconds()/1e6, c.Len(), s.TableGrowths(), s.TablePurges())
	t.Logf("total to a warm cache: %s", (allocated + filled).Round(time.Millisecond))
}
