//go:build probestats

// Overflow-filter accounting: replays the read probe with and without the group-edge check on the same table, so the
// saving is the difference on one key stream. Temporary measurement harness.
package memstash

import (
	"context"
	"hash/maphash"
	"math/rand/v2"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash/internal/itemstore"
)

const itemBytes = int(unsafe.Sizeof(itemstore.Item[uint64, uint64]{}))

// lines counts the 64-byte lines a run of consecutive slots touches.
func lines(pos uint32, count int) int {
	if count <= 0 {
		return 0
	}
	first := int(pos) * itemBytes / 64
	last := (int(pos)+count)*itemBytes - 1
	return last/64 - first + 1
}

type walk struct {
	on, off                       int // slots touched with and without the filter
	onLines, offLines             int
	hit, reached, fired, firedAny bool // firedAny: what a plain per-group flag (no tag split) would have cut
}

// walkCost walks the probe chain on HasNext alone, then asks what the group-edge filter would have done to it.
func walkCost(c *Cache[uint64, uint64], key uint64) walk {
	keyHash := maphash.Comparable(c.seed, key)
	sh := &c.shards[uint32(keyHash)&c.shardMask]
	storage := sh.items.GetStorage()
	tagged := itemstore.TagOf(keyHash)
	home := storage.Home(keyHash)
	gLen := int((home|7)+1) - int(home)

	var w walk
	for pos := home; w.off <= storage.Len(); pos++ {
		item := storage.At(pos)
		meta := item.Metadata()
		w.off++
		if meta&itemstore.DeadOrTag == tagged && item.Entry().Key == key {
			w.hit = true
			break
		}
		if meta&itemstore.HasNext == 0 {
			break
		}
	}
	w.on, w.reached = w.off, w.off > gLen
	if w.reached {
		w.firedAny = !anyOverflowed(storage, home)
		if !storage.OverflowedTag(home, itemstore.OverflowBucket(keyHash)) {
			w.fired, w.on = true, gLen
		}
	}
	w.offLines = lines(home, w.off)
	w.onLines = lines(home, w.on)
	if w.reached {
		w.onLines++ // the overflow word lives in its own array
	}
	return w
}

// anyOverflowed is the group-wide question the counters would answer without the tag split.
func anyOverflowed(storage *itemstore.FlatHashMap[uint64, uint64], home uint32) bool {
	for bucket := uint32(0); bucket < 8; bucket++ {
		if storage.OverflowedTag(home, bucket) {
			return true
		}
	}
	return false
}

type walkStats struct {
	n, reached, fired, firedAny int
	onSum, offSum               int64
	onLineSum, offLineSum       int64
	onHist, offHist             []int
	onMax, offMax               int
}

func newWalkStats() *walkStats {
	return &walkStats{onHist: make([]int, histMax+1), offHist: make([]int, histMax+1)}
}

func (s *walkStats) add(w walk) {
	s.n++
	s.onSum, s.offSum = s.onSum+int64(w.on), s.offSum+int64(w.off)
	s.onLineSum, s.offLineSum = s.onLineSum+int64(w.onLines), s.offLineSum+int64(w.offLines)
	s.onHist[min(w.on, histMax)]++
	s.offHist[min(w.off, histMax)]++
	s.onMax, s.offMax = max(s.onMax, w.on), max(s.offMax, w.off)
	if w.reached {
		s.reached++
	}
	if w.fired {
		s.fired++
	}
	if w.firedAny {
		s.firedAny++
	}
}

func (s *walkStats) log(t *testing.T, label string) {
	if s.n == 0 {
		return
	}
	t.Logf("  %-5s n=%d | slots on=%.2f off=%.2f (saved %.2f, -%.0f%%) | p50 %d/%d p99 %d/%d max %d/%d",
		label, s.n, mean(s.onSum, s.n), mean(s.offSum, s.n),
		mean(s.offSum-s.onSum, s.n), 100*float64(s.offSum-s.onSum)/float64(s.offSum),
		percentile(s.onHist, s.n, 0.5), percentile(s.offHist, s.n, 0.5),
		percentile(s.onHist, s.n, 0.99), percentile(s.offHist, s.n, 0.99), s.onMax, s.offMax)
	t.Logf("  %-5s lines on=%.2f off=%.2f (saved %.2f) | reached edge %.1f%% | filter cut %.1f%% of all, "+
		"%.1f%% of those that reached",
		label, mean(s.onLineSum, s.n), mean(s.offLineSum, s.n), mean(s.offLineSum-s.onLineSum, s.n),
		100*float64(s.reached)/float64(s.n), 100*float64(s.fired)/float64(s.n),
		100*float64(s.fired)/float64(max(s.reached, 1)))
	t.Logf("  %-5s tag split: cuts %.1f%% of the walks that reached the edge; a plain per-group flag would cut "+
		"%.1f%%", label, 100*float64(s.fired)/float64(max(s.reached, 1)),
		100*float64(s.firedAny)/float64(max(s.reached, 1)))
}

// displacedShare counts the live items sitting outside their home group: exactly the ones NoteDisplaced pays a
// load/store for, on every insert and every reclaim move.
func displacedShare(c *Cache[uint64, uint64]) (live, displaced int) {
	for i := range c.shards {
		storage := c.shards[i].items.GetStorage()
		for pos := uint32(0); pos < uint32(storage.Len()); pos++ {
			item := storage.At(pos)
			meta := item.Metadata()
			if itemstore.Empty(meta) || meta&itemstore.Dead != 0 {
				continue
			}
			live++
			home := storage.Home(maphash.Comparable(c.seed, item.Entry().Key))
			if home>>3 != pos>>3 {
				displaced++
			}
		}
	}
	return live, displaced
}

func TestOverflowFilterValue(t *testing.T) {
	for _, cfg := range []struct {
		name string
		opts []Option
	}{
		{"default-growth", nil},
		{"prealloc", []Option{WithPreallocatedSize()}},
		{"prealloc-0.5", []Option{WithPreallocationRatio(0.5)}},
		{"prealloc-0.85", []Option{WithPreallocationRatio(0.85)}},
	} {
		t.Run(cfg.name, func(t *testing.T) { measureOverflow(t, cfg.opts) })
	}
}

func measureOverflow(t *testing.T, opts []Option) {
	ctx := context.Background()
	c, err := New[uint64, uint64](append([]Option{WithMemoryCapacity(statsCapacity), WithStats()}, opts...)...)
	require.NoError(t, err)
	defer c.Close()

	rng := rand.New(rand.NewPCG(1, 2))
	cold := uint64(1) << 32
	inserts := 0
	for inserts < 10*statsCapacity {
		var key uint64
		if rng.IntN(100) < statsHotShare {
			key = uint64(rng.IntN(statsHotKeys))
		} else {
			key = cold
			cold++
		}
		if _, ok := c.GetFromMemory(key); !ok {
			require.NoError(t, c.Set(ctx, key, key))
			inserts++
			if rng.IntN(100) < statsDeleteRate {
				require.NoError(t, c.Delete(ctx, uint64(rng.IntN(statsHotKeys))))
			}
		}
	}

	st := scanTables(c, false)
	t.Logf("live=%d slots=%d load=%.2f dead=%d hasNext=%.1f%% | item=%dB (%.1f slots/line)",
		st.live, st.slots, float64(st.live+st.dead)/float64(st.slots), st.dead,
		100*float64(st.hasNext)/float64(st.slots), itemBytes, 64.0/float64(itemBytes))

	live, displaced := displacedShare(c)
	t.Logf("displaced past their home group: %d of %d live items (%.1f%%) - the share of inserts that pay a counter "+
		"write", displaced, live, 100*float64(displaced)/float64(live))

	hits, misses, all := newWalkStats(), newWalkStats(), newWalkStats()
	for range statsSample {
		var key uint64
		if rng.IntN(100) < statsHotShare {
			key = uint64(rng.IntN(statsHotKeys))
		} else {
			key = cold
			cold++
		}
		w := walkCost(c, key)
		require.False(t, w.hit && w.fired, "the filter cut a walk that had already found its key")
		all.add(w)
		if w.hit {
			hits.add(w)
		} else {
			misses.add(w)
		}
	}
	all.log(t, "all")
	hits.log(t, "hit")
	misses.log(t, "miss")
}

// TestABLoadFactors reports the load the A/B bench configurations actually reach: the filter's value is a function of
// it, so a capacity that lands the table halfway up the growth ladder is a different experiment.
func TestABLoadFactors(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		capacity int64
		warm     uint64
		ratio    float64
	}{
		{1 << 17, 1 << 17 * 8, 0},
		{1 << 17, 1 << 17 * 3 / 4, 0},
		{100_000, 800_000, 0},
		{1 << 17, 1 << 17 * 8, 0.85},
		{1 << 17, 1 << 17 * 8, 0.75},
	} {
		opts := []Option{WithMemoryCapacity(tc.capacity)}
		if tc.ratio != 0 {
			opts = append(opts, WithPreallocationRatio(tc.ratio))
		}
		c, err := New[uint64, uint64](opts...)
		require.NoError(t, err)
		for k := uint64(0); k < tc.warm; k++ {
			require.NoError(t, c.Set(ctx, k, k))
		}
		st := scanTables(c, false)
		live, displaced := displacedShare(c)
		t.Logf("cap=%d warm=%d ratio=%.2f | shards=%d slots=%d live=%d load=%.3f | mean probe=%.2f p99=%d | "+
			"displaced=%.1f%%",
			tc.capacity, tc.warm, tc.ratio, len(c.shards), st.slots, st.live, float64(st.live)/float64(st.slots),
			mean(st.probeSum, st.live), percentile(st.hist, st.live, 0.99),
			100*float64(displaced)/float64(live))
		c.Close()
	}
}
