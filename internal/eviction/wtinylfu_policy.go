package eviction

import (
	"hash/maphash"
	"math/bits"
	"unsafe"

	"github.com/zakonnic/memstash/internal/itemstore"
)

// WTinyLFUPolicy is W-TinyLFU adapted to lock-free reads: an admission window of ~1% of the shard capacity in front
// of a main queue run as GCLOCK, gated by a Count-Min frequency sketch. Reads only set the reference counter and
// cannot move items, so the LRU segments are approximated as ClockPolicy approximates LRU. The sketch hears of an
// access on every insert and whenever an eviction scan finds a set counter. It outlives evictions, so a key's
// frequency history survives its residency, which is what TinyLFU's admission filter relies on.
//
// Eviction drains the window first. A candidate touched in the window enters main outright. An untouched one must
// beat the frequency estimate of main's next victim; otherwise it is evicted and main stays intact (scan resistance).
type WTinyLFUPolicy[K comparable, V any] struct {
	admission[K, V] // the window
	sketch          freqSketch
	seed            maphash.Seed
}

// NewWTinyLFU creates a W-TinyLFU policy for a shard with the given capacity; sketchKeys sizes the frequency sketch
// (in keys, like the S3-FIFO ghost size).
func NewWTinyLFU[K comparable, V any](items *itemstore.StorageProxy[K, V], shardCap int64, sketchKeys int) *WTinyLFUPolicy[K, V] {
	return &WTinyLFUPolicy[K, V]{
		admission: admission[K, V]{items: items, cap: max(shardCap/100, 1)},
		sketch:    newFreqSketch(sketchKeys),
		seed:      maphash.MakeSeed(),
	}
}

// keyHash is what the sketch counts a key by: its own hash, since its history must outlive the table growing under
// it (a table fingerprint would not).
func (p *WTinyLFUPolicy[K, V]) keyHash(idx uint32) uint64 {
	return maphash.Comparable(p.seed, p.items.At(idx).Entry().Key)
}

func (p *WTinyLFUPolicy[K, V]) Add(node itemstore.QNode) {
	p.sketch.increment(p.keyHash(node.Idx))
	p.admission.Add(node)
}

func (p *WTinyLFUPolicy[K, V]) Bytes() int64 { return int64(unsafe.Sizeof(*p)) + p.sketch.bytes() }

func (p *WTinyLFUPolicy[K, V]) Evict(nowOff uint32) (uint32, bool) {
	t := p.items.GetStorage()
	for {
		idx, fromWindow, ok := p.next(t)
		if !ok {
			return 0, false
		}
		item := t.At(idx)
		metaWord := item.Metadata()
		switch {
		case chanced(metaWord, nowOff):
			// Reused: record it in the sketch; a window candidate is admitted without consulting the filter.
			p.sketch.increment(p.keyHash(idx))
			if fromWindow {
				item.ResetChances()
				p.promote(t, idx)
			} else {
				item.RevokeChance(metaWord)
				t.Requeue(mainQueue, idx)
			}
		case fromWindow && !itemstore.Expired(metaWord, nowOff) && p.admits(t, idx):
			p.promote(t, idx)
		default:
			return idx, true
		}
	}
}

// admits is the TinyLFU admission filter: the candidate enters main only when its frequency estimate beats that of
// main's next victim. An empty main admits freely.
func (p *WTinyLFUPolicy[K, V]) admits(t *itemstore.FlatHashMap[K, V], candidate uint32) bool {
	victim, ok := t.Oldest(mainQueue)
	return !ok || p.sketch.estimate(p.keyHash(candidate)) > p.sketch.estimate(p.keyHash(victim))
}

// freqSketch is a Count-Min sketch of 4-bit saturating counters, 16 to a word, allocated once. A key charges 4
// counters picked by Kirsch-Mitzenmacher double hashing of its 64-bit hash, and its estimate is their minimum. After
// sampleSize successful increments every counter is halved (TinyLFU's reset), so estimates track recent frequency.
type freqSketch struct {
	table      []uint64
	mask       uint32 // len(table) - 1; len is a power of two
	additions  uint32
	sampleSize uint32
}

// counterMax is the 4-bit saturation ceiling.
const counterMax = 15

// newFreqSketch creates a sketch sized for about `keys` distinct keys: one 64-bit word (16 counters) per key,
// rounded up to a power of two, and a sample window of 10x the keys - the Caffeine proportions, which keep the
// counters far from saturation within one aging window. keys <= 0 disables the sketch: increments are dropped and
// every estimate is 0.
func newFreqSketch(keys int) freqSketch {
	if keys <= 0 {
		return freqSketch{}
	}
	words := 1 << bits.Len(uint(keys-1)) // pow2 >= keys
	return freqSketch{
		table:      make([]uint64, words),
		mask:       uint32(words - 1),
		sampleSize: uint32(min(int64(words)*10, 1<<30)),
	}
}

func (s *freqSketch) bytes() int64 {
	return int64(len(s.table)) * int64(unsafe.Sizeof(uint64(0)))
}

// increment bumps the key's 4 counters (saturating) and ages the sketch when the sample window is full.
func (s *freqSketch) increment(hash uint64) {
	if len(s.table) == 0 {
		return
	}
	// An odd step keeps the 4 nibble positions distinct (i*step is injective mod 16 for an odd step).
	lo, hi := uint32(hash), uint32(hash>>32)|1
	added := false
	for i := uint32(0); i < 4; i++ {
		h := lo + i*hi
		shift := (h & 15) * 4
		word := &s.table[(h>>4)&s.mask]
		if (*word>>shift)&counterMax < counterMax {
			*word += 1 << shift
			added = true
		}
	}
	if added {
		if s.additions++; s.additions >= s.sampleSize {
			s.reset()
		}
	}
}

// estimate returns the key's frequency estimate: the minimum of its 4 counters.
func (s *freqSketch) estimate(hash uint64) uint32 {
	if len(s.table) == 0 {
		return 0
	}
	lo, hi := uint32(hash), uint32(hash>>32)|1
	est := uint32(counterMax)
	for i := uint32(0); i < 4; i++ {
		h := lo + i*hi
		if c := uint32(s.table[(h>>4)&s.mask]>>((h&15)*4)) & counterMax; c < est {
			est = c
		}
	}
	return est
}

// reset halves every counter in place (the aging step).
func (s *freqSketch) reset() {
	for i := range s.table {
		s.table[i] = (s.table[i] >> 1) & 0x7777777777777777
	}
	s.additions >>= 1
}
