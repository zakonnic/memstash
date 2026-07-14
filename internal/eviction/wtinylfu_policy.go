package eviction

import (
	"hash/maphash"
	"math/bits"
	"unsafe"

	"github.com/zakonnic/memstash/internal/itemstate"
)

// WTinyLFUPolicy is W-TinyLFU adapted to lock-free reads: a small admission window (~1% of the shard capacity) in
// front of a protected main queue run as GCLOCK, gated by a Count-Min frequency sketch. Reads cannot move queue
// nodes here (they only set the record's reference counter), so the classic LRU segments are approximated the same
// way ClockPolicy approximates LRU, and access frequency is fed into the sketch at the points the policy does see:
// every insert, and the reference counters observed during eviction scans. The sketch outlives evictions, so a key's
// frequency history survives its residency - the property TinyLFU's admission filter relies on.
//
// Eviction drains the window first: a candidate touched while in the window is admitted to main outright; an
// untouched one must beat the frequency estimate of main's next victim, otherwise it is evicted on the spot and
// main stays intact (scan resistance).
type WTinyLFUPolicy[K comparable, V any] struct {
	pool   *itemstate.Pool[K, V]
	seed   maphash.Seed
	window itemstate.EvictQueue
	main   itemstate.EvictQueue
	sketch freqSketch

	windowWeight int64 // total weight of nodes in window (including tombstones not yet reclaimed)
	windowCap    int64 // target weight of window: ~1% of the shard capacity
}

// NewWTinyLFU creates a W-TinyLFU policy for a shard with the given capacity; sketchKeys sizes the frequency sketch
// (in keys, like the S3-FIFO ghost size).
func NewWTinyLFU[K comparable, V any](pool *itemstate.Pool[K, V], shardCap int64, sketchKeys int) *WTinyLFUPolicy[K, V] {
	windowCap := shardCap / 100
	if windowCap < 1 {
		windowCap = 1
	}
	return &WTinyLFUPolicy[K, V]{
		pool:      pool,
		seed:      maphash.MakeSeed(),
		sketch:    newFreqSketch(sketchKeys),
		windowCap: windowCap,
	}
}

func (p *WTinyLFUPolicy[K, V]) keyHash(key K) uint64 { return maphash.Comparable(p.seed, key) }

func (p *WTinyLFUPolicy[K, V]) Add(node itemstate.QNode) {
	p.sketch.increment(p.keyHash(p.pool.At(node.Idx).Entry().Key))
	p.window.Push(node)
	p.windowWeight += int64(node.Cost)
}

func (p *WTinyLFUPolicy[K, V]) Len() int { return p.window.Len() + p.main.Len() }

func (p *WTinyLFUPolicy[K, V]) Bytes() int64 {
	return int64(unsafe.Sizeof(*p)) + p.window.Bytes() + p.main.Bytes() + p.sketch.bytes()
}

func (p *WTinyLFUPolicy[K, V]) Sweep(release func(idx uint32)) {
	// Dead nodes leaving window give their weight back, as in evictWindowOnce.
	itemstate.SweepQueue(&p.window, p.pool, func(node itemstate.QNode) {
		p.windowWeight -= int64(node.Cost)
		release(node.Idx)
	})
	itemstate.SweepQueue(&p.main, p.pool, func(node itemstate.QNode) { release(node.Idx) })
}

func (p *WTinyLFUPolicy[K, V]) Range(f func(itemstate.QNode)) {
	p.window.Range(f)
	p.main.Range(f)
}

func (p *WTinyLFUPolicy[K, V]) Evict(nowOff uint32) (uint32, bool) {
	for {
		fromWindow := p.windowWeight > p.windowCap && p.window.Len() > 0
		if !fromWindow && p.main.Len() == 0 {
			if p.window.Len() == 0 {
				return 0, false
			}
			fromWindow = true
		}
		var (
			victim uint32
			found  bool
		)
		if fromWindow {
			victim, found = p.evictWindowOnce(nowOff)
		} else {
			victim, found = p.evictMainOnce(nowOff)
		}
		if found {
			return victim, true
		}
	}
}

// evictWindowOnce processes the head of window: (idx, true) means the record is reclaimed, (0, false) means the
// candidate was admitted to main.
func (p *WTinyLFUPolicy[K, V]) evictWindowOnce(nowOff uint32) (uint32, bool) {
	candidate, ok := p.window.Pop()
	if !ok {
		return 0, false
	}
	p.windowWeight -= int64(candidate.Cost)

	state := p.pool.At(candidate.Idx)
	metaWord := state.Load()
	switch {
	case metaWord&itemstate.Dead != 0:
		return candidate.Idx, true
	case itemstate.Expired(metaWord, nowOff):
		state.Kill()
		return candidate.Idx, true
	case metaWord&itemstate.ChanceMask != 0:
		// Touched while in the window: record the reuse and admit without consulting the filter.
		p.sketch.increment(p.keyHash(state.Entry().Key))
		state.ResetChances()
		p.main.Push(candidate)
		return 0, false
	default:
		if p.admit(state.Entry().Key) {
			p.main.Push(candidate)
			return 0, false
		}
		state.Kill()
		return candidate.Idx, true
	}
}

// admit is the TinyLFU admission filter: the candidate enters main only when its frequency estimate beats the next
// main victim's. An empty main or a tombstone at its head admits freely - nothing of value is displaced.
func (p *WTinyLFUPolicy[K, V]) admit(candidateKey K) bool {
	victim, ok := p.main.Peek()
	if !ok {
		return true
	}
	victimState := p.pool.At(victim.Idx)
	if victimState.Load()&itemstate.Dead != 0 {
		return true
	}
	return p.sketch.estimate(p.keyHash(candidateKey)) > p.sketch.estimate(p.keyHash(victimState.Entry().Key))
}

// evictMainOnce performs a single GCLOCK step over main, feeding observed reuse into the sketch.
func (p *WTinyLFUPolicy[K, V]) evictMainOnce(nowOff uint32) (uint32, bool) {
	candidate, ok := p.main.Pop()
	if !ok {
		return 0, false
	}
	state := p.pool.At(candidate.Idx)
	metaWord := state.Load()
	switch {
	case metaWord&itemstate.Dead != 0:
		return candidate.Idx, true
	case itemstate.Expired(metaWord, nowOff):
		state.Kill()
		return candidate.Idx, true
	case metaWord&itemstate.ChanceMask != 0:
		state.RevokeChance(metaWord)
		p.sketch.increment(p.keyHash(state.Entry().Key))
		p.main.Push(candidate)
		return 0, false
	default:
		state.Kill()
		return candidate.Idx, true
	}
}

// freqSketch is a Count-Min sketch with 4-bit saturating counters, 16 to a uint64 word, and periodic aging: after
// sampleSize successful increments every counter is halved, so estimates track recent frequency (TinyLFU's "reset").
// A key charges 4 counters picked by Kirsch-Mitzenmacher double hashing off its 64-bit hash; the estimate is their
// minimum. Allocated once at construction, no per-operation allocations.
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
