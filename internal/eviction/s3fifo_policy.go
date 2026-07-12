package eviction

import (
	"hash/maphash"
	"math/bits"
	"unsafe"

	"github.com/zakonnic/memstash/internal/itemstate"
)

// S3FIFOPolicy implements S3-FIFO: a small quarantine (~10% of the shard capacity) for newcomers, a protected main
// queue for items proven to be reused, and ghost - a memory of keys recently evicted from small. A new key goes to
// small (or straight to main on a ghost hit); eviction from small promotes touched items to main and remembers the
// rest in ghost; main runs GCLOCK. Promotion just moves an 8-byte node - the record and its table slot stay put.
type S3FIFOPolicy[K comparable, V any] struct {
	pool  *itemstate.Pool[K, V]
	small itemstate.EvictQueue
	main  itemstate.EvictQueue
	ghost ghostTable[K]

	smallWeight int64 // total weight of nodes in small (including tombstones not yet reclaimed)
	smallCap    int64 // target weight of small: ~10% of the shard capacity
}

// NewS3FIFO creates an S3-FIFO policy for a shard with the given capacity and ghost size (in keys).
func NewS3FIFO[K comparable, V any](pool *itemstate.Pool[K, V], shardCap int64, ghostSize int) *S3FIFOPolicy[K, V] {
	smallCap := shardCap / 10
	if smallCap < 1 {
		smallCap = 1
	}
	return &S3FIFOPolicy[K, V]{
		pool:     pool,
		ghost:    newGhostTable[K](ghostSize),
		smallCap: smallCap,
	}
}

func (p *S3FIFOPolicy[K, V]) Add(node itemstate.QNode) {
	if p.ghost.hit(p.pool.At(node.Idx).Entry().Key) {
		// Recently evicted and needed again - straight to the protected queue.
		p.main.Push(node)
		return
	}
	p.small.Push(node)
	p.smallWeight += int64(node.Cost)
}

func (p *S3FIFOPolicy[K, V]) Len() int { return p.small.Len() + p.main.Len() }

func (p *S3FIFOPolicy[K, V]) Bytes() int64 {
	return int64(unsafe.Sizeof(*p)) + p.small.Bytes() + p.main.Bytes() + p.ghost.bytes()
}

func (p *S3FIFOPolicy[K, V]) Sweep(release func(idx uint32)) {
	// Dead nodes leaving small give their weight back, as in evictSmallOnce.
	itemstate.SweepQueue(&p.small, p.pool, func(node itemstate.QNode) {
		p.smallWeight -= int64(node.Cost)
		release(node.Idx)
	})
	itemstate.SweepQueue(&p.main, p.pool, func(node itemstate.QNode) { release(node.Idx) })
}

func (p *S3FIFOPolicy[K, V]) Range(f func(itemstate.QNode)) {
	p.small.Range(f)
	p.main.Range(f)
}

func (p *S3FIFOPolicy[K, V]) Evict(nowOff uint32) (uint32, bool) {
	for {
		fromSmall := p.smallWeight > p.smallCap && p.small.Len() > 0
		if !fromSmall && p.main.Len() == 0 {
			if p.small.Len() == 0 {
				return 0, false
			}
			fromSmall = true
		}
		var (
			victim uint32
			found  bool
		)
		if fromSmall {
			victim, found = p.evictSmallOnce(nowOff)
		} else {
			victim, found = p.evictMainOnce(nowOff)
		}
		if found {
			return victim, true
		}
	}
}

// evictSmallOnce processes the head of small: (idx, true) means the record is reclaimed, (0, false) means the node
// was skipped or promoted to main.
func (p *S3FIFOPolicy[K, V]) evictSmallOnce(nowOff uint32) (uint32, bool) {
	candidate, ok := p.small.Pop()
	if !ok {
		return 0, false
	}
	p.smallWeight -= int64(candidate.Cost)

	state := p.pool.At(candidate.Idx)
	metaWord := state.Load()
	switch {
	case metaWord&itemstate.Dead != 0:
		return candidate.Idx, true
	case itemstate.Expired(metaWord, nowOff):
		state.Kill()
		return candidate.Idx, true
	case metaWord&itemstate.ChanceMask != 0:
		// Touched while in quarantine - promote to main with a clean counter.
		state.ResetChances()
		p.main.Push(candidate)
		return 0, false
	default:
		state.Kill()
		p.ghost.push(state.Entry().Key)
		return candidate.Idx, true
	}
}

// evictMainOnce performs a single GCLOCK step over main.
func (p *S3FIFOPolicy[K, V]) evictMainOnce(nowOff uint32) (uint32, bool) {
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
		p.main.Push(candidate)
		return 0, false
	default:
		state.Kill()
		return candidate.Idx, true
	}
}

// ghostTable is the S3-FIFO "ghost" queue: 32-bit key fingerprints in a fixed 2-way set-associative hash table -
// 8 bytes per slot, allocated once, no per-operation allocations. Every push stamps its slot with a sequence number;
// entries older than ageWindow pushes count as absent, approximating FIFO expiry. Collisions are benign both ways: a
// false hit sends a newcomer straight to main (GCLOCK still evicts it if unused), a lost entry costs one missed
// promotion.
type ghostTable[K comparable] struct {
	seed      maphash.Seed
	slots     []ghostSlot
	mask      uint32 // number of 2-slot buckets minus one
	seq       uint32 // pushes so far; ages are uint32 differences, so wraparound is fine
	ageWindow uint32
}

// ghostSlot is one fingerprint entry: fp is never 0 for an occupied slot.
type ghostSlot struct {
	fp  uint32
	seq uint32
}

// newGhostTable creates a ghost table with at least `capacity` slots (rounded up to a power of two).
func newGhostTable[K comparable](capacity int) ghostTable[K] {
	if capacity <= 0 {
		return ghostTable[K]{seed: maphash.MakeSeed()}
	}
	slots := 1 << bits.Len(uint(capacity-1)) // pow2 >= capacity, and >= 2 so buckets hold full pairs
	if slots < 2 {
		slots = 2
	}
	return ghostTable[K]{
		seed:      maphash.MakeSeed(),
		slots:     make([]ghostSlot, slots),
		mask:      uint32(slots/2 - 1),
		ageWindow: uint32(slots),
	}
}

// locate maps a key to its bucket's first slot and its fingerprint. The table hashes with its own seed so slot
// selection is uncorrelated with shard selection.
func (g *ghostTable[K]) locate(key K) (uint32, uint32) {
	hash := maphash.Comparable(g.seed, key)
	fp := uint32(hash >> 32)
	if fp == 0 {
		fp = 1
	}
	return (uint32(hash) & g.mask) * 2, fp
}

func (g *ghostTable[K]) bytes() int64 {
	return int64(len(g.slots)) * int64(unsafe.Sizeof(ghostSlot{}))
}

// push records the key: refreshes its entry if present, otherwise replaces the older slot of the bucket.
func (g *ghostTable[K]) push(key K) {
	if len(g.slots) == 0 {
		return
	}
	first, fp := g.locate(key)
	g.seq++
	a, b := &g.slots[first], &g.slots[first+1]
	target := a
	switch {
	case a.fp == fp:
	case b.fp == fp:
		target = b
	case g.seq-b.seq > g.seq-a.seq: // empty slots (seq 0) look maximally old and are taken first
		target = b
	}
	*target = ghostSlot{fp: fp, seq: g.seq}
}

// hit reports whether the key was pushed within the age window, consuming the entry on a hit.
func (g *ghostTable[K]) hit(key K) bool {
	if len(g.slots) == 0 {
		return false
	}
	first, fp := g.locate(key)
	for i := first; i < first+2; i++ {
		slot := &g.slots[i]
		if slot.fp == fp && g.seq-slot.seq < g.ageWindow {
			*slot = ghostSlot{}
			return true
		}
	}
	return false
}
