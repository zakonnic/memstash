package eviction

import (
	"hash/maphash"
	"math/bits"
	"unsafe"

	"github.com/zakonnic/memstash/internal/itemstate"
)

// S3FIFOPolicy implements S3-FIFO: a small quarantine (~10% of the shard capacity) for newcomers, a protected main queue
// for items proven to be reused, and ghost - a memory of keys recently evicted from small.
//
//   - a new key goes to small; a key found in ghost goes straight to main;
//   - eviction from small: reference counter > 0 promotes the item to main (with the counter reset), otherwise the item
//     is a victim and its key goes to ghost;
//   - eviction from main: GCLOCK (second-chance by the reference counter).
//
// The queues store 8-byte nodes referencing stable pool slots, so promotion from small to main is just moving a node -
// the record itself and the map entry stay put.
type S3FIFOPolicy[K comparable] struct {
	pool  *itemstate.Pool[K]
	small itemstate.EvictQueue
	main  itemstate.EvictQueue
	ghost ghostTable[K]

	smallWeight int64 // total weight of nodes in small (including tombstones not yet reclaimed)
	smallCap    int64 // target weight of small: ~10% of the shard capacity
}

// NewS3FIFO creates an S3-FIFO policy for a shard with the given capacity and ghost size (in keys). The pool is the
// owning shard's record pool, used to resolve queue node indices.
func NewS3FIFO[K comparable](pool *itemstate.Pool[K], shardCap int64, ghostSize int) *S3FIFOPolicy[K] {
	smallCap := shardCap / 10
	if smallCap < 1 {
		smallCap = 1
	}
	return &S3FIFOPolicy[K]{
		pool:     pool,
		ghost:    newGhostTable[K](ghostSize),
		smallCap: smallCap,
	}
}

func (p *S3FIFOPolicy[K]) Add(node itemstate.QNode) {
	if p.ghost.hit(p.pool.At(node.Idx).Key) {
		// The key was recently evicted from small and is needed again - send it straight to the protected queue.
		p.main.Push(node)
		return
	}
	p.small.Push(node)
	p.smallWeight += int64(node.Cost)
}

func (p *S3FIFOPolicy[K]) Len() int { return p.small.Len() + p.main.Len() }

func (p *S3FIFOPolicy[K]) Bytes() int64 {
	return int64(unsafe.Sizeof(*p)) + p.small.Bytes() + p.main.Bytes() + p.ghost.bytes()
}

func (p *S3FIFOPolicy[K]) Sweep(release func(idx uint32)) {
	// Dead nodes leaving small must give their weight back, exactly as evictSmallOnce does.
	itemstate.SweepQueue(&p.small, p.pool, func(node itemstate.QNode) {
		p.smallWeight -= int64(node.Cost)
		release(node.Idx)
	})
	itemstate.SweepQueue(&p.main, p.pool, func(node itemstate.QNode) { release(node.Idx) })
}

func (p *S3FIFOPolicy[K]) Evict(nowOff uint32) (uint32, bool) {
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
func (p *S3FIFOPolicy[K]) evictSmallOnce(nowOff uint32) (uint32, bool) {
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
		// The item was accessed while in quarantine - it has proven useful and is promoted to main with a clean
		// counter.
		state.ResetChances()
		p.main.Push(candidate)
		return 0, false
	default:
		state.Kill()
		p.ghost.push(state.Key)
		return candidate.Idx, true
	}
}

// evictMainOnce performs a single GCLOCK step over main.
func (p *S3FIFOPolicy[K]) evictMainOnce(nowOff uint32) (uint32, bool) {
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

// ghostTable is the S3-FIFO "ghost" queue: a memory of keys recently evicted from small. Instead of storing keys (the
// old design was a ring of keys plus a map[K]uint32 index, ~40+ bytes per slot for uint64 keys), it keeps 32-bit key
// fingerprints in a fixed 2-way set-associative hash table - 8 bytes per slot, allocated once at construction, no map
// and no per-operation allocations.
//
// Aging: every push stamps its slot with a sequence number; an entry older than ageWindow pushes is treated as absent,
// which approximates the FIFO expiry of the old ring. Collisions are benign either way: a false hit sends a newcomer
// straight to main (GCLOCK will still evict it if unused), a lost entry costs one missed promotion.
type ghostTable[K comparable] struct {
	seed      maphash.Seed
	slots     []ghostSlot
	mask      uint32 // number of 2-slot buckets minus one
	seq       uint32 // pushes so far; wraparound is handled by uint32 subtraction
	ageWindow uint32 // entries older than this many pushes are treated as absent
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

// locate maps a key to its bucket's first slot index and its fingerprint. The seed is the table's own (not the
// cache's shard seed), so slot selection is uncorrelated with shard selection.
func (g *ghostTable[K]) locate(key K) (uint32, uint32) {
	hash := maphash.Comparable(g.seed, key)
	fp := uint32(hash >> 32)
	if fp == 0 {
		fp = 1
	}
	return (uint32(hash) & g.mask) * 2, fp
}

// bytes returns the table's memory footprint.
func (g *ghostTable[K]) bytes() int64 {
	return int64(len(g.slots)) * int64(unsafe.Sizeof(ghostSlot{}))
}

// push records the key, refreshing its entry if present, otherwise replacing the older slot of its bucket.
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

// hit checks whether the key was pushed within the age window and, on a hit, clears its entry (mirroring the old
// ring's remove-on-hit semantics). Returns true on a hit.
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
