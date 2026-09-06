package eviction

import (
	"math/bits"
	"unsafe"

	"github.com/zakonnic/memstash/internal/itemstore"
)

// S3FIFOPolicy implements S3-FIFO: a small quarantine (~10% of the shard capacity) for newcomers, a protected main
// queue for items proven to be reused, and ghost - a memory of keys recently evicted from small. A new key goes to
// small (or straight to main on a ghost hit); eviction from small promotes touched items to main and remembers the
// rest in ghost; main runs GCLOCK. Promotion just moves an 8-byte node - the item and its table slot stay put.
type S3FIFOPolicy[K comparable, V any] struct {
	items *itemstore.StorageProxy[K, V]
	small itemstore.EvictQueue
	main  itemstore.EvictQueue
	ghost ghostTable

	smallWeight int64 // total weight of nodes in small (including nodes of keys already gone)
	smallCap    int64 // target weight of small: ~10% of the shard capacity
}

// NewS3FIFO creates an S3-FIFO policy for a shard with the given capacity and ghost size (in keys).
func NewS3FIFO[K comparable, V any](items *itemstore.StorageProxy[K, V], shardCap int64, ghostSize int) *S3FIFOPolicy[K, V] {
	smallCap := shardCap / 10
	if smallCap < 1 {
		smallCap = 1
	}
	return &S3FIFOPolicy[K, V]{
		items:    items,
		ghost:    newGhostTable(ghostSize),
		smallCap: smallCap,
	}
}

func (p *S3FIFOPolicy[K, V]) Add(node itemstore.QNode) {
	if p.ghost.hit(node.Hash()) {
		// Recently evicted and needed again - straight to the protected queue.
		p.main.Push(node)
		return
	}
	p.small.Push(node)
	p.smallWeight += int64(node.Cost())
}

func (p *S3FIFOPolicy[K, V]) Len() int { return p.small.Len() + p.main.Len() }

func (p *S3FIFOPolicy[K, V]) Bytes() int64 {
	return int64(unsafe.Sizeof(*p)) + p.small.Bytes() + p.main.Bytes() + p.ghost.bytes()
}

func (p *S3FIFOPolicy[K, V]) Sweep(limit int) bool {
	if !p.small.Sweeping() && !p.main.Sweeping() {
		p.small.BeginSweep()
		p.main.BeginSweep()
	}
	// Nodes leaving small give their weight back, as in evictSmallOnce.
	more := itemstore.SweepStep(&p.small, p.items, limit, func(node itemstore.QNode) { p.smallWeight -= int64(node.Cost()) })
	return itemstore.SweepStep(&p.main, p.items, limit, nil) || more
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

// evictSmallOnce processes the head of small: (idx, true) means the item is the victim, (0, false) means the node
// was dropped or promoted to main.
func (p *S3FIFOPolicy[K, V]) evictSmallOnce(nowOff uint32) (uint32, bool) {
	candidate, ok := p.small.Pop()
	if !ok {
		return 0, false
	}
	p.smallWeight -= int64(candidate.Cost())

	item, idx, found := p.items.Find(candidate)
	if !found {
		return 0, false // the key left the cache; its node goes with it
	}
	if chanced(item.Metadata(), nowOff) {
		// Touched while in quarantine - promote to main with a clean counter.
		item.ResetChances()
		p.main.Push(candidate)
		return 0, false
	}
	if !itemstore.Expired(item.Metadata(), nowOff) {
		p.ghost.push(candidate.Hash())
	}
	return idx, true
}

// evictMainOnce performs a single GCLOCK step over main.
func (p *S3FIFOPolicy[K, V]) evictMainOnce(nowOff uint32) (uint32, bool) {
	candidate, ok := p.main.Pop()
	if !ok {
		return 0, false
	}
	item, idx, found := p.items.Find(candidate)
	if !found {
		return 0, false
	}
	if metaWord := item.Metadata(); chanced(metaWord, nowOff) {
		item.RevokeChance(metaWord)
		p.main.Push(candidate)
		return 0, false
	}
	return idx, true
}

// ghostTable is the S3-FIFO "ghost" queue: 32-bit key fingerprints in a fixed 2-way set-associative hash table -
// 8 bytes per slot, allocated once, no per-operation allocations. Every push stamps its slot with a sequence number;
// entries older than ageWindow pushes count as absent, approximating FIFO expiry. Collisions are benign both ways: a
// false hit sends a newcomer straight to main (GCLOCK still evicts it if unused), a lost entry costs one missed
// promotion. Keys are identified by the 40 hash bits a queue node carries, mixed so the slot and the fingerprint
// draw on all of them.
type ghostTable struct {
	slots     []ghostSlot
	mask      uint32 // number of 2-slot buckets minus one
	seq       uint32 // pushes so far; ages are uint32 differences, so wraparound is fine
	ageWindow uint32
}

// ghostSlot is one fingerprint entry: shortHash is never 0 for an occupied slot.
type ghostSlot struct {
	shortHash uint32
	seq       uint32
}

// newGhostTable creates a ghost table with at least `capacity` slots (rounded up to a power of two).
func newGhostTable(capacity int) ghostTable {
	if capacity <= 0 {
		return ghostTable{}
	}
	slots := 1 << bits.Len(uint(capacity-1)) // pow2 >= capacity, and >= 2 so buckets hold full pairs
	if slots < 2 {
		slots = 2
	}
	return ghostTable{
		slots:     make([]ghostSlot, slots),
		mask:      uint32(slots/2 - 1),
		ageWindow: uint32(slots),
	}
}

// mixHash spreads a node's identity bits (the high hash word and eight tag bits) over a full 64-bit word.
func mixHash(hash uint64) uint64 {
	hash ^= hash >> 33
	hash *= 0xff51afd7ed558ccd
	hash ^= hash >> 33
	hash *= 0xc4ceb9fe1a85ec53
	return hash ^ hash>>33
}

// locate maps a key's node hash to its bucket's first slot and its fingerprint.
func (g *ghostTable) locate(hash uint64) (uint32, uint32) {
	mixed := mixHash(hash)
	shortHash := uint32(mixed >> 32)
	if shortHash == 0 {
		shortHash = 1
	}
	return (uint32(mixed) & g.mask) * 2, shortHash
}

func (g *ghostTable) bytes() int64 {
	return int64(len(g.slots)) * int64(unsafe.Sizeof(ghostSlot{}))
}

// push records the key: refreshes its entry if present, otherwise replaces the older slot of the bucket.
func (g *ghostTable) push(hash uint64) {
	if len(g.slots) == 0 {
		return
	}
	first, shortHash := g.locate(hash)
	g.seq++
	a, b := &g.slots[first], &g.slots[first+1]
	target := a
	switch {
	case a.shortHash == shortHash:
	case b.shortHash == shortHash:
		target = b
	case g.seq-b.seq > g.seq-a.seq: // empty slots (seq 0) look maximally old and are taken first
		target = b
	}
	*target = ghostSlot{shortHash: shortHash, seq: g.seq}
}

// hit reports whether the key was pushed within the age window, consuming the entry on a hit.
func (g *ghostTable) hit(hash uint64) bool {
	if len(g.slots) == 0 {
		return false
	}
	first, fp := g.locate(hash)
	for i := first; i < first+2; i++ {
		slot := &g.slots[i]
		if slot.shortHash == fp && g.seq-slot.seq < g.ageWindow {
			*slot = ghostSlot{}
			return true
		}
	}
	return false
}
