package eviction

import (
	"math/bits"
	"unsafe"

	"github.com/zakonnic/memstash/internal/itemstore"
)

// S3FIFOPolicy implements S3-FIFO. A new key enters small, a quarantine of ~10% of the shard capacity, or goes
// straight to main when ghost remembers it. Eviction from small promotes the touched items to main and remembers the
// rest in ghost. Main runs GCLOCK.
type S3FIFOPolicy[K comparable, V any] struct {
	admission[K, V] // small
	ghost           ghostTable
}

// NewS3FIFO creates an S3-FIFO policy for a shard with the given capacity and ghost size (in keys).
func NewS3FIFO[K comparable, V any](items *itemstore.StorageProxy[K, V], shardCap int64, ghostSize int) *S3FIFOPolicy[K, V] {
	return &S3FIFOPolicy[K, V]{
		admission: admission[K, V]{items: items, cap: max(shardCap/10, 1)},
		ghost:     newGhostTable(ghostSize),
	}
}

func (p *S3FIFOPolicy[K, V]) Add(node itemstore.QNode) {
	t := p.items.GetStorage()
	if p.ghost.hit(t.Fingerprint(node.Idx)) {
		t.Requeue(mainQueue, node.Idx) // recently evicted and needed again
		return
	}
	p.admission.Add(node)
}

func (p *S3FIFOPolicy[K, V]) Bytes() int64 { return int64(unsafe.Sizeof(*p)) + p.ghost.bytes() }

func (p *S3FIFOPolicy[K, V]) Evict(nowOff uint32) (uint32, bool) {
	t := p.items.GetStorage()
	for {
		idx, fromSmall, ok := p.next(t)
		if !ok {
			return 0, false
		}
		item := t.At(idx)
		metaWord := item.Metadata()
		switch {
		case !chanced(metaWord, nowOff):
			if fromSmall && !itemstore.Expired(metaWord, nowOff) {
				p.ghost.push(t.Fingerprint(idx))
			}
			return idx, true
		case fromSmall: // touched while in quarantine: to main with a clean counter
			item.ResetChances()
			p.promote(t, idx)
		default:
			item.RevokeChance(metaWord)
			t.Requeue(mainQueue, idx)
		}
	}
}

// ghostTable is S3-FIFO's ghost queue: 32-bit short hashes of table fingerprints in a fixed 2-way set-associative
// table, 8 bytes a slot, allocated once. Each push stamps its slot with a sequence number, and entries older than
// ageWindow pushes count as absent, which approximates FIFO expiry. A collision is cheap either way: a false hit sends
// a newcomer straight to main, where GCLOCK still evicts it if unused, and a lost entry costs one missed promotion.
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
	// pow2 >= capacity, and >= 2 so buckets hold full pairs
	var slotsNum = max(1<<bits.Len(uint(capacity-1)), 2)
	return ghostTable{
		slots:     make([]ghostSlot, slotsNum),
		mask:      uint32(slotsNum/2 - 1),
		ageWindow: uint32(slotsNum),
	}
}

// mixHash spreads a fingerprint over 64 bits, so the bucket and the short hash both draw on all of it.
func mixHash(hash uint64) uint64 {
	hash ^= hash >> 33
	hash *= 0xff51afd7ed558ccd
	hash ^= hash >> 33
	hash *= 0xc4ceb9fe1a85ec53
	return hash ^ hash>>33
}

// locate maps a key's fingerprint to its bucket's first slot and its short hash.
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
