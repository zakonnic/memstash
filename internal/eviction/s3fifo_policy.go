package eviction

import (
	"unsafe"

	"github.com/zakonnic/memstash/internal/itemstate"
)

// ghostEntryOverhead is a rough per-entry estimate for ghost's hit-count map (Go does not expose the real bucket
// layout of a map to reflect on): one tophash byte per slot plus padding, rounded up.
const ghostEntryOverhead = 8

// S3FIFOPolicy implements S3-FIFO: a small quarantine (~10% of the shard capacity) for newcomers, a protected main queue
// for items proven to be reused, and ghost - a memory of keys recently evicted from small.
//
//   - a new key goes to small; a key found in ghost goes straight to main;
//   - eviction from small: reference counter > 0 promotes the item to main (with the counter reset), otherwise the item
//     is a victim and its key goes to ghost;
//   - eviction from main: GCLOCK (second-chance by the reference counter).
//
// The queues store nodes that reference stable state records, so promotion from small to main is just moving a
// 16-byte node - the record itself and the map entry stay put.
type S3FIFOPolicy[K comparable] struct {
	small itemstate.EvictQueue[K]
	main  itemstate.EvictQueue[K]
	ghost *ghostRing[K]

	smallWeight int64 // total weight of nodes in small (including tombstones not yet reclaimed)
	smallCap    int64 // target weight of small: ~10% of the shard capacity
}

// ghostRing is the S3-FIFO "ghost" queue: a ring buffer of keys recently evicted from small. It stores keys only (no
// values) and allocates its memory once at creation. Access is under the cache mutex.
//
// push keeps an occurrence count because the same key may enter the ring several times before its older entry is
// evicted.
type ghostRing[K comparable] struct {
	ring []K
	head int
	size int
	set  map[K]uint32
}

// NewS3FIFO creates an S3-FIFO policy for a shard with the given capacity and ghost queue size.
func NewS3FIFO[K comparable](shardCap int64, ghostSize int) *S3FIFOPolicy[K] {
	smallCap := shardCap / 10
	if smallCap < 1 {
		smallCap = 1
	}
	return &S3FIFOPolicy[K]{
		ghost:    newGhostRing[K](ghostSize),
		smallCap: smallCap,
	}
}

func newGhostRing[K comparable](capacity int) *ghostRing[K] {
	if capacity < 0 {
		capacity = 0
	}
	return &ghostRing[K]{
		ring: make([]K, capacity),
		set:  make(map[K]uint32, capacity),
	}
}

func (p *S3FIFOPolicy[K]) Add(item itemstate.QNode[K]) {
	if p.ghost.hit(item.State.Key) {
		// The key was recently evicted from small and is needed again - send it straight to the protected queue.
		p.main.Push(item)
		return
	}
	p.small.Push(item)
	p.smallWeight += int64(item.Cost)
}

func (p *S3FIFOPolicy[K]) Len() int { return p.small.Len() + p.main.Len() }

func (p *S3FIFOPolicy[K]) Bytes() int64 { return p.small.Bytes() + p.main.Bytes() + p.ghost.bytes() }

func (p *S3FIFOPolicy[K]) Sweep(release func(*itemstate.State[K])) {
	// Dead nodes leaving small must give their weight back, exactly as evictSmallOnce does.
	p.small.Sweep(func(node itemstate.QNode[K]) {
		p.smallWeight -= int64(node.Cost)
		release(node.State)
	})
	p.main.Sweep(func(node itemstate.QNode[K]) { release(node.State) })
}

func (p *S3FIFOPolicy[K]) Evict(nowOff uint32) (*itemstate.State[K], bool) {
	for {
		fromSmall := p.smallWeight > p.smallCap && p.small.Len() > 0
		if !fromSmall && p.main.Len() == 0 {
			if p.small.Len() == 0 {
				return nil, false
			}
			fromSmall = true
		}
		var (
			victim *itemstate.State[K]
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

// evictSmallOnce processes the head of small: (record, true) means the record is reclaimed, (nil, false) means the
// node was skipped or promoted to main.
func (p *S3FIFOPolicy[K]) evictSmallOnce(nowOff uint32) (*itemstate.State[K], bool) {
	candidate, ok := p.small.Pop()
	if !ok {
		return nil, false
	}
	p.smallWeight -= int64(candidate.Cost)

	metaWord := candidate.State.Load()
	switch {
	case metaWord&itemstate.Dead != 0:
		return candidate.State, true
	case itemstate.Expired(metaWord, nowOff):
		candidate.State.Kill()
		return candidate.State, true
	case metaWord&itemstate.ChanceMask != 0:
		// The item was accessed while in quarantine - it has proven useful and is promoted to main with a clean
		// counter.
		candidate.State.ResetChances()
		p.main.Push(candidate)
		return nil, false
	default:
		candidate.State.Kill()
		p.ghost.push(candidate.State.Key)
		return candidate.State, true
	}
}

// evictMainOnce performs a single GCLOCK step over main.
func (p *S3FIFOPolicy[K]) evictMainOnce(nowOff uint32) (*itemstate.State[K], bool) {
	candidate, ok := p.main.Pop()
	if !ok {
		return nil, false
	}
	metaWord := candidate.State.Load()
	switch {
	case metaWord&itemstate.Dead != 0:
		return candidate.State, true
	case itemstate.Expired(metaWord, nowOff):
		candidate.State.Kill()
		return candidate.State, true
	case metaWord&itemstate.ChanceMask != 0:
		candidate.State.RevokeChance(metaWord)
		p.main.Push(candidate)
		return nil, false
	default:
		candidate.State.Kill()
		return candidate.State, true
	}
}

// bytes estimates the ring's memory footprint.
func (g *ghostRing[K]) bytes() int64 {
	var zeroKey K
	keySize := int64(unsafe.Sizeof(zeroKey))
	ringBytes := int64(len(g.ring)) * keySize
	setBytes := int64(len(g.set)) * (keySize + int64(unsafe.Sizeof(uint32(0))) + ghostEntryOverhead)
	return ringBytes + setBytes
}

// push adds a key, evicting the oldest one on overflow.
func (g *ghostRing[K]) push(key K) {
	ringLen := len(g.ring)
	if ringLen == 0 {
		return
	}
	if g.size == ringLen {
		g.popOldest()
	}
	g.ring[(g.head+g.size)%ringLen] = key
	g.size++
	g.set[key]++
}

func (g *ghostRing[K]) popOldest() {
	key := g.ring[g.head]
	var zero K
	g.ring[g.head] = zero
	g.head = (g.head + 1) % len(g.ring)
	g.size--
	// The key may have been removed from set early by a hit() call: the count guards against decrementing into nowhere.
	if count := g.set[key]; count > 1 {
		g.set[key] = count - 1
	} else if count == 1 {
		delete(g.set, key)
	}
}

// hit checks whether the key is present and, on a hit, removes it from the index (stale ring slots are cleaned up
// later by popOldest). Returns true on a hit.
func (g *ghostRing[K]) hit(key K) bool {
	if _, ok := g.set[key]; !ok {
		return false
	}
	delete(g.set, key)
	return true
}
