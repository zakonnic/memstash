package memstash

import "github.com/zakonnic/memstash/internal/itemstore"

// QNode is an eviction-queue element: a claim on a cached key - 40 bits of its hash - plus the item's weight at
// enqueue time (Cost). A node never names a slot: items move when a deletion compacts their run, so a policy resolves
// a node to its item through Items.Find whenever it needs one. The weight may drift after an overwrite - only queue
// selection depends on it, the global accounting is recomputed from the live value.
type QNode = itemstore.QNode

// Item is one cached item: its key/value Entry alongside eviction metadata (a 2-bit reference counter set by
// lock-free reads, the expiration offset and an occupancy generation). See Metadata/Entry/TouchWith/RevokeChance/
// ResetChances for what a policy may do with it; killing is the cache's job, after Evict names the victim.
type Item[K comparable, V any] = itemstore.Item[K, V]

// Items resolves queue nodes into the items themselves; the cache hands one to a custom eviction policy's factory.
type Items[K comparable, V any] interface {
	// Find returns the item a node was enqueued for and its current slot index, or false when that key has since
	// left the cache - the node is stale then and the policy drops it. It reads no key: the match is the home slot
	// and twelve hash bits, and two live keys sharing both - a twin pair, once in ~4096 keys homed together - look
	// alike to it. That is harmless for eviction: the twins' nodes serve either item, so evicting one for the
	// other's node leaves the class with as many nodes as items, and nothing ends up unevictable. Call under the
	// shard mutex only, which is where every policy method already runs.
	Find(node QNode) (item *Item[K, V], idx uint32, ok bool)
	// FindExact is Find with the candidate's key rehashed against the node's hash bits, so a twin is passed over.
	// It is for the one decision a twin would corrupt: dropping a node as a duplicate in Sweep, which would leave
	// the twin's item without a node. Eviction has no use for it - see Find.
	FindExact(node QNode) (item *Item[K, V], idx uint32, ok bool)
}

const (
	// ItemDead is the meta-word bit of a killed item. A policy never meets one: Find hands out live items only, and
	// the kill happens after Evict returns; the bit is exported for tooling that reads meta words directly.
	ItemDead = itemstore.Dead
	// ItemChanceMask isolates the meta word's reference counter: non-zero means the item was read since the counter
	// was last cleared.
	ItemChanceMask = itemstore.ChanceMask

	// --- DeletionCause ---

	// CauseInvalidation is an explicit Delete or BatchDelete.
	CauseInvalidation DeletionCause = iota
	// CauseReplacement is a Set over a live key; the handler receives the value that was replaced.
	CauseReplacement
	// CauseExpiration is an elapsed TTL. It is reported when the item is actually reclaimed - by the read that finds
	// it expired, or by the eviction pass that reaches it - not at the instant the deadline passes.
	CauseExpiration
	// CauseEviction is capacity pressure: the policy picked this item as its victim.
	CauseEviction
	// CauseOverflow is a Set whose item alone outweighs the whole shard: the new value is not stored, and the value
	// already under that key is dropped so it stops serving reads.
	CauseOverflow
)

// ItemExpired reports whether the meta word's TTL has elapsed at the given coarse clock value (the nowOff passed to
// EvictionPolicy.Evict).
func ItemExpired(metaWord uint64, nowOff uint32) bool { return itemstore.Expired(metaWord, nowOff) }

// ItemQueuedIn reports whether the running sweep pass has already kept a node for this item; MarkItemQueued records
// that it has. Both take the parity handed to EvictionPolicy.Sweep, and both are for that method only.
func ItemQueuedIn(metaWord uint64, parity bool) bool { return itemstore.QueuedIn(metaWord, parity) }

// MarkItemQueued records that the running sweep pass has kept a node for this item. See ItemQueuedIn.
func MarkItemQueued[K comparable, V any](item *Item[K, V], parity bool) { item.SetQueued(parity) }

// EvictionPolicy is one shard's eviction policy: the contract the built-in policies (Clock, S3-FIFO, W-TinyLFU,
// SIEVE) implement and a custom policy plugged in through WithCustomEvictionPolicy must satisfy. Every method is
// called strictly under the owning shard's mutex, so implementations need no synchronization of their own - but
// item reference counters are set concurrently by lock-free readers, so meta words must be read through
// Item.Metadata.
type EvictionPolicy[K comparable, V any] interface {
	// Add registers the node of a newly inserted item. The item already carries its Entry; the node carries the
	// key's hash (QNode.Hash) for a policy that keeps history across residencies.
	Add(node QNode)
	// Evict removes the next victim's node from the queues and returns the item's current slot index: the policy's
	// chosen victim, or an expired item (ItemExpired). A node Find cannot resolve is stale - its key is gone - and
	// the policy drops it and moves on. The cache kills the item and
	// accounts its weight - the policy must not call Item.Kill. (0, false) means there is nothing to evict.
	Evict(nowOff uint32) (uint32, bool)
	// Len returns the total number of queued nodes, stale ones included.
	Len() int
	// Sweep keeps the one node each live item is entitled to and drops the rest: the nodes Find cannot resolve, whose
	// keys left the cache, and the duplicates a delete-then-set of one key leaves behind. It works in slices: each
	// call examines up to limit nodes and reports whether the pass that began with the first call still has nodes to
	// examine; the cache calls it from every write until it reports false.
	//
	// parity is the same for every call of one pass and the opposite of the last pass's: ItemQueuedIn tells whether
	// the pass has already kept a node for that item, and MarkItemQueued records that it has. A policy that ignores
	// parity still works - it just leaves the duplicates for the eviction path to pop.
	Sweep(limit int, parity bool) (more bool)
	// Bytes returns the footprint of the policy's own bookkeeping.
	Bytes() int64
}

// EvictionPolicyFactory builds one shard's eviction policy: items resolves node indices to the cached items,
// shardCap is the shard's capacity in weight units. The cache calls it once per shard, so per-shard state is
// naturally private.
type EvictionPolicyFactory[K comparable, V any] func(items Items[K, V], shardCap int64) EvictionPolicy[K, V]

// DeletionCause tells why an item left the first level.
type DeletionCause uint8

// Automatic reports whether the cache removed the item on its own rather than on an explicit Delete or an overwriting
// Set - the filter a handler needs when only the cache's own decisions matter.
func (c DeletionCause) Automatic() bool {
	return c == CauseExpiration || c == CauseEviction || c == CauseOverflow
}

func (c DeletionCause) String() string {
	switch c {
	case CauseInvalidation:
		return "invalidation"
	case CauseReplacement:
		return "replacement"
	case CauseExpiration:
		return "expiration"
	case CauseEviction:
		return "eviction"
	case CauseOverflow:
		return "overflow"
	}
	return "unknown"
}

// deletion is one recorded removal, buffered under the shard mutex and delivered once it is released.
type deletion[K comparable, V any] struct {
	key   K
	value V
	cause DeletionCause
}

// addDeletion records a removal for delivery after the shard mutex is released. Neither this nor callOnDeletion is
// inlinable, so call sites check onDeletion themselves - that keeps a cache without a handler free of the call.
func (c *Cache[K, V]) addDeletion(deletions *[]deletion[K, V], key K, value V, cause DeletionCause) {
	*deletions = append(*deletions, deletion[K, V]{key: key, value: value, cause: cause})
}

// callOnDeletion runs the handler for the recorded removals, in the order the items died. Must be called with no
// shard mutex held: a handler may take any amount of time and may call back into the cache.
func (c *Cache[K, V]) callOnDeletion(deletions []deletion[K, V]) {
	for i := range deletions {
		d := &deletions[i]
		c.onDeletion(d.key, d.value, d.cause)
	}
}
