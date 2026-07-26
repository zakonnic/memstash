package memstash

import "github.com/zakonnic/memstash/internal/itemstore"

// QNode is an eviction-queue element: a cached item's index in the shard's table plus the item's weight at enqueue
// time. The weight may drift after an overwrite - only queue selection depends on it, the global accounting is
// recomputed from the live value.
type QNode = itemstore.QNode

// Item is one cached item: its key/value Entry alongside eviction metadata (a dead bit, a 2-bit
// reference counter set by lock-free reads, the expiration offset and an occupancy generation). See
// Load/Entry/Kill/TouchWith/RevokeChance/ResetChances for what a policy may do with it.
type Item[K comparable, V any] = itemstore.Item[K, V]

// Items resolves queue-node indices into the items themselves; the cache hands one to a custom eviction policy's
// factory. Resolution is lock-free and stays valid for the lifetime of the cache.
type Items[K comparable, V any] interface {
	At(idx uint32) *Item[K, V]
}

const (
	// ItemDead is the meta-word bit marking a tombstone: the item was removed (Delete, TTL, overwrite races) and its
	// weight is already accounted; the policy's only job is to return the node's index from Evict or Sweep it out.
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

// EvictionPolicy is one shard's eviction policy: the contract the built-in policies (Clock, S3-FIFO, W-TinyLFU,
// SIEVE) implement and a custom policy plugged in through WithCustomEvictionPolicy must satisfy. Every method is
// called strictly under the owning shard's mutex, so implementations need no synchronization of their own - but
// item reference counters are set concurrently by lock-free readers, so meta words must be read through
// Item.Load.
type EvictionPolicy[K comparable, V any] interface {
	// Add registers the node of a newly inserted item. The item already carries its Entry.
	Add(node QNode)
	// Evict removes the next reclaimable node from the queues and returns its index: the policy's chosen victim, an
	// expired item (ItemExpired), or an item that died earlier (ItemDead). The cache kills the item and accounts its
	// weight - the policy must not call Item.Kill. (0, false) means there is nothing to evict.
	Evict(nowOff uint32) (uint32, bool)
	// Len returns the total number of queued nodes, dead ones included.
	Len() int
	// Sweep removes the nodes of dead items and hands their indices to release, preserving the order and reference
	// counters of live nodes.
	Sweep(release func(idx uint32))
	// Rebuild re-links the queues after a table rebuild: remap turns a live node's old index into its new one, or
	// reports the node dead - those must drop out (their weight-accounting mirrors Sweep). Each node is visited
	// exactly once.
	Rebuild(remap func(oldIdx uint32) (newIdx uint32, live bool))
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
