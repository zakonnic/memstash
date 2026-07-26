package memstash

import "github.com/zakonnic/memstash/internal/itemstore"

// QNode is an eviction-queue element: the slot index of a cached item's state record plus the item's weight at
// enqueue time. The weight may drift after an overwrite - only queue selection depends on it, the global accounting
// is recomputed from the live value.
type QNode = itemstore.QNode

// Item is a cached item's state record: its key/value Entry alongside eviction metadata (a dead bit, a 2-bit
// reference counter set by lock-free reads, the expiration offset and an occupancy generation). See
// Load/Entry/Kill/TouchWith/RevokeChance/ResetChances for what a policy may do with it.
type Item[K comparable, V any] = itemstore.Item[K, V]

// Items resolves queue-node indices into item state records; the cache hands one to a custom eviction policy's
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
	// Add registers the node of a newly inserted item. The record already carries its Entry.
	Add(node QNode)
	// Evict removes the next reclaimable node from the queues and returns its slot index: the policy's chosen victim,
	// an expired item (ItemExpired), or an item that died earlier (ItemDead). The cache kills the record and accounts
	// its weight - the policy must not call Item.Kill. (0, false) means there is nothing to evict.
	Evict(nowOff uint32) (uint32, bool)
	// Len returns the total number of queued nodes, dead ones included.
	Len() int
	// Sweep removes the nodes of dead items and hands their slot indices to release, preserving the order and
	// reference counters of live nodes.
	Sweep(release func(idx uint32))
	// Rebuild re-links the queues after a table rebuild: remap turns a live node's old slot index into its new one, or
	// reports the node dead - those must drop out (their weight-accounting mirrors Sweep). Each record is visited
	// exactly once.
	Rebuild(remap func(oldIdx uint32) (newIdx uint32, live bool))
	// Bytes returns the footprint of the policy's own bookkeeping.
	Bytes() int64
}

// EvictionPolicyFactory builds one shard's eviction policy: states resolves node indices to records, shardCap is the
// shard's capacity in weight units. The cache calls it once per shard, so per-shard state is naturally private.
type EvictionPolicyFactory[K comparable, V any] func(items Items[K, V], shardCap int64) EvictionPolicy[K, V]
