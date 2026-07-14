package memstash

import "github.com/zakonnic/memstash/internal/itemstate"

// QNode is an eviction-queue element: the pool index of a cached item's state record plus the item's weight at
// enqueue time. The weight may drift after an overwrite - only queue selection depends on it, the global accounting
// is recomputed from the live value.
type QNode = itemstate.QNode

// ItemState is a cached item's 16-byte state record: an atomic pointer to its immutable key/value box plus eviction
// metadata (a dead bit, a 2-bit reference counter set by lock-free reads, the expiration offset and an occupancy
// generation). See the Load/Entry/Kill/TouchWith/RevokeChance/ResetChances methods for what a policy may do with it.
type ItemState[K comparable, V any] = itemstate.State[K, V]

// ItemStates resolves queue-node indices into item state records; the cache hands one to a custom eviction policy's
// factory. Resolution is lock-free and stays valid for the lifetime of the cache.
type ItemStates[K comparable, V any] interface {
	At(idx uint32) *ItemState[K, V]
}

const (
	// ItemDead is the meta-word bit marking a tombstone: the item was removed (Delete, TTL, overwrite races) and its
	// weight is already accounted; the policy's only job is to return the node's index from Evict or Sweep it out.
	ItemDead = itemstate.Dead
	// ItemChanceMask isolates the meta word's reference counter: non-zero means the item was read since the counter
	// was last cleared.
	ItemChanceMask = itemstate.ChanceMask
)

// ItemExpired reports whether the meta word's TTL has elapsed at the given coarse clock value (the nowOff passed to
// EvictionPolicy.Evict).
func ItemExpired(metaWord uint64, nowOff uint32) bool { return itemstate.Expired(metaWord, nowOff) }

// EvictionPolicy is one shard's eviction policy: the contract the built-in policies (Clock, S3-FIFO, W-TinyLFU,
// SIEVE) implement and a custom policy plugged in through WithCustomEvictionPolicy must satisfy. Every method is
// called strictly under the owning shard's mutex, so implementations need no synchronization of their own - but
// item reference counters are set concurrently by lock-free readers, so meta words must be read through
// ItemState.Load.
type EvictionPolicy[K comparable, V any] interface {
	// Add registers the node of a newly inserted item. The record already carries its Entry.
	Add(node QNode)
	// Evict returns the pool index of the next record to reclaim: a victim the policy kills right here
	// (ItemState.Kill), or an item that died earlier (ItemDead) and whose node the scan reached. Items whose TTL has
	// elapsed (ItemExpired) should be killed and reclaimed on sight. (0, false) means there is nothing to evict.
	Evict(nowOff uint32) (uint32, bool)
	// Len returns the total number of queued nodes, dead ones included.
	Len() int
	// Sweep removes the nodes of dead items and hands their pool indices to release, preserving the order and
	// reference counters of live nodes.
	Sweep(release func(idx uint32))
	// Range calls f for every queued node; each record is visited exactly once (used for table rebuilds).
	Range(f func(QNode))
	// Bytes returns the footprint of the policy's own bookkeeping.
	Bytes() int64
}

// EvictionPolicyFactory builds one shard's eviction policy: states resolves node indices to records, shardCap is the
// shard's capacity in weight units. The cache calls it once per shard, so per-shard state is naturally private.
type EvictionPolicyFactory[K comparable, V any] func(states ItemStates[K, V], shardCap int64) EvictionPolicy[K, V]
