package memstash

import "github.com/zakonnic/memstash/internal/itemstore"

// QNode is an item as the eviction policy hears of it: its slot index in the shard's table and its weight.
type QNode = itemstore.QNode

// Item is one cached item: its key/value Entry alongside eviction metadata (a 2-bit reference counter set by
// lock-free reads, the expiration offset, the eviction queue it is in and an occupancy generation). See
// Metadata/Entry/TouchWith/RevokeChance/ResetChances for what a policy may do with it; killing is the cache's job,
// after Evict names the victim.
type Item[K comparable, V any] = itemstore.Item[K, V]

// Items is the shard's table as its eviction policy sees it; the cache hands one to a custom policy's factory. The
// cache enqueues every new item at the back of queue 0 and removes every departing one, so each live item sits in
// exactly one of the EvictionQueues queues threaded through the table; the policy moves items between the queues and
// picks victims. An index is good until the policy method returns: a deletion compacts its run and moves items. Call
// under the shard mutex only, which is where every policy method already runs.
type Items[K comparable, V any] interface {
	// At resolves a slot index into its item.
	At(idx uint32) *Item[K, V]
	// Cost is the item's weight as the cache accounts it.
	Cost(idx uint32) uint32
	// Oldest returns the item at the front of a queue, false when the queue is empty.
	Oldest(queue uint) (uint32, bool)
	// Newer returns the item queued after idx, false at the back of its queue.
	Newer(idx uint32) (uint32, bool)
	// Requeue moves the item to the back of a queue, its own or the other one; ItemInMainQueue follows.
	Requeue(queue uint, idx uint32)
	// Hand returns where a scan over queue 0 stands: the oldest item until it starts, and again once it passes the
	// back. The hand follows its item when that moves and steps on to the next one when that leaves.
	Hand() (uint32, bool)
	// AdvanceHand moves the hand to the next item of queue 0.
	AdvanceHand()
	// Fingerprint identifies the item's key without hashing it - its home slot and twelve tag bits - for a policy
	// that keeps history across residencies. It changes when the table grows.
	Fingerprint(idx uint32) uint64
}

// EvictionQueues is how many eviction queues a shard's table carries: queue 0, where every new item is enqueued, and
// queue 1.
const EvictionQueues = itemstore.EvictionQueues

const (
	// ItemDead is the meta-word bit of a killed item. A policy never meets one: the cache removes an item from its
	// queue before it kills it; the bit is exported for tooling that reads meta words directly.
	ItemDead = itemstore.Dead
	// ItemChanceMask isolates the meta word's reference counter: non-zero means the item was read since the counter
	// was last cleared.
	ItemChanceMask = itemstore.ChanceMask
	// ItemInMainQueue is the meta-word bit of an item in queue 1.
	ItemInMainQueue = itemstore.InMainQueue

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
// Item.Metadata.
type EvictionPolicy[K comparable, V any] interface {
	// Add registers a newly inserted item, already enqueued at the back of queue 0.
	Add(node QNode)
	// Remove tells the policy an item is leaving - evicted, deleted, expired or overflowed - while it is still in its
	// queue; the cache removes it from there right after. node.Cost is the weight the item is accounted with.
	Remove(node QNode)
	// Reweigh tells the policy an overwrite changed the weight of the item at idx by delta.
	Reweigh(idx uint32, delta int64)
	// Evict picks the next victim and returns its index: the policy's choice, or an expired item (ItemExpired). The
	// cache removes and kills it - the policy must not call Item.Kill. (0, false) means there is nothing to evict.
	Evict(nowOff uint32) (uint32, bool)
	// Bytes returns the footprint of the policy's own bookkeeping.
	Bytes() int64
}

// EvictionPolicyFactory builds one shard's eviction policy: items is the shard's table, shardCap the shard's
// capacity in weight units. The cache calls it once per shard, so per-shard state is naturally private.
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
