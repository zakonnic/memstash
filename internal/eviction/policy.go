// Package eviction implements the shard's eviction policies (ClockPolicy and S3-FIFO) on top of the per-item bookkeeping
// primitives in itemstate. Every method is called strictly under the owning shard's mutex, so the implementations
// perform no synchronization of their own. Queue nodes carry 32-bit pool indices, which the policies resolve through
// the owning shard's Pool (handed to them at construction).
package eviction

import "github.com/zakonnic/memstash/internal/itemstate"

// Policy is a shard's eviction policy contract.
type Policy[K comparable] interface {
	// Add registers the node of a newly inserted item.
	Add(node itemstate.QNode)
	// Evict returns the pool index of the next state record to reclaim: either a victim (marked as a tombstone right
	// here) or an item that died earlier (Delete, lazy TTL removal) whose node has reached the head of the queue. The
	// caller then removes the corresponding map entry, subtracts its weight, and returns the record to the pool.
	// (0, false) means there is nothing to evict.
	Evict(nowOff uint32) (uint32, bool)
	// Len returns the total number of queued nodes, dead ones included. The cache uses it to decide when the share of
	// tombstones justifies a Sweep.
	Len() int
	// Sweep removes the nodes of dead items from the queues and hands their pool indices to release for reuse. The
	// relative order of live nodes and their reference counters are preserved, so eviction semantics do not change.
	Sweep(release func(idx uint32))
	// Bytes returns the memory footprint of the policy's own bookkeeping (queues and, for S3-FIFO, the ghost
	// table) - not counting the state records they point to, which the shard's Pool already accounts for.
	Bytes() int64
}
