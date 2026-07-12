// Package eviction implements the shard's eviction policies (Clock and S3-FIFO) on top of the itemstate primitives.
// Every method runs strictly under the owning shard's mutex; queue nodes carry pool indices resolved through the
// shard's Pool handed in at construction.
package eviction

import "github.com/zakonnic/memstash/internal/itemstate"

// Policy is a shard's eviction policy contract.
type Policy[K comparable, V any] interface {
	// Add registers the node of a newly inserted item.
	Add(node itemstate.QNode)
	// Evict returns the pool index of the next record to reclaim: a victim tombstoned right here, or an item that
	// died earlier and whose node reached the queue head. (0, false) means there is nothing to evict.
	Evict(nowOff uint32) (uint32, bool)
	// Len returns the total number of queued nodes, dead ones included.
	Len() int
	// Sweep removes the nodes of dead items and hands their pool indices to release, preserving the order and
	// reference counters of live nodes.
	Sweep(release func(idx uint32))
	// Range calls f for every queued node; each live record is visited exactly once (used for table rebuilds).
	Range(f func(itemstate.QNode))
	// Bytes returns the footprint of the policy's own bookkeeping (queues and, for S3-FIFO, the ghost table).
	Bytes() int64
}
