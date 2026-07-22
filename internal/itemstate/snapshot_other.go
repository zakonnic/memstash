//go:build !amd64

package itemstate

import "sync/atomic"

// snapshotEntryInto does the unsynchronized copy that SnapshotInto later validates by generation.
// The fence keeps the copy's loads from completing after SnapshotInto's re-read, which would let a torn copy validate.
// An RMW on a stack dummy is an acquire+release pivot on every Go target, shares no cache line, and needs no
// assembly. It lives here rather than in SnapshotInto because a call there costs ~2 ns: it stops SnapshotInto inlining.
//
//go:norace // exempts this one benign race, so -race builds still catch races in the production path.
func (s *State[K, V]) snapshotEntryInto(dst *Entry[K, V]) {
	*dst = s.entry
	var fence uint32
	atomic.CompareAndSwapUint32(&fence, 0, 0)
}
