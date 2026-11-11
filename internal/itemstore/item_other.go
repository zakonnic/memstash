//go:build !amd64

package itemstore

import "sync/atomic"

// snapshotEntryInto does the unsynchronized copy that SnapshotInto later validates by generation.
// The fence keeps the copy's loads from completing after SnapshotInto's re-read, which would let a torn copy validate.
// An RMW on a stack dummy is an acquire+release pivot on every Go target, shares no cache line, and needs no
// assembly. It lives here rather than in SnapshotInto because a call there stops SnapshotInto inlining.
//
//go:norace // exempts this one benign race, so -race builds still catch races in the production path.
func (s *Item[K, V]) snapshotEntryInto(dst *Entry[K, V]) {
	*dst = s.entry
	var fence uint32
	atomic.CompareAndSwapUint32(&fence, 0, 0)
}

// beginWrite makes the generation odd to signal a write in progress. The store is only a release (STLR on arm64) and
// holds nothing after it, so the value stores could land first; the same pivot keeps them behind it.
func (s *Item[K, V]) beginWrite() {
	metaWord := s.meta.Load()
	storeMeta(&s.meta, metaWord&^GenMask|(metaWord+1)&GenMask)
	var fence uint32
	atomic.CompareAndSwapUint32(&fence, 0, 0)
}

// SetScalar overwrites a value that OneStore admits with one plain store: a reader gets the old or the new. Nothing
// before it is a release, so the pivot keeps the stores that built whatever the value points to ahead of it.
func (s *Item[K, V]) SetScalar(value V) {
	var fence uint32
	atomic.CompareAndSwapUint32(&fence, 0, 0)
	s.entry.Value = value
}
