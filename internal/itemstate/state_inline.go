//go:build !race && amd64

package itemstate

import (
	"sync/atomic"
	"unsafe"
)

// This build carries the item's Entry inline - no per-item box, one dependent cache line per read instead of two.
// Lock-free readers run a seqlock: copy the Entry, then re-check the meta word; a recycle or a multi-word overwrite
// advances the generation, so a torn copy never validates. The plain copy only holds on x86 (ordered loads, no
// split aligned words) and outside -race, which flags its benign race; state_boxed.go covers the rest.

// State is a cached item's record: an atomic meta word plus its Entry inline. Lives by value in a pool chunk and is
// reused without allocations.
type State[K comparable, V any] struct {
	meta  atomic.Uint64
	entry Entry[K, V]
}

// Entry returns the record's key/value pair in place. Callers must hold the shard mutex; lock-free readers use
// Snapshot instead.
func (s *State[K, V]) Entry() *Entry[K, V] { return &s.entry }

// Snapshot copies the record's Entry for a lock-free reader and reports whether the copy is consistent with
// metaWord. ok=false means a recycle or an overwrite raced the copy: reload the meta word and retry, or stop if the
// reload shows a tombstone. Do not touch the snapshot - in particular do not dereference a pointer it carries -
// unless ok.
//
// An odd generation is SetValue's write-in-progress marker: rejecting it stops a reader that both starts and
// validates inside one write window from accepting a torn value. Settled records are always even.
func (s *State[K, V]) Snapshot(metaWord uint64) (Entry[K, V], bool) {
	entry := s.entry
	return entry, (s.meta.Load()^metaWord)&AliveGenMask == 0 && metaWord&1 == 0
}

// SetValue overwrites the entry's value in place (the key stays). Called under the shard mutex.
func (s *State[K, V]) SetValue(value V) {
	if unsafe.Sizeof(value) > 8 { // no atomic store this wide: fence the write with an odd generation
		s.bumpGen()
		s.entry.Value = value
		s.bumpGen()
		return
	}
	s.entry.Value = value
}

// bumpGen advances the generation in place, preserving every other bit. Only lock-free touches race it, and a lost
// chance bit is harmless.
func (s *State[K, V]) bumpGen() {
	metaWord := s.meta.Load()
	s.meta.Store(metaWord&^GenMask | uint64(uint32(metaWord)+1))
}

// setEntry installs the pair into a claimed record. Called under the shard mutex, strictly before the meta publish.
func (s *State[K, V]) setEntry(entry Entry[K, V]) { s.entry = entry }

// clearEntry zeroes a released record's entry so its key and value do not outlive the item. Called under the shard
// mutex on a record that is already a tombstone, so a stale snapshot of the mid-zero state fails validation.
func (s *State[K, V]) clearEntry() { s.entry = Entry[K, V]{} }
