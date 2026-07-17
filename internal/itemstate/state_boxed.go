//go:build race || !amd64

package itemstate

import "sync/atomic"

// The fallback for builds state_inline.go's seqlock does not hold on: -race flags its benign copy race, and weaker
// memory models may reorder the copy past the meta re-check. An immutable box needs neither, at the price of an
// allocation per write and a second dependent cache line per read.

// State is a cached item's record: an atomic meta word plus an atomic pointer to its immutable Entry box. Lives by
// value in a pool chunk and is reused without allocations; the boxes are allocated per write.
type State[K comparable, V any] struct {
	meta  atomic.Uint64
	entry atomic.Pointer[Entry[K, V]]
}

// Entry returns the record's current key/value pair. Callers must hold the shard mutex; lock-free readers use
// Snapshot instead.
func (s *State[K, V]) Entry() *Entry[K, V] { return s.entry.Load() }

// Snapshot copies the record's Entry for a lock-free reader and reports whether the copy is consistent with
// metaWord. ok=false means a recycle raced the read, or the record is already released.
func (s *State[K, V]) Snapshot(metaWord uint64) (Entry[K, V], bool) {
	box := s.entry.Load()
	if box == nil {
		return Entry[K, V]{}, false
	}
	return *box, (s.meta.Load()^metaWord)&AliveGenMask == 0
}

// SetValue overwrites the entry's value (the key stays) by publishing a fresh box. Called under the shard mutex;
// readers see either the old or the new box, never a mix.
func (s *State[K, V]) SetValue(value V) {
	s.entry.Store(&Entry[K, V]{Key: s.entry.Load().Key, Value: value})
}

// setEntry installs the pair into a claimed record. Called under the shard mutex, strictly before the meta publish.
func (s *State[K, V]) setEntry(entry Entry[K, V]) { s.entry.Store(&entry) }

// clearEntry drops a released record's box so its key and value do not outlive the item. Called under the shard
// mutex.
func (s *State[K, V]) clearEntry() { s.entry.Store(nil) }
