//go:build amd64

package itemstore

// snapshotEntryInto does the unsynchronized copy that SnapshotInto later validates by generation.
// amd64's memory model (TSO) needs no fence between the copy and the validating re-read: it never reorders loads.
//
//go:norace // exempts this one benign race, so -race builds still catch races in the production path.
func (s *Item[K, V]) snapshotEntryInto(dst *Entry[K, V]) {
	*dst = s.entry
}

// SetScalar overwrites a value that OneStore admits with one plain store: a reader gets the old or the new. TSO keeps
// it behind the stores that built whatever the value points to.
func (s *Item[K, V]) SetScalar(value V) { s.entry.Value = value }

// beginWrite makes the generation odd to signal a write in progress. TSO keeps the value stores after it.
func (s *Item[K, V]) beginWrite() {
	metaWord := s.meta.Load()
	storeMeta(&s.meta, metaWord&^GenMask|(metaWord+1)&GenMask)
}
