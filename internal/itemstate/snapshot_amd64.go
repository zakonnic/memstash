//go:build amd64

package itemstate

// snapshotEntryInto does the unsynchronized copy that SnapshotInto later validates by generation.
// amd64's memory model (TSO) needs no fence between the copy and the validating re-read: it never reorders loads.
//
//go:norace // exempts this one benign race, so -race builds still catch races in the production path.
func (s *State[K, V]) snapshotEntryInto(dst *Entry[K, V]) {
	*dst = s.entry
}
