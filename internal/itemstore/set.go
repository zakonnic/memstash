package itemstore

import (
	"hash/maphash"
	"math/bits"
)

var setSeed = maphash.MakeSeed()

const setMinSlots = 8

// SetSlot is one slot of a Set's backing array (for using in NewSetFrom and stay at stack buffer).
type SetSlot[K comparable] struct {
	tag uint64
	key K
}

// Set is a membership test helper, faster than map[key]struct{}. A linear hash map. Not thread-safe.
type Set[K comparable] struct {
	slots []SetSlot[K]
	count int
}

func NewSet[K comparable](capacity int) Set[K] {
	if capacity <= 0 {
		return Set[K]{}
	}
	return Set[K]{slots: make([]SetSlot[K], 1<<bits.Len(uint(2*capacity-1)))}
}

// NewSetFrom backs the Set with buf instead of a fresh allocation, so a stack-allocated array keeps it off the heap.
func NewSetFrom[K comparable](buf []SetSlot[K]) Set[K] {
	n := len(buf)
	if n&(n-1) != 0 { // not a power of two: the mask trick needs one, so take the largest power-of-two prefix
		n = 1 << (bits.Len(uint(n)) - 1)
	}
	return Set[K]{slots: buf[:n]}
}

func (s *Set[K]) Len() int { return s.count }

func (s *Set[K]) Add(key K) {
	if s.count >= len(s.slots)/2 { // also the empty-table case, 0 >= 0
		s.grow()
	}
	s.place(key)
}

func (s *Set[K]) Exists(key K) bool {
	if s.count == 0 { // no key to find, so skip the hash entirely
		return false
	}
	hash := maphash.Comparable(setSeed, key)
	tag := hash | 1
	mask := uint32(len(s.slots) - 1)
	for pos := uint32(hash >> 32); ; pos++ {
		slot := &s.slots[pos&mask]
		if slot.tag == 0 { // nothing is ever deleted, so the key would have landed right here
			return false
		}
		if slot.tag == tag && slot.key == key {
			return true
		}
	}
}

// place drops the key into a table the caller already made room in.
func (s *Set[K]) place(key K) {
	hash := maphash.Comparable(setSeed, key)
	tag := hash | 1
	mask := uint32(len(s.slots) - 1)
	for pos := uint32(hash >> 32); ; pos++ {
		slot := &s.slots[pos&mask]
		if slot.tag == 0 {
			slot.key, slot.tag = key, tag
			s.count++
			return
		}
		if slot.tag == tag && slot.key == key {
			return
		}
	}
}

// grow doubles the table and refills it
func (s *Set[K]) grow() {
	crowded := s.slots
	size := len(crowded) * 2
	if size == 0 {
		size = setMinSlots
	}
	s.slots = make([]SetSlot[K], size)
	s.count = 0
	for i := range crowded {
		if crowded[i].tag != 0 {
			s.place(crowded[i].key)
		}
	}
}
