package itemstore

// QNode is an item as the eviction policy hears of it: its slot index and its weight.
type QNode struct {
	Idx  uint32
	Cost uint32
}

// EvictionQueues is how many eviction queues a table threads through its slots: queue 0, where every new item is
// queued, and queue 1, whose items carry the InMainQueue bit.
const EvictionQueues = 2

// queueLink is a slot's place in its eviction queue. The queues are rings closed by a head each, at the indices from
// the slot count up, so every item has both neighbors and relinking never branches on an end of the queue.
type queueLink struct{ prev, next uint32 }

func (t *FlatHashMap[K, V]) queueLinkAt(i uint32) *queueLink {
	switch {
	case i >= t.size:
		return &t.queueHeads[i-t.size]
	case t.size <= chunkSlots:
		return &t.queueLinks[i]
	}
	return &t.queueLinkChunks[i>>chunkShift][i&chunkMask]
}

func (t *FlatHashMap[K, V]) headOf(queue uint) uint32 { return t.size + uint32(queue) }

// Enqueue queues the item at idx at the back of queue.
func (t *FlatHashMap[K, V]) Enqueue(queue uint, idx uint32) {
	head := t.headOf(queue)
	back := t.queueLinkAt(head).prev
	*t.queueLinkAt(idx) = queueLink{prev: back, next: head}
	t.queueLinkAt(back).next = idx
	t.queueLinkAt(head).prev = idx
}

// Remove takes the item at idx out of its queue. A hand resting on it moves on to the next item.
func (t *FlatHashMap[K, V]) Remove(idx uint32) {
	l := *t.queueLinkAt(idx)
	t.queueLinkAt(l.prev).next = l.next
	t.queueLinkAt(l.next).prev = l.prev
	if t.hand == idx {
		t.hand = l.next
	}
}

// Requeue moves the item at idx to the back of queue, its own or the other one.
func (t *FlatHashMap[K, V]) Requeue(queue uint, idx uint32) {
	t.Remove(idx)
	t.Enqueue(queue, idx)
	if item, main := t.At(idx), queue == 1; (item.Metadata()&InMainQueue != 0) != main {
		item.setMain(main)
	}
}

// Oldest returns the item at the front of queue, false when the queue is empty.
func (t *FlatHashMap[K, V]) Oldest(queue uint) (uint32, bool) {
	idx := t.queueLinkAt(t.headOf(queue)).next
	return idx, idx < t.size
}

// Newer returns the item queued right after idx, false at the back of the queue.
func (t *FlatHashMap[K, V]) Newer(idx uint32) (uint32, bool) {
	next := t.queueLinkAt(idx).next
	return next, next < t.size
}

// Hand returns where a scan over queue 0 stands: the oldest item until the scan starts, and again once it passes the
// back.
func (t *FlatHashMap[K, V]) Hand() (uint32, bool) {
	if t.hand < t.size {
		return t.hand, true
	}
	return t.Oldest(0)
}

// AdvanceHand moves the hand to the next item of queue 0.
func (t *FlatHashMap[K, V]) AdvanceHand() {
	if idx, ok := t.Hand(); ok {
		t.hand = t.queueLinkAt(idx).next
	}
}

// relink gives the queue position of the item at from to the slot it moved to.
func (t *FlatHashMap[K, V]) relink(from, to uint32) {
	l := *t.queueLinkAt(from)
	*t.queueLinkAt(to) = l
	t.queueLinkAt(l.prev).next = to
	t.queueLinkAt(l.next).prev = to
	if t.hand == from {
		t.hand = to
	}
}
