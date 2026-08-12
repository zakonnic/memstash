package memstash

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"
)

// GetBatchMax caps one coalesced read batch: its keys are fetched through the adapter's BatchGet. Only distinct
// keys count against it - callers waiting for the same key share its place.
const GetBatchMax = 128

// batchIndexFrom is where the key->position map starts paying for itself; up to it a scan over keys that fit in a
// couple of cache lines is faster than hashing.
const batchIndexFrom = 8

// The read pool is idle until the first read reaches it, running until Close, and closed for good after that.
const (
	getPoolIdle uint32 = iota
	getPoolRunning
	getPoolClosed
)

// asyncGet is one queued read: a key and the slot its answer belongs in.
type asyncGet[K comparable, V any] struct {
	key  K
	slot *asyncSlot[V]
}

// asyncSlot is where a coalesced read is delivered. The caller and the worker serving it hold one reference each,
// and whoever drops the last one returns the slot to the pool - a caller leaving on ctx must not pull the memory
// out from under a worker still writing to it. done is buffered and drained before reuse, so the worker signals
// with a send that cannot block.
type asyncSlot[V any] struct {
	value V
	err   error
	done  chan struct{}
	refs  atomic.Int32
	found bool
}

// getWorkerPool owns the queue GetBatched hands an L2 read to, the workers draining it into BatchGet calls, and the
// slots those answers come back through.
type getWorkerPool[K comparable, V any] struct {
	state   atomic.Uint32
	queueCh chan asyncGet[K, V] // published by start, read only after state says getQueueCh
	slots   sync.Pool           // *asyncSlot[V], one per read in flight
	mu      sync.Mutex
	size    int
	workers int
}

// getBatch is a single batch. Slots waiting for keys[i] form a chain starting at head[i] and running through next.
type getBatch[K comparable, V any] struct {
	keys  []K // distinct keys
	head  []int32
	next  []int32
	slots []*asyncSlot[V]
	index map[K]int32 // built once the batch passes batchIndexFrom keys, then reused batch after batch
}

// GetBatched is Get whose L2 reads are coalesced into one BatchGet: misses queue up, and a worker fetches
// everything waiting at once. The caller blocks as in Get, but L2 sees batches instead of a request per
// goroutine - and a key wanted by several callers is fetched once.
// Only worth it under heavy load, and when the L2 adapter has no pipelining of its own.
func (c *Cache[K, V]) GetBatched(ctx context.Context, key K) (V, bool, error) {
	if value, ok := c.getMemory(key); ok {
		c.stats.addMemHits(1)
		return value, true, nil
	}
	return c.getBatchedFromL2(ctx, key)
}

// getBatchedFromL2 queues the read and waits for the answer, or reads L2 here when there is no pool to wait on.
func (c *Cache[K, V]) getBatchedFromL2(ctx context.Context, key K) (V, bool, error) {
	var zero V
	if c.l2Cache == nil {
		c.stats.addMemMisses(1)
		return zero, false, nil
	}
	pool := &c.batchedGets
	queueCh := pool.getQueueCh(c)
	if queueCh == nil {
		return c.getFromL2(ctx, key) // closed: nobody left to batch with
	}

	slot := pool.acquire()
	slot.refs.Store(2) // this goroutine, and the worker that will serve it
	select {
	case queueCh <- asyncGet[K, V]{key: key, slot: slot}:
	case <-ctx.Done():
		pool.release(slot, 2) // never handed over, so both references are ours to drop
		return zero, false, ctx.Err()
	case <-c.stop:
		pool.release(slot, 2)
		return c.getFromL2(ctx, key)
	}

	select {
	case <-slot.done:
		value, found, err := slot.value, slot.found, slot.err
		pool.release(slot, 1)
		return value, found, err
	case <-ctx.Done():
		pool.release(slot, 1) // the worker still holds its own reference and frees the slot when it is done
		return zero, false, ctx.Err()
	case <-c.stop:
		pool.release(slot, 1)
		return c.getFromL2(ctx, key) // the workers are gone; read it here instead of waiting for one
	}
}

// getQueueCh returns the queueCh to coalesce through, starting the workers on the first read that gets this far: a cache
// nobody calls GetBatched on runs no extra goroutines. nil means the cache is closed.
func (p *getWorkerPool[K, V]) getQueueCh(c *Cache[K, V]) chan asyncGet[K, V] {
	if p.state.Load() == getPoolRunning {
		return p.queueCh
	}
	return p.start(c)
}

func (p *getWorkerPool[K, V]) start(c *Cache[K, V]) chan asyncGet[K, V] {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch p.state.Load() {
	case getPoolRunning:
		return p.queueCh
	case getPoolClosed:
		return nil
	}
	p.queueCh = make(chan asyncGet[K, V], p.size)
	c.wg.Add(p.workers)
	for range p.workers {
		go c.getBatchLoop(p.queueCh)
	}
	p.state.Store(getPoolRunning)
	return p.queueCh
}

// close stops new callers from queueing; the ones already waiting wake on c.stop and read L2 themselves.
func (p *getWorkerPool[K, V]) close() {
	p.mu.Lock()
	p.state.Store(getPoolClosed)
	p.mu.Unlock()
}

func (p *getWorkerPool[K, V]) acquire() *asyncSlot[V] {
	if slot, ok := p.slots.Get().(*asyncSlot[V]); ok {
		return slot
	}
	return &asyncSlot[V]{done: make(chan struct{}, 1)}
}

// release drops refs references and recycles the slot once the last one is gone.
func (p *getWorkerPool[K, V]) release(slot *asyncSlot[V], refs int32) {
	if slot.refs.Add(-refs) != 0 {
		return
	}
	select {
	case <-slot.done: // an answer nobody took would fire for whoever gets this slot next
	default:
	}
	var zero V
	slot.value, slot.err = zero, nil // a pooled slot must not pin the value it delivered
	p.slots.Put(slot)
}

// settle hands the answer to the waiting caller and drops the worker's reference to the slot.
func (p *getWorkerPool[K, V]) settle(slot *asyncSlot[V], value V, found bool, err error) {
	slot.value, slot.found, slot.err = value, found, err
	select {
	case slot.done <- struct{}{}:
	default: // the caller left on ctx and is not there to take it
	}
	p.release(slot, 1)
}

// queueBytes is what the pool's queueCh costs, or 0 while no read has ever started it.
func (p *getWorkerPool[K, V]) queueBytes() int64 {
	if p.state.Load() == getPoolIdle {
		return 0
	}
	return int64(cap(p.queueCh)) * int64(unsafe.Sizeof(asyncGet[K, V]{}))
}

// getBatchLoop is one worker of the coalescing read pool. On Close it simply leaves: the callers it would have
// served wake on the same signal and read L2 themselves.
func (c *Cache[K, V]) getBatchLoop(queue chan asyncGet[K, V]) {
	defer c.wg.Done()
	defer c.recoverWorker() // resolveBatch recovers per batch, so reaching this means the loop itself broke
	var batch getBatch[K, V]
	for {
		select {
		case req := <-queue:
			c.serveBatch(queue, req, &batch)
		case <-c.stop:
			return
		}
	}
}

// serveBatch resolves one read together with everything already queued behind it.
func (c *Cache[K, V]) serveBatch(queue chan asyncGet[K, V], first asyncGet[K, V], b *getBatch[K, V]) {
	b.reset()
	c.collect(b, first)
drain:
	for len(b.keys) < GetBatchMax {
		select {
		case req := <-queue:
			c.collect(b, req)
		default:
			break drain
		}
	}
	c.resolveBatch(b)
}

// collect files a request into the batch. A key promoted into memory since the request was queued is settled right
// here - a lock-free lookup against a round trip.
func (c *Cache[K, V]) collect(b *getBatch[K, V], req asyncGet[K, V]) {
	if value, ok := c.getMemory(req.key); ok {
		c.stats.addMemHits(1)
		c.batchedGets.settle(req.slot, value, true, nil)
		return
	}
	b.add(req)
}

// resolveBatch asks L2 for the batch and hands every answer to the slots waiting for it.
func (c *Cache[K, V]) resolveBatch(b *getBatch[K, V]) {
	defer c.recoverBatch(b)
	switch len(b.keys) {
	case 0:
		return // memory had grown everything this batch was going to ask for
	case 1:
		value, found, err := c.l2Cache.Get(context.Background(), b.keys[0])
		if found {
			c.stats.addL2Hits(c.settleHit(b, 0, value))
			return
		}
		c.stats.addL2Misses(c.settleRest(b, err))
		return
	}
	fromL2, err := c.l2Cache.BatchGet(context.Background(), b.keys)
	if err != nil {
		c.stats.addL2Misses(c.settleRest(b, err))
		return
	}
	hits := int64(0)
	for _, item := range fromL2 {
		// Skip a key we did not ask for, and one the adapter answered twice.
		if at, ok := b.lookup(item.Key); ok && b.head[at] >= 0 {
			hits += c.settleHit(b, at, item.Value)
		}
	}
	c.stats.addL2Hits(hits)
	c.stats.addL2Misses(c.settleRest(b, nil))
}

// recoverBatch is recoverWorker for a coalescing read batch: an adapter that panicked would otherwise leave its
// callers waiting forever, so they are woken with the panic as their error. Must be deferred directly.
func (c *Cache[K, V]) recoverBatch(b *getBatch[K, V]) {
	r := recover()
	if r == nil {
		return
	}
	c.settleRest(b, fmt.Errorf("%w: %v", ErrPanic, r))
	c.notifyPanic(r, true)
}

// settleHit settles a key L2 answered for. The value lands in memory first, so a caller still queued behind this
// batch finds it there instead of asking again.
func (c *Cache[K, V]) settleHit(b *getBatch[K, V], at int32, value V) int64 {
	c.setMemory(b.keys[at], value, c.expireOffset())
	return c.settleKey(b, at, value, true, nil)
}

// settleKey hands one answer to every slot waiting for the key at position at and marks the key served. The count
// is what the counters need: one read per caller, not per distinct key.
func (c *Cache[K, V]) settleKey(b *getBatch[K, V], at int32, value V, found bool, err error) int64 {
	waiting := int64(0)
	for slot := b.head[at]; slot >= 0; slot = b.next[slot] {
		c.batchedGets.settle(b.slots[slot], value, found, err)
		waiting++
	}
	b.head[at] = -1
	return waiting
}

// settleRest settles the keys L2 did not answer for: a plain miss, or the error that ended the batch. Served keys
// are already marked, so a second call after a panic only picks up what is left.
func (c *Cache[K, V]) settleRest(b *getBatch[K, V], err error) int64 {
	var zero V
	waiting := int64(0)
	for at := range b.head {
		if b.head[at] >= 0 {
			waiting += c.settleKey(b, int32(at), zero, false, err)
		}
	}
	return waiting
}

// add appends the request, deduplicating keys: several callers waiting for the same key cost one entry in the L2
// request and one more link in its chain.
func (b *getBatch[K, V]) add(req asyncGet[K, V]) {
	at, known := b.lookup(req.key)
	slot := int32(len(b.slots))
	b.slots = append(b.slots, req.slot)
	if known {
		b.next = append(b.next, b.head[at])
		b.head[at] = slot
		return
	}
	b.next = append(b.next, -1)
	b.keys = append(b.keys, req.key)
	b.head = append(b.head, slot)
	b.indexKey(req.key)
}

// lookup returns the position of the key among the batch's distinct keys.
func (b *getBatch[K, V]) lookup(key K) (int32, bool) {
	if b.index != nil {
		at, ok := b.index[key]
		return at, ok
	}
	for i := range b.keys {
		if b.keys[i] == key {
			return int32(i), true
		}
	}
	return 0, false
}

// indexKey records the key just appended, building the index on the batch that first outgrows the scan.
func (b *getBatch[K, V]) indexKey(key K) {
	if b.index == nil {
		if len(b.keys) <= batchIndexFrom {
			return
		}
		b.index = make(map[K]int32, GetBatchMax)
		for i := range b.keys[:len(b.keys)-1] {
			b.index[b.keys[i]] = int32(i)
		}
	}
	b.index[key] = int32(len(b.keys) - 1)
}

// reset keeps the scratch slices and the index map and drops only what they point at: a finished batch must not
// pin the slots it settled, nor the keys it read.
func (b *getBatch[K, V]) reset() {
	clear(b.keys)
	clear(b.slots)
	clear(b.index)
	b.keys, b.slots, b.head, b.next = b.keys[:0], b.slots[:0], b.head[:0], b.next[:0]
}
