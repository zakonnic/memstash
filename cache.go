// Package memstash is an ultra-fast two-level cache.
//
// The first level is sharded: each shard owns a compact open-addressing slot table (8 bytes per slot, read lock-free),
// a pool of fixed 16-byte state records that live by value inside chunks and are reused without allocations, and an
// eviction policy (Clock or S3-FIFO) running on chunked FIFO queues of 8-byte nodes. An item's key and value sit in
// one immutable Entry box swapped atomically on overwrite. A memory hit costs one hash, one slot load and two atomic
// record reads - no locks and no allocations. The second level is any adapter that implements L2Cache.
package memstash

import (
	"context"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/puzpuzpuz/xsync/v3"
	"github.com/zakonnic/memstash/internal/eviction"
	"github.com/zakonnic/memstash/internal/itemstate"
)

// LoaderFunc loads a value when both levels miss.
type LoaderFunc[K comparable, V any] func(ctx context.Context, key K) (V, error)

// BatchLoaderFunc loads several values at once when both levels miss. A key omitted from the returned List is
// treated as "not found" rather than an error.
type BatchLoaderFunc[K comparable, V any] func(ctx context.Context, keys []K) (List[K, V], error)

// TickInterval is how often the coarse current time used for TTL is refreshed.
const (
	TickInterval          = time.Second
	DefaultMemoryCapacity = 20_000
)

// shard is an independent segment of the first level: its slot table, record pool and eviction state. A key is
// always served by the same shard (by hash), so all mutations for that key are serialized by its shard mutex - this
// is the core table <-> pool consistency invariant. Readers never take the mutex: they probe the atomically published
// table and verify candidates against the records themselves.
type shard[K comparable, V any] struct {
	mu        sync.Mutex
	table     atomic.Pointer[slotTable] // current slot table; replaced wholesale on growth/purge
	policy    eviction.Policy[K, V]
	pool      itemstate.Pool[K, V]
	weight    atomic.Int64
	cap       int64
	live      int // slots of alive items; guarded by mu
	dirty     int // tombstoned slots, reusable by inserts and purged on rebuild; guarded by mu
	deadCount int // tombstones queued by Delete / lazy TTL removal, not yet reclaimed; guarded by mu

	_ [64]byte // spreads shards across cache lines
}

// Cache is a two-level cache.
type Cache[K comparable, V any] struct {
	registry *itemstate.Registry[K, V] // resolves the pool indices kept in table slots; shared by all shard pools
	costFunc func(key K, value V) uint32

	shards    []shard[K, V]
	shardMask uint32
	seed      maphash.Seed

	// Coarse clock for cheap TTL checks: nowOff is the number of seconds since epoch, refreshed by a background ticker
	// (started only when TTL > 0).
	epoch  time.Time
	nowOff atomic.Uint32
	ttlSec uint32
	ttl    time.Duration

	l2Cache       L2Cache[K, V]
	l2WritePolicy WritePolicy // always WriteDisabled when l2Cache not set
	onL2Error     func(key K, err error)
	writeCh       chan l2Write[K, V]

	flights *xsync.MapOf[K, *flightCall[V]]

	stop      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// New creates a cache configured by the options. The returned cache must be closed with Close if a TTL is set
// or WriteBack is used (by default, but only when L2 Cache is used - background goroutines are stopped); otherwise Close is optional.
func New[K comparable, V any](opts ...Option) (*Cache[K, V], error) {
	cfg, err := buildConfig[K, V](opts)
	if err != nil {
		return nil, err
	}

	return NewWithConfig(cfg)
}

// NewWithConfig creates a cache from an assembled Config (New builds one from the options and delegates here).
// MemoryCapacity 0 falls back to DefaultMemoryCapacity unless CostFunc is set - a weighted cache must size its
// capacity explicitly.
func NewWithConfig[K comparable, V any](cfg *Config[K, V]) (*Cache[K, V], error) {
	if cfg == nil {
		cfg = &Config[K, V]{}
	}
	if cfg.MemoryCapacity < 0 {
		return nil, ErrBadCapacity
	}
	if cfg.MemoryCapacity == 0 {
		if cfg.CostFunc != nil { // must set capacity explicitly - protection from misconfiguration
			return nil, ErrBadCapacity
		}
		cfg.MemoryCapacity = DefaultMemoryCapacity
	}
	// Cache can hold up to MaxRecords, but with CostFunc MemoryCapacity can be bigger than number of records.
	if cfg.CostFunc == nil && cfg.MemoryCapacity > itemstate.MaxRecords {
		return nil, ErrCapacityTooLarge
	}
	if cfg.Policy != PolicyClock && cfg.Policy != PolicyS3FIFO {
		return nil, ErrUnknownPolicy
	}
	if cfg.TTL < 0 {
		return nil, ErrBadTTL
	}

	numShards := cfg.shardCount()
	c := &Cache[K, V]{
		registry:      &itemstate.Registry[K, V]{},
		costFunc:      cfg.CostFunc,
		shards:        make([]shard[K, V], numShards),
		shardMask:     uint32(numShards - 1),
		seed:          maphash.MakeSeed(),
		epoch:         time.Now(),
		ttl:           cfg.TTL,
		l2Cache:       cfg.L2Cache,
		l2WritePolicy: cfg.WritePolicy,
		onL2Error:     cfg.OnL2Error,
		flights:       xsync.NewMapOf[K, *flightCall[V]](),
		stop:          make(chan struct{}),
	}
	if c.l2Cache == nil {
		c.l2WritePolicy = WriteDisabled
	}

	baseCap, remainder := cfg.MemoryCapacity/int64(numShards), cfg.MemoryCapacity%int64(numShards)
	ghostPerShard := max(cfg.ghostSize()/numShards, 1)
	for i := range c.shards {
		sh := &c.shards[i]
		sh.cap = baseCap
		if int64(i) < remainder {
			sh.cap++ // spread the capacity remainder over the first shards
		}
		sh.pool = itemstate.NewPool(c.registry)
		sh.table.Store(newSlotTable(minTableSlots))
		switch cfg.Policy {
		case PolicyS3FIFO:
			sh.policy = eviction.NewS3FIFO(&sh.pool, sh.cap, ghostPerShard)
		case PolicyClock:
			sh.policy = eviction.NewClockPolicy(&sh.pool)
		}
	}

	if cfg.TTL > 0 {
		c.ttlSec = uint32(min(cfg.TTL/time.Second, itemstate.ExpireMax))
		if c.ttlSec == 0 {
			c.ttlSec = 1 // TTL resolution is one second, so use at least one
		}
		c.wg.Add(1)
		go c.clockLoop()
	}

	if c.l2WritePolicy == WriteBack {
		c.writeCh = make(chan l2Write[K, V], cfg.writeBackBuffer())
		c.wg.Add(1)
		go c.writeBackLoop()
	}
	return c, nil
}

// Close stops the background goroutines and waits for the write-back buffer to drain. Repeated calls are safe.
// A Set that starts strictly after Close returns still reaches L2 (synchronously); a Set racing with Close may
// lose its asynchronous write.
func (c *Cache[K, V]) Close() {
	c.closeOnce.Do(func() {
		close(c.stop)
		c.wg.Wait()
		if c.writeCh != nil {
			// Catch writes that slipped into the buffer while the worker was shutting down.
			for {
				select {
				case write := <-c.writeCh:
					c.flushWrite(write)
				default:
					return
				}
			}
		}
	})
}

// Wait blocks until every asynchronous write-back write enqueued before the call has been handed to L2. Unlike Close
// it is a checkpoint, not a shutdown: the cache keeps serving reads and writes while (and after) Wait waits, and it
// is safe to call from any goroutine - for example one that wants to verify the L2 state produced by earlier Sets.
//
// Implemented as a flush marker pushed through the worker's FIFO buffer, so it costs the write path nothing. With
// WriteThrough or WriteDisabled there is nothing to wait for and Wait returns immediately; when the cache is being
// closed Wait also returns immediately - Close performs the final drain itself.
func (c *Cache[K, V]) Wait() {
	if c.writeCh == nil {
		return
	}
	flushed := make(chan struct{})
	select {
	case c.writeCh <- l2Write[K, V]{flush: flushed}:
	case <-c.stop:
		return
	}
	select {
	case <-flushed:
	case <-c.stop:
	}
}

// shardAndHash returns the shard that owns the key together with the key's hash: the hash's low bits pick the shard,
// its high bits seed the probe position within the shard's slot table (see slotHome), so one hash serves both.
func (c *Cache[K, V]) shardAndHash(key K) (*shard[K, V], uint64) {
	h := maphash.Comparable(c.seed, key)
	return &c.shards[uint32(h)&c.shardMask], h
}

// Get returns the value from memory, or - on a miss - from L2 (if configured), promoting the found value into memory. A
// memory hit is a lock-free, allocation-free path.
func (c *Cache[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	if value, ok := c.getMemory(key); ok {
		return value, true, nil
	}
	if c.l2Cache == nil {
		var zero V
		return zero, false, nil
	}
	value, ok, err := c.l2Cache.Get(ctx, key)
	if err != nil || !ok {
		var zero V
		return zero, false, err
	}
	c.setMemory(key, value)
	return value, true, nil
}

// GetFromMemory reads the first level only: the fastest possible path, without a context, L2, or errors.
func (c *Cache[K, V]) GetFromMemory(key K) (V, bool) {
	return c.getMemory(key)
}

// getMemory is the lock-free memory-hit path: probe the shard's slot table, verify the candidate record is alive and
// really belongs to this key (an exact comparison against the record's immutable Entry box - the tag in the slot is
// only a prefilter), then hand out the box's value.
//
// Every interleaving with a concurrent writer resolves safely: the box was published before the record's meta went
// live, so an alive meta implies the box is in place; a record recycled under a stale slot carries a different key
// and fails the comparison; an overwrite swaps the whole box, so the reader sees either the old or the new value,
// never a mix.
func (c *Cache[K, V]) getMemory(key K) (V, bool) {
	sh, h := c.shardAndHash(key)
	t := sh.table.Load()
	tag := tagOf(h)
	for probe := slotHome(h, t.mask); ; probe = (probe + 1) & t.mask {
		packed := t.slots[probe].Load()
		slotTag := uint32(packed >> 32)
		if slotTag == emptyTag {
			break // end of the probe chain: not present
		}
		if slotTag != tag { // covers tombstones too - tombTag never matches a key tag
			continue
		}
		state := c.registry.At(uint32(packed))
		metaWord := state.Load()
		if metaWord&itemstate.Dead != 0 {
			continue // the slot outlived its item; a rebuild will purge it
		}
		entry := state.Entry()
		if entry == nil || entry.Key != key {
			continue // tag collision or a recycled record - not our key
		}
		expireOff := uint32((metaWord & itemstate.ExpireMask) >> itemstate.ExpireShift)
		if expireOff == 0 || expireOff > c.nowOff.Load() {
			state.TouchWith(metaWord)
			return entry.Value, true
		}
		// TTL has elapsed - drop the item lazily instead of waiting for the eviction queue to reach it.
		c.dropExpired(sh, h, key, uint32(packed))
		break
	}
	var zero V
	return zero, false
}

// Set stores the value in memory and in L2 according to WritePolicy. An error can come only from a synchronous L2
// write.
func (c *Cache[K, V]) Set(ctx context.Context, key K, value V) error {
	c.setMemory(key, value)
	if c.l2WritePolicy == WriteDisabled {
		return nil
	}
	if c.l2WritePolicy == WriteThrough {
		return c.l2Cache.Set(ctx, key, value, c.ttl)
	}
	c.enqueueWriteBack(key, value)
	return nil
}

// setMemory puts the value into the first level.
//
// Overwriting an existing key swaps the record's immutable Entry box in place - the record, its queue node and its
// table slot stay put; a new key gets a record from the pool (freelist or chunk) and a slot in the table. The only
// unavoidable allocation is the Entry box itself.
func (c *Cache[K, V]) setMemory(key K, value V) {
	weight := c.rawCost(key, value)
	sh, h := c.shardAndHash(key)
	if weight > sh.cap {
		// The item plainly does not fit - do not let it wreck the cache.
		// An older value stored under the same key must not keep serving reads: drop it.
		sh.mu.Lock()
		c.deleteLocked(sh, h, key)
		sh.mu.Unlock()
		return
	}

	sh.mu.Lock()
	t := sh.table.Load()
	tag := tagOf(h)
	weightDelta := weight
	reuseAt := -1
	for probe := slotHome(h, t.mask); ; probe = (probe + 1) & t.mask {
		packed := t.slots[probe].Load()
		slotTag := uint32(packed >> 32)
		if slotTag == emptyTag {
			// New key: claim a record (its Entry box is published before the meta word goes live) and link a slot.
			_, _, idx := sh.pool.Claim(key, value, c.expireOffset())
			if reuseAt >= 0 {
				probe = uint32(reuseAt)
				sh.dirty--
			}
			t.slots[probe].Store(packSlot(tag, idx))
			sh.live++
			sh.policy.Add(itemstate.QNode{Idx: idx, Cost: uint32(weight)})
			c.maybeRebuild(sh, t)
			break
		}
		if slotTag == tombTag {
			if reuseAt < 0 {
				reuseAt = int(probe)
			}
			continue
		}
		if slotTag != tag {
			continue
		}
		state := c.registry.At(uint32(packed))
		if state.Load()&itemstate.Dead != 0 {
			continue
		}
		entry := state.Entry()
		if entry.Key != key {
			continue
		}
		// Overwrite in place: swap the box; the record, its queue node and its slot stay the same.
		weightDelta = weight - c.rawCost(key, entry.Value)
		state.SetEntry(&itemstate.Entry[K, V]{Key: key, Value: value})
		if c.ttlSec != 0 {
			c.refreshExpire(state)
		}
		break
	}
	if sh.weight.Add(weightDelta) > sh.cap {
		c.evictShard(sh)
	}
	sh.mu.Unlock()
}

// maybeRebuild replaces the shard's slot table when it runs too full (at 3/4 occupancy, tombstones included): grown
// twofold when live items genuinely need the space, rebuilt at the same size when tombstones are the bulk - either
// way tombstones are purged. Live slots are re-derived from the policy's queues, which hold exactly one node per
// claimed record. Readers keep probing the superseded table until they pick up the new pointer; the records they
// resolve stay valid either way. Called under the shard mutex.
func (c *Cache[K, V]) maybeRebuild(sh *shard[K, V], t *slotTable) {
	if (sh.live+sh.dirty)*4 < len(t.slots)*3 {
		return
	}
	newSize := len(t.slots)
	if sh.live*2 >= newSize {
		newSize *= 2
	}
	nt := newSlotTable(newSize)
	live := 0
	sh.policy.Range(func(node itemstate.QNode) {
		state := sh.pool.At(node.Idx)
		if state.Load()&itemstate.Dead != 0 {
			return
		}
		hh := maphash.Comparable(c.seed, state.Entry().Key)
		for probe := slotHome(hh, nt.mask); ; probe = (probe + 1) & nt.mask {
			if nt.slots[probe].Load() == 0 {
				nt.slots[probe].Store(packSlot(tagOf(hh), node.Idx))
				break
			}
		}
		live++
	})
	sh.live, sh.dirty = live, 0
	sh.table.Store(nt)
}

// refreshExpire extends the item's TTL while preserving its generation and reference counter. A race with a
// concurrent touch may lose one second chance - that is harmless.
func (c *Cache[K, V]) refreshExpire(state *itemstate.State[K, V]) {
	state.RefreshExpire(c.expireOffset())
}

// evictShard evicts items from the shard while its weight exceeds the capacity. Called under the shard mutex.
func (c *Cache[K, V]) evictShard(sh *shard[K, V]) {
	nowOff := c.nowOff.Load()
	for sh.weight.Load() > sh.cap {
		victimIdx, ok := sh.policy.Evict(nowOff)
		if !ok {
			return
		}
		c.unlink(sh, victimIdx)
		sh.pool.Release(victimIdx)
	}
}

// unlink tombs the table slot that points to exactly this state record and subtracts the item's weight. For items
// that died earlier (Delete, lazy TTL removal) the slot is already tombed and the weight already subtracted - the
// probe finds no slot carrying this index and nothing changes. Called under the shard mutex.
func (c *Cache[K, V]) unlink(sh *shard[K, V], victimIdx uint32) {
	victim := sh.pool.At(victimIdx)
	entry := victim.Entry()
	h := maphash.Comparable(c.seed, entry.Key)
	t := sh.table.Load()
	tag := tagOf(h)
	for probe := slotHome(h, t.mask); ; probe = (probe + 1) & t.mask {
		packed := t.slots[probe].Load()
		slotTag := uint32(packed >> 32)
		if slotTag == emptyTag {
			return // no slot for this record: it died earlier and was unlinked then
		}
		if slotTag != tag || uint32(packed) != victimIdx {
			continue // another key's slot, or the key is already backed by a different record - leave it alone
		}
		t.slots[probe].Store(tombSlot)
		sh.live--
		sh.dirty++
		sh.weight.Add(-c.rawCost(entry.Key, entry.Value))
		return
	}
}

// dropExpired lazily removes a TTL-expired item from the Get path. The state record stays in the queue as a tombstone
// and returns to the pool on the next eviction pass or tombstone sweep.
func (c *Cache[K, V]) dropExpired(sh *shard[K, V], h uint64, key K, idx uint32) {
	sh.mu.Lock()
	slot, foundIdx, state, ok := c.findSlot(sh, h, key)
	// The idx match rejects the races: the key re-claimed a different record, or this record was recycled. A
	// refreshed TTL fails the Expired re-check and the item survives.
	if ok && foundIdx == idx && itemstate.Expired(state.Load(), c.nowOff.Load()) {
		entry := state.Entry()
		state.Kill()
		slot.Store(tombSlot)
		sh.live--
		sh.dirty++
		sh.weight.Add(-c.rawCost(key, entry.Value))
		c.noteDead(sh)
	}
	sh.mu.Unlock()
}

// findSlot probes the shard's current table for the key's live slot: (slot, pool index, record, true) when found.
// Called under the shard mutex.
func (c *Cache[K, V]) findSlot(sh *shard[K, V], h uint64, key K) (*atomic.Uint64, uint32, *itemstate.State[K, V], bool) {
	t := sh.table.Load()
	tag := tagOf(h)
	for probe := slotHome(h, t.mask); ; probe = (probe + 1) & t.mask {
		packed := t.slots[probe].Load()
		slotTag := uint32(packed >> 32)
		if slotTag == emptyTag {
			return nil, 0, nil, false
		}
		if slotTag != tag {
			continue
		}
		state := c.registry.At(uint32(packed))
		if state.Load()&itemstate.Dead != 0 {
			continue
		}
		if entry := state.Entry(); entry.Key == key {
			return &t.slots[probe], uint32(packed), state, true
		}
	}
}

// sweepMinDead is the minimum number of queued tombstones before a sweep is considered: below it the piles are too
// small to matter (one pool chunk per shard) and sweeping would be pure overhead.
const sweepMinDead = 128

// noteDead accounts a tombstone queued by Delete or lazy TTL removal and, when tombstones outnumber live nodes,
// reclaims them in bulk. Called under the shard mutex.
//
// Without this, reclamation would happen only on eviction passes - which never run while the shard stays below its
// capacity, so a delete-heavy workload would grow the pool and the queues without bound. The half-dead trigger makes
// the sweep amortized O(1) per delete: one O(len) pass is paid for by at least len/2 deletions. The counter may
// overestimate (eviction passes reclaim counted tombstones too), which only means a rare sweep that finds little -
// the follow-up reset keeps it exact.
func (c *Cache[K, V]) noteDead(sh *shard[K, V]) {
	sh.deadCount++
	if sh.deadCount >= sweepMinDead && sh.deadCount*2 >= sh.policy.Len() {
		sh.policy.Sweep(func(idx uint32) { sh.pool.Release(idx) })
		sh.deadCount = 0
	}
}

// deleteLocked tombs the key's table slot, kills its state record and subtracts its weight. Called under the shard
// mutex; a missing key is a no-op.
func (c *Cache[K, V]) deleteLocked(sh *shard[K, V], h uint64, key K) {
	if slot, _, state, ok := c.findSlot(sh, h, key); ok {
		entry := state.Entry()
		state.Kill()
		slot.Store(tombSlot)
		sh.live--
		sh.dirty++
		sh.weight.Add(-c.rawCost(key, entry.Value))
		c.noteDead(sh)
	}
}

// Delete removes the key from memory and from L2 (synchronously, unless L2 writes are disabled). The state record
// returns to the pool on the next eviction pass or tombstone sweep.
func (c *Cache[K, V]) Delete(ctx context.Context, key K) error {
	sh, h := c.shardAndHash(key)
	sh.mu.Lock()
	c.deleteLocked(sh, h, key)
	sh.mu.Unlock()

	if c.l2WritePolicy != WriteDisabled {
		return c.l2Cache.Delete(ctx, key)
	}
	return nil
}

// GetOrLoad returns the value, loading it with the load function when both levels miss. Concurrent calls for the same
// key are coalesced (singleflight): load runs once and the rest wait for its result. Errors are not cached.
func (c *Cache[K, V]) GetOrLoad(ctx context.Context, key K, load LoaderFunc[K, V]) (V, error) {
	if load == nil {
		var zero V
		return zero, ErrNilLoader
	}
	if value, ok := c.getMemory(key); ok {
		return value, nil
	}

	call := &flightCall[V]{done: make(chan struct{})}
	if winner, loaded := c.flights.LoadOrStore(key, call); loaded {
		// A flight is already in progress - wait for its result or for the context to be canceled (the owner keeps
		// loading on behalf of everyone else).
		select {
		case <-winner.done:
			return winner.val, winner.err
		case <-ctx.Done():
			var zero V
			return zero, ctx.Err()
		}
	}

	finished := false
	defer func() {
		if finished {
			return
		}
		// The loader panicked (or called runtime.Goexit): resolve the flight so waiters are not stuck forever and the
		// key stays loadable; the panic itself keeps propagating to this caller.
		call.err = ErrLoaderPanic
		c.flights.Delete(key)
		close(call.done)
	}()
	value, err := c.doLoad(ctx, key, load)
	finished = true
	call.val, call.err, call.ok = value, err, err == nil
	c.flights.Delete(key) // before close: new calls will start a fresh flight
	close(call.done)
	return value, err
}

// BatchGet returns the values for keys found in memory or in L2: the memory hits are collected first, the misses go
// to L2 in a single BatchGet and are promoted into memory. A missing key is simply absent from the result. On an L2
// error the memory part gathered so far is returned alongside the error.
func (c *Cache[K, V]) BatchGet(ctx context.Context, keys []K) (List[K, V], error) {
	if c.l2Cache == nil {
		return c.batchGetMemory(keys), nil
	}
	found, missing := c.batchGetMemoryWithMissing(keys)
	if len(missing) == 0 {
		return found, nil
	}
	fromL2, err := c.l2Cache.BatchGet(ctx, missing)
	if err != nil {
		return found, err
	}
	for _, item := range fromL2 {
		c.setMemory(item.Key, item.Value)
		found = append(found, item)
	}
	return found, nil
}

func (c *Cache[K, V]) batchGetMemory(keys []K) List[K, V] {
	found := make(List[K, V], 0, len(keys))
	for _, key := range keys {
		if value, ok := c.getMemory(key); ok {
			found = append(found, KeyVal[K, V]{Key: key, Value: value})
		}
	}
	return found
}

func (c *Cache[K, V]) batchGetMemoryWithMissing(keys []K) (List[K, V], []K) {
	found := make(List[K, V], 0, len(keys))
	missing := make([]K, 0, len(keys))
	for _, key := range keys {
		if value, ok := c.getMemory(key); ok {
			found = append(found, KeyVal[K, V]{Key: key, Value: value})
		} else {
			missing = append(missing, key)
		}
	}
	return found, missing
}

// BatchSet stores all items in memory and forwards them to L2 according to WritePolicy: WriteThrough issues a single
// batch write, WriteBack enqueues the items for the background worker. An error can come only from the synchronous
// batch write.
func (c *Cache[K, V]) BatchSet(ctx context.Context, items List[K, V]) error {
	for _, item := range items {
		c.setMemory(item.Key, item.Value)
	}
	switch c.l2WritePolicy {
	case WriteThrough:
		return c.l2Cache.BatchSet(ctx, items, c.ttl)
	case WriteBack:
		for _, item := range items {
			c.enqueueWriteBack(item.Key, item.Value)
		}
	}
	return nil
}

// BatchGetOrLoad returns the values for keys, resolving the misses with one L2 BatchGet and at most one load call.
// Per-key singleflight is preserved across all GetOrLoad/BatchGetOrLoad calls: keys already being loaded elsewhere
// are joined, the rest are loaded here in a single load(ctx, missing) call. A key omitted by the loader is absent
// from the result (a concurrent GetOrLoad joined on such a key receives the zero value). On an error the resolved
// part is returned alongside it; errors are not cached.
func (c *Cache[K, V]) BatchGetOrLoad(ctx context.Context, keys []K, load BatchLoaderFunc[K, V]) (List[K, V], error) {
	if load == nil {
		return nil, ErrNilLoader
	}
	found, missing := c.batchGetMemoryWithMissing(keys)
	if len(missing) == 0 {
		return found, nil
	}

	joined, loadErr := c.singleflight(ctx, load, &found, missing)

	for _, flight := range joined {
		select {
		case <-flight.call.done:
			if flight.call.err == nil && flight.call.ok {
				found = append(found, KeyVal[K, V]{Key: flight.key, Value: flight.call.val})
			} else if loadErr == nil {
				loadErr = flight.call.err
			}
		case <-ctx.Done():
			return found, ctx.Err()
		}
	}
	return found, loadErr
}

type joinedFlight[K comparable, V any] struct {
	key  K
	call *flightCall[V]
}

func (c *Cache[K, V]) singleflight(ctx context.Context, load BatchLoaderFunc[K, V], found *List[K, V], missing []K) ([]joinedFlight[K, V], error) {
	// Split the misses into flights we own and flights we join.
	var owned []K
	var joined []joinedFlight[K, V]
	ownedCalls := make(map[K]*flightCall[V], len(missing))
	for _, key := range missing {
		call := &flightCall[V]{done: make(chan struct{})}
		if winner, loaded := c.flights.LoadOrStore(key, call); loaded {
			joined = append(joined, joinedFlight[K, V]{key: key, call: winner})
		} else {
			owned = append(owned, key)
			ownedCalls[key] = call
		}
	}

	var loadErr error
	if len(owned) > 0 {
		finished := false
		defer func() {
			if finished {
				return
			}
			// The loader panicked (or called runtime.Goexit): resolve every owned flight so waiters are not stuck
			// forever and the keys stay loadable; the panic itself keeps propagating to this caller.
			for _, key := range owned {
				call := ownedCalls[key]
				call.err = ErrLoaderPanic
				c.flights.Delete(key)
				close(call.done)
			}
		}()
		resolved, err := c.batchLoad(ctx, owned, load)
		loadErr = err

		// Publish every resolved value to its flight.
		for _, item := range resolved {
			call, isOwned := ownedCalls[item.Key]
			if !isOwned {
				continue // the loader returned a key nobody asked for: cached by batchLoad, but not part of this call
			}
			call.val, call.ok = item.Value, true
			*found = append(*found, item)
		}
		finished = true
		// Close every owned flight; the ones left unresolved carry the (possibly nil) error.
		for _, key := range owned {
			call := ownedCalls[key]
			if !call.ok {
				call.err = err // nil when the key is simply not found anywhere
			}
			c.flights.Delete(key) // before close: new calls will start a fresh flight
			close(call.done)
		}
	}

	return joined, loadErr
}

// batchLoad resolves the owned keys: first from L2 in one BatchGet, the rest with one loader call; freshly loaded
// values are stored back according to the write policy (mirroring doLoad). The returned List may be partial when
// err != nil.
func (c *Cache[K, V]) batchLoad(ctx context.Context, keys []K, load BatchLoaderFunc[K, V]) (List[K, V], error) {
	resolved := make(List[K, V], 0, len(keys))
	resolvedKeys := make(map[K]struct{}, len(keys))
	toLoad := keys
	if c.l2Cache != nil {
		fromL2, err := c.l2Cache.BatchGet(ctx, keys)
		if err != nil {
			// Fall back to the loader for everything; report the L2 error via the callback.
			for _, key := range keys {
				c.reportL2Err(key, err)
			}
		}
		for _, item := range fromL2 {
			c.setMemory(item.Key, item.Value)
			resolved = append(resolved, item)
			resolvedKeys[item.Key] = struct{}{}
		}
		if len(resolved) > 0 {
			toLoad = make([]K, 0, len(keys)-len(resolved))
			for _, key := range keys {
				if _, ok := resolvedKeys[key]; !ok {
					toLoad = append(toLoad, key)
				}
			}
		}
	}
	if len(toLoad) == 0 {
		return resolved, nil
	}

	loaded, err := load(ctx, toLoad)
	if err != nil {
		return resolved, err
	}
	for _, item := range loaded {
		c.setMemory(item.Key, item.Value)
		resolved = append(resolved, item)
	}
	switch c.l2WritePolicy {
	case WriteThrough:
		// The values are already in hand - an L2 write error must not fail the read.
		if writeErr := c.l2Cache.BatchSet(ctx, loaded, c.ttl); writeErr != nil {
			for _, item := range loaded {
				c.reportL2Err(item.Key, writeErr)
			}
		}
	case WriteBack:
		for _, item := range loaded {
			c.enqueueWriteBack(item.Key, item.Value)
		}
	}
	return resolved, nil
}

func (c *Cache[K, V]) doLoad(ctx context.Context, key K, load LoaderFunc[K, V]) (V, error) {
	// A parallel flight may have finished while we were registering.
	if value, ok := c.getMemory(key); ok {
		return value, nil
	}
	if c.l2Cache != nil {
		value, ok, err := c.l2Cache.Get(ctx, key)
		switch {
		case err == nil && ok:
			c.setMemory(key, value)
			return value, nil
		case err != nil:
			// Fall back to the loader; report the L2 error via the callback.
			c.reportL2Err(key, err)
		}
	}
	value, err := load(ctx, key)
	if err != nil {
		var zero V
		return zero, err
	}
	c.setMemory(key, value)
	if c.l2WritePolicy != WriteDisabled {
		if c.l2WritePolicy == WriteThrough {
			// The value is already in hand - an L2 write error must not fail the read.
			if writeErr := c.l2Cache.Set(ctx, key, value, c.ttl); writeErr != nil {
				c.reportL2Err(key, writeErr)
			}
		} else {
			c.enqueueWriteBack(key, value)
		}
	}
	return value, nil
}

// Len returns the number of first-level items (including expired ones not yet swept).
func (c *Cache[K, V]) Len() int {
	total := 0
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		total += sh.live
		sh.mu.Unlock()
	}
	return total
}

// Weight returns the current total weight of live first-level items.
func (c *Cache[K, V]) Weight() int64 {
	var total int64
	for i := range c.shards {
		total += c.shards[i].weight.Load()
	}
	return total
}

// xsyncBucketBytes is the fixed size of one xsync.MapOf bucket (the flights map).
const xsyncBucketBytes = 64

// TotalWeight estimates the total memory footprint of the cache's first-level structures: the per-shard slot tables,
// the record pool chunks (via the shared registry), the live items' Entry boxes, the eviction bookkeeping, and the
// fixed parts - the Cache struct itself, the shards slice, the flights map's buckets and the write-back buffer.
// Entry boxes of dead items not yet swept are not counted; the estimate catches up as reclamation runs.
func (c *Cache[K, V]) TotalWeight() int64 {
	entryBytes := int64(unsafe.Sizeof(itemstate.Entry[K, V]{}))
	total := int64(unsafe.Sizeof(*c))
	total += int64(len(c.shards)) * int64(unsafe.Sizeof(shard[K, V]{}))
	total += int64(c.flights.Stats().TotalBuckets) * xsyncBucketBytes
	total += c.registry.Bytes() // every shard pool's chunks, accounted once

	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		total += sh.table.Load().bytes() + int64(sh.live)*entryBytes + sh.pool.Bytes() + sh.policy.Bytes()
		sh.mu.Unlock()
	}

	if c.writeCh != nil {
		total += int64(cap(c.writeCh)) * int64(unsafe.Sizeof(l2Write[K, V]{}))
	}
	return total
}

// --- internals ---

func (c *Cache[K, V]) rawCost(key K, value V) int64 {
	if c.costFunc == nil {
		return 1
	}
	if weight := c.costFunc(key, value); weight > 0 {
		return int64(weight)
	}
	return 1
}

// expireOffset returns the expiration offset for a new item: 0 when TTL is disabled ("never expires").
//
// The +1 compensates for the clock's one-second resolution: nowOff may be up to a tick behind real time, so
// nowOff+ttlSec alone would let an item written just before a tick expire almost immediately. With the correction an
// item lives at least its TTL and at most one extra second - the documented resolution.
func (c *Cache[K, V]) expireOffset() uint32 {
	if c.ttlSec == 0 {
		return 0
	}
	expireOff := c.nowOff.Load() + c.ttlSec + 1
	if expireOff > itemstate.ExpireMax {
		expireOff = itemstate.ExpireMax // effectively "never": clamp instead of overflowing
	}
	return expireOff
}

func (c *Cache[K, V]) reportL2Err(key K, err error) {
	if c.onL2Error != nil {
		c.onL2Error(key, err)
	}
}

// clockLoop refreshes the coarse TTL clock.
func (c *Cache[K, V]) clockLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.nowOff.Store(uint32(time.Since(c.epoch) / time.Second))
		case <-c.stop:
			return
		}
	}
}

// enqueueWriteBack puts a write into the worker's buffer; on overflow or when the cache is closed it writes
// synchronously so no data is lost.
func (c *Cache[K, V]) enqueueWriteBack(key K, value V) {
	syncWrite := func() {
		if err := c.l2Cache.Set(context.Background(), key, value, c.ttl); err != nil {
			c.reportL2Err(key, err)
		}
	}
	select {
	case <-c.stop:
		// The worker is gone (Close was called): a buffered send would park the value in a channel nobody drains.
		syncWrite()
		return
	default:
	}
	select {
	case c.writeCh <- l2Write[K, V]{key: key, value: value}:
	default:
		syncWrite()
	}
}

// flushWrite hands one write-back task to L2; a flush marker releases its Wait checkpoint instead.
func (c *Cache[K, V]) flushWrite(write l2Write[K, V]) {
	if write.flush != nil {
		close(write.flush) // a Wait checkpoint: everything enqueued before it has already been flushed
		return
	}
	if err := c.l2Cache.Set(context.Background(), write.key, write.value, c.ttl); err != nil {
		c.reportL2Err(write.key, err)
	}
}

// writeBackLoop is the background worker for asynchronous L2 writes. On shutdown it flushes everything left in the
// buffer.
func (c *Cache[K, V]) writeBackLoop() {
	defer c.wg.Done()
	for {
		select {
		case write := <-c.writeCh:
			c.flushWrite(write)
		case <-c.stop:
			for {
				select {
				case write := <-c.writeCh:
					c.flushWrite(write)
				default:
					return
				}
			}
		}
	}
}

// flightCall is the state of a single singleflight "flight". val, err and ok are published strictly before
// close(done). ok distinguishes "loaded successfully" from "not found": a batch loader may legitimately omit a key,
// leaving the flight resolved without a value.
type flightCall[V any] struct {
	done chan struct{}
	val  V
	err  error
	ok   bool
}

type KeyVal[K comparable, V any] struct {
	Key   K
	Value V
}

type List[K comparable, V any] []KeyVal[K, V]

func (t List[K, V]) ToMap() map[K]V {
	m := make(map[K]V, len(t))
	for _, item := range t {
		m[item.Key] = item.Value
	}
	return m
}
