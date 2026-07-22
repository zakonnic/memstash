// Package memstash is an ultra-fast two-level cache.
//
// The first level is sharded: each shard owns a compact open-addressing slot table (8 bytes per slot, read
// lock-free), a pool of reusable state records and an eviction policy (Clock or S3-FIFO). An item's key and value
// live inline in its record next to the meta word, so a memory hit takes no locks and allocates nothing. The second
// level is any adapter that implements L2Cache.
package memstash

import (
	"context"
	"hash/maphash"
	"iter"
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

// shard is an independent segment of the first level: a key is always served by the same shard (by hash), and all
// mutations for that key are serialized by the shard mutex. Readers never take it: they probe the atomically
// published table and verify candidates against the records themselves.
type shard[K comparable, V any] struct {
	mu        sync.Mutex
	table     atomic.Pointer[hashSlots] // replaced wholesale on growth/purge
	policy    EvictionPolicy[K, V]
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
	registry *itemstate.Registry[K, V] // resolves pool indices from table slots; shared by all shard pools
	costFunc func(key K, value V) uint32

	shards    []shard[K, V]
	shardMask uint32
	seed      maphash.Seed

	// Coarse clock for cheap TTL checks: nowOff is the number of seconds since epoch, refreshed by a background ticker
	// (started only when TTL > 0).
	epoch        time.Time
	nowOff       atomic.Uint32
	ttlSec       uint32
	ttl          time.Duration
	refreshOnGet bool

	l2Cache           L2Cache[K, V]
	l2WritePolicy     WritePolicy // always WriteDisabled when l2Cache not set
	writeBackBatching WriteBackBatching
	onL2Error         func(key K, err error)
	writeCh           chan l2Write[K, V]

	flights *xsync.MapOf[K, *flightCall[V]]
	stats   Stats

	stop      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// New creates a cache configured by the options. The returned cache must be closed with Close when background
// goroutines run - a TTL is set, or an L2 cache is written with WriteBack (the default write policy); otherwise
// Close is optional.
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
	if cfg.MemoryBudget < 0 {
		return nil, ErrBadBudget
	}
	if cfg.MemoryBudget > 0 {
		if cfg.MemoryCapacity != 0 {
			return nil, ErrBudgetAndCapacity
		}
		if cfg.CostFunc == nil {
			autoCostFunc, err := GetAutoCostFunc[K, V]()
			if err != nil {
				return nil, err
			}
			cfg.CostFunc = autoCostFunc
		}
		// From here on the budget is an ordinary weighted capacity: costs are bytes, the capacity is the byte budget.
		cfg.MemoryCapacity = cfg.MemoryBudget
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
	if cfg.CustomPolicy == nil {
		switch cfg.Policy {
		case PolicyS3FIFO, PolicyClock, PolicyWTinyLFU, PolicySIEVE:
		default:
			return nil, ErrUnknownPolicy
		}
	}
	if cfg.TTL < 0 {
		return nil, ErrBadTTL
	}

	numShards := cfg.shardCount()
	c := &Cache[K, V]{
		registry:          &itemstate.Registry[K, V]{},
		costFunc:          cfg.CostFunc,
		shards:            make([]shard[K, V], numShards),
		shardMask:         uint32(numShards - 1),
		seed:              maphash.MakeSeed(),
		epoch:             time.Now(),
		ttl:               cfg.TTL,
		l2Cache:           cfg.L2Cache,
		l2WritePolicy:     cfg.WritePolicy,
		writeBackBatching: cfg.WriteBackBatching,
		onL2Error:         cfg.OnL2Error,
		flights:           xsync.NewMapOf[K, *flightCall[V]](),
		stats:             newStats(cfg.StatsEnabled),
		stop:              make(chan struct{}),
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
		sh.table.Store(newHashSlots(minTableSlots))
		if cfg.CustomPolicy != nil {
			sh.policy = cfg.CustomPolicy(&sh.pool, sh.cap)
			if sh.policy == nil {
				return nil, ErrNilCustomPolicy
			}
			continue
		}
		switch cfg.Policy {
		case PolicyS3FIFO:
			sh.policy = eviction.NewS3FIFO(&sh.pool, sh.cap, ghostPerShard)
		case PolicyClock:
			sh.policy = eviction.NewClockPolicy(&sh.pool)
		case PolicyWTinyLFU:
			sh.policy = eviction.NewWTinyLFU(&sh.pool, sh.cap, ghostPerShard)
		case PolicySIEVE:
			sh.policy = eviction.NewSieve(&sh.pool)
		}
	}

	if cfg.TTL > 0 {
		c.ttlSec = uint32(min(cfg.TTL/time.Second, itemstate.ExpireMax))
		if c.ttlSec == 0 {
			c.ttlSec = 1 // TTL resolution is one second, so use at least one
		}
		c.refreshOnGet = cfg.RefreshTTLOnGet
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
// it is a checkpoint, not a shutdown: the cache keeps serving traffic. With WriteThrough or WriteDisabled, or while
// the cache is closing (Close drains the buffer itself), it returns immediately.
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

// shardAndHash returns the key's shard and hash: the low bits pick the shard, the high bits seed the table probe,
// so one hash serves both.
func (c *Cache[K, V]) shardAndHash(key K) (*shard[K, V], uint64) {
	keyHash := maphash.Comparable(c.seed, key)
	return &c.shards[uint32(keyHash)&c.shardMask], keyHash
}

// Get returns the value from memory, or - on a miss - from L2 (if configured), promoting the found value into memory. A
// memory hit is a lock-free, allocation-free path.
func (c *Cache[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	if value, ok := c.getMemory(key); ok {
		c.stats.addMemHits(1)
		return value, true, nil
	}
	if c.l2Cache == nil {
		c.stats.addMemMisses(1)
		var zero V
		return zero, false, nil
	}
	value, ok, err := c.l2Cache.Get(ctx, key)
	if err != nil || !ok {
		c.stats.addL2Misses(1)
		var zero V
		return zero, false, err
	}
	c.stats.addL2Hits(1)
	c.setMemory(key, value)
	return value, true, nil
}

// GetFromMemory reads the first level only: the fastest possible path, without a context, L2, or errors.
func (c *Cache[K, V]) GetFromMemory(key K) (V, bool) {
	value, ok := c.getMemory(key)
	if ok {
		c.stats.addMemHits(1)
	} else {
		c.stats.addMemMisses(1)
	}
	return value, ok
}

// getMemory is the lock-free memory-hit path. It locates the index in the linear hash table, then uses the record's
// Entry snapshot to confirm an exact key match and to obtain a value that is never observed half-written.
//
// The body inlines the shardAndHash computation and the snapshot into a single stack slot, avoiding a call frame and
// extra Entry copies. Those overheads would either slow things down directly or clog the works so the CPU can't do
// something else while waiting.
func (c *Cache[K, V]) getMemory(key K) (V, bool) {
	keyHash := maphash.Comparable(c.seed, key)
	sh := &c.shards[uint32(keyHash)&c.shardMask]
	t := sh.table.Load()
	shortHash := shortHashOf(keyHash)
	var entry itemstate.Entry[K, V]
probe: // loop label for hashSlots table operations optimization - couldn't inline, so it lives here.
	for pos := t.home(keyHash); ; pos++ {
		packed := t.slot(pos).Load()
		slotShortHash := slotHash(packed)
		if slotShortHash == emptyShortHash {
			break // end of the probe chain: not present
		}
		if slotShortHash != shortHash { // covers tombstones too - tombShortHash never matches a key short hash
			continue
		}
		state := c.registry.At(slotIdx(packed))
		for {
			metaWord := state.Load()
			if metaWord&itemstate.Dead != 0 {
				continue probe
			}
			if !state.SnapshotInto(&entry, metaWord) {
				continue // the copy raced a recycle or an overwrite: retry against the fresh meta word
			}
			if entry.Key != key {
				continue probe // short hash collision or a recycled record
			}
			expireOff := uint32((metaWord & itemstate.ExpireMask) >> itemstate.ExpireShift)
			nowOff := c.nowOff.Load()
			if expireOff == 0 || expireOff > nowOff {
				if c.refreshOnGet {
					if newOff := c.expireOffsetAt(nowOff); newOff != expireOff && state.TouchAndRefreshExpire(metaWord, newOff) {
						return entry.Value, true
					}
				}
				state.TouchWith(metaWord)
				return entry.Value, true
			}
			// TTL has elapsed - drop the item lazily instead of waiting for the eviction queue to reach it.
			c.dropExpired(sh, keyHash, key, slotIdx(packed))
			break probe
		}
	}
	var zero V
	return zero, false
}

// Set stores the value in memory and in L2 according to WritePolicy. An error can come only from a synchronous L2
// write.
func (c *Cache[K, V]) Set(ctx context.Context, key K, value V) error {
	c.setMemory(key, value)
	c.stats.addSets(1)
	if c.l2WritePolicy == WriteDisabled {
		return nil
	}
	if c.l2WritePolicy == WriteThrough {
		return c.l2Cache.Set(ctx, key, value, c.ttl)
	}
	c.enqueueWriteBack(key, value)
	return nil
}

// setMemory puts the value into the first level. An overwrite stores into the record in place (the record, its queue
// node and its slot stay put); a new key claims a record and a slot. Neither path allocates.
func (c *Cache[K, V]) setMemory(key K, value V) {
	weight := c.rawCost(key, value)
	sh, keyHash := c.shardAndHash(key)
	if weight > sh.cap {
		// Does not fit at all; drop the older value too so it stops serving reads.
		sh.mu.Lock()
		c.deleteLocked(sh, keyHash, key)
		sh.mu.Unlock()
		return
	}

	sh.mu.Lock()
	t := sh.table.Load()
	shortHash := shortHashOf(keyHash)
	weightDelta := weight
	reuse, hasReuse := uint32(0), false
	for pos := t.home(keyHash); ; pos++ {
		packed := t.slot(pos).Load()
		slotShortHash := slotHash(packed)
		if slotShortHash == emptyShortHash {
			// New key: claim a record and link a slot (reusing the first tombstone seen on the way).
			_, _, idx := sh.pool.Claim(key, value, c.expireOffset())
			if hasReuse {
				pos = reuse
				sh.dirty--
			}
			t.slot(pos).Store(packSlot(shortHash, idx))
			sh.live++
			sh.policy.Add(itemstate.QNode{Idx: idx, Cost: uint32(weight)})
			c.maybeRebuild(sh, t)
			break
		}
		if slotShortHash == tombShortHash {
			if !hasReuse {
				reuse, hasReuse = pos, true
			}
			continue
		}
		if slotShortHash != shortHash {
			continue
		}
		state := c.registry.At(slotIdx(packed))
		if state.Load()&itemstate.Dead != 0 {
			continue
		}
		entry := state.Entry()
		if entry.Key != key {
			continue
		}
		// Overwrite in place; the old value is read for the weight delta before the store lands.
		weightDelta = weight - c.rawCost(key, entry.Value)
		state.SetValue(value)
		if c.ttlSec != 0 {
			state.RefreshExpire(c.expireOffset())
		}
		break
	}
	if sh.weight.Add(weightDelta) > sh.cap {
		c.evictShard(sh)
	}
	sh.mu.Unlock()
}

// maybeRebuild replaces the shard's table when it passes 3/4 occupancy (tombstones included): doubled when live
// items need the space, same-size otherwise - either way tombstones are purged. Live slots are re-derived from the
// policy's queues (exactly one node per record); readers finish on the superseded table just fine. Called under the
// shard mutex.
func (c *Cache[K, V]) maybeRebuild(sh *shard[K, V], t *hashSlots) {
	if (sh.live+sh.dirty)*4 < t.slotCount()*3 {
		return
	}
	newSize := t.slotCount()
	if sh.live*2 >= newSize {
		newSize *= 2
	}
	fresh := newHashSlots(newSize)
	live := 0
	sh.policy.Range(func(node itemstate.QNode) {
		state := sh.pool.At(node.Idx)
		if state.Load()&itemstate.Dead != 0 {
			return
		}
		fresh.insertFresh(maphash.Comparable(c.seed, state.Entry().Key), node.Idx)
		live++
	})
	sh.live, sh.dirty = live, 0
	sh.table.Store(fresh)
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

// unlink tombs the slot pointing to exactly this record and subtracts its weight. For items that died earlier
// (Delete, lazy TTL removal) both already happened - the probe finds nothing and nothing changes. Called under the
// shard mutex.
func (c *Cache[K, V]) unlink(sh *shard[K, V], victimIdx uint32) {
	entry := sh.pool.At(victimIdx).Entry()
	keyHash := maphash.Comparable(c.seed, entry.Key)
	if sh.table.Load().unlink(keyHash, victimIdx) {
		sh.live--
		sh.dirty++
		sh.weight.Add(-c.rawCost(entry.Key, entry.Value))
	}
}

// dropExpired lazily removes a TTL-expired item found by the Get path. The record stays in the queue as a tombstone
// until the next eviction pass or sweep.
func (c *Cache[K, V]) dropExpired(sh *shard[K, V], h uint64, key K, idx uint32) {
	sh.mu.Lock()
	slot, foundIdx, state, ok := c.findSlot(sh, h, key)
	// The idx match and the Expired re-check reject the races: a re-claimed record or a refreshed TTL means the item
	// survives.
	if ok && foundIdx == idx && itemstate.Expired(state.Load(), c.nowOff.Load()) {
		c.killAt(sh, slot, state, key)
	}
	sh.mu.Unlock()
}

// killAt tombs the found slot, kills its record and subtracts the item's weight. Called under the shard mutex.
func (c *Cache[K, V]) killAt(sh *shard[K, V], slot *atomic.Uint64, state *itemstate.State[K, V], key K) {
	entry := state.Entry()
	state.Kill()
	slot.Store(tombSlot)
	sh.live--
	sh.dirty++
	sh.weight.Add(-c.rawCost(key, entry.Value))
	c.noteDead(sh)
}

// findSlot probes the shard's current table for the key's live slot: (slot, pool index, record, true) when found.
// Called under the shard mutex.
func (c *Cache[K, V]) findSlot(sh *shard[K, V], keyHash uint64, key K) (*atomic.Uint64, uint32, *itemstate.State[K, V], bool) {
	t := sh.table.Load()
	shortHash := shortHashOf(keyHash)
	for pos := t.home(keyHash); ; pos++ {
		packed := t.slot(pos).Load()
		if slotHash(packed) == emptyShortHash {
			return nil, 0, nil, false
		}
		if slotHash(packed) != shortHash {
			continue
		}
		state := c.registry.At(slotIdx(packed))
		if state.Load()&itemstate.Dead != 0 {
			continue
		}
		if entry := state.Entry(); entry.Key == key {
			return t.slot(pos), slotIdx(packed), state, true
		}
	}
}

// sweepMinDead is the minimum number of queued tombstones before a sweep is considered; smaller piles are not worth
// the pass.
const sweepMinDead = 128

// noteDead accounts a tombstone queued by Delete or lazy TTL removal and, once tombstones outnumber live nodes,
// reclaims them in bulk. Without this a delete-heavy workload below capacity would grow the queues without bound
// (eviction, the other reclaimer, never runs there); the half-dead trigger keeps the sweep amortized O(1) per
// delete. Called under the shard mutex.
func (c *Cache[K, V]) noteDead(sh *shard[K, V]) {
	sh.deadCount++
	if sh.deadCount >= sweepMinDead && sh.deadCount*2 >= sh.policy.Len() {
		sh.policy.Sweep(func(idx uint32) { sh.pool.Release(idx) })
		sh.deadCount = 0
	}
}

// deleteLocked tombs the key's table slot, kills its state record and subtracts its weight. Called under the shard
// mutex; a missing key is a no-op.
func (c *Cache[K, V]) deleteLocked(sh *shard[K, V], keyHash uint64, key K) {
	if slot, _, state, ok := c.findSlot(sh, keyHash, key); ok {
		c.killAt(sh, slot, state, key)
	}
}

// Delete removes the key from memory and from L2 (synchronously, unless L2 writes are disabled). The state record
// returns to the pool on the next eviction pass or tombstone sweep.
func (c *Cache[K, V]) Delete(ctx context.Context, key K) error {
	sh, keyHash := c.shardAndHash(key)
	sh.mu.Lock()
	c.deleteLocked(sh, keyHash, key)
	sh.mu.Unlock()
	c.stats.addDeletes(1)

	if c.l2WritePolicy != WriteDisabled {
		return c.l2Cache.Delete(ctx, key)
	}
	return nil
}

// BatchDelete removes the keys from memory and forwards the deletions to L2 according to WritePolicy: WriteThrough
// issues one synchronous BatchDelete, WriteBack enqueues the deletes for the background worker, which always
// coalesces them into BatchDelete batches regardless of WriteBackBatching. An error can come only from the
// synchronous batch delete.
func (c *Cache[K, V]) BatchDelete(ctx context.Context, keys []K) error {
	for _, key := range keys {
		sh, keyHash := c.shardAndHash(key)
		sh.mu.Lock()
		c.deleteLocked(sh, keyHash, key)
		sh.mu.Unlock()
	}
	c.stats.addDeletes(int64(len(keys)))
	switch c.l2WritePolicy {
	case WriteThrough:
		return c.l2Cache.BatchDelete(ctx, keys)
	case WriteBack:
		for _, key := range keys {
			c.enqueueDelete(key)
		}
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
		c.stats.addMemHits(1)
		return value, nil
	}

	call := &flightCall[V]{done: make(chan struct{})}
	if winner, loaded := c.flights.LoadOrStore(key, call); loaded {
		// A flight is already in progress - wait for its result or for the context to be canceled (the owner keeps
		// loading on behalf of everyone else). The key was not in memory when this call looked, and this call itself
		// never reaches L2 - the owner does: a memory miss.
		c.stats.addMemMisses(1)
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
		found := c.batchGetMemory(keys)
		c.stats.addMemHits(int64(len(found)))
		c.stats.addMemMisses(int64(len(keys) - len(found)))
		return found, nil
	}
	found, missing := c.batchGetMemoryWithMissing(keys)
	c.stats.addMemHits(int64(len(found)))
	if len(missing) == 0 {
		return found, nil
	}
	fromL2, err := c.l2Cache.BatchGet(ctx, missing)
	if err != nil {
		c.stats.addL2Misses(int64(len(missing)))
		return found, err
	}
	c.stats.addL2Hits(int64(len(fromL2)))
	c.stats.addL2Misses(int64(len(missing) - len(fromL2)))
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
	c.stats.addSets(int64(len(items)))
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
	c.stats.addMemHits(int64(len(found)))
	if len(missing) == 0 {
		return found, nil
	}

	joined, loadErr := c.singleflight(ctx, load, &found, missing)
	// Joined keys missed memory and go no further here - their owner does the L2 lookup (the owned keys are counted
	// by batchLoad).
	c.stats.addMemMisses(int64(len(joined)))

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
			// Loader panic/Goexit: resolve the owned flights so waiters are not stuck (same as in GetOrLoad).
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
		c.stats.addL2Hits(int64(len(resolved)))
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
	if c.l2Cache != nil {
		c.stats.addL2Misses(int64(len(toLoad)))
	} else {
		c.stats.addMemMisses(int64(len(toLoad)))
	}

	loaded, err := load(ctx, toLoad)
	if err != nil {
		return resolved, err
	}
	for _, item := range loaded {
		c.setMemory(item.Key, item.Value)
		resolved = append(resolved, item)
	}
	c.stats.addSets(int64(len(loaded)))
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
		c.stats.addMemHits(1)
		return value, nil
	}
	if c.l2Cache != nil {
		value, ok, err := c.l2Cache.Get(ctx, key)
		switch {
		case err == nil && ok:
			c.stats.addL2Hits(1)
			c.setMemory(key, value)
			return value, nil
		case err != nil:
			// Fall back to the loader; report the L2 error via the callback.
			c.reportL2Err(key, err)
		}
		c.stats.addL2Misses(1)
	} else {
		c.stats.addMemMisses(1)
	}
	value, err := load(ctx, key)
	if err != nil {
		var zero V
		return zero, err
	}
	c.setMemory(key, value)
	c.stats.addSets(1)
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

// Iterator returns an iterator over all live first-level entries (L2 is not scanned). The walk is lock-free and
// weakly consistent, like sync.Map.Range: entries written or removed while iterating may or may not be seen, but a
// yielded pair is never torn.
func (c *Cache[K, V]) Iterator() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		var entry itemstate.Entry[K, V]
		for i := range c.shards {
			t := c.shards[i].table.Load()
			nowOff := c.nowOff.Load()
			for pos := uint32(0); pos <= t.mask; pos++ {
				packed := t.slot(pos).Load()
				if slotHash(packed) < minKeyShortHash {
					continue // empty or tombstone
				}
				state := c.registry.At(slotIdx(packed))
				metaWord := state.Load()
				if metaWord&itemstate.Dead != 0 || itemstate.Expired(metaWord, nowOff) {
					continue
				}
				if !state.SnapshotInto(&entry, metaWord) {
					continue
				}
				if !yield(entry.Key, entry.Value) {
					return
				}
			}
		}
	}
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

// TotalWeight estimates the total memory footprint of the cache's first-level structures: slot tables, pool chunks
// (records carry their Entry inline), eviction bookkeeping and the fixed parts (the Cache struct, shards, flights
// buckets, write-back buffer).
//
// An Entry is counted at its inline size, so heap data referenced by K or V is not included. When CostFunc measures
// those bytes, TotalWeight() + Weight() gives the full footprint.
func (c *Cache[K, V]) TotalWeight() int64 {
	total := int64(unsafe.Sizeof(*c))
	total += int64(len(c.shards)) * int64(unsafe.Sizeof(shard[K, V]{}))
	total += int64(c.flights.Stats().TotalBuckets) * xsyncBucketBytes
	total += c.registry.Bytes() // all shards' chunks, accounted once

	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		total += sh.table.Load().bytes() + sh.pool.Bytes() + sh.policy.Bytes()
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

// expireOffset returns the expiration offset for a new item: 0 when TTL is disabled ("never expires"). The +1
// compensates for the coarse clock: nowOff may be up to a tick behind real time, and without it an item written just
// before a tick would expire almost immediately. An item lives at least its TTL and at most one extra second.
func (c *Cache[K, V]) expireOffset() uint32 {
	if c.ttlSec == 0 {
		return 0
	}
	return c.expireOffsetAt(c.nowOff.Load())
}

// expireOffsetAt is expireOffset for an already-loaded clock value; only meaningful when TTL is enabled.
func (c *Cache[K, V]) expireOffsetAt(nowOff uint32) uint32 {
	expireOff := nowOff + c.ttlSec + 1
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

// enqueueDelete puts a delete into the worker's buffer;
// deletes synchronously on overflow or when the cache is closed.
func (c *Cache[K, V]) enqueueDelete(key K) {
	syncDelete := func() {
		if err := c.l2Cache.Delete(context.Background(), key); err != nil {
			c.reportL2Err(key, err)
		}
	}
	select {
	case <-c.stop:
		// The worker is gone (Close was called): a buffered send would park the delete in a channel nobody drains.
		syncDelete()
		return
	default:
	}
	select {
	case c.writeCh <- l2Write[K, V]{key: key, del: true}:
	default:
		syncDelete()
	}
}

// WriteBackBatchMax caps one drain batch: the writes are flushed through the adapter's BatchSet.
const WriteBackBatchMax = 128

// flushWrite hands one write-back task to L2, coalescing the tasks already queued behind it into one BatchSet;
func (c *Cache[K, V]) flushWrite(first l2Write[K, V]) {
	for more := true; more; {
		if first.flush != nil {
			close(first.flush) // a Wait checkpoint: everything enqueued before it has already been flushed
			return
		}
		// len(writeCh) is the buffer fill counter the adaptive mode switches on.
		if !first.del && (c.writeBackBatching == BatchingNone ||
			(c.writeBackBatching == BatchingAdaptive && len(c.writeCh) <= cap(c.writeCh)/2)) {
			c.writeBatch(List[K, V]{{Key: first.key, Value: first.value}})
			return
		}
		first, more = c.flushRun(first)
	}
}

// flushRun coalesces the tasks of first's kind queued behind it and delivers them as one batch.
// Returns next to flushWrite to seed the next run.
func (c *Cache[K, V]) flushRun(first l2Write[K, V]) (next l2Write[K, V], more bool) {
	var sets List[K, V]
	var deletes []K
	if first.del {
		deletes = append(deletes, first.key)
	} else {
		sets = append(sets, KeyVal[K, V]{Key: first.key, Value: first.value})
	}
	for len(sets)+len(deletes) < WriteBackBatchMax {
		select {
		case write := <-c.writeCh:
			switch {
			case write.flush != nil:
				c.deliverRun(sets, deletes)
				close(write.flush)
				return next, false
			case write.del != first.del:
				c.deliverRun(sets, deletes)
				return write, true
			case write.del:
				deletes = append(deletes, write.key)
			default:
				sets = append(sets, KeyVal[K, V]{Key: write.key, Value: write.value})
			}
		default:
			c.deliverRun(sets, deletes)
			return next, false
		}
	}
	c.deliverRun(sets, deletes)
	return next, false
}

func (c *Cache[K, V]) deliverRun(sets List[K, V], deletes []K) {
	if len(deletes) > 0 {
		c.deleteBatch(deletes)
		return
	}
	c.writeBatch(sets)
}

// writeBatch delivers drained writes to L2: one Set for a single item, one BatchSet otherwise (duplicate keys
// collapse to the last value there, as FIFO order would). A batch error is reported for every key it covers.
func (c *Cache[K, V]) writeBatch(batch List[K, V]) {
	if len(batch) == 1 {
		if err := c.l2Cache.Set(context.Background(), batch[0].Key, batch[0].Value, c.ttl); err != nil {
			c.reportL2Err(batch[0].Key, err)
		}
		return
	}
	if err := c.l2Cache.BatchSet(context.Background(), batch, c.ttl); err != nil {
		for _, item := range batch {
			c.reportL2Err(item.Key, err)
		}
	}
}

// deleteBatch delivers drained deletes to L2: one Delete for a single key, one BatchDelete otherwise. A batch error
// is reported for every key it covers.
func (c *Cache[K, V]) deleteBatch(keys []K) {
	if len(keys) == 1 {
		if err := c.l2Cache.Delete(context.Background(), keys[0]); err != nil {
			c.reportL2Err(keys[0], err)
		}
		return
	}
	if err := c.l2Cache.BatchDelete(context.Background(), keys); err != nil {
		for _, key := range keys {
			c.reportL2Err(key, err)
		}
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

// flightCall is one singleflight flight: val, err and ok are published strictly before close(done). ok separates
// "loaded" from "not found" - a batch loader may legitimately omit a key.
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
