// Package memstash is an ultra-fast two-level cache.
//
// The first level is sharded: each shard owns an open-addressing table whose slots - the positions of a flat array -
// hold the items themselves, plus an eviction policy. An item's key and value sit inline next to its meta word, so a
// memory hit is one lookup deep, takes no locks and allocates nothing. The second level is any adapter that
// implements L2Cache.
package memstash

import (
	"context"
	"fmt"
	"hash/maphash"
	"iter"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/zakonnic/memstash/internal/eviction"
	"github.com/zakonnic/memstash/internal/itemstore"
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

	// minTableSlots is the initial table size of every shard.
	minTableSlots = 64

	// maxTableSlots is the largest table a shard can address: the whole uint32 slot index space.
	maxTableSlots = int64(1) << 32
)

// shard is an independent segment of the first level: a key is always served by the same shard (by hash), and all
// mutations for that key are serialized by the shard mutex. Readers never take it: they probe the atomically
// published table and verify candidates against the items themselves.
type shard[K comparable, V any] struct {
	mu        sync.Mutex
	items     itemstore.StorageProxy[K, V] // the item table, swapped wholesale on growth/purge
	policy    EvictionPolicy[K, V]
	weight    atomic.Int64
	cap       int64
	live      int // items of alive items; guarded by mu
	dirty     int // tombstoned items, purged on rebuild; guarded by mu
	deadCount int // tombstones queued by Delete / lazy TTL removal, not yet reclaimed; guarded by mu

	_ [64]byte // padding - spreads shards across cache lines
}

// Cache is a two-level cache.
type Cache[K comparable, V any] struct {
	costFunc func(key K, value V) uint32

	shards    []shard[K, V]
	shardMask uint32
	// onDeletion sits with the fields the write path already touches: every removal site tests it, so a cache without
	// a handler pays one predicted branch off a hot cache line.
	onDeletion func(key K, value V, cause DeletionCause)
	seed       maphash.Seed

	// Coarse clock for cheap TTL checks: nowOff is the time since epoch in expireUnit steps, refreshed by a background
	// ticker (started only when TTL > 0). Offsets are 18-bit values on a wrapping scale, so the unit adapts to the
	// configured TTL: 1s up to ~36h TTLs, growing just enough beyond that - the granularity stays under 0.001% of TTL.
	epoch        time.Time
	nowOff       atomic.Uint32
	ttlOff       uint32
	expireUnit   time.Duration // wall time per offset step: ceil(TTL/ExpireMax), the smallest unit that fits the TTL
	ttl          time.Duration
	refreshOnGet bool

	l2Cache           L2Cache[K, V]
	l2WritePolicy     WritePolicy // always WriteDisabled when l2Cache not set
	writeBackBatching WriteBackBatching
	onL2Error         func(key K, err error)
	onPanic           PanicHandler
	writeCh           chan l2Write[K, V]

	flights []flightBucket[K, V]
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
	// Cache can hold up to MaxItems, but with CostFunc MemoryCapacity can be bigger than the number of items.
	if cfg.CostFunc == nil && cfg.MemoryCapacity > itemstore.MaxItems {
		return nil, ErrCapacityTooLarge
	}
	if cfg.PreallocateMap && cfg.CostFunc != nil {
		return nil, ErrPreallocWeighted
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
		costFunc:          cfg.CostFunc,
		shards:            make([]shard[K, V], numShards),
		flights:           make([]flightBucket[K, V], numShards),
		shardMask:         uint32(numShards - 1),
		seed:              maphash.MakeSeed(),
		epoch:             time.Now(),
		ttl:               cfg.TTL,
		l2Cache:           cfg.L2Cache,
		l2WritePolicy:     cfg.WritePolicy,
		writeBackBatching: cfg.WriteBackBatching,
		onL2Error:         cfg.OnL2Error,
		onPanic:           cfg.OnPanic,
		onDeletion:        cfg.OnDeletion,
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
		slots := minTableSlots
		if cfg.PreallocateMap {
			slots = preallocSlots(sh.cap)
		}
		sh.items.Store(itemstore.NewFlatHashMap[K, V](slots))
		if cfg.CustomPolicy != nil {
			sh.policy = cfg.CustomPolicy(&sh.items, sh.cap)
			if sh.policy == nil {
				return nil, ErrNilCustomPolicy
			}
			continue
		}
		switch cfg.Policy {
		case PolicyS3FIFO:
			sh.policy = eviction.NewS3FIFO(&sh.items, sh.cap, ghostPerShard)
		case PolicyClock:
			sh.policy = eviction.NewClockPolicy(&sh.items)
		case PolicyWTinyLFU:
			sh.policy = eviction.NewWTinyLFU(&sh.items, sh.cap, ghostPerShard)
		case PolicySIEVE:
			sh.policy = eviction.NewSieve(&sh.items)
		}
	}

	if cfg.TTL > 0 {
		ttlSec := int64((cfg.TTL + time.Second - 1) / time.Second)
		unitSec := max((ttlSec+itemstore.ExpireMax-1)/itemstore.ExpireMax, 1)
		c.expireUnit = time.Duration(unitSec) * time.Second
		// The cap keeps ttlOff+1 (see expireOffsetAt) within half the wrapping scale.
		c.ttlOff = uint32(min((ttlSec+unitSec-1)/unitSec, itemstore.ExpireMax-1))
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

func (c *Cache[K, V]) keyHash(key K) uint64 {
	return maphash.Comparable(c.seed, key)
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
	c.setMemory(key, value, c.expireOffset())
	return value, true, nil
}

// GetFromMemory reads the first level only: the fastest possible path, without a context, L2, or errors.
func (c *Cache[K, V]) GetFromMemory(key K) (V, bool) {
	value, ok := c.getMemory(key)
	c.stats.addMemHit(ok)
	return value, ok
}

// getMemory is the lock-free memory-hit path. It probes the slot table by meta word (tag prefilter), then uses the
// item's Entry snapshot to confirm an exact key match and to obtain a value that is never observed half-written.
//
// The body inlines the shardAndHash computation and the snapshot into a single stack slot, avoiding a call frame and
// extra Entry copies. Those overheads would either slow things down directly or clog the works so the CPU can't do
// something else while waiting.
func (c *Cache[K, V]) getMemory(key K) (V, bool) {
	keyHash := maphash.Comparable(c.seed, key)
	sh := &c.shards[uint32(keyHash)&c.shardMask]
	storage := sh.items.GetStorage()
	tagged := itemstore.TagOf(keyHash)
	home := storage.Home(keyHash)
	groupEnd := (home | 7) + 1 // first position past home's overflow group
	var entry itemstore.Entry[K, V]
	var zero V
	for pos := home; ; pos++ {
		if pos == groupEnd && !storage.Overflowed(home) {
			return zero, false // nothing homed in this group lives past it: definitely absent
		}
		item := storage.At(pos)
		for {
			metaWord := item.Load()
			if metaWord == 0 {
				return zero, false // a never-occupied slot ends the probe chain
			}
			if metaWord&itemstore.DeadOrTag != tagged {
				break // dead, or a foreign tag
			}
			if !item.SnapshotInto(&entry, metaWord) {
				continue // the copy raced a recycle or an overwrite: retry against the fresh meta word
			}
			if entry.Key != key {
				break // tag collision or a recycled slot
			}
			nowOff := c.nowOff.Load()
			if !itemstore.Expired(metaWord, nowOff) {
				if c.refreshOnGet {
					expireOff := uint32(metaWord & itemstore.ExpireMask >> itemstore.ExpireShift)
					if newOff := c.expireOffsetAt(nowOff); newOff != expireOff && item.TouchAndRefreshExpire(metaWord, newOff) {
						return entry.Value, true
					}
				}
				item.TouchWith(metaWord)
				return entry.Value, true
			}
			// TTL has elapsed - drop the item lazily instead of waiting for the eviction queue to reach it.
			c.dropExpired(sh, keyHash, key, storage.Wrap(pos))
			return zero, false
		}
	}
}

// Set stores the value in memory and in L2 according to WritePolicy. An error can come only from a synchronous L2
// write.
func (c *Cache[K, V]) Set(ctx context.Context, key K, value V) error {
	c.setMemory(key, value, c.expireOffset())
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

// setMemory puts the value into the first level with the given expiration offset (0 = no TTL; callers that just
// want the configured TTL pass expireOffset()). An overwrite stores into the item in place (the item, its queue
// node and its slot stay put); a new key claims a free slot. Neither path allocates.
func (c *Cache[K, V]) setMemory(key K, value V, expireOff uint32) {
	weight := c.rawCost(key, value)
	sh, keyHash := c.shardAndHash(key)
	var deletions []deletion[K, V] // stays nil unless a handler is configured
	if weight > sh.cap {
		// Does not fit at all; drop the older value too so it stops serving reads.
		sh.mu.Lock()
		c.deleteLocked(sh, keyHash, key, CauseOverflow, &deletions)
		sh.mu.Unlock()
		if len(deletions) > 0 {
			c.callOnDeletion(deletions)
		}
		return
	}

	sh.mu.Lock()
	storage := sh.items.GetStorage()
	tagged := itemstore.TagOf(keyHash)
	home := storage.Home(keyHash)
	weightDelta := weight
	reuse, hasReuse := uint32(0), false
	for pos := home; ; pos++ {
		item := storage.At(pos)
		metaWord := item.Load()
		if metaWord == 0 {
			// New key: claim this empty slot, or the first freed tombstone seen on the way.
			if hasReuse {
				pos, item = reuse, storage.At(reuse)
				sh.dirty--
			}
			item.Publish(itemstore.Entry[K, V]{Key: key, Value: value}, tagged, expireOff)
			storage.NoteDisplaced(home, pos)
			sh.live++
			sh.policy.Add(itemstore.QNode{Idx: storage.Wrap(pos), Cost: uint32(weight)})
			c.maybeRebuild(sh, storage)
			break
		}
		if metaWord&itemstore.Dead != 0 {
			if !itemstore.Reusable(metaWord) && metaWord&itemstore.TagMask == tagged&itemstore.TagMask &&
				item.Entry().Key == key {
				// The queue node still references this slot, so re-insert resurrects the entry instead of extending
				// the probe chain with a dead record, which hurts hash probing.
				item.Publish(itemstore.Entry[K, V]{Key: key, Value: value}, tagged, expireOff)
				storage.NoteDisplaced(home, pos)
				sh.live++
				sh.dirty--
				if sh.deadCount > 0 {
					sh.deadCount--
				}
				break
			}
			// Any other tombstone still referenced by its queue node cannot be reused - the node would point at a stranger.
			if !hasReuse && itemstore.Reusable(metaWord) {
				reuse, hasReuse = pos, true
			}
			continue
		}
		if metaWord&itemstore.TagMask != tagged&itemstore.TagMask {
			continue
		}
		entry := item.Entry()
		if entry.Key != key {
			continue
		}
		// Overwrite in place; the old value is read for the weight delta before the store lands.
		weightDelta = weight - c.rawCost(key, entry.Value)
		if c.onDeletion != nil {
			c.addDeletion(&deletions, key, entry.Value, CauseReplacement)
		}
		item.SetValue(value)
		if expireOff != 0 {
			item.RefreshExpire(expireOff)
		}
		break
	}
	if sh.weight.Add(weightDelta) > sh.cap {
		c.evictShard(sh, &deletions)
	}
	sh.mu.Unlock()
	if len(deletions) > 0 {
		c.callOnDeletion(deletions)
	}
}

// maybeRebuild replaces the shard's table when it passes 3/4 occupancy (tombstones included): doubled when live
// items need the space, same-size otherwise - either way tombstones are purged. Items move, so the policy re-links
// its queue nodes to the new positions and drops dead ones on the same pass. Readers finish on the superseded table:
// a probe racing the swap may return a value as of the copy moment - the documented Get-racing-Set window. Called
// under the shard mutex.
func (c *Cache[K, V]) maybeRebuild(sh *shard[K, V], t *itemstore.FlatHashMap[K, V]) {
	if (sh.live+sh.dirty)*4 < t.SlotCount()*3 {
		return
	}
	newSize := t.SlotCount()
	if sh.live*2 >= newSize {
		newSize *= 2
	}
	fresh := itemstore.NewFlatHashMap[K, V](newSize)
	live := 0
	sh.policy.Rebuild(func(oldIdx uint32) (uint32, bool) {
		item := t.At(oldIdx)
		if item.Load()&itemstore.Dead != 0 {
			return 0, false
		}
		live++
		return fresh.InsertFresh(maphash.Comparable(c.seed, item.Entry().Key), item), true
	})
	sh.live, sh.dirty, sh.deadCount = live, 0, 0
	sh.items.Store(fresh)
}

// evictShard evicts items from the shard while its weight exceeds the capacity. The policy only picks and dequeues
// the victim; the alive -> dead transition (and so the weight accounting) happens here - for items that died earlier
// (Delete, lazy TTL removal) Kill reports false and everything is already accounted. Called under the shard mutex.
func (c *Cache[K, V]) evictShard(sh *shard[K, V], deletions *[]deletion[K, V]) {
	nowOff := c.nowOff.Load()
	storage := sh.items.GetStorage()
	for sh.weight.Load() > sh.cap {
		victimIdx, ok := sh.policy.Evict(nowOff)
		if !ok {
			return
		}
		item := storage.At(victimIdx)
		if item.Kill() {
			entry := item.Entry()
			sh.weight.Add(-c.rawCost(entry.Key, entry.Value))
			storage.ForgetDisplaced(storage.Home(maphash.Comparable(c.seed, entry.Key)), victimIdx)
			sh.live--
			sh.dirty++
			if c.onDeletion != nil {
				// The policy hands over expired items too; the meta word says which of the two it was.
				cause := CauseEviction
				if itemstore.Expired(item.Load(), nowOff) {
					cause = CauseExpiration
				}
				c.addDeletion(deletions, entry.Key, entry.Value, cause)
			}
		}
		item.MakeFree() // the queue node is gone: the slot may serve a new key
	}
}

// dropExpired lazily removes a TTL-expired item found by the Get path. The item stays in the queue as a tombstone
// until the next eviction pass or sweep.
func (c *Cache[K, V]) dropExpired(sh *shard[K, V], h uint64, key K, idx uint32) {
	sh.mu.Lock()
	foundIdx, item, ok := c.findSlot(sh, h, key)
	// The idx match and the Expired re-check reject the races: a re-claimed slot or a refreshed TTL means the item
	// survives.
	var deletions []deletion[K, V] // stays nil unless a handler is configured
	if ok && foundIdx == idx && itemstore.Expired(item.Load(), c.nowOff.Load()) {
		c.killAt(sh, h, foundIdx, item, CauseExpiration, &deletions)
	}
	sh.mu.Unlock()
	if len(deletions) > 0 {
		c.callOnDeletion(deletions)
	}
}

// livePresent probes for key without taking the shard mutex, so deleting a key the cache does not hold never queues
// behind the writers of its shard. A Set racing the probe is the Get-racing-Set window: the delete lands before it.
func (c *Cache[K, V]) livePresent(sh *shard[K, V], keyHash uint64, key K) bool {
	storage := sh.items.GetStorage()
	tagged := itemstore.TagOf(keyHash)
	home := storage.Home(keyHash)
	groupEnd := (home | 7) + 1
	var entry itemstore.Entry[K, V]
	for pos := home; ; pos++ {
		if pos == groupEnd && !storage.Overflowed(home) {
			return false
		}
		item := storage.At(pos)
		for {
			metaWord := item.Load()
			if metaWord == 0 {
				return false
			}
			if metaWord&itemstore.DeadOrTag != tagged {
				break
			}
			if !item.SnapshotInto(&entry, metaWord) {
				continue
			}
			if entry.Key == key {
				return true
			}
			break
		}
	}
}

// killAt kills the item in its slot and subtracts its weight; the slot stays a non-reusable tombstone until
// its queue node is swept. Called under the shard mutex.
func (c *Cache[K, V]) killAt(sh *shard[K, V], keyHash uint64, idx uint32, item *itemstore.Item[K, V],
	cause DeletionCause, deletions *[]deletion[K, V]) {
	entry := item.Entry()
	sh.weight.Add(-c.rawCost(entry.Key, entry.Value))
	if c.onDeletion != nil {
		c.addDeletion(deletions, entry.Key, entry.Value, cause)
	}
	item.Kill()
	storage := sh.items.GetStorage()
	storage.ForgetDisplaced(storage.Home(keyHash), idx)
	sh.live--
	sh.dirty++
	c.noteDead(sh)
}

// findSlot probes the shard's current table for the key's live slot: (slot index, item, true) when found. Called
// under the shard mutex.
func (c *Cache[K, V]) findSlot(sh *shard[K, V], keyHash uint64, key K) (uint32, *itemstore.Item[K, V], bool) {
	storage := sh.items.GetStorage()
	tagged := itemstore.TagOf(keyHash)
	home := storage.Home(keyHash)
	groupEnd := (home | 7) + 1
	for pos := home; ; pos++ {
		if pos == groupEnd && !storage.Overflowed(home) {
			return 0, nil, false
		}
		item := storage.At(pos)
		metaWord := item.Load()
		if metaWord == 0 {
			return 0, nil, false
		}
		if metaWord&itemstore.DeadOrTag != tagged {
			continue
		}
		if item.Entry().Key == key {
			return storage.Wrap(pos), item, true
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
		storage := sh.items.GetStorage()
		sh.policy.Sweep(func(idx uint32) { storage.At(idx).MakeFree() })
		sh.deadCount = 0
	}
}

// deleteLocked tombs the key's table slot, kills its item and subtracts its weight. Called under the shard
// mutex; a missing key is a no-op.
func (c *Cache[K, V]) deleteLocked(sh *shard[K, V], keyHash uint64, key K, cause DeletionCause,
	deletions *[]deletion[K, V]) {
	if idx, item, ok := c.findSlot(sh, keyHash, key); ok {
		c.killAt(sh, keyHash, idx, item, cause, deletions)
	}
}

// Delete removes the key from memory and forwards the deletion to L2 according to WritePolicy (async for WriteBack).
// The memory the item held is reclaimed on the next eviction pass or tombstone sweep.
func (c *Cache[K, V]) Delete(ctx context.Context, key K) error {
	sh, keyHash := c.shardAndHash(key)
	if c.livePresent(sh, keyHash, key) {
		var deletions []deletion[K, V] // stays nil unless a handler is configured
		sh.mu.Lock()
		c.deleteLocked(sh, keyHash, key, CauseInvalidation, &deletions)
		sh.mu.Unlock()
		if len(deletions) > 0 {
			c.callOnDeletion(deletions)
		}
	}
	c.stats.addDeletes(1)

	switch c.l2WritePolicy {
	case WriteThrough:
		return c.l2Cache.Delete(ctx, key)
	case WriteBack:
		c.enqueueDelete(key)
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

	call := &flightCall[K, V]{}
	if winner, running := c.claim(c.keyHash(key), key, call); running {
		// A flight is already in progress - wait for its result or for the context to be canceled (the owner keeps
		// loading on behalf of everyone else). The key was not in memory when this call looked, and this call itself
		// never reaches L2 - the owner does: a memory miss.
		c.stats.addMemMisses(1)
		if err := winner.wait(ctx); err != nil {
			var zero V
			return zero, err
		}
		return winner.val, winner.err
	}

	// ErrLoaderPanic stands until doLoad returns: a loader that panics or Goexits must not leave waiters stuck.
	call.err = ErrLoaderPanic
	defer c.release(call)

	value, err := c.doLoad(ctx, key, load)
	call.val, call.err, call.ok = value, err, err == nil
	return value, err
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
			c.setMemory(key, value, c.expireOffset())
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
	c.storeLoaded(ctx, key, value)
	return value, nil
}

// storeLoaded caches a freshly loaded value and forwards it to L2 per WritePolicy. The value is already in hand, so
// an L2 write error is reported rather than returned.
func (c *Cache[K, V]) storeLoaded(ctx context.Context, key K, value V) {
	c.setMemory(key, value, c.expireOffset())
	c.stats.addSets(1)
	switch c.l2WritePolicy {
	case WriteDisabled:
	case WriteThrough:
		if err := c.l2Cache.Set(ctx, key, value, c.ttl); err != nil {
			c.reportL2Err(key, err)
		}
	default:
		c.enqueueWriteBack(key, value)
	}
}

// Iterator returns an iterator over all live first-level entries (L2 is not scanned). The walk is lock-free and
// weakly consistent, like sync.Map.Range: entries written or removed while iterating may or may not be seen, but a
// yielded pair is never torn.
func (c *Cache[K, V]) Iterator() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		c.iterateWithLife(func(key K, value V, _ time.Duration) bool { return yield(key, value) })
	}
}

// iterateWithLife is the Iterator walk, plus how much life each item has left (0 when it has no TTL). SaveTo needs
// the lifetime, and one walk is easier to keep correct than two.
func (c *Cache[K, V]) iterateWithLife(yield func(K, V, time.Duration) bool) {
	var entry itemstore.Entry[K, V]
	for i := range c.shards {
		storage := c.shards[i].items.GetStorage()
		nowOff := c.nowOff.Load()
		for pos := 0; pos < storage.SlotCount(); pos++ {
			item := storage.At(uint32(pos))
			metaWord := item.Load()
			if metaWord == 0 || metaWord&itemstore.Dead != 0 || itemstore.Expired(metaWord, nowOff) {
				continue
			}
			if !item.SnapshotInto(&entry, metaWord) {
				continue
			}
			if !yield(entry.Key, entry.Value, c.lifeLeft(metaWord, nowOff)) {
				return
			}
		}
	}
}

// lifeLeft turns an alive item's expiration offset into the wall time it has left; 0 means no TTL.
func (c *Cache[K, V]) lifeLeft(metaWord uint64, nowOff uint32) time.Duration {
	expireOff := uint32(metaWord & itemstore.ExpireMask >> itemstore.ExpireShift)
	if expireOff == 0 {
		return 0
	}
	return time.Duration((expireOff-nowOff)&itemstore.ExpireWrapMask) * c.expireUnit
}

// expireOffsetIn is expireOffset for an item that should outlive now by remaining, capped at the cache's own TTL.
// Rounding is upwards so a restored item never expires earlier than it would have.
func (c *Cache[K, V]) expireOffsetIn(remaining time.Duration) uint32 {
	if c.ttlOff == 0 || remaining <= 0 {
		return 0
	}
	units := min(uint32((remaining+c.expireUnit-1)/c.expireUnit), c.ttlOff)
	expireOff := (c.nowOff.Load() + units + 1) & itemstore.ExpireWrapMask
	if expireOff == 0 {
		expireOff = 1
	}
	return expireOff
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

// TotalWeight estimates the total memory footprint of the cache's first-level structures: item tables (items carry
// their Entry inline), eviction bookkeeping and the fixed parts (the Cache struct, shards, flights buckets,
// write-back buffer).
//
// An Entry is counted at its inline size, so heap data referenced by K or V is not included. When CostFunc measures
// those bytes, TotalWeight() + Weight() gives the full footprint.
func (c *Cache[K, V]) TotalWeight() int64 {
	total := int64(unsafe.Sizeof(*c))
	total += int64(len(c.shards)) * int64(unsafe.Sizeof(shard[K, V]{}))
	total += int64(len(c.flights)) * int64(unsafe.Sizeof(flightBucket[K, V]{}))

	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		total += sh.items.GetStorage().Bytes() + sh.policy.Bytes()
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
// before a tick would expire almost immediately. An item lives at least its TTL and at most one extra clock unit.
func (c *Cache[K, V]) expireOffset() uint32 {
	if c.ttlOff == 0 {
		return 0
	}
	return c.expireOffsetAt(c.nowOff.Load())
}

// expireOffsetAt is expireOffset for an already-loaded clock value; only meaningful when TTL is enabled. Offsets wrap
// on their 18-bit scale; 0 means "no TTL", so a wrap landing there shifts one unit earlier.
func (c *Cache[K, V]) expireOffsetAt(nowOff uint32) uint32 {
	expireOff := (nowOff + c.ttlOff + 1) & itemstore.ExpireWrapMask
	if expireOff == 0 {
		expireOff = 1
	}
	return expireOff
}

func (c *Cache[K, V]) reportL2Err(key K, err error) {
	if c.onL2Error != nil {
		c.onL2Error(key, err)
	}
}

func (c *Cache[K, V]) notifyPanic(recovered any, handled bool) {
	if c.onPanic == nil {
		return
	}
	defer func() { _ = recover() }()
	c.onPanic(recovered, handled)
}

// recoverWorker keeps a panic from ending the process on a goroutine the cache owns: nobody up that stack can
// recover on it. Nothing else sees the panic, so OnPanic hears it unhandled. Must be deferred directly.
func (c *Cache[K, V]) recoverWorker() {
	if r := recover(); r != nil {
		c.notifyPanic(r, false)
	}
}

// recoverWriteBack is recoverWorker for the write-back worker, where a panic comes from the L2 adapter or from
// OnL2Error itself: it is reported like any other L2 failure, unless the handler is the one that panicked.
func (c *Cache[K, V]) recoverWriteBack(key K) {
	r := recover()
	if r == nil {
		return
	}
	if c.onL2Error == nil {
		c.notifyPanic(r, false)
		return
	}
	defer c.recoverWorker() // OnL2Error is next, and it may be what panicked in the first place
	c.onL2Error(key, fmt.Errorf("%w: %v", ErrPanic, r))
	c.notifyPanic(r, true)
}

// clockLoop refreshes the coarse TTL clock.
func (c *Cache[K, V]) clockLoop() {
	defer c.wg.Done()
	defer c.recoverWorker()
	ticker := time.NewTicker(TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.nowOff.Store(uint32(time.Since(c.epoch)/c.expireUnit) & itemstore.ExpireWrapMask)
		case <-c.stop:
			return
		}
	}
}

// WriteBackBatchMax caps one drain batch: the writes are flushed through the adapter's BatchSet.
const WriteBackBatchMax = 128

// flushWrite hands one write-back task to L2, coalescing the tasks already queued behind it into one BatchSet;
// a panicking adapter costs this batch, not the worker.
func (c *Cache[K, V]) flushWrite(first l2Write[K, V]) {
	defer c.recoverWriteBack(first.key)
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
				defer close(write.flush) // a Wait checkpoint must be released even if the delivery panics
				c.deliverRun(sets, deletes)
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
	defer c.recoverWorker() // flushWrite recovers per batch, so reaching this means the loop itself broke
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

// KeyVal is one key/value pair of a batch result.
type KeyVal[K comparable, V any] struct {
	Key   K
	Value V
}

// List is the result of a batch operation: the pairs that were found, in no particular order.
type List[K comparable, V any] []KeyVal[K, V]

// ToMap collects the pairs into a map.
func (t List[K, V]) ToMap() map[K]V {
	m := make(map[K]V, len(t))
	for _, item := range t {
		m[item.Key] = item.Value
	}
	return m
}
