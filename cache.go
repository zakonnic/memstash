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
	"iter"
	"math"
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

	// DefaultWriteBackWorkers is the size of the write-back worker pool when WriteBackWorkers is not set.
	DefaultWriteBackWorkers = 4

	// DefaultGetBatchedWorkers is the size of the coalescing read pool when GetBatchedWorkers is not set.
	DefaultGetBatchedWorkers = 4

	// DefaultGetBatchedBuffer is the queue GetBatched coalesces reads through when GetBatchedBufferSize is not set.
	DefaultGetBatchedBuffer = 1024

	// DefaultPreallocationRatio is Config.PreallocationRatio when it is not set: how full a preallocated shard sits
	// once the cache holds its whole capacity.
	DefaultPreallocationRatio = 0.75

	// DefaultGrowthThreshold is Config.GrowthThreshold when it is not set: the occupancy a shard table is rebuilt at.
	DefaultGrowthThreshold = 5.0 / 6.0

	// growthShift keeps the growth threshold in fixed point, a numerator over 1<<growthShift, so the insert path's
	// growth test is two integer multiplies.
	growthShift = 12

	// minTableSlots is the initial table size of every shard.
	minTableSlots = 64

	// maxTableSlots is the largest table a shard can address: the whole uint32 slot index space.
	maxTableSlots = int64(1) << 32
)

// shard is an independent segment of the first level: a key is always served by the same shard (by hash), and all
// mutations for that key are serialized by the shard mutex. Readers never take it: they probe the atomically
// published table and verify candidates against the items themselves.
type shard[K comparable, V any] struct {
	// The first cache line is the read path's: a probe loads the table and the move sequence, and a writer taking
	// the mutex must not bounce that line out of every reader's cache. shardBytes pins the layout.
	items itemstore.StorageProxy[K, V] // the item table, swapped wholesale on growth
	// moveSeq is odd while a reclaim moves items. A lock-free probe that misses checks it was even and unchanged;
	// otherwise a key may have crossed the probe, and the miss is not trusted.
	moveSeq atomic.Uint32
	_       [52]byte

	mu       sync.Mutex
	policy   EvictionPolicy[K, V]
	weight   atomic.Int64
	cap      int64
	live     int  // alive items; guarded by mu
	sweeping bool // a sweep pass is in progress: every removal advances it by sweepBudget nodes
	removals int  // removals since the last look at the queue length; guarded by mu
	// sweepParity alternates with each pass and is what an item is stamped with when a pass keeps its node (see
	// EvictionPolicy.Sweep). An insert stamps the current one, so a key inserted between passes reads as unaccounted
	// in the next.
	sweepParity bool
	scratch     []itemstore.ShiftEntry // Reclaim's buffer for the run it compacts; guarded by mu

	_ [32]byte
}

// Cache is a two-level cache.
type Cache[K comparable, V any] struct {
	costFunc func(key K, value V) uint32

	shards    []shard[K, V]
	shardMask uint32
	// preallocated pins the table size: the whole capacity is already there, so the table never grows.
	preallocated bool
	// growthNum is Config.GrowthThreshold as a numerator over 1<<growthShift.
	growthNum int
	// onDeletion sits with the fields the write path already touches: every removal site tests it, so a cache without
	// a handler pays one predicted branch off a hot cache line.
	onDeletion func(key K, value V, cause DeletionCause)
	hasher     itemstore.Hasher[K]
	wordKeys   bool // the key type takes Hasher.HashWord: chosen once, so the hot paths branch on a field

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
	// writeQueues is one buffer per write-back worker; a key always lands on the same one, so its L2 writes stay
	// ordered even though the workers run concurrently.
	writeQueues []chan l2Write[K, V]

	// batchedGets is the read pool GetBatched coalesces through; it stays dormant until the first call reaches it.
	batchedGets getWorkerPool[K, V]

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
	if cfg.PreallocationRatio != 0 {
		if math.IsNaN(cfg.PreallocationRatio) || cfg.PreallocationRatio <= 0 || cfg.PreallocationRatio >= 1 {
			return nil, ErrBadPreallocationRatio
		}
		cfg.PreallocateMap = true // sizing the fill is asking for a preallocated table
	}
	if cfg.GrowthThreshold != 0 &&
		(math.IsNaN(cfg.GrowthThreshold) || cfg.GrowthThreshold <= 0 || cfg.GrowthThreshold >= 1) {
		return nil, ErrBadGrowthThreshold
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
	if cfg.WriteBackWorkers < 0 {
		return nil, ErrBadWorkers
	}
	if cfg.GetBatchedWorkers < 0 {
		return nil, ErrBadGetBatchedWorkers
	}

	numShards := cfg.shardCount()
	c := &Cache[K, V]{
		costFunc:          cfg.CostFunc,
		shards:            make([]shard[K, V], numShards),
		flights:           make([]flightBucket[K, V], numShards),
		shardMask:         uint32(numShards - 1),
		preallocated:      cfg.PreallocateMap,
		growthNum:         int(math.Round(cfg.growthThreshold() * (1 << growthShift))),
		hasher:            itemstore.NewHasher[K](),
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
		batchedGets:       getWorkerPool[K, V]{size: cfg.getBatchedBuffer(), workers: cfg.getBatchedWorkers()},
	}
	c.wordKeys = c.hasher.WordKeys()
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
			slots = preallocSlots(sh.cap, cfg.preallocRatio())
		}
		sh.items.Store(itemstore.NewFlatHashMap[K, V](slots, c.hasher))
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
		workers := cfg.writeBackWorkers()
		c.writeQueues = make([]chan l2Write[K, V], workers)
		c.wg.Add(workers)
		for i := range c.writeQueues {
			// worker pool with separate buffers to bind workers to keys
			c.writeQueues[i] = make(chan l2Write[K, V], cfg.writeBackBuffer())
			go c.writeBackLoop(c.writeQueues[i])
		}
	}
	return c, nil
}

// Close stops the background goroutines and waits for the write-back buffer to drain. Repeated calls are safe.
// A Set that starts strictly after Close returns still reaches L2 (synchronously); a Set racing with Close may
// lose its asynchronous write. A GetBatched waiting on the read pool loses nothing: it reads L2 itself.
func (c *Cache[K, V]) Close() {
	c.closeOnce.Do(func() {
		close(c.stop)
		c.batchedGets.close() // before the wait below: see getPool.close
		c.wg.Wait()
		// Catch writes that slipped into a buffer while its worker was shutting down. The workers are gone, so
		// this goroutine is the only receiver and a non-empty queue can always be received from.
		for _, queue := range c.writeQueues {
			for len(queue) > 0 {
				c.flushWrite(queue, <-queue)
			}
		}
	})
}

// Wait blocks until every asynchronous write-back write enqueued before the call has been handed to L2. Unlike Close
// it is a checkpoint, not a shutdown: the cache keeps serving traffic. With WriteThrough or WriteDisabled, or while
// the cache is closing (Close drains the buffer itself), it returns immediately.
func (c *Cache[K, V]) Wait() {
	if len(c.writeQueues) == 0 {
		return
	}
	// Marker sits behind everything enqueued into that queue before this call.
	markers := make([]chan struct{}, len(c.writeQueues))
	for i, queue := range c.writeQueues {
		markers[i] = make(chan struct{})
		select {
		case queue <- l2Write[K, V]{flush: markers[i]}:
		case <-c.stop:
			return
		}
	}
	for _, marker := range markers {
		select {
		case <-marker:
		case <-c.stop:
			return
		}
	}
}

// shardAndHash returns the key's shard and hash: the low bits pick the shard, the high bits seed the table probe,
// so one hash serves both.
func (c *Cache[K, V]) shardAndHash(key K) (*shard[K, V], uint64) {
	keyHash := c.hash(key)
	return &c.shards[uint32(keyHash)&c.shardMask], keyHash
}

func (c *Cache[K, V]) keyHash(key K) uint64 {
	return c.hash(key)
}

// writeQueueOf returns the write-back worker for the key, to spread across any worker count.
func (c *Cache[K, V]) writeQueueOf(key K) chan l2Write[K, V] {
	if len(c.writeQueues) == 1 {
		return c.writeQueues[0]
	}
	return c.writeQueues[(c.keyHash(key)>>32)%uint64(len(c.writeQueues))]
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
	return c.getFromL2(ctx, key)
}

// getFromL2 is the second-level half of Get: the read itself, its counters and the promotion into memory.
func (c *Cache[K, V]) getFromL2(ctx context.Context, key K) (V, bool, error) {
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
	if ok {
		c.stats.addMemHits(1)
	} else {
		c.stats.addMemMisses(1)
	}
	return value, ok
}

// getMemory is the lock-free read path. It hashes in place and inlines touchHit by hand: neither would inline as a
// call.
//
// Nothing is read before the home slot: that is the one dependent access a hit can afford. A validated snapshot is a
// hit as it stands. A miss is trusted only if no reclaim moved items during the walk (moveSeq even and unchanged);
// otherwise getMoved walks again.
func (c *Cache[K, V]) getMemory(key K) (V, bool) {
	// The hash branch is written out here rather than taken from hash: this body is never inlined, so the branch
	// costs nothing, and the word mixer inlines into the probe where a call to hash would not.
	var keyHash uint64
	if c.wordKeys {
		keyHash = c.hasher.HashWord(key)
	} else {
		keyHash = c.hasher.HashSlow(key)
	}
	sh := &c.shards[uint32(keyHash)&c.shardMask]
	tagged := tagOf(keyHash)
	seq := sh.moveSeq.Load()
	storage := sh.items.GetStorage()
	home := storage.Home(keyHash)
	groupEnd := (home | 7) + 1 // first position past home's overflow group
	var entry itemstore.Entry[K, V]
	var zero V
	for pos := home; ; pos++ {
		if pos == groupEnd && !storage.OverflowedTag(home, overflowBucket(keyHash)) {
			break // no key with this tag left the group: absent
		}
		item := storage.At(pos)
		metaWord := item.Metadata()
		for {
			if metaWord&itemstore.DeadOrTag != tagged {
				break // dead, or a foreign tag
			}
			if !item.SnapshotInto(&entry, metaWord) {
				metaWord = item.Metadata()
				continue // the copy raced a recycle or an overwrite: retry against the fresh meta word
			}
			if entry.Key != key {
				break // tag collision or a recycled slot
			}
			nowOff := c.nowOff.Load()
			if !expired(metaWord, nowOff) {
				if c.refreshOnGet {
					expireOff := uint32(metaWord & itemstore.ExpireMask >> itemstore.ExpireShift)
					if newOff := c.expireOffsetAt(nowOff); newOff != expireOff && item.TouchAndRefreshExpire(metaWord, newOff) {
						return entry.Value, true
					}
				}
				item.TouchWith(metaWord)
				return entry.Value, true
			}
			// Expired: drop it now rather than wait for the sweep or the policy to reach it.
			c.dropExpired(sh, keyHash, key)
			return zero, false
		}
		if metaWord&itemstore.HasNext == 0 {
			break // no key homed at or before this slot lives past it
		}
	}
	if seq&1 == 0 && sh.moveSeq.Load() == seq {
		return zero, false
	}
	return c.getMoved(sh, keyHash, key)
}

// getMoved settles a miss that overlapped a reclaim. It walks once more and, if the shard is still compacting, reads
// under the mutex. It lives apart so getMemory stays one pass.
func (c *Cache[K, V]) getMoved(sh *shard[K, V], keyHash uint64, key K) (V, bool) {
	tagged := tagOf(keyHash)
	var entry itemstore.Entry[K, V]
	var zero V
	seq := sh.moveSeq.Load()
	storage := sh.items.GetStorage()
	home := storage.Home(keyHash)
	groupEnd := (home | 7) + 1
	for pos := home; ; pos++ {
		if pos == groupEnd && !storage.OverflowedTag(home, overflowBucket(keyHash)) {
			break
		}
		item := storage.At(pos)
		metaWord := item.Metadata()
		for {
			if metaWord&itemstore.DeadOrTag != tagged {
				break
			}
			if !item.SnapshotInto(&entry, metaWord) {
				metaWord = item.Metadata()
				continue
			}
			if entry.Key != key {
				break
			}
			nowOff := c.nowOff.Load()
			if !expired(metaWord, nowOff) {
				item.TouchWith(metaWord)
				return entry.Value, true
			}
			c.dropExpired(sh, keyHash, key)
			return zero, false
		}
		if metaWord&itemstore.HasNext == 0 {
			break
		}
	}
	if seq&1 == 0 && sh.moveSeq.Load() == seq {
		return zero, false
	}
	return c.getLocked(sh, keyHash, key)
}

// getLocked reads under the shard mutex, where no reclaim can move items: the fallback for a probe that keeps
// overlapping compactions.
func (c *Cache[K, V]) getLocked(sh *shard[K, V], keyHash uint64, key K) (V, bool) {
	var zero V
	sh.mu.Lock()
	defer sh.mu.Unlock()
	_, item, ok := c.findSlot(sh, keyHash, key)
	if !ok {
		return zero, false
	}
	metaWord := item.Metadata()
	if expired(metaWord, c.nowOff.Load()) {
		return zero, false
	}
	item.TouchWith(metaWord)
	return item.Entry().Value, true
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
	c.enqueueWriteBack(key, value, c.ttl)
	return nil
}

// SetWithTTL sets a per-entry lifetime. The cache must have been built with a TTL option; ttl is rounded up to the
// scale's unit. A non-positive ttl is an error.
// WithRefreshTTLOnGet extends by the cache's TTL: this ttl lasts only until the first read that refreshes it.
func (c *Cache[K, V]) SetWithTTL(ctx context.Context, key K, value V, ttl time.Duration) error {
	if c.ttlOff == 0 {
		return ErrTTLDisabled
	}
	if ttl <= 0 {
		return ErrBadTTL
	}
	expireOff, effectiveTTL := c.expireOffsetOf(ttl)
	c.setMemory(key, value, expireOff)
	c.stats.addSets(1)
	if c.l2WritePolicy == WriteDisabled {
		return nil
	}
	if c.l2WritePolicy == WriteThrough {
		return c.l2Cache.Set(ctx, key, value, effectiveTTL)
	}
	c.enqueueWriteBack(key, value, effectiveTTL)
	return nil
}

// expireOffsetOf is expireOffset for a caller-supplied lifetime: the offset for the item, and the lifetime it
// actually got after rounding up to expireUnit and capping at the far edge of the unambiguous half of the scale.
func (c *Cache[K, V]) expireOffsetOf(ttl time.Duration) (uint32, time.Duration) {
	units := min((int64(ttl)+int64(c.expireUnit)-1)/int64(c.expireUnit), int64(itemstore.ExpireMax-1))
	expireOff := (c.nowOff.Load() + uint32(units) + 1) & itemstore.ExpireWrapMask
	if expireOff == 0 {
		expireOff = 1
	}
	return expireOff, time.Duration(units) * c.expireUnit
}

// setMemory puts the value into the first level with the given expiration offset (0 = no TTL; callers that just
// want the configured TTL pass expireOffset()). An overwrite stores into the item in place (the item and its slot
// stay put); a new key takes the first empty slot on its walk - deletions compact their runs, so an empty slot is
// never followed by a key homed at or before it, and the walk ends there with certainty. Neither path allocates.
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
	tagged := tagOf(keyHash)
	home := storage.Home(keyHash)
	weightDelta := weight
	for pos := home; ; pos++ {
		item := storage.At(pos)
		metaWord := item.Metadata()
		if empty(metaWord) { // deletions compact their runs, so the key cannot live past an empty slot
			item.Publish(itemstore.Entry[K, V]{Key: key, Value: value}, tagged, expireOff,
				storage.Dist(home, storage.Wrap(pos)), sh.sweepParity)
			storage.NoteDisplaced(keyHash, pos)
			storage.MarkChain(home, storage.Wrap(pos))
			sh.live++
			sh.policy.Add(newQNode(keyHash, uint32(weight)))
			c.maybeGrow(sh, storage)
			break
		}
		if metaWord&itemstore.DeadOrTag != tagged {
			continue // a foreign tag: a dead slot never outlives its reclaim, which runs under mu
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
	if weightDelta != 0 { // an overwrite of the same weight - the common one - leaves the total alone
		storeI64(&sh.weight, sh.weight.Load()+weightDelta)
	}
	if sh.weight.Load() > sh.cap {
		c.evictShard(sh, &deletions)
	}
	sh.mu.Unlock()
	if len(deletions) > 0 {
		c.callOnDeletion(deletions)
	}
}

// maybeGrow replaces the shard's table with a bigger one once live items pass the growth threshold - the table
// holds nothing else, since a deletion compacts its run on the spot. Items are re-inserted in table order into a
// table nobody reads yet; readers finish on the superseded one, and a probe racing the swap may return a value as of
// the copy moment - the documented Get-racing-Set window. The queues need no attention: nodes address keys, not
// slots. Called under the shard mutex.
//
// A preallocated cache never grows: its table already holds the whole capacity, so a live count past the line would
// mean the capacity itself was exceeded, and growing would silently double a footprint the caller asked for up front.
func (c *Cache[K, V]) maybeGrow(sh *shard[K, V], t *itemstore.FlatHashMap[K, V]) {
	if c.preallocated || sh.live<<growthShift < t.Len()*c.growthNum || int64(t.Len()) >= maxTableSlots {
		return
	}
	fresh := itemstore.NewFlatHashMap[K, V](min(itemstore.GrowSlots(t.Len()), int(maxTableSlots)), c.hasher)
	for pos := uint32(0); int(pos) < t.Len(); pos++ {
		item := t.At(pos)
		if metaWord := item.Metadata(); !empty(metaWord) && metaWord&itemstore.Dead == 0 {
			fresh.InsertFresh(c.hash(item.Entry().Key), item)
		}
	}
	sh.items.Store(fresh)
	c.stats.addTableGrowths(1)
}

// evictShard evicts items from the shard while its weight exceeds the capacity. The policy only picks and dequeues
// the victim - a live item, resolved through Items.Find - and the alive -> dead transition, the weight accounting
// and the reclaim of the slot happen here. Called under the shard mutex.
func (c *Cache[K, V]) evictShard(sh *shard[K, V], deletions *[]deletion[K, V]) {
	nowOff := c.nowOff.Load()
	storage := sh.items.GetStorage()
	for sh.weight.Load() > sh.cap {
		victimIdx, ok := sh.policy.Evict(nowOff)
		if !ok {
			return
		}
		item := storage.At(victimIdx)
		// The policy hands over expired items too; the meta word says which of the two it was.
		cause := CauseEviction
		if expired(item.Metadata(), nowOff) {
			cause = CauseExpiration
		}
		c.killAt(sh, victimIdx, item, cause, deletions)
	}
}

// dropExpired lazily removes a TTL-expired item found by the Get path. Its queue node stays behind until the next
// eviction pass or sweep drops it.
func (c *Cache[K, V]) dropExpired(sh *shard[K, V], h uint64, key K) {
	sh.mu.Lock()
	idx, item, ok := c.findSlot(sh, h, key)
	// Recheck under the lock: the TTL may have been refreshed, and a reclaim may have moved the item.
	var deletions []deletion[K, V] // stays nil unless a handler is configured
	if ok && expired(item.Metadata(), c.nowOff.Load()) {
		c.killAt(sh, idx, item, CauseExpiration, &deletions)
	}
	c.noteStale(sh)
	sh.mu.Unlock()
	if len(deletions) > 0 {
		c.callOnDeletion(deletions)
	}
}

// livePresent probes for the key without the shard mutex, so deleting a key the cache does not hold never queues
// behind the shard's writers. A Set racing the probe is the Get-racing-Set window: the delete lands before it. A miss
// that overlapped a reclaim is retried; after three the answer is "present", and the locked path settles it.
func (c *Cache[K, V]) livePresent(sh *shard[K, V], keyHash uint64, key K) bool {
	tagged := tagOf(keyHash)
	var entry itemstore.Entry[K, V]
	for attempt := 0; ; attempt++ {
		seq := sh.moveSeq.Load()
		storage := sh.items.GetStorage()
		home := storage.Home(keyHash)
		groupEnd := (home | 7) + 1
	probe:
		for pos := home; ; pos++ {
			if pos == groupEnd && !storage.OverflowedTag(home, overflowBucket(keyHash)) {
				break probe
			}
			item := storage.At(pos)
			metaWord := item.Metadata()
			for {
				if metaWord&itemstore.DeadOrTag != tagged {
					break
				}
				if !item.SnapshotInto(&entry, metaWord) {
					metaWord = item.Metadata()
					continue
				}
				if entry.Key == key {
					return true
				}
				break
			}
			if metaWord&itemstore.HasNext == 0 {
				break probe
			}
		}
		if seq&1 == 0 && sh.moveSeq.Load() == seq {
			return false
		}
		if attempt == 2 {
			return true
		}
	}
}

// killAt kills the item in its slot, subtracts its weight and reclaims the slot on the spot: the run it sat in is
// compacted (FlatHashMap.Reclaim), so the table holds live items and nothing else, and no purge is ever needed. The
// reclaim brackets its moves on moveSeq, which is what lets a lock-free probe tell a miss that overlapped them from
// a real one. The item's home and bucket come from its meta word, so nothing here hashes a key. Called under the
// shard mutex.
func (c *Cache[K, V]) killAt(sh *shard[K, V], idx uint32, item *itemstore.Item[K, V], cause DeletionCause,
	deletions *[]deletion[K, V]) {
	entry := item.Entry()
	storeI64(&sh.weight, sh.weight.Load()-c.rawCost(entry.Key, entry.Value))
	if c.onDeletion != nil {
		c.addDeletion(deletions, entry.Key, entry.Value, cause)
	}
	storage := sh.items.GetStorage()
	storage.ForgetDisplacedOf(idx, item)
	item.Kill()
	sh.live--
	storage.Reclaim(idx, &sh.scratch, &sh.moveSeq)
}

// findSlot probes the shard's current table for the key's live slot: (slot index, item, true) when found. Called
// under the shard mutex.
func (c *Cache[K, V]) findSlot(sh *shard[K, V], keyHash uint64, key K) (uint32, *itemstore.Item[K, V], bool) {
	storage := sh.items.GetStorage()
	tagged := tagOf(keyHash)
	home := storage.Home(keyHash)
	groupEnd := (home | 7) + 1
	for pos := home; ; pos++ {
		if pos == groupEnd && !storage.OverflowedTag(home, overflowBucket(keyHash)) {
			return 0, nil, false
		}
		item := storage.At(pos)
		metaWord := item.Metadata()
		if metaWord&itemstore.DeadOrTag == tagged && item.Entry().Key == key {
			return storage.Wrap(pos), item, true
		}
		if metaWord&itemstore.HasNext == 0 {
			return 0, nil, false
		}
	}
}

// sweepMinStale is the smallest pile of extra queue nodes worth a pass; below it the walk costs more than the nodes.
const sweepMinStale = 128

// sweepBudget is how many queue nodes one write examines while a sweep pass runs: a pass over millions of nodes
// completes within a few hundred thousand writes, and a write holds the shard mutex for well under a microsecond
// more - the cap matters, since every reader that missed under an open move bracket ends up waiting on that mutex.
const sweepBudget = 4

// sweepCheckMask gates how often a removal asks the policy how long its queues are: the answer moves by one per
// removal and the trigger sits thousands away, so asking on every 64th is as good and costs an interface call
// instead of sixty-four.
const sweepCheckMask = 63

// noteStale carries the sweep: it advances a running pass by one budget, or starts one once the queues hold enough
// nodes beyond the live items to be worth the walk. A removal leaves the key's node behind and a reinsertion pushes
// another, so without this a delete-heavy workload below capacity would grow the queues without bound - eviction,
// the other reclaimer, never runs there. Two things keep it off the hot paths: only removals carry a pass, which is
// what lengthens the queues in the first place, and the trigger reads the queue length rather than counting
// removals, so the evictions that already drop nodes count for something and a workload at capacity sweeps not at
// all. Called under the shard mutex.
func (c *Cache[K, V]) noteStale(sh *shard[K, V]) {
	if sh.sweeping {
		sh.sweeping = sh.policy.Sweep(sweepBudget, sh.sweepParity)
		return
	}
	sh.removals++
	if sh.removals&sweepCheckMask == 0 && sh.policy.Len() >= 2*sh.live+sweepMinStale {
		sh.sweeping, sh.sweepParity = true, !sh.sweepParity
	}
}

// deleteLocked removes the key if memory holds it.
func (c *Cache[K, V]) deleteLocked(sh *shard[K, V], keyHash uint64, key K, cause DeletionCause,
	deletions *[]deletion[K, V]) {
	if idx, item, ok := c.findSlot(sh, keyHash, key); ok {
		c.killAt(sh, idx, item, cause, deletions)
	}
	c.noteStale(sh)
}

// Delete removes the key from memory and forwards the deletion to L2 according to WritePolicy (async for WriteBack).
// The slot is reclaimed on the spot; the queue node the item leaves behind goes with the next eviction pass or sweep.
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
		c.enqueueWriteBack(key, value, c.ttl)
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
//
// No key is yielded twice. A deletion slides items toward their homes, and the one move that could carry an item past
// the walk a second time is a move across the table's end: out of the run that wraps onto the first slots, back to
// the last ones. So the walk leaves the wrapped items of that leading run - those homed past their slot - for a final
// pass over it; one that moved to the end meanwhile was seen there instead, and one that moves away later is simply
// not seen, like any concurrent removal.
func (c *Cache[K, V]) iterateWithLife(yield func(K, V, time.Duration) bool) {
	var entry itemstore.Entry[K, V]
	for i := range c.shards {
		storage := c.shards[i].items.GetStorage()
		nowOff := c.nowOff.Load()
		size := uint32(storage.Len())
		// The leading run ends at the first empty slot; no item past it can be homed past its slot.
		lead := size
		for pos := uint32(0); pos < size; pos++ {
			if empty(storage.At(pos).Metadata()) {
				lead = pos
				break
			}
		}
		wrapped := func(pos uint32) bool {
			return pos < lead && storage.Home(c.hash(entry.Key)) > pos
		}
		for pos := uint32(0); pos < size; pos++ {
			item := storage.At(pos)
			metaWord := item.Metadata()
			if empty(metaWord) || metaWord&itemstore.Dead != 0 || expired(metaWord, nowOff) {
				continue
			}
			if !item.SnapshotInto(&entry, metaWord) || wrapped(pos) {
				continue
			}
			if !yield(entry.Key, entry.Value, c.lifeLeft(metaWord, nowOff)) {
				return
			}
		}
		for pos := uint32(0); pos < lead; pos++ {
			item := storage.At(pos)
			metaWord := item.Metadata()
			if empty(metaWord) {
				break
			}
			if metaWord&itemstore.Dead != 0 || expired(metaWord, nowOff) {
				continue
			}
			if !item.SnapshotInto(&entry, metaWord) || !wrapped(pos) {
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

// TotalSize estimates the total memory footprint of the cache's first-level structures: item tables (items carry
// their Entry inline), eviction bookkeeping and the fixed parts (the Cache struct, shards, flights buckets,
// write-back buffer, read queue).
//
// An Entry is counted at its inline size, so heap data referenced by K or V is not included. When CostFunc measures
// those bytes, TotalSize() + Weight() gives the full footprint.
func (c *Cache[K, V]) TotalSize() int64 {
	total := int64(unsafe.Sizeof(*c))
	total += int64(len(c.shards)) * int64(unsafe.Sizeof(shard[K, V]{}))
	total += int64(len(c.flights)) * int64(unsafe.Sizeof(flightBucket[K, V]{}))

	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		total += sh.items.GetStorage().Bytes() + sh.policy.Bytes()
		sh.mu.Unlock()
	}

	for _, queue := range c.writeQueues {
		total += int64(cap(queue)) * int64(unsafe.Sizeof(l2Write[K, V]{}))
	}
	total += c.batchedGets.queueBytes()
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

// recoverWriteBack is recoverWorker for a write-back worker, where a panic comes from the L2 adapter or from
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

// flushWrite hands one write-back task to L2, coalescing the tasks already queued behind it in the same worker's
// queue into one BatchSet; a panicking adapter costs this batch, not the worker.
func (c *Cache[K, V]) flushWrite(queue chan l2Write[K, V], first l2Write[K, V]) {
	defer c.recoverWriteBack(first.key)
	for more := true; more; {
		if first.flush != nil {
			close(first.flush) // a Wait checkpoint: everything enqueued before it has already been flushed
			return
		}
		// len(queue) is the buffer fill counter the adaptive mode switches on.
		if !first.del && (c.writeBackBatching == BatchingNone ||
			(c.writeBackBatching == BatchingAdaptive && len(queue) <= cap(queue)/2)) {
			c.writeBatch(List[K, V]{{Key: first.key, Value: first.value}}, first.ttl)
			return
		}
		first, more = c.flushRun(queue, first)
	}
}

// flushRun coalesces the tasks of first's kind queued behind it and delivers them as one batch.
// A run ends where the kind changes - a delete after writes, or a write carrying a different ttl.
func (c *Cache[K, V]) flushRun(queue chan l2Write[K, V], first l2Write[K, V]) (next l2Write[K, V], more bool) {
	var sets List[K, V]
	var deletes []K
	if first.del {
		deletes = append(deletes, first.key)
	} else {
		sets = append(sets, KeyVal[K, V]{Key: first.key, Value: first.value})
	}
	for len(sets)+len(deletes) < WriteBackBatchMax {
		select {
		case write := <-queue:
			switch {
			case write.flush != nil:
				defer close(write.flush) // a Wait checkpoint must be released even if the delivery panics
				c.deliverRun(sets, deletes, first.ttl)
				return next, false
			case write.del != first.del || write.ttl != first.ttl:
				c.deliverRun(sets, deletes, first.ttl)
				return write, true
			case write.del:
				deletes = append(deletes, write.key)
			default:
				sets = append(sets, KeyVal[K, V]{Key: write.key, Value: write.value})
			}
		default:
			c.deliverRun(sets, deletes, first.ttl)
			return next, false
		}
	}
	c.deliverRun(sets, deletes, first.ttl)
	return next, false
}

func (c *Cache[K, V]) deliverRun(sets List[K, V], deletes []K, ttl time.Duration) {
	if len(deletes) > 0 {
		c.deleteBatch(deletes)
		return
	}
	c.writeBatch(sets, ttl)
}

// writeBatch delivers drained writes to L2: one Set for a single item, one BatchSet otherwise (duplicate keys
// collapse to the last value there, as FIFO order would). A batch error is reported for every key it covers.
func (c *Cache[K, V]) writeBatch(batch List[K, V], ttl time.Duration) {
	if len(batch) == 1 {
		if err := c.l2Cache.Set(context.Background(), batch[0].Key, batch[0].Value, ttl); err != nil {
			c.reportL2Err(batch[0].Key, err)
		}
		return
	}
	if err := c.l2Cache.BatchSet(context.Background(), batch, ttl); err != nil {
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

// writeBackLoop is one worker of the asynchronous L2 write pool, draining the queue it owns. On shutdown it flushes
// everything left in that queue.
func (c *Cache[K, V]) writeBackLoop(queue chan l2Write[K, V]) {
	defer c.wg.Done()
	defer c.recoverWorker() // flushWrite recovers per batch, so reaching this means the loop itself broke
	for {
		select {
		case write := <-queue:
			c.flushWrite(queue, write)
		case <-c.stop:
			for {
				select {
				case write := <-queue:
					c.flushWrite(queue, write)
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

// shardBytes pins the shard layout the read path depends on: the table pointer and the move sequence alone on the
// first cache line, the mutex and everything a writer touches past it, and a whole number of lines overall so that
// the shards of one cache keep that separation down the array.
const shardBytes = 192
