// Package memstash is an ultra-fast two-level cache.
//
// The first level is sharded. Each shard owns an eviction policy and an open-addressing table whose slots hold the
// items themselves, key and value inline next to a meta word. A memory hit is one lookup deep, takes no locks and
// allocates nothing. The second level is any adapter that implements L2Cache.
package memstash

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"iter"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/zakonnic/memstash/internal/eviction"
	"github.com/zakonnic/memstash/internal/itemstore"
)

// LoaderFunc loads a value when both levels miss. It returns ErrKeyNotFound for a key the source does not have.
type LoaderFunc[K comparable, V any] func(ctx context.Context, key K) (V, error)

// BatchLoaderFunc loads several values at once when both levels miss. A key omitted from the returned List is
// treated as "not found" rather than an error.
type BatchLoaderFunc[K comparable, V any] func(ctx context.Context, keys []K) (List[K, V], error)

// ErrKeyNotFound is a loader's answer for a key the source does not have. Nothing is cached, and GetOrLoad returns
// it, also to a GetOrLoad waiting on a batch load that omitted its key.
var ErrKeyNotFound = errors.New("memstash: key not found")

const (
	// TickInterval is how often the coarse clock behind TTLs advances.
	TickInterval = time.Second
	// DefaultMemoryCapacity is the item capacity of a cache that sets neither MemoryCapacity nor MemoryBudget.
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

	// maxTableSlots is the largest table a shard can address.
	maxTableSlots = itemstore.MaxSlots

	// maxTTL caps the TTL at ~146 years. A TTL near math.MaxInt64, the usual way to write forever, would overflow the
	// lifetimes the expiry scale computes.
	maxTTL = time.Duration(math.MaxInt64 / 2)

	// shardBytes pins the shard layout the read path depends on: the table pointer and the move sequence alone on the
	// first cache line, the mutex and everything a writer touches past it, and a whole number of lines overall so that
	// the shards of one cache keep that separation down the array. The padding makes it 192 with 4- and 8-byte words.
	shardBytes = 192

	cacheLine = 64
	wordBytes = unsafe.Sizeof(uintptr(0))
)

// shard is one segment of the first level, picked by the key's hash. Every mutation runs under the shard mutex.
// Readers never take it: they probe the atomically published table and validate candidates against the items.
type shard[K comparable, V any] struct {
	// The first cache line is the read path's: a probe loads the table and the move sequence, and a writer taking
	// the mutex must not bounce that line out of every reader's cache. shardBytes pins the layout.
	items itemstore.StorageProxy[K, V] // the item table, swapped wholesale on growth
	// moveSeq is odd while a reclaim moves items. A lock-free probe that misses checks it was even and unchanged;
	// otherwise a key may have crossed the probe, and the miss is not trusted.
	moveSeq atomic.Uint32
	_       [cacheLine - 2*wordBytes - 4]byte // items is two words

	mu      sync.Mutex
	policy  EvictionPolicy[K, V]
	weight  atomic.Int64
	cap     int64
	live    int     // alive items; guarded by mu
	scratch []int64 // Reclaim's buffer for the run it compacts; guarded by mu
	sweepAt uint32  // the next slot the TTL sweep looks at; guarded by mu
	// evicted is set once the policy has picked a victim. From then on its queues carry more than insertion order,
	// and growth must keep the queue order. Guarded by mu.
	evicted bool
	// deletes counts each stripe's Deletes. A value read from L2 or a loader is stored only if its stripe's count
	// still equals the mark taken before the read; otherwise it may be the value deleted.
	deletes [deleteStripes]atomic.Uint32

	_ [cacheLine - 6*wordBytes]byte // policy 2 words, live 1, scratch 3; the rest from mu on is 64 bytes
}

// deleteStripes splits a shard's Delete count by hash bits 7..9, which the shard, the tag and the home do not use:
// a Delete refuses only the fills of its own eighth of the shard.
const deleteStripes = 8

func deleteStripe(keyHash uint64) uint64 { return keyHash >> 7 & (deleteStripes - 1) }

// markAt is the Delete count of keyHash's stripe as it stands, taken by a fill before it reads.
func (c *Cache[K, V]) markAt(keyHash uint64) uint32 {
	if !c.fills.Load() {
		c.fills.Store(true)
	}
	return c.shards[uint32(keyHash)&c.shardMask].deletes[deleteStripe(keyHash)].Load()
}

func (c *Cache[K, V]) deleteMark(key K) uint32 { return c.markAt(c.keyHash(key)) }

// fillMode is what setMemory does with a value that was read rather than written, given its key's mark.
type fillMode uint8

const (
	write fillMode = iota // Set: replaces whatever the key holds
	// fill answers a miss from L2 or a loader: it keeps a live value, which was written after the miss, and gives way
	// to a Delete since the mark.
	fill
	// refresh is GetAndRefresh's reload: it replaces the value the key holds, which is usually live, and gives way to
	// a Delete since the mark.
	refresh
)

// Cache is a two-level cache, safe for concurrent use.
type Cache[K comparable, V any] struct {
	costFunc func(key K, value V) uint32

	shards    []shard[K, V]
	shardMask uint32
	// preallocated pins the table size: the whole capacity is already there, so the table never grows.
	preallocated bool
	nanKeys      bool // K can hold a NaN, and a key may be unequal to itself
	scalarValues bool // V is written by one store (itemstore.OneStore), so an overwrite skips the seqlock bracket
	// growthNum is Config.GrowthThreshold as a numerator over 1<<growthShift.
	growthNum int64
	// onDeletion sits with the fields the write path already touches: every removal site tests it, so a cache without
	// a handler pays one predicted branch off a hot cache line.
	onDeletion func(key K, value V, cause DeletionCause)
	seed       maphash.Seed

	// The coarse clock for TTL checks: nowOff is the time since epoch in expireUnit steps, advanced by clockLoop when
	// TTL > 0. Offsets wrap at 18 bits, so the unit grows with the TTL: 1s up to a ~36h TTL, then just enough to fit,
	// which keeps the granularity under 0.001% of the TTL.
	epoch        time.Time
	nowOff       atomic.Uint32
	ttlOff       uint32
	expireUnit   time.Duration // wall time per offset step: ceil(TTL/ExpireMax), the smallest unit that fits the TTL
	ttl          time.Duration
	refreshOnGet bool

	l2Cache       L2Cache[K, V]
	l2WritePolicy WritePolicy // always WriteDisabled when l2Cache not set
	// fills turns true once a value may come from L2 or a loader; from then on a Delete marks its stripe. A fill sets
	// it before it takes its mark, so a Delete that still saw it false came before that read.
	fills             atomic.Bool
	writeBackBatching WriteBackBatching
	writeBackSingle   bool // the L2 is a SingleCallL2 and sized the pool: runs go out through deliverEach
	batchedGetsDirect bool // the L2 is a SingleCallL2 without a multi-get: GetBatched reads like Get
	onL2Error         func(key K, err error)
	onPanic           PanicHandler
	// writeQueues is one buffer per write-back worker; a key always lands on the same one, so its L2 writes stay
	// ordered even though the workers run concurrently.
	writeQueues []chan l2Write[K, V]

	// batchedGets is the read pool GetBatched coalesces through; it stays dormant until the first call reaches it.
	batchedGets getWorkerPool[K, V]

	flights []flightBucket[K, V]
	stats   counters

	stop      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once

	// refreshes counts the GetAndRefresh loads in flight: Close cancels their context (life) and waits for them.
	// refreshMu keeps one from registering while Close waits, which sync.WaitGroup does not allow.
	refreshes sync.WaitGroup
	refreshMu sync.RWMutex
	life      context.Context
	endLife   context.CancelFunc
}

// New creates a cache configured by the options. Close it when it runs background goroutines: when a TTL is set,
// when L2 is written with WriteBack (the default write policy), once GetBatched has been called, or while a
// GetAndRefresh load may be out. Otherwise Close is optional.
func New[K comparable, V any](opts ...Option) (*Cache[K, V], error) {
	cfg, err := buildConfig[K, V](opts)
	if err != nil {
		return nil, err
	}

	return NewWithConfig(cfg)
}

// NewWithConfig creates a cache from a Config. MemoryCapacity 0 means DefaultMemoryCapacity, unless CostFunc is set:
// a weighted cache must size its capacity explicitly. cfg is not modified, so one Config can build several caches.
func NewWithConfig[K comparable, V any](cfg *Config[K, V]) (*Cache[K, V], error) {
	var local Config[K, V]
	if cfg != nil {
		local = *cfg
	}
	cfg = &local
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
		if cfg.CostFunc != nil { // a weighted cache must say its capacity
			return nil, ErrBadCapacity
		}
		cfg.MemoryCapacity = DefaultMemoryCapacity
	}
	// With CostFunc the capacity is a weight, not an item count, so MaxItems does not bound it.
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
	if cfg.WritePolicy > WriteDisabled {
		return nil, ErrUnknownWritePolicy
	}
	if cfg.WriteBackBatching > BatchingAdaptive {
		return nil, ErrUnknownBatching
	}
	if cfg.TTL < 0 {
		return nil, ErrBadTTL
	}
	cfg.TTL = min(cfg.TTL, maxTTL)
	if cfg.WriteBackWorkers < 0 {
		return nil, ErrBadWorkers
	}
	if _, single := cfg.L2Cache.(SingleCallL2); single && cfg.WriteBackWorkers > 0 {
		return nil, ErrWorkersSetByL2
	}
	if cfg.GetBatchedWorkers < 0 {
		return nil, ErrBadGetBatchedWorkers
	}
	switch {
	case cfg.ShardsCount < 0:
		return nil, ErrBadShardsCount
	case cfg.GhostSize < 0:
		return nil, ErrBadGhostSize
	case cfg.WriteBackBufferSize < 0 || cfg.GetBatchedBufferSize < 0:
		return nil, ErrBadBufferSize
	}

	numShards := cfg.shardCount()
	growthNum := max(int64(math.Round(cfg.growthThreshold()*(1<<growthShift))), 1) // 0 would grow on every insert
	c := &Cache[K, V]{
		costFunc:          cfg.CostFunc,
		shards:            make([]shard[K, V], numShards),
		flights:           make([]flightBucket[K, V], numShards),
		shardMask:         uint32(numShards - 1),
		preallocated:      cfg.PreallocateMap,
		nanKeys:           canBeNaN(reflect.TypeFor[K]()),
		scalarValues:      itemstore.OneStore(reflect.TypeFor[V]()),
		growthNum:         growthNum,
		seed:              maphash.MakeSeed(),
		epoch:             time.Now(),
		ttl:               cfg.TTL,
		l2Cache:           cfg.L2Cache,
		l2WritePolicy:     cfg.WritePolicy,
		writeBackBatching: cfg.WriteBackBatching,
		onL2Error:         cfg.OnL2Error,
		onPanic:           cfg.OnPanic,
		onDeletion:        cfg.OnDeletion,
		stats:             newCounters(cfg.StatsEnabled),
		stop:              make(chan struct{}),
		batchedGets:       getWorkerPool[K, V]{size: cfg.getBatchedBuffer(), workers: cfg.getBatchedWorkers()},
	}
	if c.l2Cache == nil {
		c.l2WritePolicy = WriteDisabled
	} else {
		c.fills.Store(true)
	}
	if store, ok := cfg.L2Cache.(SingleCallL2); ok {
		c.batchedGetsDirect = !store.HasMultiGet()
	}
	c.life, c.endLife = context.WithCancel(context.Background())

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
		sh.items.Store(itemstore.NewFlatHashMap[K, V](slots, c.seed))
		sh.items.SetCostFunc(cfg.CostFunc)
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
		ttlSec := ceilDiv(cfg.TTL, time.Second)
		unitSec := max((ttlSec+itemstore.ExpireMax-1)/itemstore.ExpireMax, 1)
		c.expireUnit = time.Duration(unitSec) * time.Second
		// The cap keeps ttlOff+1 (see expireOffsetAt) within half the wrapping scale.
		c.ttlOff = uint32(min((ttlSec+unitSec-1)/unitSec, itemstore.ExpireMax-1))
		c.refreshOnGet = cfg.RefreshTTLOnGet
		c.wg.Add(1)
		go c.clockLoop()
	}

	if c.l2WritePolicy == WriteBack {
		workers, buffer := cfg.writeBackWorkers(), cfg.writeBackBuffer()
		if store, ok := cfg.L2Cache.(SingleCallL2); ok {
			c.writeBackSingle = true
			workers = max(store.ConcurrentCalls(), 1)
			if cfg.WriteBackBufferSize == 0 {
				buffer = max(singleCallBufferTotal/workers, singleCallBufferMin)
			}
		}
		c.writeQueues = make([]chan l2Write[K, V], workers)
		c.wg.Add(workers)
		for i := range c.writeQueues {
			c.writeQueues[i] = make(chan l2Write[K, V], buffer)
			go c.writeBackLoop(c.writeQueues[i])
		}
	}
	return c, nil
}

// Close stops the background goroutines. It waits for the write-back buffer to drain and for GetAndRefresh loads,
// whose context it cancels. Repeated calls are safe.
//
// A Set that starts after Close returns still reaches L2, synchronously. A Set racing Close may lose its asynchronous
// write, and one still waiting for buffer room writes synchronously, possibly ahead of the drain. A GetBatched waiting
// on the read pool loses nothing: it reads L2 itself.
func (c *Cache[K, V]) Close() {
	c.closeOnce.Do(func() {
		c.refreshMu.Lock()
		close(c.stop)
		c.refreshMu.Unlock()
		c.endLife()
		c.batchedGets.close() // before the wait below: see getWorkerPool.close
		c.refreshes.Wait()
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

// Wait blocks until every write-back write enqueued before the call has been handed to L2. It is a checkpoint, not a
// shutdown: the cache keeps serving. It returns ctx's error if ctx ends first, which a stuck L2 can cause, and
// ErrClosed once the cache is closing, since Close drains the buffer itself. With WriteThrough or WriteDisabled it
// returns at once.
func (c *Cache[K, V]) Wait(ctx context.Context) error {
	if len(c.writeQueues) == 0 {
		return nil
	}
	// Marker sits behind everything enqueued into that queue before this call.
	markers := make([]chan struct{}, len(c.writeQueues))
	for i, queue := range c.writeQueues {
		markers[i] = make(chan struct{})
		select {
		case queue <- l2Write[K, V]{flush: markers[i]}:
		case <-c.stop:
			return ErrClosed
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for _, marker := range markers {
		select {
		case <-marker:
		case <-c.stop:
			return ErrClosed
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// shardAndHash returns the key's shard and hash. The low bits pick the shard and the high bits the home slot, so one
// hash serves both.
func (c *Cache[K, V]) shardAndHash(key K) (*shard[K, V], uint64) {
	keyHash := maphash.Comparable(c.seed, key)
	return &c.shards[uint32(keyHash)&c.shardMask], keyHash
}

func (c *Cache[K, V]) keyHash(key K) uint64 {
	return maphash.Comparable(c.seed, key)
}

// writeQueueOf returns the key's write-back queue.
func (c *Cache[K, V]) writeQueueOf(key K) chan l2Write[K, V] {
	if len(c.writeQueues) == 1 {
		return c.writeQueues[0]
	}
	return c.writeQueues[(c.keyHash(key)>>32)%uint64(len(c.writeQueues))]
}

// Get returns the value from memory or, on a miss, from L2 if one is configured, and promotes a value found there into
// memory. A memory hit takes no lock and allocates nothing. A promoted value gets the cache's full TTL, whatever L2 had
// left, so memory may serve it up to a TTL after L2 dropped it.
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
	mark := c.deleteMark(key)
	value, ok, err := c.l2Cache.Get(ctx, key)
	if err != nil || !ok {
		c.stats.addL2Misses(1)
		var zero V
		return zero, false, err
	}
	c.stats.addL2Hits(1)
	c.setMemory(key, value, c.expireOffset(), fill, mark)
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
	keyHash := maphash.Comparable(c.seed, key)
	sh := &c.shards[uint32(keyHash)&c.shardMask]
	tagged := tagOf(keyHash)
	seq := sh.moveSeq.Load()
	storage := sh.items.GetStorage()
	home := storage.Home(keyHash)
	var entry itemstore.Entry[K, V]
	var zero V
	for pos := home; ; {
		item := storage.At(pos)
		metaWord := item.Metadata()
		for metaWord&itemstore.DeadOrTag == tagged { // a dead slot or a foreign tag ends it
			if !item.SnapshotInto(&entry, metaWord) {
				metaWord = item.Metadata()
				continue // the copy raced a recycle or an overwrite: retry against the fresh meta word
			}
			if entry.Key != key {
				break // tag collision or a recycled slot
			}
			nowOff := c.nowOff.Load()
			if !expired(metaWord, nowOff) {
				if c.refreshOnGet { // touchHit, inlined by hand: it is past the inlining budget
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
		// At a group's edge the walk goes on only if a key with this tag lives past the edge of home's group. Asked
		// in two steps: joined by &&, the answer goes through a stack slot.
		if pos++; pos&itemstore.GroupMask == 0 {
			if !storage.OverflowedTag(home, overflowBucket(keyHash)) {
				break
			}
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
	for pos := home; ; {
		item := storage.At(pos)
		metaWord := item.Metadata()
		for metaWord&itemstore.DeadOrTag == tagged {
			if !item.SnapshotInto(&entry, metaWord) {
				metaWord = item.Metadata()
				continue
			}
			if entry.Key != key {
				break
			}
			nowOff := c.nowOff.Load()
			if !expired(metaWord, nowOff) {
				c.touchHit(item, metaWord, nowOff)
				return entry.Value, true
			}
			c.dropExpired(sh, keyHash, key)
			return zero, false
		}
		if metaWord&itemstore.HasNext == 0 {
			break
		}
		if pos++; pos&itemstore.GroupMask == 0 {
			if !storage.OverflowedTag(home, overflowBucket(keyHash)) {
				break
			}
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
	nowOff := c.nowOff.Load()
	if expired(metaWord, nowOff) {
		return zero, false
	}
	c.touchHit(item, metaWord, nowOff)
	return item.Entry().Value, true
}

// touchHit grants a hit its reference and, with RefreshTTLOnGet, a full TTL.
func (c *Cache[K, V]) touchHit(item *itemstore.Item[K, V], metaWord uint64, nowOff uint32) {
	if c.refreshOnGet {
		expireOff := uint32(metaWord & itemstore.ExpireMask >> itemstore.ExpireShift)
		if newOff := c.expireOffsetAt(nowOff); newOff != expireOff && item.TouchAndRefreshExpire(metaWord, newOff) {
			return
		}
	}
	item.TouchWith(metaWord)
}

// Set stores the value in memory and in L2 according to WritePolicy. An error can come only from a synchronous L2
// write. A key unequal to itself (a NaN, or a struct holding one) never enters memory: no read could find it.
func (c *Cache[K, V]) Set(ctx context.Context, key K, value V) error {
	c.setMemory(key, value, c.expireOffset(), write, 0)
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

// SetWithTTL is Set with a lifetime of its own for this entry. The cache must have a TTL (ErrTTLDisabled otherwise),
// and ttl must be positive (ErrBadTTL); it is rounded up to the expiry unit. A write-back batch carries one TTL, so
// writes with different TTLs reach L2 in separate batches. With WithRefreshTTLOnGet, the first read that refreshes
// the entry gives it the cache's TTL instead.
func (c *Cache[K, V]) SetWithTTL(ctx context.Context, key K, value V, ttl time.Duration) error {
	if c.ttlOff == 0 {
		return ErrTTLDisabled
	}
	if ttl <= 0 {
		return ErrBadTTL
	}
	expireOff, effectiveTTL := c.expireOffsetFor(ttl, itemstore.ExpireMax-1)
	c.setMemory(key, value, expireOff, write, 0)
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

// expireOffsetFor is expireOffset for the caller's lifetime. It also returns the lifetime the item actually got,
// rounded up to expireUnit and capped at maxUnits, which is what L2 must get too. TTL must be enabled.
func (c *Cache[K, V]) expireOffsetFor(ttl time.Duration, maxUnits uint32) (uint32, time.Duration) {
	units := uint32(min(ceilDiv(ttl, c.expireUnit), int64(maxUnits)))
	expireOff := (c.nowOff.Load() + units + 1) & itemstore.ExpireWrapMask
	if expireOff == 0 {
		expireOff = itemstore.ExpireWrapMask // see expireOffsetAt
	}
	return expireOff, time.Duration(units) * c.expireUnit
}

// setMemory stores the value in the first level with expiration offset expireOff (0: no TTL). An overwrite writes the
// item in place; a new key takes the first empty slot on its walk. Neither allocates.
//
// A value that was read rather than written gives way as mode and mark say, and then setMemory returns false.
func (c *Cache[K, V]) setMemory(key K, value V, expireOff uint32, mode fillMode, mark uint32) bool {
	if c.nanKeys && key != key { // no read could find it, and its hash changes from call to call
		return true
	}
	weight := c.rawCost(key, value)
	sh, keyHash := c.shardAndHash(key)
	var deletions []deletion[K, V] // stays nil unless a handler is configured
	deletes := &sh.deletes[deleteStripe(keyHash)]
	if weight > sh.cap {
		// Does not fit at all; drop the older value too so it stops serving reads.
		sh.mu.Lock()
		if mode != write && deletes.Load() != mark {
			sh.mu.Unlock()
			return false
		}
		if mode == fill {
			if _, item, ok := c.findSlot(sh, keyHash, key); ok && !expired(item.Metadata(), c.nowOff.Load()) {
				sh.mu.Unlock()
				return false
			}
		}
		deletes.Add(1) // the drop is a delete: a fill of the dropped value must not bring it back
		c.deleteLocked(sh, keyHash, key, CauseOverflow, &deletions)
		sh.mu.Unlock()
		c.stats.addRejected(1)
		if len(deletions) > 0 {
			c.callOnDeletion(deletions)
		}
		return true
	}

	sh.mu.Lock()
	if mode != write && deletes.Load() != mark {
		sh.mu.Unlock()
		return false
	}
	storage := sh.items.GetStorage()
	tagged := tagOf(keyHash)
	home := storage.Home(keyHash)
	weightDelta := weight
	for pos := home; ; pos++ {
		item := storage.At(pos)
		metaWord := item.Metadata()
		if empty(metaWord) { // deletions compact their runs, so the key cannot live past an empty slot
			if sh.live+1 >= storage.Len() { // the last free slot stays free: a probe needs one to end at
				weightDelta = 0
				c.stats.addRejected(1)
				break
			}
			idx := storage.Wrap(pos)
			item.Publish(itemstore.Entry[K, V]{Key: key, Value: value}, tagged, expireOff, storage.Dist(home, idx))
			storage.NoteDisplaced(keyHash, pos)
			storage.MarkChain(home, idx)
			storage.Enqueue(0, idx)
			sh.live++
			sh.policy.Add(itemstore.QNode{Idx: idx, Cost: uint32(weight)})
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
		if mode == fill && !expired(metaWord, c.nowOff.Load()) {
			sh.mu.Unlock()
			return false
		}
		// Overwrite in place; the old value is read for the weight delta before the store lands.
		if weightDelta = weight - c.rawCost(key, entry.Value); weightDelta != 0 {
			sh.policy.Reweigh(storage.Wrap(pos), weightDelta)
		}
		if c.onDeletion != nil {
			c.addDeletion(&deletions, key, entry.Value, CauseReplacement)
		}
		if c.scalarValues {
			item.SetScalar(value)
		} else {
			item.SetValue(value)
		}
		if expireOff != 0 {
			item.RefreshExpire(expireOff)
		}
		break
	}
	if weightDelta != 0 { // an overwrite of the same weight - the common one - leaves the total alone
		storeI64(&sh.weight, sh.weight.Load()+weightDelta)
	}
	// A Delete of a key memory lacks takes no lock. The RMW orders this check against its bump: a Delete that bumped
	// first is seen here, and one that bumps later probes after the value above is visible, and kills it.
	if mode != write && deletes.Add(0) != mark {
		c.deleteLocked(sh, keyHash, key, CauseInvalidation, &deletions)
		sh.mu.Unlock()
		if len(deletions) > 0 {
			c.callOnDeletion(deletions)
		}
		return false
	}
	if sh.weight.Load() > sh.cap {
		c.evictShard(sh, &deletions)
	}
	sh.mu.Unlock()
	if len(deletions) > 0 {
		c.callOnDeletion(deletions)
	}
	return true
}

// maybeGrow replaces the shard's table with a bigger one once the live items pass the growth threshold. The items
// are copied into a table nobody reads yet, then the pointer is swapped: a probe still on the old table sees the
// cache as of the copy, the Get-racing-Set window.
//
// A preallocated table never grows: it already fits the whole capacity, and growing would double a footprint the
// caller sized up front.
func (c *Cache[K, V]) maybeGrow(sh *shard[K, V], t *itemstore.FlatHashMap[K, V]) {
	lastSlot := sh.live+1 >= t.Len() // setMemory keeps it free: at a threshold near 1 the table must grow first
	// int64: with a 32-bit int both sides of the line overflow from ~524k items.
	if c.preallocated || int64(t.Len()) >= maxTableSlots ||
		!lastSlot && int64(sh.live)<<growthShift < int64(t.Len())*c.growthNum {
		return
	}
	grown := t.Grow(itemstore.GrowSlots(t.Len()), sh.evicted)
	sh.items.Store(grown)
	sh.sweepAt = uint32(uint64(sh.sweepAt) * uint64(grown.Len()) / uint64(t.Len())) // homes scale with the table
	c.stats.addTableGrowths(1)
}

// evictShard evicts items while the shard's weight exceeds its capacity. The policy picks the victims; the removal
// happens here.
func (c *Cache[K, V]) evictShard(sh *shard[K, V], deletions *[]deletion[K, V]) {
	nowOff := c.nowOff.Load()
	storage := sh.items.GetStorage()
	sh.evicted = true
	for sh.weight.Load() > sh.cap {
		victimIdx, ok := sh.policy.Evict(nowOff)
		if !ok || victimIdx >= uint32(storage.Len()) {
			return
		}
		item := storage.At(victimIdx)
		metaWord := item.Metadata()
		if empty(metaWord) || metaWord&itemstore.Dead != 0 {
			return // a policy's bug: killing a free slot would corrupt the shard's counts
		}
		// The policy hands over expired items too; the meta word says which of the two it was.
		cause := CauseEviction
		if expired(metaWord, nowOff) {
			cause = CauseExpiration
		}
		c.killAt(sh, victimIdx, item, cause, deletions)
	}
}

// dropExpired lazily removes a TTL-expired item found by the Get path.
func (c *Cache[K, V]) dropExpired(sh *shard[K, V], h uint64, key K) {
	sh.mu.Lock()
	idx, item, ok := c.findSlot(sh, h, key)
	// Recheck under the lock: the TTL may have been refreshed, and a reclaim may have moved the item.
	var deletions []deletion[K, V] // stays nil unless a handler is configured
	if ok && expired(item.Metadata(), c.nowOff.Load()) {
		c.killAt(sh, idx, item, CauseExpiration, &deletions)
	}
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
		for pos := home; ; {
			item := storage.At(pos)
			metaWord := item.Metadata()
			for metaWord&itemstore.DeadOrTag == tagged {
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
				break
			}
			if pos++; pos&itemstore.GroupMask == 0 {
				if !storage.OverflowedTag(home, overflowBucket(keyHash)) {
					break
				}
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

// killAt removes the item at idx: out of its eviction queue, killed, its weight subtracted and its slot reclaimed on
// the spot. It leaves its queue before it dies, so a policy never meets a dead item.
func (c *Cache[K, V]) killAt(sh *shard[K, V], idx uint32, item *itemstore.Item[K, V], cause DeletionCause,
	deletions *[]deletion[K, V]) {
	entry := item.Entry()
	weight := c.rawCost(entry.Key, entry.Value)
	storeI64(&sh.weight, sh.weight.Load()-weight)
	if c.onDeletion != nil {
		c.addDeletion(deletions, entry.Key, entry.Value, cause)
	}
	sh.policy.Remove(itemstore.QNode{Idx: idx, Cost: uint32(weight)})
	storage := sh.items.GetStorage()
	storage.Remove(idx)
	storage.ForgetDisplacedOf(idx, item)
	item.Kill()
	sh.live--
	storage.Reclaim(idx, &sh.scratch, &sh.moveSeq)
}

// findSlot finds the key's live slot. The caller holds the shard mutex: the key is read without a snapshot.
func (c *Cache[K, V]) findSlot(sh *shard[K, V], keyHash uint64, key K) (uint32, *itemstore.Item[K, V], bool) {
	storage := sh.items.GetStorage()
	tagged := tagOf(keyHash)
	home := storage.Home(keyHash)
	for pos := home; ; {
		item := storage.At(pos)
		metaWord := item.Metadata()
		if metaWord&itemstore.DeadOrTag == tagged && item.Entry().Key == key {
			return storage.Wrap(pos), item, true
		}
		if metaWord&itemstore.HasNext == 0 {
			return 0, nil, false
		}
		if pos++; pos&itemstore.GroupMask == 0 {
			if !storage.OverflowedTag(home, overflowBucket(keyHash)) {
				return 0, nil, false
			}
		}
	}
}

// deleteLocked removes the key if memory holds it.
func (c *Cache[K, V]) deleteLocked(sh *shard[K, V], keyHash uint64, key K, cause DeletionCause,
	deletions *[]deletion[K, V]) {
	if idx, item, ok := c.findSlot(sh, keyHash, key); ok {
		c.killAt(sh, idx, item, cause, deletions)
	}
}

// Delete removes the key from memory and forwards the deletion to L2 according to WritePolicy (async for WriteBack).
func (c *Cache[K, V]) Delete(ctx context.Context, key K) error {
	var err error
	if c.l2WritePolicy == WriteThrough {
		err = c.l2Cache.Delete(ctx, key) // first: a fill marked after the bump below reads L2 after this
	}
	sh, keyHash := c.shardAndHash(key)
	if c.fills.Load() {
		sh.deletes[deleteStripe(keyHash)].Add(1)
	}
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
	if c.l2WritePolicy == WriteBack {
		c.enqueueDelete(key)
	}
	return err
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

	keyHash := c.keyHash(key)
	for {
		call := &flightCall[K, V]{}
		winner, running := c.claim(keyHash, key, call)
		if !running {
			return c.loadFlight(ctx, key, keyHash, load, call)
		}
		// Another call is loading the key: wait for it or for ctx, while the owner keeps loading for the others. This
		// call missed memory and never reaches L2 itself, so it counts as a memory miss.
		c.stats.addMemMisses(1)
		if err := winner.wait(ctx); err != nil {
			var zero V
			return zero, err
		}
		if !winner.canceled || ctx.Err() != nil {
			return winner.val, winner.err
		}
		c.stats.addMemMisses(-1) // the next attempt counts this read
	}
}

// loadFlight loads the key as the owner of call and hands the result to everyone waiting on it.
func (c *Cache[K, V]) loadFlight(ctx context.Context, key K, keyHash uint64, load LoaderFunc[K, V], call *flightCall[K, V]) (V, error) {
	// ErrLoaderPanic stands until doLoad returns: a loader that panics or Goexits must not leave waiters stuck.
	call.err = ErrLoaderPanic
	defer c.release(call)

	value, err := c.doLoad(ctx, key, keyHash, load)
	call.val, call.err, call.ok = value, err, err == nil
	call.canceled = err != nil && ctx.Err() != nil
	return value, err
}

func (c *Cache[K, V]) doLoad(ctx context.Context, key K, keyHash uint64, load LoaderFunc[K, V]) (V, error) {
	// A parallel flight may have finished while we were registering.
	if value, ok := c.getMemory(key); ok {
		c.stats.addMemHits(1)
		return value, nil
	}
	mark := c.markAt(keyHash)
	if c.l2Cache != nil {
		value, ok, err := c.l2Cache.Get(ctx, key)
		switch {
		case err == nil && ok:
			c.stats.addL2Hits(1)
			c.setMemory(key, value, c.expireOffset(), fill, mark)
			return value, nil
		case err != nil:
			c.reportL2Err(key, err) // and fall back to the loader
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
	c.storeLoaded(ctx, key, value, fill, mark)
	return value, nil
}

// storeLoaded caches a freshly loaded value and forwards it to L2 per WritePolicy. A value that gives way in memory
// stays out of L2 too: the write or Delete it gave way to reached L2 itself. The value is already in hand, so an L2
// write error is reported rather than returned.
func (c *Cache[K, V]) storeLoaded(ctx context.Context, key K, value V, mode fillMode, mark uint32) {
	if !c.setMemory(key, value, c.expireOffset(), mode, mark) {
		return
	}
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

// Iterator returns an iterator over the live first-level entries; L2 is not scanned. The walk is lock-free and weakly
// consistent: an entry written, removed, or moved by a deletion during the walk may or may not be seen. A key that
// stays in the cache throughout is yielded at most once, and a yielded pair is never torn.
func (c *Cache[K, V]) Iterator() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		c.iterateWithLife(func(key K, value V, _ time.Duration) bool { return yield(key, value) })
	}
}

// iterateWithLife is the Iterator walk, plus the life each item has left (0 when it has no TTL), which SaveTo needs.
//
// No key that stays is yielded twice. A moved item dies in its old slot before it goes live in the new one, and the
// only move that can carry an item past the walk a second time crosses the table's end: out of the run that wraps
// onto the first slots, back to the last ones. So the walk leaves the wrapped items, those homed past their slot, to
// a final pass over that run. One that moved to the end meanwhile was seen there instead; one that moves away later
// is not seen, like any concurrent removal.
func (c *Cache[K, V]) iterateWithLife(yield func(K, V, time.Duration) bool) {
	var entry itemstore.Entry[K, V]
	for i := range c.shards {
		storage := c.shards[i].items.GetStorage()
		nowOff := c.nowOff.Load()
		size := uint32(storage.Len())
		wrapped := func(pos uint32, metaWord uint64) bool {
			if disp := itemstore.DispOf(metaWord); disp != itemstore.DispMax || pos < itemstore.DispMax {
				return disp > pos
			}
			return storage.Home(c.keyHash(entry.Key)) > pos // the distance saturated: only the hash knows
		}
		for pos := range size {
			item := storage.At(pos)
			metaWord := item.Metadata()
			if empty(metaWord) || metaWord&itemstore.Dead != 0 || expired(metaWord, nowOff) {
				continue
			}
			if !item.SnapshotInto(&entry, metaWord) || wrapped(pos, metaWord) {
				continue
			}
			if !yield(entry.Key, entry.Value, c.lifeLeft(metaWord, nowOff)) {
				return
			}
		}
		for pos := range size {
			item := storage.At(pos)
			metaWord := item.Metadata()
			if empty(metaWord) {
				break // the wrapping run ends here
			}
			if metaWord&itemstore.Dead != 0 || expired(metaWord, nowOff) {
				continue
			}
			if !item.SnapshotInto(&entry, metaWord) || !wrapped(pos, metaWord) {
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

// ceilDiv is d/unit rounded up; (d+unit-1)/unit would overflow for a d near math.MaxInt64, the "forever" TTL.
func ceilDiv(d, unit time.Duration) int64 {
	q := int64(d / unit)
	if d%unit > 0 {
		q++
	}
	return q
}

// Len returns the number of first-level items, expired ones not yet removed included.
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

// expireOffset returns a new item's expiration offset, 0 when TTL is disabled. An item lives at least its TTL and at
// most one clock unit more. The exception is the one unit in 2^18 where the offset would wrap onto 0: then it lives
// up to a unit short.
func (c *Cache[K, V]) expireOffset() uint32 {
	if c.ttlOff == 0 {
		return 0
	}
	return c.expireOffsetAt(c.nowOff.Load())
}

// expireOffsetAt is expireOffset at a loaded clock value; TTL must be enabled. The +1 covers the coarse clock. nowOff
// lags real time by up to a tick, so without it an item written just before a tick would expire almost at once.
// Offset 0 means no TTL, so a wrap that lands there moves one unit earlier. Moving it later would push a TTL at the
// cap past ExpireMax, and the item would be expired at birth.
func (c *Cache[K, V]) expireOffsetAt(nowOff uint32) uint32 {
	expireOff := (nowOff + c.ttlOff + 1) & itemstore.ExpireWrapMask
	if expireOff == 0 {
		expireOff = itemstore.ExpireWrapMask
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

// deliver runs one write-back call to L2. An adapter that panics costs that batch, not the worker: the panic comes back
// as an ErrPanic error, which the caller reports for every key of the batch.
func (c *Cache[K, V]) deliver(call func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrPanic, r)
			c.notifyPanic(r, c.onL2Error != nil)
		}
	}()
	return call()
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
			nowOff := uint32(time.Since(c.epoch)/c.expireUnit) & itemstore.ExpireWrapMask
			if units := (nowOff - c.nowOff.Load()) & itemstore.ExpireWrapMask; units > 0 {
				c.nowOff.Store(nowOff)
				c.sweep(nowOff, units)
			}
		case <-c.stop:
			return
		}
	}
}

// sweepUnits paces the TTL sweep: a lap over a table takes that many clock units plus one per item it removes. An
// expired offset reads as live again ExpireMax+2 units on; a clock that jumps further than that (a process suspended
// for as long) cannot be helped with 18 bits.
const sweepUnits = 1 << 14

// sweep kills the expired items in the next stretch of every table, sized for units clock units of the pass.
func (c *Cache[K, V]) sweep(nowOff, units uint32) {
	for i := range c.shards {
		c.sweepShard(&c.shards[i], nowOff, units)
	}
}

func (c *Cache[K, V]) sweepShard(sh *shard[K, V], nowOff, units uint32) {
	defer c.recoverWorker()
	if deletions := c.sweepLocked(sh, nowOff, units); len(deletions) > 0 {
		c.callOnDeletion(deletions)
	}
}

func (c *Cache[K, V]) sweepLocked(sh *shard[K, V], nowOff, units uint32) (deletions []deletion[K, V]) {
	sh.mu.Lock()
	defer sh.mu.Unlock() // killAt runs user code: CostFunc, a custom policy
	storage := sh.items.GetStorage()
	size := uint32(storage.Len())
	for n := min(uint64(size), (uint64(size)*uint64(units)+sweepUnits-1)/sweepUnits); n > 0; n-- {
		if sh.sweepAt >= size {
			sh.sweepAt = 0
		}
		if item := storage.At(sh.sweepAt); expired(item.Metadata(), nowOff) {
			c.killAt(sh, sh.sweepAt, item, CauseExpiration, &deletions)
			continue // the compaction may have moved the next item into this slot
		}
		sh.sweepAt++
	}
	return deletions
}

// WriteBackBatchMax caps one write-back batch: the writes are flushed through the adapter's BatchSet.
const WriteBackBatchMax = 128

// writeBackFanout is how many batches a worker keeps in flight once its queue holds more than a batch.
const writeBackFanout = 4

// writeBackRunMax caps what a worker takes off its queue at once: 3/4 of writeBackFanout batches, because the split
// by key hash is uneven. A part of distinct keys then gets 96 tasks with a binomial deviation of 8.5, so one past
// WriteBackBatchMax, which costs it a second batch, is nearly 4 deviations out.
const writeBackRunMax = writeBackFanout * WriteBackBatchMax * 3 / 4

// singleCallBufferTotal is what a SingleCallL2 pool buffers by default, shared by its workers: as much as the batched
// pool's default workers do.
const singleCallBufferTotal = DefaultWriteBackWorkers * defaultWriteBackBuffer

// singleCallBufferMin is the smallest default share: 64 tasks still collapse many queued writes of a hot key into one
// call.
const singleCallBufferMin = 64

// flushWrite hands one write-back task to L2, coalescing the tasks already queued behind it in the same worker's
// queue into batches.
func (c *Cache[K, V]) flushWrite(queue chan l2Write[K, V], first l2Write[K, V]) {
	for more := true; more; {
		if first.flush != nil {
			close(first.flush) // a Wait checkpoint: everything enqueued before it has already been flushed
			return
		}
		// len(queue) is the buffer fill counter the adaptive mode switches on.
		if c.writeBackBatching == BatchingNone ||
			(c.writeBackBatching == BatchingAdaptive && len(queue) <= cap(queue)/2) {
			c.deliverBatch(List[K, V]{{Key: first.key, Value: first.value}}, first.del, first.ttl)
			return
		}
		first, more = c.flushRun(queue, first)
	}
}

// flushRun takes the tasks of first's kind queued behind it and delivers them together. A delete travels as its key
// with a zero value. A run ends where the kind changes - a delete after writes, or a write carrying a different ttl.
func (c *Cache[K, V]) flushRun(queue chan l2Write[K, V], first l2Write[K, V]) (next l2Write[K, V], more bool) {
	run := List[K, V]{{Key: first.key, Value: first.value}}
	for len(run) < writeBackRunMax {
		select {
		case write := <-queue:
			switch {
			case write.flush != nil:
				defer close(write.flush) // a Wait checkpoint must be released even if the delivery panics
				c.deliverRun(run, first.del, first.ttl)
				return next, false
			case write.del != first.del || write.ttl != first.ttl:
				c.deliverRun(run, first.del, first.ttl)
				return write, true
			default:
				run = append(run, KeyVal[K, V]{Key: write.key, Value: write.value})
			}
		default:
			c.deliverRun(run, first.del, first.ttl)
			return next, false
		}
	}
	c.deliverRun(run, first.del, first.ttl)
	return next, false
}

// deliverRun hands a run to L2: one batch, or writeBackFanout parts at once when it is more than a batch, each part
// in batches one after another. The split is by key, so the tasks of a key share a part and reach L2 in the order
// they were queued.
func (c *Cache[K, V]) deliverRun(run List[K, V], del bool, ttl time.Duration) {
	if c.writeBackSingle {
		c.deliverEach(run, del, ttl)
		return
	}
	if len(run) <= WriteBackBatchMax {
		c.deliverBatch(run, del, ttl)
		return
	}
	var wg sync.WaitGroup
	for _, part := range c.splitByKey(run) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for len(part) > 0 {
				n := min(len(part), WriteBackBatchMax)
				c.deliverBatch(part[:n:n], del, ttl) // capped: an adapter appending to its batch stays out of the next
				part = part[n:]
			}
		}()
	}
	wg.Wait()
}

// deliverEach hands a run to a SingleCallL2 one call per key, newest task first. A key's older tasks in the run are
// dropped, as a batch would drop them.
func (c *Cache[K, V]) deliverEach(run List[K, V], del bool, ttl time.Duration) {
	if len(run) == 1 {
		c.deliverBatch(run, del, ttl)
		return
	}
	sent := make(map[K]struct{}, len(run))
	for i := len(run) - 1; i >= 0; i-- {
		if _, dup := sent[run[i].Key]; dup {
			continue
		}
		sent[run[i].Key] = struct{}{}
		c.deliverBatch(run[i:i+1:i+1], del, ttl)
	}
}

// splitByKey sorts run into writeBackFanout parts by key hash, keeping the order within a part. The low hash bits
// pick the part: writeQueueOf spent the high ones.
func (c *Cache[K, V]) splitByKey(run List[K, V]) (parts [writeBackFanout]List[K, V]) {
	var partOf [writeBackRunMax]uint8
	var sizes [writeBackFanout]int
	for i, item := range run {
		partOf[i] = uint8(uint32(c.keyHash(item.Key)) % writeBackFanout)
		sizes[partOf[i]]++
	}
	sorted := make(List[K, V], len(run))
	start := 0
	for part, size := range sizes {
		parts[part] = sorted[start : start : start+size]
		start += size
	}
	for i, item := range run {
		parts[partOf[i]] = append(parts[partOf[i]], item)
	}
	return parts
}

func (c *Cache[K, V]) deliverBatch(batch List[K, V], del bool, ttl time.Duration) {
	if !del {
		c.writeBatch(batch, ttl)
		return
	}
	keys := make([]K, len(batch))
	for i, item := range batch {
		keys[i] = item.Key
	}
	c.deleteBatch(keys)
}

// writeBatch delivers drained writes to L2: one Set for a single item, one BatchSet otherwise. A batch error is
// reported for every key it concerns (see KeyError).
func (c *Cache[K, V]) writeBatch(batch List[K, V], ttl time.Duration) {
	defer c.recoverWorker() // deliver recovers the adapter: a panic reaching here is OnL2Error's own
	err := c.deliver(func() error {
		if len(batch) == 1 {
			return c.l2Cache.Set(context.Background(), batch[0].Key, batch[0].Value, ttl)
		}
		return c.l2Cache.BatchSet(context.Background(), batch, ttl)
	})
	if err != nil {
		for _, item := range batch {
			if err := keyErr(err, item.Key); err != nil {
				c.reportL2Err(item.Key, err)
			}
		}
	}
}

// deleteBatch delivers drained deletes to L2: one Delete for a single key, one BatchDelete otherwise. A batch error
// is reported for every key it concerns (see KeyError).
func (c *Cache[K, V]) deleteBatch(keys []K) {
	defer c.recoverWorker() // as in writeBatch
	err := c.deliver(func() error {
		if len(keys) == 1 {
			return c.l2Cache.Delete(context.Background(), keys[0])
		}
		return c.l2Cache.BatchDelete(context.Background(), keys)
	})
	if err != nil {
		for _, key := range keys {
			if err := keyErr(err, key); err != nil {
				c.reportL2Err(key, err)
			}
		}
	}
}

// writeBackLoop is one worker of the asynchronous L2 write pool, draining the queue it owns. On shutdown it flushes
// everything left in that queue.
func (c *Cache[K, V]) writeBackLoop(queue chan l2Write[K, V]) {
	defer c.wg.Done()
	defer c.recoverWorker() // deliver recovers per batch, so reaching this means the loop itself broke
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
