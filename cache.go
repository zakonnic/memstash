// Package memstash is an ultra-fast two-level cache.
//
// The first level is a thread-safe map (xsync) plus sharded eviction state: per-item state records sit by value
// inside the pool's chunks and are reused without allocations, while the policies (Clock and S3-FIFO) run on chunked
// FIFO queues of nodes. A memory hit costs one map lookup and one atomic metadata read - no locks and no allocations.
// The second level is any adapter that implements L2Cache.
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

// cacheItem is the item as stored in the map: the payload, a pointer to the item's state record, and the generation
// the record was handed out to this key at. A generation mismatch means the record has already been reused, i.e. the
// item was evicted.
type cacheItem[K comparable, V any] struct {
	value V
	state *itemstate.State[K]
	gen   uint32
}

// shard is an independent segment of the eviction state. A key is always served by the same shard (by hash), so all map
// mutations for that key are serialized by its shard mutex - this is the core map <-> pool consistency invariant.
type shard[K comparable, V any] struct {
	mu        sync.Mutex
	policy    eviction.Policy[K]
	pool      itemstate.Pool[K]
	weight    atomic.Int64
	cap       int64
	deadCount int // tombstones queued by Delete / lazy TTL removal, not yet reclaimed; guarded by mu

	_ [64]byte // spreads shards across cache lines
}

// Cache is a two-level cache.
type Cache[K comparable, V any] struct {
	items    *xsync.MapOf[K, cacheItem[K, V]]
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
	if cfg.Policy != PolicyClock && cfg.Policy != PolicyS3FIFO {
		return nil, ErrUnknownPolicy
	}

	numShards := cfg.shardCount()
	c := &Cache[K, V]{
		items:         xsync.NewMapOf[K, cacheItem[K, V]](),
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
		switch cfg.Policy {
		case PolicyS3FIFO:
			sh.policy = eviction.NewS3FIFO[K](sh.cap, ghostPerShard)
		case PolicyClock:
			sh.policy = eviction.NewClockPolicy[K]()
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
func (c *Cache[K, V]) Close() {
	c.closeOnce.Do(func() {
		close(c.stop)
		c.wg.Wait()
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

// shardOf returns the shard that owns the key.
func (c *Cache[K, V]) shardOf(key K) *shard[K, V] {
	return &c.shards[uint32(maphash.Comparable(c.seed, key))&c.shardMask]
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

func (c *Cache[K, V]) getMemory(key K) (V, bool) {
	item, ok := c.items.Load(key)
	if ok {
		metaWord := item.state.Load()
		// A single meta load checks both the generation and the tombstone bit: a state record reused by another key is
		// rejected by the generation comparison.
		if uint32(metaWord) == item.gen && metaWord&itemstate.Dead == 0 {
			if !itemstate.Expired(metaWord, c.nowOff.Load()) {
				item.state.TouchWith(metaWord)
				return item.value, true
			}
			// TTL has elapsed - drop the item lazily instead of waiting for the eviction queue to reach it.
			c.dropExpired(key, item.state, item.gen)
		}
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
// Allocation-free fast path: overwriting an existing key updates the value in place (the state record, its queue
// node, and the weight are adjusted rather than recreated); a new key gets a record from the pool (freelist or
// chunk). The only unavoidable allocation is the internal xsync-map entry.
func (c *Cache[K, V]) setMemory(key K, value V) {
	weight := c.rawCost(key, value)
	sh := c.shardOf(key)
	if weight > sh.cap {
		// The item plainly does not fit - do not let it wreck the cache.
		return
	}

	var (
		claimedState *itemstate.State[K]
		weightDelta  = weight
	)
	sh.mu.Lock()
	c.items.Compute(key, func(old cacheItem[K, V], loaded bool) (cacheItem[K, V], bool) {
		if loaded {
			// Overwrite in place: the state record and its position in the queue stay the same.
			weightDelta = weight - c.rawCost(key, old.value)
			if c.ttlSec != 0 {
				c.refreshExpire(old.state)
			}
			return cacheItem[K, V]{value: value, state: old.state, gen: old.gen}, false
		}
		newState, gen := sh.pool.Claim(key, c.expireOffset())
		claimedState = newState
		return cacheItem[K, V]{value: value, state: newState, gen: gen}, false
	})
	if claimedState != nil {
		sh.policy.Add(itemstate.QNode[K]{State: claimedState, Cost: uint32(weight)})
	}
	if sh.weight.Add(weightDelta) > sh.cap {
		c.evictShard(sh)
	}
	sh.mu.Unlock()
}

// refreshExpire extends the item's TTL while preserving its generation and reference counter. A race with a
// concurrent touch may lose one second chance - that is harmless.
func (c *Cache[K, V]) refreshExpire(state *itemstate.State[K]) {
	state.RefreshExpire(c.expireOffset())
}

// evictShard evicts items from the shard while its weight exceeds the capacity. Called under the shard mutex.
func (c *Cache[K, V]) evictShard(sh *shard[K, V]) {
	nowOff := c.nowOff.Load()
	for sh.weight.Load() > sh.cap {
		victim, ok := sh.policy.Evict(nowOff)
		if !ok {
			return
		}
		c.unlink(sh, victim)
		sh.pool.Release(victim)
	}
}

// unlink removes the map entry that corresponds to exactly this state record in its current generation and subtracts
// its weight. For items that died earlier (Delete, lazy TTL removal) the entry is already gone and its weight already
// subtracted - Compute finds nothing and zero is subtracted.
func (c *Cache[K, V]) unlink(sh *shard[K, V], victim *itemstate.State[K]) {
	var removedWeight int64
	victimGen := victim.Gen()
	c.items.Compute(victim.Key, func(old cacheItem[K, V], loaded bool) (cacheItem[K, V], bool) {
		if !loaded {
			// No entry (Delete/TTL already removed it): delete as a no-op - returning (old, false) would insert a zero
			// value.
			return old, true
		}
		if old.state == victim && old.gen == victimGen {
			removedWeight = c.rawCost(victim.Key, old.value)
			return old, true
		}
		return old, false // the key is already backed by a different state record - leave it alone
	})
	sh.weight.Add(-removedWeight)
}

// dropExpired lazily removes a TTL-expired item from the Get path. The state record stays in the queue as a tombstone
// and returns to the pool on the next eviction pass or tombstone sweep.
func (c *Cache[K, V]) dropExpired(key K, state *itemstate.State[K], gen uint32) {
	sh := c.shardOf(key)
	sh.mu.Lock()
	metaWord := state.Load()
	if uint32(metaWord) == gen && metaWord&itemstate.Dead == 0 && itemstate.Expired(metaWord, c.nowOff.Load()) {
		state.Kill()
		if entry, ok := c.items.LoadAndDelete(key); ok {
			sh.weight.Add(-c.rawCost(key, entry.value))
		}
		c.noteDead(sh)
	}
	sh.mu.Unlock()
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
		sh.policy.Sweep(func(state *itemstate.State[K]) { sh.pool.Release(state) })
		sh.deadCount = 0
	}
}

// Delete removes the key from memory and from L2 (synchronously, unless L2 writes are disabled). The state record
// returns to the pool on the next eviction pass or tombstone sweep.
func (c *Cache[K, V]) Delete(ctx context.Context, key K) error {
	sh := c.shardOf(key)
	sh.mu.Lock()
	if entry, ok := c.items.LoadAndDelete(key); ok {
		entry.state.Kill()
		sh.weight.Add(-c.rawCost(key, entry.value))
		c.noteDead(sh)
	}
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

	value, err := c.doLoad(ctx, key, load)
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
		resolved, err := c.batchLoad(ctx, owned, load)
		loadErr = err

		// Publish every resolved value to its flight.
		for _, item := range resolved {
			call := ownedCalls[item.Key]
			call.val, call.ok = item.Value, true
			*found = append(*found, item)
		}
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
func (c *Cache[K, V]) Len() int { return c.items.Size() }

// Weight returns the current total weight of live first-level items.
func (c *Cache[K, V]) Weight() int64 {
	var total int64
	for i := range c.shards {
		total += c.shards[i].weight.Load()
	}
	return total
}

const xsyncBucketBytes = 64

// TotalWeight estimates the total memory footprint of the cache's first-level structures.
func (c *Cache[K, V]) TotalWeight() int64 {
	stats := c.items.Stats()
	var zeroKey K
	entryBytes := int64(unsafe.Sizeof(zeroKey)) + int64(unsafe.Sizeof(cacheItem[K, V]{}))
	total := int64(stats.TotalBuckets)*xsyncBucketBytes + int64(stats.Size)*entryBytes

	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		total += sh.pool.Bytes() + sh.policy.Bytes()
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
func (c *Cache[K, V]) expireOffset() uint32 {
	if c.ttlSec == 0 {
		return 0
	}
	expireOff := c.nowOff.Load() + c.ttlSec
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

// enqueueWriteBack puts a write into the worker's buffer; on overflow or when the cache is closed items writes
// synchronously so no data is lost.
func (c *Cache[K, V]) enqueueWriteBack(key K, value V) {
	select {
	case c.writeCh <- l2Write[K, V]{key: key, value: value}:
	default:
		if err := c.l2Cache.Set(context.Background(), key, value, c.ttl); err != nil {
			c.reportL2Err(key, err)
		}
	}
}

// writeBackLoop is the background worker for asynchronous L2 writes. On shutdown items flushes everything left in the
// buffer.
func (c *Cache[K, V]) writeBackLoop() {
	defer c.wg.Done()
	flush := func(write l2Write[K, V]) {
		if write.flush != nil {
			close(write.flush) // a Wait checkpoint: everything enqueued before it has already been flushed
			return
		}
		if err := c.l2Cache.Set(context.Background(), write.key, write.value, c.ttl); err != nil {
			c.reportL2Err(write.key, err)
		}
	}
	for {
		select {
		case write := <-c.writeCh:
			flush(write)
		case <-c.stop:
			for {
				select {
				case write := <-c.writeCh:
					flush(write)
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
