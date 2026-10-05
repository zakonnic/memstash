package memstash

import (
	"context"
	"hash/maphash"

	"github.com/zakonnic/memstash/internal/itemstore"
)

// GetAndRefresh returns what memory holds for key - a value past its TTL included, as long as no read has reclaimed it -
// and reloads the key in the background, where the result replaces the cached value and follows WritePolicy into L2.
//
// ctx is used for its values only: the load outlives this call, and Close cancels it and waits for it. A load error,
// ErrKeyNotFound included, leaves the current value in place; a GetOrLoad waiting on the same key receives it.
// Concurrent loads of one key are coalesced into one.
func (c *Cache[K, V]) GetAndRefresh(ctx context.Context, key K, load LoaderFunc[K, V]) (V, bool) {
	value, ok := c.peekMemory(key)
	if ok {
		c.stats.addMemHits(1)
	} else {
		c.stats.addMemMisses(1)
	}
	if load == nil || c.closing() {
		return value, ok
	}
	// ErrLoaderPanic stands from the moment the flight is visible:
	// a loader that panics or Goexits must not leave waiters stuck.
	call := &flightCall[K, V]{err: ErrLoaderPanic}
	if _, running := c.claim(c.keyHash(key), key, call); running {
		return value, ok
	}
	if !c.refresh(ctx, func(ctx context.Context) { c.loadInto(ctx, key, load, call) }) {
		call.err = context.Canceled // Close came in between: the flight ends unloaded
		c.release(call)
	}
	return value, ok
}

// BatchGetAndRefresh is GetAndRefresh for many keys: one background load covers the whole set, minus the keys another
// load is already resolving.
func (c *Cache[K, V]) BatchGetAndRefresh(ctx context.Context, keys []K, load BatchLoaderFunc[K, V]) List[K, V] {
	found := make(List[K, V], 0, len(keys))
	for _, key := range keys {
		if value, ok := c.peekMemory(key); ok {
			found = append(found, KeyVal[K, V]{Key: key, Value: value})
		}
	}
	c.stats.addMemHits(int64(len(found)))
	c.stats.addMemMisses(int64(len(keys) - len(found)))
	if load == nil || len(keys) == 0 || c.closing() {
		return found
	}
	keys = append([]K(nil), keys...) // the caller may reuse its slice the moment this returns
	c.refresh(ctx, func(ctx context.Context) { c.loadBatchInto(ctx, keys, load) })
	return found
}

// refresh runs load on a goroutine of its own, under ctx's values and the cache's lifetime, unless Close has begun.
func (c *Cache[K, V]) refresh(ctx context.Context, load func(context.Context)) bool {
	c.refreshMu.RLock()
	defer c.refreshMu.RUnlock()
	if c.closing() {
		return false
	}
	c.refreshes.Add(1)
	go func() {
		defer c.refreshes.Done()
		ctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		defer cancel()
		defer context.AfterFunc(c.life, cancel)()
		load(ctx)
	}()
	return true
}

// closing reports whether Close has been called; a refresh started after that has no worker left to outlive it.
func (c *Cache[K, V]) closing() bool {
	select {
	case <-c.stop:
		return true
	default:
		return false
	}
}

func (c *Cache[K, V]) loadInto(ctx context.Context, key K, load LoaderFunc[K, V], call *flightCall[K, V]) {
	defer c.release(call)
	defer c.recoverLoader(&call.err)

	value, err := load(ctx, key)
	if err != nil {
		call.err = err
		return
	}
	c.storeLoaded(ctx, key, value)
	call.val, call.ok, call.err = value, true, nil
}

func (c *Cache[K, V]) loadBatchInto(ctx context.Context, keys []K, load BatchLoaderFunc[K, V]) {
	var fl batchFlights[K, V]
	for i, key := range keys {
		fl.claim(c, keys, i, c.keyHash(key))
	}
	if len(fl.owned) == 0 {
		return // every key is already being loaded; that load publishes for them
	}
	defer c.releaseAll(fl.calls)
	defer c.recoverLoaderBatch(fl.calls)

	loaded, err := load(ctx, fl.owned)
	c.storeLoadedBatch(ctx, loaded) // a partial result is still worth caching
	assignLoaded(fl.calls, loaded, err, nil)
}

func (c *Cache[K, V]) storeLoadedBatch(ctx context.Context, loaded List[K, V]) {
	if len(loaded) == 0 {
		return
	}
	expireOff := c.expireOffset()
	for _, item := range loaded {
		c.setMemory(item.Key, item.Value, expireOff)
	}
	c.stats.addSets(int64(len(loaded)))
	switch c.l2WritePolicy {
	case WriteDisabled:
	case WriteThrough:
		if err := c.l2Cache.BatchSet(ctx, loaded, c.ttl); err != nil {
			for _, item := range loaded {
				c.reportL2Err(item.Key, err)
			}
		}
	default:
		for _, item := range loaded {
			c.enqueueWriteBack(item.Key, item.Value, c.ttl)
		}
	}
}

// peekMemory is getMemory without the TTL rules: an expired value is returned instead of reclaimed and the reference
// counter is left alone. A miss that overlapped a reclaim is probed again, the third time under the mutex.
func (c *Cache[K, V]) peekMemory(key K) (V, bool) {
	keyHash := maphash.Comparable(c.seed, key)
	sh := &c.shards[uint32(keyHash)&c.shardMask]
	tagged := tagOf(keyHash)
	var entry itemstore.Entry[K, V]
	var zero V
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
				if entry.Key != key {
					break
				}
				return entry.Value, true
			}
			if metaWord&itemstore.HasNext == 0 {
				break probe
			}
		}
		if seq&1 == 0 && sh.moveSeq.Load() == seq {
			return zero, false
		}
		if attempt == 2 {
			sh.mu.Lock()
			_, item, ok := c.findSlot(sh, keyHash, key)
			if ok {
				entry = *item.Entry()
			}
			sh.mu.Unlock()
			return entry.Value, ok
		}
	}
}
