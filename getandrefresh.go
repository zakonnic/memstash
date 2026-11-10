package memstash

import (
	"context"
	"hash/maphash"

	"github.com/zakonnic/memstash/internal/itemstore"
)

// GetAndRefresh returns what memory holds for key, even a value past its TTL that nothing has reclaimed yet, and
// reloads the key in the background. The loaded value replaces the cached one and follows WritePolicy into L2.
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
	keyHash := c.keyHash(key)
	if _, running := c.claim(keyHash, key, call); running {
		return value, ok
	}
	call.mark = c.markAt(keyHash)
	if !c.refresh(ctx, func(ctx context.Context) { c.loadInto(ctx, key, load, call) }) {
		call.err, call.canceled = context.Canceled, true // Close came in between: the flight ends unloaded
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
		call.err, call.canceled = err, ctx.Err() != nil
		return
	}
	c.storeLoaded(ctx, key, value, refresh, call.mark)
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
	c.storeLoadedBatch(ctx, loaded, refresh, fl.calls) // a partial result is still worth caching
	assignLoaded(ctx, fl.calls, loaded, err, nil)
}

// storeLoadedBatch is storeLoaded for a batch, each key under the mark its flight took; L2 gets only the values memory
// took. A key the loader added unasked is a fill marked now.
func (c *Cache[K, V]) storeLoadedBatch(ctx context.Context, loaded List[K, V], mode fillMode, calls []flightCall[K, V]) {
	expireOff := c.expireOffset()
	var kept List[K, V] // stays nil while memory takes every value: then loaded itself goes to L2
	var index map[K]int
	for i, item := range loaded {
		itemMode, mark := fill, uint32(0)
		if at := ownedAt(calls, item.Key, i, &index); at >= 0 {
			itemMode, mark = mode, calls[at].mark
		} else {
			mark = c.deleteMark(item.Key)
		}
		stored := c.setMemory(item.Key, item.Value, expireOff, itemMode, mark)
		switch {
		case !stored && kept == nil:
			kept = append(make(List[K, V], 0, len(loaded)-1), loaded[:i]...)
		case stored && kept != nil:
			kept = append(kept, item)
		}
	}
	if kept == nil {
		kept = loaded
	}
	if len(kept) == 0 {
		return
	}
	c.stats.addSets(int64(len(kept)))
	switch c.l2WritePolicy {
	case WriteDisabled:
	case WriteThrough:
		if err := c.l2Cache.BatchSet(ctx, kept, c.ttl); err != nil {
			for _, item := range kept {
				if err := keyErr(err, item.Key); err != nil {
					c.reportL2Err(item.Key, err)
				}
			}
		}
	default:
		for _, item := range kept {
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
		for pos := home; ; {
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
