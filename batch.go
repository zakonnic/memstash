package memstash

import (
	"context"

	"github.com/zakonnic/memstash/internal/itemstore"
)

// batchGroupSize is the pipeline width of the batched memory lookups: enough independent loads in flight to cover
// the miss latency of the next level, small enough to keep the scratch state on one stack frame.
const batchGroupSize = 12

// batchGroup is batchGetGroup's per-group scratch buffer. The caller keeps one zeroed instance for the entire batch.
type batchGroup[K comparable, V any] struct {
	keyHash [batchGroupSize]uint64
	sh      [batchGroupSize]*shard[K, V]
	table   [batchGroupSize]*itemstore.FlatHashMap[K, V]
	pos     [batchGroupSize]uint32
	item    [batchGroupSize]*itemstore.Item[K, V]
	meta    [batchGroupSize]uint64
	value   [batchGroupSize]V
	hit     [batchGroupSize]bool
}

// BatchGetFromMemory looks the keys up in memory only and appends every found pair to dst, which it returns;
// Lookups are software-pipelined in groups. Pass a reused dst (dst[:0]) to keep the call allocation-free.
func (c *Cache[K, V]) BatchGetFromMemory(keys []K, dst List[K, V]) List[K, V] {
	dst, _, hits := c.batchGetMemory(keys, dst, false)
	c.stats.addMemHits(hits)
	c.stats.addMemMisses(int64(len(keys)) - hits)
	return dst
}

// BatchGetFromMemoryWithMissing is BatchGetFromMemory that also returns the keys not found, appended to a fresh
// slice.
func (c *Cache[K, V]) BatchGetFromMemoryWithMissing(keys []K, dst List[K, V]) (List[K, V], []K) {
	dst, missing, hits := c.batchGetMemory(keys, dst, true)
	c.stats.addMemHits(hits)
	c.stats.addMemMisses(int64(len(keys)) - hits)
	return dst, missing
}

// smallBatch is where the pipeline stops paying for itself: batchGroup's scratch buffer is zeroed per call whatever
// the key count, and below this it costs more than the overlapped misses save.
const smallBatch = 4

// batchGetMemory reads the keys from memory. dst and the missing list are grown on first use and sized for the whole
// batch, so a call that hits nothing (or misses nothing) allocates nothing.
func (c *Cache[K, V]) batchGetMemory(keys []K, dst List[K, V], wantMissing bool) (List[K, V], []K, int64) {
	var missing []K
	hits := int64(0)
	if len(keys) <= smallBatch {
		for _, key := range keys {
			if value, ok := c.getMemory(key); ok {
				if dst == nil {
					dst = make(List[K, V], 0, len(keys))
				}
				dst = append(dst, KeyVal[K, V]{Key: key, Value: value})
				hits++
			} else if wantMissing {
				if missing == nil {
					missing = make([]K, 0, len(keys))
				}
				missing = append(missing, key)
			}
		}
		return dst, missing, hits
	}
	var group batchGroup[K, V]
	for start := 0; start < len(keys); start += batchGroupSize {
		part := keys[start:min(start+batchGroupSize, len(keys))]
		c.batchGetGroup(part, &group)
		for i := range part {
			if group.hit[i] {
				if dst == nil {
					dst = make(List[K, V], 0, len(keys))
				}
				dst = append(dst, KeyVal[K, V]{Key: part[i], Value: group.value[i]})
				hits++
			} else if wantMissing {
				if missing == nil {
					missing = make([]K, 0, len(keys))
				}
				missing = append(missing, part[i])
			}
		}
	}
	return dst, missing, hits
}

// batchGetGroup resolves up to batchGroupSize keys against the first level.
// Anything off the straight path (a dead or mismatched candidate, a racing write) falls back to the single-key
// getMemory: rare, and its slot lines are already warm.
func (c *Cache[K, V]) batchGetGroup(keys []K, g *batchGroup[K, V]) {
	// hash, home slot, and the group's first meta loads.
	for i := range keys {
		sh, keyHash := c.shardAndHash(keys[i])
		storage := sh.items.GetStorage()
		pos := storage.Home(keyHash)
		g.sh[i], g.keyHash[i], g.table[i], g.pos[i] = sh, keyHash, storage, pos
		g.meta[i] = storage.At(pos).Load()
	}
	// walk each probe chain to its first candidate (or a definite miss); a candidate is its own slot, so the
	// walk already loaded the line the validation stage needs.
	for i := range keys {
		t := g.table[i]
		tagged := itemstore.TagOf(g.keyHash[i])
		home := g.pos[i]
		groupEnd := (home | 7) + 1
		metaWord, pos := g.meta[i], home
		for {
			if metaWord == 0 || (pos == groupEnd && !t.Overflowed(home)) {
				g.item[i] = nil // end of the probe chain: a definite miss
				break
			}
			if metaWord&itemstore.DeadOrTag == tagged {
				g.item[i] = t.At(pos)
				g.meta[i] = metaWord
				g.pos[i] = t.Wrap(pos)
				break
			}
			pos++
			metaWord = t.At(pos).Load()
		}
	}
	// validate the candidates against the now-loaded items.
	var entry itemstore.Entry[K, V]
	for i := range keys {
		item := g.item[i]
		if item == nil {
			g.hit[i] = false
			continue
		}
		metaWord := g.meta[i]
		if !item.SnapshotInto(&entry, metaWord) || entry.Key != keys[i] {
			g.value[i], g.hit[i] = c.getMemory(keys[i]) // tag collision or a racing write
			continue
		}
		nowOff := c.nowOff.Load()
		if itemstore.Expired(metaWord, nowOff) {
			c.dropExpired(g.sh[i], g.keyHash[i], keys[i], g.pos[i])
			g.hit[i] = false
			continue
		}
		if c.refreshOnGet {
			expireOff := uint32(metaWord & itemstore.ExpireMask >> itemstore.ExpireShift)
			if newOff := c.expireOffsetAt(nowOff); newOff != expireOff && item.TouchAndRefreshExpire(metaWord, newOff) {
				g.value[i], g.hit[i] = entry.Value, true
				continue
			}
		}
		item.TouchWith(metaWord)
		g.value[i], g.hit[i] = entry.Value, true
	}
}

// BatchGet returns the values for keys found in memory or in L2: the memory hits are collected first, the misses go
// to L2 in a single BatchGet and are promoted into memory. A missing key is simply absent from the result. On an L2
// error the memory part gathered so far is returned alongside the error.
func (c *Cache[K, V]) BatchGet(ctx context.Context, keys []K) (List[K, V], error) {
	if c.l2Cache == nil {
		found, _, hits := c.batchGetMemory(keys, nil, false)
		c.stats.addMemHits(hits)
		c.stats.addMemMisses(int64(len(keys)) - hits)
		return found, nil
	}
	found, missing, hits := c.batchGetMemory(keys, nil, true)
	c.stats.addMemHits(hits)
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
		c.setMemory(item.Key, item.Value, c.expireOffset())
		found = append(found, item)
	}
	return found, nil
}

// BatchSet stores all items in memory and forwards them to L2 according to WritePolicy: WriteThrough issues a single
// batch write, WriteBack enqueues the items for the background worker. An error can come only from the synchronous
// batch write.
func (c *Cache[K, V]) BatchSet(ctx context.Context, items List[K, V]) error {
	for _, item := range items {
		c.setMemory(item.Key, item.Value, c.expireOffset())
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

// enqueueWriteBack puts a write into the worker's buffer; on overflow or when the cache is closed it writes
// synchronously so no data is lost.
func (c *Cache[K, V]) enqueueWriteBack(key K, value V) {
	c.enqueueL2(l2Write[K, V]{key: key, value: value})
}

// enqueueDelete is enqueueWriteBack for a delete task.
func (c *Cache[K, V]) enqueueDelete(key K) {
	c.enqueueL2(l2Write[K, V]{key: key, del: true})
}

// enqueueL2 hands a task to the write-back worker; on overflow or when the cache is closed the task runs
// synchronously so nothing is lost.
func (c *Cache[K, V]) enqueueL2(write l2Write[K, V]) {
	sync := func() {
		var err error
		if write.del {
			err = c.l2Cache.Delete(context.Background(), write.key)
		} else {
			err = c.l2Cache.Set(context.Background(), write.key, write.value, c.ttl)
		}
		if err != nil {
			c.reportL2Err(write.key, err)
		}
	}
	select {
	case <-c.stop:
		// The worker is gone (Close was called): a buffered send would park the task in a channel nobody drains.
		sync()
		return
	default:
	}
	select {
	case c.writeCh <- write:
	default:
		sync()
	}
}

// BatchDelete removes the keys from memory and forwards the deletions to L2 according to WritePolicy:
// WriteThrough issues one synchronous BatchDelete, WriteBack enqueues the deletes for the background worker,
// which always coalesces them into BatchDelete batches regardless of WriteBackBatching. An error can come only
// from the synchronous batch delete.
func (c *Cache[K, V]) BatchDelete(ctx context.Context, keys []K) error {
	var deletions []deletion[K, V] // stays nil unless a handler is configured
	for _, key := range keys {
		sh, keyHash := c.shardAndHash(key)
		sh.mu.Lock()
		c.deleteLocked(sh, keyHash, key, CauseInvalidation, &deletions)
		sh.mu.Unlock()
	}
	if len(deletions) > 0 {
		c.callOnDeletion(deletions)
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

// BatchGetOrLoad returns values for keys, resolving misses with one L2 BatchGet and at most one load call.
// Per-key singleflight is preserved: keys already being loaded elsewhere are waited on; the rest are loaded in a
// single load(ctx, missing) call. A key omitted by the loader is absent from the result (any concurrent GetOrLoad
// waiting on it gets the zero value). On error, the resolved part is returned with the error; errors are not cached.
func (c *Cache[K, V]) BatchGetOrLoad(ctx context.Context, keys []K, load BatchLoaderFunc[K, V]) (List[K, V], error) {
	if load == nil {
		return nil, ErrNilLoader
	}
	found, missing, hits := c.batchGetMemory(keys, nil, true)
	c.stats.addMemHits(hits)
	if len(missing) == 0 {
		return found, nil
	}

	joined, loadErr := c.singleflight(ctx, load, &found, missing)
	// Joined keys missed memory and go no further here - their owner does the L2 lookup (the owned keys are counted
	// by batchLoad).
	c.stats.addMemMisses(int64(len(joined)))

	for _, flight := range joined {
		if err := flight.call.wait(ctx); err != nil {
			return found, err
		}
		if flight.call.err == nil && flight.call.ok {
			found = append(found, KeyVal[K, V]{Key: flight.key, Value: flight.call.val})
		} else if loadErr == nil {
			loadErr = flight.call.err
		}
	}
	return found, loadErr
}

type joinedFlight[K comparable, V any] struct {
	key  K
	call *flightCall[V]
}

func (c *Cache[K, V]) singleflight(ctx context.Context, load BatchLoaderFunc[K, V], found *List[K, V], missing []K) ([]joinedFlight[K, V], error) {
	owned, calls, joined := c.claimBatch(missing)
	if len(owned) == 0 {
		return joined, nil
	}
	defer c.releaseAll(owned, calls)

	resolved, err := c.batchLoad(ctx, owned, load)
	assignLoaded(owned, calls, resolved, err, found)
	return joined, err
}

// batchLoad resolves the owned keys: first from L2 in one BatchGet, the rest with one loader call; freshly loaded
// values are stored back according to the write policy (mirroring doLoad). The returned List may be partial when
// err != nil.
func (c *Cache[K, V]) batchLoad(ctx context.Context, keys []K, load BatchLoaderFunc[K, V]) (List[K, V], error) {
	if c.l2Cache == nil {
		c.stats.addMemMisses(int64(len(keys)))
		return c.runLoader(ctx, keys, load)
	}
	resolved := make(List[K, V], 0, len(keys))
	resolvedKeys := make(map[K]struct{}, len(keys))
	toLoad := keys
	{
		fromL2, err := c.l2Cache.BatchGet(ctx, keys)
		if err != nil {
			// Fall back to the loader for everything; report the L2 error via the callback.
			for _, key := range keys {
				c.reportL2Err(key, err)
			}
		}
		for _, item := range fromL2 {
			c.setMemory(item.Key, item.Value, c.expireOffset())
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
	c.stats.addL2Misses(int64(len(toLoad)))
	loaded, err := c.runLoader(ctx, toLoad, load)
	return append(resolved, loaded...), err
}

// runLoader loads the keys and caches whatever came back, handing the loader's own list straight to the caller.
func (c *Cache[K, V]) runLoader(ctx context.Context, keys []K, load BatchLoaderFunc[K, V]) (List[K, V], error) {
	loaded, err := load(ctx, keys)
	if err != nil {
		return nil, err
	}
	c.storeLoadedBatch(ctx, loaded)
	return loaded, nil
}
