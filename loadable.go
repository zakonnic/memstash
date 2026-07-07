package memstash

import "context"

// LoadableCache is a cache with a loader fixed at construction time: GetOrLoad takes only a key.
type LoadableCache[K comparable, V any] struct {
	c         *Cache[K, V]
	load      LoaderFunc[K, V]
	batchLoad BatchLoaderFunc[K, V]
}

// NewLoadable creates a LoadableCache configured by the options (see New). load is required.
func NewLoadable[K comparable, V any](load LoaderFunc[K, V], opts ...Option) (*LoadableCache[K, V], error) {
	if load == nil {
		return nil, ErrNilLoader
	}
	c, err := New[K, V](opts...)
	if err != nil {
		return nil, err
	}
	batchLoad := func(ctx context.Context, missing []K) (List[K, V], error) {
		loaded := make(List[K, V], 0, len(missing))
		for _, key := range missing {
			value, err := load(ctx, key)
			if err != nil {
				return loaded, err
			}
			loaded = append(loaded, KeyVal[K, V]{Key: key, Value: value})
		}
		return loaded, nil
	}
	return &LoadableCache[K, V]{c: c, load: load, batchLoad: batchLoad}, nil
}

// NewBatchLoadable creates a LoadableCache configured by the options (see New). load is required.
func NewBatchLoadable[K comparable, V any](batchLoad BatchLoaderFunc[K, V], opts ...Option) (*LoadableCache[K, V], error) {
	if batchLoad == nil {
		return nil, ErrNilLoader
	}
	c, err := New[K, V](opts...)
	if err != nil {
		return nil, err
	}
	load := func(ctx context.Context, key K) (V, error) {
		res, err := batchLoad(ctx, []K{key})
		if err != nil {
			var zero V
			return zero, err
		}
		if len(res) > 0 { // at most one item for a single key
			return res[0].Value, nil
		}
		var zero V
		return zero, nil
	}
	return &LoadableCache[K, V]{c: c, load: load, batchLoad: batchLoad}, nil
}

// GetOrLoad returns the value, loading it with the constructor's loader on a miss (with singleflight coalescing of
// concurrent calls).
func (lc *LoadableCache[K, V]) GetOrLoad(ctx context.Context, key K) (V, error) {
	return lc.c.GetOrLoad(ctx, key, lc.load)
}

// BatchGetOrLoad The batch loader is synthesized from the constructor's single-key loader (sequential calls);
// when the data source has a real batch API, prefer Cache.BatchGetOrLoad with a BatchLoaderFunc.
func (lc *LoadableCache[K, V]) BatchGetOrLoad(ctx context.Context, keys []K) (List[K, V], error) {
	return lc.c.BatchGetOrLoad(ctx, keys, lc.batchLoad)
}

// Get: see Cache.Get.
func (lc *LoadableCache[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
	return lc.c.Get(ctx, key)
}

// BatchGet: see Cache.BatchGet.
func (lc *LoadableCache[K, V]) BatchGet(ctx context.Context, keys []K) (List[K, V], error) {
	return lc.c.BatchGet(ctx, keys)
}

// Set: see Cache.Set.
func (lc *LoadableCache[K, V]) Set(ctx context.Context, key K, value V) error {
	return lc.c.Set(ctx, key, value)
}

// BatchSet: see Cache.BatchSet.
func (lc *LoadableCache[K, V]) BatchSet(ctx context.Context, items List[K, V]) error {
	return lc.c.BatchSet(ctx, items)
}

// Delete: see Cache.Delete.
func (lc *LoadableCache[K, V]) Delete(ctx context.Context, key K) error {
	return lc.c.Delete(ctx, key)
}

// Close: see Cache.Close.
func (lc *LoadableCache[K, V]) Close() { lc.c.Close() }

// Wait: see Cache.Wait.
func (lc *LoadableCache[K, V]) Wait() { lc.c.Wait() }

// Cache exposes the underlying cache (for example, for Len/Weight).
func (lc *LoadableCache[K, V]) Cache() *Cache[K, V] { return lc.c }
