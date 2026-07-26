package memstash

import "context"

// LoadableCache is a Cache with a loader fixed at construction time: GetOrLoad and BatchGetOrLoad take only keys.
// Every other Cache method is promoted as-is; the loader-taking Cache.GetOrLoad stays reachable as lc.Cache.GetOrLoad.
type LoadableCache[K comparable, V any] struct {
	*Cache[K, V]
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
	return &LoadableCache[K, V]{Cache: c, load: load, batchLoad: batchLoad}, nil
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
	return &LoadableCache[K, V]{Cache: c, load: load, batchLoad: batchLoad}, nil
}

// GetOrLoad returns the value, loading it with the constructor's loader on a miss (with singleflight coalescing of
// concurrent calls).
func (lc *LoadableCache[K, V]) GetOrLoad(ctx context.Context, key K) (V, error) {
	return lc.Cache.GetOrLoad(ctx, key, lc.load)
}

// BatchGetOrLoad returns the values for keys, resolving misses with the constructor's loader.
//
// NewLoadable synthesizes the batch loader from the single-key one (sequential calls); when the data source has a real
// batch API, build the cache with NewBatchLoadable instead.
func (lc *LoadableCache[K, V]) BatchGetOrLoad(ctx context.Context, keys []K) (List[K, V], error) {
	return lc.Cache.BatchGetOrLoad(ctx, keys, lc.batchLoad)
}
