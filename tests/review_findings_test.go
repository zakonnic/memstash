//go:build review

// Package tests, review build: each test here reproduces a finding from API_REVIEW.md and fails on purpose,
// documenting the behaviour as it is today. Run with: go test -tags review -run TestReview -v ./tests/
package tests

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zakonnic/memstash"
)

// revL2 records every L2 operation in completion order and can hold its very first Set.
type revL2 struct {
	mu      sync.Mutex
	m       map[string]string
	log     []string
	ttls    []time.Duration
	first   atomic.Bool
	entered chan struct{}
	gate    chan struct{}
	blockOn string // only Sets of this value block; "" disables the gate
}

func newRevL2() *revL2 {
	return &revL2{m: map[string]string{}, entered: make(chan struct{}), gate: make(chan struct{})}
}

func (l *revL2) maybeBlock(value string) {
	if l.blockOn != "" && value == l.blockOn && l.first.CompareAndSwap(false, true) {
		close(l.entered)
		<-l.gate
	}
}

func (l *revL2) Get(_ context.Context, key string) (string, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.log = append(l.log, "get:"+key)
	v, ok := l.m[key]
	return v, ok, nil
}

func (l *revL2) BatchGet(_ context.Context, keys []string) (memstash.List[string, string], error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.log = append(l.log, "batchget:"+strings.Join(keys, ","))
	out := make(memstash.List[string, string], 0, len(keys))
	for _, k := range keys {
		if v, ok := l.m[k]; ok {
			out = append(out, memstash.KeyVal[string, string]{Key: k, Value: v})
		}
	}
	return out, nil
}

func (l *revL2) Set(_ context.Context, key, value string, ttl time.Duration) error {
	l.maybeBlock(value)
	l.mu.Lock()
	defer l.mu.Unlock()
	l.log = append(l.log, "set:"+key+"="+value)
	l.ttls = append(l.ttls, ttl)
	l.m[key] = value
	return nil
}

func (l *revL2) BatchSet(_ context.Context, items memstash.List[string, string], ttl time.Duration) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, it := range items {
		l.log = append(l.log, "bset:"+it.Key+"="+it.Value)
		l.ttls = append(l.ttls, ttl)
		l.m[it.Key] = it.Value
	}
	return nil
}

func (l *revL2) Delete(_ context.Context, key string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.log = append(l.log, "del:"+key)
	delete(l.m, key)
	return nil
}

func (l *revL2) BatchDelete(_ context.Context, keys []string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, k := range keys {
		l.log = append(l.log, "bdel:"+k)
		delete(l.m, k)
	}
	return nil
}

func (l *revL2) snapshot() (map[string]string, []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	m := make(map[string]string, len(l.m))
	for k, v := range l.m {
		m[k] = v
	}
	return m, append([]string(nil), l.log...)
}

// --- F1: write-back buffer overflow reorders writes of one key -------------------------------------------------

func TestReviewWriteBackOverflowReordersSet(t *testing.T) {
	l2 := newRevL2()
	l2.blockOn = "v1"
	cache, err := memstash.New[string, string](
		memstash.WithMemoryCapacity(100),
		memstash.WithL2Cache[string, string](l2),
		memstash.WithWritePolicy(memstash.WriteBack),
		memstash.WithWriteBackWorkers(1),
		memstash.WithWriteBackBufferSize(1),
		memstash.WithWriteBackBatching(memstash.BatchingNone),
	)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, cache.Set(ctx, "k", "v1")) // taken by the worker, blocks inside L2.Set
	<-l2.entered
	require.NoError(t, cache.Set(ctx, "k", "v2")) // fills the 1-slot buffer
	require.NoError(t, cache.Set(ctx, "k", "v3")) // buffer full -> written synchronously, right now
	close(l2.gate)
	cache.Close()

	m, log := l2.snapshot()
	t.Logf("L2 write order: %v", log)
	t.Logf("L2 final value for k: %q (last Set by the caller was v3)", m["k"])
	if m["k"] != "v3" {
		t.Errorf("STALE: L2 holds %q after Set(v1),Set(v2),Set(v3)", m["k"])
	}
}

func TestReviewWriteBackOverflowResurrectsDeletedKey(t *testing.T) {
	l2 := newRevL2()
	l2.blockOn = "v1"
	cache, err := memstash.New[string, string](
		memstash.WithMemoryCapacity(100),
		memstash.WithL2Cache[string, string](l2),
		memstash.WithWritePolicy(memstash.WriteBack),
		memstash.WithWriteBackWorkers(1),
		memstash.WithWriteBackBufferSize(1),
		memstash.WithWriteBackBatching(memstash.BatchingNone),
	)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, cache.Set(ctx, "k", "v1"))
	<-l2.entered
	require.NoError(t, cache.Set(ctx, "k", "v2")) // buffered
	require.NoError(t, cache.Delete(ctx, "k"))    // buffer full -> deleted synchronously, before v1/v2 land
	close(l2.gate)
	cache.Close()

	m, log := l2.snapshot()
	t.Logf("L2 op order: %v", log)
	if v, ok := m["k"]; ok {
		t.Errorf("RESURRECTED: key deleted last is back in L2 with %q", v)
	}
}

// --- F1b: one BatchSet batch can carry the same key twice ------------------------------------------------------

// batchRecL2 records the exact item list of every BatchSet and holds its first Set.
type batchRecL2 struct {
	revL2
	batches [][]string
}

func (l *batchRecL2) BatchSet(_ context.Context, items memstash.List[string, string], _ time.Duration) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	batch := make([]string, 0, len(items))
	for _, it := range items {
		batch = append(batch, it.Key+"="+it.Value)
		l.m[it.Key] = it.Value
	}
	l.batches = append(l.batches, batch)
	return nil
}

func TestReviewWriteBackBatchCarriesDuplicateKeys(t *testing.T) {
	l2 := &batchRecL2{revL2: *newRevL2()}
	l2.blockOn = "warm"
	cache, err := memstash.New[string, string](
		memstash.WithMemoryCapacity(100),
		memstash.WithL2Cache[string, string](l2),
		memstash.WithWritePolicy(memstash.WriteBack),
		memstash.WithWriteBackWorkers(1),
		memstash.WithWriteBackBufferSize(64),
		memstash.WithWriteBackBatching(memstash.BatchingFull),
	)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, cache.Set(ctx, "w", "warm")) // holds the only worker inside L2.Set
	<-l2.entered
	require.NoError(t, cache.Set(ctx, "k", "v1"))
	require.NoError(t, cache.Set(ctx, "k", "v2")) // both queue up behind the busy worker
	close(l2.gate)
	cache.Close()

	l2.mu.Lock()
	batches := append([][]string(nil), l2.batches...)
	l2.mu.Unlock()
	t.Logf("BatchSet batches handed to the adapter: %v", batches)
	for _, batch := range batches {
		if len(batch) > 1 && batch[0][:1] == batch[1][:1] {
			t.Errorf("DUPLICATE KEY IN ONE BATCH: %v - order inside the batch decides the final L2 value", batch)
		}
	}
}

// --- F2: NewWithConfig mutates the caller's Config -------------------------------------------------------------

func TestReviewConfigMutatedByConstructor(t *testing.T) {
	cfg := &memstash.Config[string, string]{MemoryBudget: 1 << 20}
	c1, err := memstash.NewWithConfig(cfg)
	require.NoError(t, err)
	defer c1.Close()

	t.Logf("after the first call: MemoryBudget=%d MemoryCapacity=%d CostFunc set=%v",
		cfg.MemoryBudget, cfg.MemoryCapacity, cfg.CostFunc != nil)

	c2, err := memstash.NewWithConfig(cfg)
	if err != nil {
		t.Errorf("REUSE BROKEN: the same Config fails the second time: %v", err)
	} else {
		c2.Close()
	}
}

// --- F3: an item heavier than the shard is dropped silently ----------------------------------------------------

func TestReviewOversizedItemDroppedSilently(t *testing.T) {
	l2 := newRevL2()
	var deletions []string
	cache, err := memstash.New[string, string](
		memstash.WithMemoryCapacity(1000),
		memstash.WithCostFunc[string, string](func(_, v string) uint32 { return uint32(len(v)) }),
		memstash.WithShardsCount(1),
		memstash.WithL2Cache[string, string](l2),
		memstash.WithWritePolicy(memstash.WriteThrough),
		memstash.WithOnDeletion[string, string](func(k, _ string, cause memstash.DeletionCause) {
			deletions = append(deletions, k+":"+cause.String())
		}),
		memstash.WithStats(),
	)
	require.NoError(t, err)
	defer cache.Close()

	ctx := context.Background()
	big := strings.Repeat("x", 2000)
	require.NoError(t, cache.Set(ctx, "big", big), "Set reports success")

	_, inMemory := cache.GetFromMemory("big")
	stats := cache.Stats()
	t.Logf("stored in memory=%v  OnDeletion calls=%v  Sets()=%d  Len()=%d",
		inMemory, deletions, stats.Sets(), cache.Len())

	value, found, err := cache.Get(ctx, "big")
	require.NoError(t, err)
	t.Logf("Get after the drop: found=%v len=%d (served from L2, then dropped again)", found, len(value))
	_, stillInMemory := cache.GetFromMemory("big")
	t.Logf("promoted into memory by Get: %v", stillInMemory)
	if !inMemory && len(deletions) == 0 {
		t.Errorf("SILENT: the value never entered memory and no OnDeletion fired")
	}
}

// --- F4: NewBatchLoadable caches a zero value for a key the loader does not have --------------------------------

func TestReviewBatchLoadableCachesZeroForMissingKey(t *testing.T) {
	l2 := newRevL2()
	lc, err := memstash.NewBatchLoadable[string, string](
		func(_ context.Context, _ []string) (memstash.List[string, string], error) {
			return nil, nil // the source has none of these keys
		},
		memstash.WithMemoryCapacity(100),
		memstash.WithL2Cache[string, string](l2),
		memstash.WithWritePolicy(memstash.WriteThrough),
	)
	require.NoError(t, err)
	defer lc.Close()

	ctx := context.Background()
	value, err := lc.GetOrLoad(ctx, "ghost")
	require.NoError(t, err)
	_, cached := lc.GetFromMemory("ghost")

	res, err := lc.BatchGetOrLoad(ctx, []string{"phantom"})
	require.NoError(t, err)
	_, cachedBatch := lc.GetFromMemory("phantom")

	_, log := l2.snapshot()
	t.Logf("GetOrLoad(ghost)      -> value=%q err=nil, cached in memory=%v", value, cached)
	t.Logf("BatchGetOrLoad(phantom) -> %d results, cached in memory=%v", len(res), cachedBatch)
	t.Logf("L2 ops: %v", log)
	if cached != cachedBatch {
		t.Errorf("ASYMMETRIC: GetOrLoad caches a missing key (%v), BatchGetOrLoad does not (%v)", cached, cachedBatch)
	}
}

// --- F5: LoadWithTTL caps the memory lifetime but hands L2 the uncapped one ------------------------------------

func TestReviewSnapshotLoadTTLNotCappedForL2(t *testing.T) {
	src, err := memstash.New[string, string](memstash.WithMemoryCapacity(100), memstash.WithTTL(time.Hour))
	require.NoError(t, err)
	defer src.Close()
	require.NoError(t, src.Set(context.Background(), "k", "v"))

	var buf bytes.Buffer
	require.NoError(t, src.SaveTo(&buf, stringCodec{}, stringCodec{}))

	l2 := newRevL2()
	dst, err := memstash.New[string, string](
		memstash.WithMemoryCapacity(100),
		memstash.WithTTL(time.Minute), // much shorter than what the snapshot carries
		memstash.WithL2Cache[string, string](l2),
		memstash.WithWritePolicy(memstash.WriteThrough),
	)
	require.NoError(t, err)
	defer dst.Close()

	require.NoError(t, dst.LoadFrom(context.Background(), &buf, stringCodec{}, stringCodec{},
		memstash.LoadWithTTL, memstash.LoadToL2))

	l2.mu.Lock()
	ttls := append([]time.Duration(nil), l2.ttls...)
	l2.mu.Unlock()
	t.Logf("cache TTL=1m, TTL handed to L2: %v", ttls)
	for _, ttl := range ttls {
		if ttl > time.Minute {
			t.Errorf("UNCAPPED: L2 got ttl=%v while memory caps at 1m", ttl)
		}
	}
}

type stringCodec struct{}

func (stringCodec) Marshal(v string) ([]byte, error)   { return []byte(v), nil }
func (stringCodec) Unmarshal(b []byte) (string, error) { return string(b), nil }

// --- F6/F7: BatchGet counters vs what the adapter and the caller actually asked --------------------------------

// dupL2 answers every requested key twice.
type dupL2 struct{ revL2 }

func (l *dupL2) BatchGet(_ context.Context, keys []string) (memstash.List[string, string], error) {
	out := make(memstash.List[string, string], 0, 2*len(keys))
	for _, k := range keys {
		out = append(out, memstash.KeyVal[string, string]{Key: k, Value: "v"})
		out = append(out, memstash.KeyVal[string, string]{Key: k, Value: "v"})
	}
	return out, nil
}

func TestReviewBatchGetTrustsAdapterCount(t *testing.T) {
	l2 := &dupL2{revL2: *newRevL2()}
	cache, err := memstash.New[string, string](
		memstash.WithMemoryCapacity(100),
		memstash.WithL2Cache[string, string](l2),
		memstash.WithWritePolicy(memstash.WriteDisabled),
		memstash.WithStats(),
	)
	require.NoError(t, err)
	defer cache.Close()

	found, err := cache.BatchGet(context.Background(), []string{"a"})
	require.NoError(t, err)
	stats := cache.Stats()
	t.Logf("1 key requested -> %d results, Gets()=%d L2Hits()=%d L2Misses()=%d",
		len(found), stats.Gets(), stats.L2Hits(), stats.L2Misses())
	if len(found) != 1 || stats.Gets() != 1 {
		t.Errorf("INFLATED: a duplicate-answering adapter inflates the result and the counters")
	}
}

func TestReviewBatchGetDuplicateInputKeys(t *testing.T) {
	l2 := newRevL2()
	l2.m["a"] = "v"
	cache, err := memstash.New[string, string](
		memstash.WithMemoryCapacity(100),
		memstash.WithL2Cache[string, string](l2),
		memstash.WithWritePolicy(memstash.WriteDisabled),
		memstash.WithStats(),
	)
	require.NoError(t, err)
	defer cache.Close()

	found, err := cache.BatchGet(context.Background(), []string{"a", "a"})
	require.NoError(t, err)
	stats := cache.Stats()
	t.Logf("keys [a a], present in L2 -> %d results, L2Hits()=%d L2Misses()=%d HitRate=%.2f",
		len(found), stats.L2Hits(), stats.L2Misses(), stats.HitRate())
	if stats.L2Misses() != 0 {
		t.Errorf("BOGUS MISS: a key that was found is counted as a miss because it was asked for twice")
	}
}

// --- F8: Stats() is not a snapshot ------------------------------------------------------------------------------

func TestReviewStatsIsLiveView(t *testing.T) {
	cache, err := memstash.New[string, string](memstash.WithMemoryCapacity(100), memstash.WithStats())
	require.NoError(t, err)
	defer cache.Close()

	ctx := context.Background()
	require.NoError(t, cache.Set(ctx, "k", "v"))
	_, _, _ = cache.Get(ctx, "k")

	snap := cache.Stats()
	before := snap.Gets()
	for range 10 {
		_, _, _ = cache.Get(ctx, "k")
	}
	after := snap.Gets()
	t.Logf("the same Stats value read before=%d and after 10 more Gets=%d", before, after)
	if before != after {
		t.Errorf("NOT A SNAPSHOT: the returned Stats keeps tracking the live cache")
	}
}
