# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org).

Every module - the root and each `l2/*_adapter` - is tagged with the same version.

## [0.9.8] - 2026-08-13

### Fixed

- Every `l2/*_adapter` module required core `v0.9.6` while its code used APIs added in `v0.9.7` (`l2.ExtractKeyFunc`,
  `l2.ExtractOrderedMget`, `l2.ResolveOrderedMget`), so `go get` of any adapter at v0.9.7 resolved the core one
  version back and failed to compile. Adapters now require the core version they actually need.

## [0.9.7] - 2026-08-12

### Added

- **`GetBatched`** - `Get` that coalesces its L2 read with the ones other goroutines are making right then: a memory
  hit is unchanged, a miss joins one shared queue, and a worker pool fetches everything waiting there in a single
  `BatchGet`, so a key several callers want is read once. The caller blocks as in `Get`, and a full queue makes it
  wait rather than drop anything. Worth it where the adapter has no pipelining of its own (go-redis, `database/sql`)
  and callers outnumber its connection pool; with an auto-pipelining client plain `Get` stays ahead. Sized by
  **`WithGetBatchedBuffer`** (1024) and **`WithGetBatchedWorkers`** (4), started by the first call that reaches it.
- **`SetWithTTL`** - a lifetime for one entry. Needs a cache built with `WithTTL` (the expiry scale is fixed at
  construction), rounds the lifetime up to that scale's unit and caps it at the scale's range; the resulting lifetime
  goes to L2 as well. `WithRefreshTTLOnGet` still extends by the cache's own TTL, so a custom lifetime holds until the
  first read that refreshes it.
- **`ErrTTLDisabled`** - `SetWithTTL` on a cache without a TTL.
- **`l2/valkey_adapter`** - Valkey backend on [valkey-io/valkey-go](https://github.com/valkey-io/valkey-go), with the
  same batching as the rueidis adapter: MGET/MSET/DEL multi-key commands while a batch stays under the wire budget, a
  pipeline of per-key commands above it. Works against a standalone node or a cluster.
- **`l2.WithOrderedMget`** and **`memstash.DetectMode`** (`AutoDetect`/`Disabled`/`Enabled`) - let the Redis-family
  adapters read an MGET reply array by position instead of going through the key-to-reply map their client's
  multi-get helper builds. Valid only while every key of a batch reaches one server; adapters probe for that
  themselves through their new `IsOrderedMgetAvailable` method, so the default needs no configuration.

### Changed

- A shard's table is now allocated in chunks of 64Ki slots instead of one contiguous block, so a big cache no longer
  asks the allocator for a single multi-gigabyte run of memory. Slot indices, capacity and behavior are unchanged; a
  table that fits one chunk still gets exactly one allocation of its own size.
- The write-back worker carries a lifetime per queued write instead of always using the cache's TTL: batches now end
  where the lifetime changes, since `BatchSet` takes one per call. A restored snapshot's remaining lifetime reaches L2
  through the write-back path too, which it previously lost.
- `BatchGet` and `BatchDelete` on the go-redis and redispipe adapters map every key once and hand the result to
  whichever branch runs. A batch over the wire budget used to map its keys for the multi-key command, drop them, and
  map them all a second time for the pipeline - so with a prefixing key function it paid two string allocations per
  key. On redispipe the discarded copy was boxed as well.

### Fixed

- The write-back worker of a key is now picked with a remainder instead of a multiply-shift, so any `WriteBackWorkers`
  count spreads evenly by construction. It still divides the hash bits the shard index did not use.
- The rueidis and valkey adapters send oversized pipelines on a dedicated connection. Both clients auto-pipeline
  onto shared connections and only split off a pipeline past `ClientOption.BlockingPipeline`, which counts commands -
  a write-back batch of large values stays far below that limit yet holds the connection long enough to stall
  reads behind it. Cluster clients keep the shared pipeline (cross-slot batches can't be dedicated). Pool-based adapters
  (go-redis, redigo) take a connection per pipeline and were never affected.

### Removed

- **`l2/valyala_adapter`** - the memcached backend on [valyala/ybc](https://github.com/valyala/ybc). The client needs
  cgo and hasn't been touched upstream since 2018; the three pure-Go memcached adapters cover the same backend.

## [0.9.6] - 2026-07-31

### Added

- **`WithPreallocatedSize` / `Config.PreallocateMap`** - pre-allocate an FlatHashMap for every shard at construction,
to avoid it grows and memory allocations on Set at all.

### Changed

- Revert a Stats micro-refactoring that prevented inlining and added (embarrassingly, unforgivable) 0.06 ns to Get.

## [0.9.5] - 2026-07-30

Add GetAndRefresh, panic recovery, faster loads and deletes.

### Added

- **`GetAndRefresh` / `BatchGetAndRefresh`** - serve the cached value, even past its TTL, and reload the key in the
  background. The reload follows WritePolicy into L2.
- **`WithPanicHandler` / `Config.OnPanic`** - a panic on one of the cache's goroutines or inside a loader no longer
  ends the process, it reaches this handler. `handled` says the panic also went out as an error; under a synchronous
  `GetOrLoad` it still reaches the caller.
- **`Error`** - the base sentinel every package error wraps, so one `errors.Is` tells memstash errors from the rest.
  `ErrPanic` and `ErrLoaderPanic` among them.
- Benchmarks: `BenchmarkDelete` / `BenchmarkDeleteMiss`, etc.

### Changed

- **Singleflight optimizations**: `GetOrLoad`, `BatchGetOrLoad`, `BatchGetAndRefresh` are now cheaper.
- **Cheaper `Delete`**: deleting an absent key is nearly free, and re-inserting a deleted key no longer degrades
  lookups until the next rebuild. `BatchDelete` too.
- Dependency updates across the adapter modules.

### Fixed

- `BatchGetOrLoad` panicked when an L2 adapter answered with duplicate keys, or with keys nobody asked for.
- `BatchGetOrLoad` dropped the loader's partial answer when the loader also returned an error.
- A loader answering out of order matched its keys in quadratic time.
- `Wait` never returned if a write-back delivery panicked mid-run.

## [0.9.4] - 2026-07-27

Faster core, snapshots, removal events.

### Added

- **`SaveTo` / `LoadFrom`** - dump the first level to an `io.Writer` and load it back, with options to restore the
  original expirations (`LoadWithTTL`) and to mirror into L2 (`LoadToL2`).
- **`WithOnDeletion`** - a handler for every item leaving the first level, with the cause.
- Project plumbing: CI, golangci-lint config, pkg.go.dev examples, contribution guide, security policy, templates.

### Changed

- **Core rewrite: the table slots are the items themselves** - a hit resolves in one lookup, with no separate slot
  table and no item-state pool.
- **`LoadableCache` embeds `*Cache`**: every cache method is available on it directly.
- Integration-test services moved to ports 43210-43220.

### Fixed

- A generation wrapping to 0 made a live item read as an empty slot, hiding every key stored behind it.
- `Delete` ignored `WritePolicy` and always wrote to L2 synchronously.
- Benchmarks: Ristretto ran at 1.75% of its nominal capacity, which is what its low hit rate measured.

### Removed

- `LoadableCache.Cache()` - use the embedded field, `lc.Cache`.

## [0.9.3] - 2026-07-23

Faster reads, zero-alloc writes, batched memory reads.

### Added

- **`BatchGetFromMemory` / `BatchGetFromMemoryWithMissing`** - read many keys from L1 in a single synchronous call,
  appending hits into a caller-provided `List` (reuse it to keep the read path allocation-free). The `WithMissing`
  variant also returns the keys that missed.

### Changed

- **Allocation-free `Set`** and a leaner read path, from a hot-path rework: raw-pointer hash slots, a single unified
  item state across platforms, and other small optimizations.
- New realistic random-load and batch-read benchmarks.

### Removed

- Internal: the boxed item state and the inc-stats path.

## [0.9.2] - 2026-07-17

Inline entries, lower memory footprint.

### Added

- **Per-tier stats counters**: `MemoryHits`, `L2Hits`, `L2Gets`, `MemoryHitRate` and `L2HitRate` report which tier
  answered a read.

### Changed

- **An item's Entry lives inside its state record** instead of behind a pointer to a separately allocated box. Same
  API, same behaviour, ~18% less memory: 46.59 B/entry against 56.59 on 10M `uint64 -> uint64` entries, and filling
  the cache got ~30% faster because a `Set` no longer allocates per write.

## [0.9.1] - 2026-07-14

Rearchitected core, pluggable eviction, faster L2 adapters.

### Added

- **Pluggable eviction policies**: the public `EvictionPolicy` interface plus `WithCustomEvictionPolicy` for supplying
  your own.
- **`PolicySIEVE`** (NSDI'24) - a single insertion-ordered list with a hand that evicts unvisited items in place.
- **`PolicyWTinyLFU`** adapted for lock-free reads - a small admission window in front of a GCLOCK main store.
- `Stats()` for hit/miss and eviction metrics.
- `Iterator()` - range over the cached entries without taking a full snapshot.
- `BatchDelete` - remove several keys in one call.

### Changed

- **The core was rebuilt from the ground up**: item state, storage and eviction bookkeeping, radically cutting the
  per-entry memory footprint while staying allocation-free and lock-free on the hot path.
- L2 adapters: batching and pipelining paths tightened across all of them (goredis, redigo, redispipe, rueidis, sql,
  pgx, dynamo and the rest). Every `l2/*_adapter` submodule was bumped to `v0.9.1` to match the updated interface.

## [0.9.0] - 2026-07-07

First tagged release.

[0.9.7]: https://github.com/zakonnic/memstash/compare/v0.9.6...v0.9.7
[0.9.6]: https://github.com/zakonnic/memstash/compare/v0.9.5...v0.9.6
[0.9.5]: https://github.com/zakonnic/memstash/compare/v0.9.4...v0.9.5
[0.9.4]: https://github.com/zakonnic/memstash/compare/v0.9.3...v0.9.4
[0.9.3]: https://github.com/zakonnic/memstash/compare/v0.9.2...v0.9.3
[0.9.2]: https://github.com/zakonnic/memstash/compare/v0.9.1...v0.9.2
[0.9.1]: https://github.com/zakonnic/memstash/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/zakonnic/memstash/releases/tag/v0.9.0
