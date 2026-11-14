# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org).

Every module - the root, each `l2/*_adapter` and `benchmarks/load_generator` - is tagged with the same version.

## [0.10.0] - 2026-11-14

An in-memory level (L1) without tombstones, eviction queues threaded through its table, and fixes across the core,
the loading layers and the adapters.

### Added

- The load generator runs next to its L2 servers: `make load-generator-docker` runs it in compose, away from the
  Docker Desktop port relay that capped the L2 scenarios. It logs what `Set`, `GetFromMemory` and `Get` cost on the
  live cache, and `-config` repeats, a later file overriding the fields it sets.
- Write-back sends single calls to stores without batches. An adapter whose `BatchSet` and `BatchDelete` are single
  calls underneath implements the optional `SingleCallL2`, and each write-back worker then sends one call at a time,
  so `WriteBackWorkers` is how many calls write-back keeps in flight. By default the adapter picks that number and
  leaves the client room for reads. The gomemcache, mc and rainycape adapters implement it: `WithWriteBackWorkers(n)`
  now means n calls at once there, where in 0.9.8 it meant up to 8n.

### Changed

- **The in-memory level never holds a tombstone.** A deletion (`Delete`, an eviction, a lazy TTL removal)
  compacts its probe run on the spot with backward-shift deletion. The table always holds what inserting the live keys
  into a fresh table would: probe walks do not grow with churn, and dead slots never eat into the free space.
- No same-size rebuilds. With nothing to purge, a preallocated table is never allocated again after `New`, and a
  growable one only grows. `Stats.TableGrowths` counts the growths.
- **The eviction queues run through the table.** Every slot carries its place in its queue, 8 bytes beside the item,
  and a compaction that moves an item moves its place along. A policy names its victim by slot index. The cache
  enqueues every new item and dequeues every departing one on the spot, so nothing is left for a sweep and no item can
  become unevictable. `EvictionPolicy` is `Add`/`Remove`/`Reweigh`/`Evict`/`Bytes` (`Len`, `Sweep` and `Rebuild` are
  gone), and a policy moves items between the `EvictionQueues` queues through `Items`. SIEVE drops its own node array.
- The item's meta word carries its distance from its home slot, so a compaction reads the run it moves instead of
  rehashing the keys it passes. It takes the six generation bits the seqlock had to spare, leaving 22.
- The slot tag keeps all 13 hash bits: only a zero tag is remapped, where a top bit used to be forced. A foreign key in
  the probe path costs a key compare half as often, and so do S3-FIFO ghost collisions.
- A shard's read line carries nothing a writer touches. The mutex moved to the next cache line, so a `Set` no longer
  invalidates the line every read loads.
- Lock-free reads stay exact across a compaction: a miss that overlapped moves re-probes, the third time under the
  shard mutex. `Iterator` and `SaveTo` may skip an item a concurrent deletion moved, and never yield a key twice.
- `WithPreallocationRatio` sets how full a preallocated table sits at capacity and preallocates on its own.
  `WithGrowthThreshold` sets the occupancy a growable table grows at.
- **Meta words are written with plain stores under the shard mutex on amd64.** One writer holds the mutex, and on
  amd64 a plain aligned store is already release-ordered, so it needs no locked instruction. Readers keep their atomic
  loads and compare-and-swaps; race builds use the atomic variant.
- The hot paths inline in user programs. A generic method is compiled in the user's module, where a helper from
  `internal/` inlined only if this package's own build had inlined it. The package now instantiates the cache once
  itself, so every hot helper inlines everywhere.
- What the compaction costs. Reads and mixed workloads get faster. A `Set` at capacity, a `Delete`+`Set` on one hot
  key and filling a cache that is not preallocated get slower. Hit rates are unchanged.
- A compaction reads its run once and picks its moves on a copy of the homes, instead of walking the run again for
  every move. An insert that evicts gets cheaper under every policy.
- A probe checks its group's edge only once a slot has sent it on, so a hit in the home slot never does.
- The aerospike, badger, goredis, mongo, pgx, rueidis and valkey adapters require Go 1.26, as the current releases of
  their clients do. The core and the other adapters stay on Go 1.24.
- The core depends on xsync v4, for the `WithStats` counters.
- Per-item write-back covers deletes. `BatchingNone`, and `BatchingAdaptive` below half a buffer, send every queued
  delete as its own `Delete`, as they already did writes.

### Fixed

- A lock-free read could return a torn multi-word value on arm64 and other weakly ordered CPUs. The seqlock's odd
  mark is a release store and did not keep the value stores after it; it is now followed by the pivot the read side
  uses. amd64 is unaffected.
- **A lock-free read could return a value half old and half new.** An in-place overwrite skipped the seqlock for
  values up to 8 bytes, but on a 32-bit platform an 8-byte string, interface or int64 is two stores, and a two-field
  struct is stored field by field everywhere. `New` now classifies the value type once: only a single scalar store
  skips the seqlock.
- An in-place overwrite of a pointer value had no release ordering off amd64: a reader could get the new pointer and
  the old contents of what it points to.
- **A write-back write could overtake the writes its key had already queued.** A write that found its worker's buffer
  full ran on the caller's goroutine, ahead of the queue, so L2 could end with an older value or a resurrected key. It
  now waits for room. A worker that falls behind sends up to four key-split batches at once. `OnL2Error` runs on the
  worker and must not write to the cache.
- `sql_adapter.DefaultTable` and `tarantool_adapter.DefaultSpace` were `"github.com/zakonnic/memstash_cache"`, so an
  adapter left on the default failed on every call. Both are `"memstash_cache"` again.
- **A batch that repeated a key could leave an older value in L2.** Write-back coalesces a hot key's writes into one
  `BatchSet`, and the memcached adapters, Aerospike, Tarantool and redispipe on a cluster could apply them out of
  order. They keep the last value now (`l2.LastValues`), and the `BatchSet` contract allows repeats.
- **A batch loader that reordered its keys handed other callers the wrong values.** Loaded values were matched to the
  waiting calls by position, so a loader that sorted its keys answered the call for `a` with the value of `c`. They are
  matched by key now.
- **The Badger adapter's `Get` could hand out memory Badger later unmaps.** With a pass-through codec the cached value
  pointed into Badger's memory, and reading it after a value-log GC or `Close` crashed the process. `Get` copies the
  value.
- **The rueidis and valkey adapters panicked on a one-master cluster.** They took it for a single server and sent
  cross-slot batches. They now ask the client's `Mode()`.
- `NewBatchLoadable`'s `GetOrLoad` cached a zero value, in memory and in L2, for a key the loader did not return. The
  new `ErrKeyNotFound` is a loader's answer for a missing key: nothing is cached, and `GetOrLoad` returns it.
- `NewWithConfig` wrote into the caller's `Config`, so a second cache built from it failed.
- An out-of-range `WritePolicy` passed validation and made `Set` panic, and an unknown `WriteBackBatching` acted as
  `BatchingFull`. Both are rejected now (`ErrUnknownWritePolicy`, `ErrUnknownBatching`).
- A write-back batch that panicked in the adapter was reported for one key, sometimes the wrong one, and could lose
  the next task. Every key of the batch now gets the `ErrPanic`, and nothing else is lost.
- A panic while a `GetBatched` worker filed a read killed the worker and left its callers waiting. They get `ErrPanic`
  now, and the worker goes on.
- `BatchGetOrLoad` could load again a key that a concurrent load had just stored.
- `DeletionCause` values shifted with unrelated constants: they were 2..6 in 0.9.8. They have their own block now and
  stay at 1..5; the zero value is no cause.
- A "forever" TTL (`WithTTL(math.MaxInt64)`) overflowed and expired every item at once, and the L2 TTL conversions
  and the Aerospike adapter overflowed the same way. A TTL is capped now, and Aerospike stores anything past its range
  as "never expire".
- `LoadFrom` with `LoadWithTTL` and `LoadToL2` gave L2 a different lifetime than memory. L2 now gets the one memory
  ended up with.
- A call waiting on another's load got that caller's cancellation. A waiter whose own context is live now loads the
  key itself.
- L2 items with a TTL expired up to a second early in the SQL, pgx, Badger, DynamoDB and Tarantool adapters, which
  rounded the deadline down. `l2.ExpiresAt` rounds it up.
- The redigo adapter failed every single command on a pool whose connections have no `DoContext`. The fallback to
  `Do` works now.
- One failed item cancelled the rest of a batch write in the `l2` batch helpers (gomemcache, mc, rainycape, and the
  SQL and Aerospike fallbacks). Every item is tried now, and the failures come back joined.
- The rainycape adapter's `BatchGet` turned every failure into misses, since the client's `GetMulti` drops its
  errors. Keys missing from its answer are read again with `Get`, which reports them.
- The memcached adapters failed on keys memcached rejects: a space, a control character, over 250 bytes.
  `l2.MemcacheKey` stores such a key under a hashed stand-in.
- Keys and values some backends reject failed whole batches: Badger's and DynamoDB's key limits, a nil value in
  DynamoDB and Tarantool. Such keys go under `l2.HashedKey`, and a nil value goes out as an empty one.
- The DynamoDB adapter gave up on a throttled batch after one immediate retry, and a `BatchGet` past the service's
  page limit always failed. It retries with backoff now.
- `Stats()` returned a live view whose copies kept counting. It is now a snapshot with value-receiver methods.
- `Close` did not wait for `GetAndRefresh` loads, which could write to memory and L2 after it returned. It now cancels
  and waits for them.
- A shard table could grow to 2^32 slots, whose count wrapped to 0 in the table's uint32 size. The largest table is
  now the last whole chunk below 2^32.
- `Wait` could block forever and could not tell a finished checkpoint from a closing cache. It is now
  `Wait(ctx) error`, returning `ctx.Err()` or the new `ErrClosed`. This changes its signature.
- **A value read from L2 or loaded could overwrite a newer `Set`.** It now fills only a key memory holds no live value
  for, and a loaded value that loses does not go to L2 either. `GetAndRefresh` still replaces.
- A write could be expired at birth once every 2^18 clock units: an expiry offset that wrapped onto 0 moved one unit
  later, past the end of the scale. It now moves one unit earlier.
- A NaN key could lose other keys. Each `Set` stored an unreachable copy, and removing one could decrement the
  overflow counter of another key's group. Such a key no longer enters memory.
- **An expired item nothing read or evicted came back to life** once the 18-bit expiry scale wrapped, about 36 hours
  past its deadline at the 1 s unit. The clock goroutine now sweeps the tables for expired items.
- A value too heavy for memory vanished from it without a trace. `Stats.Rejected()` now counts the values memory did
  not take, and the docs name the per-shard limit.
- One value the codec rejected failed a whole L2 batch, without saying which. An adapter now skips such an item and
  reports it as a `KeyError`, and the cache fails or reports only the keys a batch error names. For adapters outside
  this repository: values returned alongside a `BatchGet` error are now used, so return only keys that were read.
- **A value read from L2 or loaded could bring back a key deleted while it was read.** A read now stores its answer
  only if no `Delete` came in between. With write-back, a read that reaches L2 before the queued delete can still
  bring the value back.
- An adapter that answered a key twice inflated the L2 counters. They count by key now, and the `BatchGet` contract
  says each key at most once.
- A custom eviction policy that named a bad victim could crash the process or corrupt its shard. Such an index now
  ends the eviction round.
- A corrupt snapshot could make `LoadFrom` allocate a gigabyte, and `ctx` did not stop a long load. Large chunks grow
  as their bytes arrive, and `ctx` ends a load between items.
- Negative `ShardsCount`, `GhostSize` and buffer sizes quietly meant the default, while negative worker counts were
  rejected. They are rejected now (`ErrBadShardsCount`, `ErrBadGhostSize`, `ErrBadBufferSize`).
- The load generator's hit rates were totals since start. They cover the interval since the previous line now.
- A load generator worker that stalled lost those ops for good. It catches up on up to a second of backlog, and the
  start line carries `target_rps`.
- The release targets left the load generator behind. It is now tagged with every release and requires the core of
  the same release, and `release-check` fails when it cannot reach origin.

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

- Revert a Stats micro-refactoring that prevented inlining and slowed down Get.

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
  API, same behaviour, less memory per entry, and filling the cache got faster because a `Set` no longer allocates
  per write.

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

[0.10.0]: https://github.com/zakonnic/memstash/compare/v0.9.8...v0.10.0
[0.9.8]: https://github.com/zakonnic/memstash/compare/v0.9.7...v0.9.8
[0.9.7]: https://github.com/zakonnic/memstash/compare/v0.9.6...v0.9.7
[0.9.6]: https://github.com/zakonnic/memstash/compare/v0.9.5...v0.9.6
[0.9.5]: https://github.com/zakonnic/memstash/compare/v0.9.4...v0.9.5
[0.9.4]: https://github.com/zakonnic/memstash/compare/v0.9.3...v0.9.4
[0.9.3]: https://github.com/zakonnic/memstash/compare/v0.9.2...v0.9.3
[0.9.2]: https://github.com/zakonnic/memstash/compare/v0.9.1...v0.9.2
[0.9.1]: https://github.com/zakonnic/memstash/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/zakonnic/memstash/releases/tag/v0.9.0
