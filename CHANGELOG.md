# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org).

Every module - the root and each `l2/*_adapter` - is tagged with the same version.

## [Unreleased]

### Changed

- **64-bit platforms only.** A build for 386, arm or mips now stops with a compile error naming
  `requires64BitPlatform`. Up to 0.9.8 the package compiled there, but not safely: an in-place overwrite of an 8-byte
  value - a string, an interface, an int64 - skipped the generation bracket as if it were one machine word, so a
  lock-free read could see it half-written, and for a string or an interface that pairs one value's pointer with the
  other's length or type. Table sizes and the shard layout were only ever written for 64-bit words.
- **The first level never holds a tombstone.** A deletion - `Delete`, an eviction, a lazy TTL removal - compacts its
  probe run on the spot with backward-shift deletion (Knuth's Algorithm R, taking the farthest eligible item first,
  which halves the moves), so the table always holds exactly what inserting the live keys into a fresh table would:
  probe walks stay at their fresh-table values however long the cache churns, and dead slots never eat into the free
  space. Measured on the churn harness at the default preallocation ratio: 0 dead slots and 0 purges over 4M inserts
  with 30% deletes (before: 4-5% dead held there by a purge every ~630 inserts per shard), hit walks 2.45 slots
  instead of 3.0-3.6, p99 hit walk 18 instead of 27.
- **No same-size rebuilds.** With nothing to purge, a preallocated table is never allocated again after `New`, and a
  non-preallocated one only grows; the transient second table of a purge is gone. `Stats.TablePurges` is always 0
  and will be removed with the next stats revision.
- **The eviction queues run through the table.** Every slot carries its place in its eviction queue - `prev`/`next`,
  8 bytes in an array beside the items - and a compaction that moves an item moves its place along, so a policy
  names its victim by slot index and the cache reads it directly, with no lookup. The cache enqueues every new item
  at the back of queue 0 and removes every departing one on the spot: nothing is left behind for a sweep, a
  delete-then-set leaves no duplicate, and no item can drop out of the queues and become unevictable. A policy moves
  items between the `EvictionQueues` queues (`Items.Oldest`, `Newer`, `Requeue`, the `ItemInMainQueue` bit, and a
  scan `Hand` that follows its item through moves and removals). `EvictionPolicy` is
  `Add`/`Remove`/`Reweigh`/`Evict`/`Bytes`: `QNode{Idx, Cost}`, `Add` and `Evict` are as in 0.9.8; `Len`, `Sweep`
  and `Rebuild` are gone; `Remove` hears of every departing item while it is still queued and `Reweigh` of every
  overwrite that changes a weight, so S3-FIFO's small and W-TinyLFU's window weigh exactly what they hold. `Items`
  also gives `Cost` and `Fingerprint` - a key's home slot and tag bits, which S3-FIFO's ghost table keys on without
  hashing the key; W-TinyLFU's sketch hashes the key as in 0.9.8, since
  a fingerprint changes when the table grows and the sketch's history must not. The links take 8 bytes per slot: a
  preallocated 100M-item `uint64` cache measures 43.7 bytes per item instead of 41.0 (heap 4.1 GiB instead of 3.9);
  a table that is not preallocated runs at a lower load and pays ~8 bytes per item more (+12-14% at 1M items), except
  under SIEVE, which drops its own 16-byte node array and comes out ~3% smaller.
- **The item's meta word carries its distance from its home slot**, so a compaction reads the run it moves instead of
  rehashing every key it passes - the difference between 1.0 us and 0.6 us per delete on 100M string keys. It costs
  the six generation bits the seqlock had to spare (22 left, 2M writes to one slot before a stalled reader could
  validate against a stale word).
- **The slot tag carries all 13 hash bits.** A tag must never be 0, or an empty slot would pass the candidate compare;
  that used to be a forced top bit, leaving 4096 distinct tags. Now only a zero tag is remapped, so 8191 stay distinct:
  a foreign key in the probe path costs a key compare half as often, and S3-FIFO's ghost fingerprints collide half as
  often.
- **A shard's read line carries nothing a writer touches.** The table pointer and the move sequence share the first
  cache line and the mutex opens the second, where before a `Set` taking the mutex invalidated the line every reader
  loads on every probe. Worth 30-37% on every mixed read/write benchmark.
- Lock-free reads stay exact across the compaction: a miss that overlapped moves is re-probed (a per-shard move
  sequence on the line every read loads anyway), the third time under the shard mutex; hits are unchanged.
  `Iterator` and `SaveTo` may skip an item a concurrent deletion slid toward its home - the same weak consistency the
  documentation already claims for concurrent writes - and never yield a key that stays in the cache twice.
- `WithPreallocationRatio` is now a pure memory-for-latency dial, and `WithGrowthThreshold` only means growth; the
  rule that the ratio must stay below the threshold is gone.
- **Meta words are written plainly under the shard mutex on amd64.** One writer holds the mutex, so a meta write
  needs release ordering, not a locked instruction, and on amd64 a plain aligned store is release-ordered; five
  locked writes measure 18.5 ns against 1.5 ns plain, and an insert that evicts issues a dozen. Readers keep their
  atomic loads and compare-and-swaps; the race detector, which cannot see the ordering argument, builds the atomic
  variant (`internal/itemstore/write_generic.go`).
- **The hot paths inline in user programs too.** A generic method of this package is compiled where it is
  instantiated - in the user's module - and there a plain helper from `internal/itemstore` or `internal/eviction`
  inlines only if its body is in this package's export data, which the compiler writes only for helpers it inlined
  in this package's own build. Every such helper was a call in every program but this repository's own tests: the
  tag, the overflow bucket and the empty test on every read; the meta-word and weight stores, the generation and
  distance encoders, the queue node accessors and the policies' own helpers on every write - 683 call sites in the
  benchmarks module. The package now instantiates the cache once inside itself, which inlines them all there; the
  155 calls left go to functions that are not inlinable anywhere. The instance adds nothing to a program's binary and
  ~0.15 s to this package's compile. On the benchmarks module, which builds the way a user does: `GetHit` -16%, `Set`
  and `Delete` -4% to -6%, the reads and writes of `Mixed90_10` unchanged.
- **What the compaction costs.** Measured against the tombstone design: reads are ahead (`GetHit` -13%, `Get` -21%,
  `GetMiss` -9%, `DeleteMiss` -19%) and so is every mixed workload (`Mixed90_10` -41%, parallel throughput -11% at
  100% reads to -38% at 75%: the shard read line no longer bounces on every `Set`). Writes that remove still pay for a
  table that never holds a tombstone: `Set` at capacity costs +17%, and the `Delete`+`Set` pair of one hot key 48 ns
  against 34 ns, where the old design resurrected the item in place. Filling a cache that is not preallocated costs
  more too: 8M items into one shard +8% on the inserts alone and +11% with the table growing under them. A growth
  after the first eviction carries the eviction queues over in order; one before it, while the queues hold nothing
  but insertion order, queues the items in their new slot order and spares that pass. Hit rates are unchanged.

### Fixed

- **A lock-free read could return a torn multi-word value on arm64 and other weakly ordered CPUs.** An in-place
  overwrite of a value wider than a word marks the item's generation odd, writes the value, then marks it even; the
  odd mark is an atomic store, which on arm64 is a release (`STLR`) and orders nothing after it, so the value stores
  could become visible first, and a reader copying in that window validated against the old, even generation. The
  odd mark is now followed by the same acquire+release pivot the read side already uses. amd64 is unaffected - its
  stores never pass one another.
- **A lock-free read could return a torn value of up to 8 bytes made of several fields.** An in-place overwrite
  skipped the generation bracket for any value no wider than a word, but gc stores a `struct{A, B uint32}` or a
  `struct{X, Y float32}` one field at a time, so a reader could pair `A` of one write with `B` of another. Only a
  single scalar - an integer, a float, a pointer - skips the bracket now; its code is unchanged.
- **A write-back write could overtake the writes its key had already queued.** When a worker's buffer was full, the
  write ran synchronously on the caller's goroutine, ahead of everything still in the queue: `Set(v1)`, `Set(v2)`,
  `Set(v3)` could leave `v2` in L2, and a `Delete` could land before the queued `Set` that resurrected the key. A write
  that finds the buffer full now waits for room, so the writes of one key reach L2 in order; after `Close` it still
  runs synchronously. `OnL2Error` receives write-back errors on the worker goroutine and must not write to the cache.
- **`sql_adapter.DefaultTable` and `tarantool_adapter.DefaultSpace` were `"github.com/zakonnic/memstash_cache"`.** A
  module-path rename had rewritten both names, so an adapter built with an empty table or space name failed on every
  call (a SQL syntax error, a missing space). Both are `"memstash_cache"` again, the name the docker setup and the
  documented schema create.
- **A batch that repeated a key could leave an older value in L2.** Write-back coalesces a key's queued writes into
  one `BatchSet`, so a hot key can appear in it twice, and `L2Cache.BatchSet` relied on the adapter to keep the
  last value. The memcached adapters (gomemcache, mc, rainycape) sent the batch as concurrent `Set`s, Aerospike ran it
  in no fixed order outside inline processing, Tarantool's connection shards and redispipe on a cluster could send
  its commands out of order: any of them could land `v1` after `v2`. They now collapse a repeated key to its last
  value first (the new `l2.LastValues`), and the `BatchSet` contract says a key may repeat.
- **A batch loader that reordered its keys handed other callers the wrong values.** The keys a `BatchGetOrLoad` or
  `BatchGetAndRefresh` loader receives may be the very list the flights were registered from, and the loaded values
  were matched to the flights by position in it: a loader that sorted its keys for the query, as most do, answered the calls
  waiting on `a` with the value of `c`. Flights are now matched by their own key. A loader may reorder the slice; for
  `BatchGetOrLoad` that reorders the caller's `keys`, which it may alias.
- **The Badger adapter's `Get` could hand out memory Badger later unmaps.** It decoded the value inside
  `item.Value`, whose slice points into Badger's own memory and is valid only for the transaction; with a
  pass-through codec (`NewBytes`, `BytesCodec`) that slice went into the first level, and for a value in the
  value log a later GC or `Close` unmapped it - the next read of the cached value crashed the process. `Get` copies
  the value first, as `BatchGet` already did.
- **The rueidis and valkey adapters panicked on a one-master cluster.** They told a single server from a cluster by
  `len(client.Nodes()) <= 1`, and a cluster client with one master (a single-shard cluster, a local node with
  `cluster-enabled yes`) has one node: the adapters then took the ordered-MGET and dedicated-pipeline paths, where
  the client panics on keys from different slots - in `BatchGet`, in every `BatchSet` with a TTL, and in large
  `BatchDelete`s. They now ask the client's `Mode()`.
- **`NewBatchLoadable`'s `GetOrLoad` cached a zero value for a key the loader did not return.** Its single-key
  loader turned "the batch loader omitted the key" into `(zero, nil)`, which `GetOrLoad` stored in memory and wrote
  to L2, where it outlived restarts; `GetAndRefresh` did the same. The new `ErrKeyNotFound` is how a loader says the
  source has no such key: nothing is cached and `GetOrLoad` returns it. `NewBatchLoadable` answers an omitted key
  with it, `NewLoadable`'s synthesized batch loader leaves a key its loader reports that way out of the batch instead
  of failing it, and a `GetOrLoad` waiting on a batch load that omitted its key gets it instead of a zero value.
- **`NewWithConfig` wrote into the caller's `Config`.** A budget filled in `CostFunc` and moved itself into
  `MemoryCapacity`, and a `PreallocationRatio` set `PreallocateMap`, so a second cache built from the same `Config`
  failed with `ErrBudgetAndCapacity`. The constructor now works on its own copy.
- **An out-of-range `WritePolicy` passed validation and made `Set` panic.** `WritePolicy(3)` built a cache with no
  write-back workers that still took the write-back path, dividing by zero workers on the first `Set`, while
  `Delete` silently skipped L2; an unknown `WriteBackBatching` quietly behaved as `BatchingFull`. The constructor now
  rejects both, with the new `ErrUnknownWritePolicy` and `ErrUnknownBatching`.
- **A write-back batch that panicked in the adapter was reported for one key, sometimes the wrong one, and could
  take the next task down with it.** The worker recovered once per drain with the key of the drain's first task:
  `OnL2Error` heard of that key only - one that may already have reached L2 - and never of the rest of the failed
  batch, and a task taken off the queue to end the batch's run, a `Delete` for instance, was never sent. Every L2 call
  of the write-back now recovers on its own: the batch's keys all get the `ErrPanic`, and the worker goes on with the
  task it already holds.
- **A panic while a `GetBatched` worker filed a read took the worker down and left the reads it held waiting.** The
  memory check a worker makes before queuing a key for L2 can reclaim an expired item, and an `OnDeletion` handler
  that panicked there escaped the batch's recovery: the reads already collected and the one in hand were never
  answered, and the pool lost a worker for good - with all of them gone, every `GetBatched` miss waited for its
  context. The recovery now covers the whole batch: its callers get `ErrPanic` and the worker goes on.
- **`BatchGetOrLoad` could load a key that a concurrent load had just stored.** A flight that ended between the
  batch's memory pass and its claim on the key left the key in memory with its flight gone, so the batch owned it
  anew and sent it to L2 and the loader again. It now checks memory once more for the keys it owns, as `GetOrLoad`
  always did, and loads only those still missing.
- **`DeletionCause` values shifted with unrelated constants.** The causes shared an `iota` block with the
  `ItemDead`/`ItemChanceMask`/`ItemInMainQueue` bits, so they were 2..6 in 0.9.8 and 3..7 once a bit was added, and a
  cause stored or exported as a number meant something else after an upgrade. They have their own block now and stay
  at 1..5 (`CauseInvalidation` to `CauseOverflow`); the zero value is no cause.
- **A "forever" TTL overflowed.** `WithTTL(math.MaxInt64)` rounded the TTL up past the int64 range, and every item
  was expired the moment it was written; `SetWithTTL` with such a lifetime handed L2 a negative TTL;
  `l2.RedisMillis` turned it into one millisecond and `l2.MemcacheExpiration` into a negative expiration, both of
  which expire the key at once; the Aerospike adapter wrapped TTLs of 2^32 seconds or more into its reserved values
  (keep the old TTL, the namespace default). The roundings no longer overflow, a TTL is capped at ~146 years, and
  the Aerospike adapter stores anything past its range as "never expire".
- **`LoadFrom` with `LoadWithTTL` and `LoadToL2` gave L2 a different lifetime than memory.** Memory capped a
  restored item at the loading cache's TTL, L2 got the uncapped remainder from the snapshot - an hour where memory
  kept a minute - and a cache without a TTL kept the item forever while L2 expired it. L2 now gets the lifetime
  memory ended up with, rounded and capped the same way, and none from a cache without a TTL.
- **A call waiting on another's load got that caller's cancellation.** `GetOrLoad` and `BatchGetOrLoad` coalesce
  concurrent loads of a key, and the load runs under the context of whoever started it: when that context was
  canceled or timed out, every call waiting on the key received `context.Canceled` or `DeadlineExceeded`, though its
  own context was live. A waiter now loads the key itself when the error is a context error and its own context is
  fine.
- **L2 items with a TTL expired up to a second early in the SQL, pgx, Badger, DynamoDB and Tarantool adapters.**
  They stored the deadline as `time.Now().Add(ttl).Unix()`, rounded down, and read it back as expired once the
  clock's second reached it: every item lived somewhere in (ttl-1s, ttl], and one with a TTL under a second was
  usually gone before anyone read it. The new `l2.ExpiresAt` rounds the deadline up, and all five use it.

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
