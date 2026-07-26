# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org).

Every module - the root and each `l2/*_adapter` - is tagged with the same version.

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

[0.9.4]: https://github.com/zakonnic/memstash/compare/v0.9.3...v0.9.4
[0.9.3]: https://github.com/zakonnic/memstash/compare/v0.9.2...v0.9.3
[0.9.2]: https://github.com/zakonnic/memstash/compare/v0.9.1...v0.9.2
[0.9.1]: https://github.com/zakonnic/memstash/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/zakonnic/memstash/releases/tag/v0.9.0
