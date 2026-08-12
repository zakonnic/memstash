# Contributing

Thanks for taking the time. Bug reports and small, focused pull requests are the most useful contributions.

## Layout

The repository is a multi-module workspace:

| Path | Module | What it is |
|---|---|---|
| `/` | `github.com/zakonnic/memstash` | the cache itself; one dependency (xsync) |
| `/internal/itemstore`, `/internal/eviction` | (part of the root) | the slot table and the eviction policies |
| `/tests` | (part of the root) | the behavioural suite |
| `/tests/integration` | separate | tests against live Redis, memcached, Postgres, ... |
| `/benchmarks` | separate | throughput, hit-ratio and footprint benchmarks vs other caches |
| `/benchmarks/load_generator` | separate | the soak-test engine, importable: describe scenarios, `New`, `Start` |
| `/l2/<name>_adapter` | separate, one each | L2 adapters, so the core never pulls in a client SDK |

An adapter lives in its own module on purpose: adding one never changes the root module's dependency graph.

## Getting set up

```sh
go test ./...        # or: make test
make lint            # golangci-lint, settings in .golangci.yml
```

`make lint` needs **golangci-lint v2** - the config uses the v2 schema, and a v1 binary cannot even typecheck
Go 1.24+ code:

```sh
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2
```

Integration tests need backing services. They skip themselves when nothing is listening, so this step is optional
until you touch an adapter:

```sh
make up                 # docker compose, ports 43210-43220
make test-integration
make down
```

Ports are in the 43210+ block instead of the servers' defaults, which are usually already taken on a dev machine.
Copy `docker/docker-compose.override.example.yml` to `docker/docker-compose.override.yml` to remap them.

## Changes that touch the hot path

Reads and writes are measured in nanoseconds, so `Get`, `Set`, the probe loop and eviction are held to a different
standard than the rest:

- **Measure, don't reason.** Post a `benchstat` comparison of several runs per side, not one timing. In-process
  drift alone is worth ~10%, so `-count=8` is a floor.
- **Watch allocations.** `-benchmem` on every benchmark; the memory path is allocation-free and should stay that way.
- **Any concurrency change runs under `-race`** before it is called done.

```sh
go test -run xxx -bench 'BenchmarkSet' -benchmem -count=8 ./tests/ > new.txt
benchstat old.txt new.txt
```

The cross-library benchmarks live in their own module:

```sh
make bench-speed     # throughput vs other caches
make bench-hitrate   # hit ratio across Zipf, Zipf+scan, one-hit-wonder workloads
```

So does the load generator, which is a module of its own so importing it never touches the benchmark module's
dependencies:

```sh
make load-generator        # build ./cmd/load-generator into benchmarks/bin
make test-load-generator   # its own tests, under -race
```

## Style

- Comments carry the essence and nothing more: a short lead sentence, then only the contract facts a caller cannot
  infer from the signature. Unexported identifiers get a comment only to answer "why".
- Comments and code wrap at 120 columns.
- `gofmt` decides formatting arguments.

## Adding an L2 adapter

Implement `memstash.L2Cache[K, V]` (`Get`/`BatchGet`/`Set`/`BatchSet`/`Delete`/`BatchDelete`) in a new
`l2/<name>_adapter` module. Take an interface rather than a concrete client so the adapter stays independent of the
client library's version, and offer both the adapter-only constructor and the all-in-one `New*Cache`. Add the module
to `ADAPTERS` in the Makefile, to the matrix in `.github/workflows/ci.yml`, and to `.github/dependabot.yml`.

## Releases

Every module is tagged together, root and adapters:

```sh
make tag V=1.2.3
make push
```
