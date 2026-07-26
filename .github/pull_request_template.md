## What and why

<!-- One or two sentences. Link the issue if there is one. -->

## Checklist

- [ ] `make test` passes (`go test -race ./...`)
- [ ] `make lint` is clean
- [ ] New behaviour is covered by a test
- [ ] Public API changes are reflected in the doc comments and the README option table

## Performance

<!--
Delete this section if the change cannot touch Get/Set/eviction.
Otherwise paste the benchstat comparison - `-benchmem`, several runs each side:

    go test -run xxx -bench 'BenchmarkSet' -benchmem -count=8 ./tests/ > new.txt
    benchstat old.txt new.txt
-->
