DC = docker compose
DC_FILES = -f docker/docker-compose.yml
ifneq (,$(wildcard docker/docker-compose.override.yml))
DC_FILES += -f docker/docker-compose.override.yml
endif
ADAPTERS = \
	l2/aerospike_adapter \
	l2/badger_adapter \
	l2/dynamo_adapter \
	l2/gomemcache_adapter \
	l2/goredis_adapter \
	l2/mc_adapter \
	l2/mongo_adapter \
	l2/pgx_adapter \
	l2/rainycape_adapter \
	l2/redigo_adapter \
	l2/redispipe_adapter \
	l2/rueidis_adapter \
	l2/sql_adapter \
	l2/tarantool_adapter \
	l2/valkey_adapter
TAGS = v$(V) $(addsuffix /v$(V), $(ADAPTERS))
MODULES = . benchmarks benchmarks/load_generator tests/integration $(ADAPTERS)

.PHONY: help
help: ## Show help message
	@cat $(MAKEFILE_LIST) | grep -e "^[a-zA-Z_\%-]*: *.*## *" | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

up: ## Start containers for the integration tests (waits for healthchecks)
	$(DC) $(DC_FILES) up -d --wait
down: ## Stop and remove the integration containers
	$(DC) $(DC_FILES) down

.PHONY: update-packages
update-packages: ## Update go modules versions in every module of the repo
	@for m in $(MODULES); do \
		echo "==> $$m"; \
		go -C $$m get -u ./... || exit 1; \
		go -C $$m mod tidy || exit 1; \
	done

lint: ## Run linter with settings from .golangci.yml (needs golangci-lint v2)
	golangci-lint run -v
lint-fix: ## Linter tries to fix issues automatically
	golangci-lint run -v --fix

.PHONY: test test-all test-all-race
test: ## Run local tests
	go test -v -race ./...
test-load-generator: ## Run the load generator's own tests
	go -C benchmarks/load_generator test -race ./...
test-integration: up ## Run integration tests against live L2 servers (make up first)
	go -C tests/integration test ./... -v
test-all: ## Run all tests
	@for m in $(MODULES); do \
		echo "==> $$m"; \
		go -C $$m test ./... || exit 1; \
	done
test-all-race: ## Run all tests under the race detector
	@for m in $(MODULES); do \
		echo "==> $$m"; \
		go -C $$m test -race ./... || exit 1; \
	done

cover-gen: ## Generate merged test coverage across all packages (tests/ and l2/ exercise the root and internal packages)
	@mkdir -p var
	go test -count=1 -coverpkg=./... -coverprofile=var/coverage.out ./...
	go tool cover -func=var/coverage.out | tail -1
cover-func: cover-gen ## Show coverage by func
	go tool cover -func=var/coverage.out
.PHONY: cover
cover: cover-gen ## Show coverage html
	go tool cover -html=var/coverage.out

bench-short: ## ShortCheckup benchmark
	go -C benchmarks test -run=xxx -bench='^Benchmark(ShortCheckup)$$' ./...
bench-speed: ## Run the speed_test.go benchmarks (Zipf hot-set micro-benchmarks)
	go -C benchmarks test -run=xxx -bench='^Benchmark(GetHit|Get|GetMiss|Set|Delete|DeleteMiss|Mixed90_10|Throughput)$$' ./...
bench-speed-random: ## Run the speed_random_test.go benchmarks (realistic random load)
	go -C benchmarks test -run=xxx -bench='^BenchmarkRandom' ./...
bench-hitrate: ## Run hitrate benchmarks
	go -C benchmarks test -run=xxx -bench='^BenchmarkHitRate$$' -benchtime=1x -v
bench-real: ## Run realistic and non-standard pattern benchmarks
	go -C benchmarks test -run=xxx -bench='^Benchmark(MemstashGetHitSerial)$$' -v
	go -C benchmarks test -run=xxx -bench='^BenchmarkHitRateRealistic$$' -benchtime=1x -v
bench-flight: ## Run hitrate benchmarks for singleflight
	go -C benchmarks test -run=xxx -bench='^BenchmarkFlight' ./...
bench-100kk: ## Run memstash benchmarks for 100M items cache
	go -C benchmarks test -run xxx -bench BenchmarkMemoryFootprintMemstash -tags=long -benchtime=1x
bench-100kk-others: ## Run benchmarks for 100M items other caches
	go -C benchmarks test -run xxx -bench BenchmarkMemoryFootprint -tags=others -benchtime=1x
bench-integration: up ## Run L1+L2 load-profile benchmarks against the live servers (make up first)
	go -C tests/integration test -run xxx -bench . -benchtime 1s ./...

.PHONY: bench
ifeq ($(strip $(NAME)),)
bench: bench-speed bench-hitrate ## Run benchmarks; NAME=<regexp> runs only the matching ones (make bench NAME=GetHit)
else
bench:
	go -C benchmarks test -run=xxx -bench='^Benchmark$(NAME)$$' ./...
endif

.PHONY: load-generator
load-generator: ## Build the long-running load generator (+ config.yaml) into benchmarks/bin
	go -C benchmarks/load_generator build -o ../bin/load-generator$(if $(filter Windows_NT,$(OS)),.exe,) ./cmd/load-generator
	[ -f benchmarks/bin/config.yaml ] || cp benchmarks/load_generator/config.yaml benchmarks/bin/config.yaml

check-new-libs: ## Checks for new versions of libraries
	@OUT=$$(go list -m -u -f '{{if .Update}}{{.Path}}: {{.Version}} -> {{.Update.Version}}{{printf "\n"}}{{end}}' all); \
	if [ -n "$$OUT" ]; then \
		echo "$$OUT"; \
		echo "Run 'make update-packages' to update"; \
	else \
		echo "All dependencies are up to date"; \
	fi

.PHONY: tag
tag: ## Tag the root module and every l2 adapter module with the given version (make tag V=1.2.3), then 'make push'
	@test -n "$(V)" || { echo "V is required, e.g. make tag V=1.2.3"; exit 1; }
	@for t in $(TAGS); do \
		if git rev-parse "$$t" >/dev/null 2>&1; then \
			echo "Error: tag $$t already exists. Aborting."; \
			exit 1; \
		fi; \
	done
	$(foreach t, $(TAGS), git tag "$(t)";)

untag: ## Delete the root module and every l2 adapter module tag with the given version (make untag V=1.2.3)
	@test -n "$(V)" || { echo "V is required, e.g. make untag V=1.2.3"; exit 1; }
	@failed=0; \
	for t in $(TAGS); do \
		git tag -d "$$t" 2>/dev/null || { echo "Warning: tag $$t not found, skipping."; failed=1; }; \
	done; \
	if [ $$failed -eq 0 ]; then \
		echo "All tags deleted successfully."; \
	else \
		echo "Some tags were missing, but remaining tags deleted."; \
	fi

push:
	git push origin main --tags

.PHONY: release release-check release-verify
release: ## Full release: check, tag every module, push, verify from the proxy (make release V=1.2.3)
	@test -n "$(V)" || { echo "V is required, e.g. make release V=1.2.3"; exit 1; }
	@test -z "$$(git status --porcelain)" || { \
		echo "Error: the working tree is dirty. Tags would point at the last commit, not at what you see."; \
		exit 1; \
	}
	@echo "==> release v$(V)"
	"$(MAKE)" release-check
	"$(MAKE)" tag V=$(V)
	"$(MAKE)" push
	"$(MAKE)" release-verify V=$(V)

release-check: ## Build every published module the way a consumer resolves it - no workspace, no replace (run before 'make tag')
	@status=0; \
	for m in $(ADAPTERS); do \
		if grep -q '^replace' $$m/go.mod; then \
			echo "$$m: a replace directive must not ship in a published module"; status=1; \
		fi; \
	done; \
	core=$$(grep -m1 -h 'zakonnic/memstash v' $(firstword $(ADAPTERS))/go.mod | grep -o 'v[0-9][^ ]*'); \
	for m in $(ADAPTERS); do \
		got=$$(grep -m1 -h 'zakonnic/memstash v' $$m/go.mod | grep -o 'v[0-9][^ ]*'); \
		if [ "$$got" != "$$core" ]; then echo "$$m: requires core $$got, expected $$core"; status=1; fi; \
	done; \
	if git ls-remote --exit-code --tags origin "$$core" >/dev/null 2>&1; then \
		echo "adapters require core $$core (published)"; \
		for m in . $(ADAPTERS); do \
			echo "==> $$m"; \
			GOWORK=off GOFLAGS=-mod=mod go -C $$m build ./... || status=1; \
			GOWORK=off GOFLAGS=-mod=mod go -C $$m vet ./... || status=1; \
		done; \
	else \
		echo "adapters require core $$core, which is not pushed yet - this is the release commit."; \
		echo "Nothing to resolve against until the tag lands: run 'make release-verify V=$${core#v}' after 'make push'."; \
		GOWORK=off go build ./... || status=1; \
	fi; \
	exit $$status

release-verify: ## Install every published module at V from the proxy into a throwaway module and build it (run after 'make push')
	@test -n "$(V)" || { echo "V is required, e.g. make release-verify V=1.2.3"; exit 1; }
	@dir=$$(mktemp -d); status=0; \
	(cd $$dir && go mod init release-verify >/dev/null 2>&1); \
	for m in $(ADAPTERS); do \
		pkg=github.com/zakonnic/memstash/$$m; \
		echo "==> $$pkg@v$(V)"; \
		(cd $$dir && GOWORK=off go get $$pkg@v$(V) >/dev/null 2>&1 && GOWORK=off go build $$pkg) \
			|| { echo "FAILED: $$pkg@v$(V) does not build as published"; status=1; }; \
	done; \
	rm -rf $$dir; \
	if [ $$status -eq 0 ]; then echo "every adapter builds from the proxy at v$(V)"; fi; \
	exit $$status
