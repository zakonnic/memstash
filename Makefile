DC = docker compose
DC_FILES = -f docker/docker-compose.yml
ifneq (,$(wildcard docker/docker-compose.override.yml))
DC_FILES += -f docker/docker-compose.override.yml
endif

.PHONY: help
help: ## Show help message
	@cat $(MAKEFILE_LIST) | grep -e "^[a-zA-Z_\%-]*: *.*## *" | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

up: ## Start containers for the integration tests (waits for healthchecks)
	$(DC) $(DC_FILES) up -d --wait
down: ## Stop and remove the integration containers
	$(DC) $(DC_FILES) down

update-packages: ## Update go modules versions
	go get -u ./...
	go mod tidy

lint: ## Run linter with settings from .golangci.yml
	golangci-lint run -v
lint-fix: ## Linter tries to fix issues automatically
	golangci-lint run -v --fix

.PHONY: test
test: ## Run local tests
	go test -v ./...

cover-gen: ## Generate merged test coverage across all packages (tests/ and l2/ exercise the root and internal packages)
	@mkdir -p var
	go test -count=1 -coverpkg=./... -coverprofile=var/coverage.out ./...
	go tool cover -func=var/coverage.out | tail -1
cover-func: cover-gen ## Show coverage by func
	go tool cover -func=var/coverage.out
.PHONY: cover
cover: cover-gen ## Show coverage html
	go tool cover -html=var/coverage.out

bench-speed: ## Run speed benchmarks
	go -C benchmarks test -run='^BenchmarkGetHit$$' -bench . ./...
bench-hitrate: ## Run hitrate benchmarks
	go -C benchmarks test -run='^TestHitRate$$' -v
bench-hitrate-real: ## Run hitrate benchmarks
	go -C benchmarks test -run='^TestHitRateRealistic$$' -v
.PHONY: bench
bench: bench-speed bench-hitrate ## Run benchmarks
bench-100kk:
	go -C benchmarks test -run xxx -bench BenchmarkMemoryFootprintMemstash -tags=long
bench-100kk-all:
	go -C benchmarks test -run xxx -bench BenchmarkMemoryFootprint -tags=others

integration-tests: ## Run integration tests against live redis/memcached (make up first); CGO off so the cgo-only valyala adapter is skipped
	CGO_ENABLED=0 go -C tests/integration test ./... -v
integration-bench: up ## Run L1+L2 load-profile benchmarks against the live servers (make up first)
	CGO_ENABLED=0 go -C tests/integration test -run xxx -bench . -benchtime 1s ./...

.PHONY: load-generator
load-generator: ## Build the long-running load generator (+ config.yaml) into benchmarks/bin
	go -C benchmarks build -o bin/load-generator$(if $(filter Windows_NT,$(OS)),.exe,) ./load_generator
	cp benchmarks/load_generator/config.yaml benchmarks/bin/config.yaml

check-new-libs: ## Checks for new versions of libraries
	@OUT=$$(go list -m -u -f '{{if .Update}}{{.Path}}: {{.Version}} -> {{.Update.Version}}{{printf "\n"}}{{end}}' all); \
	if [ -n "$$OUT" ]; then \
		echo "$$OUT"; \
		echo "Run 'make update-packages' to update"; \
	else \
		echo "All dependencies are up to date"; \
	fi

L2_MODULES = \
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
	l2/valyala_adapter

.PHONY: tag
tag: ## Tag the root module and every l2 adapter module with the given version (make tag V=1.2.3), then 'make push'
	@test -n "$(V)" || { echo "V is required, e.g. make tag V=1.2.3"; exit 1; }
	@for tag in v$(V) $(addsuffix /v$(V),$(L2_MODULES)); do \
		if git rev-parse -q --verify "refs/tags/$$tag" > /dev/null; then \
			echo "exists  $$tag"; \
		else \
			git tag "$$tag" || exit 1; \
			echo "tagged  $$tag"; \
		fi; \
	done

push:
	git push origin main --tags
