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

