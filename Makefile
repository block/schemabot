# Makefile for SchemaBot - Declarative Schema GitOps

# Build metadata injected via -ldflags
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT  ?= $(shell git rev-parse HEAD 2>/dev/null || echo "unknown")
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)

# Test runner: use gotestsum if available for nicer output, otherwise fall back to go test
# Check PATH first, then GOBIN, then default ~/go/bin
GOBIN := $(shell go env GOBIN 2>/dev/null)
ifeq ($(GOBIN),)
  GOBIN := $(HOME)/go/bin
endif
GOTESTSUM := $(shell command -v gotestsum 2>/dev/null || (test -x $(GOBIN)/gotestsum && echo $(GOBIN)/gotestsum))
ifdef GOTESTSUM
  GOTEST := $(GOTESTSUM) --format pkgname-and-test-fails --
else
  GOTEST := go test
endif

.PHONY: help lint lint-fix setup test test-unit test-e2e test-e2e-grpc test-integration test-coverage build install clean proto up up-grpc down down-grpc status mysql logs logs-grpc test-endpoints plan-testapp apply-testapp progress-testapp seed-testapp seed-testapp-large demo demo-grpc demo-grpc-logs wait-healthy wait-healthy-grpc cli fmt-schema fmt-schema-check

# Multi-line message definitions
define HELP_HEADER
SchemaBot - Declarative Schema GitOps

Available targets:
endef
export HELP_HEADER

define DEMO_COMMANDS

  Try these commands:
    schemabot plan -s examples/mysql/schema/testapp                  Preview DDL
    schemabot apply -s examples/mysql/schema/testapp -e staging     Execute schema change
    schemabot progress --database testapp                           Poll progress and ETA
    schemabot status                                                Show active schema changes
    schemabot logs --database testapp                               View apply logs
    schemabot help                                                  See all commands

  Tail server logs:
    make logs
============================================
endef
export DEMO_COMMANDS

define DEMO_READY_MSG

============================================
  SCHEMABOT DEMO READY
  SchemaBot API:     http://localhost:13370
  Staging MySQL:     localhost:13372 (testapp)
  Production MySQL:  localhost:13373 (testapp)
  Data: 50 MB per table in each environment
$(DEMO_COMMANDS)
endef
export DEMO_READY_MSG

define DEMO_SKIP_APPLY_MSG

============================================
  SCHEMABOT SERVER READY (SKIP_APPLY mode)
  SchemaBot API:     http://localhost:13370
  Staging MySQL:     localhost:13372 (testapp)
  Production MySQL:  localhost:13373 (testapp)

  Schema applies skipped. To apply manually:
    make apply-testapp ENV=staging
    make apply-testapp ENV=production
$(DEMO_COMMANDS)
endef
export DEMO_SKIP_APPLY_MSG

define DEMO_GRPC_READY_MSG

============================================
  SCHEMABOT gRPC DEMO READY
  SchemaBot API:        http://localhost:13380
  Tern Staging (HTTP):  http://localhost:13384
  Tern Staging (gRPC):  localhost:13385
  Tern Prod (HTTP):     http://localhost:13386
  Tern Prod (gRPC):     localhost:13387
  Staging MySQL:        localhost:13382 (testapp)
  Production MySQL:     localhost:13383 (testapp)
  Data: 50 MB per table in each environment
============================================
endef
export DEMO_GRPC_READY_MSG

define DEMO_GRPC_SKIP_APPLY_MSG

============================================
  SCHEMABOT gRPC SERVER READY (SKIP_APPLY mode)
  SchemaBot API:        http://localhost:13380
  Tern Staging (HTTP):  http://localhost:13384
  Tern Staging (gRPC):  localhost:13385
  Tern Prod (HTTP):     http://localhost:13386
  Tern Prod (gRPC):     localhost:13387
  Staging MySQL:        localhost:13382 (testapp)
  Production MySQL:     localhost:13383 (testapp)
============================================
endef
export DEMO_GRPC_SKIP_APPLY_MSG

help: ## Show this help message
	@echo "$$HELP_HEADER"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

lint: check-closeandlog ## Run all linters (golangci-lint + closeandlog)
	@echo "Running golangci-lint..."
	@docker run --rm -v $$(pwd):/app -w /app golangci/golangci-lint:latest golangci-lint run --timeout=5m

check-closeandlog: ## Run closeandlog analyzer (flags _ = x.Close() patterns)
	@echo "Running closeandlog analyzer..."
	@go run ./cmd/closeandlog-check ./...
	@go run -tags=integration ./cmd/closeandlog-check ./...

lint-fix: ## Run golangci-lint with auto-fix enabled
	@echo "Running golangci-lint with auto-fix..."
	@docker run --rm -v $$(pwd):/app -w /app golangci/golangci-lint:latest golangci-lint run --fix --timeout=5m

setup: ## Set up git hooks for development
	git config core.hooksPath .githooks
	@echo "Setup complete. Git hooks enabled (.githooks/pre-commit)."

clean: ## Clean build artifacts
	@echo "Cleaning..."
	@rm -rf bin/
	@rm -f coverage.out

# Generate protobuf code (only if .proto is newer than generated .pb.go)
proto: ## Generate protobuf code
	@if [ pkg/proto/tern.proto -nt pkg/proto/ternv1/tern.pb.go ]; then \
		echo "Proto file changed, regenerating..."; \
		buf generate; \
	fi

# =============================================================================
# Local Development (not for production)
# =============================================================================
# These commands start a local dev environment with:
# - SchemaBot MySQL (port 13371, schemabot database)
# - Staging MySQL (port 13372, testapp database)
# - Production MySQL (port 13373, testapp database)
# - SchemaBot service (port 13370)
# For production deployment, see docs/guides/deployment.md

# Start local dev environment (embedded Tern, no gRPC required)
#   make up           # Start services (ports: 13370, 13371, 13372)
#   make up FRESH=1   # Reinitialize databases (removes volumes first)
up:
ifeq ($(FRESH),1)
	docker compose -f deploy/local/docker-compose.yml down -v 2>/dev/null || true
endif
	docker compose -f deploy/local/docker-compose.yml up --build

# Start services, apply testapp schema, then show logs (full demo workflow)
#   make demo              # Start and apply testapp schema (wipes data)
#   make demo KEEP_DATA=1  # Restart without wiping data (preserves seeded rows)
#   make demo SKIP_APPLY=1 # Start server only, skip schema applies (for debugging)
demo:
ifeq ($(KEEP_DATA),1)
	docker compose -f deploy/local/docker-compose.yml down -t 10 2>/dev/null || true
else
	docker compose -f deploy/local/docker-compose.yml down -v -t 10 2>/dev/null || true
endif
	docker rm -f schemabot-mysql-schemabot-1 schemabot-mysql-staging-1 schemabot-mysql-production-1 schemabot-schemabot-1 2>/dev/null || true
	@# Start MySQL containers first (they take ~13s to init), then build Go in parallel
	docker compose -f deploy/local/docker-compose.yml up -d mysql-schemabot mysql-staging mysql-production
	CGO_ENABLED=0 GOOS=linux go build -ldflags "$(LDFLAGS)" -o bin/schemabot-linux ./pkg/cmd & \
		go build -ldflags "$(LDFLAGS)" -o bin/schemabot ./pkg/cmd & \
		wait
	@$(MAKE) cli
	cp bin/schemabot-linux deploy/local/schemabot
	docker compose -f deploy/local/docker-compose.yml up --build -d schemabot
	@rm -f deploy/local/schemabot
	@$(MAKE) wait-healthy
ifneq ($(SKIP_APPLY),1)
	@./bin/schemabot apply -s examples/mysql/schema/testapp -e staging --endpoint http://localhost:13370 -y --allow-unsafe -o log
	@./bin/schemabot apply -s examples/mysql/schema/testapp -e production --endpoint http://localhost:13370 -y --allow-unsafe -o log
	@./scripts/seed-large.sh 160 both
	@echo "$$DEMO_READY_MSG"
else
	@echo "$$DEMO_SKIP_APPLY_MSG"
endif

# Wait for SchemaBot to be healthy
wait-healthy:
	@echo "Waiting for SchemaBot to be healthy..."
	@for i in $$(seq 1 30); do \
		if curl -sf http://localhost:13370/health > /dev/null 2>&1; then \
			echo "SchemaBot is healthy"; \
			exit 0; \
		fi; \
		sleep 1; \
	done; \
	echo "Timeout waiting for SchemaBot"; exit 1

# Plan example schema changes (dry-run)
#   make plan-testapp              # Plan for staging (default)
#   make plan-testapp ENV=production
plan-testapp: build
	./bin/schemabot plan -s examples/mysql/schema/testapp -e $(or $(ENV),staging) --endpoint http://localhost:13370

# Apply example schema to testapp database using SchemaBot CLI
#   make apply-testapp              # Apply to staging (default)
#   make apply-testapp ENV=production
apply-testapp: build
	./bin/schemabot apply -s examples/mysql/schema/testapp -e $(or $(ENV),staging) --endpoint http://localhost:13370 -y --allow-unsafe -o log

# Check migration progress for testapp
#   make progress-testapp              # Check staging (default)
#   make progress-testapp ENV=production
progress-testapp: build
	./bin/schemabot progress \
		--database testapp \
		-e $(or $(ENV),staging) \
		--endpoint http://localhost:13370

# Seed testapp with large dataset for testing progress rendering
#   make seed-testapp-large                    # 100 MB per table, staging only
#   make seed-testapp-large TARGET_MB=500      # 500 MB per table
#   make seed-testapp-large ENV=both           # Both environments
seed-testapp-large:
	@./scripts/seed-large.sh $(or $(TARGET_MB),100) $(or $(ENV),staging)

# Seed testapp with sample data (10k rows each for users and orders)
#   make seed-testapp              # Seed both staging and production
#   make seed-testapp ENV=staging  # Seed staging only
seed-testapp:
	@./scripts/seed.sh $(or $(ENV),both)

# Run all e2e tests (local + gRPC) in isolated docker-compose environments.
test-e2e: test-e2e-local test-e2e-grpc ## Run all e2e tests

# Run local e2e tests in an isolated environment (no conflicts with make demo)
# This spins up a separate docker-compose stack on different ports, runs tests, then tears down.
# Ports: SchemaBot=14370, MySQL-SchemaBot=14371, MySQL-Staging=14372, MySQL-Production=14373
test-e2e-local: build ## Run local e2e tests in isolated environment
	@echo "Starting isolated e2e environment..."
	CGO_ENABLED=0 GOOS=linux go build -ldflags "$(LDFLAGS)" -o bin/schemabot-linux ./pkg/cmd
	cp bin/schemabot-linux deploy/local/schemabot
	@SCHEMABOT_PORT=14370 \
	SCHEMABOT_MYSQL_PORT=14371 \
	STAGING_MYSQL_PORT=14372 \
	PRODUCTION_MYSQL_PORT=14373 \
	docker compose -p schemabot-e2e -f deploy/local/docker-compose.yml down -v 2>/dev/null || true
	@SCHEMABOT_PORT=14370 \
	SCHEMABOT_MYSQL_PORT=14371 \
	STAGING_MYSQL_PORT=14372 \
	PRODUCTION_MYSQL_PORT=14373 \
	docker compose -p schemabot-e2e -f deploy/local/docker-compose.yml up --build -d || \
		(echo "docker compose up failed — dumping logs:"; \
		SCHEMABOT_PORT=14370 SCHEMABOT_MYSQL_PORT=14371 STAGING_MYSQL_PORT=14372 PRODUCTION_MYSQL_PORT=14373 \
		docker compose -p schemabot-e2e -f deploy/local/docker-compose.yml logs; \
		SCHEMABOT_PORT=14370 SCHEMABOT_MYSQL_PORT=14371 STAGING_MYSQL_PORT=14372 PRODUCTION_MYSQL_PORT=14373 \
		docker compose -p schemabot-e2e -f deploy/local/docker-compose.yml down -v; \
		rm -f deploy/local/schemabot; \
		exit 1)
	@rm -f deploy/local/schemabot
	@echo "Waiting for SchemaBot e2e environment to be healthy..."
	@for i in $$(seq 1 60); do \
		if curl -sf http://localhost:14370/health > /dev/null 2>&1; then \
			echo "SchemaBot e2e environment is healthy"; \
			break; \
		fi; \
		if [ $$i -eq 60 ]; then \
			echo "Timeout waiting for SchemaBot e2e environment"; \
			SCHEMABOT_PORT=14370 SCHEMABOT_MYSQL_PORT=14371 STAGING_MYSQL_PORT=14372 PRODUCTION_MYSQL_PORT=14373 \
			docker compose -p schemabot-e2e -f deploy/local/docker-compose.yml logs; \
			SCHEMABOT_PORT=14370 SCHEMABOT_MYSQL_PORT=14371 STAGING_MYSQL_PORT=14372 PRODUCTION_MYSQL_PORT=14373 \
			docker compose -p schemabot-e2e -f deploy/local/docker-compose.yml down -v; \
			exit 1; \
		fi; \
		sleep 1; \
	done
	@echo "Applying testapp schema to e2e environment..."
	@./bin/schemabot apply -s examples/mysql/schema/testapp -e staging --endpoint http://localhost:14370 -y --allow-unsafe -o log
	@./bin/schemabot apply -s examples/mysql/schema/testapp -e production --endpoint http://localhost:14370 -y --allow-unsafe -o log
	@echo "Running e2e tests..."
	@E2E_SCHEMABOT_URL=http://localhost:14370 \
	E2E_MYSQL_DSN="root:testpassword@tcp(localhost:14371)/schemabot" \
	E2E_TESTAPP_STAGING_DSN="root:testpassword@tcp(localhost:14372)/testapp" \
	E2E_TESTAPP_PRODUCTION_DSN="root:testpassword@tcp(localhost:14373)/testapp" \
	$(GOTEST) -count=1 -timeout=5m -tags=e2e ./e2e/local/... ; \
	TEST_EXIT_CODE=$$?; \
	echo "Tearing down e2e environment..."; \
	SCHEMABOT_PORT=14370 SCHEMABOT_MYSQL_PORT=14371 STAGING_MYSQL_PORT=14372 PRODUCTION_MYSQL_PORT=14373 \
	docker compose -p schemabot-e2e -f deploy/local/docker-compose.yml down -v; \
	exit $$TEST_EXIT_CODE

# Run gRPC e2e tests in an isolated environment
# This spins up SchemaBot + separate Tern services (gRPC mode), runs tests, then tears down.
# Ports: SchemaBot=15370, SchemaBot-MySQL=15371, Tern-Staging-MySQL=15372, Tern-Production-MySQL=15373
#        Tern-Staging-HTTP=15380, Tern-Staging-gRPC=15390, Tern-Production-HTTP=15382, Tern-Production-gRPC=15392
E2E_GRPC_ENV := SCHEMABOT_PORT=15370 \
	SCHEMABOT_MYSQL_PORT=15371 \
	TERN_STAGING_MYSQL_PORT=15372 \
	TERN_PRODUCTION_MYSQL_PORT=15373 \
	TERN_STAGING_PORT=15380 \
	TERN_STAGING_GRPC_PORT=15390 \
	TERN_PRODUCTION_PORT=15382 \
	TERN_PRODUCTION_GRPC_PORT=15392

test-e2e-grpc: build ## Run gRPC e2e tests in isolated environment
	@echo "Starting isolated gRPC e2e environment..."
	CGO_ENABLED=0 GOOS=linux go build -ldflags "$(LDFLAGS)" -o bin/schemabot-linux ./pkg/cmd
	cp bin/schemabot-linux deploy/local/schemabot
	@$(E2E_GRPC_ENV) docker compose -p schemabot-e2e-grpc -f deploy/local/docker-compose.grpc.yml down -v 2>/dev/null || true
	@$(E2E_GRPC_ENV) docker compose -p schemabot-e2e-grpc -f deploy/local/docker-compose.grpc.yml up --build -d || \
		(echo "docker compose up failed — dumping logs:"; \
		$(E2E_GRPC_ENV) docker compose -p schemabot-e2e-grpc -f deploy/local/docker-compose.grpc.yml logs; \
		$(E2E_GRPC_ENV) docker compose -p schemabot-e2e-grpc -f deploy/local/docker-compose.grpc.yml down -v; \
		rm -f deploy/local/schemabot; \
		exit 1)
	@rm -f deploy/local/schemabot
	@echo "Waiting for SchemaBot gRPC e2e environment to be healthy..."
	@for i in $$(seq 1 90); do \
		if curl -sf http://localhost:15370/health > /dev/null 2>&1; then \
			echo "SchemaBot gRPC e2e environment is healthy"; \
			break; \
		fi; \
		if [ $$i -eq 90 ]; then \
			echo "Timeout waiting for SchemaBot gRPC e2e environment"; \
			$(E2E_GRPC_ENV) docker compose -p schemabot-e2e-grpc -f deploy/local/docker-compose.grpc.yml logs; \
			$(E2E_GRPC_ENV) docker compose -p schemabot-e2e-grpc -f deploy/local/docker-compose.grpc.yml down -v; \
			exit 1; \
		fi; \
		sleep 1; \
	done
	@echo "Running gRPC e2e tests..."
	@E2E_SCHEMABOT_URL=http://localhost:15370 \
	E2E_SCHEMABOT_MYSQL_DSN="root:testpassword@tcp(localhost:15371)/schemabot" \
	E2E_TERN_STAGING_MYSQL_DSN="root:testpassword@tcp(localhost:15372)/testapp" \
	E2E_TERN_PRODUCTION_MYSQL_DSN="root:testpassword@tcp(localhost:15373)/testapp" \
	$(GOTEST) -count=1 -v -tags=e2e -timeout=10m ./e2e/grpc/... ; \
	TEST_EXIT_CODE=$$?; \
	echo "Tearing down gRPC e2e environment..."; \
	$(E2E_GRPC_ENV) docker compose -p schemabot-e2e-grpc -f deploy/local/docker-compose.grpc.yml down -v; \
	exit $$TEST_EXIT_CODE

# Start local dev environment with gRPC backend (separate Tern server)
up-grpc:
	docker compose -f deploy/local/docker-compose.grpc.yml up --build

# Full gRPC demo: start services, apply schema, seed data
#   make demo-grpc              # Start and apply testapp schema (wipes data)
#   make demo-grpc KEEP_DATA=1  # Restart without wiping data
#   make demo-grpc SKIP_APPLY=1 # Start server only, skip schema applies
#   make demo-grpc-logs         # Same as demo-grpc, then tail logs
demo-grpc:
ifeq ($(KEEP_DATA),1)
	docker compose -f deploy/local/docker-compose.grpc.yml down -t 10 2>/dev/null || true
else
	docker compose -f deploy/local/docker-compose.grpc.yml down -v -t 10 2>/dev/null || true
endif
	@# Start MySQL containers first, then build Go in parallel
	docker compose -f deploy/local/docker-compose.grpc.yml up -d tern-staging-mysql tern-production-mysql schemabot-mysql
	CGO_ENABLED=0 GOOS=linux go build -ldflags "$(LDFLAGS)" -o bin/schemabot-linux ./pkg/cmd & \
		go build -ldflags "$(LDFLAGS)" -o bin/schemabot ./pkg/cmd & \
		wait
	@$(MAKE) cli
	cp bin/schemabot-linux deploy/local/schemabot
	docker compose -f deploy/local/docker-compose.grpc.yml up --build -d
	@rm -f deploy/local/schemabot
	@$(MAKE) wait-healthy-grpc
ifneq ($(SKIP_APPLY),1)
	@./bin/schemabot apply -s examples/mysql/schema/testapp -e staging --endpoint http://localhost:13380 -y --allow-unsafe -o log
	@./bin/schemabot apply -s examples/mysql/schema/testapp -e production --endpoint http://localhost:13380 -y --allow-unsafe -o log
	@./scripts/seed-grpc.sh 50 both
	@echo "$$DEMO_GRPC_READY_MSG"
else
	@echo "$$DEMO_GRPC_SKIP_APPLY_MSG"
endif

# Full gRPC demo + tail logs
demo-grpc-logs: demo-grpc
	-docker compose -f deploy/local/docker-compose.grpc.yml logs -f

# Wait for gRPC SchemaBot to be healthy
wait-healthy-grpc:
	@echo "Waiting for gRPC SchemaBot to be healthy..."
	@for i in $$(seq 1 60); do \
		if curl -sf http://localhost:13380/health > /dev/null 2>&1; then \
			echo "gRPC SchemaBot is healthy"; \
			exit 0; \
		fi; \
		sleep 1; \
	done; \
	echo "Timeout waiting for gRPC SchemaBot"; exit 1

# Show endpoint URLs (useful with PORTS=dynamic)
status:
	@echo "SchemaBot: http://localhost:$$(docker compose -f deploy/local/docker-compose.yml port schemabot 8080 2>/dev/null | cut -d: -f2)"
	@echo "MySQL:     localhost:$$(docker compose -f deploy/local/docker-compose.yml port mysql 3306 2>/dev/null | cut -d: -f2)"

# Connect to MySQL
#   make mysql                  # Connect to schemabot storage (port 13371)
#   make mysql DB=staging       # Connect to staging testapp (port 13372)
#   make mysql DB=production    # Connect to production testapp (port 13373)
DB ?= schemabot
PORT ?= $(if $(filter staging,$(DB)),13372,$(if $(filter production,$(DB)),13373,13371))
DBNAME ?= $(if $(filter staging production,$(DB)),testapp,$(DB))
mysql:
	mysql -h 127.0.0.1 -P $(PORT) -u root -ptestpassword $(DBNAME)

# Stop local services
down:
	docker compose -f deploy/local/docker-compose.yml down

# Stop gRPC services
down-grpc:
	docker compose -f deploy/local/docker-compose.grpc.yml down

# View local logs
logs:
	docker compose -f deploy/local/docker-compose.yml logs -f

# View gRPC logs
logs-grpc:
	docker compose -f deploy/local/docker-compose.grpc.yml logs -f

# Build binaries
build: ## Build the schemabot binary
	go build -ldflags "$(LDFLAGS)" -o bin/schemabot ./pkg/cmd

# Install CLI to GOBIN
install: ## Install schemabot CLI to GOBIN
	go build -ldflags "$(LDFLAGS)" -o $(GOBIN)/schemabot ./pkg/cmd

# Install CLI to /usr/local/bin (available on PATH)
cli: build ## Install schemabot CLI to /usr/local/bin
	cp bin/schemabot /usr/local/bin/schemabot

# Run all tests (unit with race detection + integration + e2e)
test: proto test-unit test-integration test-e2e ## Run all tests

# Run unit tests only (with race detection, no testcontainers)
test-unit: ## Run unit tests with race detection
	$(GOTEST) -race ./...

# Run integration tests (uses testcontainers for MySQL)
# Note: -race is omitted because Spirit (upstream) has known data races in
# Runner.initChunkers/Progress. Our unit tests cover race detection for our code.
test-integration: ## Run integration tests
	$(GOTEST) -tags=integration -timeout=10m ./...

# Run tests with coverage (unit + integration, merged per-package breakdown)
#   make test-coverage              # Unit + integration (requires Docker for testcontainers)
#   make test-coverage MODE=unit    # Unit tests only (no Docker needed)
#   make test-coverage MODE=all     # Unit + integration + e2e local (requires Docker Compose)
test-coverage: ## Run tests with coverage report
ifeq ($(MODE),unit)
	@./scripts/coverage.sh --unit-only
else ifeq ($(MODE),all)
	@./scripts/coverage.sh --all
else
	@./scripts/coverage.sh
endif

fmt-schema: ## Format embedded schema files via spirit fmt (auto-detects or starts MySQL)
	@scripts/run-spirit-fmt.sh

fmt-schema-check: ## Check if embedded schema files are canonically formatted (for CI)
	@scripts/run-spirit-fmt.sh --check

.DEFAULT_GOAL := help

# Test API endpoints
test-endpoints:
	@./scripts/test-endpoints.sh
