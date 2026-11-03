VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS     := -s -w -X main.version=$(VERSION)
STATICCHECK := honnef.co/go/tools/cmd/staticcheck@v0.8.2
GORELEASER  := github.com/goreleaser/goreleaser/v2@v2.19.0

.PHONY: build test test-short lint fmt bench demo run docker up down release-snapshot clean help

build: ## Build bin/vigil
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/vigil ./cmd/vigil

test: ## Run every test with the race detector
	go test -race ./...

test-short: ## Run tests without the race detector (faster)
	go test ./...

lint: ## gofmt, go vet, and staticcheck
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed on:"; gofmt -l .; exit 1)
	go vet ./...
	go run $(STATICCHECK) ./...

fmt: ## Format the code
	gofmt -w .

bench: ## Run benchmarks
	go test -run '^$$' -bench . -benchmem ./internal/sketch ./internal/consensus ./internal/engine

demo: ## Seed 90 days of synthetic history and serve it on :4210
	go run ./cmd/vigil demo

run: ## Run a single-node server with the example config
	go run ./cmd/vigil server --config examples/single-node.yaml

docker: ## Build the container image
	docker build --build-arg VERSION=$(VERSION) -t vigil:local .

up: ## Start server, three probes, flaky target, and Mailpit
	docker compose up --build -d

down: ## Stop the compose stack and delete its volume
	docker compose down -v

release-snapshot: ## Build release archives locally without publishing
	go run $(GORELEASER) release --snapshot --clean

clean:
	rm -rf bin dist vigil.db* vigil-demo.db*

help:
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'
