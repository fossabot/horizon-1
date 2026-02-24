.PHONY: all build build-linux build-windows build-darwin build-all docker docker-build docker-run clean test

# Version info
VERSION ?= 0.1.0
COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "none")
BUILD_DATE := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS := -ldflags "-s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.buildDate=$(BUILD_DATE)"

# Output directory
DIST := dist

all: build

# Build for current platform
build:
	go build $(LDFLAGS) -o horizon ./cmd/horizon

# Build for Linux
build-linux:
	@mkdir -p $(DIST)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o $(DIST)/horizon-linux-amd64 ./cmd/horizon
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build $(LDFLAGS) -o $(DIST)/horizon-linux-arm64 ./cmd/horizon

# Build for Windows
build-windows:
	@mkdir -p $(DIST)
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build $(LDFLAGS) -o $(DIST)/horizon-windows-amd64.exe ./cmd/horizon
	CGO_ENABLED=0 GOOS=windows GOARCH=arm64 go build $(LDFLAGS) -o $(DIST)/horizon-windows-arm64.exe ./cmd/horizon

# Build for macOS
build-darwin:
	@mkdir -p $(DIST)
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build $(LDFLAGS) -o $(DIST)/horizon-darwin-amd64 ./cmd/horizon
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build $(LDFLAGS) -o $(DIST)/horizon-darwin-arm64 ./cmd/horizon

# Build for all platforms
build-all: build-linux build-windows build-darwin

# Build using Docker (cross-platform)
docker-build:
	docker-compose -f deployments/docker-compose.yml --profile build run --rm builder

# Build Docker image
docker:
	docker build -f build/Dockerfile -t horizon:$(VERSION) -t horizon:latest .

# Run in Docker
docker-run:
	docker-compose -f deployments/docker-compose.yml up -d horizon

# Stop Docker container
docker-stop:
	docker-compose -f deployments/docker-compose.yml down

# Run tests
test:
	go test -v ./...

# Run tests with coverage
test-coverage:
	go test -v -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Format code
fmt:
	go fmt ./...

# Lint code
lint:
	golangci-lint run ./...

# Clean build artifacts
clean:
	rm -rf $(DIST)
	rm -f horizon horizon.exe
	rm -f coverage.out coverage.html

# Download dependencies
deps:
	go mod download
	go mod tidy

# Generate (if needed)
generate:
	go generate ./...

# Help
help:
	@echo "Horizon Build System"
	@echo ""
	@echo "Usage:"
	@echo "  make build          - Build for current platform"
	@echo "  make build-linux    - Build for Linux (amd64, arm64)"
	@echo "  make build-windows  - Build for Windows (amd64, arm64)"
	@echo "  make build-darwin   - Build for macOS (amd64, arm64)"
	@echo "  make build-all      - Build for all platforms"
	@echo "  make docker-build   - Build all platforms using Docker"
	@echo "  make docker         - Build Docker image"
	@echo "  make docker-run     - Run in Docker"
	@echo "  make test           - Run tests"
	@echo "  make clean          - Clean build artifacts"
