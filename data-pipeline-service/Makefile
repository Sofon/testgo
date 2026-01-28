.PHONY: build run test clean docker-build docker-up docker-down lint fmt tidy

# Variables
APP_NAME := data-pipeline-service
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME := $(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS := -ldflags "-X github.com/sofon/data-pipeline-service/internal/service.Version=$(VERSION) -X github.com/sofon/data-pipeline-service/internal/service.BuildTime=$(BUILD_TIME)"

# Build
build:
	go build $(LDFLAGS) -o bin/$(APP_NAME) ./cmd/server

# Run locally
run: build
	./bin/$(APP_NAME) -config configs/config.yaml

# Run with hot reload (requires air: go install github.com/cosmtrek/air@latest)
dev:
	air -c .air.toml

# Test
test:
	go test -v -race ./...

# Test with coverage
test-coverage:
	go test -v -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Lint (requires golangci-lint)
lint:
	golangci-lint run ./...

# Format code
fmt:
	go fmt ./...
	goimports -w .

# Tidy dependencies
tidy:
	go mod tidy

# Clean build artifacts
clean:
	rm -rf bin/
	rm -f coverage.out coverage.html

# Docker build
docker-build:
	docker build -t $(APP_NAME):$(VERSION) \
		--build-arg VERSION=$(VERSION) \
		--build-arg BUILD_TIME=$(BUILD_TIME) \
		.

# Docker compose up
docker-up:
	cd deployments && docker-compose up -d

# Docker compose down
docker-down:
	cd deployments && docker-compose down

# Docker compose logs
docker-logs:
	cd deployments && docker-compose logs -f data-pipeline

# Generate mocks (requires mockgen)
mocks:
	mockgen -source=internal/api/handlers.go -destination=internal/api/mocks/service_mock.go -package=mocks

# Install development tools
tools:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install golang.org/x/tools/cmd/goimports@latest
	go install github.com/cosmtrek/air@latest
	go install github.com/golang/mock/mockgen@latest

# Help
help:
	@echo "Available targets:"
	@echo "  build         - Build the application"
	@echo "  run           - Build and run locally"
	@echo "  dev           - Run with hot reload"
	@echo "  test          - Run tests"
	@echo "  test-coverage - Run tests with coverage"
	@echo "  lint          - Run linter"
	@echo "  fmt           - Format code"
	@echo "  tidy          - Tidy go modules"
	@echo "  clean         - Clean build artifacts"
	@echo "  docker-build  - Build Docker image"
	@echo "  docker-up     - Start docker-compose"
	@echo "  docker-down   - Stop docker-compose"
	@echo "  docker-logs   - View docker-compose logs"
	@echo "  tools         - Install development tools"
