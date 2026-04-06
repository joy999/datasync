.PHONY: all build test lint clean proto

# Default target
all: build test

# Build the project
build:
	go build -v ./...

# Run all tests
test:
	go test -v -race ./...

# Run tests with coverage
test-coverage:
	go test -v -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Run linter
lint:
	golangci-lint run ./...

# Fix linting issues
lint-fix:
	golangci-lint run --fix ./...

# Generate protobuf files
proto:
	protoc --proto_path=. --go_out=. --go_opt=paths=source_relative proto/datasync.proto

# Format code
fmt:
	go fmt ./...
	gofmt -s -w .

# Tidy go modules
tidy:
	go mod tidy

# Clean build artifacts
clean:
	rm -f coverage.out coverage.html
	go clean -cache

# Run all checks (build, test, lint)
check: build test lint

# Help
help:
	@echo "Available targets:"
	@echo "  make build          - Build the project"
	@echo "  make test           - Run tests"
	@echo "  make test-coverage  - Run tests with coverage report"
	@echo "  make lint           - Run linter"
	@echo "  make lint-fix       - Fix linting issues automatically"
	@echo "  make proto          - Generate protobuf files"
	@echo "  make fmt            - Format code"
	@echo "  make tidy           - Tidy go modules"
	@echo "  make clean          - Clean build artifacts"
	@echo "  make check          - Run all checks (build, test, lint)"
	@echo "  make help           - Show this help message"
