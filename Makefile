.PHONY: build test clean run lint fmt vet install

# Configuration Note:
# CBT uses a single config.yaml file by default for all components.
# Each component (coordinator, worker, CLI) reads the same file but only uses
# the sections relevant to its operation. You can override with --config flag.

# Variables
BINARY_NAME=cbt
MAIN_PATH=main.go
BUILD_DIR=./bin
GO=go
GOLINT=golangci-lint
GOFMT=gofmt

# Build the binary
build:
	@echo "Building $(BINARY_NAME)..."
	@$(GO) build -o $(BUILD_DIR)/$(BINARY_NAME) $(MAIN_PATH)
	@echo "Build complete: $(BUILD_DIR)/$(BINARY_NAME)"

# Run tests
test:
	@echo "Running tests..."
	@$(GO) test -v ./...

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	@$(GO) test -v -coverprofile=coverage.out ./...
	@$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -rf $(BUILD_DIR)
	@rm -f coverage.out coverage.html
	@echo "Clean complete"

# Run the application (shows help)
run: build
	@echo "Running $(BINARY_NAME) help..."
	@$(BUILD_DIR)/$(BINARY_NAME) --help

run-coordinator: build
	@echo "Running coordinator..."
	@$(BUILD_DIR)/$(BINARY_NAME) coordinator

run-worker: build
	@echo "Running worker..."
	@$(BUILD_DIR)/$(BINARY_NAME) worker

# Run linter
lint:
	@echo "Running linter..."
	@$(GOLINT) run ./...

# Format code
fmt:
	@echo "Formatting code..."
	@$(GOFMT) -s -w .
	@$(GO) fmt ./...

# Run go vet
vet:
	@echo "Running go vet..."
	@$(GO) vet ./...

# Install dependencies
deps:
	@echo "Installing dependencies..."
	@$(GO) mod download
	@$(GO) mod tidy

# Install the binary to GOPATH/bin
install:
	@echo "Installing $(BINARY_NAME)..."
	@$(GO) install
	@echo "$(BINARY_NAME) installed to GOPATH/bin"

# Development mode with hot reload (requires air)
dev:
	@which air > /dev/null || (echo "Installing air..." && go install github.com/cosmtrek/air@latest)
	@air

# Docker build
docker-build:
	@echo "Building Docker image..."
	@docker build -t $(BINARY_NAME):latest .

# Generate mocks (if needed)
generate:
	@echo "Generating code..."
	@$(GO) generate ./...

# Check for security vulnerabilities
security:
	@echo "Checking for vulnerabilities..."
	@$(GO) list -json -deps ./... | nancy sleuth

# All checks before commit
check: fmt vet lint test
	@echo "All checks passed!"

# Help
help:
	@echo "Available targets:"
	@echo "  build         - Build the binary"
	@echo "  test          - Run tests"
	@echo "  test-coverage - Run tests with coverage report"
	@echo "  clean         - Remove build artifacts"
	@echo "  run           - Build and show help"
	@echo "  run-coordinator - Run coordinator service"
	@echo "  run-worker    - Run worker service"
	@echo "  lint          - Run golangci-lint"
	@echo "  fmt           - Format code"
	@echo "  vet           - Run go vet"
	@echo "  deps          - Download and tidy dependencies"
	@echo "  install       - Install binary to GOPATH/bin"
	@echo "  dev           - Run in development mode with hot reload"
	@echo "  docker-build  - Build Docker image"
	@echo "  generate      - Run go generate"
	@echo "  security      - Check for security vulnerabilities"
	@echo "  check         - Run all checks (fmt, vet, lint, test)"
	@echo "  help          - Show this help message"