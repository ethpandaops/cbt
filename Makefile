.PHONY: build test test-race test-integration coverage lint generate generate-api generate-frontend-types build-frontend clean

build:
	@go build -o bin/cbt .

# Coverage excludes generated code (*.gen.go, *.mock.go) and test scaffolding (internal/testutil).
# The raw profile is per-PID so concurrent invocations cannot clobber each other.
test:
	@go test -race -shuffle=on -coverprofile=coverage.out.$$$$.tmp -covermode=atomic ./... && \
	grep -a -v -E '(\.gen\.go|\.mock\.go|/internal/testutil/)' coverage.out.$$$$.tmp > coverage.out; \
	rc=$$?; rm -f coverage.out.$$$$.tmp; exit $$rc

coverage: test
	@go tool cover -func=coverage.out | tail -1

test-race:
	@go test -race ./...

test-integration:
	@go test -tags=integration -v ./...

lint:
	@golangci-lint run

generate: generate-api generate-frontend-types
	@go generate ./...

generate-api:
	@go tool oapi-codegen -config api/.oapi-codegen.yaml api/openapi.yaml
# oapi-codegen only emits fiber v2 servers; rewrite imports/signatures for the fiber v3 used by this project
	@sed -i.bak 's/github.com\/gofiber\/fiber\/v2/github.com\/gofiber\/fiber\/v3/g' pkg/api/generated/server.gen.go && rm -f pkg/api/generated/server.gen.go.bak
	@sed -i.bak 's/\*fiber\.Ctx/fiber.Ctx/g' pkg/api/generated/server.gen.go && rm -f pkg/api/generated/server.gen.go.bak

generate-frontend-types:
	@pnpm --prefix ./frontend run generate:api

build-frontend:
	@echo "Building frontend..."
	@pnpm --prefix ./frontend install && pnpm --prefix ./frontend run build

clean:
	@rm -f cbt coverage.out coverage.out.*.tmp
	@rm -rf bin
