.PHONY: all generate-api build-frontend

all: generate-api build-frontend

generate-api:
	@oapi-codegen -generate fiber,types,spec -package generated -o pkg/api/generated/server.gen.go api/openapi.yaml
	@sed -i 's/github.com\/gofiber\/fiber\/v2/github.com\/gofiber\/fiber\/v3/g' pkg/api/generated/server.gen.go
	@sed -i 's/\*fiber\.Ctx/fiber.Ctx/g' pkg/api/generated/server.gen.go

build-frontend:
	@echo "Building frontend..."
	@pnpm --prefix ./frontend install && pnpm --prefix ./frontend run build
