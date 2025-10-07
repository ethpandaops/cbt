.PHONY: all generate-api build-web

all: generate-api build-web

generate-api:
	@oapi-codegen -generate fiber,types,spec -package generated -o pkg/api/generated/server.gen.go api/openapi.yaml
	@sed -i 's/github.com\/gofiber\/fiber\/v2/github.com\/gofiber\/fiber\/v3/g' pkg/api/generated/server.gen.go
	@sed -i 's/\*fiber\.Ctx/fiber.Ctx/g' pkg/api/generated/server.gen.go

build-web:
	@echo "Building web frontend..."
	@pnpm --prefix ./web install && pnpm --prefix ./web run build
