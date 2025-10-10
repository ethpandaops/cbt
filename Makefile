.PHONY: generate-api generate-frontend-types build-frontend

generate-api:
	@oapi-codegen -generate fiber,types,spec -package generated -o pkg/api/generated/server.gen.go api/openapi.yaml
	@sed -i 's/github.com\/gofiber\/fiber\/v2/github.com\/gofiber\/fiber\/v3/g' pkg/api/generated/server.gen.go
	@sed -i 's/\*fiber\.Ctx/fiber.Ctx/g' pkg/api/generated/server.gen.go

generate-frontend-types:
	@pnpm --prefix ./frontend run generate:api

build-frontend:
	@echo "Building frontend..."
	@pnpm --prefix ./frontend install && pnpm --prefix ./frontend run build
