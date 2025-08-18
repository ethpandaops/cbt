.PHONY: up down restart logs status clean build

# Start all services
up:
	docker-compose up -d

# Stop all services
down:
	docker-compose down

# Restart all services
restart: down up

# View logs
logs:
	docker-compose logs -f

# Check service status
status:
	docker-compose ps

# Clean up volumes and containers
clean:
	docker-compose down -v

# Build images
build:
	docker-compose build --no-cache

# View coordinator logs
logs-coordinator:
	docker-compose logs -f coordinator

# View worker logs
logs-worker:
	docker-compose logs -f worker

# Scale workers
scale-workers:
	docker-compose up -d --scale worker=$(WORKERS)

# Example: make scale-workers WORKERS=5