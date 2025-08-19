# CBT Example

This directory contains an example deployment configuration for the ClickHouse Build Tool (CBT).

## Directory Structure

```
example/
├── config.yaml          # Single configuration file for all services
├── docker-compose.yml   # Docker Compose orchestration
├── init-clickhouse/     # ClickHouse initialization scripts
└── models/             # Example data models
    ├── external/       # External data source models
    └── transformations/ # Transformation models
```

## Quick Start

1. **Start all services:**
   ```bash
   cd example
   docker-compose up -d
   ```

2. **Check service status:**
   ```bash
   docker-compose ps
   ```

3. **View logs:**
   ```bash
   docker-compose logs -f engine
   ```

4. **Stop services:**
   ```bash
   docker-compose down
   ```

## Services

- **ClickHouse** (port 8123): Data warehouse
- **Redis** (port 6379): Task queue backend
- **Coordinator**: Schedules and manages tasks (health check on port 8080)
- **Workers** (2 instances): Process transformation tasks in parallel (health check on port 8080)
- **Data Generator**: Simulates blockchain data with backfill and real-time generation
- **Asynqmon** (port 8080): Task queue monitoring UI

All services use the same `config.yaml` file, with each component using only the configuration sections it needs.

## Configuration

The example uses a single `config.yaml` file for all components, which simplifies deployment and ensures consistency. The configuration includes:

- **ClickHouse connection** (uses Docker service name `clickhouse`)
- **Redis connection** (uses Docker service name `redis`)
- **Coordinator settings**: Scheduling intervals, batch sizes
- **Worker settings**: Queue configuration, concurrency limits
- **Logging levels**: Set to `debug` for detailed output

Each component (engine, CLI) reads the same configuration file but only uses the sections relevant to its operation. This approach:
- Ensures all components use the same database connections
- Simplifies configuration management
- Makes it easier to deploy and maintain

### Data Generator Configuration

The data generator can be configured via environment variables in `docker-compose.yml`:
- `BACKFILL_HOURS`: Hours of historical data to generate on startup (default: 2)
- `INTERVAL_SECONDS`: Seconds between blocks (default: 12)

The generator will:
1. **Backfill** historical data for the specified hours on startup
2. **Forward fill** real-time data continuously 
3. **Occasionally backfill** random gaps to simulate late-arriving data

## Models

The example includes several models demonstrating CBT's capabilities:

### External Models (Data Sources)
- **beacon_blocks**: Raw blockchain data with propagation metrics
- **validator_entity**: Maps validators to their operating entities

### Transformation Models (Data Pipeline)
1. **block_propagation**: Calculates propagation statistics per block
   - Dependencies: beacon_blocks
   - Aggregates: avg, median, p90 propagation times

2. **block_entity**: Enriches blocks with entity information
   - Dependencies: beacon_blocks, validator_entity
   - Joins validator data with entity names

3. **entity_network_effects**: Aggregated entity performance metrics per time window
   - Dependencies: block_entity, block_propagation
   - Calculates: avg, median, min, max propagation times per entity per 5-minute window
   - Demonstrates multi-level dependency chains and different interval sizes

Models will be automatically discovered and processed by the coordinator.

## Monitoring

- **Asynqmon UI**: http://localhost:8080 - Monitor task queues and processing status
- **ClickHouse**: http://localhost:8123/play - Query interface for data inspection

### Health Checks

Services expose health check endpoints on port 8081:
- `/health` - Liveness probe (is the service running?)
- `/ready` - Readiness probe (is the service ready to accept work?)

Docker Compose automatically monitors these endpoints and will restart unhealthy services.

Check health status:
```bash
# Check coordinator health
docker exec cbt-coordinator wget -qO- http://localhost:8081/health

# Check worker readiness (from host, requires port mapping)
docker exec cbt-example-worker-1 wget -qO- http://localhost:8081/ready

# View health status in docker-compose
docker-compose ps
```

## Scaling

Scale workers horizontally:
```bash
docker-compose up -d --scale worker=5
```

Note: Coordinators are designed to run as a single instance. Multiple coordinators are safe during deployments but inefficient for permanent operation.