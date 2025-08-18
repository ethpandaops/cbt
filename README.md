# CBT - ClickHouse Build Tool

A simplified, ClickHouse-focused data transformation tool that provides idempotent transformations, DAG-based dependency management, and interval-based processing.

## Features

- **Idempotent/Replayable Transformations**: Uses ReplacingMergeTree to handle updates cleanly
- **DAG-based Dependency Management**: Models declare dependencies, system validates cycles and processes in topological order
- **Interval-based Processing**: Data processed in fixed chunks enabling parallel processing and efficient retries
- **Pull-through Validation**: Workers verify dependencies before execution using cached external models and admin table tracking
- **Task Queue Architecture**: Asynq with Redis provides distributed, resilient task processing
- **Redis-Persisted Scheduling**: Asynq Scheduler with real cron expression support and persistence across restarts
- **Hybrid Push-Pull Scheduling**: Event-driven dependent processing with scheduled sweeps for reliability
- **Configurable Backfill**: Separate scheduling for forward processing and gap detection with minimum position support
- **Tag-based Worker Filtering**: Workers can selectively process models based on tags for multi-tenant or specialized processing
- **External Script Support**: Models can execute Python or other scripts for complex transformations beyond SQL
- **Lag Support for External Models**: Configure lag to ignore recent data that may still be arriving (e.g., `lag: 30` ignores last 30 seconds)

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Coordinator   │◄──►│      Redis      │◄──►│     Worker      │
│  (+ Scheduler)  │    │ (Queue + Sched) │    │                 │
└─────┬───────────┘    └─────────────────┘    └─────┬───────────┘
      │                                              │
      │                ┌─────────────────┐          │
      └───────────────►│   ClickHouse    │◄─────────┘
                       │  (Data + Admin) │
                       └─────────────────┘
```

**Multi-instance behavior:**
- **Coordinator**: Designed to run as a single instance. Includes Asynq Scheduler for Redis-persisted scheduling with real cron expression support. Multiple coordinators are safe during deployments (rolling updates, failover) but inefficient for permanent operation as they duplicate scheduling work. Task deduplication prevents duplicate processing.
- **Worker**: Designed for horizontal scaling. Run multiple workers to increase throughput. Tasks are distributed across workers automatically.

## Requirements

- ClickHouse 21.8+ (ReplacingMergeTree, FINAL queries)
- Redis 6.0+ (task queue)
- Go 1.21+ (for building from source)

## Installation

### From Source
```bash
git clone https://github.com/ethpandaops/cbt.git
cd cbt
make build
```

### Docker
```bash
docker pull ghcr.io/ethpandaops/cbt:latest
```

## Configuration

Copy `config.example.yaml` to `config.yaml` and adjust for your environment:

```yaml
logging: info
metricsAddr: :9090

clickhouse:
  url: http://localhost:8123
  # For cluster deployments:
  # cluster: my_cluster
  # local_suffix: _local

redis:
  url: redis://localhost:6379/0

coordinator:
  enabled: true

worker:
  enabled: true
  queues:
    - "*"
  concurrency: 10
  # Optional: Filter models by tags
  # model_tags:
  #   include: [batch, analytics]  # Process models with any of these tags
  #   exclude: [realtime]         # Skip models with any of these tags
  #   require: [production]       # Only process models with all of these tags
```

## Model Definition

Models define your data pipelines and should be stored in your own repository or directory.
Create a `models/` directory in your deployment with the following structure:

```
models/
├── external/         # External data source definitions
└── transformations/  # Data transformation pipelines
```

### External Models

External models define source data boundaries. Place in `models/external/`:

```sql
---
database: ethereum
table: beacon_blocks
partition: slot_start_date_time
external: true
ttl: 60s
lag: 30  # Optional: ignore last 30 seconds of data
---
SELECT 
    min(slot_start_date_time) as min,
    max(slot_start_date_time) as max
FROM ethereum.beacon_blocks
WHERE slot_start_date_time >= now() - INTERVAL 7 DAY
```

### Transformation Models

Transformation models process data in intervals. Place in `models/transformations/`:

```sql
---
database: analytics
table: block_propagation
partition: slot_start_date_time
interval: 3600
schedule: "@every 1m"
backfill:
  enabled: true
  schedule: "@every 5m"  # How often to scan for gaps
  minimum: 1704067200     # Optional: earliest position to backfill from
tags:
  - batch
  - aggregation
dependencies:
  - ethereum.beacon_blocks
---
INSERT INTO {{ .self.database }}.{{ .self.table }}
SELECT 
    slot_start_date_time,
    slot,
    epoch,
    block_root,
    -- additional fields
    now() as processed_at
FROM {{ index .dep "ethereum" "beacon_blocks" "database" }}.{{ index .dep "ethereum" "beacon_blocks" "table" }}
WHERE {{ index .dep "ethereum" "beacon_blocks" "partition" }} >= {{ .range.start }}
  AND {{ index .dep "ethereum" "beacon_blocks" "partition" }} < {{ .range.end }}
```

### Python/External Script Models

Models can execute external scripts instead of SQL. The script receives environment variables with ClickHouse credentials and task context:

```yaml
database: analytics
table: python_metrics
partition: hour_start
interval: 3600
schedule: "@every 5m"
backfill:
  enabled: true
  schedule: "@every 5m"
tags:
  - python
  - metrics
dependencies:
  - ethereum.beacon_blocks
exec: "python3 /app/scripts/process_metrics.py"
```

Environment variables provided to scripts:
- `CLICKHOUSE_URL`: Connection URL (e.g., `clickhouse://host:9000`)
- `RANGE_START`, `RANGE_END`: Unix timestamps for processing interval
- `TASK_START`: Task execution timestamp
- `SELF_DATABASE`, `SELF_TABLE`: Target table info
- `DEP_<MODEL>_DATABASE`, `DEP_<MODEL>_TABLE`: Dependency info

## Quick Start

### Try the Example

The example deployment demonstrates CBT's capabilities with sample models including SQL transformations, Python scripts, and tag-based filtering.

#### What's Included
- **External Models**: `beacon_blocks`, `validator_entity` (simulated data sources)
- **SQL Transformations**: 
  - `block_propagation` - Aggregates block propagation metrics
  - `block_entity` - Joins blocks with validator entities
  - `entity_network_effects` - Complex aggregation across multiple dependencies
- **Python Model**: `entity_changes` - Demonstrates external script execution with ClickHouse HTTP API
- **Backfill Configuration**: Models demonstrate the new backfill format with separate scheduling
- **Data Generator**: Continuously inserts sample blockchain data
- **Chaos Generator**: Simulates data gaps and out-of-order arrivals for resilience testing

#### Running the Example

```bash
# Start all services (ClickHouse, Redis, Coordinator, Worker)
# Data generation starts automatically
make example-up

# Check model processing status
make example-models-status

# View model dependency graph
make example-models-dag

# Generate Graphviz visualization
make example-models-dag-dot > dag.dot
dot -Tpng dag.dot -o dag.png  # Requires graphviz

# View real-time logs
make example-logs

# Stop and cleanup
make example-down
```

#### Verify It's Working

```bash
# Check if models are processing
docker exec cbt-clickhouse clickhouse-client -q "
  SELECT table, COUNT(*) as rows 
  FROM system.tables 
  WHERE database = 'analytics' 
  GROUP BY table"

# View coordinator logs
docker logs cbt-coordinator --tail 50

# View worker logs  
docker logs example-worker-1 --tail 50

# Check admin table for completed tasks
docker exec cbt-clickhouse clickhouse-client -q "
  SELECT database, table, COUNT(*) as runs 
  FROM admin.cbt 
  GROUP BY database, table"

# View task queue web UI
open http://localhost:8080  # Asynqmon dashboard
```

#### Testing Tag-Based Filtering

Modify `example/config.yaml` to test worker filtering:

```yaml
worker:
  enabled: true
  concurrency: 10
  model_tags:
    include: [python]  # Only process Python models
    # Or try: exclude: [aggregation]
    # Or try: require: [batch]
```

Then restart the worker:
```bash
docker-compose -f example/docker-compose.yml restart worker
```

### Set Up Your Own
```bash
# 1. Build CBT
make build

# 2. Copy and customize config
cp config.example.yaml config.yaml
# Edit config.yaml with your ClickHouse and Redis settings

# 3. Create your models directory
mkdir -p models/external models/transformations
# Add your model definitions

# 4. Run CBT
./bin/cbt coordinator --config config.yaml  # In one terminal
./bin/cbt worker --config config.yaml      # In another terminal
```

## Usage

### CLI Commands

```bash
# List all models with metadata
cbt models list

# Show model status and last run times
cbt models status

# Validate model configurations
cbt models validate

# Visualize dependency graph
cbt models dag
cbt models dag --dot  # Output in Graphviz format

# Rerun specific model for a time range
# Note: Rerun invalidates completion records for the model AND all dependent models
# The gaps will be detected and filled by backfill processes on their schedules
cbt rerun --model analytics.block_propagation \
          --start 1704067200 --end 1704070800

# Start coordinator service
cbt coordinator --config config.yaml

# Start worker service
cbt worker --config config.yaml
```

## Template Variables

Models support Go template syntax with the following variables:

- `{{ .clickhouse.cluster }}` - ClickHouse cluster name
- `{{ .clickhouse.local_suffix }}` - Local table suffix for cluster setups
- `{{ .self.database }}` - Current model's database
- `{{ .self.table }}` - Current model's table
- `{{ .self.partition }}` - Current model's partition column
- `{{ .range.start }}` - Processing interval start
- `{{ .range.end }}` - Processing interval end
- `{{ .task.start }}` - Task start timestamp
- `{{ index .dep "db" "table" "field" }}` - Access dependency configuration

## Admin Table

CBT tracks completed transformations in `admin.cbt` for idempotency and gap detection:

```sql
CREATE TABLE admin.cbt (
    updated_date_time DateTime(3),
    database LowCardinality(String),
    table LowCardinality(String), 
    position UInt64,
    interval UInt64
) ENGINE = ReplacingMergeTree(updated_date_time)
ORDER BY (database, table, position, interval)
```

Query this table to monitor progress, find gaps, or debug processing issues.

## Development

### Project Structure

```
cbt/
├── cmd/                    # CLI commands
├── pkg/
│   ├── core/              # Core types and interfaces
│   ├── config/            # Configuration management
│   ├── clickhouse/        # ClickHouse client
│   ├── models/            # Model discovery and parsing
│   ├── tasks/             # Task queue management
│   ├── coordinator/       # Coordination and scheduling
│   └── worker/            # Task execution
├── models/
│   ├── external/          # External model definitions
│   └── transformations/   # Transformation model definitions
└── config.yaml            # Configuration file
```

### Building from Source

```bash
git clone https://github.com/ethpandaops/cbt
cd cbt
go build -o cbt main.go
```

## Key Concepts

### Position-based Processing
CBT uses Unix timestamps as "positions" to track progress. Each task processes data from `position` to `position + interval`. This enables:
- Exact replay of any time range
- Parallel processing of different intervals
- Efficient gap detection and backfilling

### Model Configuration Fields
- `interval`: Processing window size in seconds
- `schedule`: How often to check for new data. Supports:
  - `@every` format: `@every 30s`, `@every 5m`, `@every 1h`
  - Named formats: `@hourly`, `@daily`, `@weekly`, `@monthly`
  - **Real cron expressions**: `"*/5 * * * *"` (every 5 minutes), `"0 */6 * * *"` (every 6 hours)
- `backfill`: Automatic gap filling configuration:
  - `enabled`: Enable backfill scanning (required)
  - `schedule`: How often to scan for gaps (required, same formats as model schedule)
  - `minimum`: Earliest position to backfill from (optional, Unix timestamp)
- `ttl`: Cache duration for external model bounds (reduces ClickHouse queries)
- `lag`: Seconds to subtract from max for external models (handles late-arriving data)
- `exec`: Command to execute instead of SQL

### TTL Caching for External Models

CBT supports TTL-based caching of external model boundaries to improve performance:

#### How It Works
1. When `ttl` is configured on an external model, CBT caches the min/max boundaries in Redis
2. Subsequent queries within the TTL period use cached values instead of querying ClickHouse
3. After TTL expires, the next query refreshes the cache
4. The `lag` configuration is applied to cached values dynamically

#### Benefits
- **Reduced ClickHouse Load**: Fewer queries for frequently-checked external models
- **Faster Validation**: Dependency checks complete faster using cached boundaries
- **Configurable Trade-off**: Balance between data freshness and performance

#### Example Configuration
```yaml
---
database: ethereum
table: beacon_blocks
partition: slot_start_date_time
external: true
ttl: 60s  # Cache boundaries for 60 seconds
lag: 30   # Ignore last 30 seconds of data
---
```

## How CBT Ensures Data Consistency

CBT uses comprehensive dependency validation to ensure data consistency across your pipelines. Before processing any interval, the system validates that all required data is available:

### Dependency Validation Rules

```
┌──────────────────────────────────────────────────────┐
│           VALIDATION DECISION TREE                   │
├──────────────────────────────────────────────────────┤
│                                                      │
│  For each dependency:                                │
│                                                      │
│  Is it External?                                     │
│    ├─ YES → Check min/max bounds                     │
│    │         ├─ Apply lag if configured              │
│    │         │   (max = max - lag seconds)           │
│    │         ├─ Outside bounds? → FAIL ❌            │
│    │         └─ Within bounds?                       │
│    │              └─ Check actual data exists        │
│    │                   ├─ No data? → FAIL ❌         │
│    │                   └─ Has data? → PASS ✅        │
│    │                                                 │
│    └─ NO (Transformation) →                          │
│            Check admin.cbt coverage                  │
│              ├─ Not covered? → FAIL ❌               │
│              └─ Fully covered? → PASS ✅             │
│                                                      │
│  All dependencies pass? → CAN PROCESS ✅             │
│  Any dependency fails? → CANNOT PROCESS ❌           │
└──────────────────────────────────────────────────────┘
```

### Key Validation Features

- **Pull-through validation**: Workers always verify dependencies at execution time, not just at scheduling
- **Two-phase external validation**: For external models, CBT checks both data bounds AND actual data existence to handle sparse datasets
- **Lag handling**: External models with `lag` configured have their max boundary adjusted during validation to ignore recent, potentially incomplete data
- **Coverage tracking**: The admin table tracks all completed intervals, enabling precise dependency validation
- **Automatic retry**: Failed validations are automatically retried on the next schedule cycle
- **Cascade triggering**: When a model completes, all dependent models are immediately checked for processing

This validation system ensures that:
1. No model processes data before its dependencies are ready
2. Sparse or missing data in external sources is properly detected
3. Processing can automatically resume when dependencies become available
4. Data consistency is maintained even in distributed environments

## License

MIT