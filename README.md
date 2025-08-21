<img align="left" height="120px" src="logo.png">
  <h1> CBT - ClickHouse Build Tool </h1>
</img>

A simple ClickHouse-focused data transformation tool that provides fast idempotent transformations with pure SQL or external scripts.

## Architecture

```
         ┌───────────────┐
         │      CBT      │
         └───────┬───────┘
                 │
        ┌────────┴────────┐
        │                 │
        ▼                 ▼
┌──────────────┐  ┌──────────────┐
│    Redis     │  │  ClickHouse  │
│              │  │              │
│ • Task Queue │  │ • Data       │
│ • Scheduling │  │ • Admin      │
└──────────────┘  └──────────────┘
```

**Multi-instance behavior:**
CBT runs as a unified binary that handles both coordination/scheduling and task execution. You can run multiple instances for high availability and increased throughput:
- All instances process transformation tasks from the queue unless filtered by tags in the `worker.tags` configuration.
- [Asynq](https://github.com/hibiken/asynq) prevents duplicate transformation tasks from being scheduled.


### Requirements

- ClickHouse
- Redis

## Configuration

CBT uses a single configuration file (`config.yaml`) for all settings.

### Default Configuration

Copy `config.example.yaml` to `config.yaml` and adjust for your environment:

```yaml
# CBT Configuration

# Logging level: panic, fatal, warn, info, debug, trace
logging: info

# Metrics server address
metricsAddr: ":9090"

# Health check server address (optional)
healthCheckAddr: ":8080"

# Pprof server address for profiling (optional)
# Uncomment to enable profiling
# pprofAddr: ":6060"

# ClickHouse configuration
clickhouse:
  # Connection URL (required)
  url: "clickhouse://localhost:9000"
  
  # Cluster configuration (optional, for distributed deployments)
  # cluster: "default"
  # localSuffix: "_local"
  
  # Admin table configuration (optional)
  # Defaults to admin.cbt if not specified
  # admin_database: admin
  # admin_table: cbt
  
  # Query timeout
  queryTimeout: 30s
  
  # Insert timeout
  insertTimeout: 60s
  
  # Enable debug logging for queries
  debug: false
  
  # Keep-alive interval
  keepAlive: 30s

# Redis configuration
redis:
  # Redis connection URL (required)
  url: "redis://localhost:6379/0"

# Scheduling settings
scheduler:
  # How often to check for tasks to schedule
  interval: 1m
  
  # Maximum number of concurrent scheduling operations
  concurrency: 10

# Worker settings
worker:
  # Number of concurrent tasks to process
  concurrency: 10
  
  # Model tags for filtering which models this instance processes (optional)
  # Useful for running specialized instances for specific model types
  # tags:
  #   - "batch"
  #   - "analytics"

  # Seconds to wait for graceful shutdown
  shutdownTimeout: 30

# Model paths configuration (optional)
# Configure where to find external and transformation models
# Defaults to models/external and models/transformations if not specified
# models:
#   external:
#     paths:
#       - "models/external"
#       - "/additional/external/models"
#   transformations:
#     paths:
#       - "models/transformations"
#       - "/additional/transformation/models"
```

## Models

Models define your data pipelines and should be stored in your own repository or directory.

### Model Paths

By default, CBT looks for models in `models/external` and `models/transformations`. You can configure multiple paths for each model type in your `config.yaml`:

```yaml
models:
  external:
    paths:
      - "models/external"           # Default path
      - "/shared/models/external"   # Additional shared models
      - "/team/models/external"     # Team-specific models
  transformations:
    paths:
      - "models/transformations"    # Default path
      - "/shared/transformations"   # Shared transformations
```

### External Models

External models define source data boundaries.

#### Template Variables

Models support Go template syntax with the following variables:

- `{{ .clickhouse.cluster }}` - ClickHouse cluster name
- `{{ .clickhouse.local_suffix }}` - Local table suffix for cluster setups
- `{{ .self.database }}` - Current model's database
- `{{ .self.table }}` - Current model's table

#### Example

```sql
---
database: ethereum
table: beacon_blocks
ttl: 60s # Optional: how long to cache the min/max bounds for to reduce queries to the source data
lag: 30  # Optional: ignore last 30 seconds of data to avoid incomplete data
---
SELECT 
    toUnixTimestamp(min(slot_start_date_time)) as min,
    toUnixTimestamp(max(slot_start_date_time)) as max
FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
```

### Transformation Models

Transformation models process data in intervals. Intervals are agnostic to the source data and could be a time interval, a block number etc.

> Note: CBT does not create transformation tables and **requires** you to create them manually by design.

#### Template Variables

Models support Go template syntax with the following variables:

- `{{ .clickhouse.cluster }}` - ClickHouse cluster name
- `{{ .clickhouse.local_suffix }}` - Local table suffix for cluster setups
- `{{ .self.database }}` - Current model's database
- `{{ .self.table }}` - Current model's table
- `{{ .bounds.start }}` - Processing interval start
- `{{ .bounds.end }}` - Processing interval end
- `{{ .task.start }}` - Task start timestamp
- `{{ index .dep "db" "table" "field" }}` - Access dependency configuration

#### Example

```sql
---
database: analytics
table: block_propagation
interval: 3600
schedule: "@every 1m" # How often to trigger the transformation
backfill:
  enabled: true
  schedule: "@every 5m"  # How often to try to backfill
  minimum: 1704067200    # Optional: earliest position to backfill from (default: 0)
tags:
  - batch
  - aggregation
dependencies:
  - ethereum.beacon_blocks
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT 
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    now64(3) as event_date_time,
    slot_start_date_time,
    slot,
    block_root,
    count(DISTINCT meta_client_name) as client_count,
    avg(propagation_slot_start_diff) as avg_propagation,
    {{ .bounds.start }} as position
FROM `{{ index .dep "ethereum" "beacon_blocks" "database" }}`.`{{ index .dep "ethereum" "beacon_blocks" "table" }}`
WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
GROUP BY slot_start_date_time, slot, block_root;

-- Lazy delete deuplicate old rows (optional) to allow intervals to be re-processed
DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE
  slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
  AND updated_date_time != fromUnixTimestamp({{ .task.start }});
```

### External Script Models

Models can execute external scripts instead of SQL. The script receives environment variables with ClickHouse credentials and task context.

> Note: CBT does not create transformation tables and **requires** you to create them manually by design.

#### Environment Variables

Environment variables provided to scripts:
- `CLICKHOUSE_URL`: Connection URL (e.g., `clickhouse://host:9000`)
- `RANGE_START`, `RANGE_END`: Unix timestamps for processing interval
- `TASK_START`: Task execution timestamp
- `SELF_DATABASE`, `SELF_TABLE`: Target table info
- `DEP_<MODEL>_DATABASE`, `DEP_<MODEL>_TABLE`: Dependency info

#### Example

```yaml
database: analytics
table: python_metrics
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

See the [example script](./example/scripts/entity_changes.py) for a the python script.

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
cd example

docker-compose up -d
```

#### Verify It's Working

```bash
# Check if models are processing
docker exec cbt-clickhouse clickhouse-client -q "
  SELECT table, COUNT(*) as rows 
  FROM system.tables 
  WHERE database = 'analytics' 
  GROUP BY table"

# View logs
docker-compose logs -f

# Check admin table for completed tasks
docker exec cbt-clickhouse clickhouse-client -q "
  SELECT database, table, COUNT(*) as runs 
  FROM admin.cbt 
  GROUP BY database, table"

# View task queue web UI
open http://localhost:8080  # Asynqmon dashboard
```

## Usage

### Running CBT

```bash
# Run CBT with default config.yaml
cbt

# Run with custom config
cbt --config production.yaml
```


### Admin Table Setup

CBT tracks completed transformations in an admin table for idempotency and gap detection. This table must be created before running CBT.

### Configuration

The admin table location is configurable in your `config.yaml`:

```yaml
clickhouse:
  url: http://localhost:8123
  # Optional: Custom admin table (defaults shown)
  admin_database: admin  # Default: "admin"
  admin_table: cbt       # Default: "cbt"
```

This allows running multiple CBT instances on the same cluster (e.g., `dev_admin.cbt`, `prod_admin.cbt`).

### Single-Node Setup

For single-node ClickHouse deployments:

```sql
-- Create admin database
CREATE DATABASE IF NOT EXISTS admin;

-- Create admin tracking table
CREATE TABLE IF NOT EXISTS admin.cbt (
    updated_date_time DateTime(3) CODEC(DoubleDelta, ZSTD(1)),
    database LowCardinality(String) COMMENT 'The database name',
    table LowCardinality(String) COMMENT 'The table name', 
    position UInt64 COMMENT 'The starting position of the processed interval',
    interval UInt64 COMMENT 'The size of the interval processed',
    INDEX idx_model (database, table) TYPE minmax GRANULARITY 1
) ENGINE = ReplacingMergeTree(updated_date_time)
ORDER BY (database, table, position);
```

### Clustered Setup

For ClickHouse clusters with replication:

```sql
-- Create admin database on all nodes
CREATE DATABASE IF NOT EXISTS admin ON CLUSTER '{cluster}';

-- Create local table on each node
CREATE TABLE IF NOT EXISTS admin.cbt_local ON CLUSTER '{cluster}' (
    updated_date_time DateTime(3) CODEC(DoubleDelta, ZSTD(1)),
    database LowCardinality(String) COMMENT 'The database name',
    table LowCardinality(String) COMMENT 'The table name',
    position UInt64 COMMENT 'The starting position of the processed interval',
    interval UInt64 COMMENT 'The size of the interval processed',
    INDEX idx_model (database, table) TYPE minmax GRANULARITY 1
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/{database}/tables/{table}/{shard}',
    '{replica}',
    updated_date_time
)
ORDER BY (database, table, position);

-- Create distributed table for querying
CREATE TABLE IF NOT EXISTS admin.cbt ON CLUSTER '{cluster}' AS admin.cbt_local
ENGINE = Distributed(
    '{cluster}',
    'admin',
    'cbt_local',
    cityHash64(database, table)
);
```

### Using Custom Admin Tables

If you need to use a different database or table name:

1. Update your `config.yaml`:
```yaml
clickhouse:
  admin_database: custom_admin
  admin_table: custom_tracking
```

2. Create the tables using your custom names:
```sql
CREATE DATABASE IF NOT EXISTS custom_admin;
CREATE TABLE IF NOT EXISTS custom_admin.custom_tracking (
    -- Same schema as above
);
```

### Monitoring Admin Table

Query the admin table to monitor progress, find gaps, or debug processing issues:

```sql
-- View model processing status
SELECT 
    database,
    table,
    count(*) as intervals_processed,
    min(position) as earliest_position,
    max(position + interval) as latest_position
FROM admin.cbt FINAL
GROUP BY database, table;

-- Find gaps in processing
WITH intervals AS (
    SELECT 
        database,
        table,
        position,
        position + interval as end_pos,
        lead(position) OVER (PARTITION BY database, table ORDER BY position) as next_position
    FROM admin.cbt FINAL
)
SELECT 
    database,
    table,
    end_pos as gap_start,
    next_position as gap_end
FROM intervals
WHERE next_position > end_pos;
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
│    │         └─ Within bounds? → PASS ✅             │
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
- **Lag handling**: External models with `lag` configured have their max boundary adjusted during validation to ignore recent, potentially incomplete data
- **Coverage tracking**: The admin table tracks all completed intervals, enabling precise dependency validation
- **Automatic retry**: Failed validations are automatically retried on the next schedule cycle
- **Cascade triggering**: When a model completes, all dependent models are immediately (within 5 seconds) checked for processing

This validation system ensures that:
1. No model processes data before its dependencies are ready
1. Processing can automatically resume when dependencies become available
1. Data consistency is maintained even in distributed environments

## License

MIT