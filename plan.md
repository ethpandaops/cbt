# ClickHouse Built Tool (cbt)

## Problem Statement

- We store huge amounts of raw data in ClickHouse
- Multiple parties are doing their own transformations for the data
- Expensive and slow

## Why Not Existing Solutions? (giga cope)

Off-the-shelf SQL transformation tools like [SQLMesh](https://sqlmesh.readthedocs.io/en/stable/), [DBT](https://www.getdbt.com/), and [Dataform](https://cloud.google.com/dataform?hl=en) have limitations:

- ORM-like hidden complexity, steep learning curve, and vendor lock-in
- ClickHouse is rarely a first-class integration, especially in cluster mode
- Abstraction layers for multiple integrations lead to limitations or inefficiencies
- Typically run in 5+ minute chunks minimum
- Python-based

## Solution Overview

A simplified DBT/SQLMesh-like service in Go that:
- ClickHouse only
- Doesn't manage database/tables - just executes transformations
- ClickHouse tracks/manages its own state
- Simple DAG with task system for (re)running models at any interval

### Core Concepts

- **Idempotent/Replayable** - Transformations produce same results when re-run. Uses ReplacingMergeTree to handle updates
- **DAG-based** - Models declare dependencies, forming a directed graph. System validates no cycles and processes in topological order
- **Interval-based** - Data processed in fixed-size chunks (position + interval). Enables parallel processing and efficient retries
- **Pull-through validation** - Workers verify dependencies before execution. External models cache min/max, transformations check admin table for data availability

## Architecture

### Task Management (Asynq)

Asynq is a Go library for distributed task queue backed by Redis. It handles task persistence, retries, and concurrent processing across multiple workers.

```
Coordinator                     Redis Queue                      Worker
    │                               │                               │
    ├─check schedule────────────────┤                               │
    ├─validate deps─────────────────┤                               │
    ├─enqueue task──────────────────►                               │
    │                               ├─────────pull task─────────────►
    │                               │                               ├─validate deps
    │                               │                               ├─execute transformation
    │                               │                               ├─update admin
    │◄──────task complete───────────┼───────────────────────────────┤
    ├─check dependents──────────────┤                               │
    └─enqueue related deps──────────►                               │
```

- A task is a **SINGLE** run of a model for a specific interval
- Tasks are **UNIQUE** per `model_id + position + interval` where:
  - `model_id` = `database.table` (e.g., `mainnet.eventstream_block_propagation`)
  - `position` = Starting position (timestamp, block number, etc.)
  - `interval` = Size of the chunk to process
- Task lifecycle:
  1. Validate dependencies:
     - External model dependencies: Check min/max from cached values
     - Transformation model dependencies: Query admin table for position/interval completion
  2. Execute transformation
  3. Update admin table on success

### Worker Architecture

**Model-Based Queues:**
Each model gets its own queue.

**Benefits:**
- **Isolated deployments** - Update one model's worker without affecting others
- **Resource allocation** - Dedicated workers for resource-intensive models
- **Failure isolation** - One broken model doesn't block others
- **Fine-grained scaling** - Scale specific models based on load
- **Operational flexibility** - Route models to specific hardware/regions

**Worker Configuration:**
```go
type WorkerConfig struct {
    QueuePatterns       []string  // e.g., ["mainnet.*", "testnet.specific_model"]
    ConcurrencyPerQueue int       // Workers per matched queue
    MaxConcurrency      int       // Total worker limit
}
```

### Coordinator/Scheduler

The coordinator uses a hybrid push-pull approach for efficient scheduling.

**Multiple Coordinators:**
For simplicity, the system starts without leader election:
- Multiple coordinators can run simultaneously without issues
- Asynq's unique task IDs (`model_id + position + interval`) prevent duplicate processing
- Redundant dependency checks are harmless, just slightly inefficient
- Can add Redis-based leader election later if needed

Leader election would only be required if:
- Coordinators maintained critical in-memory state
- Exactly-once scheduling semantics were needed
- Non-idempotent operations were performed

**Coordinator State:**
```go
import "github.com/heimdalr/dag"

type Coordinator struct {
    // Model dependency graph using heimdalr/dag
    depGraph      *dag.DAG              // Thread-safe DAG with cycle detection
    
    // Asynq inspector for task state
    inspector     *asynq.Inspector
    
    // Model metadata cache
    modelConfigs  map[string]ModelConfig // schedule, interval, dependencies
    
    // Last known positions (from admin table)
    lastPositions map[string]uint64      // model → last processed position
}
```

**Building Dependency Graph:**
```go
func (c *Coordinator) buildDependencyGraph(models []ModelConfig) error {
    c.depGraph = dag.NewDAG()
    
    // Add all models as vertices
    for _, model := range models {
        modelID := fmt.Sprintf("%s.%s", model.Database, model.Table)
        c.depGraph.AddVertexByID(modelID, model)
    }
    
    // Add edges (dependency → dependent)
    for _, model := range models {
        modelID := fmt.Sprintf("%s.%s", model.Database, model.Table)
        for _, dep := range model.Dependencies {
            // AddEdge returns error if it would create a cycle
            if err := c.depGraph.AddEdge(dep, modelID); err != nil {
                return fmt.Errorf("invalid dependency %s → %s: %w", dep, modelID, err)
            }
        }
    }
    
    return nil
}

// Get models that depend on the given model
func (c *Coordinator) getDependents(modelID string) []string {
    // GetChildren returns all direct dependents
    children, _ := c.depGraph.GetChildren(modelID)
    
    dependents := make([]string, 0, len(children))
    for id := range children {
        dependents = append(dependents, id)
    }
    return dependents
}
```

**Task State Inspection:**
```go
// Check if a task was recently completed using Asynq
func (c *Coordinator) wasRecentlyCompleted(modelID string, position, interval uint64) bool {
    taskID := fmt.Sprintf("%s:%d:%d", modelID, position, interval)
    
    info, err := c.inspector.GetTaskInfo("default", taskID)
    if err != nil {
        return false
    }
    
    return info.State == asynq.TaskStateCompleted
}
```

**Event-Driven Flow (Push):**
```
1. Task completes successfully in Asynq
2. Asynq SuccessHandler notifies coordinator: "model X completed position Y interval Z"
3. Coordinator immediately checks dependents of X
4. Enqueues any dependents that are now ready
```

**Task → Coordinator Notification:**
```go
// Asynq server configuration
srv := asynq.NewServer(
    redisOpt,
    asynq.Config{
        // Hook into task completion
        SuccessHandler: func(ctx context.Context, task *asynq.Task) {
            var payload TaskPayload
            json.Unmarshal(task.Payload(), &payload)
            
            // Notify coordinator of completion
            coordinator.onTaskComplete(
                payload.ModelID,
                payload.Position,
                payload.Interval,
            )
        },
        
        // Optional: Track failures
        FailureHandler: func(ctx context.Context, task *asynq.Task, err error) {
            coordinator.recordFailure(task, err)
        },
    },
)
```

**Scheduled Flow (Pull):**
```
Based on each model's schedule (e.g., @every 1m):
1. Check if new data can be processed (forward)
2. If backfill=true, also check for gaps to fill (backward)
3. Validate dependencies for both
4. Enqueue ready tasks (both forward and backfill)
```

**Backfill Behavior:**
When `backfill: true` is set on a model:
- Coordinator checks for the earliest gap in processed data
- Enqueues both forward tasks (latest data) and backfill tasks (historical gaps)
- Both run concurrently, competing for worker resources
- Prevents getting "stuck" on historical data while new data arrives

Example with model at position 1000, interval 60:
- Forward: Check if position 1060 is ready → enqueue
- Backfill: Find earliest gap (e.g., position 400) → enqueue
- Both tasks enter the same queue and process based on worker availability

**Initial Position Discovery:**
When a model has no entries in the admin table:
- Query all dependencies for their maximum processed position
- Take the minimum of these maximums
- Subtract the model's interval to find the starting position
- Example: If deps are at positions [1000, 1200, 800], start at 800 - interval

**Smart Dependency Checking:**
```go
func (c *Coordinator) onTaskComplete(modelID string, position, interval uint64) {
    // Get models that depend on this one using DAG
    dependents := c.getDependents(modelID)
    
    // Check each dependent
    for _, dep := range dependents {
        depConfig := c.modelConfigs[dep]
        nextPos := c.lastPositions[dep] + depConfig.Interval
        
        // Skip if already enqueued/processing
        taskID := fmt.Sprintf("%s:%d:%d", dep, nextPos, depConfig.Interval)
        if c.isTaskPendingOrRunning(taskID) {
            continue
        }
        
        // Check if this completion unblocks the dependent
        if c.canProcess(dep, nextPos, depConfig.Interval) {
            c.enqueueTask(dep, nextPos, depConfig.Interval)
        }
    }
}
```

**Asynq Configuration:**
```go
srv := asynq.NewServer(
    redisOpt,
    asynq.Config{
        // Retain completed task info for inspection
        CompletedTaskRetention: 1 * time.Hour,
        
        // Hook into task lifecycle
        SuccessHandler: coordinator.onTaskComplete,
        
        // Error handling - tasks retry then get archived
        MaxRetry: 3,
        IsFailure: func(err error) bool {
            // Determine if error is retryable
            return true
        },
    },
)
```

**Error Handling:**
- Failed tasks are retried based on Asynq configuration
- Exhausted tasks (after max retries) are archived
- Coordinator's next scheduled sweep will re-enqueue the same position+interval
- Archived/exhausted tasks should be pruned periodically to prevent queue bloat

**Benefits:**
- **Low latency**: Dependents run immediately when data is ready
- **Efficient**: Only check models that might be unblocked
- **Resilient**: Scheduled sweeps catch anything missed by events
- **Thread-safe**: heimdalr/dag handles concurrent access
- **Cycle detection**: DAG library prevents circular dependencies

**Additional DAG Features:**
```go
// Get all upstream dependencies (for validation)
ancestors, _ := c.depGraph.GetAncestors(modelID)

// Get topological order (for batch processing)
order, _ := c.depGraph.GetOrderedDescendants(modelID)

// Check if path exists
hasPath := c.depGraph.IsPathBetween(sourceID, targetID)
```

### Dependency Validation

The system validates whether dependencies have the required data for a given position+interval:

**External Model Dependencies:**
- Use pull-through cache for min/max values:
  - Check cache first
  - If cache miss or expired (based on TTL), execute the external model's SQL/exec
  - Cache the result for TTL duration
- Validate that `position` >= `min` and `position + interval` <= `max`
- Example: If processing timestamp 1735776000-1735776060, verify external table has data up to at least 1735776060

**Transformation Model Dependencies:**
- Query the admin table for range coverage, not exact position+interval
- Check if the required range [position, position+interval) is fully covered by dependency
- Handles interval changes gracefully (e.g., if dependency changed from 60s to 120s intervals)
- Example: If processing [1735776000, 1735776060), verify dependency has coverage for entire range

**Range Coverage Check:**
```sql
-- Need data for range [240, 360)
WITH coverage AS (
  SELECT position, position + interval as end_pos
  FROM admin.cbt
  WHERE database = 'mainnet' AND table = 'source_table'
    AND position < 360  -- end of needed range
    AND position + interval > 240  -- start of needed range
)
-- Verify no gaps in coverage
SELECT CASE 
  WHEN MIN(position) <= 240 AND MAX(end_pos) >= 360 
  THEN 1 ELSE 0 
END as fully_covered
FROM coverage
```

This approach allows models to change their interval size without breaking dependencies - as long as the required range is covered, it doesn't matter if it's covered by one large chunk or multiple smaller ones.

**Validation Example:**
```
Model: mainnet.derived_table
Position: 1735776000
Interval: 60

Checking dependencies:
  - mainnet.external_source (external model)
    → Check: cached max >= 1735776060 ✓
  - mainnet.intermediate_table (transformation model)  
    → Check: admin table has (mainnet.intermediate_table, 1735776000, 60) ✓
Result: All dependencies satisfied
```

### State Management

ClickHouse admin table tracks completed intervals per model, enabling recovery and preventing duplicate processing.

#### Admin Table Schema

**Single Node:**
```sql
CREATE TABLE admin.cbt (
    `updated_date_time` DateTime(3) CODEC(DoubleDelta, ZSTD(1)),
    `database` LowCardinality(String) COMMENT 'The database name',
    `table` LowCardinality(String) COMMENT 'The table name',
    `position` UInt64 COMMENT 'The starting position' CODEC(DoubleDelta, ZSTD(1)),
    `interval` UInt64 COMMENT 'The interval size' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplacingMergeTree(updated_date_time)
ORDER BY (database, table, position, interval);
```

**Cluster Setup:**
```sql
-- Local table on each node
CREATE TABLE admin.cbt_local ON CLUSTER '{cluster}' (
    `updated_date_time` DateTime(3) CODEC(DoubleDelta, ZSTD(1)),
    `database` LowCardinality(String) COMMENT 'The database name',
    `table` LowCardinality(String) COMMENT 'The table name',
    `position` UInt64 COMMENT 'The starting position' CODEC(DoubleDelta, ZSTD(1)),
    `interval` UInt64 COMMENT 'The interval size' CODEC(DoubleDelta, ZSTD(1))
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/{installation}/{cluster}/{database}/tables/{table}/{shard}',
    '{replica}',
    updated_date_time
)
ORDER BY (database, table, position, interval);

-- Distributed table for querying
CREATE TABLE admin.cbt ON CLUSTER '{cluster}' AS admin.cbt_local 
ENGINE = Distributed(
    '{cluster}',
    admin,
    cbt_local,
    cityHash64(database, table, position, interval)
);
```

The ReplacingMergeTree with `updated_date_time` ensures that reruns automatically update the state, keeping only the latest successful run for each model + position/interval combination.

## Configuration

### Global Settings

```yaml
logging: "debug" # panic,fatal,warn,info,debug,trace
metricsAddr: ":9090"

clickhouse:
  url: http://localhost:8123/
  cluster: "{cluster}" # clickhouse cluster name, if omitted will not run in cluster mode
  local_suffix: _local # default to _local if omitted

redis:
  url: "redis://localhost:6379"

coordinator:
  enabled: true

worker:
  enabled: true
  queues: ["*"]  # Queue patterns to process
  concurrency: 10
  # Or more specific:
  # queues:
  #   - pattern: "mainnet.*"
  #     concurrency: 5
  #   - pattern: "*.daily_*"
  #     concurrency: 2
```

## Models

Models are files with YAML frontmatter that define data transformations. Models are organized by type in the filesystem:

```
models/
├── external/          # External models (read-only reference tables)
│   └── ...           # Can have any nested directory structure
└── transformations/   # Transformation models (create derived data)
    └── ...           # Can have any nested directory structure
```

The directory structure determines the model type:
- `models/external/**/*.{sql,yaml,yml}` - External models that reference existing tables
- `models/transformations/**/*.{sql,yaml,yml}` - Transformation models that create derived data

You can organize files however you want within these directories - flat, nested, or any structure that makes sense for your project. The actual model reference is determined by the `database` and `table` settings in the file's frontmatter, not the file path.

### Model Configuration

#### Shared Settings

All models support these configuration options:

| Setting | Type | Description | Required |
|---------|------|-------------|----------|
| `database` | string | Database name | Yes |
| `table` | string | Table name | Yes |
| `partition` | string | Column used for time-based partitioning | Yes |

#### External Model Settings

Additional settings for external models (`external: true`):

| Setting | Type | Description | Required |
|---------|------|-------------|----------|
| `ttl` | duration | How long to cache min/max values | No (default: 60s) |

#### Transformation Model Settings

Additional settings for transformation models (`external: false` or omitted):

| Setting | Type | Description | Required |
|---------|------|-------------|----------|
| `interval` | integer | Size of data chunks to process (seconds) | Yes |
| `schedule` | string/integer | How often to check for new data | Yes |
| `backfill` | boolean | Automatically backfill historical data | No (default: false) |
| `dependencies` | array | List of dependent tables (format: `database.table`) | No |

**Schedule formats:**
- Cron: `"*/5 * * * *"` (every 5 minutes)
- Descriptive: `"@every 1h"`, `"@hourly"`
- Seconds: `300` (every 300 seconds)

### Template Variables

Available in SQL templates via Sprig:

| Variable | Description | Example Usage |
|----------|-------------|---------------|
| **clickhouse** | ClickHouse configuration values | |
| `clickhouse.cluster` | ClickHouse cluster name | `{{ if .clickhouse.cluster }}ON CLUSTER '{{ .clickhouse.cluster }}'{{ end }}` |
| `clickhouse.local_suffix` | Local table suffix | `{{ .self.table }}{{ .clickhouse.local_suffix }}` |
| **self** | Current model settings | |
| `self.database` | Model's database | `{{ .self.database }}.{{ .self.table }}` |
| `self.table` | Model's table name | `INSERT INTO {{ .self.database }}.{{ .self.table }}` |
| `self.partition` | Partition column | `WHERE {{ .self.partition }} BETWEEN ...` |
| `self.interval` | Processing interval | Used internally for scheduling |
| **task** | Running task information | |
| `task.start` | Task start timestamp (unix) | `{{ .task.start }} as updated_date_time` |
| **range** | Processing time range | |
| `range.start` | Range start (unix timestamp) | `WHERE timestamp >= {{ .range.start }}` |
| `range.end` | Range end (unix timestamp) | `WHERE timestamp < {{ .range.end }}` |
| **dep** | References to dependency models | |
| `dep.<db>.<table>.database` | Dependency's database | `FROM {{ .dep.mainnet.raw_table.database }}.{{ .dep.mainnet.raw_table.table }}` |
| `dep.<db>.<table>.table` | Dependency's table | See above |
| `dep.<db>.<table>.partition` | Dependency's partition column | `WHERE {{ .dep.mainnet.raw_table.partition }} BETWEEN ...` |

### External Model Example (SQL)

External models reference existing tables that are managed outside this tool.

| Setting | Value | Description |
|---------|-------|-------------|
| `database` | mainnet | Database containing the external table |
| `table` | raw_beacon_api_eth_v1_events_block | External table name |
| `partition` | slot_start_date_time | Column used for time-based partitioning |
| `external` | true | Marks this as a read-only external model |
| `ttl` | 30s | How long to cache min/max values |

```sql
---
database: mainnet
table: raw_beacon_api_eth_v1_events_block
partition: slot_start_date_time
external: true
ttl: 30s
---
SELECT
  toUnixTimestamp(min({{ .self.partition }})) as min,
  toUnixTimestamp(max({{ .self.partition }})) as max
FROM
  `{{ .self.database }}`.`{{ .self.table }}`
FINAL
```

### External Model Example (YAML)

For external models that require custom logic to determine min/max values:

```yaml
database: mainnet
table: complex_external_source
partition: timestamp
ttl: 60s
exec: /usr/local/bin/get_table_bounds.sh
```

The executable must output JSON with `min` and `max` fields:
```json
{
  "min": 1735689600,
  "max": 1735776000
}
```

### Transformation Model Example (SQL)

Transformation models create derived data from other models or external tables.

| Setting | Value | Description |
|---------|-------|-------------|
| `database` | mainnet | Target database for transformed data |
| `table` | eventstream_block_propagation | Target table name |
| `partition` | slot_start_date_time | Column used for time-based processing |
| `dependencies` | mainnet.raw_beacon_api_eth_v1_events_block | Tables this model depends on |
| `interval` | 60 | Process data in 60-second chunks |
| `schedule` | "@every 30s" | Check for new data every 30 seconds |
| `backfill` | true | Automatically backfill historical data |
| `external` | false (default) | This is a transformation model |

```sql
---
database: mainnet
table: eventstream_block_propagation
partition: slot_start_date_time
dependencies:
  - mainnet.raw_beacon_api_eth_v1_events_block
interval: 60
schedule: "@every 30s"
backfill: true
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
  {{ .task.start }} as updated_date_time,
  slot,
  slot_start_date_time,
  epoch,
  epoch_start_date_time,
  block,
  avg(propagation_slot_start_diff) as avg_propagation
FROM
  `{{ .dep.mainnet.raw_beacon_api_eth_v1_events_block.database }}`.`{{ .dep.mainnet.raw_beacon_api_eth_v1_events_block.table }}` FINAL
WHERE
  {{ .dep.mainnet.raw_beacon_api_eth_v1_events_block.partition }} BETWEEN {{ .range.start }} AND {{ .range.end }};

-- Clean up old data from previous runs
DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE
  {{ .self.partition }} BETWEEN {{ .range.start }} AND {{ .range.end }}
  AND updated_date_time != {{ .task.start }};
```

### Transformation Model Example (YAML)

For transformations that require custom logic beyond SQL, use a YAML file instead of SQL:

```yaml
database: mainnet
table: complex_aggregation
partition: timestamp
dependencies:
  - mainnet.source_table
interval: 300
schedule: "@every 5m"
backfill: true
exec: /usr/local/bin/process_aggregation.sh
```

The executable receives all template variables as environment variables:
- `CLICKHOUSE_URL` - ClickHouse HTTP URL (e.g., http://localhost:8123)
- `CLICKHOUSE_CLUSTER` - Cluster name (if configured)
- `CLICKHOUSE_LOCAL_SUFFIX` - Local table suffix for cluster mode
- `SELF_DATABASE`, `SELF_TABLE`, `SELF_PARTITION`, etc.
- `TASK_START`, `TASK_MODEL`, `TASK_INTERVAL`
- `RANGE_START`, `RANGE_END`
- `DEP_MAINNET_SOURCE_TABLE_DATABASE`, `DEP_MAINNET_SOURCE_TABLE_TABLE`, etc.

**Example Script (`/usr/local/bin/process_aggregation.sh`):**
```bash
#!/bin/bash
set -euo pipefail

# Error handler
error_exit() {
    echo "ERROR: $1" >&2
    exit "${2:-1}"
}

# Log execution details
echo "Processing aggregation for range: $RANGE_START to $RANGE_END"

# Fetch data from source
echo "Fetching data from dependency..."
if ! curl -f -s "$CLICKHOUSE_URL" \
  --data-urlencode "query=
    SELECT * FROM $DEP_MAINNET_SOURCE_TABLE_DATABASE.$DEP_MAINNET_SOURCE_TABLE_TABLE 
    WHERE $DEP_MAINNET_SOURCE_TABLE_PARTITION >= $RANGE_START 
    AND $DEP_MAINNET_SOURCE_TABLE_PARTITION < $RANGE_END
    FORMAT JSONEachRow" \
  > /tmp/source_data.json; then
    error_exit "Failed to fetch data from ClickHouse" 10
fi

# Check if we got any data
if [ ! -s /tmp/source_data.json ]; then
    error_exit "No data found for range $RANGE_START to $RANGE_END" 11
fi

# Process data (example: complex aggregation in Python)
echo "Processing $(wc -l < /tmp/source_data.json) rows..."
if ! python3 /usr/local/bin/aggregate.py \
  --input /tmp/source_data.json \
  --output /tmp/aggregated_data.json \
  --start $RANGE_START \
  --end $RANGE_END; then
    error_exit "Python aggregation script failed" 20
fi

# Check output was generated
if [ ! -s /tmp/aggregated_data.json ]; then
    error_exit "Aggregation produced no output" 21
fi

# Insert results
echo "Inserting $(wc -l < /tmp/aggregated_data.json) rows to ClickHouse..."
if ! response=$(curl -f -s -w "\n%{http_code}" "$CLICKHOUSE_URL" \
  --data-binary "@/tmp/aggregated_data.json" \
  --data-urlencode "query=INSERT INTO $SELF_DATABASE.$SELF_TABLE FORMAT JSONEachRow"); then
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | head -n-1)
    error_exit "Failed to insert data: HTTP $http_code - $body" 30
fi

# Clean up
rm -f /tmp/source_data.json /tmp/aggregated_data.json

echo "Successfully processed aggregation"
```

This allows complex transformations using any language/tool while maintaining the same scheduling, dependency management, and state tracking as SQL models.

## Rerun Strategy

Manual reruns allow reprocessing historical data ranges, useful for fixes or backfills.

### Rerun Request Flow

When requesting a rerun for a specific time range and model:

**Implementation via Admin Table Deletion:**

1. **Delete admin entries for the requested range:**
```sql
-- Single node
DELETE FROM admin.cbt
WHERE database = 'mainnet' 
  AND table = 'derived_table'
  AND position >= 1735430400  -- 2024-12-29T00:00:00
  AND position < 1735516800  -- 2024-12-30T00:00:00
  AND position + interval > 1735430400  -- overlapping ranges

-- Cluster (delete from local table)
DELETE FROM admin.cbt_local ON CLUSTER '{cluster}'
WHERE database = 'mainnet' 
  AND table = 'derived_table'
  AND position >= 1735430400
  AND position < 1735516800
  AND position + interval > 1735430400
```

2. **Cascade deletions to dependent models:**
```go
// Use DAG to find all dependents
dependents := c.depGraph.GetDescendants("mainnet.derived_table")
for _, dep := range dependents {
    // Delete same range for each dependent
    deleteAdminEntries(dep, fromPosition, toPosition)
}
```

3. **Let natural backfill process the gaps:**
- Coordinator's scheduled checks will detect the gaps
- Backfill logic will enqueue tasks to fill them
- Dependencies ensure correct processing order
- Current interval settings are used (handles interval changes gracefully)

**Benefits of this approach:**
- Simple implementation - just delete and let backfill work
- Handles interval changes automatically 
- No special task states or flags needed
- Natural dependency ordering via existing validation

**Considerations:**
- Range-based deletion ensures clean boundaries
- Interval changes may cause reprocessing at edges
- Forward processing continues normally alongside backfill
