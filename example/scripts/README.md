# Python Transformation Models

This directory contains Python scripts that can be executed as transformation models using the `exec` field in model YAML files.

## How It Works

When a model uses `exec` instead of SQL content, CBT worker:
1. Sets up environment variables with ClickHouse connection and task context
2. Executes the specified command
3. Expects the script to handle all data processing

## Environment Variables

The following environment variables are provided to exec scripts:

### ClickHouse Connection
- `CLICKHOUSE_URL` - Full ClickHouse connection URL (e.g., `clickhouse://localhost:9000/default`)
- `CLICKHOUSE_CLUSTER` - Cluster name (if configured)
- `CLICKHOUSE_LOCAL_SUFFIX` - Local table suffix for clusters (if configured)

### Model Information
- `SELF_DATABASE` - Target database name
- `SELF_TABLE` - Target table name
- `SELF_PARTITION` - Partition column name

### Task Context
- `TASK_START` - Unix timestamp when task started
- `TASK_MODEL` - Full model ID (database.table)
- `TASK_INTERVAL` - Interval size being processed
- `RANGE_START` - Start position (Unix timestamp) of current interval
- `RANGE_END` - End position (Unix timestamp) of current interval

### Dependencies
For each dependency, environment variables are created:
- `DEP_<DB>_<TABLE>_DATABASE` - Dependency's database
- `DEP_<DB>_<TABLE>_TABLE` - Dependency's table
- `DEP_<DB>_<TABLE>_PARTITION` - Dependency's partition column

## Example: entity_changes.py

The `entity_changes.py` script demonstrates:
1. Reading ClickHouse credentials from environment
2. Querying dependency data using HTTP interface
3. Processing data in Python (calculating statistics)
4. Writing results back to ClickHouse

### Model Definition

```yaml
---
database: analytics
table: entity_changes
partition: hour_start
interval: 3600  # 1 hour
schedule: "@every 1m"
tags:
  - python
  - entity
dependencies:
  - ethereum.validator_entity
exec: "python3 /app/scripts/entity_changes.py"
---
```

## Benefits of Python Transformation Models

1. **Complex Logic**: Implement algorithms that are difficult in SQL
2. **External APIs**: Call external services for enrichment
3. **Machine Learning**: Apply ML models to data
4. **Custom Libraries**: Use Python's rich ecosystem
5. **Debugging**: Easier to debug complex transformations

## Best Practices

1. **Error Handling**: Always exit with non-zero code on failure
2. **Logging**: Print progress to stdout, errors to stderr
3. **Idempotency**: Ensure transformations can be safely re-run
4. **Performance**: Consider batch processing for large datasets
5. **Dependencies**: Keep scripts simple, use standard library when possible