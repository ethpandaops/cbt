-- Create CBT admin database
CREATE DATABASE IF NOT EXISTS admin;

-- Create admin tracking table for incremental transformations
CREATE TABLE IF NOT EXISTS admin.cbt_incremental (
    updated_date_time DateTime(3) CODEC(DoubleDelta, ZSTD(1)),
    database LowCardinality(String) COMMENT 'The database name',
    table LowCardinality(String) COMMENT 'The table name',
    position UInt64 COMMENT 'The starting position of the processed interval',
    interval UInt64 COMMENT 'The size of the interval processed',
    INDEX idx_model (database, table) TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(updated_date_time)
ORDER BY (database, table, position);

-- Create admin tracking table for scheduled transformations
CREATE TABLE IF NOT EXISTS admin.cbt_scheduled (
    updated_date_time DateTime(3) CODEC(DoubleDelta, ZSTD(1)),
    database LowCardinality(String) COMMENT 'The database name',
    table LowCardinality(String) COMMENT 'The table name',
    start_date_time DateTime(3) COMMENT 'The start time of the scheduled job',
    INDEX idx_model (database, table) TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(updated_date_time)
ORDER BY (database, table);
