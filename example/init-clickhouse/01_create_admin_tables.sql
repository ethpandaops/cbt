-- Create CBT admin database
CREATE DATABASE IF NOT EXISTS admin;

-- Create admin tracking table for completed transformations
CREATE TABLE IF NOT EXISTS admin.cbt (
    updated_date_time DateTime(3) CODEC(DoubleDelta, ZSTD(1)),
    database LowCardinality(String) COMMENT 'The database name',
    table LowCardinality(String) COMMENT 'The table name',
    position UInt64 COMMENT 'The starting position of the processed interval',
    interval UInt64 COMMENT 'The size of the interval processed',
    INDEX idx_model (database, table) TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(updated_date_time)
ORDER BY (database, table, position)
SETTINGS index_granularity = 8192;