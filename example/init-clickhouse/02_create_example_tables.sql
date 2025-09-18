-- Create example databases
CREATE DATABASE IF NOT EXISTS ethereum;
CREATE DATABASE IF NOT EXISTS analytics;
CREATE DATABASE IF NOT EXISTS reference;
CREATE DATABASE IF NOT EXISTS monitoring;

-- Create example beacon_blocks table (source)
-- Each row represents a client's observation of a block
CREATE TABLE IF NOT EXISTS ethereum.beacon_blocks
(
    updated_date_time DateTime DEFAULT now() CODEC(DoubleDelta, ZSTD(1)),
    event_date_time DateTime64(3) DEFAULT now64(3) CODEC(DoubleDelta, ZSTD(1)),
    slot_start_date_time DateTime,
    slot UInt64,
    block_root String,
    validator_index UInt32, -- validator that proposed this block
    propagation_slot_start_diff UInt32, -- milliseconds from slot start
    meta_client_name String
)
ENGINE = ReplacingMergeTree(updated_date_time)
PARTITION BY toYYYYMM(slot_start_date_time)
ORDER BY (slot_start_date_time, slot, meta_client_name, block_root)
;

-- Create validator_entity table (maps validator indexes to entity names)
CREATE TABLE IF NOT EXISTS ethereum.validator_entity
(
    updated_date_time DateTime DEFAULT now() CODEC(DoubleDelta, ZSTD(1)),
    event_date_time DateTime64(3) DEFAULT now64(3) CODEC(DoubleDelta, ZSTD(1)),
    slot_start_date_time DateTime,
    slot UInt64,
    validator_index UInt32,
    entity_name String
)
ENGINE = ReplacingMergeTree(updated_date_time)
PARTITION BY toYYYYMM(slot_start_date_time)
ORDER BY (slot_start_date_time, slot, validator_index)
;

-- Create example block_propagation table (destination for transformation)
-- Aggregated propagation statistics per block
CREATE TABLE IF NOT EXISTS analytics.block_propagation
(
    updated_date_time DateTime DEFAULT now() CODEC(DoubleDelta, ZSTD(1)),
    event_date_time DateTime64(3) DEFAULT now64(3) CODEC(DoubleDelta, ZSTD(1)),
    slot_start_date_time DateTime,
    slot UInt64,
    block_root String,
    client_count UInt32,
    avg_propagation Float32,
    median_propagation Float32,
    p90_propagation Float32,
    min_propagation UInt32,
    max_propagation UInt32,
    position UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(updated_date_time)
PARTITION BY toYYYYMM(slot_start_date_time)
ORDER BY (slot_start_date_time, slot, block_root, position)
;

-- Create block_entity table (aggregated block production by entity)
CREATE TABLE IF NOT EXISTS analytics.block_entity
(
    updated_date_time DateTime DEFAULT now() CODEC(DoubleDelta, ZSTD(1)),
    event_date_time DateTime64(3) DEFAULT now64(3) CODEC(DoubleDelta, ZSTD(1)),
    slot_start_date_time DateTime,
    slot UInt64,
    block_root String,
    validator_index UInt32,
    entity_name String,
    position UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(updated_date_time)
PARTITION BY toYYYYMM(slot_start_date_time)
ORDER BY (slot_start_date_time, slot, entity_name, position)
;

-- Create entity_network_effects table (aggregated entity performance metrics)
CREATE TABLE IF NOT EXISTS analytics.entity_network_effects
(
    updated_date_time DateTime DEFAULT now() CODEC(DoubleDelta, ZSTD(1)),
    event_date_time DateTime64(3) DEFAULT now64(3) CODEC(DoubleDelta, ZSTD(1)),
    slot_start_date_time DateTime,
    
    -- Entity identifier
    entity_name String,
    
    -- Aggregated metrics for this entity in this interval
    blocks_in_window UInt32,
    entity_avg_propagation Float32,
    entity_median_propagation Float32,
    entity_min_propagation Float32,
    entity_max_propagation Float32,
    entity_propagation_stddev Float32,
    
    -- Interval tracking
    interval UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(updated_date_time)
PARTITION BY toYYYYMM(slot_start_date_time)
ORDER BY (slot_start_date_time, entity_name, interval)
;

-- Create entity_changes table (for Python transformation model example)
CREATE TABLE IF NOT EXISTS analytics.entity_changes
(
    updated_date_time DateTime DEFAULT now() CODEC(DoubleDelta, ZSTD(1)),
    event_date_time DateTime64(3) DEFAULT now64(3) CODEC(DoubleDelta, ZSTD(1)),
    hour_start DateTime,
    entity_name String,
    validator_count UInt32,
    unique_slots UInt32,
    position UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(updated_date_time)
PARTITION BY toYYYYMM(hour_start)
ORDER BY (hour_start, entity_name, position)
;

-- Create hourly_block_stats table (nested directory model example)
CREATE TABLE IF NOT EXISTS analytics.hourly_block_stats
(
    updated_date_time DateTime DEFAULT now() CODEC(DoubleDelta, ZSTD(1)),
    event_date_time DateTime64(3) DEFAULT now64(3) CODEC(DoubleDelta, ZSTD(1)),
    hour_start DateTime,
    total_blocks UInt64,
    avg_entities Float32,
    max_entities UInt32,
    min_entities UInt32,
    position UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(updated_date_time)
PARTITION BY toYYYYMM(hour_start)
ORDER BY (hour_start, position)
;

-- Create entity_network_effects_or_test table (OR dependency test)
CREATE TABLE IF NOT EXISTS analytics.entity_network_effects_or_test
(
    updated_date_time DateTime DEFAULT now() CODEC(DoubleDelta, ZSTD(1)),
    event_date_time DateTime64(3) DEFAULT now64(3) CODEC(DoubleDelta, ZSTD(1)),
    slot_start_date_time DateTime,
    
    -- Entity identifier
    entity_name String,
    
    -- Aggregated metrics for this entity in this interval
    blocks_in_window UInt32,
    entity_avg_propagation Float32,
    entity_median_propagation Float32,
    entity_min_propagation Float32,
    entity_max_propagation Float32,
    entity_propagation_stddev Float32,
    
    -- Interval tracking
    interval UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(updated_date_time)
PARTITION BY toYYYYMM(slot_start_date_time)
ORDER BY (slot_start_date_time, entity_name, interval)
;

-- Create block_never_loads table (test transformation that never loads)
CREATE TABLE IF NOT EXISTS analytics.block_never_loads
(
    updated_date_time DateTime DEFAULT now() CODEC(DoubleDelta, ZSTD(1)),
    event_date_time DateTime64(3) DEFAULT now64(3) CODEC(DoubleDelta, ZSTD(1)),
    slot_start_date_time DateTime,
    slot UInt64,
    position UInt64 DEFAULT 0
)
ENGINE = ReplacingMergeTree(updated_date_time)
PARTITION BY toYYYYMM(slot_start_date_time)
ORDER BY (slot_start_date_time, slot, position)
;

-- Create tables for transformation dependency examples

-- Level 1: Minute Block Summary (minute aggregations)
CREATE TABLE IF NOT EXISTS analytics.minute_block_summary (
    updated_date_time DateTime64(3),
    event_date_time DateTime64(3),
    minute DateTime,
    block_count UInt32,
    unique_validators UInt32,
    avg_slot Float64,
    min_slot UInt64,
    max_slot UInt64,
    position UInt64
) ENGINE = ReplacingMergeTree(updated_date_time)
ORDER BY (minute)
PARTITION BY toYYYYMM(minute);

-- Level 2: Two Minute Aggregation (2-minute aggregations)
CREATE TABLE IF NOT EXISTS analytics.two_minute_aggregation (
    updated_date_time DateTime64(3),
    event_date_time DateTime64(3),
    two_minute_window DateTime,
    total_blocks UInt64,
    total_unique_validators UInt64,
    avg_blocks_per_minute Float64,
    window_min_slot UInt64,
    window_max_slot UInt64,
    minutes_with_data UInt32,
    position UInt64
) ENGINE = ReplacingMergeTree(updated_date_time)
ORDER BY (two_minute_window)
PARTITION BY toYYYYMM(two_minute_window);

-- Level 3: Five Minute Report (5-minute aggregations)
CREATE TABLE IF NOT EXISTS analytics.five_minute_report (
    updated_date_time DateTime64(3),
    event_date_time DateTime64(3),
    five_minute_window DateTime,
    window_blocks UInt64,
    max_validators_in_window UInt64,
    avg_blocks_per_minute Float64,
    period_min_slot UInt64,
    period_max_slot UInt64,
    total_minutes_with_data UInt64,
    two_minute_windows_with_data UInt32,
    position UInt64
) ENGINE = ReplacingMergeTree(updated_date_time)
ORDER BY (five_minute_window)
PARTITION BY toYYYYMM(five_minute_window);

-- SCHEDULED TRANSFORMATION TABLES

-- Exchange rates reference data (scheduled transformation)
CREATE TABLE IF NOT EXISTS reference.exchange_rates (
    updated_at DateTime DEFAULT now(),
    base_currency String,
    target_currency String,
    rate Float64,
    refresh_timestamp UInt64
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (base_currency, target_currency);

-- System health monitoring (scheduled transformation)
CREATE TABLE IF NOT EXISTS monitoring.system_health_check (
    check_time DateTime,
    component String,
    service String,
    status String,
    uptime_seconds UInt64,
    memory_usage String,
    timestamp UInt64
) ENGINE = MergeTree()
ORDER BY (check_time, component, service)
PARTITION BY toYYYYMM(check_time);

-- INCREMENTAL TRANSFORMATION DEPENDING ON SCHEDULED DATA

-- Raw transactions source table
CREATE TABLE IF NOT EXISTS ethereum.raw_transactions (
    transaction_id String,
    timestamp DateTime,
    amount Decimal(18, 8),
    currency String,
    position UInt64
) ENGINE = MergeTree()
ORDER BY (timestamp, transaction_id)
PARTITION BY toYYYYMM(timestamp);

-- Transactions normalized with exchange rates (incremental depending on scheduled)
CREATE TABLE IF NOT EXISTS analytics.transactions_normalized (
    transaction_id String,
    timestamp DateTime,
    amount Decimal(18, 8),
    currency String,
    amount_usd Decimal(18, 8),
    _position UInt64,
    _interval UInt64
) ENGINE = MergeTree()
ORDER BY (timestamp, transaction_id, _position)
PARTITION BY toYYYYMM(timestamp);
