-- Create example databases
CREATE DATABASE IF NOT EXISTS ethereum;
CREATE DATABASE IF NOT EXISTS analytics;

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

-- No initial data - the data generator will create all sample data
