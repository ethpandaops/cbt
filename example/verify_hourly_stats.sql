-- Verification query for analytics.hourly_block_stats
-- This query replicates exactly what the hourly_block_stats model does
-- Compare this output with: SELECT * FROM analytics.hourly_block_stats ORDER BY hour_start

SELECT 
    toStartOfHour(slot_start_date_time) as hour_start,
    count() as total_blocks,
    count(DISTINCT entity_name) as unique_entities,
    -- The model uses the same count(DISTINCT) for all three metrics
    count(DISTINCT entity_name) as avg_entities,
    count(DISTINCT entity_name) as max_entities,  
    count(DISTINCT entity_name) as min_entities
FROM analytics.block_entity
GROUP BY hour_start
ORDER BY hour_start;

-- Detailed breakdown by hour and entity
SELECT 
    toStartOfHour(slot_start_date_time) as hour_start,
    entity_name,
    count() as blocks_per_entity,
    min(slot) as min_slot,
    max(slot) as max_slot
FROM analytics.block_entity
GROUP BY hour_start, entity_name
ORDER BY hour_start, blocks_per_entity DESC;

-- Verify total counts match
SELECT 
    'Source (block_entity)' as source,
    count() as total_rows,
    min(slot_start_date_time) as min_time,
    max(slot_start_date_time) as max_time,
    count(DISTINCT toStartOfHour(slot_start_date_time)) as distinct_hours
FROM analytics.block_entity;