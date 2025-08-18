---
database: analytics
table: hourly_block_stats
partition: hour_start
interval: 3600
schedule: "@every 5m"
backfill:
  enabled: true
  schedule: "@every 5m"
tags:
  - aggregation
  - hourly
dependencies:
  - analytics.block_entity
---
-- Hourly aggregation of block statistics
-- Demonstrates nested directory model structure

INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT 
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    now64(3) as event_date_time,
    toStartOfHour(slot_start_date_time) as hour_start,
    count() as total_blocks,
    count(DISTINCT entity_name) as avg_entities,  -- Count unique entities as metric
    count(DISTINCT entity_name) as max_entities,  -- Same for now
    count(DISTINCT entity_name) as min_entities,   -- Same for now
    {{ .range.start }} as position
FROM `{{ index .dep "analytics" "block_entity" "database" }}`.`{{ index .dep "analytics" "block_entity" "table" }}`
WHERE slot_start_date_time >= fromUnixTimestamp({{ .range.start }})
  AND slot_start_date_time < fromUnixTimestamp({{ .range.end }})
GROUP BY hour_start;

-- Delete old rows
DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE
  {{ .self.partition }} BETWEEN fromUnixTimestamp({{ .range.start }}) AND fromUnixTimestamp({{ .range.end }})
  AND updated_date_time != fromUnixTimestamp({{ .task.start }})