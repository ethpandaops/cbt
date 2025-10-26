---
# database is set in the config models.transformations.defaultDatabase
# database: analytics
type: incremental
table: hourly_block_stats
limits:
  min: 1000  # Optional: minimum position to process
  max: 0     # Optional: maximum position to process (0 = no limit)
interval:
  max: 3600
  min: 0     # Allow any size partial intervals
  type: slot
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 5s"
tags:
  - aggregation
  - hourly
dependencies:
  - "{{transformation}}.block_entity"
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
    {{ .bounds.start }} as position
FROM {{ index .dep "{{transformation}}" "block_entity" "helpers" "from" }}
WHERE slot_start_date_time >= fromUnixTimestamp({{ .bounds.start }})
  AND slot_start_date_time < fromUnixTimestamp({{ .bounds.end }})
GROUP BY hour_start;

-- Delete old rows
DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE
  hour_start BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
  AND updated_date_time != fromUnixTimestamp({{ .task.start }})