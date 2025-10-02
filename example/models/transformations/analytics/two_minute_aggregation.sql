---
# Second level transformation - depends on another transformation
type: incremental
table: two_minute_aggregation
interval:
  max: 120  # 2 minutes
  min: 60   # Allow partial intervals down to 1 minute
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 1m"
tags:
  - analytics
  - level2
dependencies:
  - "{{transformation}}.minute_block_summary"  # Depends on the minute_block_summary transformation
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT 
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    now64(3) as event_date_time,
    toStartOfInterval(minute, INTERVAL 2 MINUTE) as two_minute_window,
    sum(block_count) as total_blocks,
    sum(unique_validators) as total_unique_validators,
    avg(block_count) as avg_blocks_per_minute,
    min(min_slot) as window_min_slot,
    max(max_slot) as window_max_slot,
    count() as minutes_with_data,
    {{ .bounds.start }} as position
FROM `{{ index .dep "{{transformation}}" "minute_block_summary" "database" }}`.`{{ index .dep "{{transformation}}" "minute_block_summary" "table" }}`
WHERE minute BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
GROUP BY two_minute_window;

-- Clean up old data
DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE
  two_minute_window BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
  AND updated_date_time != fromUnixTimestamp({{ .task.start }});