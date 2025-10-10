---
# Third level transformation - depends on two minute aggregation
type: incremental
table: five_minute_report
interval:
  max: 300  # 5 minutes
  min: 120  # Allow partial intervals down to 2 minutes
  type: slot
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 5s"
tags:
  - analytics
  - level3
  - reporting
dependencies:
  - "{{transformation}}.two_minute_aggregation"  # Depends on the two_minute_aggregation transformation
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT 
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    now64(3) as event_date_time,
    toStartOfInterval(two_minute_window, INTERVAL 5 MINUTE) as five_minute_window,
    sum(total_blocks) as window_blocks,
    max(total_unique_validators) as max_validators_in_window,
    avg(avg_blocks_per_minute) as avg_blocks_per_minute,
    min(window_min_slot) as period_min_slot,
    max(window_max_slot) as period_max_slot,
    sum(minutes_with_data) as total_minutes_with_data,
    count() as two_minute_windows_with_data,
    {{ .bounds.start }} as position
FROM `{{ index .dep "{{transformation}}" "two_minute_aggregation" "database" }}`.`{{ index .dep "{{transformation}}" "two_minute_aggregation" "table" }}`
WHERE two_minute_window BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
GROUP BY five_minute_window;

-- Clean up old data
DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE
  five_minute_window BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
  AND updated_date_time != fromUnixTimestamp({{ .task.start }});