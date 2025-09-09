---
# First level transformation - depends only on external data
table: minute_block_summary
interval:
  max: 60   # 1 minute
  min: 30   # Allow partial intervals down to 30 seconds
schedules:
  forwardfill: "@every 5s"
  backfill: "@every 10s"
tags:
  - analytics
  - level1
dependencies:
  - "{{external}}.beacon_blocks"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT 
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    now64(3) as event_date_time,
    toStartOfMinute(slot_start_date_time) as minute,
    count() as block_count,
    countDistinct(validator_index) as unique_validators,
    avg(slot) as avg_slot,
    min(slot) as min_slot,
    max(slot) as max_slot,
    {{ .bounds.start }} as position
FROM `{{ index .dep "{{external}}" "beacon_blocks" "database" }}`.`{{ index .dep "{{external}}" "beacon_blocks" "table" }}`
WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
GROUP BY minute;

-- Clean up old data
DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE
  minute BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
  AND updated_date_time != fromUnixTimestamp({{ .task.start }});