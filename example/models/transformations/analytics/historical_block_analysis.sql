---
# Example demonstrating tail-first fill with no gap skipping
# This model processes blocks chronologically from the beginning
# and requires sequential processing (no gaps allowed)
type: incremental
table: historical_block_analysis
interval:
  max: 3600   # 1 hour worth of data
  min: 1800   # Allow partial intervals down to 30 minutes
  type: slot
fill:
  direction: "tail"           # Start from oldest data (beginning)
  allow_gap_skipping: false   # Must process sequentially, no gaps
schedules:
  forwardfill: "@every 30s"
  backfill: "@every 1m"
tags:
  - analytics
  - historical
dependencies:
  - "{{external}}.beacon_blocks"
---
-- This transformation processes blocks chronologically from the beginning
-- Perfect for historical analysis that requires complete sequential data
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    now64(3) as event_date_time,
    slot_start_date_time,
    count() as block_count,
    countDistinct(validator_index) as unique_validators,
    avg(slot) as avg_slot,
    min(slot) as min_slot,
    max(slot) as max_slot,
    {{ .bounds.start }} as position,
    {{ .bounds.end }} as position_end,
    '{{ .task.direction }}' as fill_direction
FROM `{{ index .dep "{{external}}" "beacon_blocks" "database" }}`.`{{ index .dep "{{external}}" "beacon_blocks" "table" }}`
WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
GROUP BY slot_start_date_time;

-- Clean up old data for reprocessing
DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE
  slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
  AND updated_date_time != fromUnixTimestamp({{ .task.start }});
