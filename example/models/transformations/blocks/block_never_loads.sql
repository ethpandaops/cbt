---
database: analytics
table: block_never_loads
interval:
  max: 3600
  min: 300
schedules:
  forwardfill: "@every 9999h"
  backfill: "@every 9999h"
tags:
  - test
  - never-loads
dependencies:
  - "{{external}}.beacon_blocks"
---
-- This transformation will never load data due to schedules

INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT 
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    now64(3) as event_date_time,
    slot_start_date_time,
    slot,
    {{ .bounds.start }} as position
FROM `{{ index .dep "ethereum" "beacon_blocks" "database" }}`.`{{ index .dep "ethereum" "beacon_blocks" "table" }}`
WHERE slot_start_date_time >= fromUnixTimestamp({{ .bounds.start }})
  AND slot_start_date_time < fromUnixTimestamp({{ .bounds.end }})
  -- Additional impossible condition to ensure no data loads
  AND 1 = 0;

-- Clean up (will never execute due to no data being inserted)
DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE
  slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
  AND updated_date_time != fromUnixTimestamp({{ .task.start }});