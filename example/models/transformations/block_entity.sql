---
database: analytics
table: block_entity
partition: slot_start_date_time
interval: 60
schedule: "@every 10s"
backfill: true
tags:
  - entity
  - block
dependencies:
  - ethereum.beacon_blocks
  - ethereum.validator_entity
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT 
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    now64(3) as event_date_time,
    b.slot_start_date_time,
    b.slot,
    b.block_root,
    b.validator_index,
    ve.entity_name,
    {{ .range.start }} as position
FROM (
    SELECT DISTINCT
        slot_start_date_time,
        slot,
        block_root,
        validator_index
    FROM `{{ index .dep "ethereum" "beacon_blocks" "database" }}`.`{{ index .dep "ethereum" "beacon_blocks" "table" }}`
    WHERE {{ index .dep "ethereum" "beacon_blocks" "partition" }} BETWEEN fromUnixTimestamp({{ .range.start }}) AND fromUnixTimestamp({{ .range.end }})
) b
-- Always look back 24 hours to get the most recent entity mapping for each validator
LEFT JOIN (
    SELECT 
        validator_index,
        argMax(entity_name, slot_start_date_time) as entity_name
    FROM `{{ index .dep "ethereum" "validator_entity" "database" }}`.`{{ index .dep "ethereum" "validator_entity" "table" }}`
    WHERE slot_start_date_time >= fromUnixTimestamp({{ .range.start }}) - INTERVAL 24 HOUR
      AND slot_start_date_time <= fromUnixTimestamp({{ .range.end }})
    GROUP BY validator_index
) ve ON b.validator_index = ve.validator_index;

-- Delete old rows
DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE
  {{ .self.partition }} BETWEEN fromUnixTimestamp({{ .range.start }}) AND fromUnixTimestamp({{ .range.end }})
  AND updated_date_time != fromUnixTimestamp({{ .task.start }});
