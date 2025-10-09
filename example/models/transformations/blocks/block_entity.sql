---
# database is set in the config models.transformations.defaultDatabase
# database: analytics
type: incremental
table: block_entity
interval:
  max: 60
  min: 40  # Allow partial intervals down to 40 seconds
  type: slot
schedules:
  forwardfill: "@every 10s"
  backfill: "@every 10s"
tags:
  - entity
  - block
dependencies:
  - "{{external}}.beacon_blocks"
  - "{{external}}.validator_entity"
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
    {{ .bounds.start }} as position
FROM (
    SELECT DISTINCT
        slot_start_date_time,
        slot,
        block_root,
        validator_index
    FROM `{{ index .dep "{{external}}" "beacon_blocks" "database" }}`.`{{ index .dep "{{external}}" "beacon_blocks" "table" }}`
    WHERE slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
) b
-- Always look back 24 hours to get the most recent entity mapping for each validator
LEFT JOIN (
    SELECT 
        validator_index,
        argMax(entity_name, slot_start_date_time) as entity_name
    FROM `{{ index .dep "{{external}}" "validator_entity" "database" }}`.`{{ index .dep "{{external}}" "validator_entity" "table" }}`
    WHERE slot_start_date_time >= fromUnixTimestamp({{ .bounds.start }}) - INTERVAL 24 HOUR
      AND slot_start_date_time <= fromUnixTimestamp({{ .bounds.end }})
    GROUP BY validator_index
) ve ON b.validator_index = ve.validator_index;

-- Delete old rows
DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE
  slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
  AND updated_date_time != fromUnixTimestamp({{ .task.start }});
