---
database: analytics
table: block_propagation
partition: slot_start_date_time
interval: 60
schedule: "@every 10s"
backfill:
  enabled: true
  schedule: "@every 10s"
tags:
  - propagation
  - block
dependencies:
  - ethereum.beacon_blocks
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT 
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    now64(3) as event_date_time,
    slot_start_date_time,
    slot,
    block_root,
    count(DISTINCT meta_client_name) as client_count,
    avg(propagation_slot_start_diff) as avg_propagation,
    median(propagation_slot_start_diff) as median_propagation,
    quantile(0.9)(propagation_slot_start_diff) as p90_propagation,
    min(propagation_slot_start_diff) as min_propagation,
    max(propagation_slot_start_diff) as max_propagation,
    {{ .bounds.start }} as position
FROM `{{ index .dep "ethereum" "beacon_blocks" "database" }}`.`{{ index .dep "ethereum" "beacon_blocks" "table" }}`
WHERE {{ index .dep "ethereum" "beacon_blocks" "partition" }} BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
GROUP BY slot_start_date_time, slot, block_root;

-- Delete old rows
DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE
  {{ .self.partition }} BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
  AND updated_date_time != fromUnixTimestamp({{ .task.start }});
