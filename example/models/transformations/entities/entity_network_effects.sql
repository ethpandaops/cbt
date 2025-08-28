---
# database is set in the config models.transformations.defaultDatabase
# database: analytics
table: entity_network_effects
interval:
  max: 300
  min: 0  # Allow any size partial intervals
schedules:
  forwardfill: "@every 30s"
  backfill: "@every 30s"
tags:
  - aggregation
  - entity
  - network
dependencies:
  # you can use the models.transformations.defaultDatabase template variable here
  - "{{transformation}}.block_entity"
  # or directly reference the analytics database
  - "analytics.block_propagation"
---
INSERT INTO
  `{{ .self.database }}`.`{{ .self.table }}`
SELECT 
    fromUnixTimestamp({{ .task.start }}) as updated_date_time,
    now64(3) as event_date_time,
    fromUnixTimestamp({{ .bounds.start }}) as slot_start_date_time,
    
    -- Entity identifier
    be.entity_name,
    
    -- Aggregated metrics for this entity in this interval
    count() as blocks_in_window,
    avg(bp.avg_propagation) as entity_avg_propagation,
    median(bp.avg_propagation) as entity_median_propagation,
    min(bp.avg_propagation) as entity_min_propagation,
    max(bp.avg_propagation) as entity_max_propagation,
    stddevPop(bp.avg_propagation) as entity_propagation_stddev,
    
    -- Interval tracking
    {{ .self.interval }} as interval
    
FROM `{{ index .dep "{{transformation}}" "block_entity" "database" }}`.`{{ index .dep "{{transformation}}" "block_entity" "table" }}` be
INNER JOIN `{{ index .dep "analytics" "block_propagation" "database" }}`.`{{ index .dep "analytics" "block_propagation" "table" }}` bp
    ON be.slot = bp.slot 
    AND be.block_root = bp.block_root
WHERE be.slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
  AND bp.slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
GROUP BY be.entity_name
HAVING blocks_in_window > 0;

-- Delete old rows
DELETE FROM
  `{{ .self.database }}`.`{{ .self.table }}{{ if .clickhouse.cluster }}{{ .clickhouse.local_suffix }}{{ end }}`
{{ if .clickhouse.cluster }}
  ON CLUSTER '{{ .clickhouse.cluster }}'
{{ end }}
WHERE
  slot_start_date_time BETWEEN fromUnixTimestamp({{ .bounds.start }}) AND fromUnixTimestamp({{ .bounds.end }})
  AND updated_date_time != fromUnixTimestamp({{ .task.start }});