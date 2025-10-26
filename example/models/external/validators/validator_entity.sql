---
# cluster and database are set in the config models.external.defaultCluster and models.external.defaultDatabase
# cluster: my_cluster
# database: ethereum
table: validator_entity
interval:
  type: slot
cache:
  incremental_scan_interval: 10s
  full_scan_interval: 1h
lag: 10
# Example: External models can also use environment variables
# env:
#   ENTITY_TYPE_FILTER: "lido"
#   MIN_VALIDATOR_COUNT: "100"
---
-- External models can access environment variables via {{ .env.KEY_NAME }}
-- This is useful for filtering data sources or adjusting query behavior
SELECT
    toUnixTimestamp(min(slot_start_date_time)) as min,
    toUnixTimestamp(max(slot_start_date_time)) as max
FROM {{ .self.helpers.from }} FINAL
WHERE 1=1
{{ if .cache.is_incremental_scan }}
  AND (slot_start_date_time < fromUnixTimestamp({{ .cache.previous_min }})
   OR slot_start_date_time > fromUnixTimestamp({{ .cache.previous_max }}))
{{ end }}
-- Example: You could filter by entity type from environment variables
-- {{ if .env.ENTITY_TYPE_FILTER }}
--   AND entity_type = '{{ .env.ENTITY_TYPE_FILTER }}'
-- {{ end }}
-- Example: Or filter by minimum validator count
-- {{ if .env.MIN_VALIDATOR_COUNT }}
--   AND validator_count >= {{ .env.MIN_VALIDATOR_COUNT }}
-- {{ end }}
