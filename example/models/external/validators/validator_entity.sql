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
---
SELECT
    toUnixTimestamp(min(slot_start_date_time)) as min,
    toUnixTimestamp(max(slot_start_date_time)) as max
FROM {{ .self.helpers.from }} FINAL
{{ if .cache.is_incremental_scan }}
WHERE slot_start_date_time < fromUnixTimestamp({{ .cache.previous_min }})
   OR slot_start_date_time > fromUnixTimestamp({{ .cache.previous_max }})
{{ end }}
