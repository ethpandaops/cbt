---
# database is set in the config models.external.defaultDatabase
# database: ethereum
table: validator_entity
cache:
  incremental_scan_interval: 10s
  full_scan_interval: 1h
lag: 10
---
SELECT 
    toUnixTimestamp(min(slot_start_date_time)) as min,
    toUnixTimestamp(max(slot_start_date_time)) as max
FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
{{ if .cache.is_incremental_scan }}
WHERE slot_start_date_time < fromUnixTimestamp({{ .cache.previous_min }})
   OR slot_start_date_time > fromUnixTimestamp({{ .cache.previous_max }})
{{ end }}
