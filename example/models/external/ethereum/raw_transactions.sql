---
# External model for raw transactions data
# This table contains transaction data with position tracking based on slot
table: raw_transactions
cache:
  incremental_scan_interval: 30s
  full_scan_interval: 5m
lag: 5
---
SELECT 
    min(slot) as min,
    max(slot) as max
FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL
{{ if .cache.is_incremental_scan }}
WHERE slot < {{ .cache.previous_min }}
   OR slot > {{ .cache.previous_max }}
{{ end }}