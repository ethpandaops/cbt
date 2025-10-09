---
# External model for raw transaction data
table: raw_transactions
interval:
  type: block
cache:
  incremental_scan_interval: 1m
  full_scan_interval: 1h
lag: 10  # Ignore last 10 positions to avoid incomplete data
---
SELECT 
    min(position) as min,
    max(position) as max
FROM `{{ .self.database }}`.`{{ .self.table }}`
{{ if .cache.is_incremental_scan }}
WHERE position < {{ .cache.previous_min }}
   OR position > {{ .cache.previous_max }}
{{ end }}