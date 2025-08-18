---
database: ethereum
table: validator_entity
partition: slot_start_date_time
external: true
ttl: 60s
lookback: 2  # Process last 2 intervals to handle validator data updates
---
SELECT 
    toUnixTimestamp(min(slot_start_date_time)) as min,
    toUnixTimestamp(max(slot_start_date_time)) as max
FROM `{{ .self.database }}`.`{{ .self.table }}`
FINAL;