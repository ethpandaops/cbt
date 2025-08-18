---
database: ethereum
table: beacon_blocks
partition: slot_start_date_time
external: true
ttl: 60s
lookback: 3  # Process last 3 intervals to handle late-arriving blocks
---
SELECT 
    toUnixTimestamp(min(slot_start_date_time)) as min,
    toUnixTimestamp(max(slot_start_date_time)) as max
FROM `{{ .self.database }}`.`{{ .self.table }}`
FINAL;