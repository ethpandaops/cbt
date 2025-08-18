---
database: ethereum
table: beacon_blocks
partition: slot_start_date_time
external: true
ttl: 60s
lag: 30  # Ignore last 30 seconds to account for late-arriving blocks
---
SELECT 
    toUnixTimestamp(min(slot_start_date_time)) as min,
    toUnixTimestamp(max(slot_start_date_time)) as max
FROM `{{ .self.database }}`.`{{ .self.table }}`
FINAL;