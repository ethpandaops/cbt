---
database: ethereum
table: beacon_blocks
ttl: 30s
lag: 10
---
SELECT 
    toUnixTimestamp(min(slot_start_date_time)) as min,
    toUnixTimestamp(max(slot_start_date_time)) as max
FROM `{{ .self.database }}`.`{{ .self.table }}` FINAL;