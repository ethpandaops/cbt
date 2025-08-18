---
database: ethereum
table: validator_entity
partition: slot_start_date_time
external: true
ttl: 30s
lag: 10
---
SELECT 
    toUnixTimestamp(min(slot_start_date_time)) as min,
    toUnixTimestamp(max(slot_start_date_time)) as max
FROM `{{ .self.database }}`.`{{ .self.table }}`
FINAL;