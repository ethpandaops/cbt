---
database: ethereum
table: validator_entity
partition: slot_start_date_time
external: true
ttl: 60s
lag: 20  # Ignore last 20 seconds for validator data consistency
---
SELECT 
    toUnixTimestamp(min(slot_start_date_time)) as min,
    toUnixTimestamp(max(slot_start_date_time)) as max
FROM `{{ .self.database }}`.`{{ .self.table }}`
FINAL;