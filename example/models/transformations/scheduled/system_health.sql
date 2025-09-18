---
type: scheduled
database: monitoring
table: system_health_check
schedule: "@every 1m"
tags:
  - monitoring
  - scheduled
---
-- System health monitoring query
-- Checks various system metrics on schedule
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    now() as check_time,
    'database' as component,
    'clickhouse' as service,
    'healthy' as status,
    uptime() as uptime_seconds,
    formatReadableSize((SELECT value FROM system.metrics WHERE metric = 'MemoryTracking')) as memory_usage,
    {{ .task.start }} as timestamp