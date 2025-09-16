---
# Scheduled transformation - no dependencies, no interval
type: scheduled
table: system_health_check
schedule: "@every 5m"
---
-- System health check - runs independently on schedule
-- No position/interval tracking for scheduled transformations

INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    now() as check_time,
    'healthy' as status,
    'scheduled' as execution_mode,
    {{ .execution.timestamp }} as execution_timestamp,
    '{{ .execution.datetime }}' as execution_datetime,
    'scheduled maintenance check' as check_type,
    (SELECT count() FROM system.processes) as active_processes,
    (SELECT count() FROM system.tables) as total_tables