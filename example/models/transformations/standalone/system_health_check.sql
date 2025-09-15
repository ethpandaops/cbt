---
# Standalone transformation - no dependencies, no interval
table: system_health_check
schedules:
  forwardfill: "@every 5m"
---
-- System health check - runs independently on schedule
-- No position/interval tracking for standalone transformations

INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT 
    now() as check_time,
    'healthy' as status,
    '{{ .task.direction }}' as execution_mode,
    {{ .execution.timestamp }} as execution_timestamp,
    '{{ .execution.datetime }}' as execution_datetime,
    {{ if .is_forward }}
        'real-time check' as check_type
    {{ else }}
        'scheduled maintenance check' as check_type
    {{ end }},
    (SELECT count() FROM system.processes) as active_processes,
    (SELECT count() FROM system.tables) as total_tables