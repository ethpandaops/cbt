---
# Regular transformation that depends on a standalone transformation
table: transactions_normalized
interval:
  max: 3600  # Process in 1-hour chunks
  min: 0
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 30m"
dependencies:
  # exchange_rates is a standalone transformation - always available
  - "{{transformation}}.exchange_rates"
  # raw_transactions is a regular data source with position tracking
  - "{{external}}.raw_transactions"
---
-- This transformation depends on both:
-- 1. A standalone transformation (exchange_rates) - always available
-- 2. A regular external model (raw_transactions) - has bounded data

INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT 
    t.transaction_id,
    t.user_id,
    t.amount,
    t.currency,
    t.timestamp,
    -- Join with the standalone transformation's data
    -- Since exchange_rates is standalone, it's always available
    t.amount * r.rate as amount_usd,
    r.rate as exchange_rate,
    r.updated_at as rate_updated_at,
    {{ .bounds.start }} as position,
    '{{ .task.direction }}' as processing_mode,
    {{ if .is_backfill }}
        'historical' as data_type
    {{ else }}
        'real-time' as data_type
    {{ end }}
FROM `{{ index .dep "{{external}}" "raw_transactions" "database" }}`.`{{ index .dep "{{external}}" "raw_transactions" "table" }}` t
LEFT JOIN (
    SELECT 
        target_currency,
        rate,
        updated_at,
        row_number() OVER (PARTITION BY target_currency ORDER BY updated_at DESC) as rn
    FROM `{{ index .dep "{{transformation}}" "exchange_rates" "database" }}`.`{{ index .dep "{{transformation}}" "exchange_rates" "table" }}`
    WHERE base_currency = 'USD'
) r ON t.currency = r.target_currency AND r.rn = 1
WHERE t.timestamp >= fromUnixTimestamp({{ .bounds.start }})
  AND t.timestamp < fromUnixTimestamp({{ .bounds.end }})