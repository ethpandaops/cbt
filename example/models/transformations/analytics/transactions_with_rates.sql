---
type: incremental
table: transactions_normalized
interval:
  max: 3600
  min: 0
  type: block
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 5m"
tags:
  - analytics
  - cross-type-dependency
dependencies:
  - "reference.exchange_rates"
  - "{{external}}.raw_transactions"
---
-- Incremental transformation using scheduled reference data
-- This shows how incremental transformations can depend on scheduled ones
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    t.transaction_id,
    t.timestamp,
    t.amount,
    t.currency,
    -- Get the latest exchange rate from the scheduled transformation
    t.amount * (
        SELECT rate 
        FROM {{ index .dep "reference" "exchange_rates" "database" }}.{{ index .dep "reference" "exchange_rates" "table" }} 
        WHERE base_currency = t.currency 
          AND target_currency = 'USD' 
        ORDER BY updated_at DESC 
        LIMIT 1
    ) as amount_usd,
    {{ .bounds.start }} as _position,
    {{ .bounds.end }} - {{ .bounds.start }} as _interval
FROM {{ index .dep "{{external}}" "raw_transactions" "database" }}.{{ index .dep "{{external}}" "raw_transactions" "table" }} t
WHERE t.position >= {{ .bounds.start }}
  AND t.position < {{ .bounds.end }}