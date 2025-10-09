---
type: incremental
table: transactions_with_rates
interval:
  max: 3600
  min: 0
  type: block
schedules:
  forwardfill: "@every 5m"
  backfill: "@every 5s"
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
    -- Get the latest exchange rate from the scheduled transformation using a JOIN
    -- Cast to Decimal to match table schema
    -- If no rate found (base_currency is empty), use 1.0 as the default rate
    toDecimal128(t.amount * if(er.base_currency = '', 1.0, er.rate), 8) as amount_usd,
    {{ .bounds.start }} as _position,
    {{ .bounds.end }} - {{ .bounds.start }} as _interval
FROM {{ index .dep "{{external}}" "raw_transactions" "database" }}.{{ index .dep "{{external}}" "raw_transactions" "table" }} t
LEFT JOIN (
    SELECT
        base_currency,
        target_currency,
        argMax(rate, updated_at) as rate
    FROM {{ index .dep "reference" "exchange_rates" "database" }}.{{ index .dep "reference" "exchange_rates" "table" }}
    WHERE target_currency = 'USD'
    GROUP BY base_currency, target_currency
) er ON t.currency = er.base_currency
WHERE t.position >= {{ .bounds.start }}
  AND t.position < {{ .bounds.end }}