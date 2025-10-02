---
type: scheduled
database: reference
table: exchange_rates
schedule: "@every 30s"
tags:
  - reference-data
  - scheduled
---
-- Exchange rate reference data update
-- This runs on schedule without tracking positions
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    now() as updated_at,
    'USD' as base_currency,
    'EUR' as target_currency,
    0.85 + (rand() / 4294967295.0 * 0.1 - 0.05) as rate,  -- Random rate for demo (0.80-0.90)
    {{ .task.start }} as refresh_timestamp