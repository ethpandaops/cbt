---
type: scheduled
database: reference
table: exchange_rates
schedule: "@every 30s"
tags:
  - reference-data
  - scheduled
env:
  TARGET_CURRENCY: "EUR"  # Model-specific env var
---
-- Exchange rate reference data update
-- This runs on schedule without tracking positions
-- Demonstrates using environment variables in SQL templates:
-- - BASE_CURRENCY comes from global config (models.env.BASE_CURRENCY = "USD")
-- - TARGET_CURRENCY is model-specific (overrides any global value)
-- - ENVIRONMENT is global (used in comment for demo)
INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    now() as updated_at,
    '{{ .env.BASE_CURRENCY }}' as base_currency,  -- Uses global env var
    '{{ .env.TARGET_CURRENCY }}' as target_currency,  -- Uses model-specific env var
    0.85 + (rand() / 4294967295.0 * 0.1 - 0.05) as rate,  -- Random rate for demo (0.80-0.90)
    {{ .task.start }} as refresh_timestamp,
    '{{ .env.ENVIRONMENT }}' as environment  -- Track which environment generated this data