---
type: scheduled
database: analytics
table: daily_block_summary
schedule: "0 0 * * *"  # Daily at midnight
dependencies:
  - "{{external}}.beacon_blocks"
  - "{{transformation}}.minute_block_summary"
tags:
  - analytics
  - scheduled
  - daily-summary
---
-- Daily block summary scheduled transformation
-- This runs on a time-based schedule (midnight daily) without position tracking
-- The dependencies field serves two purposes:
--   1. Metadata: Documents what data this transformation uses (for DAG visualization/lineage)
--   2. Templating: Makes dependency references available via {{ .dep }} in SQL
-- Note: Dependencies do NOT control when this runs (purely time-based via schedule)

INSERT INTO `{{ .self.database }}`.`{{ .self.table }}`
SELECT
    toDate(now()) as summary_date,
    count() as total_blocks,
    countDistinct(validator_index) as unique_validators,
    avg(slot) as avg_slot,
    {{ .execution.timestamp }} as generated_at
FROM `{{ .dep.beacon_blocks.database }}`.`{{ .dep.beacon_blocks.table }}` AS blocks
LEFT JOIN `{{ .dep.minute_block_summary.database }}`.`{{ .dep.minute_block_summary.table }}` AS summary
  ON toStartOfMinute(blocks.slot_start_date_time) = summary.minute
WHERE toDate(blocks.slot_start_date_time) = yesterday()
GROUP BY summary_date
