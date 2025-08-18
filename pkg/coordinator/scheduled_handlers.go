package coordinator

import (
	"context"
	"strings"

	"github.com/ethpandaops/cbt/pkg/observability"
	"github.com/hibiken/asynq"
)

const taskPrefix = "cbt:"

// HandleScheduledForward processes scheduled forward fill checks
func (c *Coordinator) HandleScheduledForward(_ context.Context, t *asynq.Task) error {
	modelID := extractModelID(t.Type())

	c.logger.WithField("model_id", modelID).Debug("Processing scheduled forward check")

	// This triggers the existing forward fill logic
	c.checkAndEnqueueModel(modelID)

	// Record metrics
	observability.ScheduledTaskExecutions.WithLabelValues(
		modelID, "forward", "success",
	).Inc()

	return nil
}

// HandleScheduledBackfill processes scheduled backfill scans
func (c *Coordinator) HandleScheduledBackfill(_ context.Context, t *asynq.Task) error {
	modelID := extractModelID(t.Type())

	c.logger.WithField("model_id", modelID).Debug("Processing scheduled backfill scan")

	// This triggers the existing backfill logic
	c.checkBackfillForModel(modelID)

	// Record metrics
	observability.ScheduledTaskExecutions.WithLabelValues(
		modelID, "backfill", "success",
	).Inc()

	return nil
}

// extractModelID extracts the model ID from a task type
// Example: "cbt:analytics.block_propagation:forward" -> "analytics.block_propagation"
func extractModelID(taskType string) string {
	trimmed := strings.TrimPrefix(taskType, taskPrefix)
	parts := strings.Split(trimmed, ":")
	if len(parts) > 0 {
		return parts[0]
	}
	return ""
}
