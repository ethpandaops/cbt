package coordinator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/tasks"
	"github.com/hibiken/asynq"
)

// TriggerScheduledRun enqueues an immediate run for a scheduled transformation.
// Returns ErrScheduledRunInProgress when an existing task is already active/pending.
func (s *service) TriggerScheduledRun(_ context.Context, modelID string) error {
	node, err := s.dag.GetNode(modelID)
	if err != nil {
		return fmt.Errorf("failed to get model node: %w", err)
	}

	if node.NodeType != models.NodeTypeTransformation {
		return ErrNotScheduledModel
	}

	trans, err := s.dag.GetTransformationNode(modelID)
	if err != nil {
		return fmt.Errorf("failed to get transformation node: %w", err)
	}

	if !trans.GetConfig().IsScheduledType() {
		return ErrNotScheduledModel
	}

	now := time.Now().UTC()
	payload := tasks.ScheduledTaskPayload{
		Type:          tasks.TaskTypeScheduled,
		ModelID:       modelID,
		ExecutionTime: now,
		EnqueuedAt:    now,
	}

	if s.queueManager == nil {
		return ErrQueueManagerNil
	}

	if err := s.queueManager.EnqueueTransformation(payload); err != nil {
		if errors.Is(err, asynq.ErrTaskIDConflict) {
			return ErrScheduledRunInProgress
		}

		return fmt.Errorf("failed to enqueue scheduled run: %w", err)
	}

	return nil
}
