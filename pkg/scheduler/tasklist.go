package scheduler

import (
	"fmt"
	"strings"

	"github.com/ethpandaops/cbt/pkg/coordinator"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/transformation/incremental"
	"github.com/hibiken/asynq"
)

// scheduleProvider is implemented by transformation handlers that expose a cron
// schedule expression (e.g. the scheduled transformation handler's GetSchedule).
type scheduleProvider interface {
	GetSchedule() string
}

// transformationTaskID builds the scheduled-task ID for a transformation model.
// It is the single inverse of extractModelID: the format is
// "transformation:{modelID}:{suffix}" where suffix is "scheduled" or a
// coordinator.Direction ("forward"/"back").
func transformationTaskID(modelID, suffix string) string {
	return fmt.Sprintf("%s%s:%s", TransformationTaskPrefix, modelID, suffix)
}

// buildScheduledTasks constructs the list of scheduled tasks from DAG config
func (s *service) buildScheduledTasks() []scheduledTask {
	systemTasks := s.buildSystemScheduledTasks()
	externalTasks := s.buildExternalScheduledTasks()
	transformationTasks := s.buildTransformationScheduledTasks()

	scheduledTasks := make(
		[]scheduledTask,
		0,
		len(systemTasks)+len(externalTasks)+len(transformationTasks),
	)
	scheduledTasks = append(scheduledTasks, systemTasks...)
	scheduledTasks = append(scheduledTasks, externalTasks...)
	scheduledTasks = append(scheduledTasks, transformationTasks...)

	return scheduledTasks
}

// buildSystemScheduledTasks creates scheduled tasks for system operations
func (s *service) buildSystemScheduledTasks() []scheduledTask {
	var scheduledTasks []scheduledTask

	// Consolidation task
	if s.cfg.Consolidation != "" {
		interval, err := parseScheduleInterval(s.cfg.Consolidation)
		if err != nil {
			s.log.WithError(err).Error("Invalid consolidation schedule")
		} else {
			scheduledTasks = append(scheduledTasks, scheduledTask{
				ID:       ConsolidationTaskType,
				Schedule: s.cfg.Consolidation,
				Interval: interval,
				Task:     asynq.NewTask(ConsolidationTaskType, nil),
				Queue:    QueueName,
			})
		}
	}

	return scheduledTasks
}

// buildExternalScheduledTasks creates scheduled tasks for external model scans
func (s *service) buildExternalScheduledTasks() []scheduledTask {
	var scheduledTasks []scheduledTask

	for _, node := range s.dag.GetExternalNodes() {
		model, ok := node.Model.(models.External)
		if !ok {
			continue
		}

		config := model.GetConfig()
		if config.Cache == nil {
			continue
		}

		modelID := model.GetID()

		// Incremental scan
		if config.Cache.IncrementalScanInterval > 0 {
			schedule := "@every " + config.Cache.IncrementalScanInterval.String()
			taskID := fmt.Sprintf("%s%s:incremental", ExternalTaskPrefix, modelID)
			scheduledTasks = append(scheduledTasks, scheduledTask{
				ID:       taskID,
				Schedule: schedule,
				Interval: config.Cache.IncrementalScanInterval,
				Task:     asynq.NewTask(taskID, nil),
				Queue:    QueueName,
			})
		}

		// Full scan
		if config.Cache.FullScanInterval > 0 {
			schedule := "@every " + config.Cache.FullScanInterval.String()
			taskID := fmt.Sprintf("%s%s:full", ExternalTaskPrefix, modelID)
			scheduledTasks = append(scheduledTasks, scheduledTask{
				ID:       taskID,
				Schedule: schedule,
				Interval: config.Cache.FullScanInterval,
				Task:     asynq.NewTask(taskID, nil),
				Queue:    QueueName,
			})
		}
	}

	return scheduledTasks
}

// buildTransformationScheduledTasks creates scheduled tasks for transformations
func (s *service) buildTransformationScheduledTasks() []scheduledTask {
	var scheduledTasks []scheduledTask

	for _, node := range s.dag.GetTransformationNodes() {
		trans := node
		modelID := trans.GetID()
		handler := trans.GetHandler()
		config := trans.GetConfig()

		// Handle scheduled transformations
		if config.Type == TransformationTypeScheduled {
			provider, ok := handler.(scheduleProvider)
			if !ok || provider == nil {
				continue
			}

			schedule := provider.GetSchedule()
			if schedule == "" {
				continue
			}

			interval, err := parseScheduleInterval(schedule)
			if err != nil {
				s.log.WithError(err).
					WithField("model_id", modelID).
					Error("Invalid schedule")
				continue
			}

			taskID := transformationTaskID(modelID, "scheduled")
			scheduledTasks = append(scheduledTasks, scheduledTask{
				ID:       taskID,
				Schedule: schedule,
				Interval: interval,
				Task:     asynq.NewTask(taskID, nil),
				Queue:    QueueName,
			})
			continue
		}

		// Handle incremental transformations
		if handler == nil {
			continue
		}

		cfg, ok := handler.Config().(*incremental.Config)
		if !ok || cfg.Schedules == nil {
			continue
		}

		// Forward fill
		if cfg.Schedules.ForwardFill != "" {
			interval, err := parseScheduleInterval(cfg.Schedules.ForwardFill)
			if err != nil {
				s.log.WithError(err).
					WithField("model_id", modelID).
					Error("Invalid forward fill schedule")
			} else {
				taskID := transformationTaskID(modelID, string(coordinator.DirectionForward))
				scheduledTasks = append(scheduledTasks, scheduledTask{
					ID:       taskID,
					Schedule: cfg.Schedules.ForwardFill,
					Interval: interval,
					Task:     asynq.NewTask(taskID, nil),
					Queue:    QueueName,
				})
			}
		}

		// Backfill
		if cfg.Schedules.Backfill != "" {
			interval, err := parseScheduleInterval(cfg.Schedules.Backfill)
			if err != nil {
				s.log.WithError(err).
					WithField("model_id", modelID).
					Error("Invalid backfill schedule")
			} else {
				taskID := transformationTaskID(modelID, string(coordinator.DirectionBack))
				scheduledTasks = append(scheduledTasks, scheduledTask{
					ID:       taskID,
					Schedule: cfg.Schedules.Backfill,
					Interval: interval,
					Task:     asynq.NewTask(taskID, nil),
					Queue:    QueueName,
				})
			}
		}
	}

	return scheduledTasks
}

// extractModelID extracts the model ID from a task type
// Example: "transformation:analytics.block_propagation:forward" -> "analytics.block_propagation"
func extractModelID(taskType string) string {
	// Only extract model ID from transformation tasks
	if !strings.HasPrefix(taskType, TransformationTaskPrefix) {
		// Not a transformation task (e.g., "consolidation")
		return ""
	}

	trimmed := strings.TrimPrefix(taskType, TransformationTaskPrefix)
	// strings.Split always returns at least one element, so parts[0] is safe.
	parts := strings.Split(trimmed, ":")

	return parts[0]
}

// extractExternalTaskComponents extracts the model ID from external task types.
// External tasks follow the format: external:{model_id}:suffix
// Returns the model ID and an error if the format is invalid.
func extractExternalTaskComponents(taskType string) (modelID string, err error) {
	parts := strings.Split(taskType, ":")
	if len(parts) != 3 {
		return "", fmt.Errorf("%w: %s", ErrInvalidExternalTaskType, taskType)
	}

	return parts[1], nil
}
