package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ethpandaops/cbt/pkg/coordinator"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/transformation/incremental"
	"github.com/ethpandaops/cbt/pkg/observability"
	"github.com/ethpandaops/cbt/pkg/tasks"
	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
)

// registerAllHandlers registers all task handlers on the mux (called once at startup)
func (s *service) registerAllHandlers() {
	transformations := s.dag.GetTransformationNodes()
	externalModels := s.dag.GetExternalNodes()

	// Register transformation task handlers
	for _, trans := range transformations {
		s.registerTransformationHandlers(trans)
	}

	// Register external model task handlers
	for _, node := range externalModels {
		if model, ok := node.Model.(models.External); ok {
			s.registerExternalHandlers(model)
		}
	}

	// Register system task handlers
	s.registerSystemHandlers()
}

// registerTransformationHandlers registers task handlers for a single transformation
func (s *service) registerTransformationHandlers(trans models.Transformation) {
	config := trans.GetConfig()
	modelID := trans.GetID()
	handler := trans.GetHandler()

	// Handle scheduled transformations
	if config.Type == TransformationTypeScheduled {
		scheduledTask := transformationTaskID(modelID, "scheduled")
		s.mux.HandleFunc(scheduledTask, s.HandleScheduledTransformation)
		s.log.WithField("model_id", modelID).Debug("Registered scheduled transformation handler")
		return
	}

	// Handle incremental transformations
	if handler == nil {
		return
	}

	if _, ok := handler.Config().(*incremental.Config); !ok {
		return
	}

	// Always register both forward and backfill handlers regardless of current
	// schedule config. Schedules can be enabled at runtime via config overrides,
	// and the handlers themselves already guard against disabled schedules.
	forwardTask := transformationTaskID(modelID, string(coordinator.DirectionForward))
	s.mux.HandleFunc(forwardTask, s.HandleScheduledForward)
	s.log.WithField("model_id", modelID).Debug("Registered forward fill handler")

	backfillTask := transformationTaskID(modelID, string(coordinator.DirectionBack))
	s.mux.HandleFunc(backfillTask, s.HandleScheduledBackfill)
	s.log.WithField("model_id", modelID).Debug("Registered backfill handler")
}

// registerExternalHandlers registers task handlers for a single external model
func (s *service) registerExternalHandlers(model models.External) {
	config := model.GetConfig()
	modelID := model.GetID()

	if config.Cache == nil {
		return
	}

	if config.Cache.IncrementalScanInterval > 0 {
		incrementalTask := fmt.Sprintf("%s%s:incremental", ExternalTaskPrefix, modelID)
		s.mux.HandleFunc(incrementalTask, s.HandleExternalIncremental)
		s.log.WithField("model_id", modelID).Debug("Registered external incremental handler")
	}

	if config.Cache.FullScanInterval > 0 {
		fullTask := fmt.Sprintf("%s%s:full", ExternalTaskPrefix, modelID)
		s.mux.HandleFunc(fullTask, s.HandleExternalFull)
		s.log.WithField("model_id", modelID).Debug("Registered external full scan handler")
	}
}

// registerSystemHandlers registers system task handlers
func (s *service) registerSystemHandlers() {
	if s.cfg.Consolidation != "" {
		s.mux.HandleFunc(ConsolidationTaskType, s.HandleConsolidation)
		s.log.Debug("Registered consolidation handler")
	}
}

// HandleScheduledForward processes scheduled forward fill checks
func (s *service) HandleScheduledForward(_ context.Context, t *asynq.Task) error {
	modelID := extractModelID(t.Type())

	if s.isModelDisabled(modelID) {
		s.log.WithField("model_id", modelID).Debug("Skipping forward fill for live-disabled model")

		return nil
	}

	trans, err := s.dag.GetTransformationNode(modelID)
	if err != nil {
		s.log.WithError(err).WithField("model_id", modelID).Error("Failed to get transformation node")

		return err
	}

	s.log.WithField("model_id", modelID).Debug("Processing scheduled forward check")

	// This triggers the existing forward fill logic
	s.coordinator.Process(trans, coordinator.DirectionForward)

	// Record metrics
	observability.RecordScheduledTaskExecution(modelID, string(coordinator.DirectionForward), "success")

	return nil
}

// HandleScheduledBackfill processes scheduled backfill scans
func (s *service) HandleScheduledBackfill(_ context.Context, t *asynq.Task) error {
	modelID := extractModelID(t.Type())

	if s.isModelDisabled(modelID) {
		s.log.WithField("model_id", modelID).Debug("Skipping backfill for live-disabled model")

		return nil
	}

	trans, err := s.dag.GetTransformationNode(modelID)
	if err != nil {
		s.log.WithError(err).WithField("model_id", modelID).Error("Failed to get transformation node")

		return err
	}

	s.log.WithField("model_id", modelID).Debug("Processing scheduled backfill scan")

	// This triggers the existing backfill logic
	s.coordinator.Process(trans, coordinator.DirectionBack)

	// Record metrics
	observability.RecordScheduledTaskExecution(modelID, string(coordinator.DirectionBack), "success")

	return nil
}

// HandleExternalIncremental processes incremental scan for external model
func (s *service) HandleExternalIncremental(_ context.Context, t *asynq.Task) error {
	modelID, err := extractExternalTaskComponents(t.Type())
	if err != nil {
		return err
	}

	s.log.WithField("model_id", modelID).Debug("Running incremental scan for external model")

	// Process the incremental scan
	s.coordinator.ProcessExternalScan(modelID, tasks.ScanTypeIncremental)
	return nil
}

// HandleExternalFull processes full scan for external model
func (s *service) HandleExternalFull(_ context.Context, t *asynq.Task) error {
	modelID, err := extractExternalTaskComponents(t.Type())
	if err != nil {
		return err
	}

	s.log.WithField("model_id", modelID).Debug("Running full scan for external model")

	// Process the full scan
	s.coordinator.ProcessExternalScan(modelID, tasks.ScanTypeFull)
	return nil
}

// HandleConsolidation triggers admin table consolidation
func (s *service) HandleConsolidation(ctx context.Context, _ *asynq.Task) error {
	s.log.Info("Running admin table consolidation")

	// Call the coordinator to actually run the consolidation.
	// This ensures only one instance handles it at a time via asynq.
	s.coordinator.RunConsolidation(ctx)
	s.log.Info("Admin consolidation completed")

	return nil
}

// HandleScheduledTransformation handles the execution of scheduled (cron-based) transformations
func (s *service) HandleScheduledTransformation(ctx context.Context, t *asynq.Task) error {
	modelID := extractModelID(t.Type())

	if s.isModelDisabled(modelID) {
		s.log.WithField("model_id", modelID).Debug("Skipping scheduled transformation for live-disabled model")

		return nil
	}

	trans, err := s.dag.GetTransformationNode(modelID)
	if err != nil {
		s.log.WithError(err).WithField("model_id", modelID).Error("Failed to get transformation node")
		return err
	}

	config := trans.GetConfig()

	// Verify this is a scheduled transformation
	if !config.IsScheduledType() {
		s.log.WithField("model_id", modelID).Error("Task is not a scheduled transformation")
		return fmt.Errorf("%w: %s", ErrNotScheduledType, modelID)
	}

	currentTime := time.Now()
	s.log.WithFields(logrus.Fields{
		"model_id":       modelID,
		"type":           "scheduled",
		"execution_time": currentTime.Format(time.RFC3339),
	}).Debug("Processing scheduled transformation")

	// Enqueue the scheduled run through the coordinator. A duplicate in-flight
	// run is not an error: the existing run is already covering this tick, so
	// skip without failing the handler (preserves the prior dedup behavior).
	if err := s.coordinator.TriggerScheduledRun(ctx, modelID); err != nil {
		if errors.Is(err, coordinator.ErrScheduledRunInProgress) {
			s.log.WithField("model_id", modelID).Debug("Scheduled run already in progress, skipping")

			return nil
		}

		s.log.WithError(err).WithField("model_id", modelID).Error("Failed to enqueue scheduled transformation task")

		return err
	}

	s.log.WithFields(logrus.Fields{
		"model_id":       modelID,
		"execution_time": currentTime.Format(time.RFC3339),
	}).Info("Enqueued scheduled transformation task")

	return nil
}

// isModelDisabled checks if a model is live-disabled via config overrides.
func (s *service) isModelDisabled(modelID string) bool {
	if s.liveOverrides == nil {
		return false
	}

	return s.liveOverrides.IsModelDisabled(modelID)
}
