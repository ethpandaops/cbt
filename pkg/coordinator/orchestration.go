package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
)

// RunConsolidation performs admin table consolidation for all models
func (s *service) RunConsolidation(ctx context.Context) {
	transformations := s.dag.GetTransformationNodes()
	for _, transformation := range transformations {
		modelID := transformation.GetID()

		// Try to consolidate
		consolidated, err := s.admin.ConsolidateHistoricalData(ctx, modelID)
		if err != nil {
			if consolidated > 0 {
				s.log.WithError(err).WithField("model_id", modelID).Debug("Consolidation partially succeeded")
			}
			continue
		}

		if consolidated > 0 {
			s.log.WithFields(logrus.Fields{
				"model_id":          modelID,
				"rows_consolidated": consolidated,
			}).Info("Consolidated admin.cbt rows")
		}
	}
}

// ProcessBoundsOrchestration checks all external models and enqueues bounds update tasks as needed
func (s *service) ProcessBoundsOrchestration(ctx context.Context) {
	// Get all external models from DAG
	externalNodes := s.dag.GetExternalNodes()

	if len(externalNodes) == 0 {
		s.log.Debug("No external models to check for bounds updates")
		return
	}

	now := time.Now()

	for _, node := range externalNodes {
		model, ok := node.Model.(models.External)
		if !ok {
			s.log.WithField("node_type", node.NodeType).Error("Invalid external node type")
			continue
		}
		modelID := model.GetID()
		config := model.GetConfig()

		// Skip if cache config not defined
		if config.Cache == nil {
			s.log.WithField("model_id", modelID).Debug("Skipping model without cache config")
			continue
		}

		// Get current cache entry
		cache, err := s.admin.GetExternalBounds(ctx, modelID)
		if err != nil {
			s.log.WithError(err).WithField("model_id", modelID).Error("Failed to get cache bounds")
			continue
		}

		// Determine if update is needed
		needsUpdate := false

		if cache == nil {
			// No cache entry - needs initial full scan
			s.log.WithField("model_id", modelID).Debug("No cache entry, needs initial bounds")
			needsUpdate = true
		} else {
			// Check if incremental or full scan is due
			if now.Sub(cache.LastFullScan) > config.Cache.FullScanInterval {
				s.log.WithFields(logrus.Fields{
					"model_id":       modelID,
					"last_full_scan": cache.LastFullScan,
					"interval":       config.Cache.FullScanInterval,
				}).Debug("Full scan interval exceeded")
				needsUpdate = true
			} else if now.Sub(cache.LastIncrementalScan) > config.Cache.IncrementalScanInterval {
				s.log.WithFields(logrus.Fields{
					"model_id":              modelID,
					"last_incremental_scan": cache.LastIncrementalScan,
					"interval":              config.Cache.IncrementalScanInterval,
				}).Debug("Incremental scan interval exceeded")
				needsUpdate = true
			}
		}

		if needsUpdate {
			// Enqueue bounds update task
			s.enqueueBoundsUpdate(ctx, modelID)
		}
	}
}

// enqueueBoundsUpdate enqueues a bounds cache update task for an external model
func (s *service) enqueueBoundsUpdate(_ context.Context, modelID string) {
	// Create unique task ID
	taskID := fmt.Sprintf("bounds:cache:%s", modelID)

	// Create task payload
	payload := map[string]string{
		"model_id": modelID,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		s.log.WithError(err).WithField("model_id", modelID).Error("Failed to marshal bounds task payload")
		return
	}

	// Create task with unique ID to prevent duplicates
	task := asynq.NewTask(BoundsCacheTaskType, payloadBytes,
		asynq.TaskID(taskID),
		asynq.Queue(modelID),
		asynq.MaxRetry(0),
		asynq.Timeout(5*time.Minute),
	)

	// Enqueue the task
	if _, err := s.queueManager.Enqueue(task); err != nil {
		if strings.Contains(err.Error(), "task ID conflicts with another task") || strings.Contains(err.Error(), "task already exists") {
			s.log.WithField("model_id", modelID).Warn("Bounds update task already exists")
		} else {
			s.log.WithError(err).WithField("model_id", modelID).Error("Failed to enqueue bounds update task")
		}

		return
	}

	s.log.WithField("model_id", modelID).Debug("Enqueued bounds update task")
}
