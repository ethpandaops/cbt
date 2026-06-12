package handlers

import (
	"github.com/ethpandaops/cbt/pkg/api/generated"
	"github.com/gofiber/fiber/v3"
)

// ListScheduledRuns handles GET /api/v1/models/transformations/runs
func (s *Server) ListScheduledRuns(c fiber.Ctx, params generated.ListScheduledRunsParams) error {
	dag := s.modelsService.GetDAG()

	// Collect all matching model IDs first
	modelIDs := make([]string, 0)

	for _, node := range dag.GetTransformationNodes() {
		cfg := node.GetConfig()
		if !cfg.IsScheduledType() {
			continue
		}

		if params.Database != nil && cfg.Database != *params.Database {
			continue
		}

		modelIDs = append(modelIDs, cfg.GetID())
	}

	// Batch fetch all last executions in a single query
	allExecutions, err := s.adminService.GetAllLastScheduledExecutions(c.Context(), modelIDs)
	if err != nil {
		s.log.WithError(err).Warn("Failed to batch get last scheduled executions")
		return fiber.NewError(fiber.StatusInternalServerError, "failed to get last scheduled executions")
	}

	runs := make([]generated.ScheduledRun, 0, len(modelIDs))

	for _, modelID := range modelIDs {
		run := generated.ScheduledRun{
			Id: modelID,
		}
		if lastRun := allExecutions[modelID]; lastRun != nil {
			run.LastRun = lastRun
		}

		runs = append(runs, run)
	}

	return c.JSON(map[string]any{
		"runs":  runs,
		"total": len(runs),
	})
}

// GetScheduledRun handles GET /api/v1/models/transformations/{id}/runs
func (s *Server) GetScheduledRun(c fiber.Ctx, id string) error {
	// Verify it's a scheduled transformation
	dag := s.modelsService.GetDAG()
	node, err := dag.GetTransformationNode(id)
	if err != nil {
		return ErrModelNotFound
	}

	cfg := node.GetConfig()
	if !cfg.IsScheduledType() {
		return fiber.NewError(fiber.StatusBadRequest, "model is not scheduled type")
	}

	// Get last run timestamp from admin service
	lastRun, err := s.adminService.GetLastScheduledExecution(c.Context(), id)
	if err != nil {
		s.log.WithError(err).WithField("model_id", id).Error("Failed to get last scheduled execution")
		return fiber.NewError(fiber.StatusInternalServerError, "failed to get last scheduled execution")
	}

	run := generated.ScheduledRun{
		Id: id,
	}
	if lastRun != nil {
		run.LastRun = lastRun
	}

	return c.JSON(run)
}
