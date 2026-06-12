package handlers

import (
	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/api/generated"
	"github.com/gofiber/fiber/v3"
)

// ListTransformationCoverage handles GET /api/v1/models/transformations/coverage
func (s *Server) ListTransformationCoverage(c fiber.Ctx, params generated.ListTransformationCoverageParams) error {
	dag := s.modelsService.GetDAG()

	// Collect all matching model IDs first
	modelIDs := make([]string, 0)

	for _, node := range dag.GetTransformationNodes() {
		cfg := node.GetConfig()
		if !cfg.IsIncrementalType() {
			continue
		}

		if params.Database != nil && cfg.Database != *params.Database {
			continue
		}

		modelIDs = append(modelIDs, cfg.GetID())
	}

	// Batch fetch all processed ranges in a single query
	allRanges, err := s.adminService.GetAllProcessedRanges(c.Context(), modelIDs)
	if err != nil {
		s.log.WithError(err).Warn("Failed to batch get processed ranges")
		return fiber.NewError(fiber.StatusInternalServerError, "failed to get processed ranges")
	}

	coverage := make([]generated.CoverageSummary, 0, len(modelIDs))

	for _, modelID := range modelIDs {
		ranges := allRanges[modelID]
		coverage = append(coverage, generated.CoverageSummary{
			Id:     modelID,
			Ranges: mapRangesToAPI(ranges),
		})
	}

	return c.JSON(map[string]any{
		"coverage": coverage,
		"total":    len(coverage),
	})
}

// GetTransformationCoverage handles GET /api/v1/models/transformations/{id}/coverage
func (s *Server) GetTransformationCoverage(c fiber.Ctx, id string) error {
	// Verify it's an incremental transformation
	dag := s.modelsService.GetDAG()
	node, err := dag.GetTransformationNode(id)
	if err != nil {
		return ErrModelNotFound
	}

	cfg := node.GetConfig()
	if !cfg.IsIncrementalType() {
		return fiber.NewError(fiber.StatusBadRequest, "model is not incremental type")
	}

	// Get all processed ranges from admin service
	ranges, err := s.adminService.GetProcessedRanges(c.Context(), id)
	if err != nil {
		s.log.WithError(err).WithField("model_id", id).Error("Failed to get processed ranges")
		return fiber.NewError(fiber.StatusInternalServerError, "failed to get processed ranges")
	}

	detail := generated.CoverageDetail{
		Id:     id,
		Ranges: mapRangesToAPI(ranges),
	}

	return c.JSON(detail)
}

// mapRangesToAPI converts admin.ProcessedRange to API schema
func mapRangesToAPI(ranges []admin.ProcessedRange) []generated.Range {
	apiRanges := make([]generated.Range, len(ranges))
	for i, r := range ranges {
		apiRanges[i] = generated.Range{
			Position: toInt(r.Position),
			Interval: toInt(r.Interval),
		}
	}
	return apiRanges
}
