package handlers

import (
	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/api/generated"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/gofiber/fiber/v3"
)

// ListExternalBounds handles GET /api/v1/models/external/bounds
func (s *Server) ListExternalBounds(c fiber.Ctx) error {
	dag := s.modelsService.GetDAG()
	bounds := make([]generated.ExternalBounds, 0)

	for _, node := range dag.GetExternalNodes() {
		external, ok := node.Model.(models.External)
		if !ok {
			continue
		}

		modelID := external.GetID()

		// Get bounds from Redis cache via admin service
		boundsCache, err := s.adminService.GetExternalBounds(c.Context(), modelID)
		if err != nil {
			s.log.WithError(err).WithField("model_id", modelID).Debug("No bounds cache found")
			continue
		}
		if boundsCache == nil {
			s.log.WithField("model_id", modelID).Debug("Bounds cache is nil")
			continue
		}

		bounds = append(bounds, mapBoundsCacheToAPI(modelID, boundsCache))
	}

	return c.JSON(map[string]any{
		"bounds": bounds,
		"total":  len(bounds),
	})
}

// GetExternalBounds handles GET /api/v1/models/external/{id}/bounds
func (s *Server) GetExternalBounds(c fiber.Ctx, id string) error {
	boundsCache, err := s.adminService.GetExternalBounds(c.Context(), id)
	if err != nil {
		s.log.WithError(err).WithField("model_id", id).Error("Failed to get external bounds")
		return fiber.NewError(fiber.StatusInternalServerError, "failed to get external bounds")
	}
	if boundsCache == nil {
		return ErrModelNotFound
	}

	return c.JSON(mapBoundsCacheToAPI(id, boundsCache))
}

// mapBoundsCacheToAPI converts BoundsCache to API schema
func mapBoundsCacheToAPI(id string, cache *admin.BoundsCache) generated.ExternalBounds {
	bounds := generated.ExternalBounds{
		Id:  id,
		Min: toInt(cache.Min),
		Max: toInt(cache.Max),
	}

	if cache.PreviousMin != 0 {
		prevMin := toInt(cache.PreviousMin)
		bounds.PreviousMin = &prevMin
	}
	if cache.PreviousMax != 0 {
		prevMax := toInt(cache.PreviousMax)
		bounds.PreviousMax = &prevMax
	}
	if !cache.LastIncrementalScan.IsZero() {
		bounds.LastIncrementalScan = &cache.LastIncrementalScan
	}
	if !cache.LastFullScan.IsZero() {
		bounds.LastFullScan = &cache.LastFullScan
	}
	// Always set InitialScanComplete since it's a bool
	bounds.InitialScanComplete = &cache.InitialScanComplete
	if cache.InitialScanStarted != nil {
		bounds.InitialScanStarted = cache.InitialScanStarted
	}

	return bounds
}
