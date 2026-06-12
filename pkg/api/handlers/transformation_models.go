package handlers

import (
	"sort"

	"github.com/ethpandaops/cbt/pkg/api/generated"
	"github.com/ethpandaops/cbt/pkg/models/modelid"
	"github.com/gofiber/fiber/v3"
)

// ListTransformations handles GET /api/v1/models/transformations
func (s *Server) ListTransformations(c fiber.Ctx, params generated.ListTransformationsParams) error {
	dag := s.modelsService.GetDAG()
	transformationModels := make([]generated.TransformationModel, 0)
	overrideStatusByModelID, err := s.getConfigOverrideStatusMap(c.Context())
	if err != nil {
		s.log.WithError(err).Error("Failed to load config override status")
		return fiber.NewError(fiber.StatusInternalServerError, "failed to load config override status")
	}

	for _, transformationModel := range dag.GetTransformationNodes() {
		cfg := transformationModel.GetConfig()
		if params.Database != nil && cfg.Database != *params.Database {
			continue
		}

		// Filter by transformation type if provided
		if params.Type != nil {
			switch *params.Type {
			case generated.Scheduled:
				if !cfg.IsScheduledType() {
					continue
				}
			case generated.Incremental:
				if !cfg.IsIncrementalType() {
					continue
				}
			}
		}

		// Status filtering will be implemented when domain models support these fields
		_ = params.Status // nolint:staticcheck

		model := buildTransformationModel(cfg.GetID(), transformationModel, dag, overrideStatusByModelID[cfg.GetID()])
		transformationModels = append(transformationModels, model)
	}

	// Sort by ID for consistent ordering
	sort.Slice(transformationModels, func(i, j int) bool {
		return transformationModels[i].Id < transformationModels[j].Id
	})

	response := map[string]any{
		"models": transformationModels,
		"total":  len(transformationModels),
	}

	return c.Status(fiber.StatusOK).JSON(response)
}

// GetTransformation handles GET /api/v1/models/transformations/{id}
func (s *Server) GetTransformation(c fiber.Ctx, id string) error {
	// Validate model ID format (should be database.table)
	if _, _, err := modelid.Parse(id); err != nil {
		return ErrInvalidModelID
	}

	dag := s.modelsService.GetDAG()
	transformationNode, err := dag.GetTransformationNode(id)
	if err != nil {
		return ErrModelNotFound
	}

	overrideStatus, err := s.getConfigOverrideStatus(c.Context(), id)
	if err != nil {
		s.log.WithError(err).WithField("model_id", id).Error("Failed to load config override")
		return fiber.NewError(fiber.StatusInternalServerError, "failed to load config override status")
	}

	model := buildTransformationModel(id, transformationNode, dag, overrideStatus)
	return c.Status(fiber.StatusOK).JSON(model)
}
