package handlers

import (
	"sort"

	"github.com/ethpandaops/cbt/pkg/api/generated"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/modelid"
	"github.com/gofiber/fiber/v3"
)

// ListExternalModels handles GET /api/v1/models/external
func (s *Server) ListExternalModels(c fiber.Ctx, params generated.ListExternalModelsParams) error {
	dag := s.modelsService.GetDAG()
	externalModels := make([]generated.ExternalModel, 0)
	overrideStatusByModelID, err := s.getConfigOverrideStatusMap(c.Context())
	if err != nil {
		s.log.WithError(err).Error("Failed to load config override status")
		return fiber.NewError(fiber.StatusInternalServerError, "failed to load config override status")
	}

	for _, node := range dag.GetExternalNodes() {
		external, ok := node.Model.(models.External)
		if !ok {
			s.log.WithField("node", node).Debug("Node is not an external model")
			continue
		}

		cfg := external.GetConfig()
		if params.Database != nil && cfg.Database != *params.Database {
			continue
		}

		model := buildExternalModel(cfg.GetID(), external, dag, overrideStatusByModelID[cfg.GetID()])
		externalModels = append(externalModels, model)
	}

	// Sort by ID for consistent ordering
	sort.Slice(externalModels, func(i, j int) bool {
		return externalModels[i].Id < externalModels[j].Id
	})

	response := map[string]any{
		"models": externalModels,
		"total":  len(externalModels),
	}

	return c.Status(fiber.StatusOK).JSON(response)
}

// GetExternalModel handles GET /api/v1/models/external/{id}
func (s *Server) GetExternalModel(c fiber.Ctx, id string) error {
	// Validate model ID format (should be database.table)
	if _, _, err := modelid.Parse(id); err != nil {
		return ErrInvalidModelID
	}

	dag := s.modelsService.GetDAG()
	externalNode, err := dag.GetExternalNode(id)
	if err != nil {
		return ErrModelNotFound
	}

	overrideStatus, err := s.getConfigOverrideStatus(c.Context(), id)
	if err != nil {
		s.log.WithError(err).WithField("model_id", id).Error("Failed to load config override")
		return fiber.NewError(fiber.StatusInternalServerError, "failed to load config override status")
	}

	model := buildExternalModel(id, externalNode, dag, overrideStatus)
	return c.Status(fiber.StatusOK).JSON(model)
}
