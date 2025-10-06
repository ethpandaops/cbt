package handlers

import (
	"strings"

	"github.com/ethpandaops/cbt/pkg/api/generated"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/gofiber/fiber/v3"
)

// GetModels handles GET /api/v1/models
func (s *Server) GetModels(c fiber.Ctx, params generated.GetModelsParams) error {
	dag := s.modelsService.GetDAG()
	var modelDetails []generated.ModelDetail

	// Get transformation models
	if params.Type == nil || *params.Type == generated.Transformation {
		for _, transformationModel := range dag.GetTransformationNodes() {
			cfg := transformationModel.GetConfig()
			if params.Database != nil && cfg.Database != *params.Database {
				continue
			}

			modelID := cfg.GetID()

			// Build config map
			configMap := make(map[string]interface{})
			configMap["type"] = string(cfg.Type)
			configMap["database"] = cfg.Database
			configMap["table"] = cfg.Table
			if cfg.Env != nil {
				configMap["env"] = cfg.Env
			}

			// Get dependencies and dependents
			deps := dag.GetDependencies(modelID)
			dependents := dag.GetDependents(modelID)

			detail := generated.ModelDetail{
				Id:           modelID,
				Type:         "transformation",
				Database:     cfg.Database,
				Table:        cfg.Table,
				Config:       configMap,
				Dependencies: &deps,
				Dependents:   &dependents,
			}

			modelDetails = append(modelDetails, detail)
		}
	}

	// Get external models
	if params.Type == nil || *params.Type == generated.External {
		for _, node := range dag.GetExternalNodes() {
			external, ok := node.Model.(models.External)
			if !ok {
				s.log.WithField("node", node).Debug("Node is not an external model")
				continue
			}

			externalNode, err := dag.GetExternalNode(external.GetID())
			if err != nil {
				s.log.WithError(err).WithField("model_id", external.GetID()).Debug("Failed to get external node")
				continue
			}

			cfg := externalNode.GetConfig()
			if params.Database != nil && cfg.Database != *params.Database {
				continue
			}

			modelID := cfg.GetID()

			// Build config map
			configMap := make(map[string]interface{})
			configMap["database"] = cfg.Database
			configMap["table"] = cfg.Table

			// External models can still have dependents
			dependents := dag.GetDependents(modelID)

			detail := generated.ModelDetail{
				Id:         modelID,
				Type:       "external",
				Database:   cfg.Database,
				Table:      cfg.Table,
				Config:     configMap,
				Dependents: &dependents,
			}

			modelDetails = append(modelDetails, detail)
		}
	}

	total := len(modelDetails)

	response := generated.ModelsResponse{
		Models: modelDetails,
		Total:  total,
	}

	return c.Status(fiber.StatusOK).JSON(response)
}

// GetModelByID handles GET /api/v1/models/{model_id}
func (s *Server) GetModelByID(c fiber.Ctx, modelID string) error {
	// Validate model ID format (should be database.table)
	if !strings.Contains(modelID, ".") {
		return ErrInvalidModelID
	}

	dag := s.modelsService.GetDAG()
	_, err := dag.GetNode(modelID)
	if err != nil {
		return ErrModelNotFound
	}

	// Try to get transformation node
	transformationNode, transformErr := dag.GetTransformationNode(modelID)
	if transformErr == nil {
		detail := buildTransformationDetail(modelID, transformationNode, dag)
		return c.Status(fiber.StatusOK).JSON(detail)
	}

	// Try to get external node
	externalNode, externalErr := dag.GetExternalNode(modelID)
	if externalErr == nil {
		detail := buildExternalDetail(modelID, externalNode, dag)
		return c.Status(fiber.StatusOK).JSON(detail)
	}

	// Model exists but couldn't determine type
	return ErrModelNotFound
}

func buildTransformationDetail(modelID string, node models.Transformation, dag models.DAGReader) generated.ModelDetail {
	cfg := node.GetConfig()
	configMap := make(map[string]interface{})
	configMap["type"] = string(cfg.Type)
	configMap["database"] = cfg.Database
	configMap["table"] = cfg.Table
	if cfg.Env != nil {
		configMap["env"] = cfg.Env
	}

	deps := dag.GetDependencies(modelID)
	dependents := dag.GetDependents(modelID)

	return generated.ModelDetail{
		Id:           modelID,
		Type:         "transformation",
		Database:     cfg.Database,
		Table:        cfg.Table,
		Config:       configMap,
		Dependencies: &deps,
		Dependents:   &dependents,
	}
}

func buildExternalDetail(modelID string, node models.External, dag models.DAGReader) generated.ModelDetail {
	cfg := node.GetConfig()
	configMap := make(map[string]interface{})
	configMap["database"] = cfg.Database
	configMap["table"] = cfg.Table

	dependents := dag.GetDependents(modelID)

	detail := generated.ModelDetail{
		Id:       modelID,
		Type:     "external",
		Database: cfg.Database,
		Table:    cfg.Table,
		Config:   configMap,
	}

	if len(dependents) > 0 {
		detail.Dependents = &dependents
	}

	return detail
}
