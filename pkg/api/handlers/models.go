package handlers

import (
	"sort"
	"strings"

	"github.com/ethpandaops/cbt/pkg/api/generated"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/gofiber/fiber/v3"
)

// ListAllModels handles GET /api/v1/models
func (s *Server) ListAllModels(c fiber.Ctx, params generated.ListAllModelsParams) error {
	dag := s.modelsService.GetDAG()
	summaries := make([]generated.ModelSummary, 0)

	// Get transformation models
	if s.shouldIncludeTransformations(params.Type) {
		summaries = s.appendTransformationSummaries(summaries, dag, params)
	}

	// Get external models
	if s.shouldIncludeExternals(params.Type) {
		summaries = s.appendExternalSummaries(summaries, dag, params)
	}

	// Sort by ID for consistent ordering
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].Id < summaries[j].Id
	})

	response := map[string]interface{}{
		"models": summaries,
		"total":  len(summaries),
	}

	return c.Status(fiber.StatusOK).JSON(response)
}

func (s *Server) shouldIncludeTransformations(typeParam *generated.ListAllModelsParamsType) bool {
	return typeParam == nil || *typeParam == generated.ListAllModelsParamsTypeTransformation
}

func (s *Server) shouldIncludeExternals(typeParam *generated.ListAllModelsParamsType) bool {
	return typeParam == nil || *typeParam == generated.ListAllModelsParamsTypeExternal
}

func (s *Server) appendTransformationSummaries(summaries []generated.ModelSummary, dag models.DAGReader, params generated.ListAllModelsParams) []generated.ModelSummary {
	for _, transformationModel := range dag.GetTransformationNodes() {
		cfg := transformationModel.GetConfig()
		if params.Database != nil && cfg.Database != *params.Database {
			continue
		}

		summary := generated.ModelSummary{
			Id:       cfg.GetID(),
			Type:     generated.ModelSummaryTypeTransformation,
			Database: cfg.Database,
			Table:    cfg.Table,
		}

		if s.matchesSearch(summary.Id, params.Search) {
			summaries = append(summaries, summary)
		}
	}
	return summaries
}

func (s *Server) appendExternalSummaries(summaries []generated.ModelSummary, dag models.DAGReader, params generated.ListAllModelsParams) []generated.ModelSummary {
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

		summary := generated.ModelSummary{
			Id:       cfg.GetID(),
			Type:     generated.ModelSummaryTypeExternal,
			Database: cfg.Database,
			Table:    cfg.Table,
		}

		if s.matchesSearch(summary.Id, params.Search) {
			summaries = append(summaries, summary)
		}
	}
	return summaries
}

func (s *Server) matchesSearch(id string, search *string) bool {
	if search == nil {
		return true
	}
	searchTerm := strings.ToLower(*search)
	modelID := strings.ToLower(id)
	return strings.Contains(modelID, searchTerm)
}

// ListExternalModels handles GET /api/v1/models/external
func (s *Server) ListExternalModels(c fiber.Ctx, params generated.ListExternalModelsParams) error {
	dag := s.modelsService.GetDAG()
	externalModels := make([]generated.ExternalModel, 0)

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

		model := buildExternalModel(cfg.GetID(), external, dag)
		externalModels = append(externalModels, model)
	}

	// Sort by ID for consistent ordering
	sort.Slice(externalModels, func(i, j int) bool {
		return externalModels[i].Id < externalModels[j].Id
	})

	response := map[string]interface{}{
		"models": externalModels,
		"total":  len(externalModels),
	}

	return c.Status(fiber.StatusOK).JSON(response)
}

// GetExternalModel handles GET /api/v1/models/external/{id}
func (s *Server) GetExternalModel(c fiber.Ctx, id string) error {
	// Validate model ID format (should be database.table)
	if !strings.Contains(id, ".") {
		return ErrInvalidModelID
	}

	dag := s.modelsService.GetDAG()
	externalNode, err := dag.GetExternalNode(id)
	if err != nil {
		return ErrModelNotFound
	}

	model := buildExternalModel(id, externalNode, dag)
	return c.Status(fiber.StatusOK).JSON(model)
}

// ListTransformations handles GET /api/v1/models/transformations
func (s *Server) ListTransformations(c fiber.Ctx, params generated.ListTransformationsParams) error {
	dag := s.modelsService.GetDAG()
	transformationModels := make([]generated.TransformationModel, 0)

	for _, transformationModel := range dag.GetTransformationNodes() {
		cfg := transformationModel.GetConfig()
		if params.Database != nil && cfg.Database != *params.Database {
			continue
		}

		// Type and status filtering will be implemented when domain models support these fields
		_ = params.Type   // nolint:staticcheck
		_ = params.Status // nolint:staticcheck

		model := buildTransformationModel(cfg.GetID(), transformationModel, dag)
		transformationModels = append(transformationModels, model)
	}

	// Sort by ID for consistent ordering
	sort.Slice(transformationModels, func(i, j int) bool {
		return transformationModels[i].Id < transformationModels[j].Id
	})

	response := map[string]interface{}{
		"models": transformationModels,
		"total":  len(transformationModels),
	}

	return c.Status(fiber.StatusOK).JSON(response)
}

// GetTransformation handles GET /api/v1/models/transformations/{id}
func (s *Server) GetTransformation(c fiber.Ctx, id string) error {
	// Validate model ID format (should be database.table)
	if !strings.Contains(id, ".") {
		return ErrInvalidModelID
	}

	dag := s.modelsService.GetDAG()
	transformationNode, err := dag.GetTransformationNode(id)
	if err != nil {
		return ErrModelNotFound
	}

	model := buildTransformationModel(id, transformationNode, dag)
	return c.Status(fiber.StatusOK).JSON(model)
}

// buildExternalModel constructs an ExternalModel from domain model
func buildExternalModel(modelID string, node models.External, _ models.DAGReader) generated.ExternalModel {
	cfg := node.GetConfig()

	model := generated.ExternalModel{
		Id:       modelID,
		Database: cfg.Database,
		Table:    cfg.Table,
	}

	// Populate cache configuration if available
	if cfg.Cache != nil {
		incrementalInterval := cfg.Cache.IncrementalScanInterval.String()
		fullInterval := cfg.Cache.FullScanInterval.String()

		model.Cache = &struct {
			FullScanInterval        *string `json:"full_scan_interval,omitempty"`
			IncrementalScanInterval *string `json:"incremental_scan_interval,omitempty"`
		}{
			IncrementalScanInterval: &incrementalInterval,
			FullScanInterval:        &fullInterval,
		}
	}

	// Populate lag if set
	if cfg.Lag > 0 {
		// Safe conversion from uint64 to int
		lag := int(cfg.Lag) // nolint:gosec
		model.Lag = &lag
	}

	return model
}

// buildTransformationModel constructs a TransformationModel from domain model
func buildTransformationModel(modelID string, node models.Transformation, dag models.DAGReader) generated.TransformationModel {
	cfg := node.GetConfig()

	model := generated.TransformationModel{
		Id:       modelID,
		Database: cfg.Database,
		Table:    cfg.Table,
		Content:  node.GetValue(),
	}

	// Determine content type based on node type
	nodeType := node.GetType()
	switch nodeType {
	case "sql":
		model.ContentType = generated.TransformationModelContentTypeSql
	case "exec":
		model.ContentType = generated.TransformationModelContentTypeExec
	default:
		// Default to SQL for unknown types
		model.ContentType = generated.TransformationModelContentTypeSql
	}

	// Map transformation type
	if cfg.IsScheduledType() {
		model.Type = generated.TransformationModelTypeScheduled
		populateScheduledFields(&model, node)
	} else {
		model.Type = generated.TransformationModelTypeIncremental
		populateIncrementalFields(&model, node)
	}

	// Get dependencies
	deps := dag.GetDependencies(modelID)
	if len(deps) > 0 {
		model.DependsOn = &deps
	}

	return model
}

func populateScheduledFields(model *generated.TransformationModel, node models.Transformation) {
	handler := node.GetHandler()
	if scheduledHandler, ok := handler.(interface {
		GetSchedule() string
		GetTags() []string
	}); ok {
		schedule := scheduledHandler.GetSchedule()
		model.Schedule = &schedule

		tags := scheduledHandler.GetTags()
		if len(tags) > 0 {
			model.Tags = &tags
		}
	}
}

func populateIncrementalFields(model *generated.TransformationModel, node models.Transformation) {
	handler := node.GetHandler()
	if incrementalHandler, ok := handler.(interface {
		GetInterval() (minInterval uint64, maxInterval uint64)
		GetSchedules() (forwardfill string, backfill string)
		GetLimits() (minLimit uint64, maxLimit uint64)
		GetTags() []string
		GetDependencies() []string
	}); ok {
		// Interval
		minInterval, maxInterval := incrementalHandler.GetInterval()
		model.Interval = &struct {
			Max *int `json:"max,omitempty"`
			Min *int `json:"min,omitempty"`
		}{
			Min: intPtr(int(minInterval)), // nolint:gosec
			Max: intPtr(int(maxInterval)), // nolint:gosec
		}

		// Schedules
		forwardfill, backfill := incrementalHandler.GetSchedules()
		if forwardfill != "" || backfill != "" {
			model.Schedules = &struct {
				Backfill    *string `json:"backfill,omitempty"`
				Forwardfill *string `json:"forwardfill,omitempty"`
			}{
				Forwardfill: stringPtr(forwardfill),
				Backfill:    stringPtr(backfill),
			}
		}

		// Limits
		minLimit, maxLimit := incrementalHandler.GetLimits()
		if minLimit > 0 || maxLimit > 0 {
			model.Limits = &struct {
				Max *int `json:"max,omitempty"`
				Min *int `json:"min,omitempty"`
			}{
				Min: intPtr(int(minLimit)), // nolint:gosec
				Max: intPtr(int(maxLimit)), // nolint:gosec
			}
		}

		// Tags
		tags := incrementalHandler.GetTags()
		if len(tags) > 0 {
			model.Tags = &tags
		}
	}
}

func intPtr(i int) *int {
	if i == 0 {
		return nil
	}
	return &i
}

func stringPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
