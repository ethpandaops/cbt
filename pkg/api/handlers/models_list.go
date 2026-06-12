package handlers

import (
	"context"
	"sort"
	"strings"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/api/generated"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/gofiber/fiber/v3"
)

type configOverrideStatus struct {
	hasOverride bool
	isDisabled  bool
}

// ListAllModels handles GET /api/v1/models
func (s *Server) ListAllModels(c fiber.Ctx, params generated.ListAllModelsParams) error {
	dag := s.modelsService.GetDAG()
	summaries := make([]generated.ModelSummary, 0)
	overrideStatusByModelID, err := s.getConfigOverrideStatusMap(c.Context())
	if err != nil {
		s.log.WithError(err).Error("Failed to load config override status")
		return fiber.NewError(fiber.StatusInternalServerError, "failed to load config override status")
	}

	// Get transformation models
	if s.shouldIncludeTransformations(params.Type) {
		summaries = s.appendTransformationSummaries(summaries, dag, params, overrideStatusByModelID)
	}

	// Get external models
	if s.shouldIncludeExternals(params.Type) {
		summaries = s.appendExternalSummaries(summaries, dag, params, overrideStatusByModelID)
	}

	// Sort by ID for consistent ordering
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].Id < summaries[j].Id
	})

	response := map[string]any{
		"models": summaries,
		"total":  len(summaries),
	}

	return c.Status(fiber.StatusOK).JSON(response)
}

func (s *Server) shouldIncludeTransformations(typeParam *generated.ListAllModelsParamsType) bool {
	return typeParam == nil || *typeParam == generated.Transformation
}

func (s *Server) shouldIncludeExternals(typeParam *generated.ListAllModelsParamsType) bool {
	return typeParam == nil || *typeParam == generated.External
}

// appendSummary builds a ModelSummary and appends it when it matches the
// database filter and the search term. It is the shared core of the
// transformation and external summary builders.
func (s *Server) appendSummary(
	summaries []generated.ModelSummary,
	id, database, table string,
	summaryType generated.ModelSummaryType,
	params generated.ListAllModelsParams,
	overrideStatusByModelID map[string]configOverrideStatus,
) []generated.ModelSummary {
	if params.Database != nil && database != *params.Database {
		return summaries
	}

	overrideStatus := overrideStatusByModelID[id]

	summary := generated.ModelSummary{
		Id:          id,
		Type:        summaryType,
		Database:    database,
		Table:       table,
		HasOverride: new(overrideStatus.hasOverride),
		IsDisabled:  new(overrideStatus.isDisabled),
	}

	if s.matchesSearch(summary.Id, params.Search) {
		summaries = append(summaries, summary)
	}

	return summaries
}

func (s *Server) appendTransformationSummaries(
	summaries []generated.ModelSummary,
	dag models.DAGReader,
	params generated.ListAllModelsParams,
	overrideStatusByModelID map[string]configOverrideStatus,
) []generated.ModelSummary {
	for _, transformationModel := range dag.GetTransformationNodes() {
		cfg := transformationModel.GetConfig()
		summaries = s.appendSummary(
			summaries,
			cfg.GetID(), cfg.Database, cfg.Table,
			generated.ModelSummaryTypeTransformation,
			params,
			overrideStatusByModelID,
		)
	}
	return summaries
}

func (s *Server) appendExternalSummaries(
	summaries []generated.ModelSummary,
	dag models.DAGReader,
	params generated.ListAllModelsParams,
	overrideStatusByModelID map[string]configOverrideStatus,
) []generated.ModelSummary {
	for _, node := range dag.GetExternalNodes() {
		external, ok := node.Model.(models.External)
		if !ok {
			s.log.WithField("node", node).Debug("Node is not an external model")
			continue
		}

		cfg := external.GetConfig()
		summaries = s.appendSummary(
			summaries,
			cfg.GetID(), cfg.Database, cfg.Table,
			generated.ModelSummaryTypeExternal,
			params,
			overrideStatusByModelID,
		)
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

func configOverrideStatusFromModel(override *admin.ConfigOverride) configOverrideStatus {
	if override == nil {
		return configOverrideStatus{}
	}

	return configOverrideStatus{
		hasOverride: true,
		isDisabled:  override.Enabled != nil && !*override.Enabled,
	}
}

func (s *Server) getConfigOverrideStatusMap(ctx context.Context) (map[string]configOverrideStatus, error) {
	if s.adminService == nil {
		return map[string]configOverrideStatus{}, nil
	}

	overrides, err := s.adminService.GetAllConfigOverrides(ctx)
	if err != nil {
		return nil, err
	}

	overrideStatusByModelID := make(map[string]configOverrideStatus, len(overrides))
	for _, override := range overrides {
		overrideCopy := override
		overrideStatusByModelID[override.ModelID] = configOverrideStatusFromModel(&overrideCopy)
	}

	return overrideStatusByModelID, nil
}

func (s *Server) getConfigOverrideStatus(ctx context.Context, modelID string) (configOverrideStatus, error) {
	if s.adminService == nil {
		return configOverrideStatus{}, nil
	}

	override, err := s.adminService.GetConfigOverride(ctx, modelID)
	if err != nil {
		return configOverrideStatus{}, err
	}

	return configOverrideStatusFromModel(override), nil
}
