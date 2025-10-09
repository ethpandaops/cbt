package handlers

import (
	"sort"
	"strings"

	"github.com/ethpandaops/cbt/pkg/admin"
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

	// Populate interval configuration if available
	if intervalProvider, ok := node.(interface{ GetIntervalType() string }); ok {
		intervalType := intervalProvider.GetIntervalType()
		if intervalType != "" {
			model.Interval = &struct {
				Type *string `json:"type,omitempty"`
			}{
				Type: &intervalType,
			}
		}
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
		GetInterval() (minInterval, maxInterval uint64)
		GetSchedules() (forwardfill, backfill string)
		GetTags() []string
		GetFlattenedDependencies() []string
	}); ok {
		// Interval
		minInterval, maxInterval := incrementalHandler.GetInterval()

		// Get interval type if available
		var intervalType *string
		if intervalProvider, ok := handler.(interface{ GetIntervalType() string }); ok {
			if it := intervalProvider.GetIntervalType(); it != "" {
				intervalType = &it
			}
		}

		model.Interval = &struct {
			Max  *int    `json:"max,omitempty"`
			Min  *int    `json:"min,omitempty"`
			Type *string `json:"type,omitempty"`
		}{
			Min:  intPtr(int(minInterval)), // nolint:gosec
			Max:  intPtr(int(maxInterval)), // nolint:gosec
			Type: intervalType,
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
		if limitsProvider, ok := handler.(interface{ GetLimits() *struct{ Min, Max uint64 } }); ok {
			limits := limitsProvider.GetLimits()
			if limits != nil && (limits.Min > 0 || limits.Max > 0) {
				model.Limits = &struct {
					Max *int `json:"max,omitempty"`
					Min *int `json:"min,omitempty"`
				}{
					Min: intPtr(int(limits.Min)), // nolint:gosec
					Max: intPtr(int(limits.Max)), // nolint:gosec
				}
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

	return c.JSON(map[string]interface{}{
		"bounds": bounds,
		"total":  len(bounds),
	})
}

// GetExternalBounds handles GET /api/v1/models/external/{id}/bounds
func (s *Server) GetExternalBounds(c fiber.Ctx, id string) error {
	boundsCache, err := s.adminService.GetExternalBounds(c.Context(), id)
	if err != nil {
		return err
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
		Min: int(cache.Min), // nolint:gosec
		Max: int(cache.Max), // nolint:gosec
	}

	if cache.PreviousMin != 0 {
		prevMin := int(cache.PreviousMin) // nolint:gosec
		bounds.PreviousMin = &prevMin
	}
	if cache.PreviousMax != 0 {
		prevMax := int(cache.PreviousMax) // nolint:gosec
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

// ListTransformationCoverage handles GET /api/v1/models/transformations/coverage
func (s *Server) ListTransformationCoverage(c fiber.Ctx, params generated.ListTransformationCoverageParams) error {
	dag := s.modelsService.GetDAG()
	coverage := make([]generated.CoverageSummary, 0)

	for _, node := range dag.GetTransformationNodes() {
		cfg := node.GetConfig()
		if !cfg.IsIncrementalType() {
			continue // Skip scheduled transformations
		}

		// Filter by database if provided
		if params.Database != nil && cfg.Database != *params.Database {
			continue
		}

		modelID := cfg.GetID()

		// Get all processed ranges from admin service
		ranges, err := s.adminService.GetProcessedRanges(c.Context(), modelID)
		if err != nil {
			s.log.WithError(err).WithField("model_id", modelID).Error("Failed to get processed ranges")
			continue
		}

		coverage = append(coverage, generated.CoverageSummary{
			Id:     modelID,
			Ranges: mapRangesToAPI(ranges),
		})
	}

	return c.JSON(map[string]interface{}{
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
		return err
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
			Position: int(r.Position), // nolint:gosec
			Interval: int(r.Interval), // nolint:gosec
		}
	}
	return apiRanges
}

// ListScheduledRuns handles GET /api/v1/models/transformations/runs
func (s *Server) ListScheduledRuns(c fiber.Ctx, params generated.ListScheduledRunsParams) error {
	dag := s.modelsService.GetDAG()
	runs := make([]generated.ScheduledRun, 0)

	for _, node := range dag.GetTransformationNodes() {
		cfg := node.GetConfig()
		if !cfg.IsScheduledType() {
			continue // Skip incremental transformations
		}

		// Filter by database if provided
		if params.Database != nil && cfg.Database != *params.Database {
			continue
		}

		modelID := cfg.GetID()

		// Get last run timestamp from admin service
		lastRun, err := s.adminService.GetLastScheduledExecution(c.Context(), modelID)
		if err != nil {
			s.log.WithError(err).WithField("model_id", modelID).Error("Failed to get last run")
			continue
		}

		run := generated.ScheduledRun{
			Id: modelID,
		}
		if lastRun != nil {
			run.LastRun = lastRun
		}

		runs = append(runs, run)
	}

	return c.JSON(map[string]interface{}{
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
		return err
	}

	run := generated.ScheduledRun{
		Id: id,
	}
	if lastRun != nil {
		run.LastRun = lastRun
	}

	return c.JSON(run)
}
