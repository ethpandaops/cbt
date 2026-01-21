package handlers

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/api/generated"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
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
	return typeParam == nil || *typeParam == generated.Transformation
}

func (s *Server) shouldIncludeExternals(typeParam *generated.ListAllModelsParamsType) bool {
	return typeParam == nil || *typeParam == generated.External
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

	// Get structured dependencies (preserving OR groups)
	structuredDeps := dag.GetStructuredDependencies(modelID)
	if len(structuredDeps) > 0 {
		apiDeps := convertDepsToAPIFormat(structuredDeps)
		model.DependsOn = &apiDeps
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
	incrementalHandler, ok := handler.(interface {
		GetInterval() (minInterval, maxInterval uint64)
		GetSchedules() (forwardfill, backfill string)
		GetTags() []string
		GetFlattenedDependencies() []string
	})
	if !ok {
		return
	}

	populateInterval(model, incrementalHandler, handler)
	populateSchedules(model, incrementalHandler)
	populateLimits(model, handler)
	populateFill(model, handler)
	populateTags(model, incrementalHandler)
}

func populateInterval(model *generated.TransformationModel, incrementalHandler interface {
	GetInterval() (minInterval, maxInterval uint64)
}, handler interface{}) {
	minInterval, maxInterval := incrementalHandler.GetInterval()

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
}

func populateSchedules(model *generated.TransformationModel, incrementalHandler interface {
	GetSchedules() (forwardfill, backfill string)
}) {
	forwardfill, backfill := incrementalHandler.GetSchedules()
	if forwardfill == "" && backfill == "" {
		return
	}

	model.Schedules = &struct {
		Backfill    *string `json:"backfill,omitempty"`
		Forwardfill *string `json:"forwardfill,omitempty"`
	}{
		Forwardfill: stringPtr(forwardfill),
		Backfill:    stringPtr(backfill),
	}
}

func populateLimits(model *generated.TransformationModel, handler interface{}) {
	limitsProvider, ok := handler.(transformation.LimitsHandler)
	if !ok {
		return
	}

	limits := limitsProvider.GetLimits()
	if limits == nil || (limits.Min == 0 && limits.Max == 0) {
		return
	}

	model.Limits = &struct {
		Max *int `json:"max,omitempty"`
		Min *int `json:"min,omitempty"`
	}{
		Min: intPtr(int(limits.Min)), // nolint:gosec
		Max: intPtr(int(limits.Max)), // nolint:gosec
	}
}

func populateFill(model *generated.TransformationModel, handler interface{}) {
	fillHandler, ok := handler.(transformation.FillHandler)
	if !ok {
		return
	}

	fill := &struct {
		AllowGapSkipping *bool                                       `json:"allow_gap_skipping,omitempty"`
		Buffer           *int                                        `json:"buffer,omitempty"`
		Direction        *generated.TransformationModelFillDirection `json:"direction,omitempty"`
	}{}

	if direction := fillHandler.GetFillDirection(); direction != "" {
		dir := generated.TransformationModelFillDirection(direction)
		fill.Direction = &dir
	}

	allowGapSkipping := fillHandler.AllowGapSkipping()
	fill.AllowGapSkipping = &allowGapSkipping

	if buffer := fillHandler.GetFillBuffer(); buffer > 0 {
		fill.Buffer = intPtr(int(buffer)) // nolint:gosec
	}

	model.Fill = fill
}

func populateTags(model *generated.TransformationModel, incrementalHandler interface {
	GetTags() []string
}) {
	tags := incrementalHandler.GetTags()
	if len(tags) > 0 {
		model.Tags = &tags
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

// convertDepsToAPIFormat converts structured dependencies to API format
// Returns []generated.TransformationModel_DependsOn_Item where each element is either string (AND) or []string (OR group)
func convertDepsToAPIFormat(deps []transformation.Dependency) []generated.TransformationModel_DependsOn_Item {
	result := make([]generated.TransformationModel_DependsOn_Item, len(deps))
	for i, dep := range deps {
		var item generated.TransformationModel_DependsOn_Item
		if dep.IsGroup {
			// OR group - marshal as []string
			_ = item.FromTransformationModelDependsOn1(dep.GroupDeps)
		} else {
			// Single dependency - marshal as string
			_ = item.FromTransformationModelDependsOn0(dep.SingleDep)
		}
		result[i] = item
	}
	return result
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
			s.log.WithError(err).WithField("model_id", modelID).Warn("Failed to get processed ranges for model, continuing")
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
			s.log.WithError(err).WithField("model_id", modelID).Warn("Failed to get last run for model, continuing")
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

// DebugCoverageAtPosition handles GET /api/v1/models/transformations/{id}/coverage/{position}
// This is the comprehensive debugging endpoint that uses the same validation logic
// as backfill and dependency checking. It provides a single source of truth for debugging
// why a position cannot be processed by showing:
// - The model's own coverage and gaps
// - All dependencies' bounds and gaps (recursively)
// - Validation results using the same logic as the coordinator
func (s *Server) DebugCoverageAtPosition(c fiber.Ctx, id string, position int) error {
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

	ctx := c.Context()
	pos := uint64(position) // nolint:gosec

	// Auto-detect interval: check if position is in a gap and use gap size, otherwise use model's max interval
	interval := s.autoDetectInterval(ctx, id, pos, node.GetHandler())

	// Build the comprehensive debug response
	endPosition := int(pos + interval) // nolint:gosec
	debug := generated.CoverageDebug{
		ModelId:     id,
		Position:    position,
		Interval:    int(interval), // nolint:gosec
		EndPosition: &endPosition,
	}

	// Get model's own coverage information
	modelCoverage, err := s.buildModelCoverageInfo(ctx, id, pos, interval)
	if err != nil {
		return err
	}
	debug.ModelCoverage = modelCoverage

	// Build recursive dependency tree with coverage/bounds for each
	dependencies := s.buildDependencyDebugTree(ctx, dag, node, pos, interval)
	debug.Dependencies = dependencies

	// Run validation using the same logic as coordinator (if validator available)
	validationInfo := s.buildValidationDebugInfo(ctx, id, pos, interval, dependencies)
	debug.Validation = validationInfo

	// Determine if can process based on validation
	canProcess := validationInfo.InBounds && !validationInfo.HasDependencyGaps
	debug.CanProcess = canProcess

	return c.JSON(debug)
}

// buildModelCoverageInfo gets coverage info for the target model itself
func (s *Server) buildModelCoverageInfo(ctx context.Context, modelID string, position, interval uint64) (generated.ModelCoverageInfo, error) {
	info := generated.ModelCoverageInfo{
		HasData:         false,
		FirstPosition:   0,
		LastEndPosition: 0,
	}

	// Get first and last positions
	firstPos, err := s.adminService.GetFirstPosition(ctx, modelID)
	if err != nil {
		return info, err
	}

	lastEndPos, err := s.adminService.GetNextUnprocessedPosition(ctx, modelID)
	if err != nil {
		return info, err
	}

	info.HasData = lastEndPos > 0
	info.FirstPosition = int(firstPos)     // nolint:gosec
	info.LastEndPosition = int(lastEndPos) // nolint:gosec

	// Get all processed ranges and filter to window
	allRanges, err := s.adminService.GetProcessedRanges(ctx, modelID)
	if err != nil {
		return info, err
	}

	// Filter to ranges that overlap our window [position, position+interval]
	rangesInWindow := []generated.Range{}
	for _, r := range allRanges {
		rangeEnd := r.Position + r.Interval
		if rangeEnd > position && r.Position < position+interval {
			rangesInWindow = append(rangesInWindow, generated.Range{
				Position: int(r.Position), // nolint:gosec
				Interval: int(r.Interval), // nolint:gosec
			})
		}
	}
	if len(rangesInWindow) > 0 {
		info.RangesInWindow = &rangesInWindow
	}

	// Find ALL gaps across the model's full range (same as coordinator does)
	// Then filter to gaps that overlap our requested position window
	allGaps, err := s.adminService.FindGaps(ctx, modelID, firstPos, lastEndPos, interval)
	if err != nil {
		return info, err
	}

	// Filter to gaps that overlap the requested position window
	gapsInWindow := []generated.GapInfo{}
	for _, gap := range allGaps {
		overlaps := gap.StartPos < position+interval && gap.EndPos > position
		if overlaps {
			gapsInWindow = append(gapsInWindow, generated.GapInfo{
				Start:           int(gap.StartPos),              // nolint:gosec
				End:             int(gap.EndPos),                // nolint:gosec
				Size:            int(gap.EndPos - gap.StartPos), // nolint:gosec
				OverlapsRequest: &overlaps,
			})
		}
	}
	if len(gapsInWindow) > 0 {
		info.GapsInWindow = &gapsInWindow
	}

	return info, nil
}

// buildDependencyDebugTree recursively builds dependency debug info
func (s *Server) buildDependencyDebugTree(ctx context.Context, dag models.DAGReader, node models.Transformation, position, interval uint64) []generated.DependencyDebugInfo {
	handler := node.GetHandler()
	if handler == nil {
		return []generated.DependencyDebugInfo{}
	}

	// Try to get structured dependencies (with OR group support)
	type structuredDepProvider interface {
		GetDependencies() []struct {
			IsGroup   bool
			SingleDep string
			GroupDeps []string
		}
	}

	if provider, ok := handler.(structuredDepProvider); ok {
		return s.processStructuredDependencies(ctx, dag, provider, position, interval)
	}

	// Fallback: try flat dependencies
	type flatDepProvider interface {
		GetFlattenedDependencies() []string
	}

	if provider, ok := handler.(flatDepProvider); ok {
		deps := provider.GetFlattenedDependencies()
		result := make([]generated.DependencyDebugInfo, 0, len(deps))

		for _, depID := range deps {
			depInfo, err := s.buildSingleDependencyDebugInfo(ctx, dag, depID, position, interval)
			if err != nil {
				s.log.WithError(err).WithField("dep_id", depID).Debug("Failed to build dependency debug info")
				continue
			}
			result = append(result, depInfo)
		}

		return result
	}

	return []generated.DependencyDebugInfo{}
}

func (s *Server) processStructuredDependencies(ctx context.Context, dag models.DAGReader, provider interface {
	GetDependencies() []struct {
		IsGroup   bool
		SingleDep string
		GroupDeps []string
	}
}, position, interval uint64) []generated.DependencyDebugInfo {
	deps := provider.GetDependencies()
	result := make([]generated.DependencyDebugInfo, 0, len(deps))

	for _, dep := range deps {
		if dep.IsGroup {
			// OR group - analyze all members
			orGroupInfo := s.buildORGroupDebugInfo(ctx, dag, dep.GroupDeps, position, interval)
			result = append(result, orGroupInfo)
		} else {
			// Single dependency
			depInfo, err := s.buildSingleDependencyDebugInfo(ctx, dag, dep.SingleDep, position, interval)
			if err != nil {
				s.log.WithError(err).WithField("dep_id", dep.SingleDep).Debug("Failed to build dependency debug info")
				continue
			}
			result = append(result, depInfo)
		}
	}

	return result
}

// buildORGroupDebugInfo builds debug info for an OR group
func (s *Server) buildORGroupDebugInfo(ctx context.Context, dag models.DAGReader, groupDeps []string, position, interval uint64) generated.DependencyDebugInfo {
	orGroupInfo := generated.DependencyDebugInfo{
		Id:       fmt.Sprintf("[OR: %v]", groupDeps),
		Type:     generated.OrGroup,
		NodeType: generated.DependencyDebugInfoNodeTypeTransformation, // placeholder
	}

	// Analyze each member of the OR group
	members := make([]generated.DependencyDebugInfo, 0, len(groupDeps))
	hasAnyData := false

	for _, depID := range groupDeps {
		memberInfo, err := s.buildSingleDependencyDebugInfo(ctx, dag, depID, position, interval)
		if err != nil {
			s.log.WithError(err).WithField("dep_id", depID).Debug("Failed to build OR group member debug info")
			continue
		}

		members = append(members, memberInfo)

		// Check if this member has data
		if memberInfo.CoverageStatus != nil && *memberInfo.CoverageStatus != generated.NotInitialized && *memberInfo.CoverageStatus != generated.NoData {
			hasAnyData = true
		}
	}

	orGroupInfo.OrGroupMembers = &members

	// Determine if OR group is blocking
	blocking := !hasAnyData
	orGroupInfo.Blocking = &blocking

	return orGroupInfo
}

// buildSingleDependencyDebugInfo builds debug info for a single dependency
func (s *Server) buildSingleDependencyDebugInfo(ctx context.Context, dag models.DAGReader, depID string, position, interval uint64) (generated.DependencyDebugInfo, error) {
	depNode, err := dag.GetNode(depID)
	if err != nil {
		return generated.DependencyDebugInfo{}, err
	}

	depInfo := generated.DependencyDebugInfo{
		Id:   depID,
		Type: generated.Required, // AND dependency
	}

	// Set node type
	switch depNode.NodeType {
	case models.NodeTypeExternal:
		depInfo.NodeType = generated.DependencyDebugInfoNodeTypeExternal
		result, err := s.buildExternalDependencyDebugInfo(ctx, depNode, &depInfo, position, interval)
		if err != nil {
			return generated.DependencyDebugInfo{}, err
		}
		return *result, nil

	case models.NodeTypeTransformation:
		depInfo.NodeType = generated.DependencyDebugInfoNodeTypeTransformation
		result, err := s.buildTransformationDependencyDebugInfo(ctx, dag, depNode, &depInfo, position, interval)
		if err != nil {
			return generated.DependencyDebugInfo{}, err
		}
		return *result, nil

	default:
		return depInfo, nil
	}
}

// Define static errors for models package
var (
	ErrNodeNotExternal       = errors.New("node is not external model")
	ErrNodeNotTransformation = errors.New("node is not transformation model")
)

// buildExternalDependencyDebugInfo builds debug info for external dependencies
func (s *Server) buildExternalDependencyDebugInfo(ctx context.Context, depNode models.Node, depInfo *generated.DependencyDebugInfo, position, interval uint64) (*generated.DependencyDebugInfo, error) {
	external, ok := depNode.Model.(models.External)
	if !ok {
		return depInfo, ErrNodeNotExternal
	}

	// Get bounds from cache
	boundsCache, err := s.adminService.GetExternalBounds(ctx, external.GetID())
	if err != nil || boundsCache == nil {
		// No bounds available
		depInfo.Bounds = generated.BoundsInfo{
			HasData: false,
			Min:     0,
			Max:     0,
		}
		notInit := generated.NotInitialized
		depInfo.CoverageStatus = &notInit
		return depInfo, nil
	}

	// External models don't have gaps (assumed continuous)
	// Apply lag if configured
	maxBound := boundsCache.Max
	cfg := external.GetConfig()
	var lagApplied *int
	if cfg.Lag > 0 {
		if maxBound > cfg.Lag {
			maxBound -= cfg.Lag
		}
		lagInt := int(cfg.Lag) // nolint:gosec
		lagApplied = &lagInt
	}

	depInfo.Bounds = generated.BoundsInfo{
		HasData:    boundsCache.Max > 0,
		Min:        int(boundsCache.Min), // nolint:gosec
		Max:        int(maxBound),        // nolint:gosec
		LagApplied: lagApplied,
	}

	// Determine coverage status
	endPos := position + interval
	switch {
	case boundsCache.Max == 0:
		status := generated.NoData
		depInfo.CoverageStatus = &status
	case position >= uint64(depInfo.Bounds.Min) && endPos <= uint64(depInfo.Bounds.Max): // nolint:gosec
		status := generated.FullCoverage
		depInfo.CoverageStatus = &status
	default:
		status := generated.NoData
		depInfo.CoverageStatus = &status
	}

	// External is blocking if position is out of bounds
	blocking := endPos > uint64(depInfo.Bounds.Max) || position < uint64(depInfo.Bounds.Min) // nolint:gosec
	depInfo.Blocking = &blocking

	return depInfo, nil
}

// buildTransformationDependencyDebugInfo builds debug info for transformation dependencies
func (s *Server) buildTransformationDependencyDebugInfo(ctx context.Context, dag models.DAGReader, depNode models.Node, depInfo *generated.DependencyDebugInfo, position, interval uint64) (*generated.DependencyDebugInfo, error) {
	trans, ok := depNode.Model.(models.Transformation)
	if !ok {
		return depInfo, ErrNodeNotTransformation
	}

	depID := trans.GetID()
	handler := trans.GetHandler()

	// Set incremental flag
	s.setIncrementalFlag(depInfo, handler)

	// Get and set bounds
	firstPos, lastEndPos := s.getTransformationBounds(ctx, depID)
	depInfo.Bounds = generated.BoundsInfo{
		HasData: lastEndPos > 0,
		Min:     int(firstPos),   // nolint:gosec
		Max:     int(lastEndPos), // nolint:gosec
	}

	// Find gaps if incremental
	isIncremental := depInfo.IsIncremental != nil && *depInfo.IsIncremental
	var gapsList []generated.GapInfo
	if isIncremental {
		gapsList = s.findOverlappingGaps(ctx, depID, firstPos, lastEndPos, position, interval)
		if len(gapsList) > 0 {
			depInfo.Gaps = &gapsList
		}
	}

	// Determine coverage status and blocking
	endPos := position + interval
	s.setCoverageStatus(depInfo, lastEndPos, firstPos, position, endPos, gapsList)
	s.setBlockingStatus(depInfo, gapsList, lastEndPos, firstPos, position, endPos)

	// Recursively get child dependencies
	childDeps := s.buildDependencyDebugTree(ctx, dag, trans, position, interval)
	if len(childDeps) > 0 {
		depInfo.ChildDependencies = &childDeps
	}

	return depInfo, nil
}

func (s *Server) setIncrementalFlag(depInfo *generated.DependencyDebugInfo, handler interface{}) {
	isIncremental := handler != nil
	if trackable, ok := handler.(interface{ ShouldTrackPosition() bool }); ok {
		isIncremental = trackable.ShouldTrackPosition()
	}
	depInfo.IsIncremental = &isIncremental
}

func (s *Server) getTransformationBounds(ctx context.Context, depID string) (firstPos, lastEndPos uint64) {
	firstPos, err := s.adminService.GetFirstPosition(ctx, depID)
	if err != nil {
		s.log.WithError(err).WithField("dep_id", depID).Debug("Failed to get first position")
	}

	lastEndPos, err = s.adminService.GetNextUnprocessedPosition(ctx, depID)
	if err != nil {
		s.log.WithError(err).WithField("dep_id", depID).Debug("Failed to get last end position")
	}

	return firstPos, lastEndPos
}

func (s *Server) findOverlappingGaps(ctx context.Context, depID string, firstPos, lastEndPos, position, interval uint64) []generated.GapInfo {
	var gapsList []generated.GapInfo

	allGaps, err := s.adminService.FindGaps(ctx, depID, firstPos, lastEndPos, interval)
	if err != nil {
		s.log.WithError(err).WithField("dep_id", depID).Debug("Failed to find gaps")
		return gapsList
	}

	endPos := position + interval
	for _, gap := range allGaps {
		overlaps := gap.StartPos < endPos && gap.EndPos > position
		if overlaps {
			gapsList = append(gapsList, generated.GapInfo{
				Start:           int(gap.StartPos),              // nolint:gosec
				End:             int(gap.EndPos),                // nolint:gosec
				Size:            int(gap.EndPos - gap.StartPos), // nolint:gosec
				OverlapsRequest: &overlaps,
			})
		}
	}

	return gapsList
}

func (s *Server) setCoverageStatus(depInfo *generated.DependencyDebugInfo, lastEndPos, firstPos, position, endPos uint64, gapsList []generated.GapInfo) {
	var status generated.DependencyDebugInfoCoverageStatus

	switch {
	case lastEndPos == 0:
		status = generated.NotInitialized
	case len(gapsList) > 0:
		status = generated.HasGaps
	case position >= firstPos && endPos <= lastEndPos:
		status = generated.FullCoverage
	default:
		status = generated.NoData
	}

	depInfo.CoverageStatus = &status
}

func (s *Server) setBlockingStatus(depInfo *generated.DependencyDebugInfo, gapsList []generated.GapInfo, lastEndPos, firstPos, position, endPos uint64) {
	blocking := len(gapsList) > 0 || endPos > lastEndPos || position < firstPos
	depInfo.Blocking = &blocking
}

// buildValidationDebugInfo builds validation info using dependency analysis
func (s *Server) buildValidationDebugInfo(_ context.Context, _ string, position, interval uint64, dependencies []generated.DependencyDebugInfo) generated.ValidationDebugInfo {
	info := s.initializeValidationInfo()

	// Process dependencies and calculate valid range
	minValid, maxValid, hasAnyDep := s.processValidationDependencies(&info, dependencies, position)

	// Set valid range and check bounds if we have dependencies
	if hasAnyDep {
		s.setValidationRange(&info, minValid, maxValid, position, interval)
	}

	// Calculate next valid position from blocking gaps
	s.calculateNextValidPosition(&info)

	return info
}

func (s *Server) initializeValidationInfo() generated.ValidationDebugInfo {
	blockingGaps := []struct {
		DependencyId string            `json:"dependency_id"` // nolint:revive
		Gap          generated.GapInfo `json:"gap"`
	}{}
	reasons := []string{}

	return generated.ValidationDebugInfo{
		InBounds:          false,
		HasDependencyGaps: false,
		BlockingGaps:      &blockingGaps,
		Reasons:           &reasons,
	}
}

func (s *Server) processValidationDependencies(info *generated.ValidationDebugInfo, dependencies []generated.DependencyDebugInfo, position uint64) (minValid, maxValid uint64, hasAnyDep bool) {
	minValid = uint64(0)
	maxValid = ^uint64(0) // max uint64
	hasAnyDep = false

	blockingGaps := *info.BlockingGaps
	reasons := *info.Reasons

	for i := range dependencies {
		dep := &dependencies[i]
		hasAnyDep = true

		// Update min/max based on dependency bounds
		minValid, maxValid = s.updateValidRange(dep, minValid, maxValid)

		// Check for blocking issues
		s.processBlockingDependency(dep, &blockingGaps, &reasons, info, position)
	}

	info.BlockingGaps = &blockingGaps
	info.Reasons = &reasons

	return minValid, maxValid, hasAnyDep
}

func (s *Server) updateValidRange(dep *generated.DependencyDebugInfo, minValid, maxValid uint64) (newMinValid, newMaxValid uint64) {
	newMinValid = minValid
	newMaxValid = maxValid
	if dep.Bounds.HasData {
		if uint64(dep.Bounds.Min) > newMinValid { // nolint:gosec
			newMinValid = uint64(dep.Bounds.Min) // nolint:gosec
		}
		if uint64(dep.Bounds.Max) < newMaxValid { // nolint:gosec
			newMaxValid = uint64(dep.Bounds.Max) // nolint:gosec
		}
	}
	return newMinValid, newMaxValid
}

func (s *Server) processBlockingDependency(dep *generated.DependencyDebugInfo, blockingGaps *[]struct {
	DependencyId string            `json:"dependency_id"` // nolint:revive
	Gap          generated.GapInfo `json:"gap"`
}, reasons *[]string, info *generated.ValidationDebugInfo, position uint64) {
	if dep.Blocking == nil || !*dep.Blocking {
		return
	}

	if dep.Gaps != nil && len(*dep.Gaps) > 0 {
		s.processBlockingGaps(dep, blockingGaps, reasons, info)
	} else if dep.CoverageStatus != nil {
		s.processBlockingStatus(dep, reasons, position)
	}
}

func (s *Server) processBlockingGaps(dep *generated.DependencyDebugInfo, blockingGaps *[]struct {
	DependencyId string            `json:"dependency_id"` // nolint:revive
	Gap          generated.GapInfo `json:"gap"`
}, reasons *[]string, info *generated.ValidationDebugInfo) {
	info.HasDependencyGaps = true
	for _, gap := range *dep.Gaps {
		if gap.OverlapsRequest != nil && *gap.OverlapsRequest {
			*blockingGaps = append(*blockingGaps, struct {
				DependencyId string            `json:"dependency_id"` // nolint:revive
				Gap          generated.GapInfo `json:"gap"`
			}{
				DependencyId: dep.Id,
				Gap:          gap,
			})
			*reasons = append(*reasons, fmt.Sprintf("Dependency %s has gap from %d to %d", dep.Id, gap.Start, gap.End))
		}
	}
}

func (s *Server) processBlockingStatus(dep *generated.DependencyDebugInfo, reasons *[]string, position uint64) {
	switch *dep.CoverageStatus {
	case generated.NotInitialized:
		*reasons = append(*reasons, fmt.Sprintf("Dependency %s is not initialized", dep.Id))
	case generated.NoData:
		*reasons = append(*reasons, fmt.Sprintf("Dependency %s has no data for position %d", dep.Id, position))
	case generated.FullCoverage, generated.HasGaps:
		// These cases shouldn't happen when blocking is true, but handle for exhaustiveness
	}
}

func (s *Server) setValidationRange(info *generated.ValidationDebugInfo, minValid, maxValid, position, interval uint64) {
	info.ValidRange = &struct {
		Max int `json:"max"`
		Min int `json:"min"`
	}{
		Min: int(minValid), // nolint:gosec
		Max: int(maxValid), // nolint:gosec
	}

	endPos := position + interval
	info.InBounds = position >= minValid && endPos <= maxValid

	reasons := *info.Reasons
	if !info.InBounds {
		if position < minValid {
			reasons = append(reasons, fmt.Sprintf("Position %d is before earliest available data at %d", position, minValid))
		}
		if endPos > maxValid {
			reasons = append(reasons, fmt.Sprintf("Position end %d exceeds latest available data at %d", endPos, maxValid))
			nextValid := int(maxValid) // nolint:gosec
			info.NextValidPosition = &nextValid
		}
	}
	info.Reasons = &reasons
}

func (s *Server) calculateNextValidPosition(info *generated.ValidationDebugInfo) {
	blockingGaps := *info.BlockingGaps
	if info.HasDependencyGaps && len(blockingGaps) > 0 {
		maxGapEnd := 0
		for _, bg := range blockingGaps {
			if bg.Gap.End > maxGapEnd {
				maxGapEnd = bg.Gap.End
			}
		}
		if maxGapEnd > 0 {
			info.NextValidPosition = &maxGapEnd
		}
	}
}

// autoDetectInterval determines the appropriate interval for debugging a position.
// It checks if the position falls within a gap and uses the gap size if so,
// otherwise falls back to the model's max interval.
func (s *Server) autoDetectInterval(ctx context.Context, modelID string, position uint64, handler interface{}) uint64 {
	// Default to model's max interval
	defaultInterval := uint64(60) // fallback minimum
	if incrementalHandler, ok := handler.(interface {
		GetInterval() (minInterval, maxInterval uint64)
	}); ok {
		_, maxInterval := incrementalHandler.GetInterval()
		if maxInterval > 0 {
			defaultInterval = maxInterval
		}
	}

	// Get model bounds to find gaps across full range
	firstPos, err := s.adminService.GetFirstPosition(ctx, modelID)
	if err != nil {
		s.log.WithError(err).WithField("model_id", modelID).Debug("Failed to get first position for auto-detect")
		return defaultInterval
	}

	lastEndPos, err := s.adminService.GetNextUnprocessedPosition(ctx, modelID)
	if err != nil {
		s.log.WithError(err).WithField("model_id", modelID).Debug("Failed to get last end position for auto-detect")
		return defaultInterval
	}

	// If no data yet, use default
	if lastEndPos == 0 {
		return defaultInterval
	}

	// Find all gaps across the model's full range
	allGaps, err := s.adminService.FindGaps(ctx, modelID, firstPos, lastEndPos, defaultInterval)
	if err != nil {
		s.log.WithError(err).WithField("model_id", modelID).Debug("Failed to find gaps for auto-detect")
		return defaultInterval
	}

	// Check if position falls within any gap
	for _, gap := range allGaps {
		if position >= gap.StartPos && position < gap.EndPos {
			// Position is in a gap, use the gap size as interval
			gapSize := gap.EndPos - gap.StartPos
			s.log.WithFields(map[string]interface{}{
				"model_id": modelID,
				"position": position,
				"gap_size": gapSize,
			}).Debug("Auto-detected interval from gap size")
			return gapSize
		}
	}

	// Position is not in a gap, use default interval
	return defaultInterval
}
