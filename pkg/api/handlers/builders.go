package handlers

import (
	"github.com/ethpandaops/cbt/pkg/api/generated"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
)

// buildExternalModel constructs an ExternalModel from domain model
func buildExternalModel(
	modelID string,
	node models.External,
	_ models.DAGReader,
	overrideStatus configOverrideStatus,
) generated.ExternalModel {
	cfg := node.GetConfig()

	model := generated.ExternalModel{
		Id:          modelID,
		Database:    cfg.Database,
		Table:       cfg.Table,
		HasOverride: new(overrideStatus.hasOverride),
		IsDisabled:  new(overrideStatus.isDisabled),
	}

	// Populate interval configuration if available
	if provider, ok := node.(intervalTypeProvider); ok {
		intervalType := provider.GetIntervalType()
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
		lag := toInt(cfg.Lag)
		model.Lag = &lag
	}

	return model
}

// buildTransformationModel constructs a TransformationModel from domain model
func buildTransformationModel(
	modelID string,
	node models.Transformation,
	dag models.DAGReader,
	overrideStatus configOverrideStatus,
) generated.TransformationModel {
	cfg := node.GetConfig()

	model := generated.TransformationModel{
		Id:          modelID,
		Database:    cfg.Database,
		Table:       cfg.Table,
		Content:     node.GetValue(),
		HasOverride: new(overrideStatus.hasOverride),
		IsDisabled:  new(overrideStatus.isDisabled),
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
	if scheduledHandler, ok := handler.(scheduleProvider); ok {
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
	incrementalHandler, ok := handler.(incrementalProvider)
	if !ok {
		return
	}

	populateInterval(model, incrementalHandler, handler)
	populateSchedules(model, incrementalHandler)
	populateLimits(model, handler)
	populateFill(model, handler)
	populateTags(model, incrementalHandler)
}

func populateInterval(model *generated.TransformationModel, handler intervalProvider, rawHandler any) {
	minInterval, maxInterval := handler.GetInterval()

	var intervalType *string
	if provider, ok := rawHandler.(intervalTypeProvider); ok {
		if it := provider.GetIntervalType(); it != "" {
			intervalType = &it
		}
	}

	model.Interval = &struct {
		Max  *int    `json:"max,omitempty"`
		Min  *int    `json:"min,omitempty"`
		Type *string `json:"type,omitempty"`
	}{
		Min:  intPtr(toInt(minInterval)),
		Max:  intPtr(toInt(maxInterval)),
		Type: intervalType,
	}
}

func populateSchedules(model *generated.TransformationModel, handler schedulesProvider) {
	forwardfill, backfill := handler.GetSchedules()
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

func populateLimits(model *generated.TransformationModel, handler any) {
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
		Min: intPtr(toInt(limits.Min)),
		Max: intPtr(toInt(limits.Max)),
	}
}

func populateFill(model *generated.TransformationModel, handler any) {
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
		fill.Buffer = intPtr(toInt(buffer))
	}

	model.Fill = fill
}

func populateTags(model *generated.TransformationModel, handler tagsProvider) {
	tags := handler.GetTags()
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
