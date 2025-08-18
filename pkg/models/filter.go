package models

import (
	"github.com/sirupsen/logrus"
)

// TagFilter handles model filtering based on tags
type TagFilter struct {
	logger *logrus.Logger
}

// NewTagFilter creates a new tag filter
func NewTagFilter(logger *logrus.Logger) *TagFilter {
	return &TagFilter{
		logger: logger,
	}
}

// FilterByTags filters models based on tag configuration
func (f *TagFilter) FilterByTags(modelConfigs map[string]ModelConfig, tags *ModelTags) map[string]ModelConfig {
	// If no tags configured, return all models
	if tags == nil {
		return modelConfigs
	}

	filtered := make(map[string]ModelConfig)

	for modelID := range modelConfigs {
		model := modelConfigs[modelID]
		// Check if model should be included based on tags
		if f.shouldIncludeModel(model.Tags, tags) {
			filtered[modelID] = model
			f.logger.WithField("model", modelID).
				WithField("tags", model.Tags).
				Debug("Model included based on tag filtering")
		} else {
			f.logger.WithField("model", modelID).
				WithField("tags", model.Tags).
				Debug("Model excluded based on tag filtering")
		}
	}

	f.logger.WithField("total_models", len(modelConfigs)).
		WithField("filtered_models", len(filtered)).
		Info("Applied tag-based model filtering")

	return filtered
}

// shouldIncludeModel determines if a model should be included based on tag filters
func (f *TagFilter) shouldIncludeModel(modelTags []string, filter *ModelTags) bool {
	// Create a set of model tags for efficient lookup
	tagSet := make(map[string]bool)
	for _, tag := range modelTags {
		tagSet[tag] = true
	}

	// Check exclude tags (if model has any exclude tag, it's excluded)
	for _, excludeTag := range filter.Exclude {
		if tagSet[excludeTag] {
			return false
		}
	}

	// Check require tags (model must have ALL require tags)
	for _, requireTag := range filter.Require {
		if !tagSet[requireTag] {
			return false
		}
	}

	// Check include tags (model must have at least one include tag, if specified)
	if len(filter.Include) > 0 {
		hasIncludeTag := false
		for _, includeTag := range filter.Include {
			if tagSet[includeTag] {
				hasIncludeTag = true
				break
			}
		}
		if !hasIncludeTag {
			return false
		}
	}

	return true
}
