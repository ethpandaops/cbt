package models

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestTagFilter_FilterByTags(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel) // Quiet for tests
	filter := NewTagFilter(logger)

	testModels := map[string]ModelConfig{
		"model1": {Tags: []string{"prod", "critical"}},
		"model2": {Tags: []string{"dev", "test"}},
		"model3": {Tags: []string{"prod", "test"}},
		"model4": {Tags: []string{"staging"}},
		"model5": {Tags: []string{}}, // No tags
	}

	tests := []struct {
		name     string
		tags     *ModelTags
		expected []string
	}{
		{
			name:     "nil tags returns all",
			tags:     nil,
			expected: []string{"model1", "model2", "model3", "model4", "model5"},
		},
		{
			name:     "include prod",
			tags:     &ModelTags{Include: []string{"prod"}},
			expected: []string{"model1", "model3"},
		},
		{
			name:     "exclude test",
			tags:     &ModelTags{Exclude: []string{"test"}},
			expected: []string{"model1", "model4", "model5"},
		},
		{
			name:     "require prod",
			tags:     &ModelTags{Require: []string{"prod"}},
			expected: []string{"model1", "model3"},
		},
		{
			name:     "require prod AND critical",
			tags:     &ModelTags{Require: []string{"prod", "critical"}},
			expected: []string{"model1"},
		},
		{
			name: "include prod, exclude test",
			tags: &ModelTags{
				Include: []string{"prod"},
				Exclude: []string{"test"},
			},
			expected: []string{"model1"},
		},
		{
			name:     "include dev OR staging",
			tags:     &ModelTags{Include: []string{"dev", "staging"}},
			expected: []string{"model2", "model4"},
		},
		{
			name: "complex: require prod, exclude critical",
			tags: &ModelTags{
				Require: []string{"prod"},
				Exclude: []string{"critical"},
			},
			expected: []string{"model3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filter.FilterByTags(testModels, tt.tags)

			// Extract IDs for comparison
			var resultIDs []string
			for id := range result {
				resultIDs = append(resultIDs, id)
			}

			assert.ElementsMatch(t, tt.expected, resultIDs)
		})
	}
}

func TestTagFilter_EmptyModels(t *testing.T) {
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)
	filter := NewTagFilter(logger)

	tags := &ModelTags{Include: []string{"prod"}}
	result := filter.FilterByTags(map[string]ModelConfig{}, tags)

	assert.Empty(t, result)
}
