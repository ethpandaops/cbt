package tasks

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseTaskPayload tests the parseTaskPayload function with various payload types.
func TestParseTaskPayload(t *testing.T) {
	handler := &TaskHandler{}

	tests := []struct {
		name          string
		payload       interface{}
		expectedType  TaskType
		expectedError bool
		description   string
	}{
		{
			name: "incremental payload with type field",
			payload: IncrementalTaskPayload{
				Type:       TaskTypeIncremental,
				ModelID:    "test.model",
				Position:   100,
				Interval:   50,
				Direction:  DirectionForward,
				EnqueuedAt: time.Now(),
			},
			expectedType:  TaskTypeIncremental,
			expectedError: false,
			description:   "Payload with explicit Type field",
		},
		{
			name: "scheduled payload with type field",
			payload: ScheduledTaskPayload{
				Type:          TaskTypeScheduled,
				ModelID:       "test.model",
				ExecutionTime: time.Now(),
				EnqueuedAt:    time.Now(),
			},
			expectedType:  TaskTypeScheduled,
			expectedError: false,
			description:   "Payload with explicit Type field",
		},
		{
			name: "incremental payload with Position=0 Interval=0 but Direction set (edge case)",
			payload: IncrementalTaskPayload{
				Type:       TaskTypeIncremental,
				ModelID:    "test.model",
				Position:   0,
				Interval:   0,
				Direction:  DirectionForward,
				EnqueuedAt: time.Now(),
			},
			expectedType:  TaskTypeIncremental,
			expectedError: false,
			description:   "Position=0 and Interval=0 would misclassify without Type field",
		},
		{
			name: "legacy incremental payload without type field (backwards compat)",
			payload: map[string]interface{}{
				"model_id":    "test.model",
				"position":    100,
				"interval":    50,
				"direction":   "forward",
				"enqueued_at": time.Now().Format(time.RFC3339),
			},
			expectedType:  TaskTypeIncremental,
			expectedError: false,
			description:   "Legacy payload uses heuristic - Position > 0",
		},
		{
			name: "legacy scheduled payload without type field (backwards compat)",
			payload: map[string]interface{}{
				"model_id":       "test.model",
				"execution_time": time.Now().Format(time.RFC3339),
				"enqueued_at":    time.Now().Format(time.RFC3339),
			},
			expectedType:  TaskTypeScheduled,
			expectedError: false,
			description:   "Legacy payload without incremental fields defaults to scheduled",
		},
		{
			name: "legacy incremental with only Direction set (backwards compat)",
			payload: map[string]interface{}{
				"model_id":    "test.model",
				"position":    0,
				"interval":    0,
				"direction":   "back",
				"enqueued_at": time.Now().Format(time.RFC3339),
			},
			expectedType:  TaskTypeIncremental,
			expectedError: false,
			description:   "Legacy payload with Direction but zero Position/Interval should be detected as incremental",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal payload to JSON
			data, err := json.Marshal(tt.payload)
			require.NoError(t, err, "Failed to marshal test payload")

			// Parse the payload
			result, err := handler.parseTaskPayload(data)

			if tt.expectedError {
				assert.Error(t, err, tt.description)
				return
			}

			require.NoError(t, err, tt.description)
			assert.Equal(t, tt.expectedType, result.GetType(), tt.description)
		})
	}
}

// TestParseTaskPayload_InvalidJSON tests error handling for invalid JSON.
func TestParseTaskPayload_InvalidJSON(t *testing.T) {
	handler := &TaskHandler{}

	_, err := handler.parseTaskPayload([]byte("not valid json"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal payload")
}

// TestParseTaskPayload_ZeroValues tests the edge case where Position=0 and Interval=0
// This was the original bug that this refactoring fixes
func TestParseTaskPayload_ZeroValues(t *testing.T) {
	handler := &TaskHandler{}

	// With Type field set, Position=0 and Interval=0 should still be correctly identified
	t.Run("with type field", func(t *testing.T) {
		payload := IncrementalTaskPayload{
			Type:       TaskTypeIncremental,
			ModelID:    "test.model",
			Position:   0,
			Interval:   0,
			Direction:  DirectionForward,
			EnqueuedAt: time.Now(),
		}

		data, err := json.Marshal(payload)
		require.NoError(t, err)

		result, err := handler.parseTaskPayload(data)
		require.NoError(t, err)

		assert.Equal(t, TaskTypeIncremental, result.GetType())

		// Verify the payload values are preserved
		incPayload, ok := result.(IncrementalTaskPayload)
		require.True(t, ok)
		assert.Equal(t, uint64(0), incPayload.Position)
		assert.Equal(t, uint64(0), incPayload.Interval)
		assert.Equal(t, DirectionForward, incPayload.Direction)
	})

	// Legacy payloads without Type field rely on heuristics
	// Direction is now included in the heuristic check
	t.Run("legacy with direction only", func(t *testing.T) {
		payload := map[string]interface{}{
			"model_id":    "test.model",
			"position":    0,
			"interval":    0,
			"direction":   "forward",
			"enqueued_at": time.Now().Format(time.RFC3339),
		}

		data, err := json.Marshal(payload)
		require.NoError(t, err)

		result, err := handler.parseTaskPayload(data)
		require.NoError(t, err)

		// With Direction set, should be detected as incremental
		assert.Equal(t, TaskTypeIncremental, result.GetType())
	})

	// True scheduled payload (no incremental fields at all)
	t.Run("legacy scheduled without incremental fields", func(t *testing.T) {
		payload := map[string]interface{}{
			"model_id":       "test.model",
			"execution_time": time.Now().Format(time.RFC3339),
			"enqueued_at":    time.Now().Format(time.RFC3339),
		}

		data, err := json.Marshal(payload)
		require.NoError(t, err)

		result, err := handler.parseTaskPayload(data)
		require.NoError(t, err)

		assert.Equal(t, TaskTypeScheduled, result.GetType())
	})
}

// TestParseTaskPayload_TypeFieldPrecedence ensures Type field takes precedence over heuristics
func TestParseTaskPayload_TypeFieldPrecedence(t *testing.T) {
	handler := &TaskHandler{}

	// Even with incremental-looking fields, if Type says scheduled, use scheduled
	// (This is a weird edge case but tests that Type field is authoritative)
	t.Run("type scheduled with incremental fields", func(t *testing.T) {
		// This is a contrived case - in practice Type should match the struct
		payload := map[string]interface{}{
			"type":        "scheduled",
			"model_id":    "test.model",
			"position":    100,
			"interval":    50,
			"enqueued_at": time.Now().Format(time.RFC3339),
		}

		data, err := json.Marshal(payload)
		require.NoError(t, err)

		result, err := handler.parseTaskPayload(data)
		require.NoError(t, err)

		// Type field takes precedence
		assert.Equal(t, TaskTypeScheduled, result.GetType())
	})

	t.Run("type incremental without incremental fields", func(t *testing.T) {
		payload := map[string]interface{}{
			"type":        "incremental",
			"model_id":    "test.model",
			"enqueued_at": time.Now().Format(time.RFC3339),
		}

		data, err := json.Marshal(payload)
		require.NoError(t, err)

		result, err := handler.parseTaskPayload(data)
		require.NoError(t, err)

		// Type field takes precedence
		assert.Equal(t, TaskTypeIncremental, result.GetType())
	})
}
