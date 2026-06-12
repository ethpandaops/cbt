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
	handler := &Handler{}

	tests := []struct {
		name          string
		payload       any
		expectedType  Type
		expectedError bool
		description   string
	}{
		{
			name: "incremental payload with type field",
			payload: IncrementalPayload{
				Type:       TypeIncremental,
				ModelID:    "test.model",
				Position:   100,
				Interval:   50,
				Direction:  DirectionForward,
				EnqueuedAt: time.Now(),
			},
			expectedType:  TypeIncremental,
			expectedError: false,
			description:   "Payload with explicit Type field",
		},
		{
			name: "scheduled payload with type field",
			payload: ScheduledPayload{
				Type:          TypeScheduled,
				ModelID:       "test.model",
				ExecutionTime: time.Now(),
				EnqueuedAt:    time.Now(),
			},
			expectedType:  TypeScheduled,
			expectedError: false,
			description:   "Payload with explicit Type field",
		},
		{
			name: "incremental payload with Position=0 Interval=0 but Direction set (edge case)",
			payload: IncrementalPayload{
				Type:       TypeIncremental,
				ModelID:    "test.model",
				Position:   0,
				Interval:   0,
				Direction:  DirectionForward,
				EnqueuedAt: time.Now(),
			},
			expectedType:  TypeIncremental,
			expectedError: false,
			description:   "Position=0 and Interval=0 would misclassify without Type field",
		},
		{
			name: "legacy incremental payload without type field (backwards compat)",
			payload: map[string]any{
				"model_id":    "test.model",
				"position":    100,
				"interval":    50,
				"direction":   "forward",
				"enqueued_at": time.Now().Format(time.RFC3339),
			},
			expectedType:  TypeIncremental,
			expectedError: false,
			description:   "Legacy payload uses heuristic - Position > 0",
		},
		{
			name: "legacy scheduled payload without type field (backwards compat)",
			payload: map[string]any{
				"model_id":       "test.model",
				"execution_time": time.Now().Format(time.RFC3339),
				"enqueued_at":    time.Now().Format(time.RFC3339),
			},
			expectedType:  TypeScheduled,
			expectedError: false,
			description:   "Legacy payload without incremental fields defaults to scheduled",
		},
		{
			name: "legacy incremental with only Direction set (backwards compat)",
			payload: map[string]any{
				"model_id":    "test.model",
				"position":    0,
				"interval":    0,
				"direction":   "back",
				"enqueued_at": time.Now().Format(time.RFC3339),
			},
			expectedType:  TypeIncremental,
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
	handler := &Handler{}

	_, err := handler.parseTaskPayload([]byte("not valid json"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal payload")
}

// TestParseTaskPayload_ErrorBranches covers the unmarshal failures and the external
// type rejection that the success-path tests do not reach.
func TestParseTaskPayload_ErrorBranches(t *testing.T) {
	handler := &Handler{}

	tests := []struct {
		name    string
		data    string
		wantErr error
		wantSub string
	}{
		{
			name:    "incremental with malformed body",
			data:    `{"type":"incremental","position":"not-a-number"}`,
			wantSub: "failed to unmarshal incremental payload",
		},
		{
			name:    "scheduled with malformed body",
			data:    `{"type":"scheduled","execution_time":12345}`,
			wantSub: "failed to unmarshal scheduled payload",
		},
		{
			name:    "external type is rejected",
			data:    `{"type":"external","model_id":"src.table"}`,
			wantErr: ErrUnexpectedExternalType,
		},
		{
			name:    "no type field with malformed scheduled body",
			data:    `{"model_id":"db.model","execution_time":12345}`,
			wantSub: "failed to unmarshal payload",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := handler.parseTaskPayload([]byte(tt.data))
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantSub)
		})
	}
}

// TestPayload_GetEnqueuedAt verifies the GetEnqueuedAt accessor for both payload types.
func TestPayload_GetEnqueuedAt(t *testing.T) {
	now := time.Now()

	inc := IncrementalPayload{EnqueuedAt: now}
	assert.Equal(t, now, inc.GetEnqueuedAt())

	sch := ScheduledPayload{EnqueuedAt: now}
	assert.Equal(t, now, sch.GetEnqueuedAt())
}

// TestParseTaskPayload_ZeroValues tests the edge case where Position=0 and Interval=0
// This was the original bug that this refactoring fixes
func TestParseTaskPayload_ZeroValues(t *testing.T) {
	handler := &Handler{}

	// With Type field set, Position=0 and Interval=0 should still be correctly identified
	t.Run("with type field", func(t *testing.T) {
		payload := IncrementalPayload{
			Type:       TypeIncremental,
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

		assert.Equal(t, TypeIncremental, result.GetType())

		// Verify the payload values are preserved
		incPayload, ok := result.(IncrementalPayload)
		require.True(t, ok)
		assert.Equal(t, uint64(0), incPayload.Position)
		assert.Equal(t, uint64(0), incPayload.Interval)
		assert.Equal(t, DirectionForward, incPayload.Direction)
	})

	// Legacy payloads without Type field rely on heuristics
	// Direction is now included in the heuristic check
	t.Run("legacy with direction only", func(t *testing.T) {
		payload := map[string]any{
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
		assert.Equal(t, TypeIncremental, result.GetType())
	})

	// True scheduled payload (no incremental fields at all)
	t.Run("legacy scheduled without incremental fields", func(t *testing.T) {
		payload := map[string]any{
			"model_id":       "test.model",
			"execution_time": time.Now().Format(time.RFC3339),
			"enqueued_at":    time.Now().Format(time.RFC3339),
		}

		data, err := json.Marshal(payload)
		require.NoError(t, err)

		result, err := handler.parseTaskPayload(data)
		require.NoError(t, err)

		assert.Equal(t, TypeScheduled, result.GetType())
	})
}

// TestParseTaskPayload_TypeFieldPrecedence ensures Type field takes precedence over heuristics
func TestParseTaskPayload_TypeFieldPrecedence(t *testing.T) {
	handler := &Handler{}

	// Even with incremental-looking fields, if Type says scheduled, use scheduled
	// (This is a weird edge case but tests that Type field is authoritative)
	t.Run("type scheduled with incremental fields", func(t *testing.T) {
		// This is a contrived case - in practice Type should match the struct
		payload := map[string]any{
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
		assert.Equal(t, TypeScheduled, result.GetType())
	})

	t.Run("type incremental without incremental fields", func(t *testing.T) {
		payload := map[string]any{
			"type":        "incremental",
			"model_id":    "test.model",
			"enqueued_at": time.Now().Format(time.RFC3339),
		}

		data, err := json.Marshal(payload)
		require.NoError(t, err)

		result, err := handler.parseTaskPayload(data)
		require.NoError(t, err)

		// Type field takes precedence
		assert.Equal(t, TypeIncremental, result.GetType())
	})
}
