package tasks

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test Payload UniqueID
func TestTaskPayload_UniqueID(t *testing.T) {
	tests := []struct {
		name     string
		payload  Payload
		expected string
	}{
		{
			name: "incremental task unique ID without direction",
			payload: IncrementalPayload{
				ModelID:  "model.test",
				Position: 100,
				Interval: 50,
			},
			expected: "model.test:",
		},
		{
			name: "scheduled task unique ID",
			payload: ScheduledPayload{
				ModelID: "model.scheduled",
			},
			expected: "model.scheduled",
		},
		{
			name: "incremental with direction forward",
			payload: IncrementalPayload{
				ModelID:   "model.test",
				Position:  100,
				Interval:  50,
				Direction: DirectionForward,
			},
			expected: "model.test:forward",
		},
		{
			name: "incremental with direction back",
			payload: IncrementalPayload{
				ModelID:   "model.test",
				Position:  200,
				Interval:  50,
				Direction: DirectionBack,
			},
			expected: "model.test:back",
		},
		{
			name: "with special characters in model ID",
			payload: IncrementalPayload{
				ModelID:   "model.test-123_v2",
				Position:  500,
				Interval:  100,
				Direction: DirectionForward,
			},
			expected: "model.test-123_v2:forward",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.payload.UniqueID()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test Payload QueueName
func TestTaskPayload_QueueName(t *testing.T) {
	tests := []struct {
		name     string
		payload  Payload
		expected string
	}{
		{
			name: "incremental queue name",
			payload: IncrementalPayload{
				ModelID:  "model.test",
				Position: 100,
				Interval: 50,
			},
			expected: "model.test",
		},
		{
			name: "scheduled queue name",
			payload: ScheduledPayload{
				ModelID:       "model.scheduled",
				ExecutionTime: time.Now(),
			},
			expected: "model.scheduled",
		},
		{
			name: "queue name with special characters",
			payload: IncrementalPayload{
				ModelID:  "model.test-123_v2",
				Position: 100,
				Interval: 50,
			},
			expected: "model.test-123_v2",
		},
		{
			name: "empty model ID",
			payload: IncrementalPayload{
				ModelID:  "",
				Position: 100,
				Interval: 50,
			},
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.payload.QueueName()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test IncrementalPayload fields
func TestIncrementalTaskPayload_Fields(t *testing.T) {
	now := time.Now()

	payload := IncrementalPayload{
		ModelID:    "test.model",
		Position:   12345,
		Interval:   100,
		EnqueuedAt: now,
	}

	assert.Equal(t, "test.model", payload.ModelID)
	assert.Equal(t, uint64(12345), payload.Position)
	assert.Equal(t, uint64(100), payload.Interval)
	assert.Equal(t, now, payload.EnqueuedAt)
	assert.Equal(t, TypeIncremental, payload.GetType())
}

// Test ScheduledPayload fields
func TestScheduledTaskPayload_Fields(t *testing.T) {
	now := time.Now()
	execTime := now.Add(time.Hour)

	payload := ScheduledPayload{
		ModelID:       "test.model",
		ExecutionTime: execTime,
		EnqueuedAt:    now,
	}

	assert.Equal(t, "test.model", payload.ModelID)
	assert.Equal(t, execTime, payload.ExecutionTime)
	assert.Equal(t, now, payload.EnqueuedAt)
	assert.Equal(t, TypeScheduled, payload.GetType())
}

// Test Payload UniqueID consistency
func TestTaskPayload_UniqueID_Consistency(t *testing.T) {
	payload1 := IncrementalPayload{
		ModelID:   "model.test",
		Position:  100,
		Interval:  50,
		Direction: DirectionForward,
	}

	payload2 := IncrementalPayload{
		ModelID:   "model.test",
		Position:  100,
		Interval:  50,
		Direction: DirectionForward,
	}

	// Same payloads with same direction should produce same unique ID
	assert.Equal(t, payload1.UniqueID(), payload2.UniqueID())

	// Different position with same direction produces SAME unique ID (position not part of uniqueness)
	payload2.Position = 200
	assert.Equal(t, payload1.UniqueID(), payload2.UniqueID())

	// Different interval with same direction produces SAME unique ID (interval not part of uniqueness)
	payload2.Position = 100
	payload2.Interval = 100
	assert.Equal(t, payload1.UniqueID(), payload2.UniqueID())

	// Different model ID should produce different unique ID
	payload2.ModelID = "model.different"
	assert.NotEqual(t, payload1.UniqueID(), payload2.UniqueID())

	// Direction DOES affect uniqueness - only model.id and direction matter
	payload3 := IncrementalPayload{
		ModelID:   "model.test",
		Position:  100,
		Interval:  50,
		Direction: DirectionForward,
	}
	payload4 := IncrementalPayload{
		ModelID:   "model.test",
		Position:  100,
		Interval:  50,
		Direction: DirectionBack,
	}
	assert.NotEqual(t, payload3.UniqueID(), payload4.UniqueID())

	// Same direction but different position/interval produces SAME unique ID
	payload4.Direction = DirectionForward
	payload4.Position = 200
	payload4.Interval = 100
	assert.Equal(t, payload3.UniqueID(), payload4.UniqueID())

	// This prevents duplicate work when intervals expand (e.g., model:100:25 -> model:100:50)
	expandingInterval1 := IncrementalPayload{
		ModelID:   "model.test",
		Position:  100,
		Interval:  25,
		Direction: DirectionForward,
	}
	expandingInterval2 := IncrementalPayload{
		ModelID:   "model.test",
		Position:  100,
		Interval:  50,
		Direction: DirectionForward,
	}
	assert.Equal(t, expandingInterval1.UniqueID(), expandingInterval2.UniqueID(),
		"Tasks with different intervals at same position should have same ID to prevent duplication")
}

// Benchmark UniqueID generation for incremental
func BenchmarkIncrementalTaskPayload_UniqueID(b *testing.B) {
	payload := IncrementalPayload{
		ModelID:  "model.benchmark",
		Position: 1000000,
		Interval: 100,
	}

	b.ResetTimer()
	for range b.N {
		_ = payload.UniqueID()
	}
}

// Benchmark UniqueID generation for scheduled
func BenchmarkScheduledTaskPayload_UniqueID(b *testing.B) {
	payload := ScheduledPayload{
		ModelID:       "model.benchmark",
		ExecutionTime: time.Now(),
	}

	b.ResetTimer()
	for range b.N {
		_ = payload.UniqueID()
	}
}

// Benchmark QueueName
func BenchmarkTaskPayload_QueueName(b *testing.B) {
	payload := IncrementalPayload{
		ModelID:  "model.benchmark",
		Position: 1000000,
		Interval: 100,
	}

	b.ResetTimer()
	for range b.N {
		_ = payload.QueueName()
	}
}

// Test Payload JSON serialization with Type field
func TestIncrementalTaskPayload_JSONSerialization(t *testing.T) {
	now := time.Now().Truncate(time.Second) // Truncate for JSON round-trip

	payload := IncrementalPayload{
		Type:       TypeIncremental,
		ModelID:    "test.model",
		Position:   12345,
		Interval:   100,
		Direction:  DirectionForward,
		EnqueuedAt: now,
	}

	// Marshal to JSON
	data, err := json.Marshal(payload)
	require.NoError(t, err)

	// Verify Type field is present in JSON
	var jsonMap map[string]any

	err = json.Unmarshal(data, &jsonMap)
	require.NoError(t, err)
	assert.Equal(t, "incremental", jsonMap["type"])

	// Unmarshal back
	var decoded IncrementalPayload

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, TypeIncremental, decoded.Type)
	assert.Equal(t, payload.ModelID, decoded.ModelID)
	assert.Equal(t, payload.Position, decoded.Position)
	assert.Equal(t, payload.Interval, decoded.Interval)
	assert.Equal(t, payload.Direction, decoded.Direction)
}

func TestScheduledTaskPayload_JSONSerialization(t *testing.T) {
	now := time.Now().Truncate(time.Second) // Truncate for JSON round-trip
	execTime := now.Add(time.Hour)

	payload := ScheduledPayload{
		Type:          TypeScheduled,
		ModelID:       "test.model",
		ExecutionTime: execTime,
		EnqueuedAt:    now,
	}

	// Marshal to JSON
	data, err := json.Marshal(payload)
	require.NoError(t, err)

	// Verify Type field is present in JSON
	var jsonMap map[string]any

	err = json.Unmarshal(data, &jsonMap)
	require.NoError(t, err)
	assert.Equal(t, "scheduled", jsonMap["type"])

	// Unmarshal back
	var decoded ScheduledPayload

	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, TypeScheduled, decoded.Type)
	assert.Equal(t, payload.ModelID, decoded.ModelID)
}

// Test that Type field doesn't break existing GetType() behavior
func TestTaskPayload_GetType_WithTypeField(t *testing.T) {
	incPayload := IncrementalPayload{
		Type:      TypeIncremental,
		ModelID:   "test.model",
		Position:  100,
		Interval:  50,
		Direction: DirectionForward,
	}

	schPayload := ScheduledPayload{
		Type:          TypeScheduled,
		ModelID:       "test.model",
		ExecutionTime: time.Now(),
	}

	// GetType() should still return the correct constant regardless of Type field
	assert.Equal(t, TypeIncremental, incPayload.GetType())
	assert.Equal(t, TypeScheduled, schPayload.GetType())

	// Even with wrong Type field value, GetType() returns the hardcoded type
	// (This is intentional - GetType() is authoritative, Type field is for JSON discrimination)
	incPayload.Type = TypeScheduled
	assert.Equal(t, TypeIncremental, incPayload.GetType())
}
