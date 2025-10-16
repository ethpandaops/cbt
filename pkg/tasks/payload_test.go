package tasks

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Test TaskPayload UniqueID
func TestTaskPayload_UniqueID(t *testing.T) {
	tests := []struct {
		name     string
		payload  TaskPayload
		expected string
	}{
		{
			name: "incremental task unique ID without direction",
			payload: IncrementalTaskPayload{
				ModelID:  "model.test",
				Position: 100,
				Interval: 50,
			},
			expected: "model.test:",
		},
		{
			name: "scheduled task unique ID",
			payload: ScheduledTaskPayload{
				ModelID: "model.scheduled",
			},
			expected: "model.scheduled",
		},
		{
			name: "incremental with direction forward",
			payload: IncrementalTaskPayload{
				ModelID:   "model.test",
				Position:  100,
				Interval:  50,
				Direction: DirectionForward,
			},
			expected: "model.test:forward",
		},
		{
			name: "incremental with direction back",
			payload: IncrementalTaskPayload{
				ModelID:   "model.test",
				Position:  200,
				Interval:  50,
				Direction: DirectionBack,
			},
			expected: "model.test:back",
		},
		{
			name: "with special characters in model ID",
			payload: IncrementalTaskPayload{
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

// Test TaskPayload QueueName
func TestTaskPayload_QueueName(t *testing.T) {
	tests := []struct {
		name     string
		payload  TaskPayload
		expected string
	}{
		{
			name: "incremental queue name",
			payload: IncrementalTaskPayload{
				ModelID:  "model.test",
				Position: 100,
				Interval: 50,
			},
			expected: "model.test",
		},
		{
			name: "scheduled queue name",
			payload: ScheduledTaskPayload{
				ModelID:       "model.scheduled",
				ExecutionTime: time.Now(),
			},
			expected: "model.scheduled",
		},
		{
			name: "queue name with special characters",
			payload: IncrementalTaskPayload{
				ModelID:  "model.test-123_v2",
				Position: 100,
				Interval: 50,
			},
			expected: "model.test-123_v2",
		},
		{
			name: "empty model ID",
			payload: IncrementalTaskPayload{
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

// Test IncrementalTaskPayload fields
func TestIncrementalTaskPayload_Fields(t *testing.T) {
	now := time.Now()

	payload := IncrementalTaskPayload{
		ModelID:    "test.model",
		Position:   12345,
		Interval:   100,
		EnqueuedAt: now,
	}

	assert.Equal(t, "test.model", payload.ModelID)
	assert.Equal(t, uint64(12345), payload.Position)
	assert.Equal(t, uint64(100), payload.Interval)
	assert.Equal(t, now, payload.EnqueuedAt)
	assert.Equal(t, TaskTypeIncremental, payload.GetType())
}

// Test ScheduledTaskPayload fields
func TestScheduledTaskPayload_Fields(t *testing.T) {
	now := time.Now()
	execTime := now.Add(time.Hour)

	payload := ScheduledTaskPayload{
		ModelID:       "test.model",
		ExecutionTime: execTime,
		EnqueuedAt:    now,
	}

	assert.Equal(t, "test.model", payload.ModelID)
	assert.Equal(t, execTime, payload.ExecutionTime)
	assert.Equal(t, now, payload.EnqueuedAt)
	assert.Equal(t, TaskTypeScheduled, payload.GetType())
}

// Test TaskPayload UniqueID consistency
func TestTaskPayload_UniqueID_Consistency(t *testing.T) {
	payload1 := IncrementalTaskPayload{
		ModelID:   "model.test",
		Position:  100,
		Interval:  50,
		Direction: DirectionForward,
	}

	payload2 := IncrementalTaskPayload{
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
	payload3 := IncrementalTaskPayload{
		ModelID:   "model.test",
		Position:  100,
		Interval:  50,
		Direction: DirectionForward,
	}
	payload4 := IncrementalTaskPayload{
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
	expandingInterval1 := IncrementalTaskPayload{
		ModelID:   "model.test",
		Position:  100,
		Interval:  25,
		Direction: DirectionForward,
	}
	expandingInterval2 := IncrementalTaskPayload{
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
	payload := IncrementalTaskPayload{
		ModelID:  "model.benchmark",
		Position: 1000000,
		Interval: 100,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = payload.UniqueID()
	}
}

// Benchmark UniqueID generation for scheduled
func BenchmarkScheduledTaskPayload_UniqueID(b *testing.B) {
	payload := ScheduledTaskPayload{
		ModelID:       "model.benchmark",
		ExecutionTime: time.Now(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = payload.UniqueID()
	}
}

// Benchmark QueueName
func BenchmarkTaskPayload_QueueName(b *testing.B) {
	payload := IncrementalTaskPayload{
		ModelID:  "model.benchmark",
		Position: 1000000,
		Interval: 100,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = payload.QueueName()
	}
}
