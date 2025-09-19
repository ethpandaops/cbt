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
			name: "standard unique ID",
			payload: TaskPayload{
				ModelID:  "model.test",
				Position: 100,
				Interval: 50,
			},
			expected: "model.test:100",
		},
		{
			name: "zero values",
			payload: TaskPayload{
				ModelID:  "model.zero",
				Position: 0,
				Interval: 0,
			},
			expected: "model.zero:0",
		},
		{
			name: "large values",
			payload: TaskPayload{
				ModelID:  "model.large",
				Position: 18446744073709551615,
				Interval: 18446744073709551615,
			},
			expected: "model.large:18446744073709551615",
		},
		{
			name: "with special characters in model ID",
			payload: TaskPayload{
				ModelID:  "model.test-123_v2",
				Position: 500,
				Interval: 100,
			},
			expected: "model.test-123_v2:500",
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
			name: "standard queue name",
			payload: TaskPayload{
				ModelID:  "model.test",
				Position: 100,
				Interval: 50,
			},
			expected: "model.test",
		},
		{
			name: "queue name with special characters",
			payload: TaskPayload{
				ModelID:  "model.test-123_v2",
				Position: 100,
				Interval: 50,
			},
			expected: "model.test-123_v2",
		},
		{
			name: "empty model ID",
			payload: TaskPayload{
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

// Test TaskPayload fields
func TestTaskPayload_Fields(t *testing.T) {
	now := time.Now()

	payload := TaskPayload{
		ModelID:    "test.model",
		Position:   12345,
		Interval:   100,
		EnqueuedAt: now,
	}

	assert.Equal(t, "test.model", payload.ModelID)
	assert.Equal(t, uint64(12345), payload.Position)
	assert.Equal(t, uint64(100), payload.Interval)
	assert.Equal(t, now, payload.EnqueuedAt)
}

// Test TaskPayload UniqueID consistency
func TestTaskPayload_UniqueID_Consistency(t *testing.T) {
	payload1 := TaskPayload{
		ModelID:  "model.test",
		Position: 100,
		Interval: 50,
	}

	payload2 := TaskPayload{
		ModelID:  "model.test",
		Position: 100,
		Interval: 50,
	}

	// Same payloads should produce same unique ID
	assert.Equal(t, payload1.UniqueID(), payload2.UniqueID())

	// Different position should produce different unique ID
	payload2.Position = 200
	assert.NotEqual(t, payload1.UniqueID(), payload2.UniqueID())

	// Different interval should NOT affect unique ID anymore
	payload2.Position = 100
	payload2.Interval = 100
	assert.Equal(t, payload1.UniqueID(), payload2.UniqueID())

	// Different model ID should produce different unique ID
	payload2.Interval = 50
	payload2.ModelID = "model.different"
	assert.NotEqual(t, payload1.UniqueID(), payload2.UniqueID())
}

// Benchmark UniqueID generation
func BenchmarkTaskPayload_UniqueID(b *testing.B) {
	payload := TaskPayload{
		ModelID:  "model.benchmark",
		Position: 1000000,
		Interval: 100,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = payload.UniqueID()
	}
}

// Benchmark QueueName
func BenchmarkTaskPayload_QueueName(b *testing.B) {
	payload := TaskPayload{
		ModelID:  "model.benchmark",
		Position: 1000000,
		Interval: 100,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = payload.QueueName()
	}
}
