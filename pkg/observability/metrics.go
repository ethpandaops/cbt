package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//nolint:gochecknoglobals // Prometheus metrics must be global for registration
var (
	// TasksTotal tracks the total number of tasks processed
	TasksTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cbt_tasks_total",
			Help: "Total number of tasks processed",
		},
		[]string{"model", "status"}, // status: success, failed, retried
	)

	// TaskDuration measures task execution duration in seconds
	TaskDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "cbt_task_duration_seconds",
			Help:    "Task execution duration in seconds",
			Buckets: prometheus.ExponentialBuckets(0.1, 2, 10), // 0.1s to ~100s
		},
		[]string{"model", "status"},
	)

	// TasksRunning tracks the number of currently running tasks
	TasksRunning = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cbt_tasks_running",
			Help: "Number of currently running tasks",
		},
		[]string{"model", "worker"},
	)

	// ModelPositionLag measures how far behind real-time the model is
	ModelPositionLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cbt_model_position_lag_seconds",
			Help: "How far behind real-time the model is (in seconds)",
		},
		[]string{"model"},
	)

	// ModelLastPosition tracks the last processed position timestamp
	ModelLastPosition = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cbt_model_last_position",
			Help: "Last processed position (unix timestamp)",
		},
		[]string{"model"},
	)

	// DependencyValidationTotal counts total dependency validation attempts
	DependencyValidationTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cbt_dependency_validation_total",
			Help: "Total dependency validation attempts",
		},
		[]string{"model", "result"}, // result: satisfied, not_satisfied, error
	)

	// DependencyValidationDuration measures time taken to validate dependencies
	DependencyValidationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "cbt_dependency_validation_duration_seconds",
			Help:    "Time taken to validate dependencies",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 10), // 1ms to ~1s
		},
		[]string{"model"},
	)

	// TasksEnqueued counts total number of tasks enqueued
	TasksEnqueued = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cbt_tasks_enqueued_total",
			Help: "Total number of tasks enqueued",
		},
		[]string{"model"},
	)

	// ErrorsTotal counts total number of errors
	ErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cbt_errors_total",
			Help: "Total number of errors",
		},
		[]string{"component", "error_type"},
	)

	// ExternalCacheHits tracks cache hits for external model bounds
	ExternalCacheHits = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cbt_external_cache_hits_total",
			Help: "Total number of cache hits for external model bounds",
		},
		[]string{"model"},
	)

	// ExternalCacheMisses tracks cache misses for external model bounds
	ExternalCacheMisses = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cbt_external_cache_misses_total",
			Help: "Total number of cache misses for external model bounds",
		},
		[]string{"model"},
	)

	// ScheduledTasksRegistered tracks registered scheduled tasks
	ScheduledTasksRegistered = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cbt_scheduled_tasks_registered",
			Help: "Number of scheduled tasks currently registered",
		},
		[]string{"model_id", "operation"}, // operation: forward/backfill
	)

	// ScheduledTaskExecutions tracks scheduled task executions
	ScheduledTaskExecutions = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cbt_scheduled_task_executions_total",
			Help: "Total number of scheduled task executions",
		},
		[]string{"model_id", "operation", "status"}, // status: success/failure
	)
)

// RecordTaskStart records the start of a task
func RecordTaskStart(model, worker string) {
	TasksRunning.WithLabelValues(model, worker).Inc()
}

// RecordTaskComplete records task completion
func RecordTaskComplete(model, worker, status string, duration float64) {
	TasksRunning.WithLabelValues(model, worker).Dec()
	TasksTotal.WithLabelValues(model, status).Inc()
	TaskDuration.WithLabelValues(model, status).Observe(duration)
}

// RecordDependencyValidation records dependency validation metrics
func RecordDependencyValidation(model, result string, duration float64) {
	DependencyValidationTotal.WithLabelValues(model, result).Inc()
	DependencyValidationDuration.WithLabelValues(model).Observe(duration)
}

// RecordTaskEnqueued records task enqueue
func RecordTaskEnqueued(model string) {
	TasksEnqueued.WithLabelValues(model).Inc()
}

// RecordError records an error
func RecordError(component, errorType string) {
	ErrorsTotal.WithLabelValues(component, errorType).Inc()
}

// RecordExternalCacheHit records a cache hit for external model bounds
func RecordExternalCacheHit(model string) {
	ExternalCacheHits.WithLabelValues(model).Inc()
}

// RecordExternalCacheMiss records a cache miss for external model bounds
func RecordExternalCacheMiss(model string) {
	ExternalCacheMisses.WithLabelValues(model).Inc()
}
