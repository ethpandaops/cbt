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
		[]string{"model", "operation"}, // operation: forward/backfill
	)

	// ScheduledTaskExecutions tracks scheduled task executions
	ScheduledTaskExecutions = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cbt_scheduled_task_executions_total",
			Help: "Total number of scheduled task executions",
		},
		[]string{"model", "operation", "status"}, // status: success/failure
	)

	// ArchivedTasksDeleted tracks tasks deleted from archive
	ArchivedTasksDeleted = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cbt_archived_tasks_deleted_total",
			Help: "Total number of archived tasks deleted",
		},
		[]string{"queue", "model"},
	)

	// ModelBounds tracks the min and max bounds for all models (external and transformations)
	ModelBounds = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cbt_model_bounds",
			Help: "Current bounds (min/max positions) for all models",
		},
		[]string{"model", "bound_type"}, // bound_type: min, max
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

// RecordExternalCacheMiss records a cache miss for external model bounds
func RecordExternalCacheMiss(model string) {
	ExternalCacheMisses.WithLabelValues(model).Inc()
}

// RecordArchivedTaskDeleted records an archived task being deleted
func RecordArchivedTaskDeleted(queue, model string) {
	ArchivedTasksDeleted.WithLabelValues(queue, model).Inc()
}

// RecordScheduledTaskRegistered records when a scheduled task is registered
func RecordScheduledTaskRegistered(model, operation string) {
	ScheduledTasksRegistered.WithLabelValues(model, operation).Inc()
}

// RecordScheduledTaskExecution records when a scheduled task is executed
func RecordScheduledTaskExecution(model, operation, status string) {
	ScheduledTaskExecutions.WithLabelValues(model, operation, status).Inc()
}

// RecordModelBounds records the min and max bounds for a model
func RecordModelBounds(model string, minBound, maxBound uint64) {
	ModelBounds.WithLabelValues(model, "min").Set(float64(minBound))
	ModelBounds.WithLabelValues(model, "max").Set(float64(maxBound))
}
