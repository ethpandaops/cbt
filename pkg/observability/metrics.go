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

	// ModelCoverage measures the percentage of data coverage
	ModelCoverage = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cbt_model_coverage_percent",
			Help: "Percentage of data coverage for the model",
		},
		[]string{"model"},
	)

	// ModelGaps counts the number of gaps in model data
	ModelGaps = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cbt_model_gaps_count",
			Help: "Number of gaps in model data",
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

	// ClickHouseQueries counts total number of ClickHouse queries executed
	ClickHouseQueries = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cbt_clickhouse_queries_total",
			Help: "Total number of ClickHouse queries executed",
		},
		[]string{"query_type", "status"}, // query_type: select, insert, delete; status: success, error
	)

	// ClickHouseQueryDuration measures ClickHouse query execution time
	ClickHouseQueryDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "cbt_clickhouse_query_duration_seconds",
			Help:    "ClickHouse query execution time",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 10), // 10ms to ~10s
		},
		[]string{"query_type"},
	)

	// ClickHouseRowsProcessed counts total number of rows processed
	ClickHouseRowsProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cbt_clickhouse_rows_processed_total",
			Help: "Total number of rows processed in ClickHouse",
		},
		[]string{"model", "operation"}, // operation: insert, update, delete
	)

	// SchedulerActive indicates whether scheduler is active for model
	SchedulerActive = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cbt_scheduler_active",
			Help: "Whether scheduler is active for model (1=active, 0=inactive)",
		},
		[]string{"model"},
	)

	// TasksEnqueued counts total number of tasks enqueued
	TasksEnqueued = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cbt_tasks_enqueued_total",
			Help: "Total number of tasks enqueued",
		},
		[]string{"model", "trigger"}, // trigger: schedule, dependency, backfill, manual
	)

	// BackfillProgress tracks backfill progress percentage
	BackfillProgress = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cbt_backfill_progress_percent",
			Help: "Backfill progress percentage",
		},
		[]string{"model"},
	)

	// QueueDepth measures number of tasks in queue
	QueueDepth = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cbt_queue_depth",
			Help: "Number of tasks in queue",
		},
		[]string{"queue", "state"}, // state: pending, active, scheduled, retry
	)

	// QueueMemoryUsage tracks queue memory usage in bytes
	QueueMemoryUsage = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cbt_queue_memory_bytes",
			Help: "Queue memory usage in bytes",
		},
		[]string{"queue"},
	)

	// WorkerUtilization measures worker utilization percentage
	WorkerUtilization = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cbt_worker_utilization_percent",
			Help: "Worker utilization percentage",
		},
		[]string{"worker"},
	)

	// RedisConnections counts number of Redis connections
	RedisConnections = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cbt_redis_connections",
			Help: "Number of Redis connections",
		},
		[]string{"state"}, // state: active, idle
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

// RecordClickHouseQuery records ClickHouse query metrics
func RecordClickHouseQuery(queryType, status string, duration float64) {
	ClickHouseQueries.WithLabelValues(queryType, status).Inc()
	ClickHouseQueryDuration.WithLabelValues(queryType).Observe(duration)
}

// RecordClickHouseRows records rows processed
func RecordClickHouseRows(model, operation string, count float64) {
	ClickHouseRowsProcessed.WithLabelValues(model, operation).Add(count)
}

// RecordTaskEnqueued records task enqueue
func RecordTaskEnqueued(model, trigger string) {
	TasksEnqueued.WithLabelValues(model, trigger).Inc()
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
