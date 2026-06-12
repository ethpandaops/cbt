package observability

import (
	"bufio"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scrapeMetrics returns the default registry's exposition text via promhttp.
// It avoids the prometheus/testutil package, which is not a direct dependency.
func scrapeMetrics(t *testing.T) string {
	t.Helper()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody)
	promhttp.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	return rec.Body.String()
}

// metricValue extracts the float value of the single exposition line whose
// series prefix matches series (e.g. `cbt_tasks_total{model="m",status="ok"}`).
// It fails the test if the series is not present.
func metricValue(t *testing.T, body, series string) float64 {
	t.Helper()

	scanner := bufio.NewScanner(strings.NewReader(body))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}

		idx := strings.LastIndex(line, " ")
		if idx < 0 {
			continue
		}

		if line[:idx] != series {
			continue
		}

		v, err := strconv.ParseFloat(line[idx+1:], 64)
		require.NoError(t, err)

		return v
	}

	t.Fatalf("series %q not found in metrics output", series)

	return 0
}

func TestRecordTaskStart(t *testing.T) {
	RecordTaskStart("m_start", "w1")

	body := scrapeMetrics(t)
	assert.InDelta(t, 1,
		metricValue(t, body, `cbt_tasks_running{model="m_start",worker="w1"}`), 0.0001)
}

func TestRecordTaskComplete(t *testing.T) {
	RecordTaskStart("m_complete", "w1")
	RecordTaskComplete("m_complete", "w1", "success", 1.5)

	body := scrapeMetrics(t)
	assert.InDelta(t, 0,
		metricValue(t, body, `cbt_tasks_running{model="m_complete",worker="w1"}`), 0.0001)
	assert.InDelta(t, 1,
		metricValue(t, body, `cbt_tasks_total{model="m_complete",status="success"}`), 0.0001)
	assert.InDelta(t, 1,
		metricValue(t, body, `cbt_task_duration_seconds_count{model="m_complete",status="success"}`), 0.0001)
}

func TestRecordDependencyValidation(t *testing.T) {
	RecordDependencyValidation("m_dep", "satisfied", 0.01)

	body := scrapeMetrics(t)
	assert.InDelta(t, 1,
		metricValue(t, body,
			`cbt_dependency_validation_total{model="m_dep",result="satisfied"}`), 0.0001)
	assert.InDelta(t, 1,
		metricValue(t, body,
			`cbt_dependency_validation_duration_seconds_count{model="m_dep"}`), 0.0001)
}

func TestRecordTaskEnqueued(t *testing.T) {
	RecordTaskEnqueued("m_enqueue")
	RecordTaskEnqueued("m_enqueue")

	body := scrapeMetrics(t)
	assert.InDelta(t, 2,
		metricValue(t, body, `cbt_tasks_enqueued_total{model="m_enqueue"}`), 0.0001)
}

func TestRecordError(t *testing.T) {
	RecordError("coordinator", "timeout")

	body := scrapeMetrics(t)
	assert.InDelta(t, 1,
		metricValue(t, body,
			`cbt_errors_total{component="coordinator",error_type="timeout"}`), 0.0001)
}

func TestRecordExternalCacheMiss(t *testing.T) {
	RecordExternalCacheMiss("m_cache")

	body := scrapeMetrics(t)
	assert.InDelta(t, 1,
		metricValue(t, body, `cbt_external_cache_misses_total{model="m_cache"}`), 0.0001)
}

func TestRecordArchivedTaskDeleted(t *testing.T) {
	RecordArchivedTaskDeleted("default", "m_arch")

	body := scrapeMetrics(t)
	assert.InDelta(t, 1,
		metricValue(t, body,
			`cbt_archived_tasks_deleted_total{model="m_arch",queue="default"}`), 0.0001)
}

func TestRecordScheduledTaskRegisteredAndUnregistered(t *testing.T) {
	RecordScheduledTaskRegistered("m_sched", "forward")

	body := scrapeMetrics(t)
	assert.InDelta(t, 1,
		metricValue(t, body,
			`cbt_scheduled_tasks_registered{model="m_sched",operation="forward"}`), 0.0001)

	RecordScheduledTaskUnregistered("m_sched", "forward")

	body = scrapeMetrics(t)
	assert.InDelta(t, 0,
		metricValue(t, body,
			`cbt_scheduled_tasks_registered{model="m_sched",operation="forward"}`), 0.0001)
}

func TestRecordScheduledTaskExecution(t *testing.T) {
	RecordScheduledTaskExecution("m_exec", "backfill", "failure")

	body := scrapeMetrics(t)
	assert.InDelta(t, 1,
		metricValue(t, body,
			`cbt_scheduled_task_executions_total{model="m_exec",operation="backfill",status="failure"}`),
		0.0001)
}

func TestRecordModelBounds(t *testing.T) {
	RecordModelBounds("m_bounds", 100, 200)

	body := scrapeMetrics(t)
	assert.InDelta(t, 100,
		metricValue(t, body, `cbt_model_bounds{bound_type="min",model="m_bounds"}`), 0.0001)
	assert.InDelta(t, 200,
		metricValue(t, body, `cbt_model_bounds{bound_type="max",model="m_bounds"}`), 0.0001)
}

func TestRecordBackfillGapAnalysis(t *testing.T) {
	RecordBackfillGapAnalysis("m_gap", "fillable")

	body := scrapeMetrics(t)
	assert.InDelta(t, 1,
		metricValue(t, body,
			`cbt_backfill_gaps_analyzed_total{model="m_gap",result="fillable"}`), 0.0001)
}

func TestRecordBackfillGapCheckLimitReached(t *testing.T) {
	RecordBackfillGapCheckLimitReached("m_gaplimit")

	body := scrapeMetrics(t)
	assert.InDelta(t, 1,
		metricValue(t, body,
			`cbt_backfill_gap_check_limit_reached_total{model="m_gaplimit"}`), 0.0001)
}
