package handlers

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	"github.com/ethpandaops/cbt/pkg/api/generated"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListScheduledRuns(t *testing.T) {
	lastRun := time.Date(2026, 6, 12, 10, 0, 0, 0, time.UTC)

	t.Run("returns runs for scheduled models with database filter", func(t *testing.T) {
		dag := &testutil.FakeDAGReader{
			Transformations: []models.Transformation{
				scheduledTransformation("analytics.daily", "analytics", "daily"),
				scheduledTransformation("other.daily", "other", "daily"),
				incrementalTransformation("analytics.block_stats", "analytics", "block_stats"),
			},
		}
		adminSvc := &adminfake.FakeAdminService{
			GetAllLastScheduledExecutionsFn: func(_ context.Context, modelIDs []string) (map[string]*time.Time, error) {
				assert.Equal(t, []string{"analytics.daily"}, modelIDs)
				return map[string]*time.Time{"analytics.daily": &lastRun}, nil
			},
		}
		server := newTestServer(dag, adminSvc)

		app := fiber.New()
		app.Get("/runs", func(c fiber.Ctx) error {
			db := "analytics"
			return server.ListScheduledRuns(c, generated.ListScheduledRunsParams{Database: &db})
		})

		resp := doGet(t, app, "/runs")
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var body struct {
			Runs  []generated.ScheduledRun `json:"runs"`
			Total int                      `json:"total"`
		}
		decode(t, resp, &body)
		assert.Equal(t, 1, body.Total)
		require.Len(t, body.Runs, 1)
		assert.Equal(t, "analytics.daily", body.Runs[0].Id)
		require.NotNil(t, body.Runs[0].LastRun)
	})

	t.Run("includes run without last execution", func(t *testing.T) {
		dag := &testutil.FakeDAGReader{
			Transformations: []models.Transformation{
				scheduledTransformation("analytics.daily", "analytics", "daily"),
			},
		}
		server := newTestServer(dag, &adminfake.FakeAdminService{})

		app := fiber.New()
		app.Get("/runs", func(c fiber.Ctx) error {
			return server.ListScheduledRuns(c, generated.ListScheduledRunsParams{})
		})

		resp := doGet(t, app, "/runs")
		defer resp.Body.Close()

		var body struct {
			Runs  []generated.ScheduledRun `json:"runs"`
			Total int                      `json:"total"`
		}
		decode(t, resp, &body)
		assert.Equal(t, 1, body.Total)
		assert.Nil(t, body.Runs[0].LastRun)
	})

	t.Run("returns 500 when batch fetch fails", func(t *testing.T) {
		dag := &testutil.FakeDAGReader{
			Transformations: []models.Transformation{
				scheduledTransformation("analytics.daily", "analytics", "daily"),
			},
		}
		adminSvc := &adminfake.FakeAdminService{
			GetAllLastScheduledExecutionsFn: func(_ context.Context, _ []string) (map[string]*time.Time, error) {
				return nil, errInjected
			},
		}
		server := newTestServer(dag, adminSvc)

		app := newErrorApp()
		app.Get("/runs", func(c fiber.Ctx) error {
			return server.ListScheduledRuns(c, generated.ListScheduledRunsParams{})
		})

		resp := doGet(t, app, "/runs")
		defer resp.Body.Close()
		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	})
}

func TestGetScheduledRun(t *testing.T) {
	lastRun := time.Date(2026, 6, 12, 10, 0, 0, 0, time.UTC)

	t.Run("returns run with last execution", func(t *testing.T) {
		dag := &testutil.FakeDAGReader{
			TransformationByID: map[string]models.Transformation{
				"analytics.daily": scheduledTransformation("analytics.daily", "analytics", "daily"),
			},
		}
		adminSvc := &adminfake.FakeAdminService{
			GetLastScheduledExecutionFn: func(_ context.Context, _ string) (*time.Time, error) {
				return &lastRun, nil
			},
		}
		server := newTestServer(dag, adminSvc)

		app := newErrorApp()
		app.Get("/runs/:id", func(c fiber.Ctx) error {
			return server.GetScheduledRun(c, c.Params("id"))
		})

		resp := doGet(t, app, "/runs/analytics.daily")
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var run generated.ScheduledRun
		decode(t, resp, &run)
		assert.Equal(t, "analytics.daily", run.Id)
		require.NotNil(t, run.LastRun)
	})

	t.Run("returns run without last execution", func(t *testing.T) {
		dag := &testutil.FakeDAGReader{
			TransformationByID: map[string]models.Transformation{
				"analytics.daily": scheduledTransformation("analytics.daily", "analytics", "daily"),
			},
		}
		server := newTestServer(dag, &adminfake.FakeAdminService{})

		app := newErrorApp()
		app.Get("/runs/:id", func(c fiber.Ctx) error {
			return server.GetScheduledRun(c, c.Params("id"))
		})

		resp := doGet(t, app, "/runs/analytics.daily")
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var run generated.ScheduledRun
		decode(t, resp, &run)
		assert.Nil(t, run.LastRun)
	})

	t.Run("returns 404 when model not found", func(t *testing.T) {
		dag := &testutil.FakeDAGReader{GetTransformationErr: errInjected}
		server := newTestServer(dag, &adminfake.FakeAdminService{})

		app := newErrorApp()
		app.Get("/runs/:id", func(c fiber.Ctx) error {
			return server.GetScheduledRun(c, c.Params("id"))
		})

		resp := doGet(t, app, "/runs/missing.model")
		defer resp.Body.Close()
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	})

	t.Run("returns 400 when model is not scheduled", func(t *testing.T) {
		dag := &testutil.FakeDAGReader{
			TransformationByID: map[string]models.Transformation{
				"analytics.block_stats": incrementalTransformation("analytics.block_stats", "analytics", "block_stats"),
			},
		}
		server := newTestServer(dag, &adminfake.FakeAdminService{})

		app := newErrorApp()
		app.Get("/runs/:id", func(c fiber.Ctx) error {
			return server.GetScheduledRun(c, c.Params("id"))
		})

		resp := doGet(t, app, "/runs/analytics.block_stats")
		defer resp.Body.Close()
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})

	t.Run("returns 500 when execution fetch fails", func(t *testing.T) {
		dag := &testutil.FakeDAGReader{
			TransformationByID: map[string]models.Transformation{
				"analytics.daily": scheduledTransformation("analytics.daily", "analytics", "daily"),
			},
		}
		adminSvc := &adminfake.FakeAdminService{
			GetLastScheduledExecutionFn: func(_ context.Context, _ string) (*time.Time, error) {
				return nil, errInjected
			},
		}
		server := newTestServer(dag, adminSvc)

		app := newErrorApp()
		app.Get("/runs/:id", func(c fiber.Ctx) error {
			return server.GetScheduledRun(c, c.Params("id"))
		})

		resp := doGet(t, app, "/runs/analytics.daily")
		defer resp.Body.Close()
		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	})
}
