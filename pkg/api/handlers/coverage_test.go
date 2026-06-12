package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/api/generated"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func incrementalTransformation(id, database, table string) *testutil.FakeTransformation {
	return &testutil.FakeTransformation{
		ID: id,
		Config: transformation.Config{
			Database: database,
			Table:    table,
			Type:     transformation.TypeIncremental,
		},
	}
}

func scheduledTransformation(id, database, table string) *testutil.FakeTransformation {
	return &testutil.FakeTransformation{
		ID: id,
		Config: transformation.Config{
			Database: database,
			Table:    table,
			Type:     transformation.TypeScheduled,
		},
	}
}

func TestListTransformationCoverage(t *testing.T) {
	t.Run("returns coverage for incremental models with database filter", func(t *testing.T) {
		dag := &testutil.FakeDAGReader{
			Transformations: []models.Transformation{
				incrementalTransformation("analytics.block_stats", "analytics", "block_stats"),
				incrementalTransformation("other.model", "other", "model"),
				scheduledTransformation("analytics.scheduled", "analytics", "scheduled"),
			},
		}
		adminSvc := &adminfake.FakeAdminService{
			GetAllProcessedRangesFn: func(_ context.Context, modelIDs []string) (map[string][]admin.ProcessedRange, error) {
				assert.Equal(t, []string{"analytics.block_stats"}, modelIDs)
				return map[string][]admin.ProcessedRange{
					"analytics.block_stats": {{Position: 100, Interval: 50}},
				}, nil
			},
		}
		server := newTestServer(dag, adminSvc)

		app := fiber.New()
		app.Get("/cov", func(c fiber.Ctx) error {
			db := "analytics"
			return server.ListTransformationCoverage(c, generated.ListTransformationCoverageParams{Database: &db})
		})

		resp := doGet(t, app, "/cov")
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var body struct {
			Coverage []generated.CoverageSummary `json:"coverage"`
			Total    int                         `json:"total"`
		}
		decode(t, resp, &body)
		assert.Equal(t, 1, body.Total)
		require.Len(t, body.Coverage, 1)
		assert.Equal(t, "analytics.block_stats", body.Coverage[0].Id)
		require.Len(t, body.Coverage[0].Ranges, 1)
		assert.Equal(t, 100, body.Coverage[0].Ranges[0].Position)
		assert.Equal(t, 50, body.Coverage[0].Ranges[0].Interval)
	})

	t.Run("returns 500 when batch fetch fails", func(t *testing.T) {
		dag := &testutil.FakeDAGReader{
			Transformations: []models.Transformation{
				incrementalTransformation("analytics.block_stats", "analytics", "block_stats"),
			},
		}
		adminSvc := &adminfake.FakeAdminService{
			GetAllProcessedRangesFn: func(_ context.Context, _ []string) (map[string][]admin.ProcessedRange, error) {
				return nil, errInjected
			},
		}
		server := newTestServer(dag, adminSvc)

		app := newErrorApp()
		app.Get("/cov", func(c fiber.Ctx) error {
			return server.ListTransformationCoverage(c, generated.ListTransformationCoverageParams{})
		})

		resp := doGet(t, app, "/cov")
		defer resp.Body.Close()
		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	})
}

func TestGetTransformationCoverage(t *testing.T) {
	t.Run("returns coverage detail", func(t *testing.T) {
		dag := &testutil.FakeDAGReader{
			TransformationByID: map[string]models.Transformation{
				"analytics.block_stats": incrementalTransformation("analytics.block_stats", "analytics", "block_stats"),
			},
		}
		adminSvc := &adminfake.FakeAdminService{
			GetProcessedRangesFn: func(_ context.Context, _ string) ([]admin.ProcessedRange, error) {
				return []admin.ProcessedRange{{Position: 10, Interval: 5}}, nil
			},
		}
		server := newTestServer(dag, adminSvc)

		app := newErrorApp()
		app.Get("/cov/:id", func(c fiber.Ctx) error {
			return server.GetTransformationCoverage(c, c.Params("id"))
		})

		resp := doGet(t, app, "/cov/analytics.block_stats")
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var detail generated.CoverageDetail
		decode(t, resp, &detail)
		assert.Equal(t, "analytics.block_stats", detail.Id)
		require.Len(t, detail.Ranges, 1)
	})

	t.Run("returns 404 when model not found", func(t *testing.T) {
		dag := &testutil.FakeDAGReader{GetTransformationErr: errInjected}
		server := newTestServer(dag, &adminfake.FakeAdminService{})

		app := newErrorApp()
		app.Get("/cov/:id", func(c fiber.Ctx) error {
			return server.GetTransformationCoverage(c, c.Params("id"))
		})

		resp := doGet(t, app, "/cov/missing.model")
		defer resp.Body.Close()
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	})

	t.Run("returns 400 when model is not incremental", func(t *testing.T) {
		dag := &testutil.FakeDAGReader{
			TransformationByID: map[string]models.Transformation{
				"analytics.scheduled": scheduledTransformation("analytics.scheduled", "analytics", "scheduled"),
			},
		}
		server := newTestServer(dag, &adminfake.FakeAdminService{})

		app := newErrorApp()
		app.Get("/cov/:id", func(c fiber.Ctx) error {
			return server.GetTransformationCoverage(c, c.Params("id"))
		})

		resp := doGet(t, app, "/cov/analytics.scheduled")
		defer resp.Body.Close()
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})

	t.Run("returns 500 when ranges fetch fails", func(t *testing.T) {
		dag := &testutil.FakeDAGReader{
			TransformationByID: map[string]models.Transformation{
				"analytics.block_stats": incrementalTransformation("analytics.block_stats", "analytics", "block_stats"),
			},
		}
		adminSvc := &adminfake.FakeAdminService{
			GetProcessedRangesFn: func(_ context.Context, _ string) ([]admin.ProcessedRange, error) {
				return nil, errInjected
			},
		}
		server := newTestServer(dag, adminSvc)

		app := newErrorApp()
		app.Get("/cov/:id", func(c fiber.Ctx) error {
			return server.GetTransformationCoverage(c, c.Params("id"))
		})

		resp := doGet(t, app, "/cov/analytics.block_stats")
		defer resp.Body.Close()
		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	})
}

// --- shared HTTP test helpers ---

func newErrorApp() *fiber.App {
	return fiber.New(fiber.Config{
		ErrorHandler: func(c fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError
			message := "Internal Server Error"
			var fiberErr *fiber.Error
			if errors.As(err, &fiberErr) {
				code = fiberErr.Code
				message = fiberErr.Message
			}
			return c.Status(code).JSON(fiber.Map{"error": message, "code": code})
		},
	})
}

func doGet(t *testing.T, app *fiber.App, target string) *http.Response {
	t.Helper()
	req := httptest.NewRequest("GET", target, http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	return resp
}

func decode(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(body, v))
}
