package management

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	"github.com/ethpandaops/cbt/internal/testutil/coordinatorfake"
	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/coordinator"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/gofiber/fiber/v3"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var errBoom = errors.New("boom")

// testLogger returns a quiet logrus logger for tests.
func testLogger() logrus.FieldLogger {
	log := logrus.New()
	log.SetLevel(logrus.PanicLevel)

	return log
}

// fakeBaseConfigProvider is a configurable BaseConfigProvider for tests.
type fakeBaseConfigProvider struct {
	cfg json.RawMessage
	err error
}

func (f *fakeBaseConfigProvider) GetBaseConfig(_ string) (json.RawMessage, error) {
	return f.cfg, f.err
}

// failingBoundsLock returns an error from Unlock; used to exercise the deferred
// unlock branch without affecting the handler result.
type failingBoundsLock struct{}

func (failingBoundsLock) Unlock(_ context.Context) error { return errBoom }

// doJSON sends a request to the app and returns status + decoded JSON map.
func doJSON(
	t *testing.T,
	app *fiber.App,
	method, target string,
	body any,
) (status int, out map[string]any) {
	t.Helper()

	var reader io.Reader = http.NoBody

	if body != nil {
		switch b := body.(type) {
		case string:
			reader = bytes.NewBufferString(b)
		default:
			raw, err := json.Marshal(body)
			require.NoError(t, err)
			reader = bytes.NewBuffer(raw)
		}
	}

	req := httptest.NewRequest(method, target, reader)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := app.Test(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	out = map[string]any{}
	if len(raw) > 0 && raw[0] == '{' {
		require.NoError(t, json.Unmarshal(raw, &out))
	}

	return resp.StatusCode, out
}

// newHandlersApp wires the given handlers onto a fiber app mirroring the
// production routes so each handler can be exercised end to end.
func newHandlersApp(h *Handlers) *fiber.App {
	app := fiber.New()
	app.Post("/models/:id/delete-period", h.DeletePeriod)
	app.Post("/models/:id/consolidate", h.Consolidate)
	app.Put("/models/:id/bounds", h.UpdateBounds)
	app.Delete("/models/:id/bounds", h.DeleteBounds)
	app.Post("/models/:id/refresh-bounds", h.TriggerRefreshBounds)
	app.Post("/models/:id/run-now", h.TriggerScheduledRun)
	app.Get("/models/:id/config-override", h.GetConfigOverride)
	app.Put("/models/:id/config-override", h.SetConfigOverride)
	app.Delete("/models/:id/config-override", h.DeleteConfigOverride)
	app.Get("/config-overrides", h.ListConfigOverrides)
	app.Delete("/config-overrides", h.ClearAllConfigOverrides)

	return app
}

func TestNewHandlers(t *testing.T) {
	t.Parallel()

	adminSvc := &adminfake.FakeAdminService{}
	modelsSvc := &testutil.FakeModelsService{}
	coord := &coordinatorfake.FakeCoordinator{}

	h := NewHandlers(adminSvc, modelsSvc, coord, testLogger())
	require.NotNil(t, h)
	require.Equal(t, admin.Service(adminSvc), h.adminService)
	require.Nil(t, h.baseConfigProvider)

	provider := &fakeBaseConfigProvider{}
	h.SetBaseConfigProvider(provider)
	require.Equal(t, BaseConfigProvider(provider), h.baseConfigProvider)
}

func TestDeletePeriod(t *testing.T) {
	t.Parallel()

	trackHandler := &testutil.FakeHandler{}
	noTrack := false
	noTrackHandler := &testutil.FakeHandler{TrackPosition: &noTrack}

	tests := []struct {
		name       string
		id         string
		body       any
		admin      *adminfake.FakeAdminService
		dag        *testutil.FakeDAGReader
		wantStatus int
		check      func(t *testing.T, out map[string]any)
	}{
		{
			name:       "invalid model id",
			id:         "no-dot",
			body:       deletePeriodRequest{StartPos: 1, EndPos: 2},
			admin:      &adminfake.FakeAdminService{},
			wantStatus: fiber.StatusBadRequest,
		},
		{
			name:       "invalid body",
			id:         "db.table",
			body:       "{not-json",
			admin:      &adminfake.FakeAdminService{},
			wantStatus: fiber.StatusBadRequest,
		},
		{
			name:       "start >= end",
			id:         "db.table",
			body:       deletePeriodRequest{StartPos: 5, EndPos: 5},
			admin:      &adminfake.FakeAdminService{},
			wantStatus: fiber.StatusBadRequest,
		},
		{
			name: "admin delete error",
			id:   "db.table",
			body: deletePeriodRequest{StartPos: 1, EndPos: 10},
			admin: &adminfake.FakeAdminService{
				DeletePeriodFn: func(_ context.Context, _ string, _, _ uint64) (uint64, error) {
					return 0, errBoom
				},
			},
			wantStatus: fiber.StatusInternalServerError,
		},
		{
			name: "success no cascade",
			id:   "db.table",
			body: deletePeriodRequest{StartPos: 1, EndPos: 10},
			admin: &adminfake.FakeAdminService{
				DeletePeriodFn: func(_ context.Context, _ string, _, _ uint64) (uint64, error) {
					return 42, nil
				},
			},
			wantStatus: fiber.StatusOK,
			check: func(t *testing.T, out map[string]any) {
				t.Helper()
				require.InDelta(t, 42, out["deleted_rows"], 0)
				require.Nil(t, out["cascade_results"])
			},
		},
		{
			name: "cascade with mixed dependents",
			id:   "db.table",
			body: deletePeriodRequest{StartPos: 1, EndPos: 10, Cascade: true},
			admin: &adminfake.FakeAdminService{
				DeletePeriodFn: func(_ context.Context, modelID string, _, _ uint64) (uint64, error) {
					switch modelID {
					case "db.dep_err":
						return 0, errBoom
					default:
						return 7, nil
					}
				},
			},
			dag: &testutil.FakeDAGReader{
				AllDependents: map[string][]string{
					"db.table": {"db.dep_ok", "db.dep_missing", "db.dep_notrack", "db.dep_nilhandler", "db.dep_err"},
				},
				TransformationByID: map[string]models.Transformation{
					"db.dep_ok":         &testutil.FakeTransformation{ID: "db.dep_ok", Handler: trackHandler},
					"db.dep_notrack":    &testutil.FakeTransformation{ID: "db.dep_notrack", Handler: noTrackHandler},
					"db.dep_nilhandler": &testutil.FakeTransformation{ID: "db.dep_nilhandler"},
					"db.dep_err":        &testutil.FakeTransformation{ID: "db.dep_err", Handler: trackHandler},
				},
			},
			wantStatus: fiber.StatusOK,
			check: func(t *testing.T, out map[string]any) {
				t.Helper()
				results, ok := out["cascade_results"].([]any)
				require.True(t, ok)
				// Only db.dep_ok produces a cascade result: missing -> GetTransformationNode
				// error (skipped), notrack -> ShouldTrackPosition false (skipped),
				// nilhandler -> nil handler (skipped), err -> delete error (skipped).
				require.Len(t, results, 1)
				first, ok := results[0].(map[string]any)
				require.True(t, ok)
				require.Equal(t, "db.dep_ok", first["model_id"])
				require.InDelta(t, 7, first["deleted_rows"], 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			modelsSvc := &testutil.FakeModelsService{}
			if tc.dag != nil {
				modelsSvc.DAG = tc.dag
			}

			h := NewHandlers(tc.admin, modelsSvc, &coordinatorfake.FakeCoordinator{}, testLogger())
			app := newHandlersApp(h)

			status, out := doJSON(t, app, http.MethodPost, "/models/"+tc.id+"/delete-period", tc.body)
			require.Equal(t, tc.wantStatus, status)

			if tc.check != nil {
				tc.check(t, out)
			}
		})
	}
}

func TestConsolidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		id         string
		admin      *adminfake.FakeAdminService
		wantStatus int
	}{
		{
			name:       "invalid model id",
			id:         "bad",
			admin:      &adminfake.FakeAdminService{},
			wantStatus: fiber.StatusBadRequest,
		},
		{
			name: "admin error",
			id:   "db.table",
			admin: &adminfake.FakeAdminService{
				ConsolidateHistoricalDataFn: func(_ context.Context, _ string) (uint64, error) {
					return 0, errBoom
				},
			},
			wantStatus: fiber.StatusInternalServerError,
		},
		{
			name: "success",
			id:   "db.table",
			admin: &adminfake.FakeAdminService{
				ConsolidateHistoricalDataFn: func(_ context.Context, _ string) (uint64, error) {
					return 3, nil
				},
			},
			wantStatus: fiber.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := NewHandlers(tc.admin, &testutil.FakeModelsService{}, &coordinatorfake.FakeCoordinator{}, testLogger())
			app := newHandlersApp(h)

			status, out := doJSON(t, app, http.MethodPost, "/models/"+tc.id+"/consolidate", nil)
			require.Equal(t, tc.wantStatus, status)

			if tc.wantStatus == fiber.StatusOK {
				require.InDelta(t, 3, out["ranges_merged"], 0)
			}
		})
	}
}

func TestUpdateBounds(t *testing.T) {
	t.Parallel()

	existing := &admin.BoundsCache{
		ModelID:             "db.table",
		Min:                 1,
		Max:                 100,
		InitialScanComplete: true,
		LastFullScan:        time.Now().UTC(),
	}

	tests := []struct {
		name       string
		id         string
		body       any
		admin      *adminfake.FakeAdminService
		wantStatus int
		verify     func(t *testing.T, a *adminfake.FakeAdminService)
	}{
		{
			name:       "invalid model id",
			id:         "bad",
			body:       updateBoundsRequest{Min: 1, Max: 2},
			admin:      &adminfake.FakeAdminService{},
			wantStatus: fiber.StatusBadRequest,
		},
		{
			name:       "invalid body",
			id:         "db.table",
			body:       "{bad",
			admin:      &adminfake.FakeAdminService{},
			wantStatus: fiber.StatusBadRequest,
		},
		{
			name: "acquire lock error",
			id:   "db.table",
			body: updateBoundsRequest{Min: 1, Max: 2},
			admin: &adminfake.FakeAdminService{
				AcquireBoundsLockFn: func(_ context.Context, _ string) (admin.BoundsLock, error) {
					return nil, errBoom
				},
			},
			wantStatus: fiber.StatusInternalServerError,
		},
		{
			name: "get existing bounds error",
			id:   "db.table",
			body: updateBoundsRequest{Min: 1, Max: 2},
			admin: &adminfake.FakeAdminService{
				GetExternalBoundsFn: func(_ context.Context, _ string) (*admin.BoundsCache, error) {
					return nil, errBoom
				},
			},
			wantStatus: fiber.StatusInternalServerError,
		},
		{
			name: "set bounds error",
			id:   "db.table",
			body: updateBoundsRequest{Min: 1, Max: 2},
			admin: &adminfake.FakeAdminService{
				SetExternalBoundsFn: func(_ context.Context, _ *admin.BoundsCache) error {
					return errBoom
				},
			},
			wantStatus: fiber.StatusInternalServerError,
		},
		{
			name:       "success without existing bounds",
			id:         "db.table",
			body:       updateBoundsRequest{Min: 5, Max: 50},
			admin:      &adminfake.FakeAdminService{},
			wantStatus: fiber.StatusOK,
			verify: func(t *testing.T, a *adminfake.FakeAdminService) {
				t.Helper()
				saved := a.ExternalBounds["db.table"]
				require.NotNil(t, saved)
				require.Equal(t, uint64(5), saved.Min)
				require.Equal(t, uint64(50), saved.Max)
			},
		},
		{
			name: "success preserves existing metadata",
			id:   "db.table",
			body: updateBoundsRequest{Min: 5, Max: 50},
			admin: &adminfake.FakeAdminService{
				ExternalBounds: map[string]*admin.BoundsCache{"db.table": existing},
			},
			wantStatus: fiber.StatusOK,
			verify: func(t *testing.T, a *adminfake.FakeAdminService) {
				t.Helper()
				saved := a.ExternalBounds["db.table"]
				require.NotNil(t, saved)
				require.Equal(t, uint64(1), saved.PreviousMin)
				require.Equal(t, uint64(100), saved.PreviousMax)
				require.True(t, saved.InitialScanComplete)
			},
		},
		{
			name: "success with failing unlock",
			id:   "db.table",
			body: updateBoundsRequest{Min: 5, Max: 50},
			admin: &adminfake.FakeAdminService{
				AcquireBoundsLockFn: func(_ context.Context, _ string) (admin.BoundsLock, error) {
					return failingBoundsLock{}, nil
				},
			},
			wantStatus: fiber.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := NewHandlers(tc.admin, &testutil.FakeModelsService{}, &coordinatorfake.FakeCoordinator{}, testLogger())
			app := newHandlersApp(h)

			status, _ := doJSON(t, app, http.MethodPut, "/models/"+tc.id+"/bounds", tc.body)
			require.Equal(t, tc.wantStatus, status)

			if tc.verify != nil {
				tc.verify(t, tc.admin)
			}
		})
	}
}

func TestDeleteBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		id         string
		admin      *adminfake.FakeAdminService
		wantStatus int
	}{
		{
			name:       "invalid model id",
			id:         "bad",
			admin:      &adminfake.FakeAdminService{},
			wantStatus: fiber.StatusBadRequest,
		},
		{
			name: "acquire lock error",
			id:   "db.table",
			admin: &adminfake.FakeAdminService{
				AcquireBoundsLockFn: func(_ context.Context, _ string) (admin.BoundsLock, error) {
					return nil, errBoom
				},
			},
			wantStatus: fiber.StatusInternalServerError,
		},
		{
			name: "delete error",
			id:   "db.table",
			admin: &adminfake.FakeAdminService{
				DeleteExternalBoundsFn: func(_ context.Context, _ string) error {
					return errBoom
				},
			},
			wantStatus: fiber.StatusInternalServerError,
		},
		{
			name:       "success",
			id:         "db.table",
			admin:      &adminfake.FakeAdminService{},
			wantStatus: fiber.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := NewHandlers(tc.admin, &testutil.FakeModelsService{}, &coordinatorfake.FakeCoordinator{}, testLogger())
			app := newHandlersApp(h)

			status, out := doJSON(t, app, http.MethodDelete, "/models/"+tc.id+"/bounds", nil)
			require.Equal(t, tc.wantStatus, status)

			if tc.wantStatus == fiber.StatusOK {
				require.Equal(t, true, out["deleted"])
			}
		})
	}
}

func TestTriggerRefreshBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		id         string
		coord      *coordinatorfake.FakeCoordinator
		wantStatus int
	}{
		{
			name:       "invalid model id",
			id:         "bad",
			coord:      &coordinatorfake.FakeCoordinator{},
			wantStatus: fiber.StatusBadRequest,
		},
		{
			name: "refresh in progress",
			id:   "db.table",
			coord: &coordinatorfake.FakeCoordinator{
				TriggerBoundsRefreshFn: func(_ context.Context, _ string) error {
					return coordinator.ErrRefreshInProgress
				},
			},
			wantStatus: fiber.StatusConflict,
		},
		{
			name: "generic error",
			id:   "db.table",
			coord: &coordinatorfake.FakeCoordinator{
				TriggerBoundsRefreshFn: func(_ context.Context, _ string) error {
					return errBoom
				},
			},
			wantStatus: fiber.StatusInternalServerError,
		},
		{
			name:       "success",
			id:         "db.table",
			coord:      &coordinatorfake.FakeCoordinator{},
			wantStatus: fiber.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := NewHandlers(&adminfake.FakeAdminService{}, &testutil.FakeModelsService{}, tc.coord, testLogger())
			app := newHandlersApp(h)

			status, out := doJSON(t, app, http.MethodPost, "/models/"+tc.id+"/refresh-bounds", nil)
			require.Equal(t, tc.wantStatus, status)

			if tc.wantStatus == fiber.StatusOK {
				require.Equal(t, "full", out["scan_type"])
			}
		})
	}
}

func TestTriggerScheduledRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		id         string
		coord      *coordinatorfake.FakeCoordinator
		wantStatus int
	}{
		{
			name:       "invalid model id",
			id:         "bad",
			coord:      &coordinatorfake.FakeCoordinator{},
			wantStatus: fiber.StatusBadRequest,
		},
		{
			name: "run in progress",
			id:   "db.table",
			coord: &coordinatorfake.FakeCoordinator{
				TriggerScheduledRunFn: func(_ context.Context, _ string) error {
					return coordinator.ErrScheduledRunInProgress
				},
			},
			wantStatus: fiber.StatusConflict,
		},
		{
			name: "not scheduled model",
			id:   "db.table",
			coord: &coordinatorfake.FakeCoordinator{
				TriggerScheduledRunFn: func(_ context.Context, _ string) error {
					return coordinator.ErrNotScheduledModel
				},
			},
			wantStatus: fiber.StatusUnprocessableEntity,
		},
		{
			name: "generic error",
			id:   "db.table",
			coord: &coordinatorfake.FakeCoordinator{
				TriggerScheduledRunFn: func(_ context.Context, _ string) error {
					return errBoom
				},
			},
			wantStatus: fiber.StatusInternalServerError,
		},
		{
			name:       "success",
			id:         "db.table",
			coord:      &coordinatorfake.FakeCoordinator{},
			wantStatus: fiber.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := NewHandlers(&adminfake.FakeAdminService{}, &testutil.FakeModelsService{}, tc.coord, testLogger())
			app := newHandlersApp(h)

			status, out := doJSON(t, app, http.MethodPost, "/models/"+tc.id+"/run-now", nil)
			require.Equal(t, tc.wantStatus, status)

			if tc.wantStatus == fiber.StatusOK {
				require.Equal(t, "enqueued", out["status"])
			}
		})
	}
}
