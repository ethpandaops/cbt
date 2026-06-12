package management

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	"github.com/ethpandaops/cbt/internal/testutil/coordinatorfake"
	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/require"
)

func boolPtr(b bool) *bool { return &b }

func TestGetConfigOverride(t *testing.T) {
	t.Parallel()

	enabled := true
	override := &admin.ConfigOverride{
		ModelID:   "db.table",
		Type:      "transformation",
		Enabled:   &enabled,
		Override:  json.RawMessage(`{"schedule":"@every 1h"}`),
		UpdatedAt: time.Now().UTC(),
	}

	tests := []struct {
		name       string
		id         string
		admin      *adminfake.FakeAdminService
		provider   BaseConfigProvider
		wantStatus int
		check      func(t *testing.T, out map[string]any)
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
				GetConfigOverrideFn: func(_ context.Context, _ string) (*admin.ConfigOverride, error) {
					return nil, errBoom
				},
			},
			wantStatus: fiber.StatusInternalServerError,
		},
		{
			name:       "no override no provider",
			id:         "db.table",
			admin:      &adminfake.FakeAdminService{},
			wantStatus: fiber.StatusOK,
			check: func(t *testing.T, out map[string]any) {
				t.Helper()
				require.NotContains(t, out, "base_config")
				require.NotContains(t, out, "override")
			},
		},
		{
			name:       "no override with provider",
			id:         "db.table",
			admin:      &adminfake.FakeAdminService{},
			provider:   &fakeBaseConfigProvider{cfg: json.RawMessage(`{"a":1}`)},
			wantStatus: fiber.StatusOK,
			check: func(t *testing.T, out map[string]any) {
				t.Helper()
				require.Contains(t, out, "base_config")
			},
		},
		{
			name:       "provider error suppressed",
			id:         "db.table",
			admin:      &adminfake.FakeAdminService{},
			provider:   &fakeBaseConfigProvider{err: errBoom},
			wantStatus: fiber.StatusOK,
			check: func(t *testing.T, out map[string]any) {
				t.Helper()
				require.NotContains(t, out, "base_config")
			},
		},
		{
			name: "override present",
			id:   "db.table",
			admin: &adminfake.FakeAdminService{
				ConfigOverrideByID: map[string]*admin.ConfigOverride{"db.table": override},
			},
			provider:   &fakeBaseConfigProvider{cfg: json.RawMessage(`{"a":1}`)},
			wantStatus: fiber.StatusOK,
			check: func(t *testing.T, out map[string]any) {
				t.Helper()
				require.Equal(t, "db.table", out["model_id"])
				require.Equal(t, "transformation", out["model_type"])
				require.Equal(t, true, out["enabled"])
				require.Contains(t, out, "override")
				require.Contains(t, out, "updated_at")
			},
		},
		{
			name: "override present without enabled",
			id:   "db.table",
			admin: &adminfake.FakeAdminService{
				ConfigOverrideByID: map[string]*admin.ConfigOverride{
					"db.table": {
						ModelID:  "db.table",
						Type:     "external",
						Override: json.RawMessage(`{"lag":1}`),
					},
				},
			},
			wantStatus: fiber.StatusOK,
			check: func(t *testing.T, out map[string]any) {
				t.Helper()
				require.NotContains(t, out, "enabled")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := NewHandlers(tc.admin, &testutil.FakeModelsService{}, &coordinatorfake.FakeCoordinator{}, testLogger())
			if tc.provider != nil {
				h.SetBaseConfigProvider(tc.provider)
			}

			app := newHandlersApp(h)

			status, out := doJSON(t, app, http.MethodGet, "/models/"+tc.id+"/config-override", nil)
			require.Equal(t, tc.wantStatus, status)

			if tc.check != nil {
				tc.check(t, out)
			}
		})
	}
}

func TestSetConfigOverride(t *testing.T) {
	t.Parallel()

	transformationDAG := &testutil.FakeDAGReader{
		NodeByID: map[string]models.Node{
			"db.table": {NodeType: models.NodeTypeTransformation, Model: &testutil.FakeTransformation{ID: "db.table"}},
		},
	}
	externalDAG := &testutil.FakeDAGReader{
		NodeByID: map[string]models.Node{
			"db.table": {NodeType: models.NodeTypeExternal, Model: &testutil.FakeExternal{ID: "db.table"}},
		},
	}
	unknownDAG := &testutil.FakeDAGReader{
		NodeByID: map[string]models.Node{
			"db.table": {NodeType: models.NodeType("mystery")},
		},
	}

	tests := []struct {
		name       string
		id         string
		body       any
		admin      *adminfake.FakeAdminService
		dag        models.DAGReader
		wantStatus int
	}{
		{
			name:       "invalid model id",
			id:         "bad",
			body:       configOverrideRequest{},
			admin:      &adminfake.FakeAdminService{},
			dag:        transformationDAG,
			wantStatus: fiber.StatusBadRequest,
		},
		{
			name:       "invalid body",
			id:         "db.table",
			body:       "{bad",
			admin:      &adminfake.FakeAdminService{},
			dag:        transformationDAG,
			wantStatus: fiber.StatusBadRequest,
		},
		{
			name:       "node not found",
			id:         "db.table",
			body:       configOverrideRequest{},
			admin:      &adminfake.FakeAdminService{},
			dag:        &testutil.FakeDAGReader{GetNodeErr: errBoom},
			wantStatus: fiber.StatusNotFound,
		},
		{
			name:       "external with enabled rejected",
			id:         "db.table",
			body:       configOverrideRequest{Enabled: boolPtr(true)},
			admin:      &adminfake.FakeAdminService{},
			dag:        externalDAG,
			wantStatus: fiber.StatusBadRequest,
		},
		{
			name:       "unknown node type",
			id:         "db.table",
			body:       configOverrideRequest{},
			admin:      &adminfake.FakeAdminService{},
			dag:        unknownDAG,
			wantStatus: fiber.StatusBadRequest,
		},
		{
			name:       "invalid transformation config",
			id:         "db.table",
			body:       configOverrideRequest{Config: json.RawMessage(`{"schedule":"@every notaduration"}`)},
			admin:      &adminfake.FakeAdminService{},
			dag:        transformationDAG,
			wantStatus: fiber.StatusBadRequest,
		},
		{
			name:       "invalid external config json",
			id:         "db.table",
			body:       configOverrideRequest{Config: json.RawMessage(`{"lag":"not-a-number"}`)},
			admin:      &adminfake.FakeAdminService{},
			dag:        externalDAG,
			wantStatus: fiber.StatusBadRequest,
		},
		{
			name: "admin set error",
			id:   "db.table",
			body: configOverrideRequest{Config: json.RawMessage(`{"schedule":"@every 1h"}`)},
			admin: &adminfake.FakeAdminService{
				SetConfigOverrideFn: func(_ context.Context, _ *admin.ConfigOverride) error {
					return errBoom
				},
			},
			dag:        transformationDAG,
			wantStatus: fiber.StatusInternalServerError,
		},
		{
			name:       "transformation success",
			id:         "db.table",
			body:       configOverrideRequest{Enabled: boolPtr(false), Config: json.RawMessage(`{"schedule":"@every 1h"}`)},
			admin:      &adminfake.FakeAdminService{},
			dag:        transformationDAG,
			wantStatus: fiber.StatusOK,
		},
		{
			name:       "external success",
			id:         "db.table",
			body:       configOverrideRequest{Config: json.RawMessage(`{"lag":5}`)},
			admin:      &adminfake.FakeAdminService{},
			dag:        externalDAG,
			wantStatus: fiber.StatusOK,
		},
		{
			name:       "empty config skips validation",
			id:         "db.table",
			body:       configOverrideRequest{},
			admin:      &adminfake.FakeAdminService{},
			dag:        transformationDAG,
			wantStatus: fiber.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			modelsSvc := &testutil.FakeModelsService{DAG: tc.dag}
			h := NewHandlers(tc.admin, modelsSvc, &coordinatorfake.FakeCoordinator{}, testLogger())
			app := newHandlersApp(h)

			status, out := doJSON(t, app, http.MethodPut, "/models/"+tc.id+"/config-override", tc.body)
			require.Equal(t, tc.wantStatus, status)

			if tc.wantStatus == fiber.StatusOK {
				require.Equal(t, true, out["updated"])
			}
		})
	}
}

func TestDeleteConfigOverride(t *testing.T) {
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
				DeleteConfigOverrideFn: func(_ context.Context, _ string) error {
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

			status, out := doJSON(t, app, http.MethodDelete, "/models/"+tc.id+"/config-override", nil)
			require.Equal(t, tc.wantStatus, status)

			if tc.wantStatus == fiber.StatusOK {
				require.Equal(t, true, out["deleted"])
			}
		})
	}
}

func TestListConfigOverrides(t *testing.T) {
	t.Parallel()

	t.Run("error", func(t *testing.T) {
		t.Parallel()

		a := &adminfake.FakeAdminService{
			GetAllConfigOverridesFn: func(_ context.Context) ([]admin.ConfigOverride, error) {
				return nil, errBoom
			},
		}
		h := NewHandlers(a, &testutil.FakeModelsService{}, &coordinatorfake.FakeCoordinator{}, testLogger())
		app := newHandlersApp(h)

		status, _ := doJSON(t, app, http.MethodGet, "/config-overrides", nil)
		require.Equal(t, fiber.StatusInternalServerError, status)
	})

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		a := &adminfake.FakeAdminService{
			ConfigOverrides: []admin.ConfigOverride{{ModelID: "db.table"}},
		}
		h := NewHandlers(a, &testutil.FakeModelsService{}, &coordinatorfake.FakeCoordinator{}, testLogger())
		app := newHandlersApp(h)

		status, out := doJSON(t, app, http.MethodGet, "/config-overrides", nil)
		require.Equal(t, fiber.StatusOK, status)
		require.Contains(t, out, "overrides")
	})
}

func TestClearAllConfigOverrides(t *testing.T) {
	t.Parallel()

	t.Run("error", func(t *testing.T) {
		t.Parallel()

		a := &adminfake.FakeAdminService{
			DeleteAllConfigOverridesFn: func(_ context.Context) error {
				return errBoom
			},
		}
		h := NewHandlers(a, &testutil.FakeModelsService{}, &coordinatorfake.FakeCoordinator{}, testLogger())
		app := newHandlersApp(h)

		status, _ := doJSON(t, app, http.MethodDelete, "/config-overrides", nil)
		require.Equal(t, fiber.StatusInternalServerError, status)
	})

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		h := NewHandlers(&adminfake.FakeAdminService{}, &testutil.FakeModelsService{}, &coordinatorfake.FakeCoordinator{}, testLogger())
		app := newHandlersApp(h)

		status, out := doJSON(t, app, http.MethodDelete, "/config-overrides", nil)
		require.Equal(t, fiber.StatusOK, status)
		require.Equal(t, true, out["cleared"])
	})
}
