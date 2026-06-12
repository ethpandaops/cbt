package handlers

import (
	"context"
	"net/http"
	"testing"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/api/generated"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func failingOverridesAdmin() *adminfake.FakeAdminService {
	return &adminfake.FakeAdminService{
		GetAllConfigOverridesFn: func(_ context.Context) ([]admin.ConfigOverride, error) {
			return nil, errInjected
		},
	}
}

func TestListAllModelsOverrideError(t *testing.T) {
	dag := &testutil.FakeDAGReader{
		Transformations: []models.Transformation{
			incrementalTransformation("analytics.block_stats", "analytics", "block_stats"),
		},
	}
	server := newTestServer(dag, failingOverridesAdmin())

	app := newErrorApp()
	app.Get("/models", func(c fiber.Ctx) error {
		return server.ListAllModels(c, generated.ListAllModelsParams{})
	})

	resp := doGet(t, app, "/models")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestListAllModelsSearchFilter(t *testing.T) {
	dag := &testutil.FakeDAGReader{
		Transformations: []models.Transformation{
			incrementalTransformation("analytics.block_stats", "analytics", "block_stats"),
			incrementalTransformation("analytics.daily", "analytics", "daily"),
		},
		Externals: []models.Node{
			externalNode("ethereum.blocks", "ethereum", "blocks"),
			// Non-external node is skipped via the type assertion path.
			{NodeType: models.NodeTypeExternal, Model: "nope"},
		},
	}
	server := newTestServer(dag, &adminfake.FakeAdminService{})

	app := fiber.New()
	app.Get("/models", func(c fiber.Ctx) error {
		search := "block"
		return server.ListAllModels(c, generated.ListAllModelsParams{Search: &search})
	})

	resp := doGet(t, app, "/models")
	defer resp.Body.Close()

	var body struct {
		Models []generated.ModelSummary `json:"models"`
		Total  int                      `json:"total"`
	}
	decode(t, resp, &body)
	// "analytics.block_stats" and "ethereum.blocks" both contain "block".
	assert.Equal(t, 2, body.Total)
	ids := make([]string, 0, len(body.Models))
	for _, m := range body.Models {
		ids = append(ids, m.Id)
	}
	assert.ElementsMatch(t, []string{"analytics.block_stats", "ethereum.blocks"}, ids)
}

func TestListTransformationsOverrideError(t *testing.T) {
	dag := &testutil.FakeDAGReader{
		Transformations: []models.Transformation{
			incrementalTransformation("analytics.block_stats", "analytics", "block_stats"),
		},
	}
	server := newTestServer(dag, failingOverridesAdmin())

	app := newErrorApp()
	app.Get("/t", func(c fiber.Ctx) error {
		return server.ListTransformations(c, generated.ListTransformationsParams{})
	})

	resp := doGet(t, app, "/t")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestListTransformationsTypeAndDatabaseFilter(t *testing.T) {
	dag := &testutil.FakeDAGReader{
		Transformations: []models.Transformation{
			incrementalTransformation("analytics.block_stats", "analytics", "block_stats"),
			scheduledTransformation("analytics.daily", "analytics", "daily"),
			incrementalTransformation("other.model", "other", "model"),
		},
	}
	server := newTestServer(dag, &adminfake.FakeAdminService{})

	tests := []struct {
		name      string
		paramType *generated.ListTransformationsParamsType
		database  *string
		wantIDs   []string
	}{
		{
			name:      "scheduled filter",
			paramType: ptrTo(generated.Scheduled),
			wantIDs:   []string{"analytics.daily"},
		},
		{
			name:      "incremental filter",
			paramType: ptrTo(generated.Incremental),
			wantIDs:   []string{"analytics.block_stats", "other.model"},
		},
		{
			name:     "database filter",
			database: ptrTo("other"),
			wantIDs:  []string{"other.model"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := fiber.New()
			app.Get("/t", func(c fiber.Ctx) error {
				status := generated.ListTransformationsParamsStatus("")
				return server.ListTransformations(c, generated.ListTransformationsParams{
					Type:     tt.paramType,
					Database: tt.database,
					Status:   &status,
				})
			})

			resp := doGet(t, app, "/t")
			defer resp.Body.Close()

			var body struct {
				Models []generated.TransformationModel `json:"models"`
				Total  int                             `json:"total"`
			}
			decode(t, resp, &body)
			ids := make([]string, 0, len(body.Models))
			for _, m := range body.Models {
				ids = append(ids, m.Id)
			}
			assert.ElementsMatch(t, tt.wantIDs, ids)
		})
	}
}

func TestGetTransformationOverrideError(t *testing.T) {
	dag := &testutil.FakeDAGReader{
		TransformationByID: map[string]models.Transformation{
			"analytics.block_stats": incrementalTransformation("analytics.block_stats", "analytics", "block_stats"),
		},
	}
	adminSvc := &adminfake.FakeAdminService{
		GetConfigOverrideFn: func(_ context.Context, _ string) (*admin.ConfigOverride, error) {
			return nil, errInjected
		},
	}
	server := newTestServer(dag, adminSvc)

	app := newErrorApp()
	app.Get("/t/:id", func(c fiber.Ctx) error {
		return server.GetTransformation(c, c.Params("id"))
	})

	resp := doGet(t, app, "/t/analytics.block_stats")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestListExternalModelsOverrideError(t *testing.T) {
	dag := &testutil.FakeDAGReader{
		Externals: []models.Node{externalNode("ethereum.blocks", "ethereum", "blocks")},
	}
	server := newTestServer(dag, failingOverridesAdmin())

	app := newErrorApp()
	app.Get("/e", func(c fiber.Ctx) error {
		return server.ListExternalModels(c, generated.ListExternalModelsParams{})
	})

	resp := doGet(t, app, "/e")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestListExternalModelsDatabaseFilterAndNonExternal(t *testing.T) {
	dag := &testutil.FakeDAGReader{
		Externals: []models.Node{
			externalNode("ethereum.blocks", "ethereum", "blocks"),
			externalNode("other.table", "other", "table"),
			{NodeType: models.NodeTypeExternal, Model: "not-external"},
		},
	}
	server := newTestServer(dag, &adminfake.FakeAdminService{})

	app := fiber.New()
	app.Get("/e", func(c fiber.Ctx) error {
		db := "ethereum"
		return server.ListExternalModels(c, generated.ListExternalModelsParams{Database: &db})
	})

	resp := doGet(t, app, "/e")
	defer resp.Body.Close()

	var body struct {
		Models []generated.ExternalModel `json:"models"`
		Total  int                       `json:"total"`
	}
	decode(t, resp, &body)
	require.Equal(t, 1, body.Total)
	assert.Equal(t, "ethereum.blocks", body.Models[0].Id)
}

func TestGetExternalModelOverrideError(t *testing.T) {
	dag := &testutil.FakeDAGReader{
		ExternalByID: map[string]models.External{
			"ethereum.blocks": &testutil.FakeExternal{
				ID:     "ethereum.blocks",
				Config: external.Config{Database: "ethereum", Table: "blocks"},
			},
		},
	}
	adminSvc := &adminfake.FakeAdminService{
		GetConfigOverrideFn: func(_ context.Context, _ string) (*admin.ConfigOverride, error) {
			return nil, errInjected
		},
	}
	server := newTestServer(dag, adminSvc)

	app := newErrorApp()
	app.Get("/e/:id", func(c fiber.Ctx) error {
		return server.GetExternalModel(c, c.Params("id"))
	})

	resp := doGet(t, app, "/e/ethereum.blocks")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestConfigOverrideStatusNilAdminService(t *testing.T) {
	// When adminService is nil, the status helpers short-circuit to empty results.
	server := &Server{modelsService: &testutil.FakeModelsService{DAG: &testutil.FakeDAGReader{}}}

	statusMap, err := server.getConfigOverrideStatusMap(context.Background())
	require.NoError(t, err)
	assert.Empty(t, statusMap)

	status, err := server.getConfigOverrideStatus(context.Background(), "x.y")
	require.NoError(t, err)
	assert.Equal(t, configOverrideStatus{}, status)
}

func TestConfigOverrideStatusFromModel(t *testing.T) {
	disabled := false
	enabled := true

	assert.Equal(t, configOverrideStatus{}, configOverrideStatusFromModel(nil))
	assert.Equal(t, configOverrideStatus{hasOverride: true, isDisabled: true},
		configOverrideStatusFromModel(&admin.ConfigOverride{Enabled: &disabled}))
	assert.Equal(t, configOverrideStatus{hasOverride: true, isDisabled: false},
		configOverrideStatusFromModel(&admin.ConfigOverride{Enabled: &enabled}))
	assert.Equal(t, configOverrideStatus{hasOverride: true, isDisabled: false},
		configOverrideStatusFromModel(&admin.ConfigOverride{}))
}

func ptrTo[T any](v T) *T { return &v }
