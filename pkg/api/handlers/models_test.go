package handlers

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/api/generated"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/gofiber/fiber/v3"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListAllModels(t *testing.T) {
	tests := []struct {
		name           string
		queryParams    string
		setupMocks     func() *mockDAGReader
		wantStatus     int
		wantTotalCount int
		wantModelIDs   []string
	}{
		{
			name:        "returns all models - transformations and externals",
			queryParams: "",
			setupMocks: func() *mockDAGReader {
				return &mockDAGReader{
					transformations: []models.Transformation{
						&mockTransformation{
							id:       "analytics.block_stats",
							database: "analytics",
							table:    "block_stats",
							typ:      transformation.TypeIncremental,
						},
					},
					externals: []models.Node{
						{
							Model: &mockExternal{
								id:       "ethereum.blocks",
								database: "ethereum",
								table:    "blocks",
							},
						},
					},
					externalByID: map[string]models.External{
						"ethereum.blocks": &mockExternal{
							id:       "ethereum.blocks",
							database: "ethereum",
							table:    "blocks",
						},
					},
					dependencies: make(map[string][]string),
					dependents:   make(map[string][]string),
				}
			},
			wantStatus:     200,
			wantTotalCount: 2,
			wantModelIDs:   []string{"analytics.block_stats", "ethereum.blocks"},
		},
		{
			name:        "filters by type=transformation",
			queryParams: "?type=transformation",
			setupMocks: func() *mockDAGReader {
				return &mockDAGReader{
					transformations: []models.Transformation{
						&mockTransformation{
							id:       "analytics.block_stats",
							database: "analytics",
							table:    "block_stats",
							typ:      transformation.TypeIncremental,
						},
					},
					externals:    []models.Node{},
					dependencies: make(map[string][]string),
					dependents:   make(map[string][]string),
				}
			},
			wantStatus:     200,
			wantTotalCount: 1,
			wantModelIDs:   []string{"analytics.block_stats"},
		},
		{
			name:        "filters by type=external",
			queryParams: "?type=external",
			setupMocks: func() *mockDAGReader {
				return &mockDAGReader{
					transformations: []models.Transformation{},
					externals: []models.Node{
						{
							Model: &mockExternal{
								id:       "ethereum.blocks",
								database: "ethereum",
								table:    "blocks",
							},
						},
					},
					externalByID: map[string]models.External{
						"ethereum.blocks": &mockExternal{
							id:       "ethereum.blocks",
							database: "ethereum",
							table:    "blocks",
						},
					},
					dependencies: make(map[string][]string),
					dependents:   make(map[string][]string),
				}
			},
			wantStatus:     200,
			wantTotalCount: 1,
			wantModelIDs:   []string{"ethereum.blocks"},
		},
		{
			name:        "filters by database",
			queryParams: "?database=analytics",
			setupMocks: func() *mockDAGReader {
				return &mockDAGReader{
					transformations: []models.Transformation{
						&mockTransformation{
							id:       "analytics.block_stats",
							database: "analytics",
							table:    "block_stats",
							typ:      transformation.TypeIncremental,
						},
						&mockTransformation{
							id:       "ethereum.processed",
							database: "ethereum",
							table:    "processed",
							typ:      transformation.TypeScheduled,
						},
					},
					externals:    []models.Node{},
					dependencies: make(map[string][]string),
					dependents:   make(map[string][]string),
				}
			},
			wantStatus:     200,
			wantTotalCount: 1,
			wantModelIDs:   []string{"analytics.block_stats"},
		},
		{
			name:        "returns empty list when no models match",
			queryParams: "?database=nonexistent",
			setupMocks: func() *mockDAGReader {
				return &mockDAGReader{
					transformations: []models.Transformation{},
					externals:       []models.Node{},
					dependencies:    make(map[string][]string),
					dependents:      make(map[string][]string),
				}
			},
			wantStatus:     200,
			wantTotalCount: 0,
			wantModelIDs:   []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)

			mockDAG := tt.setupMocks()
			mockService := &mockModelsService{dag: mockDAG}
			mockAdmin := &mockAdminService{}
			server := NewServer(mockService, mockAdmin, IntervalTypesConfig{}, log)

			app := fiber.New()
			app.Get("/models", func(c fiber.Ctx) error {
				params := generated.ListAllModelsParams{}
				if err := c.Bind().Query(&params); err != nil {
					return err
				}
				return server.ListAllModels(c, params)
			})

			// Execute
			req := httptest.NewRequest("GET", "/models"+tt.queryParams, http.NoBody)
			resp, err := app.Test(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			// Assert status
			assert.Equal(t, tt.wantStatus, resp.StatusCode)

			// Assert response body
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			var response struct {
				Models []generated.ModelSummary `json:"models"`
				Total  int                      `json:"total"`
			}
			err = json.Unmarshal(body, &response)
			require.NoError(t, err)

			assert.Equal(t, tt.wantTotalCount, response.Total)
			assert.Len(t, response.Models, tt.wantTotalCount)

			// Verify model IDs
			actualIDs := make([]string, 0, len(response.Models))
			for _, model := range response.Models {
				actualIDs = append(actualIDs, model.Id)
			}
			assert.ElementsMatch(t, tt.wantModelIDs, actualIDs)
		})
	}
}

func TestListTransformationsIncludesOverrideStatus(t *testing.T) {
	disabled := false

	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	mockDAG := &mockDAGReader{
		transformations: []models.Transformation{
			&mockTransformation{
				id:       "analytics.block_stats",
				database: "analytics",
				table:    "block_stats",
				typ:      transformation.TypeIncremental,
			},
			&mockTransformation{
				id:       "analytics.daily_report",
				database: "analytics",
				table:    "daily_report",
				typ:      transformation.TypeScheduled,
			},
		},
		dependencies: make(map[string][]string),
		dependents:   make(map[string][]string),
	}
	mockService := &mockModelsService{dag: mockDAG}
	mockAdmin := &mockAdminService{
		configOverrides: []admin.ConfigOverride{
			{ModelID: "analytics.block_stats"},
			{ModelID: "analytics.daily_report", Enabled: &disabled},
		},
	}
	server := NewServer(mockService, mockAdmin, IntervalTypesConfig{}, log)

	app := fiber.New()
	app.Get("/models/transformations", func(c fiber.Ctx) error {
		params := generated.ListTransformationsParams{}
		if err := c.Bind().Query(&params); err != nil {
			return err
		}
		return server.ListTransformations(c, params)
	})

	req := httptest.NewRequest("GET", "/models/transformations", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var response struct {
		Models []generated.TransformationModel `json:"models"`
		Total  int                             `json:"total"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	assert.Equal(t, 2, response.Total)

	statusByID := make(map[string]generated.TransformationModel)
	for _, model := range response.Models {
		statusByID[model.Id] = model
	}

	require.NotNil(t, statusByID["analytics.block_stats"].HasOverride)
	require.NotNil(t, statusByID["analytics.block_stats"].IsDisabled)
	require.NotNil(t, statusByID["analytics.daily_report"].HasOverride)
	require.NotNil(t, statusByID["analytics.daily_report"].IsDisabled)
	assert.True(t, *statusByID["analytics.block_stats"].HasOverride)
	assert.False(t, *statusByID["analytics.block_stats"].IsDisabled)
	assert.True(t, *statusByID["analytics.daily_report"].HasOverride)
	assert.True(t, *statusByID["analytics.daily_report"].IsDisabled)
}

func TestListExternalModelsIncludesOverrideStatus(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	mockDAG := &mockDAGReader{
		externals: []models.Node{
			{
				Model: &mockExternal{
					id:       "ethereum.blocks",
					database: "ethereum",
					table:    "blocks",
				},
			},
			{
				Model: &mockExternal{
					id:       "ethereum.transactions",
					database: "ethereum",
					table:    "transactions",
				},
			},
		},
		dependencies: make(map[string][]string),
		dependents:   make(map[string][]string),
	}
	mockService := &mockModelsService{dag: mockDAG}
	mockAdmin := &mockAdminService{
		configOverrides: []admin.ConfigOverride{
			{ModelID: "ethereum.blocks"},
		},
	}
	server := NewServer(mockService, mockAdmin, IntervalTypesConfig{}, log)

	app := fiber.New()
	app.Get("/models/external", func(c fiber.Ctx) error {
		params := generated.ListExternalModelsParams{}
		if err := c.Bind().Query(&params); err != nil {
			return err
		}
		return server.ListExternalModels(c, params)
	})

	req := httptest.NewRequest("GET", "/models/external", http.NoBody)
	resp, err := app.Test(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var response struct {
		Models []generated.ExternalModel `json:"models"`
		Total  int                       `json:"total"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	assert.Equal(t, 2, response.Total)

	statusByID := make(map[string]generated.ExternalModel)
	for _, model := range response.Models {
		statusByID[model.Id] = model
	}

	require.NotNil(t, statusByID["ethereum.blocks"].HasOverride)
	require.NotNil(t, statusByID["ethereum.blocks"].IsDisabled)
	require.NotNil(t, statusByID["ethereum.transactions"].HasOverride)
	require.NotNil(t, statusByID["ethereum.transactions"].IsDisabled)
	assert.True(t, *statusByID["ethereum.blocks"].HasOverride)
	assert.False(t, *statusByID["ethereum.blocks"].IsDisabled)
	assert.False(t, *statusByID["ethereum.transactions"].HasOverride)
	assert.False(t, *statusByID["ethereum.transactions"].IsDisabled)
}

func TestGetTransformation(t *testing.T) {
	tests := []struct {
		name       string
		modelID    string
		setupMocks func() *mockDAGReader
		wantStatus int
		wantError  bool
	}{
		{
			name:    "returns transformation model",
			modelID: "analytics.block_stats",
			setupMocks: func() *mockDAGReader {
				return &mockDAGReader{
					transformationByID: map[string]models.Transformation{
						"analytics.block_stats": &mockTransformation{
							id:       "analytics.block_stats",
							database: "analytics",
							table:    "block_stats",
							typ:      transformation.TypeIncremental,
						},
					},
					dependencies: make(map[string][]string),
					dependents:   make(map[string][]string),
				}
			},
			wantStatus: 200,
		},
		{
			name:    "returns 400 for invalid model ID format",
			modelID: "invalid",
			setupMocks: func() *mockDAGReader {
				return &mockDAGReader{}
			},
			wantStatus: 400,
			wantError:  true,
		},
		{
			name:    "returns 404 for non-existent model",
			modelID: "nonexistent.model",
			setupMocks: func() *mockDAGReader {
				return &mockDAGReader{
					getTransformationErr: errNodeNotFound,
				}
			},
			wantStatus: 404,
			wantError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)

			mockDAG := tt.setupMocks()
			mockService := &mockModelsService{dag: mockDAG}
			mockAdmin := &mockAdminService{}
			server := NewServer(mockService, mockAdmin, IntervalTypesConfig{}, log)

			app := fiber.New(fiber.Config{
				ErrorHandler: func(c fiber.Ctx, err error) error {
					code := fiber.StatusInternalServerError
					message := "Internal Server Error"
					var fiberErr *fiber.Error
					if ok := errors.As(err, &fiberErr); ok {
						code = fiberErr.Code
						message = fiberErr.Message
					}
					return c.Status(code).JSON(fiber.Map{"error": message, "code": code})
				},
			})
			app.Get("/models/transformations/:id", func(c fiber.Ctx) error {
				return server.GetTransformation(c, c.Params("id"))
			})

			// Execute
			req := httptest.NewRequest("GET", "/models/transformations/"+tt.modelID, http.NoBody)
			resp, err := app.Test(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			// Assert
			assert.Equal(t, tt.wantStatus, resp.StatusCode)

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			if tt.wantError {
				var errResp map[string]interface{}
				err = json.Unmarshal(body, &errResp)
				require.NoError(t, err)
				assert.Contains(t, errResp, "error")
			} else {
				var model generated.TransformationModel
				err = json.Unmarshal(body, &model)
				require.NoError(t, err)
				assert.Equal(t, tt.modelID, model.Id)
			}
		})
	}
}

func TestGetExternalModel(t *testing.T) {
	tests := []struct {
		name       string
		modelID    string
		setupMocks func() *mockDAGReader
		wantStatus int
		wantError  bool
	}{
		{
			name:    "returns external model",
			modelID: "ethereum.blocks",
			setupMocks: func() *mockDAGReader {
				return &mockDAGReader{
					externalByID: map[string]models.External{
						"ethereum.blocks": &mockExternal{
							id:       "ethereum.blocks",
							database: "ethereum",
							table:    "blocks",
						},
					},
					dependencies: make(map[string][]string),
					dependents:   make(map[string][]string),
				}
			},
			wantStatus: 200,
		},
		{
			name:    "returns 400 for invalid model ID format",
			modelID: "invalid",
			setupMocks: func() *mockDAGReader {
				return &mockDAGReader{}
			},
			wantStatus: 400,
			wantError:  true,
		},
		{
			name:    "returns 404 for non-existent model",
			modelID: "nonexistent.model",
			setupMocks: func() *mockDAGReader {
				return &mockDAGReader{
					getExternalErr: errNodeNotFound,
				}
			},
			wantStatus: 404,
			wantError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)

			mockDAG := tt.setupMocks()
			mockService := &mockModelsService{dag: mockDAG}
			mockAdmin := &mockAdminService{}
			server := NewServer(mockService, mockAdmin, IntervalTypesConfig{}, log)

			app := fiber.New(fiber.Config{
				ErrorHandler: func(c fiber.Ctx, err error) error {
					code := fiber.StatusInternalServerError
					message := "Internal Server Error"
					var fiberErr *fiber.Error
					if ok := errors.As(err, &fiberErr); ok {
						code = fiberErr.Code
						message = fiberErr.Message
					}
					return c.Status(code).JSON(fiber.Map{"error": message, "code": code})
				},
			})
			app.Get("/models/external/:id", func(c fiber.Ctx) error {
				return server.GetExternalModel(c, c.Params("id"))
			})

			// Execute
			req := httptest.NewRequest("GET", "/models/external/"+tt.modelID, http.NoBody)
			resp, err := app.Test(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			// Assert
			assert.Equal(t, tt.wantStatus, resp.StatusCode)

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			if tt.wantError {
				var errResp map[string]interface{}
				err = json.Unmarshal(body, &errResp)
				require.NoError(t, err)
				assert.Contains(t, errResp, "error")
			} else {
				var model generated.ExternalModel
				err = json.Unmarshal(body, &model)
				require.NoError(t, err)
				assert.Equal(t, tt.modelID, model.Id)
			}
		})
	}
}
