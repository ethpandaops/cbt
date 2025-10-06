package handlers

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ethpandaops/cbt/pkg/api/generated"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/gofiber/fiber/v3"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetModels(t *testing.T) {
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
			server := NewServer(mockService, log)

			app := fiber.New()
			app.Get("/models", func(c fiber.Ctx) error {
				params := generated.GetModelsParams{}
				if err := c.Bind().Query(&params); err != nil {
					return err
				}
				return server.GetModels(c, params)
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

			var response generated.ModelsResponse
			err = json.Unmarshal(body, &response)
			require.NoError(t, err)

			assert.Equal(t, tt.wantTotalCount, response.Total)
			assert.Len(t, response.Models, tt.wantTotalCount)

			// Verify model IDs
			var actualIDs []string
			for _, model := range response.Models {
				actualIDs = append(actualIDs, model.Id)
			}
			assert.ElementsMatch(t, tt.wantModelIDs, actualIDs)
		})
	}
}

func TestGetModelByID(t *testing.T) {
	tests := []struct {
		name       string
		modelID    string
		setupMocks func() *mockDAGReader
		wantStatus int
		wantType   string
		wantError  bool
	}{
		{
			name:    "returns transformation model",
			modelID: "analytics.block_stats",
			setupMocks: func() *mockDAGReader {
				return &mockDAGReader{
					nodeByID: map[string]models.Node{
						"analytics.block_stats": {},
					},
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
			wantType:   "transformation",
		},
		{
			name:    "returns external model",
			modelID: "ethereum.blocks",
			setupMocks: func() *mockDAGReader {
				return &mockDAGReader{
					nodeByID: map[string]models.Node{
						"ethereum.blocks": {},
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
			wantStatus: 200,
			wantType:   "external",
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
					getNodeErr: errNodeNotFound,
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
			server := NewServer(mockService, log)

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
			app.Get("/models/:model_id", func(c fiber.Ctx) error {
				return server.GetModelByID(c, c.Params("model_id"))
			})

			// Execute
			req := httptest.NewRequest("GET", "/models/"+tt.modelID, http.NoBody)
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
				var detail generated.ModelDetail
				err = json.Unmarshal(body, &detail)
				require.NoError(t, err)
				assert.Equal(t, tt.modelID, detail.Id)
				assert.Equal(t, tt.wantType, detail.Type)
			}
		})
	}
}
