package handlers

import (
	"context"
	"net/http"
	"testing"
	"time"

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

func externalNode(id, database, table string) models.Node {
	return models.Node{
		NodeType: models.NodeTypeExternal,
		Model: &testutil.FakeExternal{
			ID:     id,
			Config: external.Config{Database: database, Table: table},
		},
	}
}

func TestListExternalBounds(t *testing.T) {
	started := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	t.Run("returns bounds, skipping errors, nils and non-externals", func(t *testing.T) {
		dag := &testutil.FakeDAGReader{
			Externals: []models.Node{
				externalNode("ethereum.blocks", "ethereum", "blocks"),
				externalNode("ethereum.errors", "ethereum", "errors"),
				externalNode("ethereum.nilcache", "ethereum", "nilcache"),
				// A node whose Model is not a models.External is skipped.
				{NodeType: models.NodeTypeExternal, Model: "not-an-external"},
			},
		}
		adminSvc := &adminfake.FakeAdminService{
			GetExternalBoundsFn: func(_ context.Context, modelID string) (*admin.BoundsCache, error) {
				switch modelID {
				case "ethereum.blocks":
					return &admin.BoundsCache{
						ModelID:             "ethereum.blocks",
						Min:                 1,
						Max:                 100,
						PreviousMin:         2,
						PreviousMax:         90,
						LastIncrementalScan: started,
						LastFullScan:        started,
						InitialScanComplete: true,
						InitialScanStarted:  &started,
					}, nil
				case "ethereum.errors":
					return nil, errInjected
				case "ethereum.nilcache":
					return nil, nil
				default:
					return nil, nil
				}
			},
		}
		server := newTestServer(dag, adminSvc)

		app := fiber.New()
		app.Get("/bounds", func(c fiber.Ctx) error {
			return server.ListExternalBounds(c)
		})

		resp := doGet(t, app, "/bounds")
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var body struct {
			Bounds []generated.ExternalBounds `json:"bounds"`
			Total  int                        `json:"total"`
		}
		decode(t, resp, &body)
		assert.Equal(t, 1, body.Total)
		require.Len(t, body.Bounds, 1)
		b := body.Bounds[0]
		assert.Equal(t, "ethereum.blocks", b.Id)
		assert.Equal(t, 1, b.Min)
		assert.Equal(t, 100, b.Max)
		require.NotNil(t, b.PreviousMin)
		assert.Equal(t, 2, *b.PreviousMin)
		require.NotNil(t, b.PreviousMax)
		assert.Equal(t, 90, *b.PreviousMax)
		require.NotNil(t, b.LastIncrementalScan)
		require.NotNil(t, b.LastFullScan)
		require.NotNil(t, b.InitialScanComplete)
		assert.True(t, *b.InitialScanComplete)
		require.NotNil(t, b.InitialScanStarted)
	})
}

func TestGetExternalBounds(t *testing.T) {
	t.Run("returns bounds with minimal optional fields", func(t *testing.T) {
		adminSvc := &adminfake.FakeAdminService{
			GetExternalBoundsFn: func(_ context.Context, _ string) (*admin.BoundsCache, error) {
				return &admin.BoundsCache{ModelID: "ethereum.blocks", Min: 0, Max: 50}, nil
			},
		}
		server := newTestServer(&testutil.FakeDAGReader{}, adminSvc)

		app := newErrorApp()
		app.Get("/bounds/:id", func(c fiber.Ctx) error {
			return server.GetExternalBounds(c, c.Params("id"))
		})

		resp := doGet(t, app, "/bounds/ethereum.blocks")
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var b generated.ExternalBounds
		decode(t, resp, &b)
		assert.Equal(t, "ethereum.blocks", b.Id)
		assert.Equal(t, 50, b.Max)
		assert.Nil(t, b.PreviousMin)
		assert.Nil(t, b.PreviousMax)
		assert.Nil(t, b.LastIncrementalScan)
		assert.Nil(t, b.LastFullScan)
		assert.Nil(t, b.InitialScanStarted)
		require.NotNil(t, b.InitialScanComplete)
		assert.False(t, *b.InitialScanComplete)
	})

	t.Run("returns 500 on lookup error", func(t *testing.T) {
		adminSvc := &adminfake.FakeAdminService{
			GetExternalBoundsFn: func(_ context.Context, _ string) (*admin.BoundsCache, error) {
				return nil, errInjected
			},
		}
		server := newTestServer(&testutil.FakeDAGReader{}, adminSvc)

		app := newErrorApp()
		app.Get("/bounds/:id", func(c fiber.Ctx) error {
			return server.GetExternalBounds(c, c.Params("id"))
		})

		resp := doGet(t, app, "/bounds/ethereum.blocks")
		defer resp.Body.Close()
		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	})

	t.Run("returns 404 when bounds are nil", func(t *testing.T) {
		adminSvc := &adminfake.FakeAdminService{
			GetExternalBoundsFn: func(_ context.Context, _ string) (*admin.BoundsCache, error) {
				return nil, nil
			},
		}
		server := newTestServer(&testutil.FakeDAGReader{}, adminSvc)

		app := newErrorApp()
		app.Get("/bounds/:id", func(c fiber.Ctx) error {
			return server.GetExternalBounds(c, c.Params("id"))
		})

		resp := doGet(t, app, "/bounds/ethereum.blocks")
		defer resp.Body.Close()
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	})
}
