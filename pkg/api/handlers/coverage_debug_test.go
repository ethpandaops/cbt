package handlers

import (
	"context"
	"net/http"
	"strconv"
	"testing"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/api/generated"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func transformationNode(id, database, table string, handler transformation.Handler) models.Node {
	return models.Node{
		NodeType: models.NodeTypeTransformation,
		Model: &testutil.FakeTransformation{
			ID:      id,
			Handler: handler,
			Config: transformation.Config{
				Database: database,
				Table:    table,
				Type:     transformation.TypeIncremental,
			},
		},
	}
}

func debugApp(server *Server) *fiber.App {
	app := newErrorApp()
	app.Get("/debug/:id/:pos", func(c fiber.Ctx) error {
		pos, err := strconv.Atoi(c.Params("pos"))
		if err != nil {
			return err
		}
		return server.DebugCoverageAtPosition(c, c.Params("id"), pos)
	})
	return app
}

// TestDebugCoverageAtPositionFull exercises the happy path with a target whose
// handler exposes structured dependencies: one external (with lag, full coverage),
// one transformation (with a blocking gap), recursing into a child dependency.
func TestDebugCoverageAtPositionFull(t *testing.T) {
	structuredHandler := &structuredDepTestHandler{
		deps: []structuredDep{
			{IsGroup: false, SingleDep: "ethereum.blocks"},
			{IsGroup: false, SingleDep: "analytics.dep"},
			{IsGroup: true, GroupDeps: []string{"ethereum.blocks", "ethereum.empty"}},
		},
	}

	// The child of analytics.dep is a flat-dependency transformation.
	childHandler := &flatDepTestHandler{deps: []string{"ethereum.blocks"}}

	nodes := map[string]models.Node{
		"target.model": transformationNode("target.model", "target", "model", structuredHandler),
		"ethereum.blocks": {
			NodeType: models.NodeTypeExternal,
			Model: &testutil.FakeExternal{
				ID:     "ethereum.blocks",
				Config: external.Config{Database: "ethereum", Table: "blocks", Lag: 5},
			},
		},
		"ethereum.empty": {
			NodeType: models.NodeTypeExternal,
			Model: &testutil.FakeExternal{
				ID:     "ethereum.empty",
				Config: external.Config{Database: "ethereum", Table: "empty"},
			},
		},
		"analytics.dep": transformationNode("analytics.dep", "analytics", "dep", childHandler),
	}

	dag := &testutil.FakeDAGReader{
		TransformationByID: map[string]models.Transformation{
			"target.model": nodes["target.model"].Model.(models.Transformation),
		},
		NodeByID: nodes,
	}

	adminSvc := &adminfake.FakeAdminService{
		FirstPositions: map[string]uint64{
			"target.model":    0,
			"analytics.dep":   0,
			"ethereum.blocks": 0,
		},
		LastPositions: map[string]uint64{
			"target.model":    200,
			"analytics.dep":   150,
			"ethereum.blocks": 0,
		},
		ExternalBounds: map[string]*admin.BoundsCache{
			"ethereum.blocks": {ModelID: "ethereum.blocks", Min: 0, Max: 1000},
			"ethereum.empty":  {ModelID: "ethereum.empty", Min: 0, Max: 0},
		},
		Gaps: map[string][]admin.GapInfo{
			"analytics.dep": {{StartPos: 90, EndPos: 110}},
		},
	}

	server := newTestServer(dag, adminSvc)
	app := debugApp(server)

	resp := doGet(t, app, "/debug/target.model/100")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var debug generated.CoverageDebug
	decode(t, resp, &debug)
	assert.Equal(t, "target.model", debug.ModelId)
	assert.Equal(t, 100, debug.Position)
	require.NotNil(t, debug.EndPosition)
	// Three top-level dependencies: external, transformation, OR group.
	require.Len(t, debug.Dependencies, 3)
}

// TestDebugCoverageAtPositionNotIncremental covers the 400 branch.
func TestDebugCoverageAtPositionNotIncremental(t *testing.T) {
	dag := &testutil.FakeDAGReader{
		TransformationByID: map[string]models.Transformation{
			"analytics.scheduled": scheduledTransformation("analytics.scheduled", "analytics", "scheduled"),
		},
	}
	server := newTestServer(dag, &adminfake.FakeAdminService{})

	app := debugApp(server)
	resp := doGet(t, app, "/debug/analytics.scheduled/100")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// TestDebugCoverageAtPositionNotFound covers the 404 branch.
func TestDebugCoverageAtPositionNotFound(t *testing.T) {
	dag := &testutil.FakeDAGReader{GetTransformationErr: errInjected}
	server := newTestServer(dag, &adminfake.FakeAdminService{})

	app := debugApp(server)
	resp := doGet(t, app, "/debug/missing.model/100")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// TestDebugCoverageModelCoverageError covers the 500 branch when buildModelCoverageInfo fails.
func TestDebugCoverageModelCoverageError(t *testing.T) {
	handler := &incrementalTestHandler{minInterval: 10, maxInterval: 100}
	dag := &testutil.FakeDAGReader{
		TransformationByID: map[string]models.Transformation{
			"target.model": &testutil.FakeTransformation{
				ID:      "target.model",
				Handler: handler,
				Config: transformation.Config{
					Database: "target",
					Table:    "model",
					Type:     transformation.TypeIncremental,
				},
			},
		},
	}
	// GetFirstPosition is called by both autoDetectInterval (logged, ignored) and
	// buildModelCoverageInfo (returned as error). Fail it to trigger the 500.
	adminSvc := &adminfake.FakeAdminService{
		GetFirstPositionFn: func(_ context.Context, _ string) (uint64, error) {
			return 0, errInjected
		},
	}
	server := newTestServer(dag, adminSvc)

	app := debugApp(server)
	resp := doGet(t, app, "/debug/target.model/100")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

// TestBuildModelCoverageInfo directly exercises the model coverage builder including
// ranges-in-window and gaps-in-window population, plus each error return.
func TestBuildModelCoverageInfo(t *testing.T) {
	t.Run("populates ranges and gaps in window", func(t *testing.T) {
		adminSvc := &adminfake.FakeAdminService{
			FirstPositions: map[string]uint64{"m": 0},
			LastPositions:  map[string]uint64{"m": 500},
			GetProcessedRangesFn: func(_ context.Context, _ string) ([]admin.ProcessedRange, error) {
				return []admin.ProcessedRange{
					{Position: 90, Interval: 30},  // overlaps [100,200)
					{Position: 500, Interval: 10}, // outside window
				}, nil
			},
			Gaps: map[string][]admin.GapInfo{
				"m": {{StartPos: 150, EndPos: 180}}, // overlaps window
			},
		}
		server := newTestServer(&testutil.FakeDAGReader{}, adminSvc)

		info, err := server.buildModelCoverageInfo(context.Background(), "m", 100, 100)
		require.NoError(t, err)
		assert.True(t, info.HasData)
		require.NotNil(t, info.RangesInWindow)
		assert.Len(t, *info.RangesInWindow, 1)
		require.NotNil(t, info.GapsInWindow)
		assert.Len(t, *info.GapsInWindow, 1)
	})

	t.Run("first position error", func(t *testing.T) {
		adminSvc := &adminfake.FakeAdminService{
			GetFirstPositionFn: func(_ context.Context, _ string) (uint64, error) {
				return 0, errInjected
			},
		}
		server := newTestServer(&testutil.FakeDAGReader{}, adminSvc)
		_, err := server.buildModelCoverageInfo(context.Background(), "m", 100, 100)
		require.Error(t, err)
	})

	t.Run("next position error", func(t *testing.T) {
		adminSvc := &adminfake.FakeAdminService{
			GetNextUnprocessedPositionFn: func(_ context.Context, _ string) (uint64, error) {
				return 0, errInjected
			},
		}
		server := newTestServer(&testutil.FakeDAGReader{}, adminSvc)
		_, err := server.buildModelCoverageInfo(context.Background(), "m", 100, 100)
		require.Error(t, err)
	})

	t.Run("processed ranges error", func(t *testing.T) {
		adminSvc := &adminfake.FakeAdminService{
			GetProcessedRangesFn: func(_ context.Context, _ string) ([]admin.ProcessedRange, error) {
				return nil, errInjected
			},
		}
		server := newTestServer(&testutil.FakeDAGReader{}, adminSvc)
		_, err := server.buildModelCoverageInfo(context.Background(), "m", 100, 100)
		require.Error(t, err)
	})

	t.Run("find gaps error", func(t *testing.T) {
		adminSvc := &adminfake.FakeAdminService{
			LastPositions: map[string]uint64{"m": 500},
			FindGapsFn: func(_ context.Context, _ string, _, _, _ uint64) ([]admin.GapInfo, error) {
				return nil, errInjected
			},
		}
		server := newTestServer(&testutil.FakeDAGReader{}, adminSvc)
		_, err := server.buildModelCoverageInfo(context.Background(), "m", 100, 100)
		require.Error(t, err)
	})
}

// TestBuildDependencyDebugTreeFallback covers the flat-dependency fallback path,
// including a dependency whose GetNode lookup fails (skipped) and one that succeeds.
func TestBuildDependencyDebugTreeFallback(t *testing.T) {
	handler := &flatDepTestHandler{deps: []string{"ethereum.blocks", "missing.dep"}}
	node := models.Transformation(&testutil.FakeTransformation{
		ID:      "target.model",
		Handler: handler,
		Config:  transformation.Config{Database: "target", Table: "model", Type: transformation.TypeIncremental},
	})

	dag := &testutil.FakeDAGReader{
		NodeByID: map[string]models.Node{
			"ethereum.blocks": {
				NodeType: models.NodeTypeExternal,
				Model: &testutil.FakeExternal{
					ID:     "ethereum.blocks",
					Config: external.Config{Database: "ethereum", Table: "blocks"},
				},
			},
		},
	}
	adminSvc := &adminfake.FakeAdminService{
		ExternalBounds: map[string]*admin.BoundsCache{
			"ethereum.blocks": {ModelID: "ethereum.blocks", Min: 0, Max: 1000},
		},
	}
	server := newTestServer(dag, adminSvc)

	result := server.buildDependencyDebugTree(context.Background(), dag, node, 100, 50)
	// missing.dep fails GetNode and is skipped; only ethereum.blocks remains.
	require.Len(t, result, 1)
	assert.Equal(t, "ethereum.blocks", result[0].Id)
}

// TestBuildDependencyDebugTreeNoHandler covers the nil-handler short circuit and the
// no-provider fallthrough.
func TestBuildDependencyDebugTreeNoHandler(t *testing.T) {
	server := newTestServer(&testutil.FakeDAGReader{}, &adminfake.FakeAdminService{})

	nilHandlerNode := models.Transformation(&testutil.FakeTransformation{ID: "x.y"})
	assert.Empty(t, server.buildDependencyDebugTree(context.Background(), &testutil.FakeDAGReader{}, nilHandlerNode, 0, 0))

	// Handler that implements neither structuredDepProvider nor flatDepProvider.
	plainNode := models.Transformation(&testutil.FakeTransformation{
		ID:      "x.y",
		Handler: &plainHandler{},
	})
	assert.Empty(t, server.buildDependencyDebugTree(context.Background(), &testutil.FakeDAGReader{}, plainNode, 0, 0))
}

// TestProcessStructuredDependenciesError covers the structured path where a single
// dependency fails GetNode and is skipped.
func TestProcessStructuredDependenciesError(t *testing.T) {
	handler := &structuredDepTestHandler{
		deps: []structuredDep{{IsGroup: false, SingleDep: "missing.dep"}},
	}
	node := models.Transformation(&testutil.FakeTransformation{
		ID:      "target.model",
		Handler: handler,
		Config:  transformation.Config{Database: "target", Table: "model", Type: transformation.TypeIncremental},
	})
	dag := &testutil.FakeDAGReader{GetNodeErr: errInjected}
	server := newTestServer(dag, &adminfake.FakeAdminService{})

	result := server.buildDependencyDebugTree(context.Background(), dag, node, 100, 50)
	assert.Empty(t, result)
}

// TestBuildORGroupDebugInfo covers OR group member analysis including a member with
// data (not blocking) and a member error (skipped).
func TestBuildORGroupDebugInfo(t *testing.T) {
	dag := &testutil.FakeDAGReader{
		NodeByID: map[string]models.Node{
			"ethereum.blocks": {
				NodeType: models.NodeTypeExternal,
				Model: &testutil.FakeExternal{
					ID:     "ethereum.blocks",
					Config: external.Config{Database: "ethereum", Table: "blocks"},
				},
			},
		},
	}
	adminSvc := &adminfake.FakeAdminService{
		ExternalBounds: map[string]*admin.BoundsCache{
			"ethereum.blocks": {ModelID: "ethereum.blocks", Min: 0, Max: 1000},
		},
	}
	server := newTestServer(dag, adminSvc)

	// "missing.dep" errors (skipped); "ethereum.blocks" has full coverage so the
	// OR group is not blocking.
	info := server.buildORGroupDebugInfo(context.Background(), dag, []string{"ethereum.blocks", "missing.dep"}, 100, 50)
	assert.Equal(t, generated.OrGroup, info.Type)
	require.NotNil(t, info.OrGroupMembers)
	assert.Len(t, *info.OrGroupMembers, 1)
	require.NotNil(t, info.Blocking)
	assert.False(t, *info.Blocking)
}

// TestBuildORGroupDebugInfoBlocking covers an OR group where no member has data.
func TestBuildORGroupDebugInfoBlocking(t *testing.T) {
	dag := &testutil.FakeDAGReader{
		NodeByID: map[string]models.Node{
			"ethereum.empty": {
				NodeType: models.NodeTypeExternal,
				Model: &testutil.FakeExternal{
					ID:     "ethereum.empty",
					Config: external.Config{Database: "ethereum", Table: "empty"},
				},
			},
		},
	}
	// No bounds -> NotInitialized -> no data -> blocking.
	server := newTestServer(dag, &adminfake.FakeAdminService{})

	info := server.buildORGroupDebugInfo(context.Background(), dag, []string{"ethereum.empty"}, 100, 50)
	require.NotNil(t, info.Blocking)
	assert.True(t, *info.Blocking)
}

// TestBuildSingleDependencyDebugInfoDefault covers the default node-type branch.
func TestBuildSingleDependencyDebugInfoDefault(t *testing.T) {
	dag := &testutil.FakeDAGReader{
		NodeByID: map[string]models.Node{
			"weird.node": {NodeType: models.NodeType("unknown"), Model: nil},
		},
	}
	server := newTestServer(dag, &adminfake.FakeAdminService{})

	info, err := server.buildSingleDependencyDebugInfo(context.Background(), dag, "weird.node", 0, 0)
	require.NoError(t, err)
	assert.Equal(t, "weird.node", info.Id)
}

// TestBuildSingleDependencyDebugInfoErrors covers the error returns from the
// external and transformation sub-builders propagating out of the single builder.
func TestBuildSingleDependencyDebugInfoErrors(t *testing.T) {
	server := newTestServer(&testutil.FakeDAGReader{}, &adminfake.FakeAdminService{})

	t.Run("external node with wrong model type", func(t *testing.T) {
		dag := &testutil.FakeDAGReader{
			NodeByID: map[string]models.Node{
				"bad.external": {NodeType: models.NodeTypeExternal, Model: "not-external"},
			},
		}
		_, err := server.buildSingleDependencyDebugInfo(context.Background(), dag, "bad.external", 0, 0)
		require.ErrorIs(t, err, ErrNodeNotExternal)
	})

	t.Run("transformation node with wrong model type", func(t *testing.T) {
		dag := &testutil.FakeDAGReader{
			NodeByID: map[string]models.Node{
				"bad.trans": {NodeType: models.NodeTypeTransformation, Model: "not-trans"},
			},
		}
		_, err := server.buildSingleDependencyDebugInfo(context.Background(), dag, "bad.trans", 0, 0)
		require.ErrorIs(t, err, ErrNodeNotTransformation)
	})
}

// TestBuildExternalDependencyDebugInfo covers external bounds variants.
func TestBuildExternalDependencyDebugInfo(t *testing.T) {
	tests := []struct {
		name           string
		bounds         *admin.BoundsCache
		boundsErr      error
		lag            uint64
		position       uint64
		interval       uint64
		wantStatus     generated.DependencyDebugInfoCoverageStatus
		wantBlocking   bool
		wantHasData    bool
		wantLagApplied bool
	}{
		{
			name:         "no bounds -> not initialized",
			bounds:       nil,
			position:     100,
			interval:     50,
			wantStatus:   generated.NotInitialized,
			wantBlocking: false,
		},
		{
			name:         "bounds error -> not initialized",
			boundsErr:    errInjected,
			position:     100,
			interval:     50,
			wantStatus:   generated.NotInitialized,
			wantBlocking: false,
		},
		{
			name:         "max zero -> no data",
			bounds:       &admin.BoundsCache{Min: 0, Max: 0},
			position:     100,
			interval:     50,
			wantStatus:   generated.NoData,
			wantBlocking: true,
		},
		{
			name:         "full coverage",
			bounds:       &admin.BoundsCache{Min: 0, Max: 1000},
			position:     100,
			interval:     50,
			wantStatus:   generated.FullCoverage,
			wantBlocking: false,
			wantHasData:  true,
		},
		{
			name:         "out of bounds -> no data and blocking",
			bounds:       &admin.BoundsCache{Min: 200, Max: 300},
			position:     100,
			interval:     50,
			wantStatus:   generated.NoData,
			wantBlocking: true,
			wantHasData:  true,
		},
		{
			name:           "lag applied reduces max",
			bounds:         &admin.BoundsCache{Min: 0, Max: 1000},
			lag:            100,
			position:       100,
			interval:       50,
			wantStatus:     generated.FullCoverage,
			wantBlocking:   false,
			wantHasData:    true,
			wantLagApplied: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ext := &testutil.FakeExternal{
				ID:     "ethereum.blocks",
				Config: external.Config{Database: "ethereum", Table: "blocks", Lag: tt.lag},
			}
			node := models.Node{NodeType: models.NodeTypeExternal, Model: ext}

			adminSvc := &adminfake.FakeAdminService{
				GetExternalBoundsFn: func(_ context.Context, _ string) (*admin.BoundsCache, error) {
					return tt.bounds, tt.boundsErr
				},
			}
			server := newTestServer(&testutil.FakeDAGReader{}, adminSvc)

			depInfo := &generated.DependencyDebugInfo{Id: "ethereum.blocks"}
			result, err := server.buildExternalDependencyDebugInfo(context.Background(), node, depInfo, tt.position, tt.interval)
			require.NoError(t, err)
			require.NotNil(t, result.CoverageStatus)
			assert.Equal(t, tt.wantStatus, *result.CoverageStatus)
			assert.Equal(t, tt.wantHasData, result.Bounds.HasData)
			if tt.wantLagApplied {
				require.NotNil(t, result.Bounds.LagApplied)
			}
			if result.Blocking != nil {
				assert.Equal(t, tt.wantBlocking, *result.Blocking)
			}
		})
	}
}

// TestBuildExternalDependencyDebugInfoNotExternal covers the wrong-model-type error.
func TestBuildExternalDependencyDebugInfoNotExternal(t *testing.T) {
	server := newTestServer(&testutil.FakeDAGReader{}, &adminfake.FakeAdminService{})
	node := models.Node{NodeType: models.NodeTypeExternal, Model: "not-external"}
	depInfo := &generated.DependencyDebugInfo{Id: "x"}

	_, err := server.buildExternalDependencyDebugInfo(context.Background(), node, depInfo, 0, 0)
	require.ErrorIs(t, err, ErrNodeNotExternal)
}

// TestBuildTransformationDependencyDebugInfoNotTransformation covers the wrong-type error.
func TestBuildTransformationDependencyDebugInfoNotTransformation(t *testing.T) {
	server := newTestServer(&testutil.FakeDAGReader{}, &adminfake.FakeAdminService{})
	node := models.Node{NodeType: models.NodeTypeTransformation, Model: "not-a-transformation"}
	depInfo := &generated.DependencyDebugInfo{Id: "x"}

	_, err := server.buildTransformationDependencyDebugInfo(context.Background(), &testutil.FakeDAGReader{}, node, depInfo, 0, 0)
	require.ErrorIs(t, err, ErrNodeNotTransformation)
}

// TestBuildTransformationDependencyDebugInfoStatuses covers coverage-status variants
// for transformation dependencies.
func TestBuildTransformationDependencyDebugInfoStatuses(t *testing.T) {
	tests := []struct {
		name          string
		first         uint64
		last          uint64
		gaps          []admin.GapInfo
		trackPosition *bool
		position      uint64
		interval      uint64
		wantStatus    generated.DependencyDebugInfoCoverageStatus
	}{
		{
			name:       "not initialized when last is zero",
			last:       0,
			position:   100,
			interval:   50,
			wantStatus: generated.NotInitialized,
		},
		{
			name:       "has gaps",
			first:      0,
			last:       500,
			gaps:       []admin.GapInfo{{StartPos: 120, EndPos: 160}},
			position:   100,
			interval:   50,
			wantStatus: generated.HasGaps,
		},
		{
			name:       "full coverage",
			first:      0,
			last:       500,
			position:   100,
			interval:   50,
			wantStatus: generated.FullCoverage,
		},
		{
			name:       "no data when out of range",
			first:      0,
			last:       120,
			position:   100,
			interval:   50,
			wantStatus: generated.NoData,
		},
		{
			name:          "non-incremental skips gap lookup",
			first:         0,
			last:          500,
			trackPosition: ptrTo(false),
			gaps:          []admin.GapInfo{{StartPos: 120, EndPos: 160}},
			position:      100,
			interval:      50,
			wantStatus:    generated.FullCoverage,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &incrementalTestHandler{}
			if tt.trackPosition != nil {
				handler.trackPositionSet = true
				handler.trackPosition = *tt.trackPosition
			}
			trans := &testutil.FakeTransformation{
				ID:      "analytics.dep",
				Handler: handler,
				Config:  transformation.Config{Database: "analytics", Table: "dep", Type: transformation.TypeIncremental},
			}
			node := models.Node{NodeType: models.NodeTypeTransformation, Model: trans}

			adminSvc := &adminfake.FakeAdminService{
				FirstPositions: map[string]uint64{"analytics.dep": tt.first},
				LastPositions:  map[string]uint64{"analytics.dep": tt.last},
				Gaps:           map[string][]admin.GapInfo{"analytics.dep": tt.gaps},
			}
			server := newTestServer(&testutil.FakeDAGReader{}, adminSvc)

			depInfo := &generated.DependencyDebugInfo{Id: "analytics.dep"}
			result, err := server.buildTransformationDependencyDebugInfo(
				context.Background(), &testutil.FakeDAGReader{}, node, depInfo, tt.position, tt.interval)
			require.NoError(t, err)
			require.NotNil(t, result.CoverageStatus)
			assert.Equal(t, tt.wantStatus, *result.CoverageStatus)
		})
	}
}

// TestGetTransformationBoundsErrors covers the logged-error branches.
func TestGetTransformationBoundsErrors(t *testing.T) {
	adminSvc := &adminfake.FakeAdminService{
		GetFirstPositionFn: func(_ context.Context, _ string) (uint64, error) {
			return 0, errInjected
		},
		GetNextUnprocessedPositionFn: func(_ context.Context, _ string) (uint64, error) {
			return 0, errInjected
		},
	}
	server := newTestServer(&testutil.FakeDAGReader{}, adminSvc)

	first, last := server.getTransformationBounds(context.Background(), "x")
	assert.Equal(t, uint64(0), first)
	assert.Equal(t, uint64(0), last)
}

// TestFindOverlappingGaps covers the error and empty-result branches.
func TestFindOverlappingGaps(t *testing.T) {
	t.Run("find gaps error returns nil", func(t *testing.T) {
		adminSvc := &adminfake.FakeAdminService{
			FindGapsFn: func(_ context.Context, _ string, _, _, _ uint64) ([]admin.GapInfo, error) {
				return nil, errInjected
			},
		}
		server := newTestServer(&testutil.FakeDAGReader{}, adminSvc)
		assert.Nil(t, server.findOverlappingGaps(context.Background(), "x", 0, 100, 0, 50))
	})

	t.Run("no overlapping gaps returns nil", func(t *testing.T) {
		adminSvc := &adminfake.FakeAdminService{
			FindGapsFn: func(_ context.Context, _ string, _, _, _ uint64) ([]admin.GapInfo, error) {
				return []admin.GapInfo{{StartPos: 1000, EndPos: 1100}}, nil
			},
		}
		server := newTestServer(&testutil.FakeDAGReader{}, adminSvc)
		assert.Nil(t, server.findOverlappingGaps(context.Background(), "x", 0, 2000, 0, 50))
	})
}

// TestAutoDetectInterval covers all branches of interval auto-detection.
func TestAutoDetectInterval(t *testing.T) {
	t.Run("no handler interval uses fallback default", func(t *testing.T) {
		adminSvc := &adminfake.FakeAdminService{}
		server := newTestServer(&testutil.FakeDAGReader{}, adminSvc)
		// handler is nil -> default 60; no data -> returns default.
		got := server.autoDetectInterval(context.Background(), "m", 100, nil)
		assert.Equal(t, uint64(60), got)
	})

	t.Run("uses handler max interval", func(t *testing.T) {
		handler := &intervalOnlyHandler{minInterval: 5, maxInterval: 120}
		adminSvc := &adminfake.FakeAdminService{
			LastPositions: map[string]uint64{"m": 500},
		}
		server := newTestServer(&testutil.FakeDAGReader{}, adminSvc)
		got := server.autoDetectInterval(context.Background(), "m", 1000, handler)
		// Position 1000 not in any gap -> default (handler max) interval.
		assert.Equal(t, uint64(120), got)
	})

	t.Run("position in gap uses gap size", func(t *testing.T) {
		handler := &intervalOnlyHandler{minInterval: 5, maxInterval: 120}
		adminSvc := &adminfake.FakeAdminService{
			LastPositions: map[string]uint64{"m": 500},
			Gaps: map[string][]admin.GapInfo{
				"m": {{StartPos: 90, EndPos: 210}},
			},
		}
		server := newTestServer(&testutil.FakeDAGReader{}, adminSvc)
		got := server.autoDetectInterval(context.Background(), "m", 100, handler)
		assert.Equal(t, uint64(120), got) // gap size 210-90 = 120
	})

	t.Run("first position error returns default", func(t *testing.T) {
		adminSvc := &adminfake.FakeAdminService{
			GetFirstPositionFn: func(_ context.Context, _ string) (uint64, error) {
				return 0, errInjected
			},
		}
		server := newTestServer(&testutil.FakeDAGReader{}, adminSvc)
		assert.Equal(t, uint64(60), server.autoDetectInterval(context.Background(), "m", 100, nil))
	})

	t.Run("next position error returns default", func(t *testing.T) {
		adminSvc := &adminfake.FakeAdminService{
			GetNextUnprocessedPositionFn: func(_ context.Context, _ string) (uint64, error) {
				return 0, errInjected
			},
		}
		server := newTestServer(&testutil.FakeDAGReader{}, adminSvc)
		assert.Equal(t, uint64(60), server.autoDetectInterval(context.Background(), "m", 100, nil))
	})

	t.Run("find gaps error returns default", func(t *testing.T) {
		adminSvc := &adminfake.FakeAdminService{
			LastPositions: map[string]uint64{"m": 500},
			FindGapsFn: func(_ context.Context, _ string, _, _, _ uint64) ([]admin.GapInfo, error) {
				return nil, errInjected
			},
		}
		server := newTestServer(&testutil.FakeDAGReader{}, adminSvc)
		assert.Equal(t, uint64(60), server.autoDetectInterval(context.Background(), "m", 100, nil))
	})
}

// TestBuildValidationDebugInfo exercises the validation builder across the
// blocking-gap, blocking-status, in-bounds and out-of-bounds branches.
func TestBuildValidationDebugInfo(t *testing.T) {
	server := newTestServer(&testutil.FakeDAGReader{}, &adminfake.FakeAdminService{})

	t.Run("no dependencies leaves bounds unset", func(t *testing.T) {
		info := server.buildValidationDebugInfo(context.Background(), "m", 100, 50, nil)
		assert.False(t, info.InBounds)
		assert.Nil(t, info.ValidRange)
	})

	t.Run("blocking gap overlapping request sets next valid position", func(t *testing.T) {
		blocking := true
		overlaps := true
		notInit := generated.NotInitialized
		deps := []generated.DependencyDebugInfo{
			{
				Id:             "analytics.dep",
				Blocking:       &blocking,
				CoverageStatus: &notInit,
				Bounds:         generated.BoundsInfo{HasData: true, Min: 0, Max: 1000},
				Gaps: &[]generated.GapInfo{
					{Start: 120, End: 160, OverlapsRequest: &overlaps},
				},
			},
		}
		info := server.buildValidationDebugInfo(context.Background(), "m", 100, 50, deps)
		assert.True(t, info.HasDependencyGaps)
		require.NotNil(t, info.NextValidPosition)
		assert.Equal(t, 160, *info.NextValidPosition)
		require.NotNil(t, info.BlockingGaps)
		assert.Len(t, *info.BlockingGaps, 1)
	})

	t.Run("blocking status not-initialized and no-data produce reasons", func(t *testing.T) {
		blocking := true
		notInit := generated.NotInitialized
		noData := generated.NoData
		deps := []generated.DependencyDebugInfo{
			{
				Id:             "dep.notinit",
				Blocking:       &blocking,
				CoverageStatus: &notInit,
				Bounds:         generated.BoundsInfo{HasData: false},
			},
			{
				Id:             "dep.nodata",
				Blocking:       &blocking,
				CoverageStatus: &noData,
				Bounds:         generated.BoundsInfo{HasData: false},
			},
		}
		info := server.buildValidationDebugInfo(context.Background(), "m", 100, 50, deps)
		require.NotNil(t, info.Reasons)
		assert.GreaterOrEqual(t, len(*info.Reasons), 2)
	})

	t.Run("position before min valid yields before-data reason", func(t *testing.T) {
		// Dependency with high Min forces minValid up; position 10 < minValid 500.
		deps := []generated.DependencyDebugInfo{
			{
				Id:     "analytics.dep",
				Bounds: generated.BoundsInfo{HasData: true, Min: 500, Max: 2000},
			},
		}
		info := server.buildValidationDebugInfo(context.Background(), "m", 10, 50, deps)
		assert.False(t, info.InBounds)
		require.NotNil(t, info.ValidRange)
		assert.Equal(t, 500, info.ValidRange.Min)
		require.NotNil(t, info.Reasons)
		assert.NotEmpty(t, *info.Reasons)
	})

	t.Run("position after max valid sets next valid and reason", func(t *testing.T) {
		deps := []generated.DependencyDebugInfo{
			{
				Id:     "analytics.dep",
				Bounds: generated.BoundsInfo{HasData: true, Min: 0, Max: 120},
			},
		}
		info := server.buildValidationDebugInfo(context.Background(), "m", 100, 50, deps)
		assert.False(t, info.InBounds)
		require.NotNil(t, info.NextValidPosition)
		assert.Equal(t, 120, *info.NextValidPosition)
	})

	t.Run("in bounds when position within valid range", func(t *testing.T) {
		deps := []generated.DependencyDebugInfo{
			{
				Id:     "analytics.dep",
				Bounds: generated.BoundsInfo{HasData: true, Min: 0, Max: 1000},
			},
		}
		info := server.buildValidationDebugInfo(context.Background(), "m", 100, 50, deps)
		assert.True(t, info.InBounds)
	})
}

// plainHandler implements transformation.Handler but none of the dependency
// provider interfaces, exercising the fallthrough in buildDependencyDebugTree.
type plainHandler struct{}

func (h *plainHandler) Type() transformation.Type { return transformation.TypeIncremental }
func (h *plainHandler) Config() any               { return nil }
func (h *plainHandler) Validate() error           { return nil }
func (h *plainHandler) ShouldTrackPosition() bool { return true }
func (h *plainHandler) GetTemplateVariables(_ context.Context, _ transformation.TaskInfo) map[string]any {
	return map[string]any{}
}
func (h *plainHandler) GetAdminTable() transformation.AdminTable { return transformation.AdminTable{} }
func (h *plainHandler) RecordCompletion(_ context.Context, _ any, _ string, _ transformation.TaskInfo) error {
	return nil
}
