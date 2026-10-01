package worker

import (
	"context"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/ethpandaops/cbt/pkg/tasks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// slotExternalTemplate mirrors the windowed incremental scan template used by the
// slot-typed external models in xatu-cbt.
const slotExternalTemplate = `
SELECT
    {{ if .cache.is_incremental_scan }}
      '{{ .cache.previous_min }}' as min,
    {{ else }}
      toUnixTimestamp(min(slot_start_date_time)) as min,
    {{ end }}
    toUnixTimestamp(max(slot_start_date_time)) as max
FROM {{ .self.helpers.from }}
WHERE
    meta_network_name = '{{ .env.NETWORK }}'
    {{- $ts := default "0" .env.EXTERNAL_MODEL_MIN_TIMESTAMP -}}
    {{- if .cache.is_incremental_scan -}}
      {{- if .cache.previous_max -}}
        {{- $ts = .cache.previous_max -}}
      {{- end -}}
    {{- end }}
    AND slot_start_date_time >= fromUnixTimestamp({{ $ts }})
    {{- if .cache.is_incremental_scan }}
      AND slot_start_date_time <= fromUnixTimestamp({{ $ts }}) + {{ default "100000" .env.EXTERNAL_MODEL_SCAN_SIZE_TIMESTAMP }}
    {{- end }}
`

var (
	minLiteralRe = regexp.MustCompile(`'(\d+)' as min`)
	lowerBoundRe = regexp.MustCompile(`slot_start_date_time >= fromUnixTimestamp\((\d+)\)`)
	upperBoundRe = regexp.MustCompile(`slot_start_date_time <= fromUnixTimestamp\((\d+)\) \+ (\d+)`)
)

// slotTable evaluates the bounds queries rendered from slotExternalTemplate against a set
// of row timestamps, following ClickHouse semantics (min/max over no rows is zero).
type slotTable struct {
	timestamps []uint64
}

func (s *slotTable) queryBounds(t *testing.T, query string, dest any) {
	t.Helper()

	lower := mustParseUint(t, lowerBoundRe.FindStringSubmatch(query)[1])
	upper := ^uint64(0)

	if m := upperBoundRe.FindStringSubmatch(query); m != nil {
		upper = mustParseUint(t, m[1]) + mustParseUint(t, m[2])
	}

	var (
		minTS = ^uint64(0)
		maxTS uint64
		found bool
	)

	for _, ts := range s.timestamps {
		if ts < lower || ts > upper {
			continue
		}

		found = true
		minTS = min(minTS, ts)
		maxTS = max(maxTS, ts)
	}

	if !found {
		minTS = 0
	}

	if m := minLiteralRe.FindStringSubmatch(query); m != nil {
		minTS = mustParseUint(t, m[1])
	}

	result, ok := dest.(*struct {
		Min clickhouse.FlexUint64 `ch:"min"`
		Max clickhouse.FlexUint64 `ch:"max"`
	})
	require.True(t, ok, "unexpected bounds destination %T", dest)

	result.Min = clickhouse.FlexUint64(minTS)
	result.Max = clickhouse.FlexUint64(maxTS)
}

func mustParseUint(t *testing.T, s string) uint64 {
	t.Helper()

	v, err := strconv.ParseUint(s, 10, 64)
	require.NoError(t, err)

	return v
}

const slotModelID = "default.canonical_beacon_block_execution_payload_bid"

// slotScanHarness runs UpdateBounds for slotExternalTemplate against an in-memory slotTable.
type slotScanHarness struct {
	t     *testing.T
	table *slotTable
	admin *adminfake.FakeAdminService
	exec  *ModelExecutor
}

func newSlotScanHarness(t *testing.T, env map[string]string) *slotScanHarness {
	t.Helper()

	h := &slotScanHarness{t: t, table: &slotTable{}, admin: &adminfake.FakeAdminService{}}

	engine := models.NewTemplateEngine(&clickhouse.Config{}, models.NewDependencyGraph(), env)
	mockModels := &testutil.FakeModelsService{
		DAG: &testutil.FakeDAGReader{ExternalNode: &testutil.FakeExternal{
			ID:     slotModelID,
			Value:  slotExternalTemplate,
			Config: external.Config{Database: "default", Table: "canonical_beacon_block_execution_payload_bid"},
		}},
		RenderExternalFn: func(model models.External, cacheState map[string]any) (string, error) {
			return engine.RenderExternal(model, cacheState)
		},
	}
	mockCH := &testutil.FakeClickHouseClient{
		QueryOneFn: func(_ context.Context, query string, dest any) error {
			h.table.queryBounds(t, query, dest)

			return nil
		},
	}
	h.exec = NewModelExecutor(quietLogger(), mockCH, mockModels, h.admin)

	return h
}

func (h *slotScanHarness) scan(scanType string) *admin.BoundsCache {
	h.t.Helper()
	require.NoError(h.t, h.exec.UpdateBounds(context.Background(), slotModelID, scanType))

	bounds, err := h.admin.GetExternalBounds(context.Background(), slotModelID)
	require.NoError(h.t, err)
	require.NotNil(h.t, bounds)

	return bounds
}

// TestUpdateBounds_EmptyExternalPicksUpFirstRows covers an external model that has no rows
// when CBT first scans it (a table for a fork that has not activated yet). Once rows
// appear, the next incremental scan must publish them without waiting for a full scan.
func TestUpdateBounds_EmptyExternalPicksUpFirstRows(t *testing.T) {
	forkTime := uint64(time.Date(2026, 10, 6, 13, 53, 36, 0, time.UTC).Unix())
	h := newSlotScanHarness(t, map[string]string{"NETWORK": "sepolia"})

	bounds := h.scan(tasks.ScanTypeFull)
	require.Zero(t, bounds.Max, "table starts empty")

	for range 3 {
		bounds = h.scan(tasks.ScanTypeIncremental)
		require.Zero(t, bounds.Max, "still empty")
	}

	h.table.timestamps = []uint64{forkTime, forkTime + 12, forkTime + 24}

	bounds = h.scan(tasks.ScanTypeIncremental)
	assert.Equal(t, forkTime, bounds.Min, "first incremental scan after rows appear must find the true min")
	assert.Equal(t, forkTime+24, bounds.Max, "first incremental scan after rows appear must find the true max")

	// Rows keep arriving over the following slots; bounds follow on every incremental scan.
	for i := range uint64(10) {
		h.table.timestamps = append(h.table.timestamps, forkTime+36+i*12)

		bounds = h.scan(tasks.ScanTypeIncremental)
		require.Equal(t, forkTime, bounds.Min)
		require.Equal(t, forkTime+36+i*12, bounds.Max)
	}
}

// TestUpdateBounds_RefreshedEmptyCacheDoesNotRewindMax covers bounds written by a full scan
// over an empty cache (for example a manual refresh), which leave previous_max at zero. The
// next incremental scan must not restart its window at the configured minimum timestamp
// and publish a max far behind the real data.
func TestUpdateBounds_RefreshedEmptyCacheDoesNotRewindMax(t *testing.T) {
	const (
		minTimestamp = uint64(1_700_000_000)
		slots        = 50_000
	)

	h := newSlotScanHarness(t, map[string]string{
		"NETWORK":                      "sepolia",
		"EXTERNAL_MODEL_MIN_TIMESTAMP": strconv.FormatUint(minTimestamp, 10),
	})

	for i := range uint64(slots) {
		h.table.timestamps = append(h.table.timestamps, minTimestamp+i*12)
	}

	lastTimestamp := minTimestamp + (slots-1)*12

	h.admin.ExternalBounds = map[string]*admin.BoundsCache{
		slotModelID: {
			ModelID:             slotModelID,
			Min:                 minTimestamp,
			Max:                 lastTimestamp,
			InitialScanComplete: true,
		},
	}

	for range 3 {
		bounds := h.scan(tasks.ScanTypeIncremental)
		require.Equal(t, minTimestamp, bounds.Min)
		require.Equal(t, lastTimestamp, bounds.Max)
	}
}

// TestUpdateBounds_IncrementalScanType verifies which scan the template is rendered for
// given the cached bounds.
func TestUpdateBounds_IncrementalScanType(t *testing.T) {
	const modelID = "test.external"

	tests := []struct {
		name            string
		scanType        string
		cache           *admin.BoundsCache
		wantIncremental bool
		wantFull        bool
		wantRendered    bool
	}{
		{
			name:         "incremental with empty cache runs as full scan",
			scanType:     tasks.ScanTypeIncremental,
			cache:        &admin.BoundsCache{InitialScanComplete: true},
			wantFull:     true,
			wantRendered: true,
		},
		{
			name:     "incremental with max but no previous max runs as full scan",
			scanType: tasks.ScanTypeIncremental,
			cache: &admin.BoundsCache{
				Min: 100, Max: 500, InitialScanComplete: true,
			},
			wantFull:     true,
			wantRendered: true,
		},
		{
			name:     "incremental with previous max but no max runs as full scan",
			scanType: tasks.ScanTypeIncremental,
			cache: &admin.BoundsCache{
				PreviousMin: 100, PreviousMax: 500, InitialScanComplete: true,
			},
			wantFull:     true,
			wantRendered: true,
		},
		{
			name:     "incremental with populated cache stays incremental",
			scanType: tasks.ScanTypeIncremental,
			cache: &admin.BoundsCache{
				Min: 100, Max: 500, PreviousMin: 100, PreviousMax: 450, InitialScanComplete: true,
			},
			wantIncremental: true,
			wantRendered:    true,
		},
		{
			name:     "full scan with populated cache stays full",
			scanType: tasks.ScanTypeFull,
			cache: &admin.BoundsCache{
				Min: 100, Max: 500, PreviousMin: 100, PreviousMax: 450, InitialScanComplete: true,
			},
			wantFull:     true,
			wantRendered: true,
		},
		{
			name:     "incremental without cache is skipped",
			scanType: tasks.ScanTypeIncremental,
			cache:    nil,
		},
		{
			name:     "incremental before the initial scan completes is skipped",
			scanType: tasks.ScanTypeIncremental,
			cache:    &admin.BoundsCache{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rendered map[string]any

			mockModels := &testutil.FakeModelsService{
				DAG: &testutil.FakeDAGReader{
					ExternalNode: &testutil.FakeExternal{
						ID:     modelID,
						Config: external.Config{Database: "test", Table: "external"},
					},
				},
				RenderExternalFn: func(_ models.External, cacheState map[string]any) (string, error) {
					rendered = cacheState

					return "SELECT 1 as min, 2 as max", nil
				},
			}
			mockAdmin := &adminfake.FakeAdminService{ExternalBoundsDefault: tt.cache}
			mockCH := &testutil.FakeClickHouseClient{BoundsMin: 1, BoundsMax: 2}

			executor := NewModelExecutor(quietLogger(), mockCH, mockModels, mockAdmin)
			require.NoError(t, executor.UpdateBounds(context.Background(), modelID, tt.scanType))

			if !tt.wantRendered {
				assert.Nil(t, rendered, "scan should have been skipped")

				return
			}

			require.NotNil(t, rendered)
			assert.Equal(t, tt.wantIncremental, rendered["is_incremental_scan"])
			assert.Equal(t, tt.wantFull, rendered["is_full_scan"])
		})
	}
}
