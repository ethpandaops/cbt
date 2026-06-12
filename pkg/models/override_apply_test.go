package models

import (
	"testing"

	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// applierHandler is a transformation.Handler that also implements
// transformationOverrideApplier so ApplyToTransformation can be exercised end to end.
type applierHandler struct {
	mockHandler
	applied *transformation.Override
}

func (h *applierHandler) ApplyOverrides(override *transformation.Override) {
	h.applied = override
}

var _ transformation.Handler = (*applierHandler)(nil)

func TestOverrideApplyToTransformation(t *testing.T) {
	maxVal := uint64(4242)
	tOverride := &TransformationOverride{
		Interval: &IntervalOverride{Max: &maxVal},
	}

	tests := []struct {
		name        string
		override    *Override
		handler     transformation.Handler
		wantApplied bool
	}{
		{
			name:        "nil override does nothing",
			override:    nil,
			handler:     &applierHandler{},
			wantApplied: false,
		},
		{
			name:        "override with nil config does nothing",
			override:    &Override{},
			handler:     &applierHandler{},
			wantApplied: false,
		},
		{
			name:        "config of wrong type does nothing",
			override:    &Override{Config: &ExternalOverride{}},
			handler:     &applierHandler{},
			wantApplied: false,
		},
		{
			name:        "nil handler does nothing",
			override:    &Override{Config: tOverride},
			handler:     nil,
			wantApplied: false,
		},
		{
			name:        "handler without applier interface does nothing",
			override:    &Override{Config: tOverride},
			handler:     &mockHandler{},
			wantApplied: false,
		},
		{
			name:        "applies override to applier handler",
			override:    &Override{Config: tOverride},
			handler:     &applierHandler{},
			wantApplied: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			model := &mockTransformation{
				id:      "db.t",
				config:  transformation.Config{Database: "db", Table: "t"},
				handler: tt.handler,
			}

			tt.override.ApplyToTransformation(model)

			if applier, ok := tt.handler.(*applierHandler); ok {
				if tt.wantApplied {
					require.NotNil(t, applier.applied)
					require.NotNil(t, applier.applied.Interval)
					assert.Equal(t, maxVal, *applier.applied.Interval.Max)
				} else {
					assert.Nil(t, applier.applied)
				}
			}
		})
	}
}

func TestOverrideApplyToExternal(t *testing.T) {
	lag := uint64(999)

	tests := []struct {
		name     string
		override *Override
		wantLag  uint64
	}{
		{
			name:     "nil override does nothing",
			override: nil,
			wantLag:  100,
		},
		{
			name:     "override with nil config does nothing",
			override: &Override{},
			wantLag:  100,
		},
		{
			name:     "config of wrong type does nothing",
			override: &Override{Config: &TransformationOverride{}},
			wantLag:  100,
		},
		{
			name:     "applies external override",
			override: &Override{Config: &ExternalOverride{Lag: &lag}},
			wantLag:  999,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &external.Config{Database: "db", Table: "t", Lag: 100}
			tt.override.ApplyToExternal(cfg)
			assert.Equal(t, tt.wantLag, cfg.Lag)
		})
	}
}

func TestOverrideResolveConfigExternalError(t *testing.T) {
	// rawConfig is a scalar where a struct is expected, so decoding fails.
	var m Override
	require.NoError(t, yaml.Unmarshal([]byte("config: not-a-map\n"), &m))

	err := m.ResolveConfig(TypeExternal)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal external config")
}

func TestOverrideResolveConfigTransformationError(t *testing.T) {
	var m Override
	require.NoError(t, yaml.Unmarshal([]byte("config: not-a-map\n"), &m))

	err := m.ResolveConfig(TypeTransformation)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal transformation config")
}
