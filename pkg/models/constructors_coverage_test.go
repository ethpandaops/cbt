package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestNewExternal(t *testing.T) {
	validSQL := []byte(`---
database: db
table: t
interval:
  type: second
cache:
  incremental_scan_interval: 1m
  full_scan_interval: 1h
---
SELECT 1`)

	tests := []struct {
		name     string
		content  []byte
		filePath string
		wantErr  error
		wantNil  bool
	}{
		{
			name:     "valid sql external",
			content:  validSQL,
			filePath: "model.sql",
		},
		{
			name:     "sql parse error surfaces",
			content:  []byte("no frontmatter"),
			filePath: "bad.sql",
			wantNil:  true,
		},
		{
			name:     "non-sql extension is invalid",
			content:  validSQL,
			filePath: "model.yaml",
			wantErr:  ErrInvalidExternalType,
			wantNil:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			model, err := NewExternal(tt.content, tt.filePath)
			if tt.wantNil {
				require.Error(t, err)
				assert.Nil(t, model)
				if tt.wantErr != nil {
					require.ErrorIs(t, err, tt.wantErr)
				}
				return
			}
			require.NoError(t, err)
			require.NotNil(t, model)
			assert.Equal(t, "db.t", model.GetID())
		})
	}
}

func TestNewTransformation(t *testing.T) {
	validSQL := []byte(`---
type: incremental
database: db
table: t
interval:
  max: 100
  min: 10
  type: second
schedules:
  forwardfill: "0 * * * *"
dependencies:
  - dep.table
---
SELECT 1`)

	validExec := []byte(`type: incremental
database: db
table: t
interval:
  max: 100
  min: 10
  type: second
schedules:
  forwardfill: "0 * * * *"
dependencies:
  - dep.table
exec: "echo test"`)

	tests := []struct {
		name     string
		content  []byte
		filePath string
		wantErr  error
		wantNil  bool
	}{
		{
			name:     "valid sql transformation",
			content:  validSQL,
			filePath: "model.sql",
		},
		{
			name:     "valid yaml exec transformation",
			content:  validExec,
			filePath: "model.yaml",
		},
		{
			name:     "valid yml exec transformation",
			content:  validExec,
			filePath: "model.yml",
		},
		{
			name:     "sql parse error surfaces",
			content:  []byte("no frontmatter"),
			filePath: "bad.sql",
			wantNil:  true,
		},
		{
			name:     "yaml parse error surfaces",
			content:  []byte("type: incremental\ninterval: not-an-object"),
			filePath: "bad.yaml",
			wantNil:  true,
		},
		{
			name:     "unsupported extension is invalid",
			content:  validExec,
			filePath: "model.txt",
			wantErr:  ErrInvalidTransformationType,
			wantNil:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			model, err := NewTransformation(tt.content, tt.filePath)
			if tt.wantNil {
				require.Error(t, err)
				assert.Nil(t, model)
				if tt.wantErr != nil {
					require.ErrorIs(t, err, tt.wantErr)
				}
				return
			}
			require.NoError(t, err)
			require.NotNil(t, model)
			assert.Equal(t, "db.t", model.GetID())
		})
	}
}

func TestOverrideUnmarshalYAMLDecodeError(t *testing.T) {
	// `enabled` must be a bool; a mapping value makes the temp-struct decode fail.
	var m Override
	err := yaml.Unmarshal([]byte("enabled:\n  nested: true\n"), &m)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal override")
}
