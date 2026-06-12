package handlers

import (
	"errors"
	"testing"

	"github.com/ethpandaops/cbt/internal/testutil"
	"github.com/ethpandaops/cbt/internal/testutil/adminfake"
	"github.com/ethpandaops/cbt/pkg/api/generated"
	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var errNodeNotFound = errors.New("node not found")

func TestNewServer(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	mockService := &testutil.FakeModelsService{
		DAG: &testutil.FakeDAGReader{},
	}
	mockAdmin := &adminfake.FakeAdminService{}

	server := NewServer(mockService, mockAdmin, IntervalTypesConfig{}, log)

	assert.NotNil(t, server)
	assert.NotNil(t, server.modelsService)
	assert.NotNil(t, server.adminService)
	assert.NotNil(t, server.log)

	// Verify interface compliance
	var _ generated.ServerInterface = server
}

func TestBuildTransformationModel(t *testing.T) {
	mockDAG := &testutil.FakeDAGReader{
		Dependencies: map[string][]string{
			"test.model": {"dep1.table"},
		},
		Dependents: map[string][]string{
			"test.model": {"dependent1.table"},
		},
	}

	mockTrans := &testutil.FakeTransformation{
		ID:    "test.model",
		Value: "SELECT 1",
		Config: transformation.Config{
			Database: "test",
			Table:    "model",
			Type:     transformation.TypeIncremental,
			Env:      map[string]string{"KEY": "value"},
		},
	}

	model := buildTransformationModel("test.model", mockTrans, mockDAG, configOverrideStatus{})

	assert.Equal(t, "test.model", model.Id)
	assert.Equal(t, "test", model.Database)
	assert.Equal(t, "model", model.Table)
	assert.Equal(t, generated.TransformationModelTypeIncremental, model.Type)
	// DependsOn is nil because mock returns nil for GetStructuredDependencies
	assert.Nil(t, model.DependsOn)
	// Content and other fields populated from handler
	assert.NotEmpty(t, model.Content)
	assert.Equal(t, generated.TransformationModelContentTypeSql, model.ContentType)
}

func TestBuildExternalModel(t *testing.T) {
	mockDAG := &testutil.FakeDAGReader{
		Dependents: map[string][]string{
			"external.table": {"dependent1.table"},
		},
	}

	mockExt := &testutil.FakeExternal{
		ID: "external.table",
		Config: external.Config{
			Database: "external",
			Table:    "table",
		},
	}

	model := buildExternalModel("external.table", mockExt, mockDAG, configOverrideStatus{})

	assert.Equal(t, "external.table", model.Id)
	assert.Equal(t, "external", model.Database)
	assert.Equal(t, "table", model.Table)
	// Cache and Lag are optional fields populated from domain model when available
	assert.Nil(t, model.Cache)
	assert.Nil(t, model.Lag)
}
