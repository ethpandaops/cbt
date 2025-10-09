package handlers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/api/generated"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/models/external"
	"github.com/ethpandaops/cbt/pkg/models/transformation"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var errNodeNotFound = errors.New("node not found")

// mockDAGReader implements models.DAGReader for testing
type mockDAGReader struct {
	transformations      []models.Transformation
	externals            []models.Node
	transformationByID   map[string]models.Transformation
	externalByID         map[string]models.External
	nodeByID             map[string]models.Node
	dependencies         map[string][]string
	dependents           map[string][]string
	allDependencies      map[string][]string
	allDependents        map[string][]string
	pathBetween          map[string]map[string]bool
	getTransformationErr error
	getExternalErr       error
	getNodeErr           error
}

func (m *mockDAGReader) GetTransformationNodes() []models.Transformation {
	return m.transformations
}

func (m *mockDAGReader) GetExternalNodes() []models.Node {
	return m.externals
}

func (m *mockDAGReader) GetTransformationNode(id string) (models.Transformation, error) {
	if m.getTransformationErr != nil {
		return nil, m.getTransformationErr
	}
	if node, ok := m.transformationByID[id]; ok {
		return node, nil
	}
	return nil, errNodeNotFound
}

func (m *mockDAGReader) GetExternalNode(id string) (models.External, error) {
	if m.getExternalErr != nil {
		return nil, m.getExternalErr
	}
	if node, ok := m.externalByID[id]; ok {
		return node, nil
	}
	return nil, errNodeNotFound
}

func (m *mockDAGReader) GetNode(id string) (models.Node, error) {
	if m.getNodeErr != nil {
		return models.Node{}, m.getNodeErr
	}
	if node, ok := m.nodeByID[id]; ok {
		return node, nil
	}
	return models.Node{}, errNodeNotFound
}

func (m *mockDAGReader) GetDependencies(id string) []string {
	if deps, ok := m.dependencies[id]; ok {
		return deps
	}
	return []string{}
}

func (m *mockDAGReader) GetDependents(id string) []string {
	if deps, ok := m.dependents[id]; ok {
		return deps
	}
	return []string{}
}

func (m *mockDAGReader) GetAllDependencies(id string) []string {
	if deps, ok := m.allDependencies[id]; ok {
		return deps
	}
	return []string{}
}

func (m *mockDAGReader) GetAllDependents(id string) []string {
	if deps, ok := m.allDependents[id]; ok {
		return deps
	}
	return []string{}
}

func (m *mockDAGReader) IsPathBetween(from, to string) bool {
	if paths, ok := m.pathBetween[from]; ok {
		return paths[to]
	}
	return false
}

// mockModelsService implements models.Service for testing
type mockModelsService struct {
	dag models.DAGReader
}

func (m *mockModelsService) Start() error {
	return nil
}

func (m *mockModelsService) Stop() error {
	return nil
}

func (m *mockModelsService) GetDAG() models.DAGReader {
	return m.dag
}

func (m *mockModelsService) RenderTransformation(_ models.Transformation, _, _ uint64, _ time.Time) (string, error) {
	return "", nil
}

func (m *mockModelsService) RenderExternal(_ models.External, _ map[string]interface{}) (string, error) {
	return "", nil
}

func (m *mockModelsService) GetTransformationEnvironmentVariables(_ models.Transformation, _, _ uint64, _ time.Time) (*[]string, error) {
	return nil, nil
}

// mockAdminService implements admin.Service for testing
type mockAdminService struct{}

func (m *mockAdminService) GetLastProcessedEndPosition(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}

func (m *mockAdminService) GetNextUnprocessedPosition(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}

func (m *mockAdminService) GetLastProcessedPosition(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}

func (m *mockAdminService) GetFirstPosition(_ context.Context, _ string) (uint64, error) {
	return 0, nil
}

func (m *mockAdminService) RecordCompletion(_ context.Context, _ string, _, _ uint64) error {
	return nil
}

func (m *mockAdminService) RecordScheduledCompletion(_ context.Context, _ string, _ time.Time) error {
	return nil
}

func (m *mockAdminService) GetLastScheduledExecution(_ context.Context, _ string) (*time.Time, error) {
	return nil, nil
}

func (m *mockAdminService) GetCoverage(_ context.Context, _ string, _, _ uint64) (bool, error) {
	return false, nil
}

func (m *mockAdminService) GetProcessedRanges(_ context.Context, _ string) ([]admin.ProcessedRange, error) {
	return []admin.ProcessedRange{}, nil
}

func (m *mockAdminService) FindGaps(_ context.Context, _ string, _, _, _ uint64) ([]admin.GapInfo, error) {
	return nil, nil
}

func (m *mockAdminService) ConsolidateHistoricalData(_ context.Context, _ string) (int, error) {
	return 0, nil
}

func (m *mockAdminService) GetExternalBounds(_ context.Context, _ string) (*admin.BoundsCache, error) {
	return nil, nil
}

func (m *mockAdminService) SetExternalBounds(_ context.Context, _ *admin.BoundsCache) error {
	return nil
}

func (m *mockAdminService) GetIncrementalAdminDatabase() string {
	return "admin"
}

func (m *mockAdminService) GetIncrementalAdminTable() string {
	return "cbt_incremental"
}

func (m *mockAdminService) GetScheduledAdminDatabase() string {
	return "admin"
}

func (m *mockAdminService) GetScheduledAdminTable() string {
	return "cbt_scheduled"
}

// mockTransformation implements models.Transformation for testing
type mockTransformation struct {
	id       string
	database string
	table    string
	typ      transformation.Type
	env      map[string]string
	query    string
}

func (m *mockTransformation) GetID() string {
	return m.id
}

func (m *mockTransformation) GetType() string {
	return string(m.typ)
}

func (m *mockTransformation) GetConfig() *transformation.Config {
	return &transformation.Config{
		Database: m.database,
		Table:    m.table,
		Type:     m.typ,
		Env:      m.env,
	}
}

func (m *mockTransformation) GetHandler() transformation.Handler {
	return nil
}

func (m *mockTransformation) GetValue() string {
	if m.query != "" {
		return m.query
	}
	return "SELECT 1"
}

func (m *mockTransformation) SetDefaultDatabase(defaultDB string) {
	if m.database == "" {
		m.database = defaultDB
	}
}

// mockExternal implements models.External for testing
type mockExternal struct {
	id       string
	database string
	table    string
}

func (m *mockExternal) GetID() string {
	return m.id
}

func (m *mockExternal) GetType() string {
	return "sql"
}

func (m *mockExternal) GetConfig() external.Config {
	return external.Config{
		Database: m.database,
		Table:    m.table,
	}
}

func (m *mockExternal) GetConfigMutable() *external.Config {
	return &external.Config{
		Database: m.database,
		Table:    m.table,
	}
}

func (m *mockExternal) GetValue() string {
	return ""
}

func (m *mockExternal) SetDefaultDatabase(defaultDB string) {
	if m.database == "" {
		m.database = defaultDB
	}
}

func TestNewServer(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	mockService := &mockModelsService{
		dag: &mockDAGReader{},
	}
	mockAdmin := &mockAdminService{}

	server := NewServer(mockService, mockAdmin, log)

	assert.NotNil(t, server)
	assert.NotNil(t, server.modelsService)
	assert.NotNil(t, server.adminService)
	assert.NotNil(t, server.log)

	// Verify interface compliance
	var _ generated.ServerInterface = server
}

func TestBuildTransformationModel(t *testing.T) {
	mockDAG := &mockDAGReader{
		dependencies: map[string][]string{
			"test.model": {"dep1.table"},
		},
		dependents: map[string][]string{
			"test.model": {"dependent1.table"},
		},
	}

	mockTrans := &mockTransformation{
		id:       "test.model",
		database: "test",
		table:    "model",
		typ:      transformation.TypeIncremental,
		env:      map[string]string{"KEY": "value"},
	}

	model := buildTransformationModel("test.model", mockTrans, mockDAG)

	assert.Equal(t, "test.model", model.Id)
	assert.Equal(t, "test", model.Database)
	assert.Equal(t, "model", model.Table)
	assert.Equal(t, generated.TransformationModelTypeIncremental, model.Type)
	assert.NotNil(t, model.DependsOn)
	assert.Equal(t, []string{"dep1.table"}, *model.DependsOn)
	// Content and other fields populated from handler
	assert.NotEmpty(t, model.Content)
	assert.Equal(t, generated.TransformationModelContentTypeSql, model.ContentType)
}

func TestBuildExternalModel(t *testing.T) {
	mockDAG := &mockDAGReader{
		dependents: map[string][]string{
			"external.table": {"dependent1.table"},
		},
	}

	mockExt := &mockExternal{
		id:       "external.table",
		database: "external",
		table:    "table",
	}

	model := buildExternalModel("external.table", mockExt, mockDAG)

	assert.Equal(t, "external.table", model.Id)
	assert.Equal(t, "external", model.Database)
	assert.Equal(t, "table", model.Table)
	// Cache and Lag are optional fields populated from domain model when available
	assert.Nil(t, model.Cache)
	assert.Nil(t, model.Lag)
}
