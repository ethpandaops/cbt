package dependencies

import (
	"testing"

	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDependencyManager_BuildGraph(t *testing.T) {
	tests := []struct {
		name          string
		testModels    []models.ModelConfig
		expectedError error
	}{
		{
			name: "simple linear dependency chain",
			testModels: []models.ModelConfig{
				{
					Database: "db", Table: "table1",
					Dependencies: []string{},
				},
				{
					Database: "db", Table: "table2",
					Dependencies: []string{"db.table1"},
				},
				{
					Database: "db", Table: "table3",
					Dependencies: []string{"db.table2"},
				},
			},
			expectedError: nil,
		},
		{
			name: "multiple dependencies",
			testModels: []models.ModelConfig{
				{
					Database: "db", Table: "source1",
					Dependencies: []string{},
				},
				{
					Database: "db", Table: "source2",
					Dependencies: []string{},
				},
				{
					Database: "db", Table: "derived",
					Dependencies: []string{"db.source1", "db.source2"},
				},
			},
			expectedError: nil,
		},
		{
			name: "diamond dependency pattern",
			testModels: []models.ModelConfig{
				{
					Database: "db", Table: "root",
					Dependencies: []string{},
				},
				{
					Database: "db", Table: "left",
					Dependencies: []string{"db.root"},
				},
				{
					Database: "db", Table: "right",
					Dependencies: []string{"db.root"},
				},
				{
					Database: "db", Table: "final",
					Dependencies: []string{"db.left", "db.right"},
				},
			},
			expectedError: nil,
		},
		{
			name: "cyclic dependency should fail",
			testModels: []models.ModelConfig{
				{
					Database: "db", Table: "table1",
					Dependencies: []string{"db.table2"},
				},
				{
					Database: "db", Table: "table2",
					Dependencies: []string{"db.table1"},
				},
			},
			expectedError: nil, // Will get a cycle error from DAG
		},
		{
			name: "non-existent dependency",
			testModels: []models.ModelConfig{
				{
					Database: "db", Table: "table1",
					Dependencies: []string{"db.nonexistent"},
				},
			},
			expectedError: ErrNonExistentDependency,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dm := NewDependencyGraph()
			err := dm.BuildGraph(tt.testModels)

			switch {
			case tt.expectedError != nil:
				assert.ErrorIs(t, err, tt.expectedError)
			case tt.name == "cyclic dependency should fail":
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "invalid dependency")
			default:
				require.NoError(t, err)
			}
		})
	}
}

func TestDependencyManager_GetDependents(t *testing.T) {
	dm := NewDependencyGraph()

	testModels := []models.ModelConfig{
		{Database: "db", Table: "base"},
		{Database: "db", Table: "derived1", Dependencies: []string{"db.base"}},
		{Database: "db", Table: "derived2", Dependencies: []string{"db.base"}},
		{Database: "db", Table: "final", Dependencies: []string{"db.derived1", "db.derived2"}},
	}

	err := dm.BuildGraph(testModels)
	require.NoError(t, err)

	tests := []struct {
		name               string
		modelID            string
		expectedDependents []string
	}{
		{
			name:               "base has two direct dependents",
			modelID:            "db.base",
			expectedDependents: []string{"db.derived1", "db.derived2"},
		},
		{
			name:               "derived1 has one dependent",
			modelID:            "db.derived1",
			expectedDependents: []string{"db.final"},
		},
		{
			name:               "final has no dependents",
			modelID:            "db.final",
			expectedDependents: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dependents := dm.GetDependents(tt.modelID)
			assert.ElementsMatch(t, tt.expectedDependents, dependents)
		})
	}
}

func TestDependencyManager_GetDependencies(t *testing.T) {
	dm := NewDependencyGraph()

	testModels := []models.ModelConfig{
		{Database: "db", Table: "base"},
		{Database: "db", Table: "derived1", Dependencies: []string{"db.base"}},
		{Database: "db", Table: "derived2", Dependencies: []string{"db.base"}},
		{Database: "db", Table: "final", Dependencies: []string{"db.derived1", "db.derived2"}},
	}

	err := dm.BuildGraph(testModels)
	require.NoError(t, err)

	tests := []struct {
		name                 string
		modelID              string
		expectedDependencies []string
	}{
		{
			name:                 "base has no dependencies",
			modelID:              "db.base",
			expectedDependencies: []string{},
		},
		{
			name:                 "derived1 depends on base",
			modelID:              "db.derived1",
			expectedDependencies: []string{"db.base"},
		},
		{
			name:                 "final depends on two models",
			modelID:              "db.final",
			expectedDependencies: []string{"db.derived1", "db.derived2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dependencies := dm.GetDependencies(tt.modelID)
			assert.ElementsMatch(t, tt.expectedDependencies, dependencies)
		})
	}
}

func TestDependencyManager_GetAllDependents(t *testing.T) {
	dm := NewDependencyGraph()

	testModels := []models.ModelConfig{
		{Database: "db", Table: "base"},
		{Database: "db", Table: "level1", Dependencies: []string{"db.base"}},
		{Database: "db", Table: "level2", Dependencies: []string{"db.level1"}},
		{Database: "db", Table: "level3", Dependencies: []string{"db.level2"}},
	}

	err := dm.BuildGraph(testModels)
	require.NoError(t, err)

	allDependents := dm.GetAllDependents("db.base")
	assert.ElementsMatch(t, []string{"db.level1", "db.level2", "db.level3"}, allDependents)
}

func TestDependencyManager_GetAllDependencies(t *testing.T) {
	dm := NewDependencyGraph()

	testModels := []models.ModelConfig{
		{Database: "db", Table: "base"},
		{Database: "db", Table: "level1", Dependencies: []string{"db.base"}},
		{Database: "db", Table: "level2", Dependencies: []string{"db.level1"}},
		{Database: "db", Table: "level3", Dependencies: []string{"db.level2"}},
	}

	err := dm.BuildGraph(testModels)
	require.NoError(t, err)

	allDependencies := dm.GetAllDependencies("db.level3")
	assert.ElementsMatch(t, []string{"db.base", "db.level1", "db.level2"}, allDependencies)
}

func TestDependencyManager_IsPathBetween(t *testing.T) {
	dm := NewDependencyGraph()

	testModels := []models.ModelConfig{
		{Database: "db", Table: "a"},
		{Database: "db", Table: "b", Dependencies: []string{"db.a"}},
		{Database: "db", Table: "c", Dependencies: []string{"db.b"}},
		{Database: "db", Table: "d"},
	}

	err := dm.BuildGraph(testModels)
	require.NoError(t, err)

	tests := []struct {
		name     string
		from     string
		to       string
		expected bool
	}{
		{
			name:     "direct path exists",
			from:     "db.a",
			to:       "db.b",
			expected: true,
		},
		{
			name:     "indirect path exists",
			from:     "db.a",
			to:       "db.c",
			expected: true,
		},
		{
			name:     "no path between disconnected nodes",
			from:     "db.a",
			to:       "db.d",
			expected: false,
		},
		{
			name:     "reverse path does not exist",
			from:     "db.c",
			to:       "db.a",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dm.IsPathBetween(tt.from, tt.to)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDependencyManager_GetModelConfig(t *testing.T) {
	dm := NewDependencyGraph()

	testModels := []models.ModelConfig{
		{
			Database:  "db",
			Table:     "table1",
			Partition: "timestamp",
			External:  true,
		},
		{
			Database:     "db",
			Table:        "table2",
			Partition:    "slot",
			Interval:     3600,
			Schedule:     "@every 1h",
			Dependencies: []string{"db.table1"},
		},
	}

	err := dm.BuildGraph(testModels)
	require.NoError(t, err)

	t.Run("existing model", func(t *testing.T) {
		config, exists := dm.GetModelConfig("db.table1")
		assert.True(t, exists)
		assert.Equal(t, "db", config.Database)
		assert.Equal(t, "table1", config.Table)
		assert.True(t, config.External)
	})

	t.Run("non-existing model", func(t *testing.T) {
		_, exists := dm.GetModelConfig("db.nonexistent")
		assert.False(t, exists)
	})
}

func TestDependencyManager_ValidateNoCycles(t *testing.T) {
	t.Run("valid DAG", func(t *testing.T) {
		dm := NewDependencyGraph()
		testModels := []models.ModelConfig{
			{Database: "db", Table: "a"},
			{Database: "db", Table: "b", Dependencies: []string{"db.a"}},
			{Database: "db", Table: "c", Dependencies: []string{"db.b"}},
		}

		err := dm.BuildGraph(testModels)
		require.NoError(t, err)

		err = dm.ValidateNoCycles()
		assert.NoError(t, err)
	})

	t.Run("cycle detection happens during build", func(t *testing.T) {
		dm := NewDependencyGraph()
		testModels := []models.ModelConfig{
			{Database: "db", Table: "a", Dependencies: []string{"db.c"}},
			{Database: "db", Table: "b", Dependencies: []string{"db.a"}},
			{Database: "db", Table: "c", Dependencies: []string{"db.b"}},
		}

		err := dm.BuildGraph(testModels)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid dependency")
	})
}

func TestDependencyManager_GetExternalModels(t *testing.T) {
	dm := NewDependencyGraph()

	testModels := []models.ModelConfig{
		{Database: "db", Table: "external1", External: true},
		{Database: "db", Table: "transform1", External: false},
		{Database: "db", Table: "external2", External: true},
		{Database: "db", Table: "transform2", External: false},
	}

	err := dm.BuildGraph(testModels)
	require.NoError(t, err)

	externals := dm.GetExternalModels()
	assert.Equal(t, []string{"db.external1", "db.external2"}, externals)
}

func TestDependencyManager_GetTransformationModels(t *testing.T) {
	dm := NewDependencyGraph()

	testModels := []models.ModelConfig{
		{Database: "db", Table: "external1", External: true},
		{Database: "db", Table: "transform1", External: false},
		{Database: "db", Table: "external2", External: true},
		{Database: "db", Table: "transform2", External: false},
	}

	err := dm.BuildGraph(testModels)
	require.NoError(t, err)

	transformations := dm.GetTransformationModels()
	assert.Equal(t, []string{"db.transform1", "db.transform2"}, transformations)
}

func TestDependencyManager_GetTopologicalOrder(t *testing.T) {
	dm := NewDependencyGraph()

	testModels := []models.ModelConfig{
		{Database: "db", Table: "c", Dependencies: []string{"db.b"}},
		{Database: "db", Table: "b", Dependencies: []string{"db.a"}},
		{Database: "db", Table: "a"},
		{Database: "db", Table: "d"},
	}

	err := dm.BuildGraph(testModels)
	require.NoError(t, err)

	order, err := dm.GetTopologicalOrder()
	require.NoError(t, err)

	// Should contain all models
	assert.Len(t, order, 4)
	assert.Contains(t, order, "db.a")
	assert.Contains(t, order, "db.b")
	assert.Contains(t, order, "db.c")
	assert.Contains(t, order, "db.d")
}
