package dependencies

import (
	"testing"

	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetDAGInfo_PrecomputedDependents(t *testing.T) {
	dm := NewDependencyGraph()

	testModels := []models.ModelConfig{
		{Database: "db", Table: "base"},
		{Database: "db", Table: "derived1", Dependencies: []string{"db.base"}},
		{Database: "db", Table: "derived2", Dependencies: []string{"db.base"}},
		{Database: "db", Table: "final", Dependencies: []string{"db.derived1", "db.derived2"}},
		{Database: "db", Table: "orphan"},
	}

	err := dm.BuildGraph(testModels)
	require.NoError(t, err)

	dagInfo := dm.GetDAGInfo()

	// Verify precomputed dependents
	assert.NotNil(t, dagInfo.Dependents)
	assert.Len(t, dagInfo.Dependents, 5)

	// Check specific dependents
	assert.ElementsMatch(t, []string{"db.derived1", "db.derived2"}, dagInfo.Dependents["db.base"])
	assert.ElementsMatch(t, []string{"db.final"}, dagInfo.Dependents["db.derived1"])
	assert.ElementsMatch(t, []string{"db.final"}, dagInfo.Dependents["db.derived2"])
	assert.Empty(t, dagInfo.Dependents["db.final"])
	assert.Empty(t, dagInfo.Dependents["db.orphan"])

	// Verify levels are calculated correctly
	assert.Equal(t, 2, len(dagInfo.Levels[0])) // Level 0 should have base and orphan
	assert.Contains(t, dagInfo.Levels[0], "db.base")
	assert.Contains(t, dagInfo.Levels[0], "db.orphan")
	assert.Equal(t, 2, len(dagInfo.Levels[1])) // Level 1 should have derived1 and derived2
	assert.Contains(t, dagInfo.Levels[1], "db.derived1")
	assert.Contains(t, dagInfo.Levels[1], "db.derived2")
	assert.Equal(t, 1, len(dagInfo.Levels[2])) // Level 2 should have final
	assert.Contains(t, dagInfo.Levels[2], "db.final")

	// Verify root nodes
	assert.ElementsMatch(t, []string{"db.base", "db.orphan"}, dagInfo.RootNodes)

	// Verify total models
	assert.Equal(t, 5, dagInfo.TotalModels)
}
