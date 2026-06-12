package transformation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// depHolder embeds a Dependency so the dependency node itself (rather than the
// document) drives UnmarshalYAML, allowing scalar/sequence/mapping cases.
type depHolder struct {
	Dep Dependency `yaml:"dep"`
}

func TestDependencyUnmarshalYAML(t *testing.T) {
	tests := []struct {
		name      string
		yamlData  string
		wantErr   bool
		errIs     error
		wantGroup bool
		wantDeps  []string
		wantOne   string
	}{
		{
			name:      "single string dependency",
			yamlData:  "dep: external.table1\n",
			wantGroup: false,
			wantOne:   "external.table1",
		},
		{
			name:      "array of string dependencies",
			yamlData:  "dep:\n  - external.table1\n  - external.table2\n",
			wantGroup: true,
			wantDeps:  []string{"external.table1", "external.table2"},
		},
		{
			name:     "empty array rejected",
			yamlData: "dep: []\n",
			wantErr:  true,
			errIs:    ErrEmptyDependencyGroup,
		},
		{
			name:     "non-string scalar rejected",
			yamlData: "dep: 42\n",
			wantErr:  true,
			errIs:    ErrInvalidDependencyType,
		},
		{
			name:     "array with non-string scalar item rejected",
			yamlData: "dep:\n  - external.table1\n  - 42\n",
			wantErr:  true,
			errIs:    ErrInvalidDependencyArrayItem,
		},
		{
			name:     "array with non-scalar item rejected",
			yamlData: "dep:\n  - [nested]\n",
			wantErr:  true,
			errIs:    ErrInvalidDependencyArrayItem,
		},
		{
			name:     "mapping node rejected",
			yamlData: "dep:\n  key: value\n",
			wantErr:  true,
			errIs:    ErrInvalidDependencyType,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var holder depHolder
			err := yaml.Unmarshal([]byte(tt.yamlData), &holder)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantGroup, holder.Dep.IsGroup)
			if tt.wantGroup {
				assert.Equal(t, tt.wantDeps, holder.Dep.GroupDeps)
			} else {
				assert.Equal(t, tt.wantOne, holder.Dep.SingleDep)
			}
		})
	}
}

func TestDependencyUnmarshalYAML_UnknownNodeKind(t *testing.T) {
	// A node with an unrecognized Kind (0) exercises the defensive default
	// branch, which the YAML decoder never produces on its own.
	var dep Dependency
	node := &yaml.Node{Kind: yaml.Kind(0)}

	err := dep.UnmarshalYAML(node)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidDependencyType)
}

func TestDependencyGetAllDependencies(t *testing.T) {
	tests := []struct {
		name     string
		dep      Dependency
		expected []string
	}{
		{
			name:     "single dependency",
			dep:      Dependency{IsGroup: false, SingleDep: "db.table"},
			expected: []string{"db.table"},
		},
		{
			name:     "group dependency",
			dep:      Dependency{IsGroup: true, GroupDeps: []string{"db.a", "db.b"}},
			expected: []string{"db.a", "db.b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.dep.GetAllDependencies())
		})
	}
}

func TestDeepCopyDependencies(t *testing.T) {
	tests := []struct {
		name     string
		input    []Dependency
		expected []Dependency
	}{
		{
			name:     "empty slice",
			input:    []Dependency{},
			expected: []Dependency{},
		},
		{
			name: "single string dependency",
			input: []Dependency{
				{IsGroup: false, SingleDep: "external.table1"},
			},
			expected: []Dependency{
				{IsGroup: false, SingleDep: "external.table1"},
			},
		},
		{
			name: "single group dependency",
			input: []Dependency{
				{IsGroup: true, GroupDeps: []string{"external.table1", "external.table2"}},
			},
			expected: []Dependency{
				{IsGroup: true, GroupDeps: []string{"external.table1", "external.table2"}},
			},
		},
		{
			name: "mixed dependencies",
			input: []Dependency{
				{IsGroup: false, SingleDep: "external.table1"},
				{IsGroup: true, GroupDeps: []string{"external.table2", "external.table3"}},
				{IsGroup: false, SingleDep: "transformation.table4"},
			},
			expected: []Dependency{
				{IsGroup: false, SingleDep: "external.table1"},
				{IsGroup: true, GroupDeps: []string{"external.table2", "external.table3"}},
				{IsGroup: false, SingleDep: "transformation.table4"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DeepCopyDependencies(tt.input)

			// Verify values are equal
			assert.Equal(t, tt.expected, result)

			// Verify it's a true deep copy (modifying original doesn't affect copy)
			if len(tt.input) > 0 {
				if tt.input[0].IsGroup && len(tt.input[0].GroupDeps) > 0 {
					tt.input[0].GroupDeps[0] = "modified"
					assert.NotEqual(t, tt.input[0].GroupDeps[0], result[0].GroupDeps[0])
				}
			}
		})
	}
}

func TestSubstituteDependencyPlaceholders(t *testing.T) {
	tests := []struct {
		name             string
		deps             []Dependency
		externalDB       string
		transformationDB string
		wantOriginal     []Dependency // What the returned original should look like
		wantModified     []Dependency // What the input should look like after modification
	}{
		{
			name:             "empty slice",
			deps:             []Dependency{},
			externalDB:       "ext_db",
			transformationDB: "trans_db",
			wantOriginal:     []Dependency{},
			wantModified:     []Dependency{},
		},
		{
			name: "substitute external placeholder in single dep",
			deps: []Dependency{
				{IsGroup: false, SingleDep: "{{external}}.table1"},
			},
			externalDB:       "ext_db",
			transformationDB: "trans_db",
			wantOriginal: []Dependency{
				{IsGroup: false, SingleDep: "{{external}}.table1"},
			},
			wantModified: []Dependency{
				{IsGroup: false, SingleDep: "ext_db.table1"},
			},
		},
		{
			name: "substitute transformation placeholder in single dep",
			deps: []Dependency{
				{IsGroup: false, SingleDep: "{{transformation}}.table1"},
			},
			externalDB:       "ext_db",
			transformationDB: "trans_db",
			wantOriginal: []Dependency{
				{IsGroup: false, SingleDep: "{{transformation}}.table1"},
			},
			wantModified: []Dependency{
				{IsGroup: false, SingleDep: "trans_db.table1"},
			},
		},
		{
			name: "substitute placeholders in group deps",
			deps: []Dependency{
				{IsGroup: true, GroupDeps: []string{"{{external}}.table1", "{{transformation}}.table2"}},
			},
			externalDB:       "ext_db",
			transformationDB: "trans_db",
			wantOriginal: []Dependency{
				{IsGroup: true, GroupDeps: []string{"{{external}}.table1", "{{transformation}}.table2"}},
			},
			wantModified: []Dependency{
				{IsGroup: true, GroupDeps: []string{"ext_db.table1", "trans_db.table2"}},
			},
		},
		{
			name: "no placeholder - no change",
			deps: []Dependency{
				{IsGroup: false, SingleDep: "already.resolved"},
			},
			externalDB:       "ext_db",
			transformationDB: "trans_db",
			wantOriginal: []Dependency{
				{IsGroup: false, SingleDep: "already.resolved"},
			},
			wantModified: []Dependency{
				{IsGroup: false, SingleDep: "already.resolved"},
			},
		},
		{
			name: "empty db strings - no substitution",
			deps: []Dependency{
				{IsGroup: false, SingleDep: "{{external}}.table1"},
			},
			externalDB:       "",
			transformationDB: "",
			wantOriginal: []Dependency{
				{IsGroup: false, SingleDep: "{{external}}.table1"},
			},
			wantModified: []Dependency{
				{IsGroup: false, SingleDep: "{{external}}.table1"},
			},
		},
		{
			name: "mixed dependencies with placeholders",
			deps: []Dependency{
				{IsGroup: false, SingleDep: "{{external}}.source"},
				{IsGroup: true, GroupDeps: []string{"{{transformation}}.dep1", "{{external}}.dep2"}},
				{IsGroup: false, SingleDep: "static.table"},
			},
			externalDB:       "ext",
			transformationDB: "trans",
			wantOriginal: []Dependency{
				{IsGroup: false, SingleDep: "{{external}}.source"},
				{IsGroup: true, GroupDeps: []string{"{{transformation}}.dep1", "{{external}}.dep2"}},
				{IsGroup: false, SingleDep: "static.table"},
			},
			wantModified: []Dependency{
				{IsGroup: false, SingleDep: "ext.source"},
				{IsGroup: true, GroupDeps: []string{"trans.dep1", "ext.dep2"}},
				{IsGroup: false, SingleDep: "static.table"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call the function - it modifies deps in place and returns original
			originalDeps := SubstituteDependencyPlaceholders(tt.deps, tt.externalDB, tt.transformationDB)

			// Verify the returned original dependencies
			require.Len(t, originalDeps, len(tt.wantOriginal))
			assert.Equal(t, tt.wantOriginal, originalDeps)

			// Verify the input slice was modified correctly
			require.Len(t, tt.deps, len(tt.wantModified))
			assert.Equal(t, tt.wantModified, tt.deps)
		})
	}
}

func TestSubstituteDependencyPlaceholders_IndependentCopies(t *testing.T) {
	// Verify that modifying the returned original doesn't affect the substituted deps
	deps := []Dependency{
		{IsGroup: true, GroupDeps: []string{"{{external}}.table1", "{{external}}.table2"}},
	}

	originalDeps := SubstituteDependencyPlaceholders(deps, "ext_db", "trans_db")

	// Modify the original
	originalDeps[0].GroupDeps[0] = "modified"

	// Verify the substituted deps were not affected
	assert.Equal(t, "ext_db.table1", deps[0].GroupDeps[0])
	assert.Equal(t, "modified", originalDeps[0].GroupDeps[0])
}
