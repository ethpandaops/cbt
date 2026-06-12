package transformation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// decodeStrictTarget is a minimal struct used to exercise DecodeStrict.
type decodeStrictTarget struct {
	Name  string `yaml:"name"`
	Value int    `yaml:"value"`
}

func TestDecodeStrict(t *testing.T) {
	tests := []struct {
		name      string
		data      string
		wantErr   bool
		errMsg    string
		wantName  string
		wantValue int
	}{
		{
			name:      "valid config",
			data:      "name: hello\nvalue: 42\n",
			wantErr:   false,
			wantName:  "hello",
			wantValue: 42,
		},
		{
			name:    "unknown field rejected",
			data:    "name: hello\nunknown: oops\n",
			wantErr: true,
			errMsg:  "failed to parse config",
		},
		{
			name:    "malformed yaml",
			data:    "name: [unterminated\n",
			wantErr: true,
			errMsg:  "failed to parse config",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := DecodeStrict[decodeStrictTarget]([]byte(tt.data))

			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
				assert.Nil(t, cfg)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, cfg)
			assert.Equal(t, tt.wantName, cfg.Name)
			assert.Equal(t, tt.wantValue, cfg.Value)
		})
	}
}

func TestCopyTags(t *testing.T) {
	tests := []struct {
		name string
		tags []string
	}{
		{name: "nil slice returns nil", tags: nil},
		{name: "empty slice", tags: []string{}},
		{name: "populated slice", tags: []string{"a", "b", "c"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cp := CopyTags(tt.tags)

			if tt.tags == nil {
				assert.Nil(t, cp)

				return
			}

			assert.Equal(t, tt.tags, cp)

			// Verify the copy is independent of the source.
			if len(tt.tags) > 0 {
				cp[0] = "mutated"
				assert.NotEqual(t, cp[0], tt.tags[0])
			}
		})
	}
}

// newTestDependencyBase wires a DependencyBase to a small set of closures backed
// by local state so the shared accessor behavior can be exercised directly.
func newTestDependencyBase(
	adminTable AdminTable,
	database, table string,
	tags []string,
	deps []Dependency,
	original *[]Dependency,
) DependencyBase {
	return NewDependencyBase(
		adminTable,
		func() (string, string) { return database, table },
		func() []string { return tags },
		func() []Dependency { return deps },
		func() []Dependency { return *original },
		func(d []Dependency) { *original = d },
	)
}

func TestDependencyBase_GettersAndID(t *testing.T) {
	var original []Dependency

	adminTable := AdminTable{Database: "admin", Table: "cbt"}
	tags := []string{"tag1", "tag2"}
	deps := []Dependency{
		{SingleDep: "db1.table1"},
		{IsGroup: true, GroupDeps: []string{"db2.table2", "db2.table3"}},
	}

	base := newTestDependencyBase(adminTable, "test_db", "test_table", tags, deps, &original)

	assert.Equal(t, "test_db.test_table", base.GetID())
	assert.Equal(t, tags, base.GetTags())
	assert.Equal(t, deps, base.GetDependencies())
	assert.Equal(t, adminTable, base.GetAdminTable())

	flat := base.GetFlattenedDependencies()
	assert.Equal(t, []string{"db1.table1", "db2.table2", "db2.table3"}, flat)

	// Original is empty until SubstituteDependencyPlaceholders runs.
	assert.Nil(t, base.GetOriginalDependencies())
}

func TestDependencyBase_SubstituteDependencyPlaceholders(t *testing.T) {
	var original []Dependency

	deps := []Dependency{
		{SingleDep: "{{external}}.source"},
		{IsGroup: true, GroupDeps: []string{"{{transformation}}.derived", "static.table"}},
	}

	base := newTestDependencyBase(AdminTable{}, "db", "table", nil, deps, &original)

	base.SubstituteDependencyPlaceholders("ext_db", "trans_db")

	// The original (pre-substitution) snapshot is stored via setOriginal.
	got := base.GetOriginalDependencies()
	require.Len(t, got, 2)
	assert.Equal(t, "{{external}}.source", got[0].SingleDep)
	assert.Equal(t, []string{"{{transformation}}.derived", "static.table"}, got[1].GroupDeps)

	// The live dependencies were substituted in place.
	live := base.GetDependencies()
	assert.Equal(t, "ext_db.source", live[0].SingleDep)
	assert.Equal(t, []string{"trans_db.derived", "static.table"}, live[1].GroupDeps)
}
