package transformation

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// withTestRegistry swaps the global registry for an isolated one for the
// duration of the test, registering a factory for each provided type. The
// factory records the data and admin table it received so behavior can be
// asserted. It restores the previous registry on cleanup.
func withTestRegistry(t *testing.T, types ...Type) {
	t.Helper()

	testRegistry := &Registry{
		factories: make(map[Type]HandlerFactory, len(types)),
	}

	original := globalRegistry
	globalRegistry = testRegistry
	t.Cleanup(func() { globalRegistry = original })

	for _, typ := range types {
		RegisterHandler(typ, func(data []byte, adminTable AdminTable) (Handler, error) {
			return &mockHandler{
				typeValue:   typ,
				configValue: string(data),
				adminTable:  adminTable,
			}, nil
		})
	}
}

func TestNewSQL(t *testing.T) {
	withTestRegistry(t, TypeIncremental)

	tests := []struct {
		name        string
		content     string
		wantErr     bool
		errIs       error
		errMsg      string
		wantType    Type
		wantContent string
	}{
		{
			name:        "valid frontmatter with content",
			content:     "---\ntype: incremental\ndatabase: test_db\ntable: test_table\n---\nSELECT * FROM source",
			wantErr:     false,
			wantType:    TypeIncremental,
			wantContent: "SELECT * FROM source",
		},
		{
			name:    "missing frontmatter separator",
			content: "type: incremental\ndatabase: test_db",
			wantErr: true,
			errIs:   ErrInvalidFrontmatter,
		},
		{
			name:    "malformed base config yaml",
			content: "---\ntype: [unterminated\n---\nSELECT 1",
			wantErr: true,
			errMsg:  "failed to parse base config",
		},
		{
			name:    "unregistered handler type",
			content: "---\ntype: bogus\ndatabase: test_db\ntable: test_table\n---\nSELECT 1",
			wantErr: true,
			errMsg:  "failed to create handler for type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			model, err := NewSQL([]byte(tt.content))

			if tt.wantErr {
				require.Error(t, err)
				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
				assert.Nil(t, model)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, model)
			assert.Equal(t, tt.wantType, model.BaseConfig.Type)
			assert.Equal(t, tt.wantContent, model.Content)
			assert.NotNil(t, model.Handler)
		})
	}
}

func TestSQL_Validate(t *testing.T) {
	validHandler := &mockHandler{typeValue: TypeIncremental}

	tests := []struct {
		name    string
		model   *SQL
		wantErr bool
		errIs   error
	}{
		{
			name: "valid model",
			model: &SQL{
				BaseConfig: Config{Type: TypeIncremental, Database: "db", Table: "tbl"},
				Handler:    validHandler,
				Content:    "SELECT 1",
			},
			wantErr: false,
		},
		{
			name: "missing content",
			model: &SQL{
				BaseConfig: Config{Type: TypeIncremental, Database: "db", Table: "tbl"},
				Content:    "",
			},
			wantErr: true,
			errIs:   ErrSQLContentRequired,
		},
		{
			name: "invalid base config",
			model: &SQL{
				BaseConfig: Config{Type: TypeIncremental, Table: "tbl"},
				Content:    "SELECT 1",
			},
			wantErr: true,
			errIs:   ErrDatabaseRequired,
		},
		{
			name: "handler validation fails",
			model: &SQL{
				BaseConfig: Config{Type: TypeIncremental, Database: "db", Table: "tbl"},
				Handler:    &mockHandler{validateErr: errMockValidate},
				Content:    "SELECT 1",
			},
			wantErr: true,
			errIs:   errMockValidate,
		},
		{
			name: "nil handler passes after base validation",
			model: &SQL{
				BaseConfig: Config{Type: TypeIncremental, Database: "db", Table: "tbl"},
				Handler:    nil,
				Content:    "SELECT 1",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.model.Validate()

			if tt.wantErr {
				require.Error(t, err)
				if tt.errIs != nil {
					require.ErrorIs(t, err, tt.errIs)
				}

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestSQL_Accessors(t *testing.T) {
	handler := &mockHandler{typeValue: TypeIncremental}
	model := &SQL{
		BaseConfig: Config{Type: TypeIncremental, Database: "test_db", Table: "test_table"},
		Handler:    handler,
		Content:    "SELECT * FROM source",
	}

	assert.Equal(t, TypeSQL, model.GetType())
	assert.Equal(t, &model.BaseConfig, model.GetConfig())
	assert.Equal(t, "SELECT * FROM source", model.GetValue())
	assert.Equal(t, "test_db.test_table", model.GetID())
	assert.Equal(t, Handler(handler), model.GetHandler())
}

func TestSQL_SetDefaultDatabase(t *testing.T) {
	tests := []struct {
		name       string
		initialDB  string
		defaultDB  string
		expectedDB string
	}{
		{name: "apply default when empty", initialDB: "", defaultDB: "default_db", expectedDB: "default_db"},
		{name: "keep existing", initialDB: "existing_db", defaultDB: "default_db", expectedDB: "existing_db"},
		{name: "empty default no change", initialDB: "", defaultDB: "", expectedDB: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			model := &SQL{
				BaseConfig: Config{Type: TypeIncremental, Database: tt.initialDB, Table: "tbl"},
				Content:    "SELECT 1",
			}

			model.SetDefaultDatabase(tt.defaultDB)
			assert.Equal(t, tt.expectedDB, model.BaseConfig.Database)
		})
	}
}

var errMockValidate = errors.New("mock validate error")
