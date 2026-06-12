package transformation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewExec(t *testing.T) {
	withTestRegistry(t, TypeIncremental)

	tests := []struct {
		name     string
		content  string
		wantErr  bool
		errMsg   string
		wantType Type
		wantExec string
	}{
		{
			name:     "valid exec config",
			content:  "type: incremental\ndatabase: test_db\ntable: test_table\nexec: /bin/run.sh\n",
			wantErr:  false,
			wantType: TypeIncremental,
			wantExec: "/bin/run.sh",
		},
		{
			name:    "malformed yaml",
			content: "type: [unterminated\n",
			wantErr: true,
			errMsg:  "failed to parse yaml",
		},
		{
			name:    "unregistered handler type",
			content: "type: bogus\ndatabase: test_db\ntable: test_table\nexec: /bin/run.sh\n",
			wantErr: true,
			errMsg:  "failed to create handler for type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			model, err := NewExec([]byte(tt.content))

			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
				assert.Nil(t, model)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, model)
			assert.Equal(t, tt.wantType, model.Type)
			assert.Equal(t, tt.wantExec, model.Exec)
			assert.NotNil(t, model.Handler)
		})
	}
}

func TestExec_Validate(t *testing.T) {
	validHandler := &mockHandler{typeValue: TypeIncremental}

	tests := []struct {
		name    string
		model   *Exec
		wantErr bool
		errIs   error
	}{
		{
			name: "valid model",
			model: &Exec{
				Config:  Config{Type: TypeIncremental, Database: "db", Table: "tbl"},
				Exec:    "/bin/run.sh",
				Handler: validHandler,
			},
			wantErr: false,
		},
		{
			name: "missing exec",
			model: &Exec{
				Config: Config{Type: TypeIncremental, Database: "db", Table: "tbl"},
				Exec:   "",
			},
			wantErr: true,
			errIs:   ErrExecRequired,
		},
		{
			name: "invalid base config",
			model: &Exec{
				Config: Config{Type: TypeIncremental, Table: "tbl"},
				Exec:   "/bin/run.sh",
			},
			wantErr: true,
			errIs:   ErrDatabaseRequired,
		},
		{
			name: "handler validation fails",
			model: &Exec{
				Config:  Config{Type: TypeIncremental, Database: "db", Table: "tbl"},
				Exec:    "/bin/run.sh",
				Handler: &mockHandler{validateErr: errMockValidate},
			},
			wantErr: true,
			errIs:   errMockValidate,
		},
		{
			name: "nil handler passes after base validation",
			model: &Exec{
				Config:  Config{Type: TypeIncremental, Database: "db", Table: "tbl"},
				Exec:    "/bin/run.sh",
				Handler: nil,
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

func TestExec_Accessors(t *testing.T) {
	handler := &mockHandler{typeValue: TypeIncremental}
	model := &Exec{
		Config:  Config{Type: TypeIncremental, Database: "test_db", Table: "test_table"},
		Exec:    "/bin/run.sh",
		Handler: handler,
	}

	assert.Equal(t, TypeExec, model.GetType())
	assert.Equal(t, &model.Config, model.GetConfig())
	assert.Equal(t, "/bin/run.sh", model.GetValue())
	assert.Equal(t, "test_db.test_table", model.GetID())
	assert.Equal(t, Handler(handler), model.GetHandler())
}

func TestExec_SetDefaultDatabase(t *testing.T) {
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
			model := &Exec{
				Config: Config{Type: TypeIncremental, Database: tt.initialDB, Table: "tbl"},
				Exec:   "/bin/run.sh",
			}

			model.SetDefaultDatabase(tt.defaultDB)
			assert.Equal(t, tt.expectedDB, model.Database)
		})
	}
}
