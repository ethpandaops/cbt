package cmd

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethpandaops/cbt/pkg/engine"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	errTest       = errors.New("boom")
	errCreateTest = errors.New("create failed")
	errStartTest  = errors.New("start failed")
	errStopTest   = errors.New("stop failed")
)

// fakeEngine is a test double for engineRunner.
type fakeEngine struct {
	startErr error
	stopErr  error
	started  bool
	stopped  bool
}

func (f *fakeEngine) Start(_ context.Context) error {
	f.started = true
	return f.startErr
}

func (f *fakeEngine) Stop() error {
	f.stopped = true
	return f.stopErr
}

// restoreSeams snapshots the package-level seams and restores them via t.Cleanup.
func restoreSeams(t *testing.T) {
	t.Helper()

	origExit := osExit
	origDefaults := setDefaults
	origNewEngine := newEngine
	origCfgFile := cfgFile

	t.Cleanup(func() {
		osExit = origExit
		setDefaults = origDefaults
		newEngine = origNewEngine
		cfgFile = origCfgFile
	})
}

func writeConfig(t *testing.T, contents string) string {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))

	return path
}

func TestExecute(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantExit bool
	}{
		{
			name:     "help_succeeds",
			args:     []string{"cbt", "--help"},
			wantExit: false,
		},
		{
			name:     "unknown_flag_triggers_exit",
			args:     []string{"cbt", "--definitely-not-a-flag"},
			wantExit: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			restoreSeams(t)

			var exitCalled bool

			osExit = func(int) { exitCalled = true }

			origArgs := os.Args
			t.Cleanup(func() { os.Args = origArgs })
			os.Args = tt.args

			Execute()

			assert.Equal(t, tt.wantExit, exitCalled)
		})
	}
}

func TestDefaultNewEngine(t *testing.T) {
	restoreSeams(t)

	// The default newEngine closure delegates to engine.NewService. Invoke it
	// with an invalid (empty) config so it returns an error without connecting
	// to any external services, while still exercising the closure body.
	runner, err := newEngine(logrus.New(), &engine.Config{})
	require.Error(t, err)
	assert.Nil(t, runner)
}

func TestLoadConfigFromFile(t *testing.T) {
	tests := []struct {
		name        string
		file        string
		contents    string
		setDefaults func(any) error
		wantErr     bool
		assertCfg   func(t *testing.T, cfg *engine.Config)
	}{
		{
			name:     "valid_file",
			contents: "logging: debug\n",
			wantErr:  false,
			assertCfg: func(t *testing.T, cfg *engine.Config) {
				t.Helper()
				assert.Equal(t, "debug", cfg.Logging)
			},
		},
		{
			name:    "missing_file",
			file:    filepath.Join(t.TempDir(), "does-not-exist.yaml"),
			wantErr: true,
		},
		{
			name:     "invalid_yaml",
			contents: "logging: [unterminated\n",
			wantErr:  true,
		},
		{
			name:     "empty_path_defaults_to_config_yaml",
			file:     "",
			wantErr:  true, // no ./config.yaml in test working dir
			contents: "",
		},
		{
			name:        "defaults_error",
			contents:    "logging: debug\n",
			setDefaults: func(any) error { return errTest },
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			restoreSeams(t)

			if tt.setDefaults != nil {
				setDefaults = tt.setDefaults
			}

			file := tt.file
			if tt.contents != "" {
				file = writeConfig(t, tt.contents)
			}

			cfg, err := loadConfigFromFile(file)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, cfg)

			if tt.assertCfg != nil {
				tt.assertCfg(t, cfg)
			}
		})
	}
}

func TestRunEngine(t *testing.T) {
	validCfg := "logging: debug\n"

	tests := []struct {
		name      string
		contents  string
		noFile    bool
		newEngine func(*logrus.Logger, *engine.Config) (engineRunner, error)
		cancelCtx bool
		wantErr   string
	}{
		{
			name:   "config_load_error",
			noFile: true,
			// cfgFile points at a missing file -> read error
			wantErr: "no such file",
		},
		{
			name:     "bad_log_level",
			contents: "logging: not-a-level\n",
			wantErr:  "not a valid logrus Level",
		},
		{
			name:     "new_engine_error",
			contents: validCfg,
			newEngine: func(*logrus.Logger, *engine.Config) (engineRunner, error) {
				return nil, errCreateTest
			},
			wantErr: "failed to create engine",
		},
		{
			name:     "start_error",
			contents: validCfg,
			newEngine: func(*logrus.Logger, *engine.Config) (engineRunner, error) {
				return &fakeEngine{startErr: errStartTest}, nil
			},
			wantErr: "failed to start engine",
		},
		{
			name:     "happy_path_stops_on_context_cancel",
			contents: validCfg,
			newEngine: func(*logrus.Logger, *engine.Config) (engineRunner, error) {
				return &fakeEngine{}, nil
			},
			cancelCtx: true,
		},
		{
			name:     "happy_path_stop_error",
			contents: validCfg,
			newEngine: func(*logrus.Logger, *engine.Config) (engineRunner, error) {
				return &fakeEngine{stopErr: errStopTest}, nil
			},
			cancelCtx: true,
			wantErr:   "stop failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			restoreSeams(t)

			if tt.newEngine != nil {
				newEngine = tt.newEngine
			}

			switch {
			case tt.noFile:
				cfgFile = filepath.Join(t.TempDir(), "missing.yaml")
			default:
				cfgFile = writeConfig(t, tt.contents)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			cmd := &cobra.Command{}
			cmd.SetContext(ctx)

			if tt.cancelCtx {
				// Cancel shortly after Start so <-ctx.Done() unblocks.
				go func() {
					time.Sleep(10 * time.Millisecond)
					cancel()
				}()
			}

			errCh := make(chan error, 1)
			go func() { errCh <- runEngine(cmd, nil) }()

			select {
			case err := <-errCh:
				if tt.wantErr != "" {
					require.Error(t, err)
					assert.Contains(t, err.Error(), tt.wantErr)
				} else {
					require.NoError(t, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("runEngine did not return in time")
			}
		})
	}
}
