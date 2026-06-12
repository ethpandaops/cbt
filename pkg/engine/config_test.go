package engine

import (
	"errors"
	"testing"

	"github.com/ethpandaops/cbt/pkg/clickhouse"
	"github.com/ethpandaops/cbt/pkg/management"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/ethpandaops/cbt/pkg/scheduler"
	"github.com/ethpandaops/cbt/pkg/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errSeedModels = errors.New("seeded models validation error")

func TestIntervalTypesConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     IntervalTypesConfig
		wantErr error
	}{
		{
			name: "valid with all supported formats",
			cfg: IntervalTypesConfig{
				"slot": {
					{Name: "timestamp", Format: "datetime"},
					{Name: "day", Format: "date"},
					{Name: "clock", Format: "time"},
					{Name: "span", Format: "duration"},
					{Name: "ago", Format: "relative"},
				},
			},
			wantErr: nil,
		},
		{
			name:    "valid empty config",
			cfg:     IntervalTypesConfig{},
			wantErr: nil,
		},
		{
			name: "valid transformation without format",
			cfg: IntervalTypesConfig{
				"epoch": {{Name: "epoch", Expression: "math.floor(value / 12)"}},
			},
			wantErr: nil,
		},
		{
			name: "empty transformations slice",
			cfg: IntervalTypesConfig{
				"slot": {},
			},
			wantErr: ErrIntervalTypeEmpty,
		},
		{
			name: "missing transformation name",
			cfg: IntervalTypesConfig{
				"slot": {{Name: ""}},
			},
			wantErr: ErrTransformationNameRequired,
		},
		{
			name: "invalid format",
			cfg: IntervalTypesConfig{
				"slot": {{Name: "timestamp", Format: "bogus"}},
			},
			wantErr: ErrInvalidTransformationFormat,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cfg.Validate()
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestFrontendConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     FrontendConfig
		wantErr error
	}{
		{
			name:    "disabled with no addr is valid",
			cfg:     FrontendConfig{Enabled: false, Addr: ""},
			wantErr: nil,
		},
		{
			name:    "enabled with addr is valid",
			cfg:     FrontendConfig{Enabled: true, Addr: ":8080"},
			wantErr: nil,
		},
		{
			name:    "enabled without addr is invalid",
			cfg:     FrontendConfig{Enabled: true, Addr: ""},
			wantErr: ErrFrontendAddrRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cfg.Validate()
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

// validConfig returns a minimal Config that passes Validate.
func validConfig() *Config {
	return &Config{
		ClickHouse: clickhouse.Config{URL: "clickhouse://localhost:9000"},
		Redis:      RedisConfig{URL: "redis://localhost:6379"},
		Scheduler:  scheduler.Config{Concurrency: 10},
		Worker:     worker.Config{Concurrency: 10},
		Frontend:   FrontendConfig{Enabled: false},
		Management: management.Config{Enabled: false},
	}
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(c *Config)
		wantErr error
	}{
		{
			name:    "valid config",
			mutate:  func(_ *Config) {},
			wantErr: nil,
		},
		{
			name:    "missing redis url",
			mutate:  func(c *Config) { c.Redis.URL = "" },
			wantErr: ErrRedisURLRequired,
		},
		{
			name:    "invalid clickhouse config",
			mutate:  func(c *Config) { c.ClickHouse.URL = "" },
			wantErr: clickhouse.ErrURLRequired,
		},
		{
			name:    "invalid scheduler config",
			mutate:  func(c *Config) { c.Scheduler.Concurrency = 0 },
			wantErr: scheduler.ErrInvalidConcurrency,
		},
		{
			name:    "invalid worker config",
			mutate:  func(c *Config) { c.Worker.Concurrency = 0 },
			wantErr: worker.ErrInvalidConcurrency,
		},
		{
			name:    "invalid frontend config",
			mutate:  func(c *Config) { c.Frontend = FrontendConfig{Enabled: true, Addr: ""} },
			wantErr: ErrFrontendAddrRequired,
		},
		{
			name: "invalid management config",
			mutate: func(c *Config) {
				c.Management = management.Config{
					Enabled: true,
					Auth: management.AuthConfig{
						GitHub: &management.GitHubConfig{},
					},
				}
			},
			wantErr: management.ErrGitHubClientIDRequired,
		},
		{
			name: "invalid interval types config",
			mutate: func(c *Config) {
				c.IntervalTypes = IntervalTypesConfig{"slot": {}}
			},
			wantErr: ErrIntervalTypeEmpty,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := validConfig()
			tt.mutate(cfg)

			err := cfg.Validate()
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestConfigModelsValidateDefaults(t *testing.T) {
	t.Parallel()

	// Models.Validate only sets defaults and never errors, but we confirm the
	// branch is reached on an otherwise valid config.
	cfg := validConfig()
	require.NoError(t, cfg.Validate())
	assert.NotEmpty(t, cfg.Models.External.Paths)
	assert.NotEmpty(t, cfg.Models.Transformation.Paths)
}

func TestConfigModelsValidateError(t *testing.T) {
	// Not parallel: mutates the package-level validateModelsConfig seam.
	orig := validateModelsConfig
	t.Cleanup(func() { validateModelsConfig = orig })

	validateModelsConfig = func(*models.Config) error { return errSeedModels }

	cfg := validConfig()
	err := cfg.Validate()
	require.ErrorIs(t, err, errSeedModels)
}
