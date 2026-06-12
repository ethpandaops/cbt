package management

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGitHubConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     GitHubConfig
		wantErr error
	}{
		{
			name: "missing client_id",
			cfg: GitHubConfig{
				ClientSecret: "secret",
				CallbackURL:  "http://localhost:8080/api/v1/auth/github/callback",
				Org:          "ethpandaops",
			},
			wantErr: ErrGitHubClientIDRequired,
		},
		{
			name: "missing client_secret",
			cfg: GitHubConfig{
				ClientID:    "client-id",
				CallbackURL: "http://localhost:8080/api/v1/auth/github/callback",
				Org:         "ethpandaops",
			},
			wantErr: ErrGitHubClientSecretRequired,
		},
		{
			name: "missing callback_url",
			cfg: GitHubConfig{
				ClientID:     "client-id",
				ClientSecret: "secret",
				Org:          "ethpandaops",
			},
			wantErr: ErrGitHubCallbackURLRequired,
		},
		{
			name: "missing org and allowed_users",
			cfg: GitHubConfig{
				ClientID:     "client-id",
				ClientSecret: "secret",
				CallbackURL:  "http://localhost:8080/api/v1/auth/github/callback",
			},
			wantErr: ErrGitHubAuthorizationRequired,
		},
		{
			name: "valid with org",
			cfg: GitHubConfig{
				ClientID:     "client-id",
				ClientSecret: "secret",
				CallbackURL:  "http://localhost:8080/api/v1/auth/github/callback",
				Org:          "ethpandaops",
			},
		},
		{
			name: "valid with allowed_users",
			cfg: GitHubConfig{
				ClientID:     "client-id",
				ClientSecret: "secret",
				CallbackURL:  "http://localhost:8080/api/v1/auth/github/callback",
				AllowedUsers: []string{"octocat"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.cfg.Validate()
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	validGitHub := func() *GitHubConfig {
		return &GitHubConfig{
			ClientID:     "client-id",
			ClientSecret: "secret",
			CallbackURL:  "http://localhost:8080/cb",
			Org:          "ethpandaops",
		}
	}

	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name: "disabled skips validation",
			cfg:  Config{Enabled: false, Auth: AuthConfig{GitHub: &GitHubConfig{}}},
		},
		{
			name: "enabled no auth",
			cfg:  Config{Enabled: true},
		},
		{
			name: "enabled valid github",
			cfg:  Config{Enabled: true, Auth: AuthConfig{GitHub: validGitHub()}},
		},
		{
			name:    "enabled invalid github",
			cfg:     Config{Enabled: true, Auth: AuthConfig{GitHub: &GitHubConfig{}}},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.cfg.Validate()
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestAuthConfigValidate(t *testing.T) {
	t.Parallel()

	t.Run("nil github", func(t *testing.T) {
		t.Parallel()

		cfg := AuthConfig{Password: "pw"}
		require.NoError(t, cfg.Validate())
	})

	t.Run("invalid github wrapped", func(t *testing.T) {
		t.Parallel()

		cfg := AuthConfig{GitHub: &GitHubConfig{}}
		err := cfg.Validate()
		require.Error(t, err)
		require.ErrorIs(t, err, ErrGitHubClientIDRequired)
		require.Contains(t, err.Error(), "github auth")
	})
}

func TestGitHubConfigValidateSessionTTLDefault(t *testing.T) {
	t.Parallel()

	cfg := GitHubConfig{
		ClientID:     "client-id",
		ClientSecret: "secret",
		CallbackURL:  "http://localhost:8080/api/v1/auth/github/callback",
		Org:          "ethpandaops",
	}

	require.NoError(t, cfg.Validate())
	require.Equal(t, 24*time.Hour, cfg.SessionTTL)
}

func TestGitHubConfigValidatePreservesSessionTTL(t *testing.T) {
	t.Parallel()

	cfg := GitHubConfig{
		ClientID:     "client-id",
		ClientSecret: "secret",
		CallbackURL:  "http://localhost:8080/api/v1/auth/github/callback",
		Org:          "ethpandaops",
		SessionTTL:   2 * time.Hour,
	}

	require.NoError(t, cfg.Validate())
	require.Equal(t, 2*time.Hour, cfg.SessionTTL)
}
