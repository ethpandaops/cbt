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
		tc := tc
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
