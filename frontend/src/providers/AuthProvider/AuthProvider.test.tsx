import { type JSX } from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useAuth } from '@/hooks/useAuth';
import { AuthProvider } from './AuthProvider';

function AuthProbe(): JSX.Element {
  const { oauthError, managementEnabled, isLoading } = useAuth();

  return (
    <div>
      <div data-testid="oauth-error">{oauthError ?? ''}</div>
      <div data-testid="management-enabled">{String(managementEnabled)}</div>
      <div data-testid="is-loading">{String(isLoading)}</div>
    </div>
  );
}

describe('AuthProvider', () => {
  beforeEach(() => {
    window.__CONFIG__ = {
      managementEnabled: false,
      authMethods: [],
    };

    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({
        ok: true,
        json: async () => ({
          management_enabled: true,
          auth_methods: ['github'],
          authenticated: false,
        }),
      })
    );
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    window.history.replaceState({}, '', '/');
    delete window.__CONFIG__;
  });

  it('parses OAuth callback errors and cleans URL params', async () => {
    window.history.replaceState({}, '', '/?error=access_denied&error_description=user_denied&foo=bar');

    render(
      <AuthProvider>
        <AuthProbe />
      </AuthProvider>
    );

    await waitFor(() => {
      expect(screen.getByTestId('oauth-error')).toHaveTextContent('GitHub login was cancelled or denied.');
    });

    expect(window.location.search).toBe('?foo=bar');
  });

  it('checks session endpoint on mount with same-origin credentials', async () => {
    render(
      <AuthProvider>
        <AuthProbe />
      </AuthProvider>
    );

    await waitFor(() => {
      expect(screen.getByTestId('management-enabled')).toHaveTextContent('true');
      expect(screen.getByTestId('is-loading')).toHaveTextContent('false');
    });

    expect(fetch).toHaveBeenCalledWith('/api/v1/auth/session', {
      headers: {},
      credentials: 'same-origin',
    });
  });
});
