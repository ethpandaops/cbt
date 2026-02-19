import { type JSX, useEffect, useState, useMemo, useCallback } from 'react';
import { AuthContext, type AuthMethod, type AuthSession } from '@/contexts/AuthContext';
import { getInjectedConfig } from '@/utils/config';
import { parseOAuthCallbackError, stripOAuthCallbackErrorParams } from '@/utils/oauth-error';

const PASSWORD_STORAGE_KEY = 'cbt_admin_password';

interface AuthProviderProps {
  children: React.ReactNode;
}

export function AuthProvider({ children }: AuthProviderProps): JSX.Element {
  const config = getInjectedConfig();

  // State that can be updated from the API response in dev mode.
  const [managementEnabled, setManagementEnabled] = useState(config?.managementEnabled ?? false);
  const [authMethods, setAuthMethods] = useState<AuthMethod[]>(() => (config?.authMethods ?? []) as AuthMethod[]);
  const [session, setSession] = useState<AuthSession | null>(null);
  const [oauthError, setOAuthError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  const checkSession = useCallback(async (): Promise<boolean> => {
    try {
      const headers: Record<string, string> = {};
      const storedPassword = localStorage.getItem(PASSWORD_STORAGE_KEY);
      if (storedPassword) {
        headers['Authorization'] = `Bearer ${storedPassword}`;
      }

      const resp = await fetch('/api/v1/auth/session', {
        headers,
        credentials: 'same-origin',
      });

      if (resp.ok) {
        const data = await resp.json();

        // Update management state from API (supports dev mode where
        // window.__CONFIG__ is not injected).
        if (data.management_enabled !== undefined) {
          setManagementEnabled(data.management_enabled);
        }

        if (data.auth_methods !== undefined) {
          setAuthMethods(data.auth_methods as AuthMethod[]);
        }

        const authenticated = data.authenticated ?? false;

        setSession({
          authenticated,
          username: data.username,
        });

        return authenticated;
      } else if (resp.status === 404) {
        // Management not enabled — endpoint doesn't exist.
        setManagementEnabled(false);
        setSession(null);
      } else {
        setSession({ authenticated: false });
      }
    } catch {
      // Network error or endpoint not found — management not available.
      setManagementEnabled(false);
      setSession(null);
    } finally {
      setIsLoading(false);
    }

    return false;
  }, []);

  useEffect(() => {
    const parsed = parseOAuthCallbackError(window.location.search);
    if (parsed) {
      setOAuthError(parsed.message);

      const cleanedSearch = stripOAuthCallbackErrorParams(window.location.search);
      const cleanedUrl = `${window.location.pathname}${cleanedSearch}${window.location.hash}`;
      window.history.replaceState(window.history.state, '', cleanedUrl);
    }
  }, []);

  useEffect(() => {
    void checkSession();
  }, [checkSession]);

  const clearOAuthError = useCallback(() => {
    setOAuthError(null);
  }, []);

  const loginWithPassword = useCallback(
    async (password: string): Promise<boolean> => {
      localStorage.setItem(PASSWORD_STORAGE_KEY, password);

      const authenticated = await checkSession();

      if (!authenticated) {
        localStorage.removeItem(PASSWORD_STORAGE_KEY);
      }

      return authenticated;
    },
    [checkSession]
  );

  const loginWithGitHub = useCallback(() => {
    window.location.href = '/api/v1/auth/github/login';
  }, []);

  const logout = useCallback(async () => {
    localStorage.removeItem(PASSWORD_STORAGE_KEY);

    if (authMethods.includes('github')) {
      try {
        await fetch('/api/v1/auth/logout', {
          method: 'POST',
          credentials: 'same-origin',
        });
      } catch {
        // Logout failure is non-critical.
      }
    }

    setSession({ authenticated: false });
  }, [authMethods]);

  const value = useMemo(
    () => ({
      managementEnabled,
      authMethods,
      session,
      oauthError,
      isLoading,
      clearOAuthError,
      loginWithPassword,
      loginWithGitHub,
      logout,
    }),
    [
      managementEnabled,
      authMethods,
      session,
      oauthError,
      isLoading,
      clearOAuthError,
      loginWithPassword,
      loginWithGitHub,
      logout,
    ]
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}
