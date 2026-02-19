import { render } from '@testing-library/react';
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { AdminMenu } from './AdminMenu';
import { useAuth } from '@/hooks/useAuth';
import { useNotification } from '@/hooks/useNotification';
import type { AuthContextValue } from '@/contexts/AuthContext';
import type { NotificationContextValue } from '@/contexts/NotificationContext';

vi.mock('@/hooks/useAuth', () => ({
  useAuth: vi.fn(),
}));

vi.mock('@/hooks/useNotification', () => ({
  useNotification: vi.fn(),
}));

function createAuthValue(overrides: Partial<AuthContextValue>): AuthContextValue {
  return {
    managementEnabled: true,
    authMethods: ['github'],
    session: { authenticated: false },
    oauthError: null,
    isLoading: false,
    clearOAuthError: () => {},
    loginWithPassword: async () => false,
    loginWithGitHub: () => {},
    logout: () => {},
    ...overrides,
  };
}

function createNotificationValue(overrides: Partial<NotificationContextValue>): NotificationContextValue {
  return {
    showNotification: () => {},
    showSuccess: () => {},
    showError: () => {},
    ...overrides,
  };
}

describe('AdminMenu', () => {
  const mockedUseAuth = vi.mocked(useAuth);
  const mockedUseNotification = vi.mocked(useNotification);

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('shows OAuth error toast and clears it once', () => {
    const showError = vi.fn();
    const clearOAuthError = vi.fn();

    mockedUseNotification.mockReturnValue(createNotificationValue({ showError }));
    mockedUseAuth.mockReturnValue(
      createAuthValue({
        oauthError: 'GitHub login was cancelled or denied.',
        clearOAuthError,
      })
    );

    render(<AdminMenu />);

    expect(showError).toHaveBeenCalledWith('GitHub login was cancelled or denied.');
    expect(clearOAuthError).toHaveBeenCalledTimes(1);
  });

  it('does not emit error toast when oauthError is empty', () => {
    const showError = vi.fn();

    mockedUseNotification.mockReturnValue(createNotificationValue({ showError }));
    mockedUseAuth.mockReturnValue(createAuthValue({ oauthError: null }));

    render(<AdminMenu />);

    expect(showError).not.toHaveBeenCalled();
  });
});
