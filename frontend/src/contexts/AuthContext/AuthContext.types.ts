export type AuthMethod = 'password' | 'github';

export interface AuthSession {
  authenticated: boolean;
  username?: string;
}

export interface AuthContextValue {
  managementEnabled: boolean;
  authMethods: AuthMethod[];
  session: AuthSession | null;
  oauthError: string | null;
  isLoading: boolean;
  clearOAuthError: () => void;
  loginWithPassword: (password: string) => Promise<boolean>;
  loginWithGitHub: () => void;
  logout: () => void;
}
