export type AuthMethod = 'password' | 'github';

export interface AuthSession {
  authenticated: boolean;
  username?: string;
}

export interface AuthContextValue {
  managementEnabled: boolean;
  authMethods: AuthMethod[];
  session: AuthSession | null;
  isLoading: boolean;
  loginWithPassword: (password: string) => Promise<boolean>;
  loginWithGitHub: () => void;
  logout: () => void;
}
