import { useContext } from 'react';
import { AuthContext, type AuthContextValue } from '@/contexts/AuthContext';

/**
 * Hook to access the current auth state and login/logout functions.
 *
 * Must be used within an AuthProvider.
 *
 * @returns The current auth context value
 * @throws Error if used outside of AuthProvider
 */
export function useAuth(): AuthContextValue {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within AuthProvider');
  }
  return context;
}
