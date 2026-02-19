import { createContext } from 'react';
import type { AuthContextValue } from './AuthContext.types';

export const AuthContext = createContext<AuthContextValue | undefined>(undefined);
