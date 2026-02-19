import { createContext } from 'react';
import type { NotificationContextValue } from './NotificationContext.types';

export const NotificationContext = createContext<NotificationContextValue | undefined>(undefined);
