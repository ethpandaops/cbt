import { useContext } from 'react';
import { NotificationContext } from '@/contexts/NotificationContext';
import type { NotificationContextValue } from '@/contexts/NotificationContext';

export function useNotification(): NotificationContextValue {
  const context = useContext(NotificationContext);

  if (!context) {
    throw new Error('useNotification must be used within NotificationProvider');
  }

  return context;
}
