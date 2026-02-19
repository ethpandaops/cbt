import type { ReactNode } from 'react';

export type NotificationVariant = 'info' | 'success' | 'warning' | 'danger';

export interface NotificationOptions {
  message: string;
  variant?: NotificationVariant;
  duration?: number;
}

export interface NotificationContextValue {
  showNotification: (options: NotificationOptions) => void;
  showSuccess: (message: string) => void;
  showError: (message: string) => void;
}

export interface NotificationProviderProps {
  children: ReactNode;
}
