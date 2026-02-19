import type { ReactNode } from 'react';

export type NotificationPosition =
  | 'top-left'
  | 'top-center'
  | 'top-right'
  | 'bottom-left'
  | 'bottom-center'
  | 'bottom-right';

export type NotificationVariant = 'info' | 'success' | 'warning' | 'danger';

export interface NotificationProps {
  open: boolean;
  onClose: () => void;
  children: ReactNode;
  position?: NotificationPosition;
  variant?: NotificationVariant;
  duration?: number | null;
  showCloseButton?: boolean;
  className?: string;
}
