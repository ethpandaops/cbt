import { type JSX } from 'react';
import { ArrowPathIcon } from '@heroicons/react/24/outline';

export interface LoadingStateProps {
  message?: string;
  size?: 'sm' | 'md' | 'lg';
}

export function LoadingState({ message = 'Loading...', size = 'md' }: LoadingStateProps): JSX.Element {
  const iconSize = size === 'sm' ? 'size-4' : size === 'lg' ? 'size-6' : 'size-5';
  const textSize = size === 'sm' ? 'text-sm/5' : size === 'lg' ? 'text-base/6' : 'text-sm/6';

  return (
    <div className="flex items-center gap-3 text-muted">
      <ArrowPathIcon className={`${iconSize} animate-spin text-accent`} />
      <span className={`font-medium ${textSize}`}>{message}</span>
    </div>
  );
}
