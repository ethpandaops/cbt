import { type JSX } from 'react';
import { XCircleIcon } from '@heroicons/react/24/outline';

export interface ErrorStateProps {
  message: string;
  variant?: 'default' | 'compact';
}

export function ErrorState({ message, variant = 'default' }: ErrorStateProps): JSX.Element {
  if (variant === 'compact') {
    return (
      <div className="rounded-lg border border-danger/50 bg-danger/15 p-4 text-danger">
        <div className="flex items-center gap-2">
          <XCircleIcon className="size-5 shrink-0" />
          <span className="text-sm/5 font-medium">Error: {message}</span>
        </div>
      </div>
    );
  }

  return (
    <div className="rounded-xl border border-danger/50 bg-gradient-to-br from-danger/15 to-danger/10 p-4 shadow-md">
      <div className="flex items-center gap-2">
        <XCircleIcon className="size-5 shrink-0 text-danger" />
        <span className="text-sm/6 font-semibold text-danger">Error: {message}</span>
      </div>
    </div>
  );
}
