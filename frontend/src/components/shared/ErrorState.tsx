import { type JSX } from 'react';
import { XCircleIcon } from '@heroicons/react/24/outline';

export interface ErrorStateProps {
  message: string;
  variant?: 'default' | 'compact';
}

export function ErrorState({ message, variant = 'default' }: ErrorStateProps): JSX.Element {
  if (variant === 'compact') {
    return (
      <div className="rounded-lg border border-red-500/50 bg-red-950/80 p-4 text-red-200">
        <div className="flex items-center gap-2">
          <XCircleIcon className="size-5 shrink-0" />
          <span className="font-medium text-sm/5">Error: {message}</span>
        </div>
      </div>
    );
  }

  return (
    <div className="rounded-xl border border-red-500/50 bg-gradient-to-br from-red-950/80 to-red-900/50 p-4 shadow-md">
      <div className="flex items-center gap-2">
        <XCircleIcon className="size-5 shrink-0 text-red-400" />
        <span className="font-semibold text-sm/6 text-red-200">Error: {message}</span>
      </div>
    </div>
  );
}
