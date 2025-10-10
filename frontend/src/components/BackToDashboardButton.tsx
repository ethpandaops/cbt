import { type JSX } from 'react';
import { Link } from '@tanstack/react-router';
import { ArrowLeftIcon } from '@heroicons/react/24/outline';

export interface BackToDashboardButtonProps {
  showLink?: boolean;
}

export function BackToDashboardButton({ showLink = true }: BackToDashboardButtonProps): JSX.Element {
  const className =
    'group mb-6 inline-flex items-center gap-2 rounded-lg bg-slate-800/60 px-4 py-2 text-sm font-medium text-slate-300 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm transition-all hover:bg-slate-800/80 hover:text-indigo-400 hover:shadow-md hover:ring-indigo-500/50';

  const content = (
    <>
      <ArrowLeftIcon className="size-4 transition-transform group-hover:-translate-x-0.5" />
      Back to Dashboard
    </>
  );

  if (showLink) {
    return (
      <Link to="/" className={className}>
        {content}
      </Link>
    );
  }

  return (
    <button className={className} type="button">
      {content}
    </button>
  );
}
