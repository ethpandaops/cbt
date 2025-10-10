import { type JSX } from 'react';
import { Link } from '@tanstack/react-router';
import { ArrowLeftIcon } from '@heroicons/react/24/outline';

export interface BackToDashboardButtonProps {
  showLink?: boolean;
}

export function BackToDashboardButton({ showLink = true }: BackToDashboardButtonProps): JSX.Element {
  const className =
    'group mb-4 inline-flex items-center gap-1.5 rounded-lg bg-slate-800/60 px-3 py-1.5 text-xs font-medium text-slate-300 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm transition-all hover:bg-slate-800/80 hover:text-indigo-400 hover:shadow-md hover:ring-indigo-500/50 sm:mb-6 sm:gap-2 sm:px-4 sm:py-2 sm:text-sm';

  const content = (
    <>
      <ArrowLeftIcon className="size-3.5 transition-transform group-hover:-translate-x-0.5 sm:size-4" />
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
