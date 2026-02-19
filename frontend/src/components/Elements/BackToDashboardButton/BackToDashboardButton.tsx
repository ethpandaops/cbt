import { type JSX } from 'react';
import { Link } from '@tanstack/react-router';
import { ArrowLeftIcon } from '@heroicons/react/24/outline';

export function BackToDashboardButton(): JSX.Element {
  return (
    <Link
      to="/"
      className="glass-control group mb-4 inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-semibold sm:mb-6 sm:gap-2 sm:px-4 sm:py-2 sm:text-sm/5"
    >
      <ArrowLeftIcon className="size-3.5 transition-transform group-hover:-translate-x-0.5 sm:size-4" />
      Back to Dashboard
    </Link>
  );
}
