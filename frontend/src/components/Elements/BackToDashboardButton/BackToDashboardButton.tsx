import { type JSX } from 'react';
import { Link } from '@tanstack/react-router';
import { ArrowLeftIcon } from '@heroicons/react/24/outline';

export function BackToDashboardButton(): JSX.Element {
  return (
    <Link
      to="/"
      className="group mb-4 inline-flex items-center gap-1.5 rounded-xl bg-surface/95 px-3 py-1.5 text-xs font-semibold text-primary shadow-sm ring-1 ring-border/70 backdrop-blur-sm transition-all hover:bg-secondary/85 hover:text-accent hover:shadow-md hover:ring-accent/55 focus:ring-2 focus:ring-accent/55 focus:outline-hidden active:scale-[0.98] sm:mb-6 sm:gap-2 sm:px-4 sm:py-2 sm:text-sm/5 dark:bg-surface/85 dark:text-foreground dark:ring-border/65 dark:hover:bg-secondary/80 dark:hover:ring-accent/65"
    >
      <ArrowLeftIcon className="size-3.5 transition-transform group-hover:-translate-x-0.5 sm:size-4" />
      Back to Dashboard
    </Link>
  );
}
