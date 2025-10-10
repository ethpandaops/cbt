import { type JSX } from 'react';
import { Link } from '@tanstack/react-router';
import { timeAgo } from '@/utils/time';

export interface ScheduledModelCardProps {
  id: string;
  lastRun?: string;
  showLink?: boolean;
}

export function ScheduledModelCard({ id, lastRun, showLink = true }: ScheduledModelCardProps): JSX.Element {
  const content = (
    <>
      <div className="absolute inset-0 bg-gradient-to-br from-emerald-500/10 via-transparent to-teal-500/10 opacity-0 transition-opacity duration-300 group-hover:opacity-100" />
      <div className="relative">
        <div className="mb-3 flex items-start justify-between gap-2">
          <h3 className="line-clamp-2 font-mono text-sm font-bold leading-tight text-slate-100" title={id}>
            {id}
          </h3>
          <div className="shrink-0 rounded-full bg-emerald-500/20 p-1.5 ring-1 ring-emerald-500/50">
            <svg className="size-3.5 text-emerald-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"
              />
            </svg>
          </div>
        </div>
        <div className="flex items-center gap-2 rounded-lg bg-slate-900/60 px-3 py-2 text-xs">
          <span className="font-medium text-slate-400">Last Run:</span>
          <span className="font-bold text-slate-200">{timeAgo(lastRun)}</span>
        </div>
      </div>
    </>
  );

  const className =
    'group relative overflow-hidden rounded-2xl border border-emerald-500/30 bg-slate-800/80 p-5 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm transition-all duration-300 hover:-translate-y-1 hover:shadow-xl hover:shadow-emerald-500/20 hover:ring-emerald-500/50';

  if (showLink) {
    return (
      <Link to="/model/$id" params={{ id: encodeURIComponent(id) }} className={className}>
        {content}
      </Link>
    );
  }

  return (
    <div className={className} role="article">
      {content}
    </div>
  );
}
