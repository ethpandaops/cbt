import { type JSX } from 'react';
import { Link } from '@tanstack/react-router';
import { timeAgo } from '@/utils/time';

export interface ScheduledModelCardProps {
  id: string;
  lastRun?: string;
}

export function ScheduledModelCard({ id, lastRun }: ScheduledModelCardProps): JSX.Element {
  return (
    <Link
      to="/model/$id"
      params={{ id: encodeURIComponent(id) }}
      className="group relative overflow-hidden rounded-2xl border border-emerald-500/30 bg-slate-800/80 p-4 shadow-xs ring-1 ring-slate-700/50 backdrop-blur-sm transition-all duration-300 hover:-translate-y-1 hover:shadow-xl hover:shadow-emerald-500/20 hover:ring-emerald-500/50 sm:p-5"
    >
      <div className="absolute inset-0 bg-linear-to-br from-emerald-500/10 via-transparent to-teal-500/10 opacity-0 transition-opacity duration-300 group-hover:opacity-100" />
      <div className="relative">
        <div className="mb-2 flex items-start justify-between gap-2 sm:mb-3">
          <h3 className="line-clamp-2 font-mono text-xs font-bold leading-tight text-slate-100 sm:text-sm/4" title={id}>
            {id}
          </h3>
          <div className="shrink-0 rounded-full bg-emerald-500/20 p-1 ring-1 ring-emerald-500/50 sm:p-1.5">
            <svg className="size-3 text-emerald-400 sm:size-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"
              />
            </svg>
          </div>
        </div>
        <div className="flex items-center gap-1.5 rounded-lg bg-slate-900/60 px-2.5 py-1.5 text-xs sm:gap-2 sm:px-3 sm:py-2">
          <span className="font-medium text-slate-400">Last Run:</span>
          <span className="font-bold text-slate-200">{timeAgo(lastRun)}</span>
        </div>
      </div>
    </Link>
  );
}
