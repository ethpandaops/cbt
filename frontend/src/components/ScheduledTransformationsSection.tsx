import { type JSX } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Link } from '@tanstack/react-router';
import { listTransformationsOptions, listScheduledRunsOptions } from '@api/@tanstack/react-query.gen';
import { ArrowPathIcon, XCircleIcon } from '@heroicons/react/24/outline';
import { timeAgo } from '@/utils/time';

export function ScheduledTransformationsSection(): JSX.Element {
  // Fetch all transformations (polling handled at root level) and filter client-side
  const allTransformations = useQuery(listTransformationsOptions());
  const scheduledTransformations = {
    ...allTransformations,
    data: allTransformations.data
      ? { ...allTransformations.data, models: allTransformations.data.models.filter(m => m.type === 'scheduled') }
      : undefined,
  };
  const runs = useQuery(listScheduledRunsOptions());

  if (allTransformations.isLoading || runs.isLoading) {
    return (
      <div className="flex items-center gap-3 text-slate-400">
        <ArrowPathIcon className="h-5 w-5 animate-spin text-indigo-400" />
        <span className="font-medium">Loading...</span>
      </div>
    );
  }

  if (scheduledTransformations.error || runs.error) {
    return (
      <div className="rounded-xl border border-red-500/50 bg-gradient-to-br from-red-950/80 to-red-900/50 p-4 shadow-md">
        <div className="flex items-center gap-2">
          <XCircleIcon className="h-5 w-5 shrink-0 text-red-400" />
          <span className="font-semibold text-red-200">
            Error: {scheduledTransformations.error?.message || runs.error?.message}
          </span>
        </div>
      </div>
    );
  }

  return (
    <div className="grid gap-5 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
      {scheduledTransformations.data?.models.map(model => {
        const modelRun = runs.data?.runs.find(r => r.id === model.id);
        return (
          <Link
            key={model.id}
            to="/model/$id"
            params={{ id: encodeURIComponent(model.id) }}
            className="group relative overflow-hidden rounded-2xl border border-emerald-500/30 bg-slate-800/80 p-5 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm transition-all duration-300 hover:-translate-y-1 hover:shadow-xl hover:shadow-emerald-500/20 hover:ring-emerald-500/50"
          >
            <div className="absolute inset-0 bg-gradient-to-br from-emerald-500/10 via-transparent to-teal-500/10 opacity-0 transition-opacity duration-300 group-hover:opacity-100" />
            <div className="relative">
              <div className="mb-3 flex items-start justify-between gap-2">
                <h3 className="line-clamp-2 font-mono text-sm font-bold leading-tight text-slate-100" title={model.id}>
                  {model.id}
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
                <span className="font-bold text-slate-200">{timeAgo(modelRun?.last_run)}</span>
              </div>
            </div>
          </Link>
        );
      })}
    </div>
  );
}
