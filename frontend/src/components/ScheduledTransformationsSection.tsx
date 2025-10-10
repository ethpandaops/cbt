import { type JSX } from 'react';
import { useQuery } from '@tanstack/react-query';
import { listTransformationsOptions, listScheduledRunsOptions } from '@api/@tanstack/react-query.gen';
import { ArrowPathIcon, XCircleIcon } from '@heroicons/react/24/outline';
import { ScheduledModelCard } from './ScheduledModelCard';

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
        return <ScheduledModelCard key={model.id} id={model.id} lastRun={modelRun?.last_run} />;
      })}
    </div>
  );
}
