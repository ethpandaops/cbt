import { type JSX } from 'react';
import { useQuery } from '@tanstack/react-query';
import { listTransformationsOptions, listScheduledRunsOptions } from '@api/@tanstack/react-query.gen';
import { ScheduledModelCard } from './ScheduledModelCard';
import { LoadingState } from './shared/LoadingState';
import { ErrorState } from './shared/ErrorState';

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
    return <LoadingState />;
  }

  if (scheduledTransformations.error || runs.error) {
    return <ErrorState message={scheduledTransformations.error?.message || runs.error?.message || 'Unknown error'} />;
  }

  return (
    <div className="grid gap-4" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(min(100%, 320px), 1fr))' }}>
      {scheduledTransformations.data?.models.map(model => {
        const modelRun = runs.data?.runs.find(r => r.id === model.id);
        return (
          <ScheduledModelCard key={model.id} id={model.id} lastRun={modelRun?.last_run} schedule={model.schedule} />
        );
      })}
    </div>
  );
}
