import { type JSX, useMemo } from 'react';
import { createFileRoute } from '@tanstack/react-router';
import { useQuery } from '@tanstack/react-query';
import {
  listAllModelsOptions,
  listTransformationsOptions,
  listExternalModelsOptions,
  listExternalBoundsOptions,
  listTransformationCoverageOptions,
  getIntervalTypesOptions,
} from '@api/@tanstack/react-query.gen';
import { ReactFlowProvider } from '@xyflow/react';
import { DagGraph, type DagData } from '@/components/Domain/DAG/DagGraph';
import { LoadingState } from '@/components/Feedback/LoadingState';
import { ErrorState } from '@/components/Feedback/ErrorState';
import { BackToDashboardButton } from '@/components/Elements/BackToDashboardButton';

function DagComponent(): JSX.Element {
  // Fetch all model data
  const allModels = useQuery(listAllModelsOptions());
  const transformations = useQuery(listTransformationsOptions());
  const externalModels = useQuery(listExternalModelsOptions());
  const bounds = useQuery(listExternalBoundsOptions());
  const coverage = useQuery(listTransformationCoverageOptions());
  const intervalTypes = useQuery(getIntervalTypesOptions());

  // Transform API data into DagData format
  const dagData: DagData | null = useMemo(() => {
    if (!allModels.data || !transformations.data || !externalModels.data) {
      return null;
    }

    return {
      externalModels: externalModels.data.models,
      incrementalModels: transformations.data.models.filter(m => m.type === 'incremental'),
      scheduledModels: transformations.data.models.filter(m => m.type === 'scheduled'),
      bounds: bounds.data?.bounds,
      coverage: coverage.data?.coverage,
      intervalTypes: intervalTypes.data?.interval_types,
    };
  }, [allModels.data, transformations.data, externalModels.data, bounds.data, coverage.data, intervalTypes.data]);

  if (allModels.isLoading || transformations.isLoading || externalModels.isLoading) {
    return <LoadingState message="Loading DAG..." />;
  }

  if (allModels.error || transformations.error || externalModels.error) {
    return (
      <ErrorState
        message={
          allModels.error?.message || transformations.error?.message || externalModels.error?.message || 'Unknown error'
        }
      />
    );
  }

  return (
    <div className="fixed inset-0 top-[130px] flex flex-col px-4 pb-4 sm:px-6 sm:pb-6 md:top-[150px]">
      <div className="mb-4 flex flex-col gap-4 sm:mb-6 sm:flex-row sm:items-center sm:justify-between">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-baseline sm:gap-4">
          <h1 className="bg-linear-to-r from-incremental via-accent to-scheduled bg-clip-text text-2xl font-black tracking-tight text-transparent sm:text-3xl lg:text-4xl">
            Dependency DAG
          </h1>
          <div className="flex flex-wrap items-center gap-3 rounded-lg bg-surface/86 px-3 py-2 text-xs ring-1 ring-border/70">
            <div className="flex items-center gap-1.5 sm:gap-2">
              <div className="size-2.5 rounded-sm bg-external sm:size-3" />
              <span className="font-semibold text-primary">External</span>
            </div>
            <div className="flex items-center gap-1.5 sm:gap-2">
              <div className="size-2.5 rounded-sm bg-scheduled sm:size-3" />
              <span className="font-semibold text-primary">Scheduled</span>
            </div>
            <div className="flex items-center gap-1.5 sm:gap-2">
              <div className="size-2.5 rounded-sm bg-incremental sm:size-3" />
              <span className="font-semibold text-primary">Incremental</span>
            </div>
          </div>
        </div>
        <BackToDashboardButton />
      </div>

      {dagData && <DagGraph data={dagData} className="flex-1" />}
    </div>
  );
}

function DagPage(): JSX.Element {
  return (
    <ReactFlowProvider>
      <DagComponent />
    </ReactFlowProvider>
  );
}

export const Route = createFileRoute('/dag')({
  component: DagPage,
  head: () => ({
    meta: [
      { title: `DAG View | ${import.meta.env.VITE_BASE_TITLE}` },
      { name: 'description', content: 'Directed acyclic graph visualization of model dependencies' },
      { property: 'og:description', content: 'Directed acyclic graph visualization of model dependencies' },
      { name: 'twitter:description', content: 'Directed acyclic graph visualization of model dependencies' },
    ],
  }),
});
