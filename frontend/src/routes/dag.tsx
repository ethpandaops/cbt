import { type JSX, useMemo } from 'react';
import { createFileRoute, Link } from '@tanstack/react-router';
import { useQuery } from '@tanstack/react-query';
import {
  listAllModelsOptions,
  listTransformationsOptions,
  listExternalModelsOptions,
  listExternalBoundsOptions,
  listTransformationCoverageOptions,
  getIntervalTypesOptions,
} from '@api/@tanstack/react-query.gen';
import { ArrowPathIcon, XCircleIcon, ArrowLeftIcon } from '@heroicons/react/24/outline';
import { ReactFlowProvider } from '@xyflow/react';
import { DagGraph, type DagData } from '@/components/DagGraph';

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
    return (
      <div className="flex items-center gap-3 text-slate-400">
        <ArrowPathIcon className="h-5 w-5 animate-spin text-indigo-400" />
        <span className="font-medium">Loading DAG...</span>
      </div>
    );
  }

  if (allModels.error || transformations.error || externalModels.error) {
    return (
      <div className="rounded-xl border border-red-500/50 bg-gradient-to-br from-red-950/80 to-red-900/50 p-4 shadow-md">
        <div className="flex items-center gap-2">
          <XCircleIcon className="h-5 w-5 shrink-0 text-red-400" />
          <span className="font-semibold text-red-200">
            Error: {allModels.error?.message || transformations.error?.message || externalModels.error?.message}
          </span>
        </div>
      </div>
    );
  }

  return (
    <div className="fixed inset-0 top-[130px] flex flex-col px-4 pb-4 md:top-[150px] sm:px-6 sm:pb-6">
      <div className="mb-4 flex flex-col gap-4 sm:mb-6 sm:flex-row sm:items-center sm:justify-between">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-baseline sm:gap-4">
          <h1 className="bg-linear-to-r from-indigo-400 via-purple-400 to-indigo-400 bg-clip-text text-2xl font-black tracking-tight text-transparent sm:text-3xl lg:text-4xl">
            Dependency DAG
          </h1>
          <div className="flex flex-wrap items-center gap-3 text-xs">
            <div className="flex items-center gap-1.5 sm:gap-2">
              <div className="size-2.5 rounded-sm bg-green-500 sm:size-3" />
              <span className="font-medium text-slate-400">External</span>
            </div>
            <div className="flex items-center gap-1.5 sm:gap-2">
              <div className="size-2.5 rounded-sm bg-emerald-500 sm:size-3" />
              <span className="font-medium text-slate-400">Scheduled</span>
            </div>
            <div className="flex items-center gap-1.5 sm:gap-2">
              <div className="size-2.5 rounded-sm bg-indigo-500 sm:size-3" />
              <span className="font-medium text-slate-400">Incremental</span>
            </div>
          </div>
        </div>
        <Link
          to="/"
          className="group inline-flex w-fit items-center gap-1.5 rounded-lg bg-slate-800/60 px-3 py-1.5 text-xs font-medium text-slate-300 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm transition-all hover:bg-slate-800/80 hover:text-indigo-400 hover:shadow-md hover:ring-indigo-500/50 sm:gap-2 sm:px-4 sm:py-2 sm:text-sm"
        >
          <ArrowLeftIcon className="size-3.5 transition-transform group-hover:-translate-x-0.5 sm:size-4" />
          Back to Dashboard
        </Link>
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
});
