import { type JSX, useMemo } from 'react';
import { createFileRoute, Link } from '@tanstack/react-router';
import { useQuery } from '@tanstack/react-query';
import {
  listAllModelsOptions,
  listTransformationsOptions,
  listExternalModelsOptions,
} from '@api/@tanstack/react-query.gen';
import { ArrowPathIcon, XCircleIcon, ArrowLeftIcon } from '@heroicons/react/24/outline';
import { ReactFlowProvider } from '@xyflow/react';
import { DagGraph, type DagData } from '@/components/DagGraph';

function DagComponent(): JSX.Element {
  // Fetch all model data
  const allModels = useQuery(listAllModelsOptions());
  const transformations = useQuery(listTransformationsOptions());
  const externalModels = useQuery(listExternalModelsOptions());

  // Transform API data into DagData format
  const dagData: DagData | null = useMemo(() => {
    if (!allModels.data || !transformations.data || !externalModels.data) {
      return null;
    }

    return {
      externalModels: externalModels.data.models,
      incrementalModels: transformations.data.models.filter(m => m.type === 'incremental'),
      scheduledModels: transformations.data.models.filter(m => m.type === 'scheduled'),
    };
  }, [allModels.data, transformations.data, externalModels.data]);

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
    <div className="fixed inset-0 top-[120px] flex flex-col px-6 pb-6">
      <div className="mb-6 flex items-center justify-between">
        <div className="flex items-baseline gap-4">
          <h1 className="bg-gradient-to-r from-indigo-400 via-purple-400 to-indigo-400 bg-clip-text text-4xl font-black tracking-tight text-transparent">
            Dependency DAG
          </h1>
          <div className="flex items-center gap-3 text-xs">
            <div className="flex items-center gap-2">
              <div className="h-3 w-3 rounded-sm bg-green-500" />
              <span className="font-medium text-slate-400">External</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="h-3 w-3 rounded-sm bg-emerald-500" />
              <span className="font-medium text-slate-400">Scheduled</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="h-3 w-3 rounded-sm bg-indigo-500" />
              <span className="font-medium text-slate-400">Incremental</span>
            </div>
          </div>
        </div>
        <Link
          to="/"
          className="group inline-flex items-center gap-2 rounded-lg bg-slate-800/60 px-4 py-2 text-sm font-medium text-slate-300 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm transition-all hover:bg-slate-800/80 hover:text-indigo-400 hover:shadow-md hover:ring-indigo-500/50"
        >
          <ArrowLeftIcon className="size-4 transition-transform group-hover:-translate-x-0.5" />
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
