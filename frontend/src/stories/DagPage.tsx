import { type JSX } from 'react';
import { ReactFlowProvider } from '@xyflow/react';
import { AppHeader } from '@/components/AppHeader';
import { DagGraph, type DagData } from '@/components/DagGraph';
import { Link } from '@tanstack/react-router';
import { ArrowLeftIcon } from '@heroicons/react/24/outline';

const dagData: DagData = {
  externalModels: [{ id: 'beacon_api.blocks' }, { id: 'beacon_api.attestations' }],
  incrementalModels: [
    { id: 'beacon_api.validators', depends_on: ['beacon_api.blocks'] },
    { id: 'beacon_api.committees', depends_on: ['beacon_api.blocks', 'beacon_api.attestations'] },
    { id: 'beacon_api.aggregated_attestations', depends_on: ['beacon_api.committees'] },
  ],
  scheduledModels: [{ id: 'beacon_api.daily_summary', depends_on: ['beacon_api.validators', 'beacon_api.committees'] }],
};

export interface DagPageProps {
  showLinks?: boolean;
}

export function DagPage({ showLinks = false }: DagPageProps): JSX.Element {
  return (
    <ReactFlowProvider>
      <div className="relative h-[50vh] bg-gradient-to-br from-slate-950 via-slate-900 to-slate-950">
        {/* Ambient background decoration */}
        <div className="pointer-events-none fixed inset-0 overflow-hidden">
          <div className="absolute -left-48 -top-48 size-96 rounded-full bg-gradient-to-br from-indigo-500/20 to-purple-500/20 blur-3xl" />
          <div className="absolute -right-48 top-48 size-96 rounded-full bg-gradient-to-br from-blue-500/20 to-cyan-500/20 blur-3xl" />
          <div className="absolute -bottom-48 left-1/2 size-96 -translate-x-1/2 rounded-full bg-gradient-to-br from-violet-500/20 to-fuchsia-500/20 blur-3xl" />
        </div>

        <AppHeader showLinks={showLinks} />

        <main className="relative mx-auto max-w-screen-2xl px-4 py-10 sm:px-6 lg:px-8">
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
              {showLinks ? (
                <Link
                  to="/"
                  className="group inline-flex items-center gap-2 rounded-lg bg-slate-800/60 px-4 py-2 text-sm font-medium text-slate-300 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm transition-all hover:bg-slate-800/80 hover:text-indigo-400 hover:shadow-md hover:ring-indigo-500/50"
                >
                  <ArrowLeftIcon className="size-4 transition-transform group-hover:-translate-x-0.5" />
                  Back to Dashboard
                </Link>
              ) : (
                <button className="group inline-flex items-center gap-2 rounded-lg bg-slate-800/60 px-4 py-2 text-sm font-medium text-slate-300 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm transition-all hover:bg-slate-800/80 hover:text-indigo-400 hover:shadow-md hover:ring-indigo-500/50">
                  <ArrowLeftIcon className="size-4 transition-transform group-hover:-translate-x-0.5" />
                  Back to Dashboard
                </button>
              )}
            </div>

            <DagGraph data={dagData} showLinks={showLinks} className="flex-1" />
          </div>
        </main>
      </div>
    </ReactFlowProvider>
  );
}
