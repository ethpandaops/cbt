import { type JSX } from 'react';
import { AppHeader } from '@/components/AppHeader';
import { IntervalTypeSection } from '@/components/IntervalTypeSection';
import { ScheduledModelCard } from '@/components/ScheduledModelCard';
import type { IncrementalModelItem } from '@/types';
import type { IntervalTypeTransformation } from '@api/types.gen';
import { Tooltip } from 'react-tooltip';

const sampleModels: IncrementalModelItem[] = [
  {
    id: 'beacon_api.validators',
    type: 'transformation',
    intervalType: 'slot_number',
    data: {
      coverage: [
        { position: 100, interval: 200 },
        { position: 400, interval: 150 },
      ],
    },
  },
  {
    id: 'beacon_api.blocks',
    type: 'external',
    intervalType: 'slot_number',
    data: {
      bounds: { min: 0, max: 800 },
    },
  },
  {
    id: 'beacon_api.attestations',
    type: 'transformation',
    intervalType: 'slot_number',
    data: {
      coverage: [{ position: 200, interval: 400 }],
    },
  },
];

const transformations: IntervalTypeTransformation[] = [
  {
    name: 'Slot Number',
    expression: 'x',
  },
  {
    name: 'Epoch',
    expression: 'x / 32',
  },
];

export function DashboardPage(): JSX.Element {
  return (
    <div className="relative bg-gradient-to-br from-slate-950 via-slate-900 to-slate-950">
      {/* Ambient background decoration */}
      <div className="pointer-events-none fixed inset-0 overflow-hidden">
        <div className="absolute -left-48 -top-48 size-96 rounded-full bg-gradient-to-br from-indigo-500/20 to-purple-500/20 blur-3xl" />
        <div className="absolute -right-48 top-48 size-96 rounded-full bg-gradient-to-br from-blue-500/20 to-cyan-500/20 blur-3xl" />
        <div className="absolute -bottom-48 left-1/2 size-96 -translate-x-1/2 rounded-full bg-gradient-to-br from-violet-500/20 to-fuchsia-500/20 blur-3xl" />
      </div>

      <AppHeader showLinks={false} />

      <main className="relative mx-auto max-w-screen-2xl px-4 py-10 sm:px-6 lg:px-8">
        <div className="mb-8">
          <h2 className="mb-6 text-2xl font-bold text-slate-100">Incremental Transformations</h2>
          <div className="space-y-6">
            <IntervalTypeSection
              intervalType="slot_number"
              models={sampleModels}
              zoomRange={{ start: 0, end: 1000 }}
              globalMin={0}
              globalMax={1000}
              transformations={transformations}
              onZoomChange={() => {}}
              onResetZoom={() => {}}
              showLinks={false}
            />
          </div>
        </div>

        <div>
          <h2 className="mb-6 text-2xl font-bold text-slate-100">Scheduled Transformations</h2>
          <div className="grid gap-5 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
            <ScheduledModelCard id="beacon_api.daily_summary" lastRun="2024-10-10T12:00:00Z" showLink={false} />
            <ScheduledModelCard id="beacon_api.weekly_report" lastRun="2024-10-09T18:30:00Z" showLink={false} />
            <ScheduledModelCard id="beacon_api.monthly_aggregation" showLink={false} />
          </div>
        </div>
      </main>

      <Tooltip id="chunk-tooltip" className="!bg-gray-900 !text-white !text-xs !px-2 !py-1 !rounded !opacity-100" />
    </div>
  );
}
