import { type JSX } from 'react';
import { BackToDashboardButton } from '@/components/BackToDashboardButton';
import { ModelHeader } from '@/components/ModelHeader';
import { ModelInfoCard, type InfoField } from '@/components/ModelInfoCard';
import { DependencyRow } from '@/components/DependencyRow';
import { CoverageBar } from '@/components/CoverageBar';
import { ZoomControls } from '@/components/ZoomControls';
import { Tooltip } from 'react-tooltip';

export interface PageProps {
  modelType: 'external' | 'scheduled' | 'incremental';
}

export function Page({ modelType }: PageProps): JSX.Element {
  if (modelType === 'external') {
    const fields: InfoField[] = [
      { label: 'Database', value: 'clickhouse' },
      { label: 'Table', value: 'beacon_api_blocks' },
      { label: 'Interval Type', value: 'slot_number' },
      { label: 'Min Position', value: '0', variant: 'highlight', highlightColor: 'green' },
      { label: 'Max Position', value: '8,500,000', variant: 'highlight', highlightColor: 'green' },
    ];

    return (
      <div className="bg-slate-950 p-8">
        <BackToDashboardButton showLink={false} />
        <ModelHeader modelId="beacon_api.external_blocks" modelType="external" />
        <ModelInfoCard title="Model Information" fields={fields} borderColor="border-green-500/30" />
      </div>
    );
  }

  if (modelType === 'scheduled') {
    const fields: InfoField[] = [
      { label: 'Database', value: 'clickhouse' },
      { label: 'Table', value: 'daily_summary' },
      { label: 'Content Type', value: 'parquet' },
      { label: 'Schedule', value: '0 0 * * *', variant: 'highlight', highlightColor: 'emerald' },
      { label: 'Last Run', value: '2 hours ago' },
      { label: 'Status', value: 'success' },
    ];

    return (
      <div className="bg-slate-950 p-8">
        <BackToDashboardButton showLink={false} />
        <ModelHeader modelId="beacon_api.scheduled_daily_report" modelType="scheduled" />
        <ModelInfoCard title="Transformation Details" fields={fields} borderColor="border-emerald-500/30" />
      </div>
    );
  }

  // Incremental
  const infoFields: InfoField[] = [
    { label: 'Database', value: 'clickhouse' },
    { label: 'Table', value: 'transformed_data' },
    { label: 'Type', value: 'incremental', variant: 'highlight', highlightColor: 'indigo' },
    { label: 'Content Type', value: 'parquet' },
  ];

  return (
    <div className="bg-slate-950 p-8">
      <BackToDashboardButton showLink={false} />
      <ModelHeader modelId="beacon_api.incremental_transformation" modelType="incremental" />
      <div className="mb-6">
        <ModelInfoCard title="Model Information" fields={infoFields} borderColor="border-indigo-500/30" columns={4} />
      </div>

      <div className="rounded-2xl border border-indigo-500/30 bg-slate-800/80 p-6 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm">
        <div className="mb-6 flex items-center justify-between">
          <h2 className="text-lg font-bold text-slate-100">Coverage Analysis</h2>
          <div className="flex items-center gap-4 text-xs">
            <div className="flex items-center gap-2">
              <div className="size-3 rounded-sm bg-indigo-500" />
              <span className="font-medium text-slate-400">This Model</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="size-3 rounded-sm bg-indigo-400" />
              <span className="font-medium text-slate-400">Dependencies (Transform)</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="size-3 rounded-sm bg-green-500" />
              <span className="font-medium text-slate-400">Dependencies (External)</span>
            </div>
          </div>
        </div>

        <div className="mb-6">
          <div className="mb-2 flex items-center justify-between">
            <span className="font-mono text-sm font-bold text-slate-200">beacon_api.incremental_transformation</span>
            <span className="rounded-lg bg-slate-900/60 px-3 py-1 font-mono text-xs font-semibold text-slate-300">
              0 - 1,000
            </span>
          </div>
          <CoverageBar
            ranges={[
              { position: 100, interval: 200 },
              { position: 400, interval: 300 },
            ]}
            zoomStart={0}
            zoomEnd={1000}
            type="transformation"
            height={96}
            tooltipId="page-tooltip"
          />
        </div>

        <div className="mt-8 border-t border-slate-700/50 pt-6">
          <h3 className="mb-4 text-base font-bold text-slate-100">
            Dependencies{' '}
            <span className="ml-2 rounded-full bg-slate-700 px-2 py-0.5 text-xs font-bold text-slate-300">2</span>
          </h3>
          <div className="space-y-3">
            <DependencyRow
              dependencyId="beacon_api.blocks"
              type="external"
              bounds={{ min: 0, max: 800 }}
              zoomStart={0}
              zoomEnd={1000}
              tooltipId="page-tooltip"
              showLink={false}
            />
            <DependencyRow
              dependencyId="beacon_api.validators"
              type="transformation"
              ranges={[{ position: 200, interval: 500 }]}
              zoomStart={0}
              zoomEnd={1000}
              tooltipId="page-tooltip"
              showLink={false}
            />
          </div>
        </div>

        <div className="mt-4 border-t border-slate-700/50 pt-4">
          <ZoomControls
            globalMin={0}
            globalMax={1000}
            zoomStart={0}
            zoomEnd={1000}
            onZoomChange={() => {}}
            onResetZoom={() => {}}
          />
        </div>
      </div>

      <Tooltip id="page-tooltip" className="!bg-gray-900 !text-white !text-xs !px-2 !py-1 !rounded !opacity-100" />
    </div>
  );
}
