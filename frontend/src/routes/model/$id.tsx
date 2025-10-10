import { type JSX, useState } from 'react';
import { createFileRoute } from '@tanstack/react-router';
import { useQuery } from '@tanstack/react-query';
import {
  listAllModelsOptions,
  getExternalModelOptions,
  getExternalBoundsOptions,
  listExternalBoundsOptions,
  getTransformationOptions,
  listTransformationCoverageOptions,
  listTransformationsOptions,
  getIntervalTypesOptions,
} from '@api/@tanstack/react-query.gen';
import type { IntervalTypeTransformation } from '@api/types.gen';
import { ArrowPathIcon, XCircleIcon } from '@heroicons/react/24/outline';
import { BackToDashboardButton } from '@/components/BackToDashboardButton';
import { ModelHeader } from '@/components/ModelHeader';
import { ModelInfoCard, type InfoField } from '@/components/ModelInfoCard';
import { DependencyRow } from '@/components/DependencyRow';
import { CoverageBar } from '@/components/CoverageBar';
import { ZoomControls } from '@/components/ZoomControls';
import { Tooltip } from 'react-tooltip';
import { timeAgo } from '@/utils/time';
import { transformValue, formatValue } from '@/utils/interval-transform';

function ModelDetailComponent(): JSX.Element {
  const { id } = Route.useParams();
  const decodedId = decodeURIComponent(id);

  // Load all models to determine type
  const allModels = useQuery(listAllModelsOptions());
  const model = allModels.data?.models.find(m => m.id === decodedId);

  // Fetch interval types for transformation selector
  const intervalTypes = useQuery(getIntervalTypesOptions());

  // Track selected transformation index
  const [selectedTransformationIndex, setSelectedTransformationIndex] = useState(0);

  // Conditional queries based on model type
  const externalModel = useQuery({
    ...getExternalModelOptions({ path: { id: decodedId } }),
    enabled: model?.type === 'external',
  });

  const externalBounds = useQuery({
    ...getExternalBoundsOptions({ path: { id: decodedId } }),
    enabled: model?.type === 'external',
  });

  const transformationModel = useQuery({
    ...getTransformationOptions({ path: { id: decodedId } }),
    enabled: model?.type === 'transformation',
  });

  const coverage = useQuery(listTransformationCoverageOptions());
  const allBounds = useQuery({
    ...listExternalBoundsOptions(),
    enabled: model?.type === 'transformation',
  });

  // Fetch all transformations for recursive dependency resolution (polling handled at root level)
  const allTransformations = useQuery({
    ...listTransformationsOptions(),
    enabled: model?.type === 'transformation',
  });

  const [zoomRange, setZoomRange] = useState<{ start: number; end: number } | null>(null);

  if (allModels.isLoading) {
    return (
      <div className="flex items-center gap-3 text-slate-400">
        <ArrowPathIcon className="h-5 w-5 animate-spin" />
        <span>Loading...</span>
      </div>
    );
  }

  if (!model) {
    return (
      <div className="rounded-lg border border-red-500/50 bg-red-950/80 p-4 text-red-200">
        <div className="flex items-center gap-2">
          <XCircleIcon className="h-5 w-5 shrink-0" />
          <span className="font-medium">Model not found: {decodedId}</span>
        </div>
      </div>
    );
  }

  if (model.type === 'external') {
    if (externalModel.isLoading || externalBounds.isLoading) {
      return (
        <div className="flex items-center gap-3 text-slate-400">
          <ArrowPathIcon className="h-5 w-5 animate-spin" />
          <span>Loading external model details...</span>
        </div>
      );
    }

    const bounds = externalBounds.data;

    const fields: InfoField[] = [
      { label: 'Database', value: model.database },
      { label: 'Table', value: model.table },
    ];

    if (externalModel.data?.interval?.type) {
      fields.push({ label: 'Interval Type', value: externalModel.data.interval.type });
    }

    if (bounds) {
      fields.push(
        { label: 'Min Position', value: bounds.min.toLocaleString(), variant: 'highlight', highlightColor: 'green' },
        { label: 'Max Position', value: bounds.max.toLocaleString(), variant: 'highlight', highlightColor: 'green' }
      );
    }

    return (
      <div>
        <BackToDashboardButton />
        <ModelHeader modelId={decodedId} modelType="external" />
        <ModelInfoCard title="Model Information" fields={fields} borderColor="border-green-500/30" />
      </div>
    );
  }

  // Transformation model
  if (transformationModel.isLoading || coverage.isLoading) {
    return (
      <div className="flex items-center gap-3 text-slate-400">
        <ArrowPathIcon className="h-5 w-5 animate-spin" />
        <span>Loading transformation model details...</span>
      </div>
    );
  }

  const transformation = transformationModel.data;
  const isIncremental = transformation?.type === 'incremental';

  if (!isIncremental) {
    // Scheduled transformation - no coverage
    const fields: InfoField[] = [
      { label: 'Database', value: model.database },
      { label: 'Table', value: model.table },
      { label: 'Content Type', value: transformation?.content_type },
    ];

    if (transformation?.schedule) {
      fields.push({
        label: 'Schedule',
        value: transformation.schedule,
        variant: 'highlight',
        highlightColor: 'emerald',
      });
    }

    if (transformation?.metadata?.last_run_at) {
      fields.push({ label: 'Last Run', value: timeAgo(transformation.metadata.last_run_at) });
    }

    if (transformation?.metadata?.last_run_status) {
      fields.push({ label: 'Status', value: transformation.metadata.last_run_status });
    }

    return (
      <div>
        <BackToDashboardButton />
        <ModelHeader modelId={decodedId} modelType="scheduled" />
        <ModelInfoCard title="Transformation Details" fields={fields} borderColor="border-emerald-500/30" />
      </div>
    );
  }

  // Incremental transformation - show coverage
  const modelCoverage = coverage.data?.coverage.find(c => c.id === decodedId);

  // Build dependency tree (deduplicated) - recursively get all transitive dependencies
  const getDependencies = (modelId: string, visited = new Set<string>()): string[] => {
    if (visited.has(modelId)) return [];
    visited.add(modelId);

    // For the current model, use the already loaded transformation data
    // For dependencies, look them up in the allTransformations list
    let deps: string[] = [];
    if (modelId === decodedId) {
      deps = transformation?.depends_on || [];
    } else {
      const depModel = allTransformations.data?.models.find(m => m.id === modelId);
      deps = depModel?.depends_on || [];
    }

    const result = [...deps];
    for (const dep of deps) {
      result.push(...getDependencies(dep, visited));
    }

    return result;
  };

  const allDeps = transformation?.depends_on ? [...new Set(getDependencies(decodedId))] : [];

  // Calculate range from dependencies only (not the main model)
  // This allows zooming out to see what data is available vs what's been processed
  let globalMin = Infinity;
  let globalMax = -Infinity;

  allDeps.forEach(depId => {
    const depCoverage = coverage.data?.coverage.find(c => c.id === depId);
    const depBounds = allBounds.data?.bounds.find(b => b.id === depId);

    // Include transformation coverage ranges
    depCoverage?.ranges.forEach(range => {
      globalMin = Math.min(globalMin, range.position);
      globalMax = Math.max(globalMax, range.position + range.interval);
    });

    // Include external model bounds
    if (depBounds) {
      globalMin = Math.min(globalMin, depBounds.min);
      globalMax = Math.max(globalMax, depBounds.max);
    }
  });

  if (globalMin === Infinity) globalMin = 0;
  if (globalMax === -Infinity) globalMax = 100;

  const currentZoom = zoomRange || { start: globalMin, end: globalMax };

  // Get available transformations for this interval type
  const intervalType = transformation?.interval?.type || 'unknown';
  const transformations = intervalTypes.data?.interval_types?.[intervalType] || [];
  const currentTransformation: IntervalTypeTransformation | undefined = transformations[selectedTransformationIndex];

  const infoFields: InfoField[] = [
    { label: 'Database', value: model.database },
    { label: 'Table', value: model.table },
    { label: 'Type', value: transformation?.type, variant: 'highlight', highlightColor: 'indigo' },
    { label: 'Content Type', value: transformation?.content_type },
  ];

  return (
    <div>
      <BackToDashboardButton />
      <ModelHeader modelId={decodedId} modelType="incremental" />
      <div className="mb-6">
        <ModelInfoCard title="Model Information" fields={infoFields} borderColor="border-indigo-500/30" columns={4} />
      </div>

      <div className="rounded-2xl border border-indigo-500/30 bg-slate-800/80 p-6 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm">
        <div className="mb-6 flex items-center justify-between">
          <div className="flex items-center gap-4">
            <h2 className="text-lg font-bold text-slate-100">Coverage Analysis</h2>
            {/* Transformation selector buttons - only show if there are 2+ transformations */}
            {transformations.length > 1 && (
              <div className="flex gap-1 rounded-lg bg-slate-900/60 p-1 ring-1 ring-slate-700/50">
                {transformations.map((transformation, index) => (
                  <button
                    key={index}
                    onClick={() => setSelectedTransformationIndex(index)}
                    className={`rounded-md px-3 py-1 text-xs font-semibold transition-all ${
                      selectedTransformationIndex === index
                        ? 'bg-indigo-500 text-white shadow-sm'
                        : 'text-slate-400 hover:bg-slate-800 hover:text-slate-200'
                    }`}
                    title={transformation.expression || 'No transformation'}
                  >
                    {transformation.name}
                  </button>
                ))}
              </div>
            )}
          </div>
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

        {/* Main model coverage */}
        <div className="mb-6">
          <div className="mb-2 flex items-center justify-between">
            <span className="font-mono text-sm font-bold text-slate-200">{decodedId}</span>
            <span className="rounded-lg bg-slate-900/60 px-3 py-1 font-mono text-xs font-semibold text-slate-300">
              {currentTransformation
                ? `${formatValue(transformValue(currentZoom.start, currentTransformation), currentTransformation.format)} - ${formatValue(transformValue(currentZoom.end, currentTransformation), currentTransformation.format)}`
                : `${currentZoom.start.toLocaleString()} - ${currentZoom.end.toLocaleString()}`}
            </span>
          </div>
          <CoverageBar
            ranges={modelCoverage?.ranges}
            zoomStart={currentZoom.start}
            zoomEnd={currentZoom.end}
            type="transformation"
            height={96}
            transformation={currentTransformation}
            tooltipId="coverage-tooltip"
          />
        </div>

        {/* Dependencies */}
        {allDeps.length > 0 && (
          <div className="mt-8 border-t border-slate-700/50 pt-6">
            <h3 className="mb-4 text-base font-bold text-slate-100">
              Dependencies{' '}
              <span className="ml-2 rounded-full bg-slate-700 px-2 py-0.5 text-xs font-bold text-slate-300">
                {allDeps.length}
              </span>
            </h3>
            <div className="space-y-3">
              {allDeps.map(depId => {
                const depCoverage = coverage.data?.coverage.find(c => c.id === depId);
                const depBounds = allBounds.data?.bounds.find(b => b.id === depId);
                const isExternalDep = !depCoverage && depBounds;
                const isScheduledDep = !depCoverage && !depBounds; // No coverage and no bounds = scheduled transformation

                return (
                  <DependencyRow
                    key={depId}
                    dependencyId={depId}
                    type={isScheduledDep ? 'scheduled' : isExternalDep ? 'external' : 'transformation'}
                    ranges={depCoverage?.ranges}
                    bounds={depBounds ? { min: depBounds.min, max: depBounds.max } : undefined}
                    zoomStart={currentZoom.start}
                    zoomEnd={currentZoom.end}
                    transformation={currentTransformation}
                    tooltipId="coverage-tooltip"
                  />
                );
              })}
            </div>
          </div>
        )}

        <div className="mt-4 border-t border-slate-700/50 pt-4">
          <ZoomControls
            globalMin={globalMin}
            globalMax={globalMax}
            zoomStart={currentZoom.start}
            zoomEnd={currentZoom.end}
            transformation={currentTransformation}
            onZoomChange={(start, end) => setZoomRange({ start, end })}
            onResetZoom={() => setZoomRange(null)}
          />
        </div>
      </div>

      <Tooltip id="coverage-tooltip" className="!bg-gray-900 !text-white !text-xs !px-2 !py-1 !rounded !opacity-100" />
    </div>
  );
}

export const Route = createFileRoute('/model/$id')({
  component: ModelDetailComponent,
});
