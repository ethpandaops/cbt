import { type JSX, useState } from 'react';
import { createFileRoute, Link } from '@tanstack/react-router';
import { useQuery } from '@tanstack/react-query';
import {
  listAllModelsOptions,
  getExternalModelOptions,
  getExternalBoundsOptions,
  listExternalBoundsOptions,
  getTransformationOptions,
  listTransformationCoverageOptions,
  listTransformationsOptions,
} from '@api/@tanstack/react-query.gen';
import { ArrowPathIcon, XCircleIcon, ArrowLeftIcon } from '@heroicons/react/24/outline';
import { RangeSlider } from '@/components/RangeSlider';
import { Tooltip } from 'react-tooltip';
import { timeAgo } from '@/utils/time';

function ModelDetailComponent(): JSX.Element {
  const { id } = Route.useParams();
  const decodedId = decodeURIComponent(id);

  // Load all models to determine type
  const allModels = useQuery(listAllModelsOptions());
  const model = allModels.data?.models.find(m => m.id === decodedId);

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

    return (
      <div>
        <Link
          to="/"
          className="group mb-6 inline-flex items-center gap-2 rounded-lg bg-slate-800/60 px-4 py-2 text-sm font-medium text-slate-300 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm transition-all hover:bg-slate-800/80 hover:text-indigo-400 hover:shadow-md hover:ring-indigo-500/50"
        >
          <ArrowLeftIcon className="size-4 transition-transform group-hover:-translate-x-0.5" />
          Back to Dashboard
        </Link>

        <div className="mb-6 flex items-baseline gap-4">
          <h1 className="bg-gradient-to-r from-green-400 via-emerald-400 to-green-400 bg-clip-text text-4xl font-black tracking-tight text-transparent">
            {decodedId}
          </h1>
          <span className="rounded-full bg-green-500/20 px-3 py-1 text-xs font-bold text-green-300 ring-1 ring-green-500/50">
            EXTERNAL
          </span>
        </div>

        <div className="rounded-2xl border border-green-500/30 bg-slate-800/80 p-6 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm">
          <h2 className="mb-4 text-lg font-bold text-slate-100">Model Information</h2>
          <dl className="grid grid-cols-2 gap-x-6 gap-y-4 text-sm">
            <div className="rounded-lg bg-slate-900/60 p-4">
              <dt className="mb-1 font-semibold text-slate-400">Database</dt>
              <dd className="font-mono text-base font-bold text-slate-100">{model.database}</dd>
            </div>
            <div className="rounded-lg bg-slate-900/60 p-4">
              <dt className="mb-1 font-semibold text-slate-400">Table</dt>
              <dd className="font-mono text-base font-bold text-slate-100">{model.table}</dd>
            </div>
            {externalModel.data?.interval?.type && (
              <div className="rounded-lg bg-slate-900/60 p-4">
                <dt className="mb-1 font-semibold text-slate-400">Interval Type</dt>
                <dd className="font-mono text-base font-bold text-slate-100">{externalModel.data.interval.type}</dd>
              </div>
            )}
            {bounds && (
              <>
                <div className="rounded-lg bg-green-500/10 p-4 ring-1 ring-green-500/50">
                  <dt className="mb-1 font-semibold text-green-400">Min Position</dt>
                  <dd className="font-mono text-base font-bold text-green-200">{bounds.min.toLocaleString()}</dd>
                </div>
                <div className="rounded-lg bg-green-500/10 p-4 ring-1 ring-green-500/50">
                  <dt className="mb-1 font-semibold text-green-400">Max Position</dt>
                  <dd className="font-mono text-base font-bold text-green-200">{bounds.max.toLocaleString()}</dd>
                </div>
              </>
            )}
          </dl>
        </div>
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
    return (
      <div>
        <Link
          to="/"
          className="group mb-6 inline-flex items-center gap-2 rounded-lg bg-slate-800/60 px-4 py-2 text-sm font-medium text-slate-300 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm transition-all hover:bg-slate-800/80 hover:text-indigo-400 hover:shadow-md hover:ring-indigo-500/50"
        >
          <ArrowLeftIcon className="size-4 transition-transform group-hover:-translate-x-0.5" />
          Back to Dashboard
        </Link>

        <div className="mb-6 flex items-baseline gap-4">
          <h1 className="bg-gradient-to-r from-emerald-400 via-teal-400 to-emerald-400 bg-clip-text text-4xl font-black tracking-tight text-transparent">
            {decodedId}
          </h1>
          <span className="rounded-full bg-emerald-500/20 px-3 py-1 text-xs font-bold text-emerald-300 ring-1 ring-emerald-500/50">
            SCHEDULED
          </span>
        </div>

        <div className="rounded-2xl border border-emerald-500/30 bg-slate-800/80 p-6 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm">
          <h2 className="mb-4 text-lg font-bold text-slate-100">Transformation Details</h2>
          <dl className="grid grid-cols-2 gap-x-6 gap-y-4 text-sm">
            <div className="rounded-lg bg-slate-900/60 p-4">
              <dt className="mb-1 font-semibold text-slate-400">Database</dt>
              <dd className="font-mono text-base font-bold text-slate-100">{model.database}</dd>
            </div>
            <div className="rounded-lg bg-slate-900/60 p-4">
              <dt className="mb-1 font-semibold text-slate-400">Table</dt>
              <dd className="font-mono text-base font-bold text-slate-100">{model.table}</dd>
            </div>
            <div className="rounded-lg bg-slate-900/60 p-4">
              <dt className="mb-1 font-semibold text-slate-400">Content Type</dt>
              <dd className="font-mono text-base font-bold text-slate-100">{transformation?.content_type}</dd>
            </div>
            {transformation?.schedule && (
              <div className="rounded-lg bg-emerald-500/10 p-4 ring-1 ring-emerald-500/50">
                <dt className="mb-1 font-semibold text-emerald-400">Schedule</dt>
                <dd className="font-mono text-base font-bold text-emerald-200">{transformation.schedule}</dd>
              </div>
            )}
            {transformation?.metadata?.last_run_at && (
              <div className="rounded-lg bg-slate-900/60 p-4">
                <dt className="mb-1 font-semibold text-slate-400">Last Run</dt>
                <dd className="font-mono text-base font-bold text-slate-100">
                  {timeAgo(transformation.metadata.last_run_at)}
                </dd>
              </div>
            )}
            {transformation?.metadata?.last_run_status && (
              <div className="rounded-lg bg-slate-900/60 p-4">
                <dt className="mb-1 font-semibold text-slate-400">Status</dt>
                <dd className="font-mono text-base font-bold text-slate-100">
                  {transformation.metadata.last_run_status}
                </dd>
              </div>
            )}
          </dl>
        </div>
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
  const range = currentZoom.end - currentZoom.start || 1;

  return (
    <div>
      <Link
        to="/"
        className="group mb-6 inline-flex items-center gap-2 rounded-lg bg-slate-800/60 px-4 py-2 text-sm font-medium text-slate-300 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm transition-all hover:bg-slate-800/80 hover:text-indigo-400 hover:shadow-md hover:ring-indigo-500/50"
      >
        <ArrowLeftIcon className="size-4 transition-transform group-hover:-translate-x-0.5" />
        Back to Dashboard
      </Link>

      <div className="mb-6 flex items-baseline gap-4">
        <h1 className="bg-gradient-to-r from-indigo-400 via-purple-400 to-indigo-400 bg-clip-text text-4xl font-black tracking-tight text-transparent">
          {decodedId}
        </h1>
        <span className="rounded-full bg-indigo-500/20 px-3 py-1 text-xs font-bold text-indigo-300 ring-1 ring-indigo-500/50">
          INCREMENTAL
        </span>
      </div>

      <div className="mb-6 rounded-2xl border border-indigo-500/30 bg-slate-800/80 p-6 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm">
        <h2 className="mb-4 text-lg font-bold text-slate-100">Model Information</h2>
        <dl className="grid grid-cols-2 gap-x-6 gap-y-4 text-sm lg:grid-cols-4">
          <div className="rounded-lg bg-slate-900/60 p-4">
            <dt className="mb-1 font-semibold text-slate-400">Database</dt>
            <dd className="font-mono text-base font-bold text-slate-100">{model.database}</dd>
          </div>
          <div className="rounded-lg bg-slate-900/60 p-4">
            <dt className="mb-1 font-semibold text-slate-400">Table</dt>
            <dd className="font-mono text-base font-bold text-slate-100">{model.table}</dd>
          </div>
          <div className="rounded-lg bg-indigo-500/10 p-4 ring-1 ring-indigo-500/50">
            <dt className="mb-1 font-semibold text-indigo-400">Type</dt>
            <dd className="font-mono text-base font-bold text-indigo-200">{transformation?.type}</dd>
          </div>
          <div className="rounded-lg bg-slate-900/60 p-4">
            <dt className="mb-1 font-semibold text-slate-400">Content Type</dt>
            <dd className="font-mono text-base font-bold text-slate-100">{transformation?.content_type}</dd>
          </div>
        </dl>
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

        {/* Main model coverage */}
        <div className="mb-6">
          <div className="mb-2 flex items-center justify-between">
            <span className="font-mono text-sm font-bold text-slate-200">{decodedId}</span>
            <span className="rounded-lg bg-slate-900/60 px-3 py-1 font-mono text-xs font-semibold text-slate-300">
              {currentZoom.start.toLocaleString()} - {currentZoom.end.toLocaleString()}
            </span>
          </div>
          <div className="relative h-24 overflow-hidden rounded-lg bg-slate-700 ring-1 ring-slate-600/50">
            {(() => {
              // Filter visible ranges
              const visibleRanges =
                modelCoverage?.ranges.filter(r => {
                  const rangeEnd = r.position + r.interval;
                  return rangeEnd >= currentZoom.start && r.position <= currentZoom.end;
                }) || [];

              // Sort by position
              const sorted = [...visibleRanges].sort((a, b) => a.position - b.position);

              // Merge adjacent/overlapping ranges
              const merged: Array<{ position: number; interval: number }> = [];
              for (const curr of sorted) {
                if (merged.length === 0) {
                  merged.push({ ...curr });
                } else {
                  const last = merged[merged.length - 1];
                  const lastEnd = last.position + last.interval;
                  const currEnd = curr.position + curr.interval;

                  // If current range overlaps or is adjacent to last, merge them
                  if (curr.position <= lastEnd) {
                    last.interval = Math.max(lastEnd, currEnd) - last.position;
                  } else {
                    merged.push({ ...curr });
                  }
                }
              }

              return merged.map((r, idx) => {
                const leftPercent = ((r.position - currentZoom.start) / range) * 100;
                const rightPercent = ((r.position + r.interval - currentZoom.start) / range) * 100;
                const left = Math.max(0, leftPercent);
                const right = Math.min(100, rightPercent);
                const width = right - left;

                return (
                  <div
                    key={idx}
                    className="absolute h-full bg-indigo-600"
                    style={{ left: `${left}%`, width: `${width}%` }}
                    data-tooltip-id="coverage-tooltip"
                    data-tooltip-content={`Min: ${r.position.toLocaleString()}, Max: ${(r.position + r.interval).toLocaleString()}`}
                  />
                );
              });
            })()}
          </div>
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
                  <div key={depId}>
                    <div className="mb-1.5 flex items-center gap-2">
                      <Link
                        to="/model/$id"
                        params={{ id: encodeURIComponent(depId) }}
                        className="font-mono text-xs font-semibold text-slate-300 transition-colors hover:text-indigo-400 hover:underline"
                      >
                        {depId}
                      </Link>
                      {isExternalDep && (
                        <span className="rounded-full bg-green-500/20 px-2 py-0.5 text-xs font-bold text-green-300">
                          EXT
                        </span>
                      )}
                      {isScheduledDep && (
                        <span className="rounded-full bg-emerald-500/20 px-2 py-0.5 text-xs font-bold text-emerald-300">
                          SCHEDULED
                        </span>
                      )}
                    </div>
                    <div
                      className={`relative h-12 overflow-hidden rounded-lg ring-1 ${isScheduledDep ? 'bg-slate-800/50 ring-slate-700/30' : 'bg-slate-700 ring-slate-600/50'}`}
                    >
                      {isScheduledDep ? (
                        // Scheduled transformation - show grayed out with message
                        <div className="flex h-full items-center justify-center">
                          <span className="text-xs font-medium italic text-slate-500">Always available</span>
                        </div>
                      ) : isExternalDep && depBounds ? (
                        // Render external model bounds as green bar
                        <div
                          className="absolute h-full bg-green-600"
                          style={{
                            left: `${Math.max(0, ((depBounds.min - currentZoom.start) / range) * 100)}%`,
                            width: `${Math.min(100, ((Math.min(depBounds.max, currentZoom.end) - Math.max(depBounds.min, currentZoom.start)) / range) * 100)}%`,
                          }}
                          data-tooltip-id="coverage-tooltip"
                          data-tooltip-content={`Min: ${depBounds.min.toLocaleString()}, Max: ${depBounds.max.toLocaleString()}`}
                        />
                      ) : (
                        // Render transformation coverage ranges (merge adjacent/overlapping chunks)
                        (() => {
                          // Filter visible ranges
                          const visibleRanges =
                            depCoverage?.ranges.filter(r => {
                              const rangeEnd = r.position + r.interval;
                              return rangeEnd >= currentZoom.start && r.position <= currentZoom.end;
                            }) || [];

                          // Sort by position
                          const sorted = [...visibleRanges].sort((a, b) => a.position - b.position);

                          // Merge adjacent/overlapping ranges
                          const merged: Array<{ position: number; interval: number }> = [];
                          for (const curr of sorted) {
                            if (merged.length === 0) {
                              merged.push({ ...curr });
                            } else {
                              const last = merged[merged.length - 1];
                              const lastEnd = last.position + last.interval;
                              const currEnd = curr.position + curr.interval;

                              // If current range overlaps or is adjacent to last, merge them
                              if (curr.position <= lastEnd) {
                                last.interval = Math.max(lastEnd, currEnd) - last.position;
                              } else {
                                merged.push({ ...curr });
                              }
                            }
                          }

                          return merged.map((r, idx) => {
                            const leftPercent = ((r.position - currentZoom.start) / range) * 100;
                            const rightPercent = ((r.position + r.interval - currentZoom.start) / range) * 100;
                            const left = Math.max(0, leftPercent);
                            const right = Math.min(100, rightPercent);
                            const width = right - left;

                            return (
                              <div
                                key={idx}
                                className="absolute h-full bg-indigo-400"
                                style={{ left: `${left}%`, width: `${width}%` }}
                                data-tooltip-id="coverage-tooltip"
                                data-tooltip-content={`Min: ${r.position.toLocaleString()}, Max: ${(r.position + r.interval).toLocaleString()}`}
                              />
                            );
                          });
                        })()
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}

        <div className="mt-4 border-t border-gray-200 pt-4">
          <RangeSlider
            globalMin={globalMin}
            globalMax={globalMax}
            zoomStart={currentZoom.start}
            zoomEnd={currentZoom.end}
            onZoomChange={(start, end) => setZoomRange({ start, end })}
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
