import { type JSX, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Link } from '@tanstack/react-router';
import {
  listTransformationsOptions,
  listTransformationCoverageOptions,
  listExternalModelsOptions,
  listExternalBoundsOptions,
} from '@api/@tanstack/react-query.gen';
import { ArrowPathIcon, XCircleIcon, ArrowsPointingOutIcon } from '@heroicons/react/24/outline';
import type { ZoomRanges, IncrementalModelItem } from '@/types';
import { RangeSlider } from './RangeSlider';
import { Tooltip } from 'react-tooltip';

interface IncrementalModelsSectionProps {
  zoomRanges: ZoomRanges;
  onZoomChange: (intervalType: string, start: number, end: number) => void;
  onResetZoom: (intervalType: string) => void;
}

export function IncrementalModelsSection({
  zoomRanges,
  onZoomChange,
  onResetZoom,
}: IncrementalModelsSectionProps): JSX.Element {
  const [hoveredModel, setHoveredModel] = useState<string | null>(null);

  const incrementalTransformations = useQuery(listTransformationsOptions({ query: { type: 'incremental' } }));
  const coverage = useQuery(listTransformationCoverageOptions());
  const externalModels = useQuery(listExternalModelsOptions());
  const bounds = useQuery(listExternalBoundsOptions());

  const isLoading =
    incrementalTransformations.isLoading || coverage.isLoading || externalModels.isLoading || bounds.isLoading;

  const error = incrementalTransformations.error || coverage.error || externalModels.error || bounds.error;

  if (isLoading) {
    return (
      <div className="flex items-center gap-3 text-slate-400">
        <ArrowPathIcon className="h-5 w-5 animate-spin" />
        <span>Loading...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="rounded-lg border border-red-500/50 bg-red-950/80 p-4 text-red-200">
        <div className="flex items-center gap-2">
          <XCircleIcon className="h-5 w-5 shrink-0" />
          <span className="font-medium">Error: {error.message}</span>
        </div>
      </div>
    );
  }

  // Build dependency map for transformation models
  const dependencyMap = new Map<string, string[]>();
  incrementalTransformations.data?.models.forEach(model => {
    if (model.depends_on) {
      dependencyMap.set(model.id, model.depends_on);
    }
  });

  // Get all dependencies (including transitive) for a model
  const getAllDependencies = (modelId: string, visited = new Set<string>()): Set<string> => {
    if (visited.has(modelId)) return new Set();
    visited.add(modelId);

    const deps = new Set<string>();
    const directDeps = dependencyMap.get(modelId) || [];

    directDeps.forEach(dep => {
      deps.add(dep);
      const transDeps = getAllDependencies(dep, visited);
      transDeps.forEach(d => deps.add(d));
    });

    return deps;
  };

  // Determine which models should be highlighted
  const highlightedModels = new Set<string>();
  if (hoveredModel) {
    highlightedModels.add(hoveredModel);
    const deps = getAllDependencies(hoveredModel);
    deps.forEach(dep => highlightedModels.add(dep));
  }

  // Combine transformations and external models with type info
  const allModels: IncrementalModelItem[] = [];

  incrementalTransformations.data?.models.forEach(model => {
    const modelCoverage = coverage.data?.coverage.find(c => c.id === model.id);
    allModels.push({
      id: model.id,
      type: 'transformation',
      intervalType: model.interval?.type || 'unknown',
      data: {
        coverage: modelCoverage?.ranges,
      },
    });
  });

  externalModels.data?.models.forEach(model => {
    const modelBounds = bounds.data?.bounds.find(b => b.id === model.id);
    allModels.push({
      id: model.id,
      type: 'external',
      intervalType: model.interval?.type || 'unknown',
      data: {
        bounds: modelBounds ? { min: modelBounds.min, max: modelBounds.max } : undefined,
      },
    });
  });

  // Group by interval type
  const grouped = new Map<string, IncrementalModelItem[]>();
  allModels.forEach(model => {
    if (!grouped.has(model.intervalType)) {
      grouped.set(model.intervalType, []);
    }
    grouped.get(model.intervalType)!.push(model);
  });

  // Sort groups by size (largest first)
  const sortedGroups = Array.from(grouped.entries()).sort((a, b) => b[1].length - a[1].length);

  return (
    <div className="space-y-6">
      {sortedGroups.map(([intervalType, models]) => {
        // Sort models by ID within each group
        const sortedModels = [...models].sort((a, b) => a.id.localeCompare(b.id));

        // Calculate the min/max range for this interval type group
        let globalMin = Infinity;
        let globalMax = -Infinity;
        let transformationMin = Infinity;
        let transformationMax = -Infinity;

        sortedModels.forEach(model => {
          if (model.data.coverage) {
            model.data.coverage.forEach(range => {
              globalMin = Math.min(globalMin, range.position);
              globalMax = Math.max(globalMax, range.position + range.interval);
              transformationMin = Math.min(transformationMin, range.position);
              transformationMax = Math.max(transformationMax, range.position + range.interval);
            });
          }
          if (model.data.bounds) {
            globalMin = Math.min(globalMin, model.data.bounds.min);
            globalMax = Math.max(globalMax, model.data.bounds.max);
          }
        });

        // If no data, set reasonable defaults
        if (globalMin === Infinity) globalMin = 0;
        if (globalMax === -Infinity) globalMax = 100;
        if (transformationMin === Infinity) transformationMin = globalMin;
        if (transformationMax === -Infinity) transformationMax = globalMax;

        // Get or initialize zoom state for this interval type
        // Default to transformation bounds, not global bounds
        const currentZoom = zoomRanges[intervalType] || {
          start: transformationMin,
          end: transformationMax,
        };
        const zoomStart = currentZoom.start;
        const zoomEnd = currentZoom.end;
        const range = zoomEnd - zoomStart || 1;

        return (
          <div
            key={intervalType}
            className="group relative overflow-hidden rounded-2xl border border-indigo-500/30 bg-slate-800/80 p-6 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm transition-all duration-300 hover:shadow-lg hover:ring-indigo-500/50"
          >
            <div className="absolute inset-0 bg-gradient-to-br from-indigo-500/10 via-transparent to-purple-500/10 opacity-0 transition-opacity duration-300 group-hover:opacity-100" />
            <div className="relative">
              <div className="mb-6 flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <div className="rounded-lg bg-indigo-500/20 p-2 ring-1 ring-indigo-500/50">
                    <svg className="size-5 text-indigo-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        strokeWidth={2}
                        d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"
                      />
                    </svg>
                  </div>
                  <h3 className="bg-gradient-to-r from-indigo-400 to-purple-400 bg-clip-text text-xl font-black tracking-tight text-transparent">
                    {intervalType}
                  </h3>
                  <span className="rounded-full bg-indigo-500/20 px-2.5 py-0.5 text-xs font-bold text-indigo-300">
                    {sortedModels.length} models
                  </span>
                </div>
                <div className="flex items-center gap-3">
                  <button
                    onClick={() => onResetZoom(intervalType)}
                    disabled={zoomStart === globalMin && zoomEnd === globalMax}
                    className="group/btn rounded-lg bg-slate-700 px-3 py-2 text-slate-300 shadow-sm ring-1 ring-slate-600/50 transition-all hover:bg-indigo-500/20 hover:text-indigo-300 hover:ring-indigo-500/50 disabled:cursor-not-allowed disabled:opacity-40"
                    title="Reset Zoom"
                  >
                    <ArrowsPointingOutIcon className="size-4 transition-transform group-hover/btn:scale-110" />
                  </button>
                  <div className="rounded-lg bg-slate-900/60 px-4 py-2 font-mono text-xs font-semibold text-slate-300 ring-1 ring-slate-700/50">
                    {zoomStart.toLocaleString()} - {zoomEnd.toLocaleString()}
                  </div>
                </div>
              </div>
              <div className="space-y-2">
                {sortedModels.map(model => {
                  const isHighlighted = highlightedModels.has(model.id);
                  const isDimmed = hoveredModel !== null && !isHighlighted;

                  return (
                    <div
                      key={model.id}
                      className={`group/row flex items-center gap-3 rounded-lg p-2 transition-all ${
                        isHighlighted
                          ? 'bg-indigo-500/20 ring-2 ring-indigo-500/50'
                          : isDimmed
                            ? 'bg-slate-900/20 opacity-40'
                            : 'bg-slate-900/40 hover:bg-slate-900/60'
                      }`}
                      onMouseEnter={() => setHoveredModel(model.id)}
                      onMouseLeave={() => setHoveredModel(null)}
                    >
                      <div className="flex w-72 shrink-0 items-center">
                        <Link
                          to="/model/$id"
                          params={{ id: encodeURIComponent(model.id) }}
                          className={`truncate font-mono text-xs font-semibold transition-colors ${
                            isHighlighted
                              ? 'text-indigo-300'
                              : isDimmed
                                ? 'text-slate-500'
                                : 'text-slate-300 hover:text-indigo-400'
                          }`}
                          title={model.id}
                        >
                          {model.id}
                        </Link>
                      </div>
                      <div
                        className="relative flex-1 overflow-hidden rounded-md bg-slate-700 ring-1 ring-slate-600/50"
                        style={{ height: '24px' }}
                        onWheel={e => {
                          e.preventDefault();
                          const rect = e.currentTarget.getBoundingClientRect();
                          const mouseX = e.clientX - rect.left;
                          const position = mouseX / rect.width; // 0 to 1

                          const delta = e.deltaY;
                          const currentRange = zoomEnd - zoomStart;
                          const zoomFactor = delta > 0 ? 1.1 : 0.9; // Zoom out or in
                          const rangeChange = currentRange * (zoomFactor - 1);

                          let newStart = zoomStart;
                          let newEnd = zoomEnd;

                          if (position < 0.33) {
                            // Left third: only adjust max (right edge), unless at bound then adjust min
                            newEnd = Math.min(globalMax, Math.max(zoomStart + 1, zoomEnd + rangeChange));
                            // If we hit the max bound, adjust min instead
                            if (newEnd === globalMax && zoomEnd === globalMax) {
                              newStart = Math.max(globalMin, Math.min(zoomEnd - 1, zoomStart - rangeChange));
                              newEnd = zoomEnd;
                            }
                          } else if (position < 0.67) {
                            // Middle third: adjust both
                            const center = (zoomStart + zoomEnd) / 2;
                            const newRange = currentRange * zoomFactor;
                            newStart = Math.max(globalMin, center - newRange / 2);
                            newEnd = Math.min(globalMax, center + newRange / 2);
                          } else {
                            // Right third: only adjust min (left edge), unless at bound then adjust max
                            newStart = Math.max(globalMin, Math.min(zoomEnd - 1, zoomStart - rangeChange));
                            // If we hit the min bound, adjust max instead
                            if (newStart === globalMin && zoomStart === globalMin) {
                              newEnd = Math.min(globalMax, Math.max(zoomStart + 1, zoomEnd + rangeChange));
                              newStart = zoomStart;
                            }
                          }

                          onZoomChange(intervalType, newStart, newEnd);
                        }}
                      >
                        {model.type === 'transformation'
                          ? // Render coverage ranges (merge adjacent/overlapping chunks)
                            (() => {
                              const visibleRanges =
                                model.data.coverage?.filter(rangeData => {
                                  const rangeEnd = rangeData.position + rangeData.interval;
                                  return rangeEnd >= zoomStart && rangeData.position <= zoomEnd;
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

                              return merged.map((rangeData, idx) => {
                                const leftPercent = ((rangeData.position - zoomStart) / range) * 100;
                                const rightPercent =
                                  ((rangeData.position + rangeData.interval - zoomStart) / range) * 100;
                                const left = Math.max(0, leftPercent);
                                const right = Math.min(100, rightPercent);
                                const width = right - left;
                                const chunkMin = rangeData.position;
                                const chunkMax = rangeData.position + rangeData.interval;
                                return (
                                  <div
                                    key={idx}
                                    className="absolute h-full bg-indigo-600"
                                    style={{
                                      left: `${left}%`,
                                      width: `${width}%`,
                                    }}
                                    data-tooltip-id="chunk-tooltip"
                                    data-tooltip-content={`Min: ${chunkMin.toLocaleString()}, Max: ${chunkMax.toLocaleString()}`}
                                  />
                                );
                              });
                            })()
                          : // Render bounds as single continuous bar
                            model.data.bounds &&
                            model.data.bounds.max >= zoomStart &&
                            model.data.bounds.min <= zoomEnd && (
                              <div
                                className="absolute h-full bg-green-600"
                                style={{
                                  left: `${Math.max(0, ((model.data.bounds.min - zoomStart) / range) * 100)}%`,
                                  width: `${Math.min(100, ((Math.min(model.data.bounds.max, zoomEnd) - Math.max(model.data.bounds.min, zoomStart)) / range) * 100)}%`,
                                }}
                                data-tooltip-id="chunk-tooltip"
                                data-tooltip-content={`Min: ${model.data.bounds.min.toLocaleString()}, Max: ${model.data.bounds.max.toLocaleString()}`}
                              />
                            )}
                      </div>
                    </div>
                  );
                })}
              </div>

              {/* Range Slider */}
              <div className="mt-6 rounded-lg border border-slate-700/50 bg-slate-900/40 p-4">
                <div className="mb-2 flex items-center justify-between">
                  <span className="text-xs font-semibold text-slate-400">Zoom Range</span>
                  <span className="font-mono text-xs text-slate-500">
                    {globalMin.toLocaleString()} → {globalMax.toLocaleString()}
                  </span>
                </div>
                <RangeSlider
                  globalMin={globalMin}
                  globalMax={globalMax}
                  zoomStart={zoomStart}
                  zoomEnd={zoomEnd}
                  onZoomChange={(start, end) => onZoomChange(intervalType, start, end)}
                />
              </div>
            </div>
          </div>
        );
      })}
      <Tooltip
        id="chunk-tooltip"
        className="!bg-gray-900 !text-white !text-xs !px-2 !py-1 !rounded !opacity-100"
        place="top"
      />
    </div>
  );
}
