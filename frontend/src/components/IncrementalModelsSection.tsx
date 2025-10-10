import { type JSX, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  listTransformationsOptions,
  listTransformationCoverageOptions,
  listExternalModelsOptions,
  listExternalBoundsOptions,
  getIntervalTypesOptions,
} from '@api/@tanstack/react-query.gen';
import { ArrowPathIcon, XCircleIcon } from '@heroicons/react/24/outline';
import type { ZoomRanges, IncrementalModelItem } from '@/types';
import type { IntervalTypeTransformation } from '@api/types.gen';
import { Tooltip } from 'react-tooltip';
import { ModelCoverageRow } from './ModelCoverageRow';
import { ZoomControls } from './ZoomControls';
import { transformValue, formatValue } from '@/utils/interval-transform';

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
  // Track selected transformation index for each interval type
  const [selectedTransformations, setSelectedTransformations] = useState<Record<string, number>>({});

  // Fetch all transformations (polling handled at root level) and filter client-side
  const allTransformations = useQuery(listTransformationsOptions());
  const incrementalTransformations = {
    ...allTransformations,
    data: allTransformations.data
      ? { ...allTransformations.data, models: allTransformations.data.models.filter(m => m.type === 'incremental') }
      : undefined,
  };

  const coverage = useQuery(listTransformationCoverageOptions());
  const externalModels = useQuery(listExternalModelsOptions());
  const bounds = useQuery(listExternalBoundsOptions());

  // Fetch interval types (polling handled at root level)
  const intervalTypes = useQuery(getIntervalTypesOptions());

  const isLoading = allTransformations.isLoading || coverage.isLoading || externalModels.isLoading || bounds.isLoading;

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

        // Get available transformations for this interval type
        const transformations = intervalTypes.data?.interval_types?.[intervalType] || [];
        const selectedTransformationIndex = selectedTransformations[intervalType] ?? 0;
        const currentTransformation: IntervalTypeTransformation | undefined =
          transformations[selectedTransformationIndex];

        // Calculate the min/max range for this interval type group
        // globalMin/globalMax include both transformations and external models
        // transformationMin is for transformations only, transformationMax includes both
        let globalMin = Infinity;
        let globalMax = -Infinity;
        let transformationMin = Infinity;
        let transformationMax = -Infinity;

        sortedModels.forEach(model => {
          if (model.data.coverage) {
            // Transformation model
            model.data.coverage.forEach(range => {
              globalMin = Math.min(globalMin, range.position);
              globalMax = Math.max(globalMax, range.position + range.interval);
              transformationMin = Math.min(transformationMin, range.position);
              transformationMax = Math.max(transformationMax, range.position + range.interval);
            });
          }
          if (model.data.bounds) {
            // External model - only affects globalMin/globalMax and transformationMax
            globalMin = Math.min(globalMin, model.data.bounds.min);
            globalMax = Math.max(globalMax, model.data.bounds.max);
            transformationMax = Math.max(transformationMax, model.data.bounds.max);
          }
        });

        // If no data, set reasonable defaults
        if (globalMin === Infinity) globalMin = 0;
        if (globalMax === -Infinity) globalMax = 100;
        if (transformationMin === Infinity) transformationMin = globalMin;
        if (transformationMax === -Infinity) transformationMax = globalMax;

        // Get or initialize zoom state for this interval type
        // Default zoom: min from transformations only, max from transformations + external
        const currentZoom = zoomRanges[intervalType] || {
          start: transformationMin,
          end: transformationMax,
        };
        const zoomStart = currentZoom.start;
        const zoomEnd = currentZoom.end;

        return (
          <div
            key={intervalType}
            className="group relative overflow-hidden rounded-2xl border border-indigo-500/30 bg-slate-800/80 p-6 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm transition-all duration-300 hover:shadow-lg hover:ring-indigo-500/50"
          >
            <div className="absolute inset-0 bg-gradient-to-br from-indigo-500/10 via-transparent to-purple-500/10 opacity-0 transition-opacity duration-300 group-hover:opacity-100" />
            <div className="relative">
              <div className="mb-6 flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <h3 className="bg-gradient-to-r from-indigo-400 to-purple-400 bg-clip-text text-xl font-black tracking-tight text-transparent">
                    {intervalType}
                  </h3>
                  {/* Transformation selector buttons - only show if there are 2+ transformations */}
                  {transformations.length > 1 && (
                    <div className="flex gap-1 rounded-lg bg-slate-900/60 p-1 ring-1 ring-slate-700/50">
                      {transformations.map((transformation, index) => (
                        <button
                          key={index}
                          onClick={() =>
                            setSelectedTransformations(prev => ({
                              ...prev,
                              [intervalType]: index,
                            }))
                          }
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
                <div className="rounded-lg bg-slate-900/60 px-4 py-2 font-mono text-xs font-semibold text-slate-300 ring-1 ring-slate-700/50">
                  {currentTransformation
                    ? `${formatValue(transformValue(zoomStart, currentTransformation), currentTransformation.format)} - ${formatValue(transformValue(zoomEnd, currentTransformation), currentTransformation.format)}`
                    : `${zoomStart.toLocaleString()} - ${zoomEnd.toLocaleString()}`}
                </div>
              </div>
              <div className="space-y-2">
                {sortedModels.map(model => {
                  const isHighlighted = highlightedModels.has(model.id);
                  const isDimmed = hoveredModel !== null && !isHighlighted;

                  return (
                    <ModelCoverageRow
                      key={model.id}
                      model={model}
                      zoomStart={zoomStart}
                      zoomEnd={zoomEnd}
                      globalMin={globalMin}
                      globalMax={globalMax}
                      isHighlighted={isHighlighted}
                      isDimmed={isDimmed}
                      transformation={currentTransformation}
                      onMouseEnter={() => setHoveredModel(model.id)}
                      onMouseLeave={() => setHoveredModel(null)}
                      onZoomChange={(start, end) => onZoomChange(intervalType, start, end)}
                    />
                  );
                })}
              </div>

              {/* Zoom Controls */}
              <div className="mt-6">
                <ZoomControls
                  globalMin={globalMin}
                  globalMax={globalMax}
                  zoomStart={zoomStart}
                  zoomEnd={zoomEnd}
                  transformation={currentTransformation}
                  onZoomChange={(start, end) => onZoomChange(intervalType, start, end)}
                  onResetZoom={() => onResetZoom(intervalType)}
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
