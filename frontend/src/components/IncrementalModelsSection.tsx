import { type JSX, useState, useRef, useCallback } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  listTransformationsOptions,
  listTransformationCoverageOptions,
  listExternalModelsOptions,
  listExternalBoundsOptions,
  getIntervalTypesOptions,
} from '@api/@tanstack/react-query.gen';
import type { ZoomRanges, IncrementalModelItem } from '@/types';
import type { IntervalTypeTransformation } from '@api/types.gen';
import { Tooltip } from 'react-tooltip';
import { ModelCoverageRow } from './ModelCoverageRow';
import { ZoomControls } from './ZoomControls';
import { CoverageTooltip } from './CoverageTooltip';
import { LoadingState } from './shared/LoadingState';
import { ErrorState } from './shared/ErrorState';
import { TransformationSelector } from './shared/TransformationSelector';
import { transformValue, formatValue } from '@utils/interval-transform';
import { getOrderedDependencies } from '@utils/dependency-resolver';
import type { DependencyWithOrGroups } from '@utils/dependency-resolver';

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
  const [hoveredCoverage, setHoveredCoverage] = useState<{
    modelId: string;
    position: number;
    mouseX: number;
    intervalType: string;
  } | null>(null);
  // Track selected transformation index for each interval type
  const [selectedTransformations, setSelectedTransformations] = useState<Record<string, number>>({});
  const sectionRefs = useRef<Map<string, HTMLDivElement>>(new Map());
  // Track previous max values to detect when max increases
  const prevMaxValues = useRef<Map<string, number>>(new Map());

  // Memoize coverage hover handler to prevent creating new objects on every mousemove
  const handleCoverageHover = useCallback((modelId: string, position: number, mouseX: number, intervalType: string) => {
    setHoveredCoverage(prev => {
      // Only update if values actually changed to prevent unnecessary re-renders
      if (
        prev &&
        prev.modelId === modelId &&
        Math.abs(prev.position - position) < 0.01 &&
        Math.abs(prev.mouseX - mouseX) < 5 &&
        prev.intervalType === intervalType
      ) {
        return prev;
      }
      return { modelId, position, mouseX, intervalType };
    });
  }, []);

  const handleCoverageLeave = useCallback(() => {
    setHoveredCoverage(null);
  }, []);

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
    return <LoadingState />;
  }

  if (error) {
    return <ErrorState message={error.message} variant="compact" />;
  }

  // Build dependency map for transformation models with OR group support
  const dependencyMap = new Map<string, Array<string | string[]>>();
  incrementalTransformations.data?.models.forEach(model => {
    if (model.depends_on) {
      dependencyMap.set(model.id, model.depends_on);
    }
  });

  // Build OR group information for each model
  const modelOrGroupInfo = new Map<
    string,
    { dependencies: DependencyWithOrGroups[]; orGroupMembers: Map<number, string[]> }
  >();
  incrementalTransformations.data?.models.forEach(model => {
    if (model.depends_on) {
      const result = getOrderedDependencies(model.id, dependencyMap);
      modelOrGroupInfo.set(model.id, result);
    }
  });

  // Get all dependencies (including transitive) for a model - flattened for highlighting
  const getAllDependencies = (modelId: string, visited = new Set<string>()): Set<string> => {
    if (visited.has(modelId)) return new Set();
    visited.add(modelId);

    const deps = new Set<string>();
    const directDeps = dependencyMap.get(modelId) || [];

    // Flatten OR groups for dependency checking
    directDeps.forEach(dep => {
      if (typeof dep === 'string') {
        deps.add(dep);
      } else {
        dep.forEach(orDep => deps.add(orDep));
      }
    });

    // Recursively get transitive dependencies
    deps.forEach(dep => {
      const transDeps = getAllDependencies(dep, visited);
      transDeps.forEach(d => deps.add(d));
    });

    return deps;
  };

  // Determine which models should be highlighted
  const highlightedModels = new Set<string>();
  const activeModelId = hoveredCoverage?.modelId || hoveredModel;
  if (activeModelId) {
    highlightedModels.add(activeModelId);
    const deps = getAllDependencies(activeModelId);
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
      depends_on: model.depends_on,
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
        // Sort models: transformations first (alphabetically), then external models (alphabetically)
        const sortedModels = [...models].sort((a, b) => {
          // If types differ, external models go last
          if (a.type !== b.type) {
            return a.type === 'external' ? 1 : -1;
          }
          // Same type: sort alphabetically by ID
          return a.id.localeCompare(b.id);
        });

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
            // Skip uninitialized models (min == max == 0)
            if (!(model.data.bounds.min === 0 && model.data.bounds.max === 0)) {
              globalMin = Math.min(globalMin, model.data.bounds.min);
              globalMax = Math.max(globalMax, model.data.bounds.max);
              transformationMax = Math.max(transformationMax, model.data.bounds.max);
            }
          }
        });

        // If no data, set reasonable defaults
        if (!isFinite(globalMin)) globalMin = 0;
        if (!isFinite(globalMax)) globalMax = 100;
        if (!isFinite(transformationMin)) transformationMin = globalMin;
        if (!isFinite(transformationMax)) transformationMax = globalMax;

        // Additional safety: ensure min <= max
        if (globalMin > globalMax) {
          [globalMin, globalMax] = [0, 100];
        }
        if (transformationMin > transformationMax) {
          transformationMin = globalMin;
          transformationMax = globalMax;
        }

        // Get or initialize zoom state for this interval type
        // Default zoom: min from transformations only, max from transformations + external
        const currentZoom = zoomRanges[intervalType] || {
          start: transformationMin,
          end: transformationMax,
        };
        const zoomStart = currentZoom.start;
        let zoomEnd = currentZoom.end;

        // Auto-expand zoom if user is at the max and new data arrives
        const prevMax = prevMaxValues.current.get(intervalType);
        if (prevMax !== undefined && transformationMax > prevMax) {
          // Max increased - check if user was zoomed to the previous max
          const tolerance = 0.01; // Small tolerance for floating point comparison
          if (Math.abs(zoomEnd - prevMax) < tolerance) {
            // User was at the max, auto-expand to new max
            zoomEnd = transformationMax;
            onZoomChange(intervalType, zoomStart, zoomEnd);
          }
        }
        // Update the tracked max value
        prevMaxValues.current.set(intervalType, transformationMax);

        return (
          <div
            key={intervalType}
            ref={el => {
              if (el) sectionRefs.current.set(intervalType, el);
            }}
            className="group relative overflow-hidden rounded-2xl border border-indigo-500/30 bg-slate-800/80 p-4 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm transition-all duration-300 hover:shadow-lg hover:ring-indigo-500/50 sm:p-6"
          >
            <div className="absolute inset-0 bg-linear-to-br from-indigo-500/10 via-transparent to-purple-500/10 opacity-0 transition-opacity duration-300 group-hover:opacity-100" />
            <div className="relative">
              <div className="mb-4 flex flex-col gap-3 sm:mb-6 sm:flex-row sm:items-center sm:justify-between">
                <div className="flex flex-col gap-3 sm:flex-row sm:items-center">
                  <h3 className="bg-linear-to-r from-indigo-400 to-purple-400 bg-clip-text text-lg font-black tracking-tight text-transparent sm:text-xl">
                    {intervalType}
                  </h3>
                  <TransformationSelector
                    transformations={transformations}
                    selectedIndex={selectedTransformationIndex}
                    onSelect={index =>
                      setSelectedTransformations(prev => ({
                        ...prev,
                        [intervalType]: index,
                      }))
                    }
                  />
                </div>
                <div className="w-fit rounded-lg bg-slate-900/60 px-3 py-1.5 font-mono text-xs font-semibold text-slate-300 ring-1 ring-slate-700/50 sm:px-4 sm:py-2">
                  {currentTransformation
                    ? `${formatValue(transformValue(zoomStart, currentTransformation), currentTransformation.format)} - ${formatValue(transformValue(zoomEnd, currentTransformation), currentTransformation.format)}`
                    : `${zoomStart.toLocaleString()} - ${zoomEnd.toLocaleString()}`}
                </div>
              </div>
              <div className="space-y-1.5">
                {sortedModels.map(model => {
                  const isHighlighted = highlightedModels.has(model.id);
                  const isDimmed = (hoveredModel !== null || hoveredCoverage !== null) && !isHighlighted;

                  // Find which OR groups this dependency belongs to when another model is hovered
                  let orGroupsForDep: number[] | undefined;
                  if (activeModelId && activeModelId !== model.id) {
                    const parentInfo = modelOrGroupInfo.get(activeModelId);
                    if (parentInfo) {
                      const depInfo = parentInfo.dependencies.find(d => d.id === model.id);
                      if (depInfo && depInfo.orGroups.length > 0) {
                        orGroupsForDep = depInfo.orGroups;
                      }
                    }
                  }

                  return (
                    <ModelCoverageRow
                      key={model.id}
                      model={model}
                      zoomStart={zoomStart}
                      zoomEnd={zoomEnd}
                      isHighlighted={isHighlighted}
                      isDimmed={isDimmed}
                      transformation={currentTransformation}
                      orGroups={orGroupsForDep}
                      onMouseEnter={() => setHoveredModel(model.id)}
                      onMouseLeave={() => setHoveredModel(null)}
                      onCoverageHover={(modelId, position, mouseX) =>
                        handleCoverageHover(modelId, position, mouseX, intervalType)
                      }
                      onCoverageLeave={handleCoverageLeave}
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

            {/* Coverage tooltip for this interval type */}
            {hoveredCoverage && hoveredCoverage.intervalType === intervalType && (
              <CoverageTooltip
                hoveredPosition={hoveredCoverage.position}
                mouseX={hoveredCoverage.mouseX}
                hoveredModelId={hoveredCoverage.modelId}
                allModels={sortedModels}
                dependencyIds={getAllDependencies(hoveredCoverage.modelId)}
                transformation={currentTransformation}
                zoomStart={zoomStart}
                zoomEnd={zoomEnd}
                containerRef={{
                  current: sectionRefs.current.get(intervalType) || null,
                }}
              />
            )}
          </div>
        );
      })}
      <Tooltip id="coverage-tooltip" className="!bg-gray-900 !text-white !text-xs !px-2 !py-1 !rounded !opacity-100" />
    </div>
  );
}
