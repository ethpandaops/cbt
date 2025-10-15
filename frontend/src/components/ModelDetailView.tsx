import { type JSX, useState, useRef, useMemo, useEffect } from 'react';
import type { UseQueryResult } from '@tanstack/react-query';
import type {
  IntervalTypeTransformation,
  TransformationModel,
  ListTransformationCoverageResponse,
  ListExternalBoundsResponse,
  ListTransformationsResponse,
  GetIntervalTypesResponse,
} from '@api/types.gen';
import { ReactFlowProvider } from '@xyflow/react';
import { ArrowsPointingOutIcon, XMarkIcon } from '@heroicons/react/24/outline';
import { ModelInfoCard, type InfoField } from './ModelInfoCard';
import { DependencyRow } from './DependencyRow';
import { CoverageBar } from './CoverageBar';
import { ZoomControls } from './ZoomControls';
import { CoverageTooltip } from './CoverageTooltip';
import { CoverageDebugDialog } from './CoverageDebugDialog';
import { SQLCodeBlock } from './SQLCodeBlock';
import { TransformationSelector } from './shared/TransformationSelector';
import { ZoomPresets } from './ZoomPresets';
import { DagGraph, type DagData } from './DagGraph';
import type { IncrementalModelItem } from '@/types';
import { getOrderedDependencies } from '@utils/dependency-resolver';
import {
  calculateDefaultZoomRange,
  calculateZoomRangeForWindow,
  getZoomInScale,
  getZoomOutScale,
} from '@utils/zoom-helpers';
import { useTransformationSelection } from '@hooks/useTransformationSelection';

export interface ModelDetailViewProps {
  decodedId: string;
  transformation: TransformationModel;
  coverage: UseQueryResult<ListTransformationCoverageResponse, Error>;
  allBounds: UseQueryResult<ListExternalBoundsResponse, Error>;
  allTransformations: UseQueryResult<ListTransformationsResponse, Error>;
  intervalTypes: UseQueryResult<GetIntervalTypesResponse, Error>;
}

export function ModelDetailView({
  decodedId,
  transformation,
  coverage,
  allBounds,
  allTransformations,
  intervalTypes,
}: ModelDetailViewProps): JSX.Element {
  const [hoveredCoverage, setHoveredCoverage] = useState<{ modelId: string; position: number; mouseX: number } | null>(
    null
  );
  const [zoomRange, setZoomRange] = useState<{ start: number; end: number } | null>(null);
  const [hoveredOrGroup, setHoveredOrGroup] = useState<number | null>(null);
  const [fullscreenSection, setFullscreenSection] = useState<'dag' | 'coverage' | null>(null);
  const [fitViewTrigger, setFitViewTrigger] = useState(0);
  const [debugPosition, setDebugPosition] = useState<{ modelId: string; position: number } | null>(null);
  const sectionRef = useRef<HTMLDivElement>(null);
  // Persistent transformation selection hook
  const { getSelectedIndex, setSelectedIndex } = useTransformationSelection();

  // Handle escape key to exit fullscreen
  useEffect(() => {
    const handleEscape = (e: KeyboardEvent): void => {
      if (e.key === 'Escape' && fullscreenSection) {
        setFullscreenSection(null);
      }
    };
    window.addEventListener('keydown', handleEscape);
    return () => window.removeEventListener('keydown', handleEscape);
  }, [fullscreenSection]);

  // Trigger fitView when entering or exiting fullscreen for DAG
  useEffect(() => {
    // Increment trigger counter whenever fullscreen state changes (entering or exiting)
    setFitViewTrigger(prev => prev + 1);
  }, [fullscreenSection]);

  const modelCoverage = coverage.data?.coverage.find(c => c.id === decodedId);

  // Build dependency map with OR group support
  const dependencyMap = useMemo(() => {
    const map = new Map<string, Array<string | string[]>>();
    if (transformation?.depends_on) {
      map.set(decodedId, transformation.depends_on);
    }

    // Add all transformations to dependency map for recursive resolution
    allTransformations.data?.models.forEach(model => {
      if (model.depends_on) {
        map.set(model.id, model.depends_on);
      }
    });

    return map;
  }, [decodedId, transformation?.depends_on, allTransformations.data?.models]);

  // Get ordered dependencies with OR group tracking
  const { dependencies: orderedDeps, orGroupMembers } = useMemo(
    () => getOrderedDependencies(decodedId, dependencyMap),
    [decodedId, dependencyMap]
  );

  // Create a map of all dependency IDs for quick lookup (used by CoverageTooltip)
  const getAllDependencyIds = useMemo(() => {
    const buildDependencySet = (modelId: string, visited = new Set<string>()): Set<string> => {
      if (visited.has(modelId)) return new Set();
      visited.add(modelId);

      const deps = new Set<string>();
      const directDeps = dependencyMap.get(modelId) || [];

      // Flatten OR groups
      directDeps.forEach(dep => {
        if (typeof dep === 'string') {
          deps.add(dep);
        } else {
          dep.forEach(orDep => deps.add(orDep));
        }
      });

      // Recursively get transitive dependencies
      deps.forEach(dep => {
        const transDeps = buildDependencySet(dep, new Set(visited));
        transDeps.forEach(d => deps.add(d));
      });

      return deps;
    };

    return (modelId: string) => buildDependencySet(modelId);
  }, [dependencyMap]);

  // Build DAG data for visualization (only if this is an incremental model)
  const dagData = useMemo<DagData | null>(() => {
    if (transformation?.type !== 'incremental') return null;

    const externalModels: DagData['externalModels'] = [];
    const incrementalModels: DagData['incrementalModels'] = [];

    // Add the current model
    incrementalModels.push(transformation);

    // Add all dependencies
    orderedDeps.forEach(dep => {
      const depModel = allTransformations.data?.models.find(m => m.id === dep.id);
      const depBounds = allBounds.data?.bounds.find(b => b.id === dep.id);

      if (depModel && depModel.type === 'incremental') {
        incrementalModels.push(depModel);
      } else if (depBounds) {
        // External model with bounds but not in transformations list
        externalModels.push({
          id: dep.id,
          database: dep.id.split('.')[0],
          table: dep.id.split('.').slice(1).join('.'),
        });
      }
    });

    return {
      externalModels,
      incrementalModels,
      scheduledModels: [],
      bounds: allBounds.data?.bounds,
      coverage: coverage.data?.coverage,
      intervalTypes: intervalTypes.data?.interval_types,
    };
  }, [transformation, orderedDeps, allTransformations.data, allBounds.data, coverage.data, intervalTypes.data]);

  // Build all models array for CoverageTooltip
  const allModelsForTooltip: IncrementalModelItem[] = [];
  if (modelCoverage) {
    allModelsForTooltip.push({
      id: decodedId,
      type: 'transformation',
      intervalType: transformation?.interval?.type || 'unknown',
      depends_on: transformation?.depends_on,
      data: {
        coverage: modelCoverage.ranges,
      },
    });
  }

  orderedDeps.forEach(dep => {
    const depCoverage = coverage.data?.coverage.find(c => c.id === dep.id);
    const depBounds = allBounds.data?.bounds.find(b => b.id === dep.id);
    const depModel = allTransformations.data?.models.find(m => m.id === dep.id);

    if (depCoverage) {
      allModelsForTooltip.push({
        id: dep.id,
        type: 'transformation',
        intervalType: depModel?.interval?.type || 'unknown',
        depends_on: depModel?.depends_on,
        data: {
          coverage: depCoverage.ranges,
        },
      });
    } else if (depBounds) {
      allModelsForTooltip.push({
        id: dep.id,
        type: 'external',
        intervalType: 'unknown',
        data: {
          bounds: { min: depBounds.min, max: depBounds.max },
        },
      });
    }
  });

  // Calculate range using same logic as IncrementalModelsSection
  // globalMin/globalMax include all models (this model + dependencies)
  // transformationMin is for transformations only, transformationMax includes both
  let globalMin = Infinity;
  let globalMax = -Infinity;
  let transformationMin = Infinity;
  let transformationMax = -Infinity;

  // Include the current model's coverage in the calculation
  if (modelCoverage) {
    modelCoverage.ranges.forEach(range => {
      globalMin = Math.min(globalMin, range.position);
      globalMax = Math.max(globalMax, range.position + range.interval);
      transformationMin = Math.min(transformationMin, range.position);
      transformationMax = Math.max(transformationMax, range.position + range.interval);
    });
  }

  // Include dependencies
  orderedDeps.forEach(dep => {
    const depCoverage = coverage.data?.coverage.find(c => c.id === dep.id);
    const depBounds = allBounds.data?.bounds.find(b => b.id === dep.id);

    if (depCoverage) {
      // Transformation dependency
      depCoverage.ranges.forEach(range => {
        globalMin = Math.min(globalMin, range.position);
        globalMax = Math.max(globalMax, range.position + range.interval);
        transformationMin = Math.min(transformationMin, range.position);
        transformationMax = Math.max(transformationMax, range.position + range.interval);
      });
    }

    if (depBounds) {
      // External model - only affects globalMin/globalMax and transformationMax
      // Skip uninitialized models (min == max == 0)
      if (!(depBounds.min === 0 && depBounds.max === 0)) {
        globalMin = Math.min(globalMin, depBounds.min);
        globalMax = Math.max(globalMax, depBounds.max);
        transformationMax = Math.max(transformationMax, depBounds.max);
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

  // Get or initialize zoom state
  const currentZoom = zoomRange || calculateDefaultZoomRange(transformationMin, transformationMax);

  // Check if data is available (same logic as ZoomControls and IncrementalModelsSection)
  const hasData = !((globalMin === 0 && globalMax === 0) || (globalMin === 0 && globalMax === 100));

  const intervalType = transformation?.interval?.type || 'unknown';
  const transformations = intervalTypes.data?.interval_types?.[intervalType] || [];
  const selectedTransformationIndex = getSelectedIndex(intervalType, transformations);
  const currentTransformation: IntervalTypeTransformation | undefined = transformations[selectedTransformationIndex];

  const infoFields: InfoField[] = [
    { label: 'Database', value: decodedId.split('.')[0] },
    { label: 'Table', value: decodedId.split('.').slice(1).join('.') },
    { label: 'Type', value: transformation?.type, variant: 'highlight', highlightColor: 'indigo' },
    { label: 'Content Type', value: transformation?.content_type, variant: 'highlight', highlightColor: 'indigo' },
  ];

  // Add interval type if available - use transformation name if available, otherwise fall back to interval.type
  if (transformation?.interval?.type) {
    const transformationName = currentTransformation?.name;
    infoFields.push({
      label: 'Interval Type',
      value: transformationName || transformation.interval.type,
    });
  }

  if (transformation?.description) {
    infoFields.push({ label: 'Description', value: transformation.description });
  }

  if (transformation?.tags && transformation.tags.length > 0) {
    infoFields.push({ label: 'Tags', value: transformation.tags.join(', ') });
  }

  if (transformation?.interval?.min !== undefined && transformation?.interval?.max !== undefined) {
    infoFields.push(
      { label: 'Min Interval', value: transformation.interval.min.toString() },
      { label: 'Max Interval', value: transformation.interval.max.toString() }
    );
  }

  if (transformation?.schedules) {
    if (transformation.schedules.forwardfill) {
      infoFields.push({
        label: 'Forwardfill Schedule',
        value: transformation.schedules.forwardfill,
        variant: 'highlight',
        highlightColor: 'emerald',
      });
    }
    if (transformation.schedules.backfill) {
      infoFields.push({
        label: 'Backfill Schedule',
        value: transformation.schedules.backfill,
        variant: 'highlight',
        highlightColor: 'amber',
      });
    }
  }

  return (
    <div className="space-y-6">
      <ModelInfoCard title="Model Information" fields={infoFields} borderColor="border-indigo-500/30" columns={4} />

      {/* DAG Visualization - only for incremental models with dependencies */}
      {dagData && orderedDeps.length > 0 && (
        <div
          className={`rounded-2xl border border-indigo-500/30 bg-slate-800/80 p-4 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm sm:p-6 ${
            fullscreenSection === 'dag' ? 'fixed inset-0 z-50 m-0 rounded-none' : ''
          }`}
        >
          <div className="mb-4 flex items-center justify-between">
            <h2 className="text-base font-bold text-slate-100 sm:text-lg">Dependency Graph</h2>
            <button
              onClick={() => setFullscreenSection(fullscreenSection === 'dag' ? null : 'dag')}
              className="rounded-lg p-2 text-slate-400 transition-colors hover:bg-slate-700/60 hover:text-slate-200"
              title={fullscreenSection === 'dag' ? 'Exit fullscreen (Esc)' : 'Fullscreen'}
            >
              {fullscreenSection === 'dag' ? (
                <XMarkIcon className="size-5" />
              ) : (
                <ArrowsPointingOutIcon className="size-5" />
              )}
            </button>
          </div>
          <ReactFlowProvider>
            <div className={fullscreenSection === 'dag' ? 'h-[calc(100vh-120px)] w-full' : 'h-[500px] w-full'}>
              <DagGraph data={dagData} className="h-full" triggerFitView={fitViewTrigger} />
            </div>
          </ReactFlowProvider>
        </div>
      )}

      <div
        ref={sectionRef}
        className="rounded-2xl border border-indigo-500/30 bg-slate-800/80 p-4 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm sm:p-6"
      >
        <div className="mb-4 flex flex-col gap-4 sm:mb-6">
          <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:gap-4">
              <h2 className="text-base font-bold text-slate-100 sm:text-lg">Coverage Analysis</h2>
              <TransformationSelector
                transformations={transformations}
                selectedIndex={selectedTransformationIndex}
                onSelect={index => setSelectedIndex(intervalType, index)}
              />
            </div>
            <ZoomPresets
              onPresetClick={presetId => {
                let newStart: number;
                let newEnd: number;

                switch (presetId) {
                  case 'all': {
                    // Full zoom to 100% bounds (globalMin → globalMax)
                    newStart = globalMin;
                    newEnd = globalMax;
                    break;
                  }
                  case 'fit': {
                    // Fit to incremental models (transformationMin → transformationMax)
                    newStart = transformationMin;
                    newEnd = transformationMax;
                    break;
                  }
                  case 'zoom-out': {
                    // Dynamic zoom out (larger window)
                    const currentWindow = currentZoom.end - currentZoom.start;
                    const newWindow = getZoomOutScale(currentWindow);
                    const range = calculateZoomRangeForWindow(newWindow, transformationMin, transformationMax);
                    newStart = range.start;
                    newEnd = range.end;
                    break;
                  }
                  case 'zoom-in': {
                    // Dynamic zoom in (smaller window)
                    const currentWindow = currentZoom.end - currentZoom.start;
                    const newWindow = getZoomInScale(currentWindow);
                    const range = calculateZoomRangeForWindow(newWindow, transformationMin, transformationMax);
                    newStart = range.start;
                    newEnd = range.end;
                    break;
                  }
                }

                setZoomRange({ start: newStart, end: newEnd });
              }}
              disabled={!hasData}
            />
          </div>
          <div className="flex flex-wrap items-center gap-3 text-xs sm:gap-4">
            <div className="flex items-center gap-1.5 sm:gap-2">
              <div className="size-2.5 rounded-sm bg-indigo-500 sm:size-3" />
              <span className="font-medium text-slate-400">This Model</span>
            </div>
            <div className="flex items-center gap-1.5 sm:gap-2">
              <div className="size-2.5 rounded-sm bg-indigo-400 sm:size-3" />
              <span className="font-medium text-slate-400">Dependencies (Transform)</span>
            </div>
            <div className="flex items-center gap-1.5 sm:gap-2">
              <div className="size-2.5 rounded-sm bg-green-500 sm:size-3" />
              <span className="font-medium text-slate-400">Dependencies (External)</span>
            </div>
          </div>
        </div>

        {/* Main model coverage */}
        <div className="mb-6" data-model-id={decodedId}>
          <div className="mb-2">
            <span className="truncate font-mono text-xs font-bold text-slate-200 sm:text-sm">{decodedId}</span>
          </div>
          <CoverageBar
            ranges={modelCoverage?.ranges}
            zoomStart={currentZoom.start}
            zoomEnd={currentZoom.end}
            type="transformation"
            height={96}
            transformation={currentTransformation}
            onCoverageHover={(position, mouseX) => setHoveredCoverage({ modelId: decodedId, position, mouseX })}
            onCoverageLeave={() => setHoveredCoverage(null)}
            onCoverageClick={position => setDebugPosition({ modelId: decodedId, position })}
          />
        </div>

        {/* Dependencies */}
        {orderedDeps.length > 0 && (
          <div className="mt-8 border-t border-slate-700/50 pt-6">
            <h3 className="mb-4 text-base font-bold text-slate-100">
              Dependencies{' '}
              <span className="ml-2 rounded-full bg-slate-700 px-2 py-0.5 text-xs font-bold text-slate-300">
                {orderedDeps.length}
              </span>
            </h3>
            <div className="space-y-3">
              {(() => {
                // Sort dependencies: transformations/scheduled first (alphabetically), then external last (alphabetically)
                const sortedDeps = [...orderedDeps].sort((a, b) => {
                  const aCoverage = coverage.data?.coverage.find(c => c.id === a.id);
                  const aBounds = allBounds.data?.bounds.find(bd => bd.id === a.id);
                  const aIsExternal = !aCoverage && aBounds;

                  const bCoverage = coverage.data?.coverage.find(c => c.id === b.id);
                  const bBounds = allBounds.data?.bounds.find(bd => bd.id === b.id);
                  const bIsExternal = !bCoverage && bBounds;

                  // If types differ, external models go last
                  if (aIsExternal !== bIsExternal) {
                    return aIsExternal ? 1 : -1;
                  }
                  // Same type: sort alphabetically by ID
                  return a.id.localeCompare(b.id);
                });

                return sortedDeps.map(dep => {
                  const depCoverage = coverage.data?.coverage.find(c => c.id === dep.id);
                  const depBounds = allBounds.data?.bounds.find(b => b.id === dep.id);
                  const isExternalDep = !depCoverage && depBounds;
                  const isScheduledDep = !depCoverage && !depBounds;

                  return (
                    <DependencyRow
                      key={dep.id}
                      dependencyId={dep.id}
                      orGroups={dep.orGroups}
                      orGroupMembers={orGroupMembers}
                      orGroupParent={decodedId}
                      type={isScheduledDep ? 'scheduled' : isExternalDep ? 'external' : 'transformation'}
                      ranges={depCoverage?.ranges}
                      bounds={depBounds ? { min: depBounds.min, max: depBounds.max } : undefined}
                      zoomStart={currentZoom.start}
                      zoomEnd={currentZoom.end}
                      transformation={currentTransformation}
                      onCoverageHover={(modelId, position, mouseX) => setHoveredCoverage({ modelId, position, mouseX })}
                      onCoverageLeave={() => setHoveredCoverage(null)}
                      hoveredOrGroup={hoveredOrGroup}
                      onOrGroupHover={setHoveredOrGroup}
                    />
                  );
                });
              })()}
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
          />
        </div>
      </div>

      {/* Transformation content - shown after coverage */}
      {transformation?.content && transformation.content_type === 'sql' && (
        <SQLCodeBlock sql={transformation.content} title="Transformation Query" />
      )}

      {transformation?.content && transformation.content_type === 'exec' && (
        <div className="overflow-hidden rounded-lg border border-indigo-500/30 bg-slate-900/80 shadow-lg">
          <div className="border-b border-slate-700/50 bg-slate-800/60 px-4 py-2">
            <span className="text-sm font-semibold text-slate-300">Execution Command</span>
          </div>
          <div className="p-4">
            <pre className="overflow-auto text-sm">
              <code className="font-mono text-slate-200">{transformation.content}</code>
            </pre>
          </div>
        </div>
      )}

      {/* Coverage tooltip */}
      {hoveredCoverage && (
        <CoverageTooltip
          hoveredPosition={hoveredCoverage.position}
          hoveredModelId={hoveredCoverage.modelId}
          mouseX={hoveredCoverage.mouseX}
          allModels={allModelsForTooltip}
          dependencyIds={getAllDependencyIds(hoveredCoverage.modelId)}
          transformation={currentTransformation}
          zoomStart={currentZoom.start}
          zoomEnd={currentZoom.end}
          containerRef={sectionRef}
        />
      )}

      {/* Coverage Debug Dialog */}
      {debugPosition && (
        <CoverageDebugDialog
          isOpen={!!debugPosition}
          onClose={() => setDebugPosition(null)}
          modelId={debugPosition.modelId}
          position={debugPosition.position}
          intervalType={intervalType}
        />
      )}
    </div>
  );
}
