import { type JSX, useState, useRef, useMemo, useEffect } from 'react';
import type { UseQueryResult } from '@tanstack/react-query';
import type {
  IntervalTypeTransformation,
  TransformationModel,
  ListTransformationCoverageResponse,
  ListExternalBoundsResponse,
  ListTransformationsResponse,
  GetIntervalTypesResponse,
} from '@/api/types.gen';
import { ReactFlowProvider } from '@xyflow/react';
import { ArrowsPointingOutIcon, XMarkIcon } from '@heroicons/react/24/outline';
import { ModelInfoCard, type InfoField } from '@/components/Domain/Models/ModelInfoCard';
import { DependencyRow } from '@/components/Domain/Coverage/DependencyRow';
import { CoverageBar } from '@/components/Domain/Coverage/CoverageBar';
import { ZoomControls } from '@/components/Navigation/ZoomControls';
import { CoverageTooltip } from '@/components/Domain/Coverage/CoverageTooltip';
import { CoverageDebugDialog } from '@/components/Overlays/CoverageDebugDialog';
import { SQLCodeBlock } from '@/components/Elements/SQLCodeBlock';
import { TransformationSelector } from '@/components/Forms/TransformationSelector';
import { ZoomPresets } from '@/components/Navigation/ZoomPresets';
import { DagGraph, type DagData } from '@/components/Domain/DAG/DagGraph';
import type { IncrementalModelItem } from '@/types';
import { getOrderedDependencies } from '@/utils/dependency-resolver';
import {
  calculateDefaultZoomRange,
  calculateZoomRangeForWindow,
  getZoomInScale,
  getZoomOutScale,
} from '@/utils/zoom-helpers';
import { useTransformationSelection } from '@/hooks/useTransformationSelection';

interface ConfigOverride {
  model_id: string;
  model_type: string;
  enabled?: boolean;
  override: Record<string, unknown> | null;
  updated_at: string;
}

export interface ModelDetailViewProps {
  decodedId: string;
  transformation: TransformationModel;
  coverage: UseQueryResult<ListTransformationCoverageResponse, unknown>;
  allBounds: UseQueryResult<ListExternalBoundsResponse, unknown>;
  allTransformations: UseQueryResult<ListTransformationsResponse, unknown>;
  intervalTypes: UseQueryResult<GetIntervalTypesResponse, unknown>;
  currentOverride?: ConfigOverride | null;
  baseConfig?: Record<string, unknown> | null;
}

/** Resolve a nested value from the override object. */
function getOv(override: Record<string, unknown> | null | undefined, ...path: string[]): unknown {
  let current: unknown = override;

  for (const key of path) {
    if (current == null || typeof current !== 'object') return undefined;
    current = (current as Record<string, unknown>)[key];
  }

  return current;
}

export function ModelDetailView({
  decodedId,
  transformation,
  coverage,
  allBounds,
  allTransformations,
  intervalTypes,
  currentOverride,
  baseConfig,
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

  const ov = currentOverride?.override;

  const infoFields: InfoField[] = [
    { label: 'Database', value: decodedId.split('.')[0] },
    { label: 'Table', value: decodedId.split('.').slice(1).join('.') },
    { label: 'Type', value: transformation?.type, variant: 'highlight', highlightColor: 'incremental' },
    { label: 'Content Type', value: transformation?.content_type, variant: 'highlight', highlightColor: 'incremental' },
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

  // Use baseConfig from API for accurate comparison (pre-override snapshot)
  const bc = baseConfig as Record<string, unknown> | null | undefined;

  // Tags — merge override, compare against base config
  {
    const baseTags = (getOv(bc, 'tags') as string[] | undefined) ?? transformation?.tags ?? [];
    const tagsOv = getOv(ov, 'tags') as string[] | undefined;
    const effectiveTags = tagsOv ?? baseTags;

    if (effectiveTags.length > 0 || tagsOv != null) {
      const tagsChanged = tagsOv != null && JSON.stringify(tagsOv) !== JSON.stringify(baseTags);
      infoFields.push({
        label: 'Tags',
        value: effectiveTags.join(', ') || 'none',
        ...(tagsChanged && { overridden: true, originalValue: baseTags.join(', ') || 'none' }),
      });
    }
  }

  // Interval min/max — merge overrides, compare against base config
  {
    const baseMin = (getOv(bc, 'interval', 'min') as number | undefined) ?? transformation?.interval?.min;
    const baseMax = (getOv(bc, 'interval', 'max') as number | undefined) ?? transformation?.interval?.max;
    const minOv = getOv(ov, 'interval', 'min') as number | undefined;
    const maxOv = getOv(ov, 'interval', 'max') as number | undefined;
    const effectiveMin = minOv ?? baseMin;
    const effectiveMax = maxOv ?? baseMax;

    if (effectiveMin !== undefined || effectiveMax !== undefined) {
      if (effectiveMin !== undefined) {
        infoFields.push({
          label: 'Min Interval',
          value: effectiveMin.toString(),
          ...(minOv != null && minOv !== baseMin && { overridden: true, originalValue: baseMin?.toString() }),
        });
      }

      if (effectiveMax !== undefined) {
        infoFields.push({
          label: 'Max Interval',
          value: effectiveMax.toString(),
          ...(maxOv != null && maxOv !== baseMax && { overridden: true, originalValue: baseMax?.toString() }),
        });
      }
    }
  }

  // Schedules — merge overrides, compare against base config
  {
    const baseFwd =
      (getOv(bc, 'schedules', 'forwardfill') as string | undefined) ?? transformation?.schedules?.forwardfill;
    const baseBf = (getOv(bc, 'schedules', 'backfill') as string | undefined) ?? transformation?.schedules?.backfill;
    const fwdOv = getOv(ov, 'schedules', 'forwardfill') as string | undefined;
    const bfOv = getOv(ov, 'schedules', 'backfill') as string | undefined;
    const effectiveFwd = fwdOv ?? baseFwd;
    const effectiveBf = bfOv ?? baseBf;

    if (effectiveFwd) {
      infoFields.push({
        label: 'Forwardfill Schedule',
        value: effectiveFwd,
        variant: 'highlight',
        highlightColor: 'scheduled',
        ...(fwdOv != null && fwdOv !== baseFwd && { overridden: true, originalValue: baseFwd }),
      });
    }

    if (effectiveBf) {
      infoFields.push({
        label: 'Backfill Schedule',
        value: effectiveBf,
        variant: 'highlight',
        highlightColor: 'warning',
        ...(bfOv != null && bfOv !== baseBf && { overridden: true, originalValue: baseBf }),
      });
    }
  }

  // Limits — merge overrides, compare against base config
  {
    const baseLimMin = (getOv(bc, 'limits', 'min') as number | undefined) ?? transformation?.limits?.min;
    const baseLimMax = (getOv(bc, 'limits', 'max') as number | undefined) ?? transformation?.limits?.max;
    const limMinOv = getOv(ov, 'limits', 'min') as number | undefined;
    const limMaxOv = getOv(ov, 'limits', 'max') as number | undefined;
    const effectiveLimMin = limMinOv ?? baseLimMin;
    const effectiveLimMax = limMaxOv ?? baseLimMax;

    if (effectiveLimMin !== undefined) {
      infoFields.push({
        label: 'Limit Min',
        value: effectiveLimMin.toString(),
        ...(limMinOv != null && limMinOv !== baseLimMin && { overridden: true, originalValue: baseLimMin?.toString() }),
      });
    }

    if (effectiveLimMax !== undefined) {
      infoFields.push({
        label: 'Limit Max',
        value: effectiveLimMax.toString(),
        ...(limMaxOv != null && limMaxOv !== baseLimMax && { overridden: true, originalValue: baseLimMax?.toString() }),
      });
    }
  }

  // Fill configuration
  if (transformation?.fill) {
    if (transformation.fill.direction) {
      infoFields.push({
        label: 'Fill Direction',
        value: transformation.fill.direction,
      });
    }
    if (transformation.fill.allow_gap_skipping !== undefined) {
      infoFields.push({
        label: 'Gap Skipping',
        value: transformation.fill.allow_gap_skipping ? 'Enabled' : 'Disabled',
      });
    }
    if (transformation.fill.buffer !== undefined && transformation.fill.buffer > 0) {
      infoFields.push({
        label: 'Fill Buffer',
        value: transformation.fill.buffer.toString(),
        variant: 'highlight',
        highlightColor: 'accent',
      });
    }
  }

  return (
    <div className="space-y-6">
      <ModelInfoCard title="Model Information" fields={infoFields} borderColor="border-incremental/30" columns={4} />

      {/* DAG Visualization - only for incremental models with dependencies */}
      {dagData && orderedDeps.length > 0 && (
        <div
          className={`glass-surface border-incremental/35 p-4 sm:p-6 ${
            fullscreenSection === 'dag' ? 'fixed inset-0 z-50 m-0 rounded-none' : ''
          }`}
        >
          <div className="mb-4 flex items-center justify-between">
            <h2 className="bg-linear-to-r from-incremental via-accent to-scheduled bg-clip-text text-base font-black tracking-tight text-transparent sm:text-lg">
              Dependency Graph
            </h2>
            <button
              onClick={() => setFullscreenSection(fullscreenSection === 'dag' ? null : 'dag')}
              className="glass-icon-control text-muted"
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

      <div ref={sectionRef} className="glass-surface border-incremental/35 p-4 sm:p-6">
        <div className="mb-4 flex flex-col gap-4 sm:mb-6">
          <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:gap-4">
              <h2 className="bg-linear-to-r from-incremental via-accent to-incremental bg-clip-text text-base font-black tracking-tight text-transparent sm:text-lg">
                Coverage Analysis
              </h2>
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
          <div className="glass-toolbar flex w-fit flex-wrap items-center gap-3 px-3 py-2 text-xs sm:gap-4">
            <div className="flex items-center gap-1.5 sm:gap-2">
              <div className="size-2.5 rounded-sm bg-incremental sm:size-3" />
              <span className="font-medium text-primary/80">This Model</span>
            </div>
            <div className="flex items-center gap-1.5 sm:gap-2">
              <div className="size-2.5 rounded-sm bg-incremental/80 sm:size-3" />
              <span className="font-medium text-primary/80">Dependencies (Transform)</span>
            </div>
            <div className="flex items-center gap-1.5 sm:gap-2">
              <div className="size-2.5 rounded-sm bg-external sm:size-3" />
              <span className="font-medium text-primary/80">Dependencies (External)</span>
            </div>
          </div>
        </div>

        {/* Main model coverage */}
        <div className="mb-6" data-model-id={decodedId}>
          <div className="mb-2">
            <p className="truncate font-mono text-xs font-bold text-primary sm:text-sm">{decodedId}</p>
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
            onCoverageClick={position => {
              setHoveredCoverage(null);
              setDebugPosition({ modelId: decodedId, position });
            }}
          />
        </div>

        {/* Dependencies */}
        {orderedDeps.length > 0 && (
          <div className="mt-8 border-t border-border/60 pt-6">
            <h3 className="mb-4 text-base font-bold text-foreground">
              Dependencies{' '}
              <span className="ml-2 rounded-full border border-border/45 bg-secondary/70 px-2 py-0.5 text-xs font-bold text-primary ring-1 ring-border/30">
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

        <div className="mt-4 border-t border-border/50 pt-4">
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
        <div className="glass-surface overflow-hidden border-incremental/35">
          <div className="border-b border-border/50 bg-surface/78 px-4 py-2">
            <span className="text-sm font-semibold text-foreground">Execution Command</span>
          </div>
          <div className="p-4">
            <pre className="overflow-auto text-sm">
              <code className="font-mono text-foreground">{transformation.content}</code>
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
