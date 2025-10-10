import { type JSX, useState, useRef } from 'react';
import type { UseQueryResult } from '@tanstack/react-query';
import type {
  IntervalTypeTransformation,
  TransformationModel,
  ListTransformationCoverageResponse,
  ListExternalBoundsResponse,
  ListTransformationsResponse,
  GetIntervalTypesResponse,
} from '@api/types.gen';
import { ModelInfoCard, type InfoField } from './ModelInfoCard';
import { DependencyRow } from './DependencyRow';
import { CoverageBar } from './CoverageBar';
import { ZoomControls } from './ZoomControls';
import { CoverageTooltip } from './CoverageTooltip';
import { SQLCodeBlock } from './SQLCodeBlock';
import { TransformationSelector } from './shared/TransformationSelector';
import { useDependencyTree } from '@/hooks/useDependencyTree';
import type { IncrementalModelItem } from '@/types';
import { transformValue, formatValue } from '@/utils/interval-transform';

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
  const [selectedTransformationIndex, setSelectedTransformationIndex] = useState(0);
  const [hoveredCoverage, setHoveredCoverage] = useState<{ modelId: string; position: number; mouseX: number } | null>(
    null
  );
  const [zoomRange, setZoomRange] = useState<{ start: number; end: number } | null>(null);
  const sectionRef = useRef<HTMLDivElement>(null);

  const modelCoverage = coverage.data?.coverage.find(c => c.id === decodedId);

  // Build dependency map
  const dependencyMap = new Map<string, string[]>();
  if (transformation?.depends_on) {
    dependencyMap.set(decodedId, transformation.depends_on);
  }

  // Get all dependencies using the custom hook
  const { allDependenciesArray: allDeps, getAllDependencies } = useDependencyTree(decodedId, dependencyMap);

  // Add dependencies to the map for nested resolution
  allDeps.forEach(depId => {
    const depModel = allTransformations.data?.models.find(m => m.id === depId);
    if (depModel?.depends_on) {
      dependencyMap.set(depId, depModel.depends_on);
    }
  });

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

  allDeps.forEach(depId => {
    const depCoverage = coverage.data?.coverage.find(c => c.id === depId);
    const depBounds = allBounds.data?.bounds.find(b => b.id === depId);
    const depModel = allTransformations.data?.models.find(m => m.id === depId);

    if (depCoverage) {
      allModelsForTooltip.push({
        id: depId,
        type: 'transformation',
        intervalType: depModel?.interval?.type || 'unknown',
        depends_on: depModel?.depends_on,
        data: {
          coverage: depCoverage.ranges,
        },
      });
    } else if (depBounds) {
      allModelsForTooltip.push({
        id: depId,
        type: 'external',
        intervalType: 'unknown',
        data: {
          bounds: { min: depBounds.min, max: depBounds.max },
        },
      });
    }
  });

  // Calculate range from dependencies only
  let globalMin = Infinity;
  let globalMax = -Infinity;

  allDeps.forEach(depId => {
    const depCoverage = coverage.data?.coverage.find(c => c.id === depId);
    const depBounds = allBounds.data?.bounds.find(b => b.id === depId);

    depCoverage?.ranges.forEach(range => {
      globalMin = Math.min(globalMin, range.position);
      globalMax = Math.max(globalMax, range.position + range.interval);
    });

    if (depBounds) {
      globalMin = Math.min(globalMin, depBounds.min);
      globalMax = Math.max(globalMax, depBounds.max);
    }
  });

  if (globalMin === Infinity) globalMin = 0;
  if (globalMax === -Infinity) globalMax = 100;

  const currentZoom = zoomRange || { start: globalMin, end: globalMax };

  const intervalType = transformation?.interval?.type || 'unknown';
  const transformations = intervalTypes.data?.interval_types?.[intervalType] || [];
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

      <div
        ref={sectionRef}
        className="rounded-2xl border border-indigo-500/30 bg-slate-800/80 p-4 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm sm:p-6"
      >
        <div className="mb-4 flex flex-col gap-4 sm:mb-6 sm:flex-row sm:items-center sm:justify-between">
          <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:gap-4">
            <h2 className="text-base font-bold text-slate-100 sm:text-lg">Coverage Analysis</h2>
            <TransformationSelector
              transformations={transformations}
              selectedIndex={selectedTransformationIndex}
              onSelect={setSelectedTransformationIndex}
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
          <div className="mb-2 flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
            <span className="truncate font-mono text-xs font-bold text-slate-200 sm:text-sm">{decodedId}</span>
            <span className="w-fit rounded-lg bg-slate-900/60 px-2.5 py-1 font-mono text-xs font-semibold text-slate-300 sm:px-3">
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
            onCoverageHover={(position, mouseX) => setHoveredCoverage({ modelId: decodedId, position, mouseX })}
            onCoverageLeave={() => setHoveredCoverage(null)}
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
                const isScheduledDep = !depCoverage && !depBounds;

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
                    onCoverageHover={(modelId, position, mouseX) => setHoveredCoverage({ modelId, position, mouseX })}
                    onCoverageLeave={() => setHoveredCoverage(null)}
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
          dependencyIds={getAllDependencies(hoveredCoverage.modelId)}
          transformation={currentTransformation}
          zoomStart={currentZoom.start}
          zoomEnd={currentZoom.end}
          containerRef={sectionRef}
        />
      )}
    </div>
  );
}
