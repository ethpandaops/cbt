import { type JSX, useState, useRef } from 'react';
import type { UseQueryResult } from '@tanstack/react-query';
import type {
  IntervalTypeTransformation,
  Transformation,
  TransformationCoverage,
  ExternalBound,
  ListTransformations200ModelsItem,
  IntervalTypes,
} from '@api/types.gen';
import { Tab, TabGroup, TabList } from '@headlessui/react';
import { ModelInfoCard, type InfoField } from './ModelInfoCard';
import { DependencyRow } from './DependencyRow';
import { CoverageBar } from './CoverageBar';
import { ZoomControls } from './ZoomControls';
import { MultiCoverageTooltip } from './MultiCoverageTooltip';
import type { IncrementalModelItem } from '@/types';
import { transformValue, formatValue } from '@/utils/interval-transform';

export interface IncrementalModelDetailViewProps {
  decodedId: string;
  transformation: Transformation;
  coverage: UseQueryResult<TransformationCoverage, Error>;
  allBounds: UseQueryResult<{ bounds: ExternalBound[] }, Error>;
  allTransformations: UseQueryResult<ListTransformations200ModelsItem, Error>;
  intervalTypes: UseQueryResult<IntervalTypes, Error>;
}

export function IncrementalModelDetailView({
  decodedId,
  transformation,
  coverage,
  allBounds,
  allTransformations,
  intervalTypes,
}: IncrementalModelDetailViewProps): JSX.Element {
  const [selectedTransformationIndex, setSelectedTransformationIndex] = useState(0);
  const [hoveredCoverage, setHoveredCoverage] = useState<{ modelId: string; position: number; mouseX: number } | null>(
    null
  );
  const [zoomRange, setZoomRange] = useState<{ start: number; end: number } | null>(null);
  const sectionRef = useRef<HTMLDivElement>(null);

  const modelCoverage = coverage.data?.coverage.find(c => c.id === decodedId);

  // Build dependency tree (deduplicated) - recursively get all transitive dependencies
  const getDependencies = (modelId: string, visited = new Set<string>()): string[] => {
    if (visited.has(modelId)) return [];
    visited.add(modelId);

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

  // Build dependency map
  const dependencyMap = new Map<string, string[]>();
  if (transformation?.depends_on) {
    dependencyMap.set(decodedId, transformation.depends_on);
  }
  allDeps.forEach(depId => {
    const depModel = allTransformations.data?.models.find(m => m.id === depId);
    if (depModel?.depends_on) {
      dependencyMap.set(depId, depModel.depends_on);
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

  // Build all models array for MultiCoverageTooltip
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
    { label: 'Content Type', value: transformation?.content_type },
  ];

  return (
    <div>
      <div className="mb-6">
        <ModelInfoCard title="Model Information" fields={infoFields} borderColor="border-indigo-500/30" columns={4} />
      </div>

      <div
        ref={sectionRef}
        className="rounded-2xl border border-indigo-500/30 bg-slate-800/80 p-4 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm sm:p-6"
      >
        <div className="mb-4 flex flex-col gap-4 sm:mb-6 sm:flex-row sm:items-center sm:justify-between">
          <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:gap-4">
            <h2 className="text-base font-bold text-slate-100 sm:text-lg">Coverage Analysis</h2>
            {transformations.length > 1 && (
              <TabGroup selectedIndex={selectedTransformationIndex} onChange={setSelectedTransformationIndex}>
                <TabList className="flex flex-wrap gap-1 rounded-lg bg-slate-900/60 p-1 ring-1 ring-slate-700/50">
                  {transformations.map((transformation, index) => (
                    <Tab
                      key={index}
                      className="rounded-md px-2.5 py-1 text-xs font-semibold text-slate-400 transition-all hover:bg-slate-800 hover:text-slate-200 data-[selected]:bg-indigo-500 data-[selected]:text-white data-[selected]:shadow-sm focus:outline-none sm:px-3"
                      title={transformation.expression || 'No transformation'}
                    >
                      {transformation.name}
                    </Tab>
                  ))}
                </TabList>
              </TabGroup>
            )}
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

      {/* Multi-coverage tooltip */}
      {hoveredCoverage && (
        <MultiCoverageTooltip
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
