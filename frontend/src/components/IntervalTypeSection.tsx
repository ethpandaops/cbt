import { type JSX, useState, useRef } from 'react';
import type { IncrementalModelItem, ZoomRange } from '@/types';
import type { IntervalTypeTransformation } from '@api/types.gen';
import { Tab, TabGroup, TabList } from '@headlessui/react';
import { ModelCoverageRow } from './ModelCoverageRow';
import { ZoomControls } from './ZoomControls';
import { MultiCoverageTooltip } from './MultiCoverageTooltip';

export interface IntervalTypeSectionProps {
  intervalType: string;
  models: IncrementalModelItem[];
  zoomRange: ZoomRange;
  globalMin: number;
  globalMax: number;
  transformations: IntervalTypeTransformation[];
  onZoomChange: (start: number, end: number) => void;
  onResetZoom: () => void;
  showLinks?: boolean;
}

export function IntervalTypeSection({
  intervalType,
  models,
  zoomRange,
  globalMin,
  globalMax,
  transformations,
  onZoomChange,
  onResetZoom,
  showLinks = true,
}: IntervalTypeSectionProps): JSX.Element {
  const [hoveredModel, setHoveredModel] = useState<string | null>(null);
  const [selectedTransformationIndex, setSelectedTransformationIndex] = useState(0);
  const [hoveredCoverage, setHoveredCoverage] = useState<{ modelId: string; position: number; mouseX: number } | null>(
    null
  );
  const sectionRef = useRef<HTMLDivElement>(null);

  const currentTransformation: IntervalTypeTransformation | undefined = transformations[selectedTransformationIndex];

  // Sort models by ID
  const sortedModels = [...models].sort((a, b) => a.id.localeCompare(b.id));

  // Build dependency map
  const dependencyMap = new Map<string, string[]>();
  models.forEach(model => {
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
  const activeModelId = hoveredCoverage?.modelId || hoveredModel;
  if (activeModelId) {
    highlightedModels.add(activeModelId);
    const deps = getAllDependencies(activeModelId);
    deps.forEach(dep => highlightedModels.add(dep));
  }

  return (
    <div
      ref={sectionRef}
      className="group relative overflow-hidden rounded-2xl border border-indigo-500/30 bg-slate-800/80 p-4 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm transition-all duration-300 hover:shadow-lg hover:ring-indigo-500/50 sm:p-6"
    >
      <div className="absolute inset-0 bg-linear-to-br from-indigo-500/10 via-transparent to-purple-500/10 opacity-0 transition-opacity duration-300 group-hover:opacity-100" />
      <div className="relative">
        <div className="mb-4 flex flex-col gap-3 sm:mb-6 sm:flex-row sm:items-center sm:justify-between">
          <div className="flex flex-col gap-3 sm:flex-row sm:items-center">
            <h3 className="bg-linear-to-r from-indigo-400 to-purple-400 bg-clip-text text-lg font-black tracking-tight text-transparent sm:text-xl">
              {intervalType}
            </h3>
            {/* Transformation selector tabs - only show if there are 2+ transformations */}
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
          <div className="w-fit rounded-lg bg-slate-900/60 px-3 py-1.5 font-mono text-xs font-semibold text-slate-300 ring-1 ring-slate-700/50 sm:px-4 sm:py-2">
            {sortedModels.length} {sortedModels.length === 1 ? 'model' : 'models'}
          </div>
        </div>

        <div className="space-y-2">
          {sortedModels.map(model => {
            const isHighlighted = highlightedModels.has(model.id);
            const isDimmed = (hoveredModel !== null || hoveredCoverage !== null) && !isHighlighted;

            return (
              <ModelCoverageRow
                key={model.id}
                model={model}
                zoomStart={zoomRange.start}
                zoomEnd={zoomRange.end}
                globalMin={globalMin}
                globalMax={globalMax}
                isHighlighted={isHighlighted}
                isDimmed={isDimmed}
                transformation={currentTransformation}
                onMouseEnter={() => setHoveredModel(model.id)}
                onMouseLeave={() => setHoveredModel(null)}
                onZoomChange={onZoomChange}
                onCoverageHover={(modelId, position, mouseX) => setHoveredCoverage({ modelId, position, mouseX })}
                onCoverageLeave={() => setHoveredCoverage(null)}
                showLink={showLinks}
              />
            );
          })}
        </div>

        {/* Zoom Controls */}
        <div className="mt-6">
          <ZoomControls
            globalMin={globalMin}
            globalMax={globalMax}
            zoomStart={zoomRange.start}
            zoomEnd={zoomRange.end}
            transformation={currentTransformation}
            onZoomChange={onZoomChange}
            onResetZoom={onResetZoom}
          />
        </div>
      </div>

      {/* Multi-coverage tooltip */}
      {hoveredCoverage && (
        <MultiCoverageTooltip
          hoveredPosition={hoveredCoverage.position}
          mouseX={hoveredCoverage.mouseX}
          hoveredModelId={hoveredCoverage.modelId}
          allModels={sortedModels}
          dependencyIds={getAllDependencies(hoveredCoverage.modelId)}
          transformation={currentTransformation}
          zoomStart={zoomRange.start}
          zoomEnd={zoomRange.end}
          containerRef={sectionRef}
        />
      )}
    </div>
  );
}
