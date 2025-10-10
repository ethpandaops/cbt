import { type JSX, useState } from 'react';
import type { IncrementalModelItem, ZoomRange } from '@/types';
import type { IntervalTypeTransformation } from '@api/types.gen';
import { ModelCoverageRow } from './ModelCoverageRow';
import { ZoomControls } from './ZoomControls';

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

  const currentTransformation: IntervalTypeTransformation | undefined = transformations[selectedTransformationIndex];

  // Sort models by ID
  const sortedModels = [...models].sort((a, b) => a.id.localeCompare(b.id));

  // Determine which models should be highlighted (simplified - no dependency tracking)
  const highlightedModels = new Set<string>();
  if (hoveredModel) {
    highlightedModels.add(hoveredModel);
  }

  return (
    <div className="group relative overflow-hidden rounded-2xl border border-indigo-500/30 bg-slate-800/80 p-6 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm transition-all duration-300 hover:shadow-lg hover:ring-indigo-500/50">
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
          <div className="rounded-lg bg-slate-900/60 px-4 py-2 font-mono text-xs font-semibold text-slate-300 ring-1 ring-slate-700/50">
            {sortedModels.length} {sortedModels.length === 1 ? 'model' : 'models'}
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
    </div>
  );
}
