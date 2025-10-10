import { type JSX } from 'react';
import { ArrowPathIcon } from '@heroicons/react/24/outline';
import { RangeSlider } from './RangeSlider';
import type { IntervalTypeTransformation } from '@api/types.gen';
import { transformValue, formatValue } from '@/utils/interval-transform';

export interface ZoomControlsProps {
  globalMin: number;
  globalMax: number;
  zoomStart: number;
  zoomEnd: number;
  transformation?: IntervalTypeTransformation;
  onZoomChange: (start: number, end: number) => void;
  transformationName?: string;
  onResetZoom?: () => void;
}

export function ZoomControls({
  globalMin,
  globalMax,
  zoomStart,
  zoomEnd,
  transformation,
  onZoomChange,
  transformationName,
  onResetZoom,
}: ZoomControlsProps): JSX.Element {
  const isZoomed = zoomStart !== globalMin || zoomEnd !== globalMax;

  return (
    <div className="rounded-lg border border-slate-700/50 bg-slate-900/40 p-3 sm:p-4">
      <div className="mb-2 flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
        <span className="text-xs font-semibold text-slate-400">
          {transformationName || transformation?.name || 'Zoom Range'}
        </span>
        <div className="flex items-center gap-2">
          <span className="font-mono text-xs text-slate-500">
            {transformation
              ? `${formatValue(transformValue(globalMin, transformation), transformation.format)} → ${formatValue(transformValue(globalMax, transformation), transformation.format)}`
              : `${globalMin.toLocaleString()} → ${globalMax.toLocaleString()}`}
          </span>
          {isZoomed && onResetZoom && (
            <button
              onClick={onResetZoom}
              className="flex items-center gap-1 rounded-md bg-indigo-500/20 px-2 py-1 text-xs font-semibold text-indigo-300 transition-all hover:bg-indigo-500/30 hover:text-indigo-200"
              title="Reset zoom"
            >
              <ArrowPathIcon className="size-3" />
              Reset
            </button>
          )}
        </div>
      </div>
      <RangeSlider
        globalMin={globalMin}
        globalMax={globalMax}
        zoomStart={zoomStart}
        zoomEnd={zoomEnd}
        onZoomChange={onZoomChange}
      />
    </div>
  );
}
