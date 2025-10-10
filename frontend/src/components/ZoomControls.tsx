import { type JSX } from 'react';
import { ArrowsPointingOutIcon } from '@heroicons/react/24/outline';
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
  onResetZoom?: () => void;
  showResetButton?: boolean;
  transformationName?: string;
}

export function ZoomControls({
  globalMin,
  globalMax,
  zoomStart,
  zoomEnd,
  transformation,
  onZoomChange,
  onResetZoom,
  showResetButton = true,
  transformationName,
}: ZoomControlsProps): JSX.Element {
  const isResetDisabled = zoomStart === globalMin && zoomEnd === globalMax;

  return (
    <div className="rounded-lg border border-slate-700/50 bg-slate-900/40 p-3 sm:p-4">
      <div className="mb-2 flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
        <span className="text-xs font-semibold text-slate-400">
          {transformationName || transformation?.name || 'Zoom Range'}
        </span>
        <div className="flex items-center gap-2 sm:gap-3">
          {showResetButton && onResetZoom && (
            <button
              onClick={onResetZoom}
              disabled={isResetDisabled}
              className="group/btn rounded-lg bg-slate-700 px-2.5 py-1.5 text-slate-300 shadow-sm ring-1 ring-slate-600/50 transition-all hover:bg-indigo-500/20 hover:text-indigo-300 hover:ring-indigo-500/50 disabled:cursor-not-allowed disabled:opacity-40 sm:px-3"
              title="Reset Zoom"
            >
              <ArrowsPointingOutIcon className="size-3.5 transition-transform group-hover/btn:scale-110 sm:size-4" />
            </button>
          )}
          <span className="font-mono text-xs text-slate-500">
            {transformation
              ? `${formatValue(transformValue(globalMin, transformation), transformation.format)} → ${formatValue(transformValue(globalMax, transformation), transformation.format)}`
              : `${globalMin.toLocaleString()} → ${globalMax.toLocaleString()}`}
          </span>
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
