import { type JSX } from 'react';
import { RangeSlider } from './RangeSlider';
import type { IntervalTypeTransformation } from '@api/types.gen';
import { transformValue, formatValue } from '@utils/interval-transform';

export interface ZoomControlsProps {
  globalMin: number;
  globalMax: number;
  zoomStart: number;
  zoomEnd: number;
  transformation?: IntervalTypeTransformation;
  onZoomChange: (start: number, end: number) => void;
  onResetZoom: () => void;
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
  transformationName,
}: ZoomControlsProps): JSX.Element {
  const isZoomed = zoomStart !== globalMin || zoomEnd !== globalMax;

  return (
    <div className="rounded-lg border border-slate-700/40 bg-slate-900/30 px-3 py-2 backdrop-blur-sm">
      <div className="mb-1 flex items-center justify-between gap-3">
        <span className="text-[10px] font-semibold uppercase tracking-wide text-slate-400">
          {transformationName || transformation?.name || 'Range'}
        </span>
        <div className="flex items-center gap-2">
          {isZoomed && (
            <button
              onClick={onResetZoom}
              className="rounded-lg bg-indigo-500/20 px-2 py-1 text-xs font-semibold text-indigo-300 ring-1 ring-indigo-500/30 transition-colors hover:bg-indigo-500/30 hover:text-indigo-200"
            >
              Reset Zoom
            </button>
          )}
          <div className="rounded-lg bg-slate-900/60 px-3 py-1.5 font-mono text-xs font-semibold text-slate-300 ring-1 ring-slate-700/50">
            {transformation
              ? `min: ${formatValue(transformValue(globalMin, transformation), transformation.format)} max: ${formatValue(transformValue(globalMax, transformation), transformation.format)}`
              : `min: ${globalMin.toLocaleString()} max: ${globalMax.toLocaleString()}`}
          </div>
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
