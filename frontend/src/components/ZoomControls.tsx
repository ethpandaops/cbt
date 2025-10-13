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
  transformationName?: string;
}

export function ZoomControls({
  globalMin,
  globalMax,
  zoomStart,
  zoomEnd,
  transformation,
  onZoomChange,
  transformationName,
}: ZoomControlsProps): JSX.Element {
  return (
    <div className="rounded-lg border border-slate-700/40 bg-slate-900/30 px-3 py-2 backdrop-blur-sm">
      <div className="mb-1 flex items-center justify-between gap-3">
        <span className="text-[10px] font-semibold uppercase tracking-wide text-slate-400">
          {transformationName || transformation?.name || 'Range'}
        </span>
        <div className="rounded-lg bg-slate-900/60 px-3 py-1.5 font-mono text-xs font-semibold text-slate-300 ring-1 ring-slate-700/50">
          {transformation
            ? `min: ${formatValue(transformValue(globalMin, transformation), transformation.format)} max: ${formatValue(transformValue(globalMax, transformation), transformation.format)}`
            : `min: ${globalMin.toLocaleString()} max: ${globalMax.toLocaleString()}`}
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
