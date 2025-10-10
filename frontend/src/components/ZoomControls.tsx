import { type JSX } from 'react';
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
    <div className="rounded-lg border border-slate-700/50 bg-slate-900/40 p-3 sm:p-4">
      <div className="mb-2 flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
        <span className="text-xs font-semibold text-slate-400">
          {transformationName || transformation?.name || 'Zoom Range'}
        </span>
        <span className="font-mono text-xs text-slate-500">
          {transformation
            ? `${formatValue(transformValue(globalMin, transformation), transformation.format)} → ${formatValue(transformValue(globalMax, transformation), transformation.format)}`
            : `${globalMin.toLocaleString()} → ${globalMax.toLocaleString()}`}
        </span>
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
