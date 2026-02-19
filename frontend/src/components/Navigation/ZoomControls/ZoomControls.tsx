import { type JSX } from 'react';
import { RangeSlider } from '@/components/Forms/RangeSlider';
import type { IntervalTypeTransformation } from '@/api/types.gen';
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
  // Disable when:
  // 1. Both are 0 (no models at all)
  // 2. Using default fallback values (0 and 100) which indicates no real data
  const isDisabled = (globalMin === 0 && globalMax === 0) || (globalMin === 0 && globalMax === 100);

  return (
    <div className={`glass-surface-subtle px-3 py-2 ${isDisabled ? 'opacity-50' : ''}`}>
      <div className="mb-1 flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
        <span className="text-[10px] font-bold tracking-[0.12em] text-muted uppercase">
          {transformationName || transformation?.name || 'Range'}
        </span>
        <div className="flex flex-wrap items-center gap-2">
          <div className="rounded-lg border border-border/55 bg-surface/92 px-2.5 py-1 font-mono text-[10px] font-semibold text-primary ring-1 ring-border/35 sm:px-3 sm:py-1.5 sm:text-xs">
            {isDisabled
              ? 'min: N/A max: N/A'
              : transformation
                ? `min: ${formatValue(transformValue(globalMin, transformation), transformation.format)} max: ${formatValue(transformValue(globalMax, transformation), transformation.format)}`
                : `min: ${globalMin.toLocaleString()} max: ${globalMax.toLocaleString()}`}
          </div>
          <div className="rounded-lg border border-accent/35 bg-linear-to-r from-accent/17 to-accent/8 px-2.5 py-1 font-mono text-[10px] font-bold text-accent ring-1 ring-accent/35 sm:px-3 sm:py-1.5 sm:text-xs">
            {isDisabled
              ? 'N/A - N/A'
              : transformation
                ? `${formatValue(transformValue(zoomStart, transformation), transformation.format)} - ${formatValue(transformValue(zoomEnd, transformation), transformation.format)}`
                : `${zoomStart.toLocaleString()} - ${zoomEnd.toLocaleString()}`}
          </div>
        </div>
      </div>
      <div className={isDisabled ? 'pointer-events-none' : ''}>
        <RangeSlider
          globalMin={globalMin}
          globalMax={globalMax}
          zoomStart={zoomStart}
          zoomEnd={zoomEnd}
          onZoomChange={onZoomChange}
        />
      </div>
    </div>
  );
}
