import { type JSX } from 'react';
import { Link } from '@tanstack/react-router';
import type { IncrementalModelItem } from '@/types';
import type { IntervalTypeTransformation } from '@api/types.gen';
import { CoverageBar } from './CoverageBar';

export interface ModelCoverageRowProps {
  model: IncrementalModelItem;
  zoomStart: number;
  zoomEnd: number;
  globalMin: number;
  globalMax: number;
  isHighlighted?: boolean;
  isDimmed?: boolean;
  transformation?: IntervalTypeTransformation;
  onMouseEnter?: () => void;
  onMouseLeave?: () => void;
  onZoomChange?: (start: number, end: number) => void;
  showLink?: boolean;
  nameWidth?: string;
}

export function ModelCoverageRow({
  model,
  zoomStart,
  zoomEnd,
  globalMin,
  globalMax,
  isHighlighted = false,
  isDimmed = false,
  transformation,
  onMouseEnter,
  onMouseLeave,
  onZoomChange,
  showLink = true,
  nameWidth = 'w-72',
}: ModelCoverageRowProps): JSX.Element {
  const handleWheel = (e: React.WheelEvent<HTMLDivElement>): void => {
    if (!onZoomChange) return;

    e.preventDefault();
    const rect = e.currentTarget.getBoundingClientRect();
    const mouseX = e.clientX - rect.left;
    const position = mouseX / rect.width; // 0 to 1

    const delta = e.deltaY;
    const currentRange = zoomEnd - zoomStart;
    const zoomFactor = delta > 0 ? 1.1 : 0.9; // Zoom out or in
    const rangeChange = currentRange * (zoomFactor - 1);

    let newStart = zoomStart;
    let newEnd = zoomEnd;

    if (position < 0.33) {
      // Left third: only adjust max (right edge), unless at bound then adjust min
      newEnd = Math.min(globalMax, Math.max(zoomStart + 1, zoomEnd + rangeChange));
      // If we hit the max bound, adjust min instead
      if (newEnd === globalMax && zoomEnd === globalMax) {
        newStart = Math.max(globalMin, Math.min(zoomEnd - 1, zoomStart - rangeChange));
        newEnd = zoomEnd;
      }
    } else if (position < 0.67) {
      // Middle third: adjust both
      const center = (zoomStart + zoomEnd) / 2;
      const newRange = currentRange * zoomFactor;
      newStart = Math.max(globalMin, center - newRange / 2);
      newEnd = Math.min(globalMax, center + newRange / 2);
    } else {
      // Right third: only adjust min (left edge), unless at bound then adjust max
      newStart = Math.max(globalMin, Math.min(zoomEnd - 1, zoomStart - rangeChange));
      // If we hit the min bound, adjust max instead
      if (newStart === globalMin && zoomStart === globalMin) {
        newEnd = Math.min(globalMax, Math.max(zoomStart + 1, zoomEnd + rangeChange));
        newStart = zoomStart;
      }
    }

    onZoomChange(newStart, newEnd);
  };

  // Determine badge type
  const isExternal = model.type === 'external';
  const isScheduled = model.type === 'transformation' && !model.data.coverage && !model.data.bounds;

  return (
    <div
      className={`group/row flex flex-col gap-2 rounded-lg p-2 transition-all sm:flex-row sm:items-center sm:gap-3 ${
        isHighlighted
          ? 'bg-indigo-500/20 ring-2 ring-indigo-500/50'
          : isDimmed
            ? 'bg-slate-900/20 opacity-40'
            : 'bg-slate-900/40 hover:bg-slate-900/60'
      }`}
      onMouseEnter={onMouseEnter}
      onMouseLeave={onMouseLeave}
    >
      <div className={`flex shrink-0 items-center gap-2 ${nameWidth}`}>
        {showLink ? (
          <Link
            to="/model/$id"
            params={{ id: encodeURIComponent(model.id) }}
            className={`truncate font-mono text-xs font-semibold transition-colors ${
              isHighlighted ? 'text-indigo-300' : isDimmed ? 'text-slate-500' : 'text-slate-300 hover:text-indigo-400'
            }`}
            title={model.id}
          >
            {model.id}
          </Link>
        ) : (
          <span
            className={`truncate font-mono text-xs font-semibold ${
              isHighlighted ? 'text-indigo-300' : isDimmed ? 'text-slate-500' : 'text-slate-300'
            }`}
            title={model.id}
          >
            {model.id}
          </span>
        )}
        {isExternal && (
          <span className="rounded-full bg-green-500/20 px-2 py-0.5 text-xs font-bold text-green-300">EXT</span>
        )}
        {isScheduled && (
          <span className="rounded-full bg-emerald-500/20 px-2 py-0.5 text-xs font-bold text-emerald-300">
            SCHEDULED
          </span>
        )}
      </div>
      <div className="flex-1" onWheel={handleWheel}>
        <CoverageBar
          ranges={model.data.coverage}
          bounds={model.data.bounds}
          zoomStart={zoomStart}
          zoomEnd={zoomEnd}
          type={isScheduled ? 'scheduled' : model.type}
          transformation={transformation}
          tooltipId="chunk-tooltip"
        />
      </div>
    </div>
  );
}
