import { type JSX } from 'react';
import { Link } from '@tanstack/react-router';
import type { IncrementalModelItem } from '@types';
import type { IntervalTypeTransformation } from '@api/types.gen';
import { CoverageBar } from './CoverageBar';
import { TypeBadge } from './shared/TypeBadge';
import { getOrGroupColor } from '@utils/or-group-colors';

export interface ModelCoverageRowProps {
  model: IncrementalModelItem;
  zoomStart: number;
  zoomEnd: number;
  globalMin: number;
  globalMax: number;
  isHighlighted?: boolean;
  isDimmed?: boolean;
  transformation?: IntervalTypeTransformation;
  orGroups?: number[];
  onMouseEnter?: () => void;
  onMouseLeave?: () => void;
  onZoomChange?: (start: number, end: number) => void;
  onCoverageHover?: (modelId: string, position: number, mouseX: number) => void;
  onCoverageLeave?: () => void;
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
  orGroups,
  onMouseEnter,
  onMouseLeave,
  onZoomChange,
  onCoverageHover,
  onCoverageLeave,
  nameWidth: _nameWidth = 'w-72',
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
  const isScheduled = model.type === 'transformation' && !model.data.coverage && !model.data.bounds;

  return (
    <Link
      to="/model/$id"
      params={{ id: encodeURIComponent(model.id) }}
      className={`group/row block transition-all ${isHighlighted ? 'brightness-125' : isDimmed ? 'opacity-40' : ''}`}
      data-model-id={model.id}
      onMouseEnter={onMouseEnter}
      onMouseLeave={onMouseLeave}
    >
      <div className="relative" onWheel={handleWheel}>
        <CoverageBar
          ranges={model.data.coverage}
          bounds={model.data.bounds}
          zoomStart={zoomStart}
          zoomEnd={zoomEnd}
          type={isScheduled ? 'scheduled' : model.type}
          transformation={transformation}
          onCoverageHover={
            onCoverageHover ? (position, mouseX) => onCoverageHover(model.id, position, mouseX) : undefined
          }
          onCoverageLeave={onCoverageLeave}
        >
          <div className="flex items-center gap-1.5 px-2">
            <span
              className={`truncate font-mono text-xs font-semibold transition-colors [text-shadow:_0_0_1px_rgb(0_0_0)] ${
                isHighlighted
                  ? 'text-white [text-shadow:_0_0_1px_rgb(0_0_0)]'
                  : isDimmed
                    ? 'text-slate-500'
                    : 'text-slate-300 group-hover/row:text-indigo-400'
              }`}
              title={model.id}
            >
              {model.id}
            </span>
            {orGroups && orGroups.length > 0 && (
              <div className="flex items-center gap-1">
                {orGroups.map(groupId => {
                  const colors = getOrGroupColor(groupId);
                  return (
                    <span
                      key={groupId}
                      className={`rounded px-1 py-0 text-[9px] font-semibold ring-1 leading-tight ${colors.bg} ${colors.text} ${colors.ring}`}
                    >
                      OR #{groupId}
                    </span>
                  );
                })}
              </div>
            )}
            {isScheduled && <TypeBadge type="scheduled" compact />}
          </div>
        </CoverageBar>
      </div>
    </Link>
  );
}
