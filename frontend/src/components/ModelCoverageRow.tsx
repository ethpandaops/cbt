import { type JSX } from 'react';
import { Link } from '@tanstack/react-router';
import type { IncrementalModelItem } from '@/types';
import type { IntervalTypeTransformation } from '@api/types.gen';
import { CoverageBar } from './CoverageBar';
import { TypeBadge } from './shared/TypeBadge';
import { getOrGroupColor } from '@utils/or-group-colors';

export interface ModelCoverageRowProps {
  model: IncrementalModelItem;
  zoomStart: number;
  zoomEnd: number;
  isHighlighted?: boolean;
  isDimmed?: boolean;
  transformation?: IntervalTypeTransformation;
  orGroups?: number[];
  onMouseEnter?: () => void;
  onMouseLeave?: () => void;
  onCoverageHover?: (modelId: string, position: number, mouseX: number) => void;
  onCoverageLeave?: () => void;
}

export function ModelCoverageRow({
  model,
  zoomStart,
  zoomEnd,
  isHighlighted = false,
  isDimmed = false,
  transformation,
  orGroups,
  onMouseEnter,
  onMouseLeave,
  onCoverageHover,
  onCoverageLeave,
}: ModelCoverageRowProps): JSX.Element {
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
      <div className="relative">
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
