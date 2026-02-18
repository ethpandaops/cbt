import { type JSX } from 'react';
import { Link } from '@tanstack/react-router';
import type { IncrementalModelItem } from '@/types';
import type { IntervalTypeTransformation } from '@api/types.gen';
import { CoverageBar } from '@/components/Domain/Coverage/CoverageBar';
import { TypeBadge } from '@/components/Elements/TypeBadge';
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
          isHighlighted={isHighlighted}
          onCoverageHover={
            onCoverageHover ? (position, mouseX) => onCoverageHover(model.id, position, mouseX) : undefined
          }
          onCoverageLeave={onCoverageLeave}
        >
          <div className="flex items-center gap-1.5 px-2">
            <span
              className={`truncate px-1 py-0.5 font-mono text-xs font-semibold drop-shadow-xs transition-colors ${
                isHighlighted
                  ? 'text-primary dark:text-primary'
                  : isDimmed
                    ? 'text-foreground/65 dark:text-foreground'
                    : 'text-foreground/78 group-hover/row:text-primary dark:text-foreground dark:group-hover/row:text-primary'
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
                      className={`rounded px-1 py-0 text-[9px] leading-tight font-semibold ring-1 ${colors.bg} ${colors.text} ${colors.ring}`}
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
