import { type JSX } from 'react';
import { Link } from '@tanstack/react-router';
import { CoverageBar } from '@/components/Domain/Coverage/CoverageBar';
import { TypeBadge } from '@/components/Elements/TypeBadge';
import type { IntervalTypeTransformation } from '@/api/types.gen';
import { getOrGroupColor } from '@/utils/or-group-colors';

export interface DependencyRowProps {
  dependencyId: string;
  orGroups?: number[];
  orGroupMembers?: Map<number, string[]>; // Map of OR group ID to all member model IDs
  orGroupParent?: string; // The parent model ID that defines this OR group
  type: 'external' | 'transformation' | 'scheduled';
  ranges?: Array<{ position: number; interval: number }>;
  bounds?: { min: number; max: number };
  zoomStart: number;
  zoomEnd: number;
  transformation?: IntervalTypeTransformation;
  onCoverageHover?: (modelId: string, position: number, mouseX: number) => void;
  onCoverageLeave?: () => void;
  hoveredOrGroup?: number | null;
  onOrGroupHover?: (groupId: number | null) => void;
}

export function DependencyRow({
  dependencyId,
  orGroups,
  orGroupMembers,
  orGroupParent,
  type,
  ranges,
  bounds,
  zoomStart,
  zoomEnd,
  transformation,
  onCoverageHover,
  onCoverageLeave,
  hoveredOrGroup,
  onOrGroupHover,
}: DependencyRowProps): JSX.Element {
  return (
    <div data-model-id={dependencyId}>
      <div className="mb-1.5 flex min-w-0 items-center gap-2">
        <Link
          to="/model/$id"
          params={{ id: encodeURIComponent(dependencyId) }}
          className="min-w-0 flex-1 truncate font-mono text-xs font-semibold text-foreground transition-colors hover:text-accent hover:underline"
        >
          {dependencyId}
        </Link>
        {orGroups && orGroups.length > 0 && (
          <div className="flex items-center gap-1">
            {orGroups.map(groupId => {
              const colors = getOrGroupColor(groupId);
              const members = orGroupMembers?.get(groupId) || [];
              const otherMembers = members.filter(m => m !== dependencyId);
              const isThisGroupHovered = hoveredOrGroup === groupId;
              const labelStyle = isThisGroupHovered
                ? {
                    boxShadow: '0 0 0 2px rgb(34, 211, 238), 0 0 12px rgb(34, 211, 238)',
                    filter: 'brightness(1.4)',
                  }
                : undefined;

              return (
                <span
                  key={groupId}
                  className={`group relative cursor-pointer rounded px-1.5 py-0.5 text-xs font-semibold ring-1 transition-all ${colors.bg} ${colors.text} ${colors.ring}`}
                  style={labelStyle}
                  onMouseEnter={() => onOrGroupHover?.(groupId)}
                  onMouseLeave={() => onOrGroupHover?.(null)}
                >
                  OR #{groupId}
                  {otherMembers.length > 0 && (
                    <span className="pointer-events-none absolute bottom-full left-1/2 z-10 mb-2 hidden w-max max-w-md -translate-x-1/2 rounded-lg bg-surface/95 px-3 py-2 text-xs text-primary shadow-xl ring-1 ring-border/70 group-hover:block">
                      <div className="mb-1.5 text-[10px] font-semibold tracking-wide text-muted uppercase">
                        At least one of these models must be available
                      </div>
                      {orGroupParent && (
                        <div className="mb-2 text-[11px] text-muted">
                          Required by: <span className="font-mono text-foreground">{orGroupParent}</span>
                        </div>
                      )}
                      <div className="space-y-0.5">
                        {members.map(member => (
                          <div key={member} className={member === dependencyId ? 'font-semibold text-accent' : ''}>
                            <span className="font-mono text-xs">{member}</span>
                            {member === dependencyId && <span className="ml-1.5 text-[10px] text-muted">(this)</span>}
                          </div>
                        ))}
                      </div>
                    </span>
                  )}
                </span>
              );
            })}
          </div>
        )}
        {type !== 'transformation' && <TypeBadge type={type} compact />}
      </div>
      <CoverageBar
        ranges={ranges}
        bounds={bounds}
        zoomStart={zoomStart}
        zoomEnd={zoomEnd}
        type={type}
        height={48}
        transformation={transformation}
        onCoverageHover={
          onCoverageHover ? (position, mouseX) => onCoverageHover(dependencyId, position, mouseX) : undefined
        }
        onCoverageLeave={onCoverageLeave}
      />
    </div>
  );
}
