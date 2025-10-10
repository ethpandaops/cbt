import { type JSX } from 'react';
import { Link } from '@tanstack/react-router';
import { CoverageBar } from './CoverageBar';
import { TypeBadge } from './shared/TypeBadge';
import type { IntervalTypeTransformation } from '@api/types.gen';

export interface DependencyRowProps {
  dependencyId: string;
  type: 'external' | 'transformation' | 'scheduled';
  ranges?: Array<{ position: number; interval: number }>;
  bounds?: { min: number; max: number };
  zoomStart: number;
  zoomEnd: number;
  transformation?: IntervalTypeTransformation;
  onCoverageHover?: (modelId: string, position: number, mouseX: number) => void;
  onCoverageLeave?: () => void;
}

export function DependencyRow({
  dependencyId,
  type,
  ranges,
  bounds,
  zoomStart,
  zoomEnd,
  transformation,
  onCoverageHover,
  onCoverageLeave,
}: DependencyRowProps): JSX.Element {
  return (
    <div data-model-id={dependencyId}>
      <div className="mb-1.5 flex items-center gap-2">
        <Link
          to="/model/$id"
          params={{ id: encodeURIComponent(dependencyId) }}
          className="font-mono text-xs font-semibold text-slate-300 transition-colors hover:text-indigo-400 hover:underline"
        >
          {dependencyId}
        </Link>
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
