import { type JSX } from 'react';
import { Link } from '@tanstack/react-router';
import { CoverageBar } from './CoverageBar';
import type { IntervalTypeTransformation } from '@api/types.gen';

export interface DependencyRowProps {
  dependencyId: string;
  type: 'transformation' | 'external' | 'scheduled';
  ranges?: Array<{ position: number; interval: number }>;
  bounds?: { min: number; max: number };
  zoomStart: number;
  zoomEnd: number;
  transformation?: IntervalTypeTransformation;
  showLink?: boolean;
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
  showLink = true,
  onCoverageHover,
  onCoverageLeave,
}: DependencyRowProps): JSX.Element {
  const badgeConfig = {
    external: {
      bg: 'bg-green-500/20',
      text: 'text-green-300',
      label: 'EXT',
    },
    scheduled: {
      bg: 'bg-emerald-500/20',
      text: 'text-emerald-300',
      label: 'SCHEDULED',
    },
  };

  const badge = type !== 'transformation' ? badgeConfig[type] : null;

  return (
    <div data-model-id={dependencyId}>
      <div className="mb-1.5 flex items-center gap-2">
        {showLink ? (
          <Link
            to="/model/$id"
            params={{ id: encodeURIComponent(dependencyId) }}
            className="font-mono text-xs font-semibold text-slate-300 transition-colors hover:text-indigo-400 hover:underline"
          >
            {dependencyId}
          </Link>
        ) : (
          <span className="font-mono text-xs font-semibold text-slate-300">{dependencyId}</span>
        )}
        {badge && (
          <span className={`rounded-full ${badge.bg} px-2 py-0.5 text-xs font-bold ${badge.text}`}>{badge.label}</span>
        )}
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
