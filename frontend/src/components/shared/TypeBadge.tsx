import { type JSX } from 'react';
import { MODEL_TYPE_CONFIG, type ModelType } from '@/types';

export interface TypeBadgeProps {
  type: ModelType;
  compact?: boolean;
}

export function TypeBadge({ type, compact = false }: TypeBadgeProps): JSX.Element {
  const config = MODEL_TYPE_CONFIG[type];

  if (compact) {
    // Short label for compact display
    const compactLabel = type === 'external' ? 'EXT' : type === 'scheduled' ? 'SCHEDULED' : 'INC';
    return (
      <span className={`rounded-full ${config.badgeBg} px-2 py-0.5 text-xs font-bold ${config.badgeText}`}>
        {compactLabel}
      </span>
    );
  }

  return (
    <span
      className={`w-fit rounded-full ${config.badgeBg} px-3 py-1 text-xs font-bold ${config.badgeText} ring-1 ${config.badgeRing}`}
    >
      {config.label}
    </span>
  );
}
