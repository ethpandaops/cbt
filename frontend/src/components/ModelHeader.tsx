import { type JSX } from 'react';

export interface ModelHeaderProps {
  modelId: string;
  modelType: 'external' | 'scheduled' | 'incremental';
}

const typeConfig = {
  external: {
    gradient: 'from-green-400 via-emerald-400 to-green-400',
    badgeBg: 'bg-green-500/20',
    badgeText: 'text-green-300',
    badgeRing: 'ring-green-500/50',
    label: 'EXTERNAL',
  },
  scheduled: {
    gradient: 'from-emerald-400 via-teal-400 to-emerald-400',
    badgeBg: 'bg-emerald-500/20',
    badgeText: 'text-emerald-300',
    badgeRing: 'ring-emerald-500/50',
    label: 'SCHEDULED',
  },
  incremental: {
    gradient: 'from-indigo-400 via-purple-400 to-indigo-400',
    badgeBg: 'bg-indigo-500/20',
    badgeText: 'text-indigo-300',
    badgeRing: 'ring-indigo-500/50',
    label: 'INCREMENTAL',
  },
};

export function ModelHeader({ modelId, modelType }: ModelHeaderProps): JSX.Element {
  const config = typeConfig[modelType];

  return (
    <div className="mb-6 flex items-baseline gap-4">
      <h1
        className={`bg-gradient-to-r ${config.gradient} bg-clip-text text-4xl font-black tracking-tight text-transparent`}
      >
        {modelId}
      </h1>
      <span
        className={`rounded-full ${config.badgeBg} px-3 py-1 text-xs font-bold ${config.badgeText} ring-1 ${config.badgeRing}`}
      >
        {config.label}
      </span>
    </div>
  );
}
