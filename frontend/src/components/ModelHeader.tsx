import { type JSX } from 'react';
import { MODEL_TYPE_CONFIG, type ModelType } from '@/types';

export interface ModelHeaderProps {
  modelId: string;
  modelType: ModelType;
}

export function ModelHeader({ modelId, modelType }: ModelHeaderProps): JSX.Element {
  const config = MODEL_TYPE_CONFIG[modelType];

  return (
    <div className="mb-6 flex min-w-0 items-center gap-4">
      <h1 className="min-w-0 flex-1">
        <span
          className={`block overflow-hidden text-ellipsis whitespace-nowrap bg-linear-to-r ${config.gradient} bg-clip-text text-xl font-black tracking-tight text-transparent sm:text-2xl lg:text-3xl`}
        >
          {modelId}
        </span>
      </h1>
      <span
        className={`hidden shrink-0 rounded-full ${config.badgeBg} px-3 py-1 text-xs font-bold ${config.badgeText} ring-1 ${config.badgeRing} sm:inline-block`}
      >
        {config.label}
      </span>
    </div>
  );
}
