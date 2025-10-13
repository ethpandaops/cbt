import { type JSX } from 'react';
import { MODEL_TYPE_CONFIG, type ModelType } from '@types';

export interface ModelHeaderProps {
  modelId: string;
  modelType: ModelType;
}

export function ModelHeader({ modelId, modelType }: ModelHeaderProps): JSX.Element {
  const config = MODEL_TYPE_CONFIG[modelType];

  return (
    <div className="mb-6 flex flex-col gap-3 sm:flex-row sm:items-baseline sm:gap-4">
      <h1
        className={`bg-linear-to-r ${config.gradient} bg-clip-text text-2xl font-black tracking-tight text-transparent sm:text-3xl lg:text-4xl`}
      >
        {modelId}
      </h1>
      <span
        className={`w-fit rounded-full ${config.badgeBg} px-3 py-1 text-xs font-bold ${config.badgeText} ring-1 ${config.badgeRing}`}
      >
        {config.label}
      </span>
    </div>
  );
}
