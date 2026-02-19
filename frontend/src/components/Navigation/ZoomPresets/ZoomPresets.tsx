import { type JSX } from 'react';
import { ZOOM_PRESETS } from '@/utils/zoom-presets-config';

export interface ZoomPresetsProps {
  activePreset?: 'all' | 'fit' | 'zoom-out' | 'zoom-in' | null;
  onPresetClick: (presetId: 'all' | 'fit' | 'zoom-out' | 'zoom-in') => void;
  disabled?: boolean;
}

export function ZoomPresets({ activePreset, onPresetClick, disabled = false }: ZoomPresetsProps): JSX.Element {
  return (
    <div className="flex items-center gap-1.5">
      {ZOOM_PRESETS.map(preset => {
        const Icon = preset.icon;
        const isActive = activePreset === preset.id;

        return (
          <button
            key={preset.id}
            onClick={() => onPresetClick(preset.id)}
            disabled={disabled}
            title={preset.description}
            className={`group relative flex items-center justify-center rounded-lg text-xs font-semibold transition-all focus:ring-2 focus:ring-accent/55 focus:outline-hidden ${
              preset.label ? 'gap-1.5 px-2.5 py-1.5 sm:px-3 sm:py-2' : 'size-8 sm:size-9'
            } ${
              disabled
                ? 'cursor-not-allowed opacity-40'
                : isActive
                  ? 'bg-accent text-white shadow-sm ring-1 shadow-accent/20 ring-accent/70'
                  : 'glass-control'
            }`}
          >
            <Icon className="size-3.5 sm:size-4" />
            {preset.label && (
              <>
                <span className="hidden sm:inline">{preset.label}</span>
                <span className="sm:hidden">{preset.label}</span>
              </>
            )}
          </button>
        );
      })}
    </div>
  );
}
