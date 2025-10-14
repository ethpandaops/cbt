import { type JSX } from 'react';
import { ZOOM_PRESETS } from './zoom-presets-config';

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
            className={`group relative flex items-center justify-center rounded-lg text-xs font-semibold transition-all ${
              preset.label ? 'gap-1.5 px-2.5 py-1.5 sm:px-3 sm:py-2' : 'size-8 sm:size-9'
            } ${
              disabled
                ? 'cursor-not-allowed opacity-40'
                : isActive
                  ? 'bg-indigo-500/20 text-indigo-300 ring-1 ring-indigo-500/50'
                  : 'bg-slate-800/60 text-slate-400 ring-1 ring-slate-700/50 hover:bg-slate-700/60 hover:text-slate-300 hover:ring-slate-600/50'
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
