import { type JSX } from 'react';
import { ZOOM_PRESETS } from './zoom-presets-config';

export interface ZoomPresetsProps {
  activePreset?: 'all' | 'fit' | 'recent' | null;
  onPresetClick: (presetId: 'all' | 'fit' | 'recent') => void;
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
            className={`group relative flex items-center gap-1.5 rounded-lg px-2.5 py-1.5 text-xs font-semibold transition-all sm:px-3 sm:py-2 ${
              disabled
                ? 'cursor-not-allowed opacity-40'
                : isActive
                  ? 'bg-indigo-500/20 text-indigo-300 ring-1 ring-indigo-500/50'
                  : 'bg-slate-800/60 text-slate-400 ring-1 ring-slate-700/50 hover:bg-slate-700/60 hover:text-slate-300 hover:ring-slate-600/50'
            }`}
          >
            <Icon className="size-3.5 sm:size-4" />
            <span className="hidden sm:inline">{preset.label}</span>
            <span className="sm:hidden">{preset.label}</span>
          </button>
        );
      })}
    </div>
  );
}
