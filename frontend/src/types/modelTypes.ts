/**
 * Shared configuration for model types across the application
 */

export type ModelType = 'external' | 'scheduled' | 'incremental';

export interface TypeConfig {
  gradient: string;
  badgeBg: string;
  badgeText: string;
  badgeRing: string;
  label: string;
  color: 'green' | 'emerald' | 'indigo';
  handleColor?: string;
  highlightedClasses?: string;
  dimmedClasses?: string;
  defaultClasses?: string;
}

export const MODEL_TYPE_CONFIG: Record<ModelType, TypeConfig> = {
  external: {
    gradient: 'from-green-400 via-emerald-400 to-green-400',
    badgeBg: 'bg-green-500/20',
    badgeText: 'text-green-300',
    badgeRing: 'ring-green-500/50',
    label: 'EXTERNAL',
    color: 'green',
    handleColor: '!bg-green-500',
    highlightedClasses:
      'border-green-400 bg-gradient-to-br from-green-900/60 to-green-800/60 ring-green-400/50 shadow-green-500/30',
    dimmedClasses:
      'border-green-500/20 bg-gradient-to-br from-slate-900/40 to-slate-950/40 opacity-30 ring-green-500/10',
    defaultClasses:
      'border-green-500/50 bg-gradient-to-br from-slate-800 to-slate-900 ring-green-500/30 hover:shadow-green-500/20',
  },
  scheduled: {
    gradient: 'from-emerald-400 via-teal-400 to-emerald-400',
    badgeBg: 'bg-emerald-500/20',
    badgeText: 'text-emerald-300',
    badgeRing: 'ring-emerald-500/50',
    label: 'SCHEDULED',
    color: 'emerald',
    handleColor: '!bg-emerald-500',
    highlightedClasses:
      'border-emerald-400 bg-gradient-to-br from-emerald-900/60 to-emerald-800/60 ring-emerald-400/50 shadow-emerald-500/30',
    dimmedClasses:
      'border-emerald-500/20 bg-gradient-to-br from-slate-900/40 to-slate-950/40 opacity-30 ring-emerald-500/10',
    defaultClasses:
      'border-emerald-500/50 bg-gradient-to-br from-slate-800 to-slate-900 ring-emerald-500/30 hover:shadow-emerald-500/20',
  },
  incremental: {
    gradient: 'from-indigo-400 via-purple-400 to-indigo-400',
    badgeBg: 'bg-indigo-500/20',
    badgeText: 'text-indigo-300',
    badgeRing: 'ring-indigo-500/50',
    label: 'INCREMENTAL',
    color: 'indigo',
    handleColor: '!bg-indigo-500',
    highlightedClasses:
      'border-indigo-400 bg-gradient-to-br from-indigo-900/60 to-indigo-800/60 ring-indigo-400/50 shadow-indigo-500/30',
    dimmedClasses:
      'border-indigo-500/20 bg-gradient-to-br from-slate-900/40 to-slate-950/40 opacity-30 ring-indigo-500/10',
    defaultClasses:
      'border-indigo-500/50 bg-gradient-to-br from-slate-800 to-slate-900 ring-indigo-500/30 hover:shadow-indigo-500/20',
  },
};
