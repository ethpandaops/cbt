/**
 * Shared configuration for model types across the application.
 *
 * Uses semantic color tokens defined in index.css (external, scheduled, incremental).
 */

export type ModelType = 'external' | 'scheduled' | 'incremental';

export interface TypeConfig {
  gradient: string;
  badgeBg: string;
  badgeText: string;
  badgeRing: string;
  label: string;
  dotColor: string;
  labelColor: string;
  handleColor?: string;
  hoverOverlay?: string;
  highlightedClasses?: string;
  dimmedClasses?: string;
  defaultClasses?: string;
}

export const MODEL_TYPE_CONFIG: Record<ModelType, TypeConfig> = {
  external: {
    gradient: 'from-external via-external/80 to-external',
    badgeBg: 'bg-external/22 dark:bg-external/20',
    badgeText: 'text-emerald-900 dark:text-external',
    badgeRing: 'ring-external/50',
    label: 'EXTERNAL',
    dotColor: 'bg-external',
    labelColor: 'text-emerald-800 dark:text-external',
    handleColor: '!bg-external',
    hoverOverlay: 'bg-external/0 group-hover:bg-external/5',
    highlightedClasses:
      'border-external bg-linear-to-br from-external/26 via-external/18 to-external/10 ring-external/55 shadow-external/30',
    dimmedClasses:
      'border-external/20 bg-linear-to-br from-background/45 to-background/70 opacity-35 ring-external/10 dark:opacity-30',
    defaultClasses:
      'border-external/55 bg-linear-to-br from-surface via-surface/90 to-secondary/35 ring-external/35 hover:shadow-external/20',
  },
  scheduled: {
    gradient: 'from-scheduled via-scheduled/80 to-scheduled',
    badgeBg: 'bg-scheduled/22 dark:bg-scheduled/20',
    badgeText: 'text-teal-900 dark:text-scheduled',
    badgeRing: 'ring-scheduled/50',
    label: 'SCHEDULED',
    dotColor: 'bg-scheduled',
    labelColor: 'text-teal-800 dark:text-scheduled',
    handleColor: '!bg-scheduled',
    hoverOverlay: 'bg-scheduled/0 group-hover:bg-scheduled/5',
    highlightedClasses:
      'border-scheduled bg-linear-to-br from-scheduled/26 via-scheduled/18 to-scheduled/10 ring-scheduled/55 shadow-scheduled/30',
    dimmedClasses:
      'border-scheduled/20 bg-linear-to-br from-background/45 to-background/70 opacity-35 ring-scheduled/10 dark:opacity-30',
    defaultClasses:
      'border-scheduled/55 bg-linear-to-br from-surface via-surface/90 to-secondary/35 ring-scheduled/35 hover:shadow-scheduled/20',
  },
  incremental: {
    gradient: 'from-incremental via-incremental/80 to-incremental',
    badgeBg: 'bg-incremental/22 dark:bg-incremental/20',
    badgeText: 'text-blue-900 dark:text-incremental',
    badgeRing: 'ring-incremental/50',
    label: 'INCREMENTAL',
    dotColor: 'bg-incremental',
    labelColor: 'text-blue-800 dark:text-incremental',
    handleColor: '!bg-incremental',
    hoverOverlay: 'bg-incremental/0 group-hover:bg-incremental/5',
    highlightedClasses:
      'border-incremental bg-linear-to-br from-incremental/26 via-incremental/18 to-incremental/10 ring-incremental/55 shadow-incremental/30',
    dimmedClasses:
      'border-incremental/20 bg-linear-to-br from-background/45 to-background/70 opacity-35 ring-incremental/10 dark:opacity-30',
    defaultClasses:
      'border-incremental/55 bg-linear-to-br from-surface via-surface/90 to-secondary/35 ring-incremental/35 hover:shadow-incremental/20',
  },
};
