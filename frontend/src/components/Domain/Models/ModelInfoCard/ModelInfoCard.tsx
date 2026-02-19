import { type JSX, type ReactNode } from 'react';

export interface InfoField {
  label: string;
  value: ReactNode;
  variant?: 'default' | 'highlight';
  highlightColor?: 'external' | 'incremental' | 'scheduled' | 'warning' | 'accent';
  overridden?: boolean;
  originalValue?: ReactNode;
}

export interface ModelInfoCardProps {
  title: string;
  fields: InfoField[];
  borderColor?: string;
  columns?: 2 | 4;
}

export function ModelInfoCard({
  title,
  fields,
  borderColor = 'border-incremental/30',
  columns = 2,
}: ModelInfoCardProps): JSX.Element {
  const gridCols = columns === 4 ? 'grid-cols-1 sm:grid-cols-2 lg:grid-cols-4' : 'grid-cols-1 sm:grid-cols-2';

  const highlightStyles: Record<string, { field: string; label: string; value: string }> = {
    external: {
      field:
        'rounded-xl bg-linear-to-br from-external/20 via-external/10 to-external/5 p-4 ring-1 ring-external/45 shadow-sm',
      label: 'mb-1 font-semibold text-external',
      value: 'font-mono text-base font-bold text-external',
    },
    incremental: {
      field:
        'rounded-xl bg-linear-to-br from-incremental/18 via-incremental/10 to-incremental/5 p-4 ring-1 ring-incremental/45 shadow-sm',
      label: 'mb-1 font-semibold text-incremental',
      value: 'font-mono text-base font-bold text-incremental',
    },
    scheduled: {
      field:
        'rounded-xl bg-linear-to-br from-scheduled/20 via-scheduled/10 to-scheduled/5 p-4 ring-1 ring-scheduled/45 shadow-sm',
      label: 'mb-1 font-semibold text-scheduled',
      value: 'font-mono text-base font-bold text-scheduled',
    },
    warning: {
      field:
        'rounded-xl bg-linear-to-br from-warning/20 via-warning/10 to-warning/5 p-4 ring-1 ring-warning/45 shadow-sm',
      label: 'mb-1 font-semibold text-warning',
      value: 'font-mono text-base font-bold text-warning',
    },
    accent: {
      field: 'rounded-xl bg-linear-to-br from-accent/20 via-accent/10 to-accent/5 p-4 ring-1 ring-accent/45 shadow-sm',
      label: 'mb-1 font-semibold text-accent',
      value: 'font-mono text-base font-bold text-accent',
    },
  };

  const getFieldClassName = (field: InfoField): string => {
    if (field.variant === 'highlight') {
      return highlightStyles[field.highlightColor || 'incremental']?.field ?? highlightStyles.incremental.field;
    }
    return 'rounded-xl bg-background/66 p-4 ring-1 ring-border/50';
  };

  const getLabelClassName = (field: InfoField): string => {
    if (field.variant === 'highlight') {
      return highlightStyles[field.highlightColor || 'incremental']?.label ?? highlightStyles.incremental.label;
    }
    return 'mb-1 font-semibold text-muted';
  };

  const getValueClassName = (field: InfoField): string => {
    if (field.variant === 'highlight') {
      return highlightStyles[field.highlightColor || 'incremental']?.value ?? highlightStyles.incremental.value;
    }
    return 'font-mono text-base font-bold text-foreground';
  };

  return (
    <div
      className={`rounded-2xl border ${borderColor} bg-linear-to-br from-surface/95 via-surface/86 to-secondary/32 p-4 shadow-lg ring-1 ring-border/55 backdrop-blur-sm sm:p-6`}
    >
      <h2 className="mb-3 text-base font-bold text-foreground sm:mb-4 sm:text-lg">{title}</h2>
      <dl className={`grid ${gridCols} gap-x-4 gap-y-3 text-sm sm:gap-x-6 sm:gap-y-4`}>
        {fields.map((field, index) => (
          <div
            key={index}
            className={`${getFieldClassName(field)} min-w-0 ${field.overridden ? 'ring-danger/40' : ''}`}
          >
            <dt className={`${getLabelClassName(field)} flex items-center gap-1.5`}>
              {field.label}
              {field.overridden && (
                <span className="rounded-sm bg-danger/15 px-1.5 py-0.5 text-[10px] font-bold tracking-wide text-danger uppercase">
                  Live
                </span>
              )}
            </dt>
            <dd
              className={`${getValueClassName(field)} truncate`}
              title={typeof field.value === 'string' ? field.value : undefined}
            >
              {field.value}
            </dd>
            {field.overridden && field.originalValue != null && (
              <dd className="mt-1 truncate text-xs text-muted" title="Original value">
                was: {field.originalValue}
              </dd>
            )}
          </div>
        ))}
      </dl>
    </div>
  );
}
