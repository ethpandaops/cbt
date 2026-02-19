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
        'rounded-xl bg-linear-to-br from-external/16 via-external/8 to-external/4 p-4 ring-1 ring-external/35 shadow-xs',
      label: 'mb-1 font-semibold text-external',
      value: 'font-mono text-base font-bold text-external',
    },
    incremental: {
      field:
        'rounded-xl bg-linear-to-br from-incremental/15 via-incremental/8 to-incremental/4 p-4 ring-1 ring-incremental/35 shadow-xs',
      label: 'mb-1 font-semibold text-incremental',
      value: 'font-mono text-base font-bold text-incremental',
    },
    scheduled: {
      field:
        'rounded-xl bg-linear-to-br from-scheduled/16 via-scheduled/8 to-scheduled/4 p-4 ring-1 ring-scheduled/35 shadow-xs',
      label: 'mb-1 font-semibold text-scheduled',
      value: 'font-mono text-base font-bold text-scheduled',
    },
    warning: {
      field:
        'rounded-xl bg-linear-to-br from-warning/16 via-warning/8 to-warning/4 p-4 ring-1 ring-warning/35 shadow-xs',
      label: 'mb-1 font-semibold text-warning',
      value: 'font-mono text-base font-bold text-warning',
    },
    accent: {
      field: 'rounded-xl bg-linear-to-br from-accent/16 via-accent/8 to-accent/4 p-4 ring-1 ring-accent/35 shadow-xs',
      label: 'mb-1 font-semibold text-accent',
      value: 'font-mono text-base font-bold text-accent',
    },
  };

  const getFieldClassName = (field: InfoField): string => {
    if (field.variant === 'highlight') {
      return highlightStyles[field.highlightColor || 'incremental']?.field ?? highlightStyles.incremental.field;
    }
    return 'rounded-xl border border-border/45 bg-surface/74 p-4 ring-1 ring-border/30';
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
    <div className={`glass-surface border ${borderColor} p-4 sm:p-6`}>
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
