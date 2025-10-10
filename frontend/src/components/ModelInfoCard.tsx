import { type JSX, type ReactNode } from 'react';

export interface InfoField {
  label: string;
  value: ReactNode;
  variant?: 'default' | 'highlight';
  highlightColor?: 'green' | 'indigo' | 'emerald';
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
  borderColor = 'border-indigo-500/30',
  columns = 2,
}: ModelInfoCardProps): JSX.Element {
  const gridCols = columns === 4 ? 'grid-cols-1 sm:grid-cols-2 lg:grid-cols-4' : 'grid-cols-1 sm:grid-cols-2';

  const getFieldClassName = (field: InfoField): string => {
    if (field.variant === 'highlight') {
      const color = field.highlightColor || 'indigo';
      if (color === 'green') {
        return 'rounded-lg bg-green-500/10 p-4 ring-1 ring-green-500/50';
      }
      if (color === 'emerald') {
        return 'rounded-lg bg-emerald-500/10 p-4 ring-1 ring-emerald-500/50';
      }
      return 'rounded-lg bg-indigo-500/10 p-4 ring-1 ring-indigo-500/50';
    }
    return 'rounded-lg bg-slate-900/60 p-4';
  };

  const getLabelClassName = (field: InfoField): string => {
    if (field.variant === 'highlight') {
      const color = field.highlightColor || 'indigo';
      if (color === 'green') {
        return 'mb-1 font-semibold text-green-400';
      }
      if (color === 'emerald') {
        return 'mb-1 font-semibold text-emerald-400';
      }
      return 'mb-1 font-semibold text-indigo-400';
    }
    return 'mb-1 font-semibold text-slate-400';
  };

  const getValueClassName = (field: InfoField): string => {
    if (field.variant === 'highlight') {
      const color = field.highlightColor || 'indigo';
      if (color === 'green') {
        return 'font-mono text-base font-bold text-green-200';
      }
      if (color === 'emerald') {
        return 'font-mono text-base font-bold text-emerald-200';
      }
      return 'font-mono text-base font-bold text-indigo-200';
    }
    return 'font-mono text-base font-bold text-slate-100';
  };

  return (
    <div
      className={`rounded-2xl border ${borderColor} bg-slate-800/80 p-4 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm sm:p-6`}
    >
      <h2 className="mb-3 text-base font-bold text-slate-100 sm:mb-4 sm:text-lg">{title}</h2>
      <dl className={`grid ${gridCols} gap-x-4 gap-y-3 text-sm sm:gap-x-6 sm:gap-y-4`}>
        {fields.map((field, index) => (
          <div key={index} className={getFieldClassName(field)}>
            <dt className={getLabelClassName(field)}>{field.label}</dt>
            <dd className={getValueClassName(field)}>{field.value}</dd>
          </div>
        ))}
      </dl>
    </div>
  );
}
