import { type JSX } from 'react';
import { Link } from '@tanstack/react-router';
import { Handle, Position, type NodeProps } from '@xyflow/react';

interface DagNodeBaseProps {
  isHighlighted: boolean;
  isDimmed: boolean;
  label: string;
  type: 'external' | 'transformation' | 'scheduled';
  showLink?: boolean;
}

const nodeConfig = {
  external: {
    badge: 'EXTERNAL',
    color: 'green',
    handleColor: '!bg-green-500',
    highlightedClasses:
      'border-green-400 bg-gradient-to-br from-green-900/60 to-green-800/60 ring-green-400/50 shadow-green-500/30',
    dimmedClasses:
      'border-green-500/20 bg-gradient-to-br from-slate-900/40 to-slate-950/40 opacity-30 ring-green-500/10',
    defaultClasses:
      'border-green-500/50 bg-gradient-to-br from-slate-800 to-slate-900 ring-green-500/30 hover:shadow-green-500/20',
  },
  transformation: {
    badge: 'INCREMENTAL',
    color: 'indigo',
    handleColor: '!bg-indigo-500',
    highlightedClasses:
      'border-indigo-400 bg-gradient-to-br from-indigo-900/60 to-indigo-800/60 ring-indigo-400/50 shadow-indigo-500/30',
    dimmedClasses:
      'border-indigo-500/20 bg-gradient-to-br from-slate-900/40 to-slate-950/40 opacity-30 ring-indigo-500/10',
    defaultClasses:
      'border-indigo-500/50 bg-gradient-to-br from-slate-800 to-slate-900 ring-indigo-500/30 hover:shadow-indigo-500/20',
  },
  scheduled: {
    badge: 'SCHEDULED',
    color: 'emerald',
    handleColor: '!bg-emerald-500',
    highlightedClasses:
      'border-emerald-400 bg-gradient-to-br from-emerald-900/60 to-emerald-800/60 ring-emerald-400/50 shadow-emerald-500/30',
    dimmedClasses:
      'border-emerald-500/20 bg-gradient-to-br from-slate-900/40 to-slate-950/40 opacity-30 ring-emerald-500/10',
    defaultClasses:
      'border-emerald-500/50 bg-gradient-to-br from-slate-800 to-slate-900 ring-emerald-500/30 hover:shadow-emerald-500/20',
  },
};

function DagNodeContent({ isHighlighted, isDimmed, label, type }: DagNodeBaseProps): JSX.Element {
  const config = nodeConfig[type];
  const colorClass = `text-${config.color}-300`;
  const bgClass = `bg-${config.color}-500`;

  const className = `block rounded-lg border-2 p-3 shadow-lg ring-1 backdrop-blur-sm transition-all hover:scale-105 hover:shadow-xl sm:p-4 ${
    isHighlighted ? config.highlightedClasses : isDimmed ? config.dimmedClasses : config.defaultClasses
  }`;

  return (
    <div className={className}>
      <div className="flex items-center gap-1.5 sm:gap-2">
        <div className={`size-1.5 rounded-full sm:size-2 ${bgClass}`} />
        <div className={`font-mono text-xs font-bold ${colorClass}`}>{config.badge}</div>
      </div>
      <div className="mt-1.5 font-mono text-xs font-semibold text-slate-100 sm:mt-2 sm:text-sm" title={label}>
        {label}
      </div>
    </div>
  );
}

export function ExternalNode({ data }: NodeProps): JSX.Element {
  const isHighlighted = data.isHighlighted as boolean;
  const isDimmed = data.isDimmed as boolean;
  const label = data.label as string;
  const showLink = data.showLink === undefined ? true : Boolean(data.showLink);

  return (
    <div className="relative">
      <Handle type="source" position={Position.Bottom} className="!bg-green-500" />
      {showLink ? (
        <Link to="/model/$id" params={{ id: encodeURIComponent(label) }}>
          <DagNodeContent isHighlighted={isHighlighted} isDimmed={isDimmed} label={label} type="external" />
        </Link>
      ) : (
        <DagNodeContent isHighlighted={isHighlighted} isDimmed={isDimmed} label={label} type="external" />
      )}
    </div>
  );
}

export function TransformationNode({ data }: NodeProps): JSX.Element {
  const isHighlighted = data.isHighlighted as boolean;
  const isDimmed = data.isDimmed as boolean;
  const label = data.label as string;
  const showLink = data.showLink === undefined ? true : Boolean(data.showLink);

  return (
    <div className="relative">
      <Handle type="target" position={Position.Top} className="!bg-indigo-500" />
      <Handle type="source" position={Position.Bottom} className="!bg-indigo-500" />
      {showLink ? (
        <Link to="/model/$id" params={{ id: encodeURIComponent(label) }}>
          <DagNodeContent isHighlighted={isHighlighted} isDimmed={isDimmed} label={label} type="transformation" />
        </Link>
      ) : (
        <DagNodeContent isHighlighted={isHighlighted} isDimmed={isDimmed} label={label} type="transformation" />
      )}
    </div>
  );
}

export function ScheduledNode({ data }: NodeProps): JSX.Element {
  const isHighlighted = data.isHighlighted as boolean;
  const isDimmed = data.isDimmed as boolean;
  const label = data.label as string;
  const showLink = data.showLink === undefined ? true : Boolean(data.showLink);

  return (
    <div className="relative">
      <Handle type="target" position={Position.Top} className="!bg-emerald-500" />
      <Handle type="source" position={Position.Bottom} className="!bg-emerald-500" />
      {showLink ? (
        <Link to="/model/$id" params={{ id: encodeURIComponent(label) }}>
          <DagNodeContent isHighlighted={isHighlighted} isDimmed={isDimmed} label={label} type="scheduled" />
        </Link>
      ) : (
        <DagNodeContent isHighlighted={isHighlighted} isDimmed={isDimmed} label={label} type="scheduled" />
      )}
    </div>
  );
}
