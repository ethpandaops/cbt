import { type JSX } from 'react';
import { Link } from '@tanstack/react-router';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import { MODEL_TYPE_CONFIG, type ModelType } from '@/types';

interface DagNodeData {
  isHighlighted: boolean;
  isDimmed: boolean;
  label: string;
}

interface DagNodeContentProps extends DagNodeData {
  type: ModelType;
}

function DagNodeContent({ isHighlighted, isDimmed, label, type }: DagNodeContentProps): JSX.Element {
  const config = MODEL_TYPE_CONFIG[type];
  const colorClass = `text-${config.color}-300`;
  const bgClass = `bg-${config.color}-500`;

  const className = `block rounded-lg border-2 p-3 shadow-lg ring-1 backdrop-blur-sm transition-all hover:scale-105 hover:shadow-xl sm:p-4 ${
    isHighlighted ? config.highlightedClasses : isDimmed ? config.dimmedClasses : config.defaultClasses
  }`;

  return (
    <Link to="/model/$id" params={{ id: encodeURIComponent(label) }} className={className}>
      <div className="flex items-center gap-1.5 sm:gap-2">
        <div className={`size-1.5 rounded-full sm:size-2 ${bgClass}`} />
        <div className={`font-mono text-xs font-bold ${colorClass}`}>{config.label}</div>
      </div>
      <div className="mt-1.5 font-mono text-xs font-semibold text-slate-100 sm:mt-2 sm:text-sm/4" title={label}>
        {label}
      </div>
    </Link>
  );
}

interface DagNodeBaseProps extends NodeProps {
  type: ModelType;
  hasSourceHandle?: boolean;
  hasTargetHandle?: boolean;
}

function DagNode({ data, type, hasSourceHandle = false, hasTargetHandle = false }: DagNodeBaseProps): JSX.Element {
  const isHighlighted = data.isHighlighted as boolean;
  const isDimmed = data.isDimmed as boolean;
  const label = data.label as string;
  const config = MODEL_TYPE_CONFIG[type];

  return (
    <div className="relative">
      {hasTargetHandle && <Handle type="target" position={Position.Top} className={config.handleColor} />}
      <DagNodeContent isHighlighted={isHighlighted} isDimmed={isDimmed} label={label} type={type} />
      {hasSourceHandle && <Handle type="source" position={Position.Bottom} className={config.handleColor} />}
    </div>
  );
}

export function ExternalNode(props: NodeProps): JSX.Element {
  return <DagNode {...props} type="external" hasSourceHandle />;
}

export function TransformationNode(props: NodeProps): JSX.Element {
  return <DagNode {...props} type="incremental" hasSourceHandle hasTargetHandle />;
}

export function ScheduledNode(props: NodeProps): JSX.Element {
  return <DagNode {...props} type="scheduled" hasSourceHandle hasTargetHandle />;
}
