import { type JSX } from 'react';
import { Link } from '@tanstack/react-router';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import { ArrowLeftIcon, CodeBracketIcon, CommandLineIcon, ClockIcon } from '@heroicons/react/24/outline';
import { MODEL_TYPE_CONFIG, type ModelType } from '@types';
import type { ExternalModel, TransformationModel, IntervalTypeTransformation } from '@api/types.gen';
import { transformValue, formatValue } from '@utils/interval-transform';
import { getNextRunDescription } from '@utils/schedule-parser';

interface DagNodeData {
  isHighlighted: boolean;
  isDimmed: boolean;
  label: string;
  model?: ExternalModel | TransformationModel;
  transformation?: IntervalTypeTransformation;
}

interface DagNodeContentProps extends DagNodeData {
  type: ModelType;
}

function DagNodeContent({
  isHighlighted,
  isDimmed,
  label,
  type,
  model,
  transformation,
}: DagNodeContentProps): JSX.Element {
  const config = MODEL_TYPE_CONFIG[type];

  // Compact design - reduced padding and tighter spacing
  const baseClasses = `group relative block rounded-md border px-2 py-1.5 shadow-md ring-1 backdrop-blur-sm transition-all hover:scale-[1.02] hover:shadow-lg`;

  // Extract model-specific info
  const isTransformation = model && 'type' in model && (model.type === 'incremental' || model.type === 'scheduled');
  const isIncremental = isTransformation && model.type === 'incremental';
  const isScheduled = isTransformation && model.type === 'scheduled';

  // Get interval type for both external and incremental models
  const intervalType = model && 'interval' in model ? model.interval?.type : undefined;
  const hasBackfill = isIncremental && 'schedules' in model && model.schedules?.backfill;

  // Get actual data ranges
  // For external: use bounds, for incremental: calculate from coverage
  let dataMax: number | undefined;

  if (
    model &&
    'bounds' in model &&
    model.bounds &&
    typeof model.bounds === 'object' &&
    'max' in model.bounds &&
    typeof model.bounds.max === 'number'
  ) {
    // External model - use bounds
    dataMax = model.bounds.max;
  } else if (
    isIncremental &&
    'coverage' in model &&
    model.coverage &&
    Array.isArray(model.coverage) &&
    model.coverage.length > 0
  ) {
    // Incremental model - calculate from coverage
    const coverage = model.coverage as Array<{ position: number; interval: number }>;
    dataMax = Math.max(...coverage.map(c => c.position + c.interval));
  }

  // Apply transformation if available - only show max for sanity
  const displayMax =
    dataMax !== undefined && transformation
      ? formatValue(transformValue(dataMax, transformation), transformation.format)
      : dataMax?.toLocaleString();

  // Get content type for transformations
  const contentType = isTransformation ? model.content_type : null;

  // Get schedule for scheduled models
  const schedule = isScheduled && 'schedule' in model ? model.schedule : null;
  // Note: We don't have lastRun in DagNode context, so next run will be calculated from now for @every schedules
  const nextRunText = schedule ? getNextRunDescription(schedule) : null;

  // Add interval type indicator to border
  const borderClass = intervalType
    ? intervalType === 'slot'
      ? 'border-l-4 border-l-blue-400'
      : intervalType === 'epoch'
        ? 'border-l-4 border-l-purple-400'
        : 'border-l-4 border-l-amber-400'
    : '';

  const className = `${baseClasses} ${borderClass} ${
    isHighlighted ? config.highlightedClasses : isDimmed ? config.dimmedClasses : config.defaultClasses
  }`;

  return (
    <Link to="/model/$id" params={{ id: encodeURIComponent(label) }} className={className}>
      {/* Header - more compact */}
      <div className="flex items-center gap-1.5">
        <div className={`size-1.5 shrink-0 rounded-full bg-${config.color}-500`} />
        <div className={`text-[10px] font-bold text-${config.color}-300`}>{config.label}</div>
      </div>

      {/* Model ID - smaller font */}
      <div className="mt-1 truncate font-mono text-[11px] font-semibold leading-tight text-slate-100" title={label}>
        {label}
      </div>

      {/* Additional Info - more compact */}
      {model && (
        <div className="mt-1.5 flex flex-col gap-1">
          {/* Data range - show max with transformation name as unit */}
          {displayMax && (
            <div className="text-[9px] font-medium text-slate-300">
              {transformation?.name ? `${transformation.name} ${displayMax}` : displayMax}
            </div>
          )}

          {/* Backfill + Content type on same line */}
          <div className="flex items-center gap-1.5">
            {/* Backfill indicator for incremental */}
            {isIncremental && hasBackfill && (
              <div className="flex items-center gap-0.5 text-[9px] text-purple-400" title="Backfill enabled">
                <ArrowLeftIcon className="size-2.5" />
                <span>Backfilling</span>
              </div>
            )}

            {/* Content type for transformations */}
            {contentType && (
              <div className="flex items-center gap-0.5 text-[9px] text-slate-400" title={`Type: ${contentType}`}>
                {contentType === 'sql' ? (
                  <>
                    <CodeBracketIcon className="size-2.5" />
                    <span>SQL</span>
                  </>
                ) : (
                  <>
                    <CommandLineIcon className="size-2.5" />
                    <span>EXEC</span>
                  </>
                )}
              </div>
            )}
          </div>

          {/* Schedule for scheduled models */}
          {isScheduled && schedule && (
            <div className="flex flex-col gap-0.5">
              <div className="flex items-center gap-0.5 text-[9px] text-emerald-400" title={schedule}>
                <ClockIcon className="size-2.5" />
                <span className="truncate">{schedule}</span>
              </div>
              {nextRunText && (
                <div className="text-[8px] font-medium text-emerald-300" title="Next scheduled run">
                  Next: {nextRunText}
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {/* Subtle hover indicator */}
      <div
        className={`absolute -inset-[1px] rounded-md bg-${config.color}-400/0 transition-all group-hover:bg-${config.color}-400/5`}
      />
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
  const model = data.model as ExternalModel | TransformationModel | undefined;
  const transformation = data.transformation as IntervalTypeTransformation | undefined;
  const config = MODEL_TYPE_CONFIG[type];

  return (
    <div className="relative">
      {hasTargetHandle && <Handle type="target" position={Position.Top} className={config.handleColor} />}
      <DagNodeContent
        isHighlighted={isHighlighted}
        isDimmed={isDimmed}
        label={label}
        type={type}
        model={model}
        transformation={transformation}
      />
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
