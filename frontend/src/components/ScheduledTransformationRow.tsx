import { type JSX } from 'react';
import { Link } from '@tanstack/react-router';
import type { TransformationModel } from '@api/types.gen';
import { timeAgo } from '@utils/time';
import { getNextRunDescription, formatNextRun } from '@utils/schedule-parser';
import { getOrGroupColor } from '@utils/or-group-colors';

export interface ScheduledTransformationRowProps {
  model: TransformationModel;
  lastRun?: string;
  isHighlighted?: boolean;
  isDimmed?: boolean;
  orGroups?: number[];
  onMouseEnter?: () => void;
  onMouseLeave?: () => void;
}

export function ScheduledTransformationRow({
  model,
  lastRun,
  isHighlighted = false,
  isDimmed = false,
  orGroups,
  onMouseEnter,
  onMouseLeave,
}: ScheduledTransformationRowProps): JSX.Element {
  const nextRunText = getNextRunDescription(model.schedule, lastRun);
  const nextRunFormatted = formatNextRun(model.schedule, lastRun);
  const isOverdue = nextRunText?.startsWith('OVERDUE');

  // Get status from metadata
  const status = model.metadata?.last_run_status;
  const lastRunAt = model.metadata?.last_run_at || lastRun;

  // Check if lastRunAt is valid (not 1970 epoch and not missing)
  const isValidLastRun = lastRunAt && new Date(lastRunAt).getFullYear() !== 1970;

  // Status indicator colors
  const statusColors = {
    success: 'bg-emerald-500/20 text-emerald-300 ring-emerald-500/50',
    failed: 'bg-red-500/20 text-red-300 ring-red-500/50',
    running: 'bg-blue-500/20 text-blue-300 ring-blue-500/50',
    pending: 'bg-amber-500/20 text-amber-300 ring-amber-500/50',
  };

  return (
    <Link
      to="/model/$id"
      params={{ id: encodeURIComponent(model.id) }}
      className={`group/row block rounded-xl border border-emerald-500/20 bg-slate-800/60 transition-all hover:border-emerald-500/40 hover:bg-slate-800/80 hover:shadow-lg hover:shadow-emerald-500/10 ${
        isHighlighted ? 'brightness-125 ring-2 ring-emerald-500/50' : isDimmed ? 'opacity-40' : ''
      }`}
      data-model-id={model.id}
      onMouseEnter={onMouseEnter}
      onMouseLeave={onMouseLeave}
    >
      <div className="relative p-4">
        <div
          className={`grid grid-cols-1 gap-3 ${isValidLastRun ? 'lg:grid-cols-[2fr_1fr_1fr_1fr]' : 'lg:grid-cols-[2fr_1fr_1fr]'} lg:items-start`}
        >
          {/* Model ID and Dependencies */}
          <div className="flex min-w-0 flex-col gap-1.5">
            <div className="flex items-center gap-2">
              <span
                className={`truncate font-mono text-sm font-bold transition-colors ${
                  isHighlighted
                    ? 'text-white'
                    : isDimmed
                      ? 'text-slate-500'
                      : 'text-slate-200 group-hover/row:text-emerald-400'
                }`}
                title={model.id}
              >
                {model.id}
              </span>
              {status && (
                <span
                  className={`rounded px-2 py-0.5 text-[10px] font-bold uppercase ring-1 ${statusColors[status] || statusColors.pending}`}
                >
                  {status}
                </span>
              )}
            </div>
            {orGroups && orGroups.length > 0 && (
              <div className="flex items-center gap-1.5">
                <span className="text-[10px] font-medium text-slate-400">Dependencies:</span>
                <div className="flex items-center gap-1">
                  {orGroups.map(groupId => {
                    const colors = getOrGroupColor(groupId);
                    return (
                      <span
                        key={groupId}
                        className={`rounded px-1.5 py-0.5 text-[9px] font-semibold leading-tight ring-1 ${colors.bg} ${colors.text} ${colors.ring}`}
                      >
                        OR #{groupId}
                      </span>
                    );
                  })}
                </div>
              </div>
            )}
            {model.description && (
              <span className="text-xs text-slate-400 line-clamp-1" title={model.description}>
                {model.description}
              </span>
            )}
          </div>

          {/* Schedule */}
          <div className="flex flex-col gap-1">
            <span className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Schedule</span>
            <span className="font-mono text-xs font-medium text-emerald-300/90" title={model.schedule}>
              {model.schedule || 'Not set'}
            </span>
          </div>

          {/* Last Run - only show if valid */}
          {isValidLastRun && (
            <div className="flex flex-col gap-1">
              <span className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Last Run</span>
              <span className="text-xs font-semibold text-slate-300" title={lastRunAt}>
                {timeAgo(lastRunAt)}
              </span>
            </div>
          )}

          {/* Next Run */}
          <div className="flex flex-col gap-1">
            <span className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Next Run</span>
            <div className="flex flex-col gap-0.5">
              {nextRunText && (
                <span className={`text-xs font-bold ${isOverdue ? 'text-red-300' : 'text-emerald-300'}`}>
                  {nextRunText}
                </span>
              )}
              {nextRunFormatted && (
                <span className="text-[10px] text-slate-400" title={nextRunFormatted}>
                  {isOverdue ? `Was due: ${nextRunFormatted}` : nextRunFormatted}
                </span>
              )}
            </div>
          </div>
        </div>
      </div>
    </Link>
  );
}
