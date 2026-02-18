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
  // Get status from metadata
  const status = model.metadata?.last_run_status;
  const lastRunAt = model.metadata?.last_run_at || lastRun;

  // Check if lastRunAt is valid (not 1970 epoch and not missing)
  const isValidLastRun = lastRunAt && new Date(lastRunAt).getFullYear() !== 1970;

  // Only calculate next run if we have a valid last run
  const nextRunText = isValidLastRun ? getNextRunDescription(model.schedule, lastRunAt) : null;
  const nextRunFormatted = isValidLastRun ? formatNextRun(model.schedule, lastRunAt) : null;
  const isOverdue = nextRunText?.startsWith('OVERDUE');

  // Status indicator colors
  const statusColors = {
    success: 'bg-scheduled/20 text-scheduled ring-scheduled/50',
    failed: 'bg-danger/20 text-danger ring-danger/50',
    running: 'bg-accent/20 text-accent ring-accent/50',
    pending: 'bg-warning/20 text-warning ring-warning/50',
  };

  return (
    <Link
      to="/model/$id"
      params={{ id: encodeURIComponent(model.id) }}
      className={`group/row block rounded-xl border border-border/75 bg-linear-to-br from-surface/95 via-surface/88 to-secondary/30 transition-all hover:border-scheduled/45 hover:bg-surface hover:shadow-lg hover:shadow-scheduled/10 ${
        isHighlighted ? 'ring-2 ring-scheduled/50 brightness-125' : isDimmed ? 'opacity-40' : ''
      }`}
      data-model-id={model.id}
      onMouseEnter={onMouseEnter}
      onMouseLeave={onMouseLeave}
    >
      <div className="relative p-4">
        <div className="grid grid-cols-1 gap-3 lg:grid-cols-[2fr_1fr_1fr_1fr] lg:items-start">
          {/* Model ID and Dependencies */}
          <div className="flex min-w-0 flex-col gap-1.5">
            <div className="flex items-center gap-2">
              <span
                className={`truncate font-mono text-sm font-bold transition-colors ${
                  isHighlighted
                    ? 'text-primary'
                    : isDimmed
                      ? 'text-muted'
                      : 'text-foreground group-hover/row:text-scheduled'
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
                <span className="text-[10px] font-medium text-muted">Dependencies:</span>
                <div className="flex items-center gap-1">
                  {orGroups.map(groupId => {
                    const colors = getOrGroupColor(groupId);
                    return (
                      <span
                        key={groupId}
                        className={`rounded px-1.5 py-0.5 text-[9px] leading-tight font-semibold ring-1 ${colors.bg} ${colors.text} ${colors.ring}`}
                      >
                        OR #{groupId}
                      </span>
                    );
                  })}
                </div>
              </div>
            )}
            {model.description && (
              <span className="line-clamp-1 text-xs text-muted" title={model.description}>
                {model.description}
              </span>
            )}
          </div>

          {/* Schedule */}
          <div className="flex flex-col gap-1">
            <span className="text-[10px] font-semibold tracking-wide text-muted uppercase">Schedule</span>
            <span className="font-mono text-xs font-medium text-scheduled" title={model.schedule}>
              {model.schedule || 'Not set'}
            </span>
          </div>

          {/* Last Run */}
          <div className="flex flex-col gap-1">
            <span className="text-[10px] font-semibold tracking-wide text-muted uppercase">Last Run</span>
            <span className="text-xs font-semibold text-foreground" title={lastRunAt || 'N/A'}>
              {isValidLastRun ? timeAgo(lastRunAt) : 'N/A'}
            </span>
          </div>

          {/* Next Run */}
          <div className="flex flex-col gap-1">
            <span className="text-[10px] font-semibold tracking-wide text-muted uppercase">Next Run</span>
            <div className="flex flex-col gap-0.5">
              {nextRunText ? (
                <>
                  <span className={`text-xs font-bold ${isOverdue ? 'text-danger' : 'text-scheduled'}`}>
                    {nextRunText}
                  </span>
                  {nextRunFormatted && (
                    <span className="text-[10px] text-muted" title={nextRunFormatted}>
                      {isOverdue ? `Was due: ${nextRunFormatted}` : nextRunFormatted}
                    </span>
                  )}
                </>
              ) : (
                <span className="text-xs font-semibold text-foreground">N/A</span>
              )}
            </div>
          </div>
        </div>
      </div>
    </Link>
  );
}
