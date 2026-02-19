import { type JSX } from 'react';
import { Link } from '@tanstack/react-router';
import { PencilSquareIcon, StopCircleIcon } from '@heroicons/react/24/outline';
import type { TransformationModel } from '@/api/types.gen';
import { timeAgo } from '@/utils/time';
import { getNextRunDescription, formatNextRun } from '@/utils/schedule-parser';
import { getOrGroupColor } from '@/utils/or-group-colors';

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
  const showDisabledIcon = !!model.is_disabled;
  const showOverrideIcon = !showDisabledIcon && !!model.has_override;

  // Status indicator colors
  const statusColors = {
    success: 'bg-scheduled/16 text-scheduled ring-scheduled/40',
    failed: 'bg-danger/14 text-danger ring-danger/40',
    running: 'bg-accent/16 text-accent ring-accent/40',
    pending: 'bg-warning/16 text-warning ring-warning/40',
  };

  return (
    <Link
      to="/model/$id"
      params={{ id: encodeURIComponent(model.id) }}
      className={`glass-surface-subtle group/row block p-4 transition-all hover:border-scheduled/40 hover:bg-surface/92 hover:shadow-md hover:shadow-scheduled/10 ${
        isHighlighted ? 'ring-2 ring-scheduled/45 brightness-110' : isDimmed ? 'opacity-40' : ''
      }`}
      data-model-id={model.id}
      onMouseEnter={onMouseEnter}
      onMouseLeave={onMouseLeave}
    >
      <div className="relative">
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
              {showDisabledIcon && (
                <StopCircleIcon
                  className="size-4 shrink-0 text-danger"
                  aria-label="Model disabled"
                  title="Model disabled"
                />
              )}
              {showOverrideIcon && (
                <PencilSquareIcon
                  className="size-4 shrink-0 text-warning"
                  aria-label="Config override active"
                  title="Config override active"
                />
              )}
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
            <span className="font-mono text-xs font-semibold text-scheduled" title={model.schedule}>
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
