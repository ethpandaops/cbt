import { useState, type JSX } from 'react';
import { Link } from '@tanstack/react-router';
import { useQuery } from '@tanstack/react-query';
import { Dialog, DialogPanel, DialogTitle, DialogBackdrop } from '@headlessui/react';
import {
  XMarkIcon,
  CheckCircleIcon,
  XCircleIcon,
  ExclamationCircleIcon,
  PlusIcon,
  MinusIcon,
} from '@heroicons/react/24/outline';
import type { DependencyDebugInfo, GapInfo, Range, IntervalTypeTransformation } from '@/api/types.gen';
import { debugCoverageAtPositionOptions, getIntervalTypesOptions } from '@/api/@tanstack/react-query.gen';
import { transformValue, formatValue } from '@/utils/interval-transform';
import { getErrorMessage } from '@/utils/error';
import { TransformationSelector } from '@/components/Forms/TransformationSelector';
import { useTransformationSelection } from '@/hooks/useTransformationSelection';

interface CoverageDebugDialogProps {
  isOpen: boolean;
  onClose: () => void;
  modelId: string;
  position: number;
  intervalType: string;
}

export function CoverageDebugDialog({
  isOpen,
  onClose,
  modelId,
  position,
  intervalType,
}: CoverageDebugDialogProps): JSX.Element | null {
  // State for collapsing processed ranges
  const [rangesExpanded, setRangesExpanded] = useState(false);

  // Fetch debug data using TanStack Query generated options
  const { data, isLoading, error } = useQuery({
    ...debugCoverageAtPositionOptions({ path: { id: modelId, position } }),
    enabled: isOpen,
  });

  // Fetch interval types
  const intervalTypes = useQuery(getIntervalTypesOptions());

  // Persistent transformation selection hook
  const { getSelectedIndex, setSelectedIndex } = useTransformationSelection();

  // Get available transformations for this interval type
  const transformations = intervalTypes.data?.interval_types?.[intervalType] || [];
  const selectedTransformationIndex = getSelectedIndex(intervalType, transformations);
  const transformation: IntervalTypeTransformation | undefined = transformations[selectedTransformationIndex];

  // Helper function to format values using the transformation
  const formatDisplayValue = (value: number): string => {
    if (!transformation) return value.toLocaleString();
    const transformed = transformValue(value, transformation);
    return formatValue(transformed, transformation.format);
  };

  // Helper function to transform positions in reason strings
  const transformReasonMessage = (reason: string): string => {
    if (!transformation) return reason;

    // Patterns for server-generated messages with positions:
    // "Dependency X has gap from 123 to 456"
    // "Dependency X is not initialized"
    // "Dependency X has no data for position 123"
    // "Position 123 is before earliest available data at 456"
    // "Position end 123 exceeds latest available data at 456"

    // First, replace the word "Position" (case-insensitive) with the transformation name
    const transformedReason = reason.replace(/\bPosition\b/gi, transformation.name);

    // Then, transform numeric position values
    return transformedReason.replace(/\b(\d+)\b/g, match => {
      const num = parseInt(match, 10);
      // Only transform numbers that look like positions (large numbers)
      // This avoids transforming small numbers that might be intervals or counts
      if (num > 1000000) {
        return formatDisplayValue(num);
      }
      return match;
    });
  };

  return (
    <Dialog open={isOpen} onClose={onClose} className="relative z-50">
      {/* Backdrop */}
      <DialogBackdrop className="fixed inset-0 bg-primary/45 backdrop-blur-sm" />

      {/* Dialog container */}
      <div className="fixed inset-0 overflow-y-auto">
        <div className="flex min-h-full items-center justify-center p-2 sm:p-4">
          {/* Dialog panel */}
          <DialogPanel className="glass-surface relative w-full max-w-7xl p-3 sm:p-6">
            {/* Header */}
            <div className="mb-4 sm:mb-6">
              <div className="mb-3 flex min-w-0 items-center justify-between gap-2">
                <DialogTitle className="min-w-0 flex-1 truncate text-sm font-bold text-foreground sm:text-base lg:text-lg">
                  {modelId}
                </DialogTitle>
                <button onClick={onClose} className="glass-icon-control shrink-0 text-muted">
                  <XMarkIcon className="size-5" />
                </button>
              </div>
              {transformations.length > 0 && (
                <TransformationSelector
                  transformations={transformations}
                  selectedIndex={selectedTransformationIndex}
                  onSelect={index => setSelectedIndex(intervalType, index)}
                  compact
                />
              )}
            </div>

            {/* Content */}
            {isLoading ? (
              <div className="space-y-4 sm:space-y-6">
                {/* Summary Card */}
                <div className="glass-surface-subtle p-3 sm:p-4">
                  <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 sm:gap-4 lg:grid-cols-4">
                    {[1, 2, 3, 4].map(i => (
                      <div key={i} className="space-y-1">
                        <div className="h-3 w-20 animate-shimmer rounded-xs bg-linear-to-r from-secondary via-surface to-secondary bg-[length:200%_100%]" />
                        <div className="h-5 w-full animate-shimmer rounded-xs bg-linear-to-r from-secondary via-surface to-secondary bg-[length:200%_100%]" />
                      </div>
                    ))}
                  </div>
                </div>

                {/* Model Coverage & Validation */}
                <div className="grid grid-cols-1 gap-4 sm:gap-6 lg:grid-cols-2">
                  {/* Model Coverage */}
                  <div className="glass-surface-subtle p-3 sm:p-4">
                    <div className="mb-3 h-5 w-32 animate-shimmer rounded-sm bg-linear-to-r from-secondary via-surface to-secondary bg-[length:200%_100%]" />
                    <div className="space-y-3">
                      {[1, 2, 3].map(i => (
                        <div key={i} className="space-y-1">
                          <div className="h-3 w-24 animate-shimmer rounded-xs bg-linear-to-r from-secondary via-surface to-secondary bg-[length:200%_100%]" />
                          <div className="h-4 w-full animate-shimmer rounded-xs bg-linear-to-r from-secondary via-surface to-secondary bg-[length:200%_100%]" />
                        </div>
                      ))}
                    </div>
                    {/* Gaps section */}
                    <div className="mt-4">
                      <div className="mb-2 h-4 w-28 animate-shimmer rounded-xs bg-linear-to-r from-secondary via-surface to-secondary bg-[length:200%_100%]" />
                      <div className="space-y-1">
                        {[1, 2].map(i => (
                          <div
                            key={i}
                            className="h-6 w-full animate-shimmer rounded-xs bg-linear-to-r from-secondary via-surface to-secondary bg-[length:200%_100%]"
                          />
                        ))}
                      </div>
                    </div>
                  </div>

                  {/* Validation */}
                  <div className="glass-surface-subtle p-3 sm:p-4">
                    <div className="mb-3 h-5 w-36 animate-shimmer rounded-sm bg-linear-to-r from-secondary via-surface to-secondary bg-[length:200%_100%]" />
                    <div className="space-y-4">
                      {[1, 2, 3].map(i => (
                        <div key={i} className="space-y-1">
                          <div className="h-3 w-20 animate-shimmer rounded-xs bg-linear-to-r from-secondary via-surface to-secondary bg-[length:200%_100%]" />
                          <div className="h-4 w-16 animate-shimmer rounded-xs bg-linear-to-r from-secondary via-surface to-secondary bg-[length:200%_100%]" />
                        </div>
                      ))}
                    </div>
                    {/* Reasons section */}
                    <div className="mt-4">
                      <div className="mb-2 h-4 w-20 animate-shimmer rounded-xs bg-linear-to-r from-secondary via-surface to-secondary bg-[length:200%_100%]" />
                      <div className="space-y-1">
                        {[1, 2, 3].map(i => (
                          <div
                            key={i}
                            className="h-5 w-full animate-shimmer rounded-xs bg-linear-to-r from-secondary via-surface to-secondary bg-[length:200%_100%]"
                          />
                        ))}
                      </div>
                    </div>
                  </div>
                </div>

                {/* Dependencies */}
                <div className="glass-surface-subtle p-3 sm:p-4">
                  <div className="mb-3 h-5 w-28 animate-shimmer rounded-sm bg-linear-to-r from-secondary via-surface to-secondary bg-[length:200%_100%]" />
                  <div className="space-y-3">
                    {[1, 2, 3].map(i => (
                      <div
                        key={i}
                        className="rounded-lg border border-border/45 bg-surface/72 p-2 ring-1 ring-border/30 sm:p-3"
                      >
                        <div className="mb-2 flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
                          <div className="flex flex-wrap items-center gap-2">
                            <div className="h-4 w-48 animate-shimmer rounded-xs bg-linear-to-r from-secondary via-surface to-secondary bg-[length:200%_100%]" />
                            <div className="h-5 w-20 animate-shimmer rounded-full bg-linear-to-r from-secondary via-surface to-secondary bg-[length:200%_100%]" />
                          </div>
                          <div className="h-4 w-24 animate-shimmer rounded-xs bg-linear-to-r from-secondary via-surface to-secondary bg-[length:200%_100%]" />
                        </div>
                        <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 md:grid-cols-3">
                          {[1, 2, 3].map(j => (
                            <div
                              key={j}
                              className="h-4 w-full animate-shimmer rounded-xs bg-linear-to-r from-secondary via-surface to-secondary bg-[length:200%_100%]"
                            />
                          ))}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            ) : error ? (
              <div className="rounded-xl border border-danger/45 bg-danger/12 p-3 ring-1 ring-danger/20 sm:p-4">
                <p className="text-sm text-danger sm:text-base">Error loading debug data: {getErrorMessage(error)}</p>
              </div>
            ) : data ? (
              <div className="space-y-4 sm:space-y-6">
                {/* Summary Card */}
                <div className="glass-surface-subtle p-3 sm:p-4">
                  <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 sm:gap-4 lg:grid-cols-4">
                    <div>
                      <p className="text-xs text-muted">{transformation?.name || 'Position'}</p>
                      <p className="mt-1 truncate font-mono text-sm text-foreground">{formatDisplayValue(position)}</p>
                    </div>
                    {data.end_position !== undefined && (
                      <div>
                        <p className="text-xs text-muted">End {transformation?.name || 'Position'}</p>
                        <p className="mt-1 truncate font-mono text-sm text-foreground">
                          {formatDisplayValue(data.end_position)}
                        </p>
                      </div>
                    )}
                    <div>
                      <p className="text-xs text-muted">Interval</p>
                      <p className="mt-1 truncate font-mono text-sm text-foreground">{data.interval}</p>
                    </div>
                    <div>
                      <p className="text-xs text-muted">Status</p>
                      <div className="mt-1 flex items-center gap-1.5">
                        {data.can_process ? (
                          <>
                            <CheckCircleIcon className="size-4 shrink-0 text-success" />
                            <span className="truncate text-sm font-medium text-success">Can Process</span>
                          </>
                        ) : (
                          <>
                            <XCircleIcon className="size-4 shrink-0 text-danger" />
                            <span className="truncate text-sm font-medium text-danger">Cannot Process</span>
                          </>
                        )}
                      </div>
                    </div>
                  </div>
                </div>

                {/* Model Coverage & Validation */}
                <div className="grid grid-cols-1 gap-4 sm:gap-6 lg:grid-cols-2">
                  {/* Model Coverage */}
                  {data.model_coverage && (
                    <div className="glass-surface-subtle p-3 sm:p-4">
                      <h4 className="mb-3 text-sm font-semibold text-foreground">Model Coverage</h4>
                      <div className="space-y-3">
                        <div>
                          <p className="text-xs text-muted">Has Data</p>
                          <p className="mt-1 text-sm text-foreground">
                            {data.model_coverage.has_data ? (
                              <span className="text-success">Yes</span>
                            ) : (
                              <span className="text-danger">No</span>
                            )}
                          </p>
                        </div>
                        {data.model_coverage.first_position !== undefined && (
                          <div>
                            <p className="text-xs text-muted">First {transformation?.name || 'Position'}</p>
                            <p className="mt-1 font-mono text-sm text-foreground">
                              {formatDisplayValue(data.model_coverage.first_position)}
                            </p>
                          </div>
                        )}
                        {data.model_coverage.last_end_position !== undefined && (
                          <div>
                            <p className="text-xs text-muted">Last End {transformation?.name || 'Position'}</p>
                            <p className="mt-1 font-mono text-sm text-foreground">
                              {formatDisplayValue(data.model_coverage.last_end_position)}
                            </p>
                          </div>
                        )}
                      </div>

                      {/* Gaps in Window */}
                      {data.model_coverage.gaps_in_window && data.model_coverage.gaps_in_window.length > 0 && (
                        <div className="mt-4">
                          <p className="mb-2 text-xs font-semibold text-warning">Gaps in Window</p>
                          <div className="space-y-1">
                            {data.model_coverage.gaps_in_window.map((gap, idx) => (
                              <GapDisplay key={idx} gap={gap} transformation={transformation} />
                            ))}
                          </div>
                        </div>
                      )}

                      {/* Ranges in Window */}
                      {data.model_coverage.ranges_in_window && data.model_coverage.ranges_in_window.length > 0 && (
                        <div className="mt-4">
                          <div className="mb-2 flex items-center justify-between">
                            <p className="text-xs font-semibold text-success">
                              Processed Ranges in Window ({data.model_coverage.ranges_in_window.length})
                            </p>
                            {data.model_coverage.ranges_in_window.length > 5 && (
                              <button
                                onClick={() => setRangesExpanded(!rangesExpanded)}
                                className="flex items-center gap-1 rounded px-1.5 py-0.5 text-xs text-muted transition-colors hover:bg-secondary/50 hover:text-primary"
                              >
                                {rangesExpanded ? (
                                  <>
                                    <MinusIcon className="size-3" />
                                    <span>Collapse</span>
                                  </>
                                ) : (
                                  <>
                                    <PlusIcon className="size-3" />
                                    <span>Expand</span>
                                  </>
                                )}
                              </button>
                            )}
                          </div>
                          <div className="space-y-1">
                            {(rangesExpanded
                              ? data.model_coverage.ranges_in_window
                              : data.model_coverage.ranges_in_window.slice(0, 5)
                            ).map((range, idx) => (
                              <RangeDisplay key={idx} range={range} transformation={transformation} />
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                  )}

                  {/* Validation */}
                  {data.validation && (
                    <div className="rounded-xl border border-border/70 bg-surface/72 p-3 shadow-sm ring-1 ring-border/35 sm:p-4">
                      <h4 className="mb-3 text-sm font-semibold text-foreground">Validation Details</h4>

                      <div className="space-y-4">
                        <div>
                          <p className="text-xs text-muted">In Bounds</p>
                          <p className="mt-1 text-sm">
                            {data.validation.in_bounds ? (
                              <span className="text-success">Yes</span>
                            ) : (
                              <span className="text-danger">No</span>
                            )}
                          </p>
                        </div>
                        <div>
                          <p className="text-xs text-muted">Has Dependency Gaps</p>
                          <p className="mt-1 text-sm">
                            {data.validation.has_dependency_gaps ? (
                              <span className="text-warning">Yes</span>
                            ) : (
                              <span className="text-success">No</span>
                            )}
                          </p>
                        </div>
                        {data.validation.next_valid_position !== undefined && (
                          <div>
                            <p className="text-xs text-muted">Next Valid {transformation?.name || 'Position'}</p>
                            <p className="mt-1 font-mono text-sm text-foreground">
                              {formatDisplayValue(data.validation.next_valid_position)}
                            </p>
                          </div>
                        )}
                      </div>

                      {/* Valid Range */}
                      {data.validation.valid_range && (
                        <div className="mt-4">
                          <p className="mb-2 text-xs font-semibold text-primary">Valid Range</p>
                          <p className="font-mono text-sm text-muted">
                            {formatDisplayValue(data.validation.valid_range.min)} →{' '}
                            {formatDisplayValue(data.validation.valid_range.max)}
                          </p>
                        </div>
                      )}

                      {/* Blocking Gaps */}
                      {data.validation.blocking_gaps && data.validation.blocking_gaps.length > 0 && (
                        <div className="mt-4">
                          <p className="mb-2 text-xs font-semibold text-danger">Blocking Gaps</p>
                          <div className="space-y-2">
                            {data.validation.blocking_gaps.map((block, idx) => (
                              <div key={idx} className="rounded-lg bg-danger/12 p-2 ring-1 ring-danger/25">
                                <Link
                                  to="/model/$id"
                                  params={{ id: block.dependency_id }}
                                  className="font-mono text-xs text-danger hover:text-danger/70"
                                >
                                  {block.dependency_id}
                                </Link>
                                {block.gap && (
                                  <GapDisplay gap={block.gap} className="mt-1" transformation={transformation} />
                                )}
                              </div>
                            ))}
                          </div>
                        </div>
                      )}

                      {/* Reasons */}
                      {data.validation.reasons && data.validation.reasons.length > 0 && (
                        <div className="mt-4">
                          <p className="mb-2 text-xs font-semibold text-primary">Reasons</p>
                          <ul className="space-y-1">
                            {data.validation.reasons.map((reason, idx) => (
                              <li key={idx} className="flex items-start gap-2">
                                <ExclamationCircleIcon className="mt-0.5 size-3 shrink-0 text-warning" />
                                <span className="text-sm text-foreground">{transformReasonMessage(reason)}</span>
                              </li>
                            ))}
                          </ul>
                        </div>
                      )}
                    </div>
                  )}
                </div>

                {/* Dependencies */}
                {data.dependencies && data.dependencies.length > 0 && (
                  <div className="rounded-xl border border-border/70 bg-surface/72 p-3 shadow-sm ring-1 ring-border/35 sm:p-4">
                    <h4 className="mb-3 text-sm font-semibold text-foreground">Dependencies</h4>
                    <div className="space-y-3">
                      {data.dependencies.map((dep, idx) => (
                        <DependencyCard key={idx} dependency={dep} transformation={transformation} />
                      ))}
                    </div>
                  </div>
                )}
              </div>
            ) : null}
          </DialogPanel>
        </div>
      </div>
    </Dialog>
  );
}

// Helper component to display a dependency
function DependencyCard({
  dependency,
  transformation,
}: {
  dependency: DependencyDebugInfo;
  transformation?: IntervalTypeTransformation;
}): JSX.Element {
  const statusColors = {
    full_coverage: 'text-success',
    has_gaps: 'text-warning',
    no_data: 'text-danger',
    not_initialized: 'text-muted',
  };

  const nodeTypeColors = {
    external: 'bg-external/20 text-emerald-900 ring-external/35 dark:text-success',
    transformation: 'bg-incremental/20 text-blue-900 ring-incremental/35 dark:text-incremental',
  };

  return (
    <div
      className={`rounded-lg p-2 ring-1 sm:p-3 ${dependency.blocking ? 'bg-danger/8 ring-danger/25' : 'bg-surface/66 ring-border/35'}`}
    >
      <div className="mb-2 flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
        <div className="flex min-w-0 flex-wrap items-center gap-2">
          <Link
            to="/model/$id"
            params={{ id: dependency.id }}
            className="truncate font-mono text-xs font-medium text-incremental hover:text-accent sm:text-sm"
          >
            {dependency.id}
          </Link>
          <span
            className={`shrink-0 rounded-full px-2 py-0.5 text-xs font-medium ring-3 ${nodeTypeColors[dependency.node_type]}`}
          >
            {dependency.node_type}
          </span>
          {dependency.is_incremental && (
            <span className="shrink-0 rounded-full bg-secondary/70 px-2 py-0.5 text-xs font-medium text-primary ring-1 ring-border/40">
              Incremental
            </span>
          )}
        </div>
        <div className="flex flex-wrap items-center gap-2">
          {dependency.blocking && (
            <span className="shrink-0 rounded-full bg-danger/22 px-2 py-0.5 text-xs font-medium text-danger ring-1 ring-danger/35">
              Blocking
            </span>
          )}
          <span
            className={`shrink-0 text-xs font-medium ${statusColors[dependency.coverage_status || 'not_initialized']}`}
          >
            {dependency.coverage_status?.replace('_', ' ')}
          </span>
        </div>
      </div>

      {/* Bounds */}
      {dependency.bounds && transformation && (
        <div className="mb-2 grid grid-cols-1 gap-2 text-xs sm:grid-cols-2 md:grid-cols-3">
          <div>
            <span className="text-muted">Min:</span>{' '}
            <span className="font-mono text-foreground">
              {(() => {
                const transformed = transformValue(dependency.bounds.min, transformation);
                return formatValue(transformed, transformation.format);
              })()}
            </span>
          </div>
          <div>
            <span className="text-muted">Max:</span>{' '}
            <span className="font-mono text-foreground">
              {(() => {
                const transformed = transformValue(dependency.bounds.max, transformation);
                return formatValue(transformed, transformation.format);
              })()}
            </span>
          </div>
          {dependency.bounds.lag_applied !== undefined && (
            <div>
              <span className="text-muted">Lag:</span>{' '}
              <span className="font-mono text-foreground">{dependency.bounds.lag_applied}</span>
            </div>
          )}
        </div>
      )}

      {/* Gaps */}
      {dependency.gaps && dependency.gaps.length > 0 && (
        <div className="mt-2">
          <p className="mb-1 text-xs font-semibold text-warning">Gaps</p>
          <div className="space-y-1">
            {dependency.gaps.slice(0, 3).map((gap, idx) => (
              <GapDisplay key={idx} gap={gap} transformation={transformation} />
            ))}
            {dependency.gaps.length > 3 && (
              <p className="text-xs text-muted">...and {dependency.gaps.length - 3} more gaps</p>
            )}
          </div>
        </div>
      )}

      {/* OR Group Members */}
      {dependency.or_group_members && dependency.or_group_members.length > 0 && (
        <div className="mt-2 ml-2 space-y-2 border-l-2 border-border pl-2 sm:ml-4 sm:pl-3">
          <p className="text-xs font-semibold text-muted">OR Group Members</p>
          {dependency.or_group_members.map((member, idx) => (
            <DependencyCard key={idx} dependency={member} transformation={transformation} />
          ))}
        </div>
      )}

      {/* Child Dependencies */}
      {dependency.child_dependencies && dependency.child_dependencies.length > 0 && (
        <div className="mt-2 ml-2 space-y-2 border-l-2 border-border pl-2 sm:ml-4 sm:pl-3">
          <p className="text-xs font-semibold text-muted">Child Dependencies</p>
          {dependency.child_dependencies.map((child, idx) => (
            <DependencyCard key={idx} dependency={child} transformation={transformation} />
          ))}
        </div>
      )}
    </div>
  );
}

// Helper component to display a gap
function GapDisplay({
  gap,
  className = '',
  transformation,
}: {
  gap: GapInfo;
  className?: string;
  transformation?: IntervalTypeTransformation;
}): JSX.Element {
  const formatDisplayValue = (value: number): string => {
    if (!transformation) return value.toLocaleString();
    const transformed = transformValue(value, transformation);
    return formatValue(transformed, transformation.format);
  };

  return (
    <div className={`flex flex-wrap items-center gap-2 text-xs ${className}`}>
      <span className="text-muted">Gap:</span>
      <span className="font-mono text-warning">
        {formatDisplayValue(gap.start)} → {formatDisplayValue(gap.end)}
      </span>
      <span className="text-muted">(size: {gap.size})</span>
      {gap.overlaps_request && (
        <span className="shrink-0 rounded bg-warning/20 px-1.5 py-0.5 text-xs font-medium text-warning">Overlaps</span>
      )}
    </div>
  );
}

// Helper component to display a range
function RangeDisplay({
  range,
  transformation,
}: {
  range: Range;
  transformation?: IntervalTypeTransformation;
}): JSX.Element {
  const formatDisplayValue = (value: number): string => {
    if (!transformation) return value.toLocaleString();
    const transformed = transformValue(value, transformation);
    return formatValue(transformed, transformation.format);
  };

  return (
    <div className="flex flex-wrap items-center gap-2 text-xs">
      <span className="text-muted">Range:</span>
      <span className="font-mono text-success">
        {formatDisplayValue(range.position)} → {formatDisplayValue(range.position + range.interval)}
      </span>
      <span className="text-muted">(interval: {range.interval})</span>
    </div>
  );
}
