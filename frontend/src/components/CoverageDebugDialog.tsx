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
import type { DependencyDebugInfo, GapInfo, Range, IntervalTypeTransformation } from '@api/types.gen';
import { debugCoverageAtPositionOptions, getIntervalTypesOptions } from '@api/@tanstack/react-query.gen';
import { transformValue, formatValue } from '@utils/interval-transform';
import { TransformationSelector } from './shared/TransformationSelector';
import { useTransformationSelection } from '@hooks/useTransformationSelection';

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
      <DialogBackdrop className="fixed inset-0 bg-black/70" />

      {/* Dialog container */}
      <div className="fixed inset-0 overflow-y-auto">
        <div className="flex min-h-full items-center justify-center p-2 sm:p-4">
          {/* Dialog panel */}
          <DialogPanel className="relative w-full max-w-7xl rounded-xl bg-slate-900 p-3 shadow-xl sm:rounded-2xl sm:p-6">
            {/* Header */}
            <div className="mb-4 sm:mb-6">
              <div className="mb-3 flex min-w-0 items-center justify-between gap-2">
                <DialogTitle className="min-w-0 flex-1 truncate text-sm font-bold text-slate-100 sm:text-base lg:text-lg">
                  {modelId}
                </DialogTitle>
                <button
                  onClick={onClose}
                  className="shrink-0 rounded-lg p-2 text-slate-400 transition-colors hover:bg-slate-800 hover:text-slate-200"
                >
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
              <div className="flex h-48 items-center justify-center sm:h-64">
                <div className="size-12 animate-spin rounded-full border-4 border-slate-600 border-t-indigo-500" />
              </div>
            ) : error ? (
              <div className="rounded-lg bg-red-500/10 p-3 sm:p-4">
                <p className="text-sm text-red-400 sm:text-base">Error loading debug data: {error.message}</p>
              </div>
            ) : data ? (
              <div className="space-y-4 sm:space-y-6">
                {/* Summary Card */}
                <div className="rounded-lg bg-slate-800/50 p-3 sm:p-4">
                  <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 sm:gap-4 lg:grid-cols-4">
                    <div>
                      <p className="text-xs text-slate-400">{transformation?.name || 'Position'}</p>
                      <p className="mt-1 truncate font-mono text-sm text-slate-200">{formatDisplayValue(position)}</p>
                    </div>
                    {data.end_position !== undefined && (
                      <div>
                        <p className="text-xs text-slate-400">End {transformation?.name || 'Position'}</p>
                        <p className="mt-1 truncate font-mono text-sm text-slate-200">
                          {formatDisplayValue(data.end_position)}
                        </p>
                      </div>
                    )}
                    <div>
                      <p className="text-xs text-slate-400">Interval</p>
                      <p className="mt-1 truncate font-mono text-sm text-slate-200">{data.interval}</p>
                    </div>
                    <div>
                      <p className="text-xs text-slate-400">Status</p>
                      <div className="mt-1 flex items-center gap-1.5">
                        {data.can_process ? (
                          <>
                            <CheckCircleIcon className="size-4 shrink-0 text-green-500" />
                            <span className="truncate text-sm font-medium text-green-400">Can Process</span>
                          </>
                        ) : (
                          <>
                            <XCircleIcon className="size-4 shrink-0 text-red-500" />
                            <span className="truncate text-sm font-medium text-red-400">Cannot Process</span>
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
                    <div className="rounded-lg border border-slate-700/50 bg-slate-800/30 p-3 sm:p-4">
                      <h4 className="mb-3 text-sm font-semibold text-slate-200">Model Coverage</h4>
                      <div className="space-y-3">
                        <div>
                          <p className="text-xs text-slate-400">Has Data</p>
                          <p className="mt-1 text-sm text-slate-200">
                            {data.model_coverage.has_data ? (
                              <span className="text-green-400">Yes</span>
                            ) : (
                              <span className="text-red-400">No</span>
                            )}
                          </p>
                        </div>
                        {data.model_coverage.first_position !== undefined && (
                          <div>
                            <p className="text-xs text-slate-400">First {transformation?.name || 'Position'}</p>
                            <p className="mt-1 font-mono text-sm text-slate-200">
                              {formatDisplayValue(data.model_coverage.first_position)}
                            </p>
                          </div>
                        )}
                        {data.model_coverage.last_end_position !== undefined && (
                          <div>
                            <p className="text-xs text-slate-400">Last End {transformation?.name || 'Position'}</p>
                            <p className="mt-1 font-mono text-sm text-slate-200">
                              {formatDisplayValue(data.model_coverage.last_end_position)}
                            </p>
                          </div>
                        )}
                      </div>

                      {/* Gaps in Window */}
                      {data.model_coverage.gaps_in_window && data.model_coverage.gaps_in_window.length > 0 && (
                        <div className="mt-4">
                          <p className="mb-2 text-xs font-semibold text-amber-400">Gaps in Window</p>
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
                            <p className="text-xs font-semibold text-green-400">
                              Processed Ranges in Window ({data.model_coverage.ranges_in_window.length})
                            </p>
                            {data.model_coverage.ranges_in_window.length > 5 && (
                              <button
                                onClick={() => setRangesExpanded(!rangesExpanded)}
                                className="flex items-center gap-1 rounded px-1.5 py-0.5 text-xs text-slate-400 transition-colors hover:bg-slate-700/50 hover:text-slate-300"
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
                    <div className="rounded-lg border border-slate-700/50 bg-slate-800/30 p-3 sm:p-4">
                      <h4 className="mb-3 text-sm font-semibold text-slate-200">Validation Details</h4>

                      <div className="space-y-4">
                        <div>
                          <p className="text-xs text-slate-400">In Bounds</p>
                          <p className="mt-1 text-sm">
                            {data.validation.in_bounds ? (
                              <span className="text-green-400">Yes</span>
                            ) : (
                              <span className="text-red-400">No</span>
                            )}
                          </p>
                        </div>
                        <div>
                          <p className="text-xs text-slate-400">Has Dependency Gaps</p>
                          <p className="mt-1 text-sm">
                            {data.validation.has_dependency_gaps ? (
                              <span className="text-amber-400">Yes</span>
                            ) : (
                              <span className="text-green-400">No</span>
                            )}
                          </p>
                        </div>
                        {data.validation.next_valid_position !== undefined && (
                          <div>
                            <p className="text-xs text-slate-400">Next Valid {transformation?.name || 'Position'}</p>
                            <p className="mt-1 font-mono text-sm text-slate-200">
                              {formatDisplayValue(data.validation.next_valid_position)}
                            </p>
                          </div>
                        )}
                      </div>

                      {/* Valid Range */}
                      {data.validation.valid_range && (
                        <div className="mt-4">
                          <p className="mb-2 text-xs font-semibold text-slate-300">Valid Range</p>
                          <p className="font-mono text-sm text-slate-400">
                            {formatDisplayValue(data.validation.valid_range.min)} →{' '}
                            {formatDisplayValue(data.validation.valid_range.max)}
                          </p>
                        </div>
                      )}

                      {/* Blocking Gaps */}
                      {data.validation.blocking_gaps && data.validation.blocking_gaps.length > 0 && (
                        <div className="mt-4">
                          <p className="mb-2 text-xs font-semibold text-red-400">Blocking Gaps</p>
                          <div className="space-y-2">
                            {data.validation.blocking_gaps.map((block, idx) => (
                              <div key={idx} className="rounded bg-red-500/10 p-2">
                                <Link
                                  to="/model/$id"
                                  params={{ id: block.dependency_id }}
                                  className="font-mono text-xs text-red-400 hover:text-red-300"
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
                          <p className="mb-2 text-xs font-semibold text-slate-300">Reasons</p>
                          <ul className="space-y-1">
                            {data.validation.reasons.map((reason, idx) => (
                              <li key={idx} className="flex items-start gap-2">
                                <ExclamationCircleIcon className="mt-0.5 size-3 shrink-0 text-amber-500" />
                                <span className="text-sm text-slate-300">{transformReasonMessage(reason)}</span>
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
                  <div className="rounded-lg border border-slate-700/50 bg-slate-800/30 p-3 sm:p-4">
                    <h4 className="mb-3 text-sm font-semibold text-slate-200">Dependencies</h4>
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
    full_coverage: 'text-green-400',
    has_gaps: 'text-amber-400',
    no_data: 'text-red-400',
    not_initialized: 'text-slate-500',
  };

  const nodeTypeColors = {
    external: 'bg-green-500/10 text-green-400 ring-green-500/30',
    transformation: 'bg-indigo-500/10 text-indigo-400 ring-indigo-500/30',
  };

  return (
    <div className={`rounded-lg p-2 sm:p-3 ${dependency.blocking ? 'bg-red-500/5' : 'bg-slate-800/40'}`}>
      <div className="mb-2 flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
        <div className="flex min-w-0 flex-wrap items-center gap-2">
          <Link
            to="/model/$id"
            params={{ id: dependency.id }}
            className="truncate font-mono text-xs font-medium text-indigo-400 hover:text-indigo-300 sm:text-sm"
          >
            {dependency.id}
          </Link>
          <span
            className={`shrink-0 rounded-full px-2 py-0.5 text-xs font-medium ring-3 ${nodeTypeColors[dependency.node_type]}`}
          >
            {dependency.node_type}
          </span>
          {dependency.is_incremental && (
            <span className="shrink-0 rounded-full bg-slate-700/50 px-2 py-0.5 text-xs font-medium text-slate-300">
              Incremental
            </span>
          )}
        </div>
        <div className="flex flex-wrap items-center gap-2">
          {dependency.blocking && (
            <span className="shrink-0 rounded-full bg-red-500/20 px-2 py-0.5 text-xs font-medium text-red-400">
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
            <span className="text-slate-500">Min:</span>{' '}
            <span className="font-mono text-slate-300">
              {(() => {
                const transformed = transformValue(dependency.bounds.min, transformation);
                return formatValue(transformed, transformation.format);
              })()}
            </span>
          </div>
          <div>
            <span className="text-slate-500">Max:</span>{' '}
            <span className="font-mono text-slate-300">
              {(() => {
                const transformed = transformValue(dependency.bounds.max, transformation);
                return formatValue(transformed, transformation.format);
              })()}
            </span>
          </div>
          {dependency.bounds.lag_applied !== undefined && (
            <div>
              <span className="text-slate-500">Lag:</span>{' '}
              <span className="font-mono text-slate-300">{dependency.bounds.lag_applied}</span>
            </div>
          )}
        </div>
      )}

      {/* Gaps */}
      {dependency.gaps && dependency.gaps.length > 0 && (
        <div className="mt-2">
          <p className="mb-1 text-xs font-semibold text-amber-400">Gaps</p>
          <div className="space-y-1">
            {dependency.gaps.slice(0, 3).map((gap, idx) => (
              <GapDisplay key={idx} gap={gap} transformation={transformation} />
            ))}
            {dependency.gaps.length > 3 && (
              <p className="text-xs text-slate-500">...and {dependency.gaps.length - 3} more gaps</p>
            )}
          </div>
        </div>
      )}

      {/* OR Group Members */}
      {dependency.or_group_members && dependency.or_group_members.length > 0 && (
        <div className="ml-2 mt-2 space-y-2 border-l-2 border-slate-700 pl-2 sm:ml-4 sm:pl-3">
          <p className="text-xs font-semibold text-slate-400">OR Group Members</p>
          {dependency.or_group_members.map((member, idx) => (
            <DependencyCard key={idx} dependency={member} transformation={transformation} />
          ))}
        </div>
      )}

      {/* Child Dependencies */}
      {dependency.child_dependencies && dependency.child_dependencies.length > 0 && (
        <div className="ml-2 mt-2 space-y-2 border-l-2 border-slate-700 pl-2 sm:ml-4 sm:pl-3">
          <p className="text-xs font-semibold text-slate-400">Child Dependencies</p>
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
      <span className="text-slate-500">Gap:</span>
      <span className="font-mono text-amber-400">
        {formatDisplayValue(gap.start)} → {formatDisplayValue(gap.end)}
      </span>
      <span className="text-slate-500">(size: {gap.size})</span>
      {gap.overlaps_request && (
        <span className="shrink-0 rounded bg-amber-500/20 px-1.5 py-0.5 text-xs font-medium text-amber-400">
          Overlaps
        </span>
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
      <span className="text-slate-500">Range:</span>
      <span className="font-mono text-green-400">
        {formatDisplayValue(range.position)} → {formatDisplayValue(range.position + range.interval)}
      </span>
      <span className="text-slate-500">(interval: {range.interval})</span>
    </div>
  );
}
