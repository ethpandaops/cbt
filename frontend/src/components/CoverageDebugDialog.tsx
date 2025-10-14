import { Fragment, type JSX } from 'react';
import { Dialog, Transition } from '@headlessui/react';
import { Link } from '@tanstack/react-router';
import { useQuery } from '@tanstack/react-query';
import { XMarkIcon, CheckCircleIcon, XCircleIcon, ExclamationCircleIcon } from '@heroicons/react/24/outline';
import type { DependencyDebugInfo, GapInfo, Range } from '@api/types.gen';
import { debugCoverageAtPositionOptions } from '@api/@tanstack/react-query.gen';

interface CoverageDebugDialogProps {
  isOpen: boolean;
  onClose: () => void;
  modelId: string;
  position: number;
}

export function CoverageDebugDialog({ isOpen, onClose, modelId, position }: CoverageDebugDialogProps): JSX.Element {
  // Fetch debug data using TanStack Query generated options
  const { data, isLoading, error } = useQuery({
    ...debugCoverageAtPositionOptions({ path: { id: modelId, position } }),
    enabled: isOpen,
  });

  return (
    <Transition appear show={isOpen} as={Fragment}>
      <Dialog as="div" className="relative z-50" onClose={onClose}>
        <Transition.Child
          as={Fragment}
          enter="ease-out duration-300"
          enterFrom="opacity-0"
          enterTo="opacity-100"
          leave="ease-in duration-200"
          leaveFrom="opacity-100"
          leaveTo="opacity-0"
        >
          <div className="fixed inset-0 bg-black/70 backdrop-blur-sm" />
        </Transition.Child>

        <div className="fixed inset-0 overflow-y-auto">
          <div className="flex min-h-full items-center justify-center p-4 text-center">
            <Transition.Child
              as={Fragment}
              enter="ease-out duration-300"
              enterFrom="opacity-0 scale-95"
              enterTo="opacity-100 scale-100"
              leave="ease-in duration-200"
              leaveFrom="opacity-100 scale-100"
              leaveTo="opacity-0 scale-95"
            >
              <Dialog.Panel className="w-full max-w-6xl transform overflow-hidden rounded-2xl bg-slate-900/95 p-6 text-left align-middle shadow-xl transition-all ring-1 ring-slate-700/50">
                {/* Header */}
                <div className="mb-6 flex items-center justify-between">
                  <Dialog.Title as="h3" className="text-lg font-semibold text-slate-100">
                    Coverage Debug Analysis
                  </Dialog.Title>
                  <button
                    onClick={onClose}
                    className="rounded-lg p-2 text-slate-400 transition-colors hover:bg-slate-800 hover:text-slate-200"
                  >
                    <XMarkIcon className="size-5" />
                  </button>
                </div>

                {/* Content */}
                {isLoading ? (
                  <div className="flex h-64 items-center justify-center">
                    <div className="animate-spin rounded-full size-12 border-4 border-slate-600 border-t-indigo-500" />
                  </div>
                ) : error ? (
                  <div className="rounded-lg bg-red-500/10 p-4">
                    <p className="text-red-400">Error loading debug data: {error.message}</p>
                  </div>
                ) : data ? (
                  <div className="space-y-6">
                    {/* Summary Card */}
                    <div className="rounded-lg bg-slate-800/50 p-4">
                      <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
                        <div>
                          <p className="text-xs text-slate-400">Model</p>
                          <Link
                            to="/model/$id"
                            params={{ id: modelId }}
                            className="mt-1 block truncate font-mono text-sm font-medium text-indigo-400 hover:text-indigo-300"
                          >
                            {modelId}
                          </Link>
                        </div>
                        <div>
                          <p className="text-xs text-slate-400">Position</p>
                          <p className="mt-1 font-mono text-sm text-slate-200">{position}</p>
                        </div>
                        <div>
                          <p className="text-xs text-slate-400">Interval</p>
                          <p className="mt-1 font-mono text-sm text-slate-200">{data.interval}</p>
                        </div>
                        <div>
                          <p className="text-xs text-slate-400">Status</p>
                          <div className="mt-1 flex items-center gap-1.5">
                            {data.can_process ? (
                              <>
                                <CheckCircleIcon className="size-4 text-green-500" />
                                <span className="text-sm font-medium text-green-400">Can Process</span>
                              </>
                            ) : (
                              <>
                                <XCircleIcon className="size-4 text-red-500" />
                                <span className="text-sm font-medium text-red-400">Cannot Process</span>
                              </>
                            )}
                          </div>
                        </div>
                      </div>
                    </div>

                    {/* Model Coverage */}
                    {data.model_coverage && (
                      <div className="rounded-lg border border-slate-700/50 bg-slate-800/30 p-4">
                        <h4 className="mb-3 text-sm font-semibold text-slate-200">Model Coverage</h4>
                        <div className="grid grid-cols-2 gap-4 sm:grid-cols-3">
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
                              <p className="text-xs text-slate-400">First Position</p>
                              <p className="mt-1 font-mono text-sm text-slate-200">
                                {data.model_coverage.first_position}
                              </p>
                            </div>
                          )}
                          {data.model_coverage.last_end_position !== undefined && (
                            <div>
                              <p className="text-xs text-slate-400">Last End Position</p>
                              <p className="mt-1 font-mono text-sm text-slate-200">
                                {data.model_coverage.last_end_position}
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
                                <GapDisplay key={idx} gap={gap} />
                              ))}
                            </div>
                          </div>
                        )}

                        {/* Ranges in Window */}
                        {data.model_coverage.ranges_in_window && data.model_coverage.ranges_in_window.length > 0 && (
                          <div className="mt-4">
                            <p className="mb-2 text-xs font-semibold text-green-400">Processed Ranges in Window</p>
                            <div className="space-y-1">
                              {data.model_coverage.ranges_in_window.map((range, idx) => (
                                <RangeDisplay key={idx} range={range} />
                              ))}
                            </div>
                          </div>
                        )}
                      </div>
                    )}

                    {/* Dependencies */}
                    {data.dependencies && data.dependencies.length > 0 && (
                      <div className="rounded-lg border border-slate-700/50 bg-slate-800/30 p-4">
                        <h4 className="mb-3 text-sm font-semibold text-slate-200">Dependencies</h4>
                        <div className="space-y-3">
                          {data.dependencies.map((dep, idx) => (
                            <DependencyCard key={idx} dependency={dep} />
                          ))}
                        </div>
                      </div>
                    )}

                    {/* Validation */}
                    {data.validation && (
                      <div className="rounded-lg border border-slate-700/50 bg-slate-800/30 p-4">
                        <h4 className="mb-3 text-sm font-semibold text-slate-200">Validation Details</h4>

                        <div className="grid grid-cols-2 gap-4 sm:grid-cols-3">
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
                              <p className="text-xs text-slate-400">Next Valid Position</p>
                              <p className="mt-1 font-mono text-sm text-slate-200">
                                {data.validation.next_valid_position}
                              </p>
                            </div>
                          )}
                        </div>

                        {/* Valid Range */}
                        {data.validation.valid_range && (
                          <div className="mt-4">
                            <p className="mb-2 text-xs font-semibold text-slate-300">Valid Range</p>
                            <p className="font-mono text-sm text-slate-400">
                              {data.validation.valid_range.min} → {data.validation.valid_range.max}
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
                                  {block.gap && <GapDisplay gap={block.gap} className="mt-1" />}
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
                                  <span className="text-sm text-slate-300">{reason}</span>
                                </li>
                              ))}
                            </ul>
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                ) : null}
              </Dialog.Panel>
            </Transition.Child>
          </div>
        </div>
      </Dialog>
    </Transition>
  );
}

// Helper component to display a dependency
function DependencyCard({ dependency }: { dependency: DependencyDebugInfo }): JSX.Element {
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
    <div className={`rounded-lg p-3 ${dependency.blocking ? 'bg-red-500/5' : 'bg-slate-800/40'}`}>
      <div className="mb-2 flex items-start justify-between">
        <div className="flex items-center gap-2">
          <Link
            to="/model/$id"
            params={{ id: dependency.id }}
            className="font-mono text-sm font-medium text-indigo-400 hover:text-indigo-300"
          >
            {dependency.id}
          </Link>
          <span
            className={`rounded-full px-2 py-0.5 text-xs font-medium ring-1 ${nodeTypeColors[dependency.node_type]}`}
          >
            {dependency.node_type}
          </span>
          {dependency.is_incremental && (
            <span className="rounded-full bg-slate-700/50 px-2 py-0.5 text-xs font-medium text-slate-300">
              Incremental
            </span>
          )}
        </div>
        <div className="flex items-center gap-2">
          {dependency.blocking && (
            <span className="rounded-full bg-red-500/20 px-2 py-0.5 text-xs font-medium text-red-400">Blocking</span>
          )}
          <span className={`text-xs font-medium ${statusColors[dependency.coverage_status || 'not_initialized']}`}>
            {dependency.coverage_status?.replace('_', ' ')}
          </span>
        </div>
      </div>

      {/* Bounds */}
      {dependency.bounds && (
        <div className="mb-2 grid grid-cols-3 gap-2 text-xs">
          <div>
            <span className="text-slate-500">Min:</span>{' '}
            <span className="font-mono text-slate-300">{dependency.bounds.min}</span>
          </div>
          <div>
            <span className="text-slate-500">Max:</span>{' '}
            <span className="font-mono text-slate-300">{dependency.bounds.max}</span>
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
              <GapDisplay key={idx} gap={gap} />
            ))}
            {dependency.gaps.length > 3 && (
              <p className="text-xs text-slate-500">...and {dependency.gaps.length - 3} more gaps</p>
            )}
          </div>
        </div>
      )}

      {/* OR Group Members */}
      {dependency.or_group_members && dependency.or_group_members.length > 0 && (
        <div className="mt-2 ml-4 space-y-2 border-l-2 border-slate-700 pl-3">
          <p className="text-xs font-semibold text-slate-400">OR Group Members</p>
          {dependency.or_group_members.map((member, idx) => (
            <DependencyCard key={idx} dependency={member} />
          ))}
        </div>
      )}

      {/* Child Dependencies */}
      {dependency.child_dependencies && dependency.child_dependencies.length > 0 && (
        <div className="mt-2 ml-4 space-y-2 border-l-2 border-slate-700 pl-3">
          <p className="text-xs font-semibold text-slate-400">Child Dependencies</p>
          {dependency.child_dependencies.map((child, idx) => (
            <DependencyCard key={idx} dependency={child} />
          ))}
        </div>
      )}
    </div>
  );
}

// Helper component to display a gap
function GapDisplay({ gap, className = '' }: { gap: GapInfo; className?: string }): JSX.Element {
  return (
    <div className={`flex items-center gap-2 text-xs ${className}`}>
      <span className="text-slate-500">Gap:</span>
      <span className="font-mono text-amber-400">
        {gap.start} → {gap.end}
      </span>
      <span className="text-slate-500">(size: {gap.size})</span>
      {gap.overlaps_request && (
        <span className="rounded bg-amber-500/20 px-1.5 py-0.5 text-xs font-medium text-amber-400">Overlaps</span>
      )}
    </div>
  );
}

// Helper component to display a range
function RangeDisplay({ range }: { range: Range }): JSX.Element {
  return (
    <div className="flex items-center gap-2 text-xs">
      <span className="text-slate-500">Range:</span>
      <span className="font-mono text-green-400">
        {range.position} → {range.position + range.interval}
      </span>
      <span className="text-slate-500">(interval: {range.interval})</span>
    </div>
  );
}
