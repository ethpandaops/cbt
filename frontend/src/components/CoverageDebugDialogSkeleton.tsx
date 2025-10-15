import type { JSX } from 'react';
import { Dialog, DialogPanel, DialogTitle, DialogBackdrop } from '@headlessui/react';
import { XMarkIcon } from '@heroicons/react/24/outline';

interface ShimmerProps {
  className?: string;
}

function Shimmer({ className = '' }: ShimmerProps): JSX.Element {
  return (
    <div
      className={`animate-shimmer bg-linear-to-r from-slate-700 via-slate-600 to-slate-700 bg-[length:200%_100%] ${className}`}
    />
  );
}

interface CoverageDebugDialogSkeletonProps {
  isOpen: boolean;
  onClose: () => void;
}

export function CoverageDebugDialogSkeleton({ isOpen, onClose }: CoverageDebugDialogSkeletonProps): JSX.Element {
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
                <DialogTitle className="min-w-0 flex-1">
                  <Shimmer className="h-6 w-96 rounded-sm" />
                </DialogTitle>
                <button
                  onClick={onClose}
                  className="shrink-0 rounded-lg p-2 text-slate-400 transition-colors hover:bg-slate-800 hover:text-slate-200"
                >
                  <XMarkIcon className="size-5" />
                </button>
              </div>
              <Shimmer className="h-8 w-32 rounded-sm" />
            </div>

            {/* Content */}
            <div className="space-y-4 sm:space-y-6">
              {/* Summary Card */}
              <div className="rounded-lg bg-slate-800/50 p-3 sm:p-4">
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 sm:gap-4 lg:grid-cols-4">
                  {[1, 2, 3, 4].map(i => (
                    <div key={i} className="space-y-1">
                      <Shimmer className="h-3 w-20 rounded-xs" />
                      <Shimmer className="h-5 w-full rounded-xs" />
                    </div>
                  ))}
                </div>
              </div>

              {/* Model Coverage & Validation */}
              <div className="grid grid-cols-1 gap-4 sm:gap-6 lg:grid-cols-2">
                {/* Model Coverage */}
                <div className="rounded-lg border border-slate-700/50 bg-slate-800/30 p-3 sm:p-4">
                  <Shimmer className="mb-3 h-5 w-32 rounded-sm" />
                  <div className="space-y-3">
                    {[1, 2, 3].map(i => (
                      <div key={i} className="space-y-1">
                        <Shimmer className="h-3 w-24 rounded-xs" />
                        <Shimmer className="h-4 w-full rounded-xs" />
                      </div>
                    ))}
                  </div>
                  {/* Gaps section */}
                  <div className="mt-4">
                    <Shimmer className="mb-2 h-4 w-28 rounded-xs" />
                    <div className="space-y-1">
                      {[1, 2].map(i => (
                        <Shimmer key={i} className="h-6 w-full rounded-xs" />
                      ))}
                    </div>
                  </div>
                </div>

                {/* Validation */}
                <div className="rounded-lg border border-slate-700/50 bg-slate-800/30 p-3 sm:p-4">
                  <Shimmer className="mb-3 h-5 w-36 rounded-sm" />
                  <div className="space-y-4">
                    {[1, 2, 3].map(i => (
                      <div key={i} className="space-y-1">
                        <Shimmer className="h-3 w-20 rounded-xs" />
                        <Shimmer className="h-4 w-16 rounded-xs" />
                      </div>
                    ))}
                  </div>
                  {/* Reasons section */}
                  <div className="mt-4">
                    <Shimmer className="mb-2 h-4 w-20 rounded-xs" />
                    <div className="space-y-1">
                      {[1, 2, 3].map(i => (
                        <Shimmer key={i} className="h-5 w-full rounded-xs" />
                      ))}
                    </div>
                  </div>
                </div>
              </div>

              {/* Dependencies */}
              <div className="rounded-lg border border-slate-700/50 bg-slate-800/30 p-3 sm:p-4">
                <Shimmer className="mb-3 h-5 w-28 rounded-sm" />
                <div className="space-y-3">
                  {[1, 2, 3].map(i => (
                    <div key={i} className="rounded-lg bg-slate-800/40 p-2 sm:p-3">
                      <div className="mb-2 flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
                        <div className="flex flex-wrap items-center gap-2">
                          <Shimmer className="h-4 w-48 rounded-xs" />
                          <Shimmer className="h-5 w-20 rounded-full" />
                        </div>
                        <Shimmer className="h-4 w-24 rounded-xs" />
                      </div>
                      <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 md:grid-cols-3">
                        {[1, 2, 3].map(j => (
                          <Shimmer key={j} className="h-4 w-full rounded-xs" />
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </DialogPanel>
        </div>
      </div>
    </Dialog>
  );
}
