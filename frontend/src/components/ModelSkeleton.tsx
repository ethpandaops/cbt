import type { JSX } from 'react';

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

export function ModelSkeleton(): JSX.Element {
  return (
    <div className="space-y-6">
      {/* Model header skeleton */}
      <div className="mb-6 flex items-center gap-4">
        <Shimmer className="h-8 flex-1 rounded-lg sm:h-10" />
        <Shimmer className="hidden h-8 w-24 rounded-full sm:block" />
      </div>

      {/* Back button skeleton */}
      <Shimmer className="h-10 w-48 rounded-lg" />

      {/* Model Information card */}
      <div className="rounded-2xl border border-indigo-500/30 bg-slate-800/80 p-4 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm sm:p-6">
        {/* Card title */}
        <div className="mb-3 sm:mb-4">
          <Shimmer className="h-5 w-32 rounded-sm sm:h-6" />
        </div>

        {/* Grid of info fields */}
        <div className="grid grid-cols-1 gap-x-4 gap-y-3 text-sm sm:grid-cols-2 sm:gap-x-6 sm:gap-y-4 lg:grid-cols-4">
          {[1, 2, 3, 4, 5, 6].map(i => (
            <div key={i} className="min-w-0 rounded-lg bg-slate-900/60 p-4">
              <Shimmer className="mb-1 h-3.5 w-20 rounded-xs" />
              <Shimmer className="h-6 w-full rounded-xs" />
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
