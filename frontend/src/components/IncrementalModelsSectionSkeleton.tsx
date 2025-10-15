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

export function IncrementalModelsSectionSkeleton(): JSX.Element {
  // Render 2-3 skeleton groups
  return (
    <div className="space-y-6">
      {[1, 2].map(groupIndex => (
        <div
          key={groupIndex}
          className="overflow-hidden rounded-2xl border border-indigo-500/30 bg-slate-800/80 p-4 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm sm:p-6"
        >
          <div className="relative">
            {/* Header section */}
            <div className="mb-4 flex flex-col gap-3 sm:mb-6">
              <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                <div className="flex flex-col gap-3 sm:flex-row sm:items-center">
                  {/* Interval type title and transformation selector as one continuous section */}
                  <Shimmer className="h-8 w-80 rounded-sm" />
                </div>
                {/* Zoom presets */}
                <div className="flex gap-2">
                  {[1, 2, 3, 4].map(i => (
                    <Shimmer key={i} className="size-8 rounded-sm" />
                  ))}
                </div>
              </div>
            </div>

            {/* Model rows - slimmer */}
            <div className="space-y-1.5">
              {[1, 2, 3, 4, 5].map(rowIndex => (
                <div key={rowIndex} className="flex items-center gap-2 rounded-xs bg-slate-700/50 px-2 py-1">
                  {/* Model name */}
                  <Shimmer className="h-4 w-44 rounded-xs" />
                  {/* Coverage bar - slimmer */}
                  <div className="flex-1">
                    <Shimmer className="h-4 w-full rounded-xs" />
                  </div>
                </div>
              ))}
            </div>

            {/* Zoom controls */}
            <div className="mt-6">
              <div className="space-y-2">
                {/* Label section */}
                <div className="flex items-center justify-between">
                  <Shimmer className="h-3 w-24 rounded-xs" />
                  <Shimmer className="h-4 w-48 rounded-xs" />
                </div>
                {/* Slider */}
                <Shimmer className="h-2 w-full rounded-full" />
                {/* Range labels */}
                <div className="flex items-center justify-between">
                  <Shimmer className="h-3 w-32 rounded-xs" />
                  <Shimmer className="h-3 w-32 rounded-xs" />
                </div>
              </div>
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}
