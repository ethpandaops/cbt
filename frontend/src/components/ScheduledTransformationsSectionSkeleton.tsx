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

export function ScheduledTransformationsSectionSkeleton(): JSX.Element {
  // Render 8 skeleton rows to match typical scheduled transformations count
  return (
    <div className="space-y-2">
      {[1, 2, 3, 4, 5, 6, 7, 8].map(rowIndex => (
        <div key={rowIndex} className="rounded-xl border border-emerald-500/20 bg-slate-800/60 backdrop-blur-sm">
          <div className="relative p-4">
            <div className="grid grid-cols-1 gap-3 lg:grid-cols-[2fr_1fr_1fr_1fr] lg:items-start">
              {/* Model ID and Dependencies column */}
              <div className="flex min-w-0 flex-col gap-1.5">
                {/* Model ID with optional status badge */}
                <div className="flex items-center gap-2">
                  <Shimmer className="h-[14px] w-56 rounded-xs" />
                  {rowIndex % 2 === 0 && <Shimmer className="h-[18px] w-16 rounded-xs" />}
                </div>
                {/* Optional dependency badges (show on some rows) */}
                {rowIndex % 4 === 0 && (
                  <div className="flex items-center gap-1.5">
                    <Shimmer className="h-[10px] w-20 rounded-xs" />
                    <div className="flex items-center gap-1">
                      <Shimmer className="h-[16px] w-12 rounded-xs" />
                      <Shimmer className="h-[16px] w-12 rounded-xs" />
                    </div>
                  </div>
                )}
                {/* Optional description (show on some rows) */}
                {rowIndex % 3 === 0 && <Shimmer className="h-3 w-72 rounded-xs" />}
              </div>

              {/* Schedule column */}
              <div className="flex flex-col gap-1">
                <Shimmer className="h-[10px] w-14 rounded-xs" />
                <Shimmer className="h-3 w-20 rounded-xs" />
              </div>

              {/* Last Run column */}
              <div className="flex flex-col gap-1">
                <Shimmer className="h-[10px] w-14 rounded-xs" />
                <Shimmer className="h-3 w-20 rounded-xs" />
              </div>

              {/* Next Run column */}
              <div className="flex flex-col gap-1">
                <Shimmer className="h-[10px] w-14 rounded-xs" />
                <Shimmer className="h-3 w-24 rounded-xs" />
                <Shimmer className="h-[10px] w-28 rounded-xs" />
              </div>
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}
