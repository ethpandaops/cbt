import { type JSX } from 'react';
import type { Range } from '@api/types.gen';
import type { IntervalTypeTransformation } from '@api/types.gen';
import { transformValue, formatValue } from '@/utils/interval-transform';

export interface CoverageBarProps {
  ranges?: Array<Range>;
  bounds?: { min: number; max: number };
  zoomStart: number;
  zoomEnd: number;
  type: 'transformation' | 'external' | 'scheduled';
  height?: number;
  transformation?: IntervalTypeTransformation;
  className?: string;
  tooltipId?: string;
}

export function CoverageBar({
  ranges,
  bounds,
  zoomStart,
  zoomEnd,
  type,
  height = 24,
  transformation,
  className = '',
  tooltipId = 'coverage-tooltip',
}: CoverageBarProps): JSX.Element {
  const range = zoomEnd - zoomStart || 1;

  // Scheduled transformations - always available
  if (type === 'scheduled') {
    return (
      <div
        className={`relative overflow-hidden rounded-lg bg-slate-800/50 ring-1 ring-slate-700/30 ${className}`}
        style={{ height: `${height}px` }}
      >
        <div className="flex h-full items-center justify-center">
          <span className="text-xs font-medium italic text-slate-500">Always available</span>
        </div>
      </div>
    );
  }

  // External models - render as single continuous bar
  if (type === 'external' && bounds) {
    const isVisible = bounds.max >= zoomStart && bounds.min <= zoomEnd;

    const boundsTooltip = transformation
      ? `${formatValue(transformValue(bounds.min, transformation), transformation.format)} → ${formatValue(transformValue(bounds.max, transformation), transformation.format)}`
      : `${bounds.min.toLocaleString()} → ${bounds.max.toLocaleString()}`;

    // Calculate gaps for external models
    const gaps: Array<{ position: number; interval: number }> = [];

    // Gap at the beginning if bounds don't start at zoomStart
    if (bounds.min > zoomStart) {
      gaps.push({
        position: zoomStart,
        interval: bounds.min - zoomStart,
      });
    }

    // Gap at the end if bounds don't reach zoomEnd
    if (bounds.max < zoomEnd) {
      gaps.push({
        position: bounds.max,
        interval: zoomEnd - bounds.max,
      });
    }

    return (
      <div
        className={`relative overflow-hidden rounded-lg bg-slate-700 ring-1 ring-slate-600/50 ${className}`}
        style={{ height: `${height}px` }}
      >
        {/* Render gaps (missing ranges) */}
        {gaps.map((gap, idx) => {
          const leftPercent = ((gap.position - zoomStart) / range) * 100;
          const rightPercent = ((gap.position + gap.interval - zoomStart) / range) * 100;
          const left = Math.max(0, leftPercent);
          const right = Math.min(100, rightPercent);
          const width = right - left;
          const gapMin = gap.position;
          const gapMax = gap.position + gap.interval;
          const gapContent = transformation
            ? `Missing: ${formatValue(transformValue(gapMin, transformation), transformation.format)} → ${formatValue(transformValue(gapMax, transformation), transformation.format)}`
            : `Missing: ${gapMin.toLocaleString()} → ${gapMax.toLocaleString()}`;

          return (
            <div
              key={`gap-${idx}`}
              className="absolute h-full cursor-help transition-colors hover:bg-red-900/20"
              style={{
                left: `${left}%`,
                width: `${width}%`,
              }}
              data-tooltip-id={tooltipId}
              data-tooltip-content={gapContent}
            />
          );
        })}

        {/* Render covered range */}
        {isVisible && (
          <div
            className="absolute h-full bg-green-600 cursor-help transition-colors hover:bg-green-500"
            style={{
              left: `${Math.max(0, ((bounds.min - zoomStart) / range) * 100)}%`,
              width: `${Math.min(100, ((Math.min(bounds.max, zoomEnd) - Math.max(bounds.min, zoomStart)) / range) * 100)}%`,
            }}
            data-tooltip-id={tooltipId}
            data-tooltip-content={boundsTooltip}
          />
        )}
      </div>
    );
  }

  // Transformation models - render coverage ranges with merging
  if (type === 'transformation' && ranges) {
    // Filter visible ranges
    const visibleRanges = ranges.filter(r => {
      const rangeEnd = r.position + r.interval;
      return rangeEnd >= zoomStart && r.position <= zoomEnd;
    });

    // Sort by position
    const sorted = [...visibleRanges].sort((a, b) => a.position - b.position);

    // Merge adjacent/overlapping ranges
    const merged: Array<{ position: number; interval: number }> = [];
    for (const curr of sorted) {
      if (merged.length === 0) {
        merged.push({ ...curr });
      } else {
        const last = merged[merged.length - 1];
        const lastEnd = last.position + last.interval;
        const currEnd = curr.position + curr.interval;

        // If current range overlaps or is adjacent to last, merge them
        if (curr.position <= lastEnd) {
          last.interval = Math.max(lastEnd, currEnd) - last.position;
        } else {
          merged.push({ ...curr });
        }
      }
    }

    // Calculate gaps (missing ranges) between merged chunks
    const gaps: Array<{ position: number; interval: number }> = [];
    for (let i = 0; i < merged.length - 1; i++) {
      const currentEnd = merged[i].position + merged[i].interval;
      const nextStart = merged[i + 1].position;
      if (currentEnd < nextStart) {
        gaps.push({
          position: currentEnd,
          interval: nextStart - currentEnd,
        });
      }
    }

    // Add gap at the beginning if coverage doesn't start at zoomStart
    if (merged.length > 0 && merged[0].position > zoomStart) {
      gaps.unshift({
        position: zoomStart,
        interval: merged[0].position - zoomStart,
      });
    }

    // Add gap at the end if coverage doesn't reach zoomEnd
    if (merged.length > 0) {
      const lastEnd = merged[merged.length - 1].position + merged[merged.length - 1].interval;
      if (lastEnd < zoomEnd) {
        gaps.push({
          position: lastEnd,
          interval: zoomEnd - lastEnd,
        });
      }
    }

    return (
      <div
        className={`relative overflow-hidden rounded-lg bg-slate-700 ring-1 ring-slate-600/50 ${className}`}
        style={{ height: `${height}px` }}
      >
        {/* Render gaps (missing ranges) */}
        {gaps.map((gap, idx) => {
          const leftPercent = ((gap.position - zoomStart) / range) * 100;
          const rightPercent = ((gap.position + gap.interval - zoomStart) / range) * 100;
          const left = Math.max(0, leftPercent);
          const right = Math.min(100, rightPercent);
          const width = right - left;
          const gapMin = gap.position;
          const gapMax = gap.position + gap.interval;
          const gapContent = transformation
            ? `Missing: ${formatValue(transformValue(gapMin, transformation), transformation.format)} → ${formatValue(transformValue(gapMax, transformation), transformation.format)}`
            : `Missing: ${gapMin.toLocaleString()} → ${gapMax.toLocaleString()}`;

          return (
            <div
              key={`gap-${idx}`}
              className="absolute h-full cursor-help transition-colors hover:bg-red-900/20"
              style={{
                left: `${left}%`,
                width: `${width}%`,
              }}
              data-tooltip-id={tooltipId}
              data-tooltip-content={gapContent}
            />
          );
        })}

        {/* Render covered ranges */}
        {merged.map((r, idx) => {
          const leftPercent = ((r.position - zoomStart) / range) * 100;
          const rightPercent = ((r.position + r.interval - zoomStart) / range) * 100;
          const left = Math.max(0, leftPercent);
          const right = Math.min(100, rightPercent);
          const width = right - left;
          const chunkMin = r.position;
          const chunkMax = r.position + r.interval;
          const tooltipContent = transformation
            ? `${formatValue(transformValue(chunkMin, transformation), transformation.format)} → ${formatValue(transformValue(chunkMax, transformation), transformation.format)}`
            : `${chunkMin.toLocaleString()} → ${chunkMax.toLocaleString()}`;

          return (
            <div
              key={idx}
              className="absolute h-full bg-indigo-600 cursor-help transition-colors hover:bg-indigo-500"
              style={{
                left: `${left}%`,
                width: `${width}%`,
              }}
              data-tooltip-id={tooltipId}
              data-tooltip-content={tooltipContent}
            />
          );
        })}
      </div>
    );
  }

  // Fallback - empty bar
  return (
    <div
      className={`relative overflow-hidden rounded-lg bg-slate-700 ring-1 ring-slate-600/50 ${className}`}
      style={{ height: `${height}px` }}
    />
  );
}
