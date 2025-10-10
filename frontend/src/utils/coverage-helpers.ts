import type { Range } from '@api/types.gen';

/**
 * Find the coverage segment that contains the given position
 */
export function findCoverageAtPosition(ranges: Range[] | undefined, position: number): Range | null {
  if (!ranges || ranges.length === 0) return null;

  for (const range of ranges) {
    const rangeEnd = range.position + range.interval;
    if (position >= range.position && position < rangeEnd) {
      return range;
    }
  }

  return null;
}

/**
 * Calculate pixel position for a data position within zoom range
 */
export function calculatePixelPosition(
  dataPosition: number,
  zoomStart: number,
  zoomEnd: number,
  containerWidth: number
): number {
  const range = zoomEnd - zoomStart || 1;
  const percent = ((dataPosition - zoomStart) / range) * 100;
  return (percent / 100) * containerWidth;
}

/**
 * Calculate data position from pixel position within zoom range
 */
export function calculateDataPosition(
  pixelX: number,
  containerWidth: number,
  zoomStart: number,
  zoomEnd: number
): number {
  const percent = (pixelX / containerWidth) * 100;
  const range = zoomEnd - zoomStart;
  return zoomStart + (percent / 100) * range;
}

/**
 * Merge adjacent/overlapping ranges (same logic as CoverageBar)
 */
export function mergeRanges(ranges: Range[]): Range[] {
  if (ranges.length === 0) return [];

  const sorted = [...ranges].sort((a, b) => a.position - b.position);
  const merged: Range[] = [];

  for (const curr of sorted) {
    if (merged.length === 0) {
      merged.push({ ...curr });
    } else {
      const last = merged[merged.length - 1];
      const lastEnd = last.position + last.interval;
      const currEnd = curr.position + curr.interval;

      if (curr.position <= lastEnd) {
        // Overlapping or adjacent - merge
        last.interval = Math.max(lastEnd, currEnd) - last.position;
      } else {
        merged.push({ ...curr });
      }
    }
  }

  return merged;
}

/**
 * Find the gap (missing coverage) that contains the given position
 */
export function findGapAtPosition(
  ranges: Range[] | undefined,
  position: number,
  zoomStart: number,
  zoomEnd: number
): Range | null {
  if (!ranges || ranges.length === 0) {
    // Entire range is a gap
    return { position: zoomStart, interval: zoomEnd - zoomStart };
  }

  const merged = mergeRanges(ranges);

  // Check if position is before first coverage
  if (position < merged[0].position) {
    return { position: zoomStart, interval: merged[0].position - zoomStart };
  }

  // Check gaps between merged ranges
  for (let i = 0; i < merged.length - 1; i++) {
    const currentEnd = merged[i].position + merged[i].interval;
    const nextStart = merged[i + 1].position;

    if (position >= currentEnd && position < nextStart) {
      return { position: currentEnd, interval: nextStart - currentEnd };
    }
  }

  // Check if position is after last coverage
  const lastEnd = merged[merged.length - 1].position + merged[merged.length - 1].interval;
  if (position >= lastEnd) {
    return { position: lastEnd, interval: zoomEnd - lastEnd };
  }

  return null;
}
