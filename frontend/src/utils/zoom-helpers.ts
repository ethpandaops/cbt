/**
 * Default zoom window size (in interval units)
 * This controls how many units are visible by default on initial load
 */
const DEFAULT_ZOOM_WINDOW = 86400;

/**
 * Predefined window scales for zoom in/out functionality.
 * Optimized for time units (seconds) but works for any unit type.
 * Zooming in moves left (smaller windows), zooming out moves right (larger windows).
 */
export const WINDOW_SCALES: readonly number[] = [
  60, // 1 minute
  300, // 5 minutes
  600, // 10 minutes
  900, // 15 minutes
  1800, // 30 minutes
  3600, // 1 hour
  7200, // 2 hours
  10800, // 3 hours
  14400, // 4 hours
  21600, // 6 hours
  43200, // 12 hours
  86400, // 24 hours / 1 day (DEFAULT)
  172800, // 2 days
  259200, // 3 days
  604800, // 7 days / 1 week
  1209600, // 2 weeks
  2592000, // 30 days / ~1 month
  7776000, // 90 days / ~3 months
  15552000, // 180 days / ~6 months
  31536000, // 365 days / ~1 year
];

/**
 * Find the nearest scale value to a given window size
 */
export function findNearestScale(windowSize: number): number {
  if (windowSize <= WINDOW_SCALES[0]) return WINDOW_SCALES[0];
  if (windowSize >= WINDOW_SCALES[WINDOW_SCALES.length - 1]) return WINDOW_SCALES[WINDOW_SCALES.length - 1];

  let nearest = WINDOW_SCALES[0];
  let minDiff = Math.abs(windowSize - nearest);

  for (const scale of WINDOW_SCALES) {
    const diff = Math.abs(windowSize - scale);
    if (diff < minDiff) {
      minDiff = diff;
      nearest = scale;
    }
  }

  return nearest;
}

/**
 * Get the next smaller window scale (zoom in)
 * @param currentWindow - Current window size
 * @returns Next smaller scale, or current if at minimum
 */
export function getZoomInScale(currentWindow: number): number {
  const currentScale = findNearestScale(currentWindow);
  const currentIndex = WINDOW_SCALES.indexOf(currentScale);

  // If at the smallest scale or not found, return current
  if (currentIndex <= 0) return WINDOW_SCALES[0];

  return WINDOW_SCALES[currentIndex - 1];
}

/**
 * Get the next larger window scale (zoom out)
 * @param currentWindow - Current window size
 * @returns Next larger scale, or current if at maximum
 */
export function getZoomOutScale(currentWindow: number): number {
  const currentScale = findNearestScale(currentWindow);
  const currentIndex = WINDOW_SCALES.indexOf(currentScale);

  // If at the largest scale or not found, return current
  if (currentIndex >= WINDOW_SCALES.length - 1) return WINDOW_SCALES[WINDOW_SCALES.length - 1];

  return WINDOW_SCALES[currentIndex + 1];
}

/**
 * Calculate zoom range for a given window size
 * @param windowSize - Desired window size
 * @param transformationMin - Minimum value from incremental models only
 * @param transformationMax - Maximum value from all models (incremental + external)
 * @returns The zoom range as { start, end }
 */
export function calculateZoomRangeForWindow(
  windowSize: number,
  transformationMin: number,
  transformationMax: number
): { start: number; end: number } {
  const calculatedMin = Math.max(transformationMax - windowSize, transformationMin);
  const start = Math.max(0, calculatedMin);

  return {
    start,
    end: transformationMax,
  };
}

/**
 * Calculate the default zoom range for a given dataset
 *
 * Logic:
 * - MAX_BOUND = max(all models including external)
 * - MIN_BOUND = max((MAX_BOUND - 86400), min(incremental models))
 * - Clamp MIN_BOUND to 0 if negative
 *
 * This creates a sliding window that shows the last 86400 units of data,
 * but won't go before the first incremental model or below 0.
 *
 * @param transformationMin - Minimum value from incremental models only
 * @param transformationMax - Maximum value from all models (incremental + external)
 * @returns The default zoom range as { start, end }
 */
export function calculateDefaultZoomRange(
  transformationMin: number,
  transformationMax: number
): { start: number; end: number } {
  return calculateZoomRangeForWindow(DEFAULT_ZOOM_WINDOW, transformationMin, transformationMax);
}
