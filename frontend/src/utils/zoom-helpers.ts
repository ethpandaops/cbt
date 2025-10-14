/**
 * Default zoom window size (in interval units)
 * This controls how many units are visible by default on initial load
 */
const DEFAULT_ZOOM_WINDOW = 86400;

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
  // Calculate MIN_BOUND: max((MAX_BOUND - 1440), min(incremental models))
  const calculatedMin = Math.max(transformationMax - DEFAULT_ZOOM_WINDOW, transformationMin);

  // Clamp to 0 if negative
  const start = Math.max(0, calculatedMin);

  return {
    start,
    end: transformationMax,
  };
}
