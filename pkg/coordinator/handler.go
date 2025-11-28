package coordinator

// determineIntervalForGap calculates the appropriate interval size for processing a gap.
// The interval is bounded by minInterval and maxInterval, and sized appropriately for the gap.
func determineIntervalForGap(gapSize, minInterval, maxInterval uint64) uint64 {
	switch {
	case minInterval == 0:
		// If min interval is 0, use gap size but cap at maxInterval
		if gapSize > maxInterval {
			return maxInterval
		}

		return gapSize
	case gapSize < minInterval:
		// If gap is smaller than min interval, use min interval (may overlap)
		return minInterval
	case gapSize < maxInterval:
		// If gap is between min and max, use the gap size
		return gapSize
	default:
		// Use max interval for large gaps
		return maxInterval
	}
}
