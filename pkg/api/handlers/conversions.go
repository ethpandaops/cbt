package handlers

// toInt converts a uint64 position/interval value to int for API responses.
//
// All values converted here originate from ClickHouse position and interval
// columns, which are bounded well within the int range on the 64-bit platforms
// this service targets. The conversion is therefore safe and gosec's overflow
// warning (G115) is suppressed once here rather than at every call site.
func toInt(v uint64) int {
	return int(v) //nolint:gosec // bounded by ClickHouse position ranges; see doc comment
}
