package handlers

import (
	"context"
	"fmt"

	"github.com/ethpandaops/cbt/pkg/admin"
	"github.com/ethpandaops/cbt/pkg/api/generated"
	"github.com/ethpandaops/cbt/pkg/models"
	"github.com/gofiber/fiber/v3"
)

// DebugCoverageAtPosition handles GET /api/v1/models/transformations/{id}/coverage/{position}
// This is the comprehensive debugging endpoint that uses the same validation logic
// as backfill and dependency checking. It provides a single source of truth for debugging
// why a position cannot be processed by showing:
// - The model's own coverage and gaps
// - All dependencies' bounds and gaps (recursively)
// - Validation results using the same logic as the coordinator
func (s *Server) DebugCoverageAtPosition(c fiber.Ctx, id string, position int) error {
	// Verify it's an incremental transformation
	dag := s.modelsService.GetDAG()
	node, err := dag.GetTransformationNode(id)
	if err != nil {
		return ErrModelNotFound
	}

	cfg := node.GetConfig()
	if !cfg.IsIncrementalType() {
		return fiber.NewError(fiber.StatusBadRequest, "model is not incremental type")
	}

	ctx := c.Context()
	pos := uint64(position) // nolint:gosec // position is a non-negative API path parameter

	// Auto-detect interval: check if position is in a gap and use gap size, otherwise use model's max interval
	interval := s.autoDetectInterval(ctx, id, pos, node.GetHandler())

	// Build the comprehensive debug response
	endPosition := toInt(pos + interval)
	debug := generated.CoverageDebug{
		ModelId:     id,
		Position:    position,
		Interval:    toInt(interval),
		EndPosition: &endPosition,
	}

	// Get model's own coverage information
	modelCoverage, err := s.buildModelCoverageInfo(ctx, id, pos, interval)
	if err != nil {
		s.log.WithError(err).WithField("model_id", id).Error("Failed to build model coverage info")
		return fiber.NewError(fiber.StatusInternalServerError, "failed to build model coverage info")
	}
	debug.ModelCoverage = modelCoverage

	// Build recursive dependency tree with coverage/bounds for each
	dependencies := s.buildDependencyDebugTree(ctx, dag, node, pos, interval)
	debug.Dependencies = dependencies

	// Run validation using the same logic as coordinator (if validator available)
	validationInfo := s.buildValidationDebugInfo(ctx, id, pos, interval, dependencies)
	debug.Validation = validationInfo

	// Determine if can process based on validation
	canProcess := validationInfo.InBounds && !validationInfo.HasDependencyGaps
	debug.CanProcess = canProcess

	return c.JSON(debug)
}

// filterOverlappingGaps converts the gaps that overlap the window
// [position, position+interval) into API GapInfo values.
func filterOverlappingGaps(allGaps []admin.GapInfo, position, interval uint64) []generated.GapInfo {
	endPos := position + interval

	gapsInWindow := make([]generated.GapInfo, 0, len(allGaps))
	for _, gap := range allGaps {
		overlaps := gap.StartPos < endPos && gap.EndPos > position
		if !overlaps {
			continue
		}
		gapsInWindow = append(gapsInWindow, generated.GapInfo{
			Start:           toInt(gap.StartPos),
			End:             toInt(gap.EndPos),
			Size:            toInt(gap.EndPos - gap.StartPos),
			OverlapsRequest: &overlaps,
		})
	}

	return gapsInWindow
}

// buildModelCoverageInfo gets coverage info for the target model itself
func (s *Server) buildModelCoverageInfo(ctx context.Context, modelID string, position, interval uint64) (generated.ModelCoverageInfo, error) {
	info := generated.ModelCoverageInfo{
		HasData:         false,
		FirstPosition:   0,
		LastEndPosition: 0,
	}

	// Get first and last positions
	firstPos, err := s.adminService.GetFirstPosition(ctx, modelID)
	if err != nil {
		return info, err
	}

	lastEndPos, err := s.adminService.GetNextUnprocessedPosition(ctx, modelID)
	if err != nil {
		return info, err
	}

	info.HasData = lastEndPos > 0
	info.FirstPosition = toInt(firstPos)
	info.LastEndPosition = toInt(lastEndPos)

	// Get all processed ranges and filter to window
	allRanges, err := s.adminService.GetProcessedRanges(ctx, modelID)
	if err != nil {
		return info, err
	}

	// Filter to ranges that overlap our window [position, position+interval]
	rangesInWindow := []generated.Range{}
	for _, r := range allRanges {
		rangeEnd := r.Position + r.Interval
		if rangeEnd > position && r.Position < position+interval {
			rangesInWindow = append(rangesInWindow, generated.Range{
				Position: toInt(r.Position),
				Interval: toInt(r.Interval),
			})
		}
	}
	if len(rangesInWindow) > 0 {
		info.RangesInWindow = &rangesInWindow
	}

	// Find ALL gaps across the model's full range (same as coordinator does)
	// Then filter to gaps that overlap our requested position window
	allGaps, err := s.adminService.FindGaps(ctx, modelID, firstPos, lastEndPos, interval)
	if err != nil {
		return info, err
	}

	// Filter to gaps that overlap the requested position window
	gapsInWindow := filterOverlappingGaps(allGaps, position, interval)
	if len(gapsInWindow) > 0 {
		info.GapsInWindow = &gapsInWindow
	}

	return info, nil
}

// buildDependencyDebugTree recursively builds dependency debug info
func (s *Server) buildDependencyDebugTree(ctx context.Context, dag models.DAGReader, node models.Transformation, position, interval uint64) []generated.DependencyDebugInfo {
	handler := node.GetHandler()
	if handler == nil {
		return []generated.DependencyDebugInfo{}
	}

	// Try to get structured dependencies (with OR group support)
	if provider, ok := handler.(structuredDepProvider); ok {
		return s.processStructuredDependencies(ctx, dag, provider, position, interval)
	}

	// Fallback: try flat dependencies
	if provider, ok := handler.(flatDepProvider); ok {
		deps := provider.GetFlattenedDependencies()
		result := make([]generated.DependencyDebugInfo, 0, len(deps))

		for _, depID := range deps {
			depInfo, err := s.buildSingleDependencyDebugInfo(ctx, dag, depID, position, interval)
			if err != nil {
				s.log.WithError(err).WithField("dep_id", depID).Debug("Failed to build dependency debug info")
				continue
			}
			result = append(result, depInfo)
		}

		return result
	}

	return []generated.DependencyDebugInfo{}
}

func (s *Server) processStructuredDependencies(ctx context.Context, dag models.DAGReader, provider structuredDepProvider, position, interval uint64) []generated.DependencyDebugInfo {
	deps := provider.GetDependencies()
	result := make([]generated.DependencyDebugInfo, 0, len(deps))

	for _, dep := range deps {
		if dep.IsGroup {
			// OR group - analyze all members
			orGroupInfo := s.buildORGroupDebugInfo(ctx, dag, dep.GroupDeps, position, interval)
			result = append(result, orGroupInfo)
		} else {
			// Single dependency
			depInfo, err := s.buildSingleDependencyDebugInfo(ctx, dag, dep.SingleDep, position, interval)
			if err != nil {
				s.log.WithError(err).WithField("dep_id", dep.SingleDep).Debug("Failed to build dependency debug info")
				continue
			}
			result = append(result, depInfo)
		}
	}

	return result
}

// buildORGroupDebugInfo builds debug info for an OR group
func (s *Server) buildORGroupDebugInfo(ctx context.Context, dag models.DAGReader, groupDeps []string, position, interval uint64) generated.DependencyDebugInfo {
	orGroupInfo := generated.DependencyDebugInfo{
		Id:       fmt.Sprintf("[OR: %v]", groupDeps),
		Type:     generated.OrGroup,
		NodeType: generated.DependencyDebugInfoNodeTypeTransformation, // placeholder
	}

	// Analyze each member of the OR group
	members := make([]generated.DependencyDebugInfo, 0, len(groupDeps))
	hasAnyData := false

	for _, depID := range groupDeps {
		memberInfo, err := s.buildSingleDependencyDebugInfo(ctx, dag, depID, position, interval)
		if err != nil {
			s.log.WithError(err).WithField("dep_id", depID).Debug("Failed to build OR group member debug info")
			continue
		}

		members = append(members, memberInfo)

		// Check if this member has data
		if memberInfo.CoverageStatus != nil && *memberInfo.CoverageStatus != generated.NotInitialized && *memberInfo.CoverageStatus != generated.NoData {
			hasAnyData = true
		}
	}

	orGroupInfo.OrGroupMembers = &members

	// Determine if OR group is blocking
	blocking := !hasAnyData
	orGroupInfo.Blocking = &blocking

	return orGroupInfo
}

// buildSingleDependencyDebugInfo builds debug info for a single dependency
func (s *Server) buildSingleDependencyDebugInfo(ctx context.Context, dag models.DAGReader, depID string, position, interval uint64) (generated.DependencyDebugInfo, error) {
	depNode, err := dag.GetNode(depID)
	if err != nil {
		return generated.DependencyDebugInfo{}, err
	}

	depInfo := generated.DependencyDebugInfo{
		Id:   depID,
		Type: generated.Required, // AND dependency
	}

	// Set node type
	switch depNode.NodeType {
	case models.NodeTypeExternal:
		depInfo.NodeType = generated.DependencyDebugInfoNodeTypeExternal
		result, err := s.buildExternalDependencyDebugInfo(ctx, depNode, &depInfo, position, interval)
		if err != nil {
			return generated.DependencyDebugInfo{}, err
		}
		return *result, nil

	case models.NodeTypeTransformation:
		depInfo.NodeType = generated.DependencyDebugInfoNodeTypeTransformation
		result, err := s.buildTransformationDependencyDebugInfo(ctx, dag, depNode, &depInfo, position, interval)
		if err != nil {
			return generated.DependencyDebugInfo{}, err
		}
		return *result, nil

	default:
		return depInfo, nil
	}
}

// buildExternalDependencyDebugInfo builds debug info for external dependencies
func (s *Server) buildExternalDependencyDebugInfo(ctx context.Context, depNode models.Node, depInfo *generated.DependencyDebugInfo, position, interval uint64) (*generated.DependencyDebugInfo, error) {
	external, ok := depNode.Model.(models.External)
	if !ok {
		return depInfo, ErrNodeNotExternal
	}

	// Get bounds from cache
	boundsCache, err := s.adminService.GetExternalBounds(ctx, external.GetID())
	if err != nil || boundsCache == nil {
		// No bounds available
		depInfo.Bounds = generated.BoundsInfo{
			HasData: false,
			Min:     0,
			Max:     0,
		}
		notInit := generated.NotInitialized
		depInfo.CoverageStatus = &notInit
		return depInfo, nil
	}

	// External models don't have gaps (assumed continuous)
	// Apply lag if configured
	maxBound := boundsCache.Max
	cfg := external.GetConfig()
	var lagApplied *int
	if cfg.Lag > 0 {
		if maxBound > cfg.Lag {
			maxBound -= cfg.Lag
		}
		lagInt := toInt(cfg.Lag)
		lagApplied = &lagInt
	}

	depInfo.Bounds = generated.BoundsInfo{
		HasData:    boundsCache.Max > 0,
		Min:        toInt(boundsCache.Min),
		Max:        toInt(maxBound),
		LagApplied: lagApplied,
	}

	// Determine coverage status
	endPos := position + interval
	switch {
	case boundsCache.Max == 0:
		status := generated.NoData
		depInfo.CoverageStatus = &status
	case position >= uint64(depInfo.Bounds.Min) && endPos <= uint64(depInfo.Bounds.Max): // nolint:gosec // bounds derived from non-negative positions
		status := generated.FullCoverage
		depInfo.CoverageStatus = &status
	default:
		status := generated.NoData
		depInfo.CoverageStatus = &status
	}

	// External is blocking if position is out of bounds
	blocking := endPos > uint64(depInfo.Bounds.Max) || position < uint64(depInfo.Bounds.Min) // nolint:gosec // bounds derived from non-negative positions
	depInfo.Blocking = &blocking

	return depInfo, nil
}

// buildTransformationDependencyDebugInfo builds debug info for transformation dependencies
func (s *Server) buildTransformationDependencyDebugInfo(ctx context.Context, dag models.DAGReader, depNode models.Node, depInfo *generated.DependencyDebugInfo, position, interval uint64) (*generated.DependencyDebugInfo, error) {
	trans, ok := depNode.Model.(models.Transformation)
	if !ok {
		return depInfo, ErrNodeNotTransformation
	}

	depID := trans.GetID()
	handler := trans.GetHandler()

	// Set incremental flag
	s.setIncrementalFlag(depInfo, handler)

	// Get and set bounds
	firstPos, lastEndPos := s.getTransformationBounds(ctx, depID)
	depInfo.Bounds = generated.BoundsInfo{
		HasData: lastEndPos > 0,
		Min:     toInt(firstPos),
		Max:     toInt(lastEndPos),
	}

	// Find gaps if incremental
	isIncremental := depInfo.IsIncremental != nil && *depInfo.IsIncremental
	var gapsList []generated.GapInfo
	if isIncremental {
		gapsList = s.findOverlappingGaps(ctx, depID, firstPos, lastEndPos, position, interval)
		if len(gapsList) > 0 {
			depInfo.Gaps = &gapsList
		}
	}

	// Determine coverage status and blocking
	endPos := position + interval
	s.setCoverageStatus(depInfo, lastEndPos, firstPos, position, endPos, gapsList)
	s.setBlockingStatus(depInfo, gapsList, lastEndPos, firstPos, position, endPos)

	// Recursively get child dependencies
	childDeps := s.buildDependencyDebugTree(ctx, dag, trans, position, interval)
	if len(childDeps) > 0 {
		depInfo.ChildDependencies = &childDeps
	}

	return depInfo, nil
}

func (s *Server) setIncrementalFlag(depInfo *generated.DependencyDebugInfo, handler any) {
	isIncremental := handler != nil
	if trackable, ok := handler.(positionTracker); ok {
		isIncremental = trackable.ShouldTrackPosition()
	}
	depInfo.IsIncremental = &isIncremental
}

func (s *Server) getTransformationBounds(ctx context.Context, depID string) (firstPos, lastEndPos uint64) {
	firstPos, err := s.adminService.GetFirstPosition(ctx, depID)
	if err != nil {
		s.log.WithError(err).WithField("dep_id", depID).Debug("Failed to get first position")
	}

	lastEndPos, err = s.adminService.GetNextUnprocessedPosition(ctx, depID)
	if err != nil {
		s.log.WithError(err).WithField("dep_id", depID).Debug("Failed to get last end position")
	}

	return firstPos, lastEndPos
}

func (s *Server) findOverlappingGaps(ctx context.Context, depID string, firstPos, lastEndPos, position, interval uint64) []generated.GapInfo {
	allGaps, err := s.adminService.FindGaps(ctx, depID, firstPos, lastEndPos, interval)
	if err != nil {
		s.log.WithError(err).WithField("dep_id", depID).Debug("Failed to find gaps")
		return nil
	}

	gapsList := filterOverlappingGaps(allGaps, position, interval)
	if len(gapsList) == 0 {
		return nil
	}

	return gapsList
}

func (s *Server) setCoverageStatus(depInfo *generated.DependencyDebugInfo, lastEndPos, firstPos, position, endPos uint64, gapsList []generated.GapInfo) {
	var status generated.DependencyDebugInfoCoverageStatus

	switch {
	case lastEndPos == 0:
		status = generated.NotInitialized
	case len(gapsList) > 0:
		status = generated.HasGaps
	case position >= firstPos && endPos <= lastEndPos:
		status = generated.FullCoverage
	default:
		status = generated.NoData
	}

	depInfo.CoverageStatus = &status
}

func (s *Server) setBlockingStatus(depInfo *generated.DependencyDebugInfo, gapsList []generated.GapInfo, lastEndPos, firstPos, position, endPos uint64) {
	blocking := len(gapsList) > 0 || endPos > lastEndPos || position < firstPos
	depInfo.Blocking = &blocking
}

// buildValidationDebugInfo builds validation info using dependency analysis
func (s *Server) buildValidationDebugInfo(_ context.Context, _ string, position, interval uint64, dependencies []generated.DependencyDebugInfo) generated.ValidationDebugInfo {
	info := s.initializeValidationInfo()

	// Process dependencies and calculate valid range
	minValid, maxValid, hasAnyDep := s.processValidationDependencies(&info, dependencies, position)

	// Set valid range and check bounds if we have dependencies
	if hasAnyDep {
		s.setValidationRange(&info, minValid, maxValid, position, interval)
	}

	// Calculate next valid position from blocking gaps
	s.calculateNextValidPosition(&info)

	return info
}

func (s *Server) initializeValidationInfo() generated.ValidationDebugInfo {
	blockingGaps := []struct {
		DependencyId string            `json:"dependency_id"` // nolint:revive
		Gap          generated.GapInfo `json:"gap"`
	}{}
	reasons := []string{}

	return generated.ValidationDebugInfo{
		InBounds:          false,
		HasDependencyGaps: false,
		BlockingGaps:      &blockingGaps,
		Reasons:           &reasons,
	}
}

func (s *Server) processValidationDependencies(info *generated.ValidationDebugInfo, dependencies []generated.DependencyDebugInfo, position uint64) (minValid, maxValid uint64, hasAnyDep bool) {
	minValid = uint64(0)
	maxValid = ^uint64(0) // max uint64
	hasAnyDep = false

	blockingGaps := *info.BlockingGaps
	reasons := *info.Reasons

	for i := range dependencies {
		dep := &dependencies[i]
		hasAnyDep = true

		// Update min/max based on dependency bounds
		minValid, maxValid = s.updateValidRange(dep, minValid, maxValid)

		// Check for blocking issues
		s.processBlockingDependency(dep, &blockingGaps, &reasons, info, position)
	}

	info.BlockingGaps = &blockingGaps
	info.Reasons = &reasons

	return minValid, maxValid, hasAnyDep
}

func (s *Server) updateValidRange(dep *generated.DependencyDebugInfo, minValid, maxValid uint64) (newMinValid, newMaxValid uint64) {
	newMinValid = minValid
	newMaxValid = maxValid
	if dep.Bounds.HasData {
		if uint64(dep.Bounds.Min) > newMinValid { // nolint:gosec // bounds derived from non-negative positions
			newMinValid = uint64(dep.Bounds.Min) // nolint:gosec // bounds derived from non-negative positions
		}
		if uint64(dep.Bounds.Max) < newMaxValid { // nolint:gosec // bounds derived from non-negative positions
			newMaxValid = uint64(dep.Bounds.Max) // nolint:gosec // bounds derived from non-negative positions
		}
	}
	return newMinValid, newMaxValid
}

func (s *Server) processBlockingDependency(dep *generated.DependencyDebugInfo, blockingGaps *[]struct {
	DependencyId string            `json:"dependency_id"` // nolint:revive
	Gap          generated.GapInfo `json:"gap"`
}, reasons *[]string, info *generated.ValidationDebugInfo, position uint64) {
	if dep.Blocking == nil || !*dep.Blocking {
		return
	}

	if dep.Gaps != nil && len(*dep.Gaps) > 0 {
		s.processBlockingGaps(dep, blockingGaps, reasons, info)
	} else if dep.CoverageStatus != nil {
		s.processBlockingStatus(dep, reasons, position)
	}
}

func (s *Server) processBlockingGaps(dep *generated.DependencyDebugInfo, blockingGaps *[]struct {
	DependencyId string            `json:"dependency_id"` // nolint:revive
	Gap          generated.GapInfo `json:"gap"`
}, reasons *[]string, info *generated.ValidationDebugInfo) {
	info.HasDependencyGaps = true
	for _, gap := range *dep.Gaps {
		if gap.OverlapsRequest != nil && *gap.OverlapsRequest {
			*blockingGaps = append(*blockingGaps, struct {
				DependencyId string            `json:"dependency_id"` // nolint:revive
				Gap          generated.GapInfo `json:"gap"`
			}{
				DependencyId: dep.Id,
				Gap:          gap,
			})
			*reasons = append(*reasons, fmt.Sprintf("Dependency %s has gap from %d to %d", dep.Id, gap.Start, gap.End))
		}
	}
}

func (s *Server) processBlockingStatus(dep *generated.DependencyDebugInfo, reasons *[]string, position uint64) {
	switch *dep.CoverageStatus {
	case generated.NotInitialized:
		*reasons = append(*reasons, fmt.Sprintf("Dependency %s is not initialized", dep.Id))
	case generated.NoData:
		*reasons = append(*reasons, fmt.Sprintf("Dependency %s has no data for position %d", dep.Id, position))
	case generated.FullCoverage, generated.HasGaps:
		// These cases shouldn't happen when blocking is true, but handle for exhaustiveness
	}
}

func (s *Server) setValidationRange(info *generated.ValidationDebugInfo, minValid, maxValid, position, interval uint64) {
	info.ValidRange = &struct {
		Max int `json:"max"`
		Min int `json:"min"`
	}{
		Min: toInt(minValid),
		Max: toInt(maxValid),
	}

	endPos := position + interval
	info.InBounds = position >= minValid && endPos <= maxValid

	reasons := *info.Reasons
	if !info.InBounds {
		if position < minValid {
			reasons = append(reasons, fmt.Sprintf("Position %d is before earliest available data at %d", position, minValid))
		}
		if endPos > maxValid {
			reasons = append(reasons, fmt.Sprintf("Position end %d exceeds latest available data at %d", endPos, maxValid))
			nextValid := toInt(maxValid)
			info.NextValidPosition = &nextValid
		}
	}
	info.Reasons = &reasons
}

func (s *Server) calculateNextValidPosition(info *generated.ValidationDebugInfo) {
	blockingGaps := *info.BlockingGaps
	if info.HasDependencyGaps && len(blockingGaps) > 0 {
		maxGapEnd := 0
		for _, bg := range blockingGaps {
			if bg.Gap.End > maxGapEnd {
				maxGapEnd = bg.Gap.End
			}
		}
		if maxGapEnd > 0 {
			info.NextValidPosition = &maxGapEnd
		}
	}
}

// autoDetectInterval determines the appropriate interval for debugging a position.
// It checks if the position falls within a gap and uses the gap size if so,
// otherwise falls back to the model's max interval.
func (s *Server) autoDetectInterval(ctx context.Context, modelID string, position uint64, handler any) uint64 {
	// Default to model's max interval
	defaultInterval := uint64(60) // fallback minimum
	if incrementalHandler, ok := handler.(intervalProvider); ok {
		_, maxInterval := incrementalHandler.GetInterval()
		if maxInterval > 0 {
			defaultInterval = maxInterval
		}
	}

	// Get model bounds to find gaps across full range
	firstPos, err := s.adminService.GetFirstPosition(ctx, modelID)
	if err != nil {
		s.log.WithError(err).WithField("model_id", modelID).Debug("Failed to get first position for auto-detect")
		return defaultInterval
	}

	lastEndPos, err := s.adminService.GetNextUnprocessedPosition(ctx, modelID)
	if err != nil {
		s.log.WithError(err).WithField("model_id", modelID).Debug("Failed to get last end position for auto-detect")
		return defaultInterval
	}

	// If no data yet, use default
	if lastEndPos == 0 {
		return defaultInterval
	}

	// Find all gaps across the model's full range
	allGaps, err := s.adminService.FindGaps(ctx, modelID, firstPos, lastEndPos, defaultInterval)
	if err != nil {
		s.log.WithError(err).WithField("model_id", modelID).Debug("Failed to find gaps for auto-detect")
		return defaultInterval
	}

	// Check if position falls within any gap
	for _, gap := range allGaps {
		if position >= gap.StartPos && position < gap.EndPos {
			// Position is in a gap, use the gap size as interval
			gapSize := gap.EndPos - gap.StartPos
			s.log.WithFields(map[string]any{
				"model_id": modelID,
				"position": position,
				"gap_size": gapSize,
			}).Debug("Auto-detected interval from gap size")
			return gapSize
		}
	}

	// Position is not in a gap, use default interval
	return defaultInterval
}
