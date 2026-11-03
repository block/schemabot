package storage

import "github.com/block/schemabot/pkg/state"

// DeferredCutoverHeldAtOnce counts the most drivers one apply deferring its
// cutover would keep waiting there at once: its work operations that are not
// already settled, counting a stopped one since starting its apply again
// resumes it, taken in the order the claim starts them (ST-10), and capped at
// maxDriversPerApply. A waiting operation keeps counting against that cap, so
// an apply wider than it holds no more drivers than the cap; apply creation
// refuses such an apply, so only one admitted under a larger cap is counted
// this way. Under `rolling`, and any policy the claim
// does not order as barrier or parallel, a member starts only once every
// earlier member has completed, and a held member completes only once an
// operator cuts it over, so one member holds at a time. Under `barrier` and
// `parallel` every deployment's members hold together. A finalizer runs after
// the work it finalizes and never waits at a cutover. A maxDriversPerApply of
// zero or less leaves the count uncapped.
func DeferredCutoverHeldAtOnce(ops []*ApplyOperation, maxDriversPerApply int) int {
	type rolloutMember struct{ deployment, target string }
	ordered := false
	byMember := make(map[rolloutMember]int)
	byDeployment := make(map[string]int)
	for _, op := range ops {
		if op.OperationKind != ApplyOperationKindWork || state.IsState(op.State, state.SettledApplyStates...) {
			continue
		}
		if IsOrderedCutoverPolicy(op.CutoverPolicy) {
			ordered = true
		}
		byMember[rolloutMember{deployment: op.Deployment, target: op.Target}]++
		byDeployment[op.Deployment]++
	}
	width := 0
	if ordered {
		for _, operations := range byDeployment {
			width += operations
		}
	} else {
		for _, operations := range byMember {
			width = max(width, operations)
		}
	}
	if maxDriversPerApply > 0 {
		width = min(width, maxDriversPerApply)
	}
	return width
}

// DeferredCutoverHeldAtOnceByDeployment counts, for each deployment, the most
// drivers of that deployment one apply deferring its cutover would keep waiting
// there at once (DeferredCutoverHeldAtOnce over the apply's operations on that
// deployment). A deployment none of whose work is still to settle is left out.
func DeferredCutoverHeldAtOnceByDeployment(ops []*ApplyOperation, maxDriversPerApply int) map[string]int {
	byDeployment := make(map[string][]*ApplyOperation)
	for _, op := range ops {
		byDeployment[op.Deployment] = append(byDeployment[op.Deployment], op)
	}
	widths := make(map[string]int, len(byDeployment))
	for deployment, deploymentOps := range byDeployment {
		if width := DeferredCutoverHeldAtOnce(deploymentOps, maxDriversPerApply); width > 0 {
			widths[deployment] = width
		}
	}
	return widths
}

// DeferredCutoverHeldAtOnceByPool counts, for each driver pool, the most
// drivers of that pool one apply deferring its cutover would keep waiting there
// at once (DeferredCutoverHeldAtOnce over the apply's operations on the
// deployments poolOf maps to it, uncapped). Several deployments, or one
// deployment under several environments, can reach the same pool, and an
// operation holds a driver of the pool whichever route sent it. An operation on
// a deployment poolOf does not name is left out, as is a pool none of whose
// work is still to settle.
func DeferredCutoverHeldAtOnceByPool(ops []*ApplyOperation, poolOf map[string]string) map[string]int {
	byPool := make(map[string][]*ApplyOperation)
	for _, op := range ops {
		if pool, ok := poolOf[op.Deployment]; ok {
			byPool[pool] = append(byPool[pool], op)
		}
	}
	widths := make(map[string]int, len(byPool))
	for pool, poolOps := range byPool {
		if width := DeferredCutoverHeldAtOnce(poolOps, 0); width > 0 {
			widths[pool] = width
		}
	}
	return widths
}
