package storage

import "github.com/block/schemabot/pkg/state"

// DeferredCutoverHeldAtOnce counts the most drivers one apply deferring its
// cutover would keep waiting there at once: its work operations that are not
// already settled, taken in the order the claim starts them (ST-10). Under
// `rolling`, and any policy the claim does not order as barrier or parallel, a
// member starts only once every earlier member has completed, and a held member
// completes only once an operator cuts it over, so one member holds at a time.
// Under `barrier` and `parallel` every deployment's members hold together. A
// finalizer runs after the work it finalizes and never waits at a cutover.
func DeferredCutoverHeldAtOnce(ops []*ApplyOperation) int {
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
	return width
}

// DeferredCutoverDrivers reports how many of one server's drivers may run work
// for an apply deferring its cutover: all but the one the server keeps for
// other work, so deferred applies together never leave every driver waiting on
// a person. A server with a single driver keeps none back, so its only driver
// can still run a deferred apply.
func DeferredCutoverDrivers(drivers int) int {
	if drivers < 2 {
		return drivers
	}
	return drivers - 1
}
