package storage

import "github.com/block/schemabot/pkg/state"

// DeferredCutoverHeldAtOnce counts the most operations of one apply that would
// hold at a deferred cutover at once: its work operations that are not already
// settled, counting a stopped one since starting its apply again resumes it, taken in the order the claim starts them (ST-10). Under `rolling`, and
// any policy the claim does not order as barrier or parallel, a member starts
// only once every earlier member has completed, and a held member completes
// only once an operator cuts it over, so one member holds at a time. Under
// `barrier` and `parallel` every deployment's members hold together. Within a
// deployment, a table step starts only once the step before it completed, so
// one step of each holds at a time, beside the operations that run a member's
// whole change. A finalizer runs after the work it finalizes and never waits
// at a cutover.
func DeferredCutoverHeldAtOnce(ops []*ApplyOperation) int {
	type rolloutMember struct{ deployment, target string }
	ordered := false
	byMember := make(map[rolloutMember]*heldAtOnce)
	byDeployment := make(map[string]*heldAtOnce)
	for _, op := range ops {
		if op.OperationKind != ApplyOperationKindWork || state.IsState(op.State, state.SettledApplyStates...) {
			continue
		}
		if IsOrderedCutoverPolicy(op.CutoverPolicy) {
			ordered = true
		}
		member := rolloutMember{deployment: op.Deployment, target: op.Target}
		if byMember[member] == nil {
			byMember[member] = &heldAtOnce{steps: make(map[int]int)}
		}
		byMember[member].add(op.RolloutStep)
		if byDeployment[op.Deployment] == nil {
			byDeployment[op.Deployment] = &heldAtOnce{steps: make(map[int]int)}
		}
		byDeployment[op.Deployment].add(op.RolloutStep)
	}
	width := 0
	if ordered {
		for _, deployment := range byDeployment {
			width += deployment.width()
		}
		return width
	}
	for _, member := range byMember {
		width = max(width, member.width())
	}
	return width
}

// heldAtOnce counts the operations of one rollout member or deployment that
// can hold at a cutover together: every operation that runs a member's whole
// change, and the operations of its widest table step.
type heldAtOnce struct {
	wholeChange int
	steps       map[int]int
}

func (h *heldAtOnce) add(rolloutStep int) {
	if rolloutStep == 0 {
		h.wholeChange++
		return
	}
	h.steps[rolloutStep]++
}

func (h *heldAtOnce) width() int {
	widest := 0
	for _, operations := range h.steps {
		widest = max(widest, operations)
	}
	return h.wholeChange + widest
}
