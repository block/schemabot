package api

import (
	"context"
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// An apply started with --defer-cutover cuts over every operation free to cut
// over together, so an operator's cutover is refused until each of those
// operations waits at its cutover. An operation an earlier member holds
// (storage CutoverBlocker) is not free yet and is left out: its deployment's
// turn comes with a later cutover.
//
// The rule is checked where the operator's command arrives, against the
// apply's stored options, which carry only the operator's --defer-cutover. A
// data plane is not checked again: a control plane that orders a rollout's
// cutovers defers each operation's cutover on the data plane itself and then
// sends one operation's cutover at a time, which is not an operator deferral.

// deferredCutoverReadiness reports how many of an apply's operations wait at
// their cutover, and which operations free to cut over have not reached it
// yet. tasksByOp holds each operation's tasks, keyed by operation ID. An
// operation that ended, or whose cutover has already started, counts as
// neither.
func deferredCutoverReadiness(ctx context.Context, opStore storage.ApplyOperationStore, ops []*storage.ApplyOperation, tasksByOp map[int64][]*storage.Task) (waiting int, notReached []*storage.ApplyOperation, err error) {
	if opStore == nil {
		return 0, nil, fmt.Errorf("apply operation store is not available")
	}
	for _, op := range ops {
		if op == nil || op.OperationKind == storage.ApplyOperationKindGroupFinalizer || state.IsApplyOperationTerminal(op.State) {
			continue
		}
		tasks := tasksByOp[op.ID]
		switch {
		case operationTasksInState(tasks, state.Task.CuttingOver):
			continue
		case operationTasksInState(tasks, state.Task.WaitingForCutover):
			waiting++
			continue
		}
		blocker, err := opStore.CutoverBlocker(ctx, op.ID)
		if err != nil {
			return 0, nil, fmt.Errorf("check whether an earlier member holds apply_operation %d before a deferred cutover: %w", op.ID, err)
		}
		if blocker != nil {
			continue
		}
		notReached = append(notReached, op)
	}
	return waiting, notReached, nil
}

// operationTasksInState reports whether any of an operation's tasks is in taskState.
func operationTasksInState(tasks []*storage.Task, taskState string) bool {
	for _, task := range tasks {
		if state.IsState(task.State, taskState) {
			return true
		}
	}
	return false
}

// deferredCutoverNamesShown bounds how many operations a not-ready refusal
// names, so a refusal for a wide apply stays one readable line.
const deferredCutoverNamesShown = 3

// deferredCutoverNotReadyMessage refuses a cutover of an apply that deferred
// its cutover while the operations in notReached have not reached it.
func deferredCutoverNotReadyMessage(waiting int, notReached []*storage.ApplyOperation) string {
	total := waiting + len(notReached)
	shown := notReached
	if len(shown) > deferredCutoverNamesShown {
		shown = shown[:deferredCutoverNamesShown]
	}
	names := make([]string, 0, len(shown))
	for _, op := range shown {
		names = append(names, cutoverOperationName(op))
	}
	list := strings.Join(names, ", ")
	if more := len(notReached) - len(shown); more > 0 {
		list = fmt.Sprintf("%s and %d more", list, more)
	}
	verb := "has"
	if len(notReached) > 1 {
		verb = "have"
	}
	return fmt.Sprintf("cutover is not ready: %s %s not reached cutover (%d of %d wait for it); --defer-cutover cuts them all over together, so run cutover again once all %d wait",
		list, verb, waiting, total, total)
}

// cutoverOperationName names an operation by the most specific
// address it carries: the shard and table of a per-shard operation, then its
// target, then its deployment.
func cutoverOperationName(op *storage.ApplyOperation) string {
	switch {
	case op.OperationKey != "":
		return op.OperationKey
	case op.Target != "":
		return op.Target
	default:
		return op.Deployment
	}
}
