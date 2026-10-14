package tern

import (
	"context"
	"fmt"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// isCutoverDriveState reports whether an operation is in a phase the ordered
// cutover drive may resume: parked at the barrier (waiting_for_cutover) or
// already mid-cutover (cutting_over / revert_window) for stale-lease recovery.
// These are exactly the states the OC-1 cutover-claim predicate selects. A
// copy-phase or terminal operation is rejected so a mismatched or stale claim
// can never force an unrelated operation through the high-risk swap.
func isCutoverDriveState(opState string) bool {
	return state.IsState(opState, state.Apply.WaitingForCutover, state.Apply.CuttingOver, state.Apply.RevertWindow)
}

// shouldAutoDeferCutover reports whether an operation-scoped copy drive must park
// at the cutover barrier automatically. It is true only for an operation of a
// multi-deployment (fan-out) apply running under an ordered-cutover policy
// (barrier or parallel), so the high-risk cutover swaps can later be driven in
// deployment order by the cutover-claim path. The two policies differ only in
// the copy-start gate, not in how cutover is sequenced, so both park here. A
// single-operation apply has no siblings to order, so it never auto-defers even
// when its stored cutover_policy is ordered: behaviour is unchanged until
// multi-deployment fan-out lands.
func shouldAutoDeferCutover(multiOperation bool, op *storage.ApplyOperation) bool {
	return multiOperation && op != nil && storage.IsOrderedCutoverPolicy(op.CutoverPolicy)
}

// shouldReleaseAtCutoverBarrier reports whether an operation-scoped copy drive
// should park at the barrier *and release its claim* for the deployment-ordered
// cutover claim (OC-3). This is the automatic barrier decision only: when the
// apply was started with manual --defer-cutover, the documented manual contract
// wins — the operator holds the claim and polls for a manual cutover (subject to
// the inaction timeout) — so we must not release. effectiveCopyDriveOptions
// still keeps DeferCutover on either way, so the cutover is deferred regardless.
func shouldReleaseAtCutoverBarrier(apply *storage.Apply, multiOperation bool, op *storage.ApplyOperation) bool {
	return !apply.GetOptions().DeferCutover && shouldAutoDeferCutover(multiOperation, op)
}

// effectiveCopyDriveOptions returns the apply options that govern a copy-phase
// drive. It starts from the apply's stored options and turns on DeferCutover
// when the operation must park at the barrier (see shouldAutoDeferCutover). The
// manual per-apply --defer-cutover option stays authoritative — it is OR'd in,
// never cleared. The returned value is execution-time only and must never be
// persisted back onto the apply: the automatic decision is per operation, while
// apply.Options is shared by every deployment of the apply.
func effectiveCopyDriveOptions(apply *storage.Apply, multiOperation bool, op *storage.ApplyOperation) storage.ApplyOptions {
	opts := apply.GetOptions()
	if !opts.DeferCutover && shouldAutoDeferCutover(multiOperation, op) {
		opts.DeferCutover = true
	}
	return opts
}

// cutoverRequestTurn is an operation-scoped drive's answer to whether it may
// take the apply's pending cutover request now. The request is apply-level, so
// under an ordered cutover policy it belongs to the member whose turn it is.
// When the drive must leave it pending, reason says why and blocker, when set,
// is the operation that holds it: an earlier sibling holding this member's
// cutover, or the operation the request is bound to. settle is set instead when
// the operation the request is bound to has ended, so the drive settles the
// request rather than taking it.
type cutoverRequestTurn struct {
	ready   bool
	reason  string
	blocker *storage.ApplyOperation
	settle  cutoverRequestSettlement
}

// cutoverRequestSettlement is how a drive settles a cutover request bound to
// an operation that has ended.
type cutoverRequestSettlement int

const (
	// cutoverRequestUnsettled leaves the request to the turn decision.
	cutoverRequestUnsettled cutoverRequestSettlement = iota
	// cutoverRequestLanded completes the request: the operation it was bound
	// to completed, which it can only do by cutting over.
	cutoverRequestLanded
	// cutoverRequestEnded fails the request: the operation it was bound to
	// ended without completing, so the command had no effect.
	cutoverRequestEnded
)

// takesCutoverRequestInOrder reports whether this drive may take a pending
// cutover request only at its own operation's turn. It holds exactly where the
// automatic cutover claim orders the swaps — an operation of a multi-operation
// apply under an ordered cutover policy — so rolling rollouts and
// single-operation applies keep taking the request as before.
func (s applyTaskScope) takesCutoverRequestInOrder() bool {
	return s.isOperationScoped() && s.multiOperation && s.operation != nil &&
		storage.IsOrderedCutoverPolicy(s.operation.CutoverPolicy)
}

// operationCutoverRequestTurn decides whether this drive's own operation may
// take the apply's cutover request. A request bound to another operation (see
// storage.CutoverRequestMetadata) is that operation's alone: it is left for
// that operation while it can still cut over, and settled once it has ended,
// so one command never cuts over a second member. Otherwise this operation's
// own tasks must be parked at the cutover (a sibling being parked does not make
// this member ready), and no earlier sibling may still hold its turn (storage
// CutoverBlocker, the same rule the automatic cutover claim follows).
func operationCutoverRequestTurn(ctx context.Context, store storage.Storage, apply *storage.Apply, scope applyTaskScope, controlReq *storage.ApplyControlRequest) (cutoverRequestTurn, error) {
	if store == nil {
		return cutoverRequestTurn{}, fmt.Errorf("storage is not available")
	}
	opStore := store.ApplyOperations()
	if opStore == nil {
		return cutoverRequestTurn{}, fmt.Errorf("apply operation store is not available")
	}
	boundID, err := controlReq.CutoverOperationID()
	if err != nil {
		return cutoverRequestTurn{}, fmt.Errorf("read the operation the cutover request of apply %s is bound to: %w", apply.ApplyIdentifier, err)
	}
	if boundID != 0 && boundID != scope.applyOperationID {
		bound, err := opStore.Get(ctx, boundID)
		if err != nil {
			return cutoverRequestTurn{}, fmt.Errorf("load apply_operation %d the cutover request of apply %s is bound to: %w", boundID, apply.ApplyIdentifier, err)
		}
		if bound == nil {
			return cutoverRequestTurn{}, fmt.Errorf("cutover request of apply %s is bound to apply_operation %d, which does not exist", apply.ApplyIdentifier, boundID)
		}
		switch {
		case state.IsState(bound.State, state.ApplyOperation.Completed):
			return cutoverRequestTurn{reason: "the member the request was bound to has cut over", blocker: bound, settle: cutoverRequestLanded}, nil
		case state.IsApplyOperationTerminal(bound.State):
			return cutoverRequestTurn{reason: "the member the request was bound to ended without cutting over", blocker: bound, settle: cutoverRequestEnded}, nil
		default:
			return cutoverRequestTurn{reason: "the request is bound to another member", blocker: bound}, nil
		}
	}
	taskStore := store.Tasks()
	if taskStore == nil {
		return cutoverRequestTurn{}, fmt.Errorf("task store is not available")
	}
	tasks, err := taskStore.GetByApplyOperationID(ctx, scope.applyOperationID)
	if err != nil {
		return cutoverRequestTurn{}, fmt.Errorf("load tasks for apply_operation %d of apply %s before cutover request: %w", scope.applyOperationID, apply.ApplyIdentifier, err)
	}
	if !tasksParkedAtCutover(tasks) {
		return cutoverRequestTurn{reason: "this member has not reached cutover"}, nil
	}
	blocker, err := opStore.CutoverBlocker(ctx, scope.applyOperationID)
	if err != nil {
		return cutoverRequestTurn{}, fmt.Errorf("check cutover order for apply_operation %d of apply %s: %w", scope.applyOperationID, apply.ApplyIdentifier, err)
	}
	if blocker != nil {
		return cutoverRequestTurn{reason: "an earlier member has not completed its cutover", blocker: blocker}, nil
	}
	return cutoverRequestTurn{ready: true}, nil
}

// tasksParkedAtCutover reports whether any of an operation's tasks is parked
// at, or already in, its cutover.
func tasksParkedAtCutover(tasks []*storage.Task) bool {
	for _, task := range tasks {
		if state.IsState(task.State, state.Task.WaitingForCutover, state.Task.CuttingOver) {
			return true
		}
	}
	return false
}
