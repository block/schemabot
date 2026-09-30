package state

import "strings"

// OperationKeyDelimiter separates the components of an operation key.
// storage.OperationKeyDelimiter, which builds the keys, is defined as this
// constant; the rollout projection reads the leading component back as a
// finalizer's scope.
const OperationKeyDelimiter = "/"

// RolloutOperation is one operation as the rollout projection reads it: the
// facts about the row that decide how its state counts toward the parent
// apply's, beyond the state itself. Every surface that projects a rollout
// (the operator's stored derivation, the PR comment and CLI presentation, and
// the local grouped drive) maps its rows into this shape and builds its
// children through RolloutChildren, so they cannot disagree about which rows
// hold the rollout open.
//
// The zero value of each flag is the conservative reading: a row that is
// neither Work nor Finalizer orphans nothing and is never an orphan, and a
// row not marked NeverStarted is treated as work a driver may have begun.
type RolloutOperation struct {
	// Deployment is the operation's deployment. A finalizer and the work it
	// finalizes share it.
	Deployment string
	// OperationKey is the operation's key. Its leading component is the scope
	// a finalizer shares with the work it finalizes (see OperationScope).
	OperationKey string
	// Work is true for a work operation.
	Work bool
	// Finalizer is true for a group_finalizer.
	Finalizer bool
	// State is the operation's state.
	State string
	// NeverStarted is true when no driver has ever claimed the row: it has no
	// start time. It matters only for a stopped row, which a stop moved there
	// from pending.
	NeverStarted bool
	// ContinueOnFailure and PauseOnFailure carry RolloutChild's flags of the
	// same names, which the caller resolves from on_failure and the release
	// latch.
	ContinueOnFailure bool
	PauseOnFailure    bool
}

// RolloutChildren builds the projection's children from ops, in the same
// order. It is the one place the rollout rules below are computed, so every
// caller of DeriveRolloutApplyState marks the same rows.
//
//   - Orphaned: a finalizer that has not started while work it finalizes has
//     terminally failed. The claim query's orphanedFinalizerSQL
//     (pkg/storage/internal/sqlstore/apply_operations.go) is the same
//     predicate.
//   - NeverStarted: a stopped row no driver ever claimed. The claim query
//     holds it to the same start gate as a pending row, so the projection
//     counts it as a pending row too.
func RolloutChildren(ops []RolloutOperation) []RolloutChild {
	children := make([]RolloutChild, len(ops))
	for i, op := range ops {
		children[i] = RolloutChild{
			State:             op.State,
			ContinueOnFailure: op.ContinueOnFailure,
			PauseOnFailure:    op.PauseOnFailure,
			Orphaned:          finalizerOrphanedByFailedWork(op, ops),
			NeverStarted:      op.NeverStarted && IsState(op.State, ApplyOperation.Pending, ApplyOperation.Stopped),
		}
	}
	return children
}

// finalizerOrphanedByFailedWork reports whether op is a group_finalizer that
// nothing will ever start: it has not started (pending, or stopped before it
// started) and work it finalizes has terminally failed. A finalizer starts
// only once that work completes, and a failed operation never runs again, so
// the row is dead rather than queued. A finalizer matches its work by
// deployment and by the leading component of the operation key.
func finalizerOrphanedByFailedWork(op RolloutOperation, ops []RolloutOperation) bool {
	if !op.Finalizer {
		return false
	}
	if !IsState(op.State, ApplyOperation.Pending, ApplyOperation.Stopped) {
		return false
	}
	scope := OperationScope(op.OperationKey)
	for _, work := range ops {
		if !work.Work || work.Deployment != op.Deployment {
			continue
		}
		if OperationScope(work.OperationKey) == scope && IsState(work.State, ApplyOperation.Failed) {
			return true
		}
	}
	return false
}

// OperationScope returns the scope a group_finalizer shares with the work it
// finalizes: the operation key's leading component, or the whole key when it
// has no delimiter.
func OperationScope(operationKey string) string {
	scope, _, _ := strings.Cut(operationKey, OperationKeyDelimiter)
	return scope
}
