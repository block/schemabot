package state

import "strings"

// OperationKeyDelimiter separates the components of an operation key.
// storage.OperationKeyDelimiter, which builds the keys, is defined as this
// constant; the rollout projection reads a finalizer's scope back out of the
// key (see FinalizerFinalizesWork).
const OperationKeyDelimiter = "/"

// GroupFinalizerKeySegment is the trailing component of a group_finalizer's
// operation key. Everything in front of it is the finalizer's scope: the
// namespace ("ns_0/group_finalizer"), or the target and namespace when a
// targets list qualifies the keys ("orders-001/ns_0/group_finalizer").
const GroupFinalizerKeySegment = "group_finalizer"

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
	// OperationKey is the operation's key. A finalizer's key names the scope
	// of the work it finalizes (see FinalizerFinalizesWork).
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
// deployment and by FinalizerFinalizesWork.
func finalizerOrphanedByFailedWork(op RolloutOperation, ops []RolloutOperation) bool {
	if !op.Finalizer {
		return false
	}
	if !IsState(op.State, ApplyOperation.Pending, ApplyOperation.Stopped) {
		return false
	}
	for _, work := range ops {
		if !work.Work || work.Deployment != op.Deployment {
			continue
		}
		if FinalizerFinalizesWork(op.OperationKey, work.OperationKey) && IsState(work.State, ApplyOperation.Failed) {
			return true
		}
	}
	return false
}

// FinalizerFinalizesWork reports whether the group_finalizer keyed
// finalizerKey finalizes the work keyed workKey. The finalizer's scope is its
// key without the trailing GroupFinalizerKeySegment, and its work is the scope
// itself and every key under it. That is one namespace of one target in either
// key shape:
//
//	"ns_0/group_finalizer"            finalizes "ns_0/-80/orders"
//	"orders-001/ns_0/group_finalizer" finalizes "orders-001/ns_0/-80/orders",
//	                                  not "orders-001/ns_1/-80/orders"
//
// A finalizer key with nothing in front of the segment (the deployment-scoped
// finalizer of a single-target plan whose only changes are finalizers) has no
// work alongside it and finalizes none. Keys compare byte-for-byte, because
// namespaces and tables are case-significant: "orders/group_finalizer" does
// not finalize "Orders/-80/orders". A finalizer and its work also share a
// deployment, which callers match separately. finalizerFinalizesWorkSQL
// (pkg/storage/internal/sqlstore/apply_operations.go) is the same rule for
// the claim query.
func FinalizerFinalizesWork(finalizerKey, workKey string) bool {
	scope, ok := FinalizerScope(finalizerKey)
	if !ok {
		return false
	}
	return workKey == scope || strings.HasPrefix(workKey, scope+OperationKeyDelimiter)
}

// FinalizerScope returns a group_finalizer key without its trailing
// GroupFinalizerKeySegment: the namespace, or the target and namespace, whose
// work the finalizer finalizes. ok is false for a key that does not end in the
// segment after a non-empty scope, which includes the bare deployment-scoped
// key.
func FinalizerScope(finalizerKey string) (scope string, ok bool) {
	scope, ok = strings.CutSuffix(finalizerKey, OperationKeyDelimiter+GroupFinalizerKeySegment)
	if !ok || scope == "" {
		return "", false
	}
	return scope, true
}
