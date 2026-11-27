package storage

import (
	"errors"
	"fmt"
)

// Common storage errors.
var (
	// ErrNotImplemented is returned by interface methods that no
	// implementation supports yet. Callers must treat it as a hard error,
	// never as an empty result.
	ErrNotImplemented = errors.New("not implemented")

	// ErrLockHeld is returned when attempting to acquire a lock that is already held.
	ErrLockHeld = errors.New("lock is already held")

	// ErrLockNotFound is returned when a lock does not exist.
	ErrLockNotFound = errors.New("lock not found")

	// ErrLockNotOwned is returned when attempting to release a lock not owned by caller.
	ErrLockNotOwned = errors.New("lock not owned by caller")

	// ErrLockReplaced is returned when a release pinned to one lock row finds
	// the lock key held by a different row: the lock the caller checked was
	// released and a new one acquired in its place.
	ErrLockReplaced = errors.New("lock was replaced by a new lock")

	// ErrLockIntentChanged is returned when the lock's pending plan is no longer
	// the one a caller observed: an apply's captured lock owner or pending plan
	// no longer matches at durable apply creation time, a conditional acquire
	// finds the same owner's lock pinned to another intent or released since a
	// pin was observed, or a row-pinned release finds the row re-acquired for
	// another plan.
	ErrLockIntentChanged = errors.New("lock intent changed")

	// ErrLockInUse is returned when a release refuses to delete a lock row
	// because an apply that has not reached a terminal state records that row
	// as its lock. The lock stays held. A *LockInUseError carries the apply.
	ErrLockInUse = errors.New("lock is in use by an apply")

	// ErrCheckNotFound is returned when a check does not exist.
	ErrCheckNotFound = errors.New("check not found")

	// ErrSettingNotFound is returned when a setting does not exist.
	ErrSettingNotFound = errors.New("setting not found")

	// ErrFreezeNotFound is returned when a freeze does not exist.
	ErrFreezeNotFound = errors.New("freeze not found")

	// ErrFreezeExists is returned when a freeze's identifier, or its source
	// and external id, is already taken.
	ErrFreezeExists = errors.New("freeze already exists")

	// ErrFreezeAlreadyLifted is returned when lifting a freeze someone else
	// already lifted.
	ErrFreezeAlreadyLifted = errors.New("freeze already lifted")

	// ErrFreezeOverrideExists is returned when an override request with the
	// same identifier already exists.
	ErrFreezeOverrideExists = errors.New("freeze override request already exists")

	// ErrFreezeOverrideNotFound is returned when an override request does
	// not exist.
	ErrFreezeOverrideNotFound = errors.New("freeze override request not found")

	// ErrFreezeOverrideAlreadyApproved is returned when approving a request
	// someone already approved.
	ErrFreezeOverrideAlreadyApproved = errors.New("freeze override request already approved")

	// ErrFreezeOverrideExpired is returned when approving a request past its
	// expiry.
	ErrFreezeOverrideExpired = errors.New("freeze override request expired")

	// ErrFreezeOverrideSelfApproval is returned when the requester or the
	// pull request's author tries to approve a request.
	ErrFreezeOverrideSelfApproval = errors.New("freeze override request needs a second person")

	// ErrFreezeOverrideLifetime is returned when creating a request that
	// expires more than FreezeOverrideMaxLifetime after the database clock.
	ErrFreezeOverrideLifetime = errors.New("freeze override request expires too late")

	// ErrFreezeEnded is returned when lifting a freeze that already reached
	// its end time.
	ErrFreezeEnded = errors.New("freeze already ended")

	// ErrFreezeEpochChanged is returned by an admission write when a freeze
	// was created in its environment after the change gate checked. The
	// change was not recorded; checking the freezes again decides it.
	ErrFreezeEpochChanged = errors.New("a freeze was created after the freeze check")

	// ErrFreezeEpochUnreadable is returned by an admission write when it
	// could not read the freeze epoch the change gate checked. The change
	// was not recorded. The error also wraps the cause.
	ErrFreezeEpochUnreadable = errors.New("the freeze epoch could not be read")

	// ErrApplyNotFound is returned when an apply does not exist.
	ErrApplyNotFound = errors.New("apply not found")

	// ErrApplyIDExists is returned when an apply_id already exists.
	ErrApplyIDExists = errors.New("apply already exists")

	// ErrActiveApplyExists is returned when another active apply already exists
	// for the same database, type, and environment.
	ErrActiveApplyExists = errors.New("active apply already exists")

	// ErrApplyTakenOver is returned when an operation would attach to an
	// apply whose work a newer apply took over (Apply.SupersededBy). The older
	// apply can only settle over what it already attached.
	ErrApplyTakenOver = errors.New("a newer apply took over the work of this apply")

	// ErrApplyNotActive is returned when a write requires the apply to still be
	// active (non-terminal) and it is not — e.g. attaching a new operation to
	// an apply no drive will pick up again.
	ErrApplyNotActive = errors.New("apply is not active")

	// ErrApplyReopenRefused is returned when a general apply update would
	// reopen a finished apply: an active state written over a terminal row, or
	// stopped written over a settled one. The guard covers Update alone: the
	// claim transitions and the rollout projection (UpdateDerivedState) move
	// the state through their own conditional writes and never return it.
	ErrApplyReopenRefused = errors.New("refused to reopen a finished apply")

	// ErrApplyOutcomeSettled is returned when a general apply update would
	// replace a settled outcome (completed, failed, cancelled, reverted) with a
	// different one. The settled row records what happened on the database, so
	// a later writer, such as a cancel that arrives after the apply completed,
	// learns the outcome already landed instead of overwriting it.
	ErrApplyOutcomeSettled = errors.New("apply outcome already settled")

	// ErrValueRejected is returned when storage refuses a write because a value
	// does not fit its column. The same write is refused on every attempt, so
	// it is not a transient failure to retry.
	ErrValueRejected = errors.New("storage rejected a value its column cannot hold")

	// ErrApplyLeaseLost is returned when an operator-owned write no longer
	// matches the apply lease token stored by the latest operator claimant.
	ErrApplyLeaseLost = errors.New("apply lease lost")

	// ErrOperationClaimContended is returned by an operation claim that asked
	// to report contention (ReportClaimContention) when it took nothing, but a
	// lock-free reread of the same eligibility found an operation it could
	// take. Usually another transaction held that row, since the claim skips
	// locked rows rather than wait on them. The reread is a fresh snapshot, so
	// it can also find a row that became claimable after the claim ran. Either
	// way this is not a failure: the caller retries shortly.
	ErrOperationClaimContended = errors.New("the claim took no apply_operation but a lock-free reread found one claimable")

	// ErrOperationLeaseActive is returned when a repair write requires an
	// operation to remain unleased but a live drive owns it.
	ErrOperationLeaseActive = errors.New("operation lease is active")

	// ErrTaskOutsideOperationLease is returned when a task write under an
	// operation lease names a task of another operation, or of none. The lease
	// authorizes writes to its own operation's tasks only.
	ErrTaskOutsideOperationLease = errors.New("task is outside the held operation lease")

	// ErrApplyAlreadySuperseded is returned when a handoff would reassign an
	// apply's superseded_by marker to a different successor. The marker is
	// write-once, so a second claimant means the takeover is ambiguous.
	ErrApplyAlreadySuperseded = errors.New("apply already superseded by another apply")

	// ErrPlanNotFound is returned when a plan does not exist.
	ErrPlanNotFound = errors.New("plan not found")

	// ErrPlanIDExists is returned when a plan_identifier already exists.
	ErrPlanIDExists = errors.New("plan already exists")

	// ErrTaskNotFound is returned when a task does not exist.
	ErrTaskNotFound = errors.New("task not found")

	// ErrApplyCommentNotFound is returned when an apply comment does not exist.
	ErrApplyCommentNotFound = errors.New("apply comment not found")

	// ErrApplyOperationNotFound is returned when an apply_operations child
	// row does not exist for the given lookup key.
	ErrApplyOperationNotFound = errors.New("apply operation not found")

	// ErrRemoteApplyDeploymentIDConflict is returned when storing a remote
	// apply id would correlate one deployment to more than one remote
	// data-plane apply — either the deployment's operations already disagree
	// with each other, or the id being stored disagrees with the one they
	// share. Callers must fail closed rather than pick one.
	ErrRemoteApplyDeploymentIDConflict = errors.New("deployment already correlates to a different remote apply")

	// ErrApplyOperationExists is returned when an apply_operations row for
	// (apply_id, deployment, operation_key) is being inserted but already exists.
	ErrApplyOperationExists = errors.New("apply operation already exists")

	// ErrApplyOperationKeyingMismatch is returned when an attached operation is
	// keyed by its target (see ApplyOperation.KeyedByTarget) and the
	// deployment's existing operations of the apply are not, or the reverse.
	// Mixing the two shapes would let one target's work attach twice under two
	// keys, so the attach is refused.
	ErrApplyOperationKeyingMismatch = errors.New("apply operation keying does not match the deployment's existing operations")

	// ErrEngineResumeStateNotFound is returned when no opaque engine resume state exists for an operation.
	ErrEngineResumeStateNotFound = errors.New("engine resume state not found")

	// ErrWebhookEventNotFound is returned when a durable webhook event does not exist.
	ErrWebhookEventNotFound = errors.New("webhook event not found")

	// ErrWebhookEventLeaseLost is returned when a driver no longer owns a durable webhook event.
	ErrWebhookEventLeaseLost = errors.New("webhook event lease lost")
)

// ActiveApplyError reports the apply that already holds a target another apply
// asked for. It matches ErrActiveApplyExists, and carries the holder's
// identifier and state so a caller can tell the operator what to do next
// without presenting the error text.
type ActiveApplyError struct {
	// HolderApplyID is the identifier of the apply holding the target.
	HolderApplyID string
	// HolderState is the holding apply's own state. It can be terminal when
	// the apply has ended but one of its operations is still in progress,
	// which keeps the rollout's targets reserved.
	HolderState string
	Database    string
	Environment string
	// Detail is the store's full account of the conflict, for the log line.
	Detail string
}

func (e *ActiveApplyError) Error() string {
	return e.Detail + ": " + ErrActiveApplyExists.Error()
}

func (e *ActiveApplyError) Unwrap() error {
	return ErrActiveApplyExists
}

// ApplyNotActiveError reports an apply that a write needed to be active but
// that the store found terminal. It matches ErrApplyNotActive, and carries the
// state the store read under the apply's row lock, which a caller's copy of the
// apply can lag: the apply may have settled after the caller read it.
type ApplyNotActiveError struct {
	ApplyID string
	// State is the apply's state as the store read it.
	State string
	// Detail is the store's full account of the refused write, for the log line.
	Detail string
}

func (e *ApplyNotActiveError) Error() string {
	return e.Detail + ": " + ErrApplyNotActive.Error()
}

func (e *ApplyNotActiveError) Unwrap() error {
	return ErrApplyNotActive
}

// LockInUseError reports the apply that kept a lock row from being released.
// It matches ErrLockInUse, and carries the apply so a caller can log it with
// Apply.LogAttrs and tell the operator which apply to wait for.
type LockInUseError struct {
	// LockID is the lock row the release was pinned to.
	LockID int64
	// Apply is the newest apply that records LockID as its lock and has not
	// reached a terminal state.
	Apply *Apply
}

func (e *LockInUseError) Error() string {
	return fmt.Sprintf("lock row %d is recorded by apply %s in state %s: %s",
		e.LockID, e.Apply.ApplyIdentifier, e.Apply.State, ErrLockInUse.Error())
}

func (e *LockInUseError) Unwrap() error {
	return ErrLockInUse
}
