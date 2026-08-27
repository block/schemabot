package storage

import "errors"

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

	// ErrLockIntentChanged is returned when an apply's captured lock owner or
	// pending plan no longer matches at durable apply creation time.
	ErrLockIntentChanged = errors.New("lock intent changed")

	// ErrCheckNotFound is returned when a check does not exist.
	ErrCheckNotFound = errors.New("check not found")

	// ErrSettingNotFound is returned when a setting does not exist.
	ErrSettingNotFound = errors.New("setting not found")

	// ErrApplyNotFound is returned when an apply does not exist.
	ErrApplyNotFound = errors.New("apply not found")

	// ErrApplyIDExists is returned when an apply_id already exists.
	ErrApplyIDExists = errors.New("apply already exists")

	// ErrActiveApplyExists is returned when another active apply already exists
	// for the same database, type, and environment.
	ErrActiveApplyExists = errors.New("active apply already exists")

	// ErrApplyNotActive is returned when a write requires the apply to still be
	// active (non-terminal) and it is not — e.g. attaching a new operation to
	// an apply no drive will pick up again.
	ErrApplyNotActive = errors.New("apply is not active")

	// ErrApplyLeaseLost is returned when an operator-owned write no longer
	// matches the apply lease token stored by the latest operator claimant.
	ErrApplyLeaseLost = errors.New("apply lease lost")

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

	// ErrEngineResumeStateNotFound is returned when no opaque engine resume state exists for an operation.
	ErrEngineResumeStateNotFound = errors.New("engine resume state not found")

	// ErrWebhookEventNotFound is returned when a durable webhook event does not exist.
	ErrWebhookEventNotFound = errors.New("webhook event not found")

	// ErrWebhookEventLeaseLost is returned when a driver no longer owns a durable webhook event.
	ErrWebhookEventLeaseLost = errors.New("webhook event lease lost")
)
