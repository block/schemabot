package storage

import "errors"

// Common storage errors.
var (
	// ErrLockHeld is returned when attempting to acquire a lock that is already held.
	ErrLockHeld = errors.New("lock is already held")

	// ErrLockNotFound is returned when a lock does not exist.
	ErrLockNotFound = errors.New("lock not found")

	// ErrLockNotOwned is returned when attempting to release a lock not owned by caller.
	ErrLockNotOwned = errors.New("lock not owned by caller")

	// ErrCheckNotFound is returned when a check does not exist.
	ErrCheckNotFound = errors.New("check not found")

	// ErrSettingNotFound is returned when a setting does not exist.
	ErrSettingNotFound = errors.New("setting not found")

	// ErrApplyNotFound is returned when an apply does not exist.
	ErrApplyNotFound = errors.New("apply not found")

	// ErrApplyIDExists is returned when an apply_id already exists.
	ErrApplyIDExists = errors.New("apply already exists")

	// ErrPlanNotFound is returned when a plan does not exist.
	ErrPlanNotFound = errors.New("plan not found")

	// ErrPlanIDExists is returned when a plan_identifier already exists.
	ErrPlanIDExists = errors.New("plan already exists")

	// ErrTaskNotFound is returned when a task does not exist.
	ErrTaskNotFound = errors.New("task not found")

	// ErrApplyCommentNotFound is returned when an apply comment does not exist.
	ErrApplyCommentNotFound = errors.New("apply comment not found")
)
