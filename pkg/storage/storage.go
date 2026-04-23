// Package storage defines the storage interface for SchemaBot.
// Currently implemented by the MySQL backend (pkg/storage/mysqlstore).
package storage

import (
	"context"
)

// Storage provides access to all stores.
type Storage interface {
	// Locks returns the lock store.
	Locks() LockStore

	// Plans returns the plan store.
	Plans() PlanStore

	// Applies returns the apply store.
	Applies() ApplyStore

	// Tasks returns the task store.
	Tasks() TaskStore

	// ApplyLogs returns the apply logs store.
	ApplyLogs() ApplyLogStore

	// ApplyComments returns the apply comment store.
	ApplyComments() ApplyCommentStore

	// Checks returns the check store.
	Checks() CheckStore

	// Settings returns the settings store.
	Settings() SettingsStore

	// VitessApplyData returns the Vitess apply data store.
	VitessApplyData() VitessApplyDataStore

	// Ping verifies the database connection is alive.
	Ping(ctx context.Context) error

	// Close closes all underlying connections.
	Close() error
}

// LockStore manages database-level deployment locks.
// Locks prevent concurrent schema changes to the same database.
// Lock key is database:type (not per-environment) to block concurrent changes
// across environments and PRs.
type LockStore interface {
	// Acquire attempts to acquire a lock. Returns ErrLockHeld if already held by another owner.
	// If the same owner already holds the lock, this is a no-op (idempotent).
	Acquire(ctx context.Context, lock *Lock) error

	// Release releases a lock. Only succeeds if caller is the owner.
	// Returns ErrLockNotOwned if the lock is not owned by the caller.
	Release(ctx context.Context, database, dbType, owner string) error

	// ForceRelease releases a lock regardless of owner (admin override).
	// Used by `schemabot unlock` command and --force flag.
	ForceRelease(ctx context.Context, database, dbType string) error

	// Get returns a lock by database name and type, or nil if not found.
	Get(ctx context.Context, database, dbType string) (*Lock, error)

	// List returns all active locks.
	List(ctx context.Context) ([]*Lock, error)

	// Update updates lock metadata (e.g., updated_at timestamp).
	Update(ctx context.Context, lock *Lock) error

	// GetByPR returns all locks associated with a PR (for cleanup on merge/close).
	GetByPR(ctx context.Context, repo string, pr int) ([]*Lock, error)
}

// CheckStore manages GitHub status check state.
// One check per (PR, environment, database) combination.
// Check name format: "SchemaBot: {env}/{type}/{database}"
type CheckStore interface {
	// Upsert creates or updates a check record.
	Upsert(ctx context.Context, check *Check) error

	// Get returns a check by its unique key (PR + env + database), or nil if not found.
	Get(ctx context.Context, repo string, pr int, environment, dbType, database string) (*Check, error)

	// GetByCheckRunID returns a check by GitHub's check run ID, or nil if not found.
	// Used for handling check_run webhooks from GitHub.
	GetByCheckRunID(ctx context.Context, checkRunID int64) (*Check, error)

	// GetByPR returns all checks for a PR (for PR cleanup on close).
	GetByPR(ctx context.Context, repo string, pr int) ([]*Check, error)

	// GetByDatabase returns all checks for a database across all PRs.
	// Used for cross-PR coordination (blocking other PRs when one is applying).
	GetByDatabase(ctx context.Context, repo, environment, dbType, database string) ([]*Check, error)

	// Delete removes a check record by ID.
	Delete(ctx context.Context, id int64) error

	// DeleteByPR removes all check records for a PR.
	// Used for cleanup when a PR is closed or merged.
	DeleteByPR(ctx context.Context, repo string, pr int) error
}

// SettingsStore manages admin-level SchemaBot settings (global config).
// Examples: feature flags, default options, maintenance mode.
// Repo-level settings may be added later if needed.
type SettingsStore interface {
	// Get returns a setting by key, or nil if not found.
	Get(ctx context.Context, key string) (*Setting, error)

	// Set saves a setting. Creates if not exists, updates if exists.
	Set(ctx context.Context, key string, value string) error

	// List returns all settings.
	List(ctx context.Context) ([]*Setting, error)

	// Delete removes a setting by key.
	Delete(ctx context.Context, key string) error
}

// PlanStore manages schema change plans.
// Plans are created by Plan() and stored for Apply() and staleness detection.
// Both GRPCClient and LocalClient are stateless - SchemaBot owns plan storage.
type PlanStore interface {
	// Create stores a new plan and returns its ID. Returns error if plan_identifier already exists.
	Create(ctx context.Context, plan *Plan) (int64, error)

	// Get returns a plan by plan_identifier (external identifier), or nil if not found.
	Get(ctx context.Context, planIdentifier string) (*Plan, error)

	// GetByID returns a plan by ID, or nil if not found.
	GetByID(ctx context.Context, id int64) (*Plan, error)

	// GetByLock returns plans for a lock (0-2: staging + production).
	GetByLock(ctx context.Context, lockID int64) ([]*Plan, error)

	// GetByPR returns all plans for a PR.
	GetByPR(ctx context.Context, repo string, pr int) ([]*Plan, error)

	// Delete removes a plan by ID.
	Delete(ctx context.Context, id int64) error

	// DeleteByPR removes all plans for a PR (cleanup on PR close/merge).
	DeleteByPR(ctx context.Context, repo string, pr int) error
}

// ApplyStore manages schema change execution state.
// Applies are created when Apply() is called and updated during execution.
type ApplyStore interface {
	// Create stores a new apply and returns its ID.
	Create(ctx context.Context, apply *Apply) (int64, error)

	// Get returns an apply by ID, or nil if not found.
	Get(ctx context.Context, id int64) (*Apply, error)

	// GetByApplyIdentifier returns an apply by apply_identifier, or nil if not found.
	// apply_identifier is the external identifier (e.g., "apply_abc123").
	GetByApplyIdentifier(ctx context.Context, applyIdentifier string) (*Apply, error)

	// GetByPlan returns the apply for a plan_id, or nil if not found.
	GetByPlan(ctx context.Context, planID int64) (*Apply, error)

	// GetByLock returns applies for a lock (0-2: staging + production).
	GetByLock(ctx context.Context, lockID int64) ([]*Apply, error)

	// GetByDatabase returns applies for a specific database and environment.
	// Used for checking active schema changes before starting a new one.
	GetByDatabase(ctx context.Context, database, dbType, environment string) ([]*Apply, error)

	// Update updates apply state and fields.
	Update(ctx context.Context, apply *Apply) error

	// GetRecent returns the most recent applies across all databases, ordered by start time desc.
	// Used by `schemabot status` (no args) to show recent activity.
	GetRecent(ctx context.Context, limit int) ([]*Apply, error)

	// GetInProgress returns all applies in non-terminal states.
	// Note: For recovery, use ClaimForRecovery which handles locking.
	GetInProgress(ctx context.Context) ([]*Apply, error)

	// ClaimForRecovery atomically claims an apply for recovery using heartbeat-based leasing.
	// Uses FOR UPDATE SKIP LOCKED to prevent race conditions between workers.
	// Only applies with stale heartbeats (updated_at > 1 minute ago) are claimed.
	// Returns the claimed apply, or nil if no apply is available to claim.
	// The caller MUST call Heartbeat periodically (every 10 seconds) to maintain the lease.
	ClaimForRecovery(ctx context.Context) (*Apply, error)

	// Heartbeat updates the apply's updated_at timestamp to maintain the lease.
	// Should be called every 10 seconds while working on an apply.
	// If not called for > 1 minute, another worker can claim the apply.
	Heartbeat(ctx context.Context, applyID int64) error

	// GetByPR returns all applies for a PR.
	GetByPR(ctx context.Context, repo string, pr int) ([]*Apply, error)

	// Delete removes an apply by ID.
	Delete(ctx context.Context, id int64) error

	// DeleteByPR removes all applies for a PR (cleanup on PR close/merge).
	DeleteByPR(ctx context.Context, repo string, pr int) error
}

// TaskStore manages schema change tasks (individual DDLs within an apply).
// Each task represents one table operation. For multi-table changes,
// one apply contains multiple tasks.
type TaskStore interface {
	// Create stores a new task and returns its ID.
	Create(ctx context.Context, task *Task) (int64, error)

	// Get returns a task by task_identifier (external identifier), or nil if not found.
	Get(ctx context.Context, taskIdentifier string) (*Task, error)

	// Update updates an existing task.
	// Returns ErrTaskNotFound if the task does not exist.
	Update(ctx context.Context, task *Task) error

	// GetByApplyID returns all tasks for an apply.
	// Used for aggregating task states to derive Apply state.
	GetByApplyID(ctx context.Context, applyID int64) ([]*Task, error)

	// GetByDatabase returns all tasks for a database.
	GetByDatabase(ctx context.Context, database string) ([]*Task, error)

	// GetActive returns all tasks in non-terminal states.
	GetActive(ctx context.Context) ([]*Task, error)

	// GetByPR returns all tasks for a repository and pull request.
	GetByPR(ctx context.Context, repo string, pr int) ([]*Task, error)

	// List returns tasks matching the filter criteria.
	List(ctx context.Context, filter TaskFilter) ([]*Task, error)
}

// ApplyCommentStore tracks GitHub PR comment IDs for apply lifecycle management.
// Enables edit-in-place behavior: comments are updated rather than posted anew.
type ApplyCommentStore interface {
	// Upsert creates or updates a comment record.
	// On conflict (same apply_id + comment_state), updates the github_comment_id.
	Upsert(ctx context.Context, comment *ApplyComment) error

	// Get returns a comment by (apply_id, comment_state), or nil if not found.
	Get(ctx context.Context, applyID int64, commentState string) (*ApplyComment, error)

	// ListByApply returns all comments for an apply, ordered by id ascending.
	ListByApply(ctx context.Context, applyID int64) ([]*ApplyComment, error)

	// DeleteByApply removes all comment records for an apply.
	DeleteByApply(ctx context.Context, applyID int64) error
}

// VitessApplyDataStore manages Vitess-specific apply data (deploy request tracking).
type VitessApplyDataStore interface {
	// Save creates or updates Vitess apply data for an apply.
	Save(ctx context.Context, data *VitessApplyData) error

	// GetByApplyID returns the Vitess apply data for the given apply ID.
	GetByApplyID(ctx context.Context, applyID int64) (*VitessApplyData, error)
}

// ApplyLogStore manages apply log entries for debugging and audit.
// Logs capture state transitions, errors, and events during schema changes.
// Logs are kept forever for audit purposes.
type ApplyLogStore interface {
	// Append adds a new log entry.
	Append(ctx context.Context, log *ApplyLog) error

	// GetByApply returns all logs for an apply, ordered by created_at.
	GetByApply(ctx context.Context, applyID int64) ([]*ApplyLog, error)

	// List returns logs matching the filter criteria, ordered by created_at.
	List(ctx context.Context, filter ApplyLogFilter) ([]*ApplyLog, error)
}
