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

	// ControlRequests returns the apply control request store.
	ControlRequests() ControlRequestStore

	// ApplyComments returns the apply comment store.
	ApplyComments() ApplyCommentStore

	// ApplyOperations returns the apply-operations store.
	ApplyOperations() ApplyOperationStore

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

// CheckStore manages SchemaBot's stored check state.
// Per-database rows track internal status for a PR/environment/database.
// Aggregate rows store the GitHub check_run_id for the visible GitHub Check Run.
type CheckStore interface {
	// Upsert creates or updates stored check state.
	Upsert(ctx context.Context, check *Check) error

	// UpsertPlanResult creates or updates stored check state from a plan result,
	// but preserves in-progress apply state for the same PR/environment/database/head SHA.
	UpsertPlanResult(ctx context.Context, check *Check) error

	// CompleteForApply updates stored check state to a terminal state only if
	// it still belongs to the given apply and no newer apply exists for the
	// same PR/environment/database. Returns false when another worker changed
	// the stored state first.
	CompleteForApply(ctx context.Context, check *Check, apply *Apply) (bool, error)

	// MarkActionRequiredForApply marks stored check state action_required after
	// a rollback only if it still belongs to that rollback apply and no newer
	// apply exists for the same PR/environment/database. Returns false when
	// another worker changed the stored state first.
	MarkActionRequiredForApply(ctx context.Context, check *Check, apply *Apply) (bool, error)

	// Get returns stored check state by its unique key (PR + env + database), or nil if not found.
	Get(ctx context.Context, repo string, pr int, environment, dbType, database string) (*Check, error)

	// GetByCheckRunID returns stored check state by GitHub's check run ID, or nil if not found.
	// Used for handling check_run webhooks from GitHub.
	GetByCheckRunID(ctx context.Context, checkRunID int64) (*Check, error)

	// GetByPR returns all stored check state for a PR (for PR cleanup on close).
	GetByPR(ctx context.Context, repo string, pr int) ([]*Check, error)

	// GetByDatabase returns all stored check state for a database across all PRs.
	// Used for cross-PR coordination (blocking other PRs when one is applying).
	GetByDatabase(ctx context.Context, repo, environment, dbType, database string) ([]*Check, error)

	// Delete removes stored check state by ID.
	Delete(ctx context.Context, id int64) error

	// DeleteByPR removes all stored check state for a PR.
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
	// Returns ErrActiveApplyExists if another active apply already exists for
	// the same database, database type, and environment.
	Create(ctx context.Context, apply *Apply) (int64, error)

	// CreateWithTasks stores a new apply and its initial tasks in one
	// transaction. Pending applies become scheduler-claimable only after the
	// task rows are committed.
	CreateWithTasks(ctx context.Context, apply *Apply, tasks []*Task) (int64, error)

	// CreateWithTasksAndOperations stores a new apply, its initial tasks, and
	// its per-deployment apply_operations rows in a single transaction. Each
	// operation row's ApplyID is set to the new apply ID before insert.
	// Pending applies become scheduler-claimable only after every row is
	// committed, so the operator never observes a partially-populated apply.
	CreateWithTasksAndOperations(ctx context.Context, apply *Apply, tasks []*Task, operations []*ApplyOperation) (int64, error)

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
	// Returns ErrActiveApplyExists when moving an apply into an active state
	// would overlap another active apply for the same database, database type,
	// and environment.
	Update(ctx context.Context, apply *Apply) error

	// GetRecent returns the most recent applies across all databases, ordered by creation time desc.
	// Used by `schemabot status` (no args) to show recent activity.
	GetRecent(ctx context.Context, filter RecentAppliesFilter) ([]*Apply, error)

	// GetInProgress returns all applies in non-terminal states.
	// Note: For recovery, use FindNextApply which handles locking.
	GetInProgress(ctx context.Context) ([]*Apply, error)

	// FindNextApply atomically claims the next apply that needs attention.
	// A claim selects one apply that needs work and refreshes its heartbeat in
	// the same transaction. That heartbeat is the scheduler's lease while it
	// reloads state and starts or resumes the apply.
	// Returns the claimed apply, or nil if nothing needs work.
	FindNextApply(ctx context.Context) (*Apply, error)

	// Heartbeat updates the apply's updated_at timestamp to maintain the lease.
	// Should be called every 10 seconds while working on an apply.
	// If not called for > 1 minute, another worker can claim the apply.
	Heartbeat(ctx context.Context, applyID int64) error

	// ExpireRetryable transitions failed_retryable applies that exhausted their
	// retry budget or recovery freshness window to permanent failed. Returns the
	// applies updated.
	ExpireRetryable(ctx context.Context) ([]*RetryableApplyExpiration, error)

	// FindMissingSummaryComment returns GitHub-backed applies that should have
	// a terminal summary comment but only have a progress comment. Used by
	// startup reconciliation to post missing summary comments after restarts.
	FindMissingSummaryComment(ctx context.Context) ([]*Apply, error)

	// GetByPR returns all applies for a PR.
	GetByPR(ctx context.Context, repo string, pr int) ([]*Apply, error)

	// Delete removes an apply by ID.
	Delete(ctx context.Context, id int64) error

	// DeleteByPR removes all applies for a PR (cleanup on PR close/merge).
	DeleteByPR(ctx context.Context, repo string, pr int) error
}

// RecentAppliesFilter controls recent apply queries for status views.
type RecentAppliesFilter struct {
	Limit       int
	Environment string
	States      []string
}

// RetryableExpirationReason identifies why scheduler retry recovery stopped.
type RetryableExpirationReason string

const (
	RetryableExpirationAttemptBudget  RetryableExpirationReason = "retry_budget_exhausted"
	RetryableExpirationRecoveryWindow RetryableExpirationReason = "recovery_window_expired"
)

// RetryableApplyExpiration is a failed_retryable apply that was made permanent
// because scheduler recovery should no longer retry it automatically.
type RetryableApplyExpiration struct {
	Apply  *Apply
	Reason RetryableExpirationReason
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

	// IncrementEditCount atomically increments the edit count and updates
	// last_edited_at for a comment. Called after each successful edit.
	IncrementEditCount(ctx context.Context, applyID int64, commentState string) error

	// DeleteByApply removes all comment records for an apply.
	DeleteByApply(ctx context.Context, applyID int64) error
}

// ApplyOperationStore manages per-(apply, deployment) child rows for
// multi-deployment applies. One apply owns 1..N apply_operations rows.
//
// Pure storage primitive: no orchestration code reads or writes these rows
// yet — the apply-create dual-write and the operator's per-row claim + lock
// relocation arrive in subsequent PRs in the multi-deployment apply
// workstream.
type ApplyOperationStore interface {
	// Insert stores a new apply_operations row and returns its ID.
	// Fails with a uniqueness error if (apply_id, deployment) already exists.
	Insert(ctx context.Context, ad *ApplyOperation) (int64, error)

	// Get returns a child row by ID, or nil if not found.
	Get(ctx context.Context, id int64) (*ApplyOperation, error)

	// GetByApplyAndDeployment returns the child row for (apply_id, deployment),
	// or nil if not found.
	GetByApplyAndDeployment(ctx context.Context, applyID int64, deployment string) (*ApplyOperation, error)

	// ListByApply returns all child rows for an apply, ordered by id ascending.
	ListByApply(ctx context.Context, applyID int64) ([]*ApplyOperation, error)

	// UpdateState transitions a child row to a new state. Updates the state
	// column only; for transitions that should also stamp started_at or
	// completed_at, use MarkStarted / MarkCompleted / MarkFailed instead.
	UpdateState(ctx context.Context, id int64, newState string) error

	// MarkStarted sets state=running and started_at on a child row.
	MarkStarted(ctx context.Context, id int64) error

	// MarkCompleted sets state=completed and completed_at on a child row.
	MarkCompleted(ctx context.Context, id int64) error

	// MarkFailed sets state=failed, error_message, and completed_at on a child row.
	MarkFailed(ctx context.Context, id int64, errMsg string) error

	// FindNextApplyOperation atomically claims the next child row that needs
	// attention and refreshes its heartbeat in the same transaction.
	//
	// Pending rows are transitioned to running and stamped with started_at;
	// already-active rows whose heartbeat has been stale for more than one
	// minute are re-leased without changing their state. Terminal rows
	// (completed/failed/cancelled/stopped/reverted) are never claimed.
	//
	// Returns the claimed row, or nil if nothing needs work.
	//
	// Pure storage primitive: no scheduler/operator loop calls this yet —
	// the per-deployment claim loop arrives in a subsequent PR in the
	// multi-deployment apply workstream.
	FindNextApplyOperation(ctx context.Context) (*ApplyOperation, error)

	// Heartbeat refreshes the child row's updated_at timestamp to extend the
	// claim's lease while a worker is acting on it. Mirrors ApplyStore.Heartbeat
	// semantics: silent no-op when the row no longer exists.
	Heartbeat(ctx context.Context, id int64) error

	// DeleteByApply removes all child rows for an apply (cleanup on apply delete).
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

// ControlRequestStore manages durable user control requests.
// A control request is behavioral state, not just audit: scheduler workers use
// pending rows to recover accepted operations after process restarts.
type ControlRequestStore interface {
	// RequestPending records a pending request for an apply operation. If the
	// same operation is already pending for the apply, the existing request is
	// returned with alreadyPending=true.
	RequestPending(ctx context.Context, req *ApplyControlRequest) (*ApplyControlRequest, bool, error)

	// GetPending returns the pending request for an apply operation.
	GetPending(ctx context.Context, applyID int64, operation ControlOperation) (*ApplyControlRequest, error)

	// CompletePending marks the pending request for an apply operation completed.
	CompletePending(ctx context.Context, applyID int64, operation ControlOperation) error

	// FailPending marks the pending request for an apply operation failed with an
	// operator-visible reason.
	FailPending(ctx context.Context, applyID int64, operation ControlOperation, errorMessage string) error
}
