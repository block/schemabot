// Package storage defines the storage interface for SchemaBot.
// Currently implemented by the MySQL backend (pkg/storage/mysqlstore).
package storage

import (
	"context"
	"time"
)

// ReapplyFailureFreshnessDays bounds deliberate reapply of terminal failed
// applies. Older failures should be handled by creating a fresh apply from a
// fresh plan so stale execution context is not reactivated unexpectedly.
const ReapplyFailureFreshnessDays = 1

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

	// PlanComments returns the plan comment store.
	PlanComments() PlanCommentStore

	// ApplyOperations returns the apply-operations store.
	ApplyOperations() ApplyOperationStore

	// Checks returns the check store.
	Checks() CheckStore

	// Settings returns the settings store.
	Settings() SettingsStore

	// WebhookEvents returns the durable webhook event inbox store.
	WebhookEvents() WebhookEventStore

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

	// ReleaseIfPendingPlanID releases a lock only while both its owner and
	// pending plan still match. A mismatch is a no-op so a superseding apply or
	// rollback intent owned by the same PR remains intact.
	ReleaseIfPendingPlanID(ctx context.Context, database, dbType, owner, pendingPlanID string) (bool, error)

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

	// UpsertPlanResult creates or updates stored check state from a plan result.
	// It fails closed: an in-progress apply-owned row for the same
	// PR/environment/database is preserved regardless of head SHA. Ownership is
	// released only by apply completion (CompleteForApply), rollback completion
	// (MarkActionRequiredForApply), or the explicit same-head no-op recovery
	// path (RecoverApplyOwnedCheckWithNoOpPlan).
	//
	// drift declares how this write treats a review-time deployment drift block:
	// a write from a path that re-ran the drift rollup can clear a stale block
	// (PlanDriftClean) or set one (PlanDriftBlocked); a write from a path that
	// did not evaluate drift (PlanDriftNotEvaluated, e.g. an apply-time plan)
	// must preserve any existing drift block rather than silently clearing it.
	UpsertPlanResult(ctx context.Context, check *Check, drift PlanDriftState) error

	// RecoverApplyOwnedCheckWithNoOpPlan updates same-head apply-owned stored check state
	// from in_progress to a successful no-op plan result. Returns true when recovery occurred.
	RecoverApplyOwnedCheckWithNoOpPlan(ctx context.Context, check *Check) (bool, error)

	// MarkStalePlanSuccessful marks plan-only stored check state successful when
	// the database it covers is no longer in the PR. It fails closed: the update
	// is skipped when the row is in_progress or owns an apply ID, so a started
	// apply that began after stale cleanup read the row keeps blocking the PR.
	// Returns true when the row was marked successful.
	MarkStalePlanSuccessful(ctx context.Context, check *Check) (bool, error)

	// ClearAggregateBlock clears the blocking reason on stored aggregate check
	// state after the PR-level guards re-verified that the blocking condition
	// no longer applies. The update is conditional on the head SHA and blocking
	// reason the caller read, so a block recorded concurrently by another
	// writer (for example for a newer commit) is never erased. Returns true
	// when the row was cleared.
	ClearAggregateBlock(ctx context.Context, check *Check) (bool, error)

	// CompleteForApply updates stored check state to a terminal state only if
	// it still belongs to the given apply and no newer apply exists for the
	// same PR/environment/database. Returns false when another driver changed
	// the stored state first.
	CompleteForApply(ctx context.Context, check *Check, apply *Apply) (bool, error)

	// MarkActionRequiredForApply marks stored check state action_required after
	// a rollback only if no apply newer than the rollback exists for the same
	// PR/environment/database. Rows owned by the rollback, by an older apply, or
	// unowned all qualify: a rollback that never claimed the row must still be
	// able to block the stale successful check left over from the apply it
	// reverted. Returns false when any apply newer than the rollback exists for
	// the target, whether or not it has claimed the row.
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

	// DeleteByPRRetainingBlockingApplyOwned removes stored check state for a
	// closed PR, retaining apply-owned rows the close must not unblock. Once
	// an apply has started, its stored check state stays authoritative across
	// a close and reopen until an operator reconciles the target environment.
	// Plan-only rows (no apply_id) are always deleted. Apply-owned rows are
	// handled by close kind:
	//
	//   - merged close: rows that are in_progress or whose conclusion is
	//     anything but success are retained; rows that concluded successfully
	//     are deleted, because the merged PR carries the applied schema and
	//     nothing remains for the row to block.
	//   - unmerged close: every apply-owned row is retained, including rows
	//     whose conclusion is success. A stored success only proves the
	//     database matched the PR when the row was last written — a commit
	//     that removed the applied change may not have been reconciled into
	//     the row yet, and the unmerged branch means the change never landed.
	//     Reopen-time stale cleanup converges the retained row: it converts
	//     it to action_required when the schema change is gone from the PR,
	//     or a fresh plan result replaces it when the change is still present.
	DeleteByPRRetainingBlockingApplyOwned(ctx context.Context, repo string, pr int, merged bool) error
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

// WebhookEventStore manages durable webhook inbox rows. It is the storage
// primitive behind fast webhook acknowledgement: handlers can persist a delivery
// before returning 2xx, and drivers can claim/retry the stored event after the
// HTTP request has finished.
type WebhookEventStore interface {
	// Create records a webhook delivery in the pending state. Returns
	// inserted=false when provider + delivery GUID already exists, so callers
	// can deduplicate redeliveries. One exception: when the existing row is
	// terminally failed, Create re-opens it (pending, attempts reset, fresh
	// payload) and returns inserted=true, so GitHub's "Redeliver" button —
	// which reuses the original delivery GUID — is a real remediation for a
	// terminally failed delivery instead of a permanent no-op.
	Create(ctx context.Context, event *WebhookEvent) (inserted bool, err error)

	// GetByDeliveryID returns a webhook event by provider + delivery GUID, or nil if not found.
	GetByDeliveryID(ctx context.Context, provider, deliveryID string) (*WebhookEvent, error)

	// HasEventForHead reports whether any delivery is recorded for the given
	// provider + repository + pull request + head SHA, in any state. The
	// reconciliation loop uses it to detect open PR heads whose webhook
	// delivery never reached the inbox.
	HasEventForHead(ctx context.Context, provider, repository string, pullRequest int, headSHA string) (bool, error)

	// FindNext atomically claims one pending, retryable, or lease-expired event.
	// The claim rotates lease_owner/lease_token, increments attempts, and sets a
	// lease expiry in the same transaction. Retryable and lease-expired rows are
	// only reclaimed while attempts < MaxWebhookEventAttempts, so a poison event
	// cannot be reclaimed forever. Returns nil when no event is claimable.
	//
	// Ordering is currently global FIFO (created_at, id). A contemplated
	// evolution is per-(repository, pull_request) claiming with coalescing of
	// superseded deliveries; callers should not depend on cross-key ordering.
	FindNext(ctx context.Context, owner string, leaseDuration time.Duration) (*WebhookEvent, error)

	// Heartbeat extends the current lease. Returns ErrWebhookEventNotFound when
	// the event no longer exists, and ErrWebhookEventLeaseLost when the token is
	// stale, so drivers can abandon work whose result has nowhere to land.
	Heartbeat(ctx context.Context, id int64, leaseToken string, leaseDuration time.Duration) error

	// MarkCompleted marks a claimed event terminal-successful.
	MarkCompleted(ctx context.Context, id int64, leaseToken string) error

	// MarkFailed marks a claimed event failed. A non-nil retryAfter keeps it
	// retryable after that time; nil makes the failure terminal.
	MarkFailed(ctx context.Context, id int64, leaseToken string, errMsg string, retryAfter *time.Time) error

	// Release returns a claimed event to pending and refunds the attempt its
	// claim consumed. Drivers use it when shutdown cancels in-flight
	// processing: an interrupted claim must not consume retry budget, or
	// repeated deploy restarts could terminally fail a delivery that never got
	// a real processing attempt. Returns ErrWebhookEventLeaseLost when the
	// lease token is stale.
	Release(ctx context.Context, id int64, leaseToken string) error

	// InboxStats returns a point-in-time snapshot of the inbox for steady-state
	// observability: row counts per state, the age of the oldest row that is
	// ready to claim but not yet claimed (backlog latency), and the count of
	// rows wedged in processing past the attempt cap with an expired lease. It
	// is read-only and safe to call on a periodic cadence.
	InboxStats(ctx context.Context) (*WebhookInboxStats, error)

	// TerminateStuckProcessing marks as terminally failed every processing row
	// whose lease has expired and whose attempts have reached
	// MaxWebhookEventAttempts. Such a row is a driver that was hard-killed on
	// its final attempt before recording a terminal state — FindNext never
	// reclaims it (it stops reclaiming at the cap). GitHub Redeliver can already
	// reopen an expired-lease processing row on demand (see
	// reopenTerminalWebhookEvent), so this sweep is the automatic complement: it
	// terminalizes rows nobody redelivered, emitting each as a durable failure
	// (for metrics/alerting) and draining the stuck-processing gauge without
	// operator action. Returns the number of rows terminated.
	TerminateStuckProcessing(ctx context.Context, reason string) (int64, error)
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
	// transaction. Pending applies become operator-claimable only after the
	// task rows are committed.
	CreateWithTasks(ctx context.Context, apply *Apply, tasks []*Task) (int64, error)

	// CreateWithTasksAndOperations stores a new apply, its initial tasks, and
	// its per-deployment apply_operations rows in a single transaction. Each
	// operation row's ApplyID is set to the new apply ID before insert.
	// Pending applies become operator-claimable only after every row is
	// committed, so the operator never observes a partially-populated apply.
	CreateWithTasksAndOperations(ctx context.Context, apply *Apply, tasks []*Task, operations []*ApplyOperation) (int64, error)

	// CreateWithGroupedOperations stores a new apply and grouped per-deployment
	// operation/task rows in a single transaction. Each operation row's ApplyID is
	// set to the new apply ID before insert, and each group's tasks are linked to
	// that operation after its auto-increment ID is known.
	CreateWithGroupedOperations(ctx context.Context, apply *Apply, groups []*ApplyOperationWithTasks) (int64, error)

	// Get returns an apply by ID, or nil if not found.
	Get(ctx context.Context, id int64) (*Apply, error)

	// GetByApplyIdentifier returns an apply by apply_identifier, or nil if not found.
	// apply_identifier is the external identifier (e.g., "apply_abc123").
	GetByApplyIdentifier(ctx context.Context, applyIdentifier string) (*Apply, error)

	// GetByIdempotencyKey returns the apply stamped with the given idempotency
	// key, or nil if none exists. An empty key always returns nil (NULL keys are
	// not deduplicated), so callers must guard against the empty case.
	GetByIdempotencyKey(ctx context.Context, idempotencyKey string) (*Apply, error)

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

	// UpdateDerivedState compare-and-swaps the rollout-projected applies.state.
	//
	// It writes only the fields owned by the rollout projection (state,
	// error_message, started_at, completed_at, updated_at) and only when the row
	// still holds expectedState, so a stale projection computed from an earlier
	// read cannot clobber a newer state another sibling drive already wrote.
	// started_at is stamped only when it is still NULL, so the projection can
	// move the parent into an active state without ever rewinding a recorded
	// start time.
	//
	// The write is authorized by whichever lease is on the context: an operation
	// lease (the operation row must still hold its token and belong to applyID)
	// takes precedence over the parent apply lease, so a multi-operation drive
	// can advance the parent only through this projection. If neither lease is
	// present the write is unguarded.
	//
	// swapped=false means the row no longer matched expectedState: the caller's
	// view is stale, so it must skip apply-level side-effects that assume its
	// write landed and let the next poll reconcile. A lost lease is returned as
	// an error (ErrApplyLeaseLost), not swapped=false, so leased callers still
	// fail closed on ownership changes.
	UpdateDerivedState(ctx context.Context, applyID int64, expectedState, newState, errorMessage string, startedAt, completedAt *time.Time) (swapped bool, err error)

	// GetRecent returns the most recent applies across all databases, ordered by creation time desc.
	// Used by `schemabot status` (no args) to show recent activity.
	GetRecent(ctx context.Context, filter RecentAppliesFilter) ([]*Apply, error)

	// GetInProgress returns all applies in non-terminal states.
	// Note: For recovery, use FindNextApply which handles locking.
	GetInProgress(ctx context.Context) ([]*Apply, error)

	// FindNextApply atomically claims the next apply that needs attention.
	// A claim selects one apply that needs work and refreshes its heartbeat in
	// the same transaction. The owner is stored with a freshly generated lease
	// token so operator-owned writes can fail closed after ownership changes.
	// Returns the claimed apply, or nil if nothing needs work.
	FindNextApply(ctx context.Context, owner string) (*Apply, error)

	// ClaimApplyByID atomically claims one specific apply by ID, scoped to the
	// same claimability rules as FindNextApply (pending with tasks, stale active
	// state, retryable within budget, or a pending start control request). On a
	// successful claim it rotates the lease (owner, token, acquired_at) and
	// refreshes the heartbeat so operator-owned writes can fail closed after
	// ownership changes. Returns the claimed apply, or nil if the apply does not
	// exist or is not currently claimable (e.g. another driver holds a fresh
	// lease or the apply is terminal). Used by the operation-level claim loop to
	// acquire the parent apply lease after claiming an apply_operations row.
	ClaimApplyByID(ctx context.Context, applyID int64, owner string) (*Apply, error)

	// FindNextApplyForStopReconciliation atomically claims one apply eligible for
	// stop reconciliation — pending or one of the active recovery-claimable states
	// (the claimableApplyStates set); the resumable non-terminal states
	// failed_retryable and stopped are excluded because they have their own resume
	// paths — that has a pending stop control request, at least one
	// pending operation, and no active operation (none being driven and none
	// awaiting stale recovery), rotating the lease onto it like FindNextApply. It
	// is the trigger for stop reconciliation when no operation is claimable to
	// carry it: under on_failure "continue" a failed earlier sibling can leave the
	// apply with only terminal and pending operations, and the claim gate keeps
	// the pending ones from starting, so without this path the apply would strand
	// non-terminal with its stop request pending forever. Skipping applies with
	// any active operation leaves in-flight (and crash-recovered) drives to settle
	// the stop themselves. Returns nil when no such apply exists or it is locked
	// by a peer.
	FindNextApplyForStopReconciliation(ctx context.Context, owner string) (*Apply, error)

	// Heartbeat updates the apply's updated_at timestamp to maintain the lease.
	// Should be called every 10 seconds while working on an apply.
	// If not called for > 1 minute, another driver can claim the apply.
	Heartbeat(ctx context.Context, applyID int64) error

	// SetRevertSkipped records when skip-revert was dispatched for an apply, so
	// progress consumers can show that revert was skipped and finalization is in
	// progress. It is a targeted write of revert_skipped_at that preserves the
	// apply's updated_at lease heartbeat and touches no other fields; both the
	// control-plane skip-revert handler (no lease) and the data-plane finalizer
	// call it without disturbing recovery-claim staleness.
	SetRevertSkipped(ctx context.Context, applyID int64, at time.Time) error

	// CheckLease verifies that an operator apply lease is still current without
	// mutating the apply row.
	CheckLease(ctx context.Context, lease ApplyLease) error

	// ExpireRetryable transitions failed_retryable applies that exhausted their
	// retry budget or recovery freshness window to permanent failed. Returns the
	// applies updated.
	ExpireRetryable(ctx context.Context) ([]*RetryableApplyExpiration, error)

	// ReapplyFailed transitions a recent permanently failed apply back onto the
	// retryable recovery path. Completed work remains completed; failed tasks and
	// operation rows become failed_retryable so operator drivers can claim and
	// drive only the remaining work. The transition re-checks active-apply
	// exclusivity under the apply target lock because it makes a terminal apply
	// active again.
	ReapplyFailed(ctx context.Context, applyID int64) (*Apply, error)

	// FindMissingSummaryComment returns GitHub-backed applies that should have
	// a terminal summary comment but only have a progress comment. Used by
	// startup reconciliation to post missing summary comments after restarts.
	FindMissingSummaryComment(ctx context.Context) ([]*Apply, error)

	// GetByPR returns all applies for a PR.
	GetByPR(ctx context.Context, repo string, pr int) ([]*Apply, error)

	// ExistsForDatabaseHead reports whether any apply for the PR and database
	// was created from a plan for headSHA. An apply whose plan row no longer
	// exists also counts: without the plan there is no proof of which head it
	// came from, so callers deciding whether a record is safe to retire must
	// treat it as owning.
	ExistsForDatabaseHead(ctx context.Context, repo string, pr int, database, databaseType, headSHA string) (bool, error)

	// Delete removes an apply by ID.
	Delete(ctx context.Context, id int64) error

	// DeleteByPR removes all applies for a PR (cleanup on PR close/merge).
	DeleteByPR(ctx context.Context, repo string, pr int) error
}

// ApplyOperationWithTasks groups one apply_operations row with the task rows it owns.
type ApplyOperationWithTasks struct {
	Operation     *ApplyOperation
	Tasks         []*Task
	AllowTaskless bool
}

// RecentAppliesFilter controls recent apply queries for status views.
type RecentAppliesFilter struct {
	Limit       int
	Environment string
	Deployment  string
	States      []string
}

// RetryableExpirationReason identifies why operator retry recovery stopped.
type RetryableExpirationReason string

const (
	RetryableExpirationAttemptBudget  RetryableExpirationReason = "retry_budget_exhausted"
	RetryableExpirationRecoveryWindow RetryableExpirationReason = "recovery_window_expired"
)

// RetryableApplyExpiration is a failed_retryable apply that was made permanent
// because operator recovery should no longer retry it automatically.
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

	// UpsertShardProgress creates or updates the per-shard task row for
	// (apply_operation_id, namespace, table_name, shard). It is the operator's
	// write-through for reflected per-shard progress (e.g. PlanetScale shards
	// discovered via SHOW VITESS_MIGRATIONS). It requires the operation lease on
	// the context: the single lease-holding operator is the only writer of an
	// operation's per-shard rows, so the lookup-then-write is serialized by that
	// lease and needs no unique constraint. A displaced operator (lost lease)
	// fails closed with ErrApplyLeaseLost. On conflict only the progress fields
	// change; identity and DDL are preserved.
	UpsertShardProgress(ctx context.Context, task *Task) error

	// GetByApplyID returns all tasks for an apply.
	// Used for aggregating task states to derive Apply state.
	GetByApplyID(ctx context.Context, applyID int64) ([]*Task, error)

	// GetByApplyOperationID returns the drive tasks for a single apply_operation.
	// Unsharded operations return their per-table tasks. Sharded work operations
	// return the task whose namespace/shard/table matches the operation key so the
	// drive can rebuild its shard selector. Reflected per-shard progress rows that
	// do not match a sharded work operation key are excluded — read them via
	// GetShardProgressByApplyOperationID.
	GetByApplyOperationID(ctx context.Context, applyOperationID int64) ([]*Task, error)

	// GetShardProgressByApplyOperationID returns the per-shard detail task rows
	// (shard != "") for an operation. These are a reflected read-model the
	// per-table loaders exclude, so they never re-enter the per-table pipeline;
	// the renderer reads the per-shard breakdown through this method.
	GetShardProgressByApplyOperationID(ctx context.Context, applyOperationID int64) ([]*Task, error)

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

	// Supersede retires the tracked comment for a single (apply_id, comment_state)
	// by stamping superseded_at — the row and the GitHub comment are kept, but
	// SchemaBot no longer treats it as the active comment for its state. A later
	// Upsert for the same state clears superseded_at. A missing or already-
	// superseded row is not an error.
	Supersede(ctx context.Context, applyID int64, commentState string) error

	// ClearPendingFreeze removes the pending-freeze marker for a single
	// (apply_id, comment_state) once the superseded comment's frozen rendering
	// has landed on GitHub. A missing row or an already-clear marker is not an
	// error.
	ClearPendingFreeze(ctx context.Context, applyID int64, commentState string) error
}

// PlanCommentStore tracks plan comments posted on PRs so a newer plan comment
// for the same database can minimize the ones it supersedes. Rows exist only
// for comments actually posted; minimized_at is set only after the GitHub
// minimize call succeeded, so an unminimized row is always retried by the next
// supersede.
type PlanCommentStore interface {
	// Insert stores a newly posted plan comment and sets comment.ID.
	Insert(ctx context.Context, comment *PlanComment) error

	// ListUnminimizedForSlot returns the not-yet-minimized comments for a
	// (repository, pull_request, database) slot, ordered by id ascending. The
	// caller decides which of them a newly posted comment supersedes.
	ListUnminimizedForSlot(ctx context.Context, repo string, pr int, database, databaseType string) ([]*PlanComment, error)

	// MarkMinimized stamps minimized_at after the GitHub minimize call
	// succeeded. An already-minimized row is not an error.
	MarkMinimized(ctx context.Context, id int64) error
}

// ApplyOperationStore manages per-(apply, deployment, operation_key) child rows
// for multi-operation applies. One apply owns 1..N apply_operations rows.
type ApplyOperationStore interface {
	// Insert stores a new apply_operations row and returns its ID.
	// Fails with a uniqueness error if (apply_id, deployment, operation_key)
	// already exists.
	Insert(ctx context.Context, ad *ApplyOperation) (int64, error)

	// Get returns a child row by ID, or nil if not found.
	Get(ctx context.Context, id int64) (*ApplyOperation, error)

	// GetByApplyAndDeployment returns the legacy unkeyed child row for
	// (apply_id, deployment), or nil if not found.
	GetByApplyAndDeployment(ctx context.Context, applyID int64, deployment string) (*ApplyOperation, error)

	// GetByApplyDeploymentAndOperationKey returns the child row for
	// (apply_id, deployment, operation_key), or nil if not found.
	GetByApplyDeploymentAndOperationKey(ctx context.Context, applyID int64, deployment, operationKey string) (*ApplyOperation, error)

	// ListByApply returns all child rows for an apply in (created_at, id) order.
	ListByApply(ctx context.Context, applyID int64) ([]*ApplyOperation, error)

	// ListByApplies returns all child rows for the requested applies in
	// (apply_id, created_at, id) order.
	ListByApplies(ctx context.Context, applyIDs []int64) ([]*ApplyOperation, error)

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

	// MarkTerminal sets a terminal state and stamps completed_at on a child row.
	// Use for terminal states that record a reconciliation time (cancelled,
	// reverted). Do not use for stopped: stopped is resumable, so it keeps
	// completed_at nil (use UpdateState). Use MarkCompleted / MarkFailed for
	// completed / failed.
	MarkTerminal(ctx context.Context, id int64, newState string) error

	// SaveExternalOperationID stores the remote data plane's apply_operation_id
	// on the operation that owns the dispatch.
	SaveExternalOperationID(ctx context.Context, operationID int64, externalOperationID string) error

	// SaveExternalID stores the remote data plane's apply_id on the operation
	// that owns the dispatch.
	SaveExternalID(ctx context.Context, operationID int64, externalID string) error

	// SaveEngineResumeState stores opaque engine resume state on the operation.
	SaveEngineResumeState(ctx context.Context, operationID int64, resumeState *EngineResumeState) error

	// GetEngineResumeState returns opaque engine resume state for the operation.
	GetEngineResumeState(ctx context.Context, operationID int64) (*EngineResumeState, error)

	// FindNextApplyOperation atomically claims the next child row that needs
	// attention and rotates a fresh operation lease (owner + token) onto it in
	// the same transaction, returning the row populated with that lease.
	//
	// Pending rows are transitioned to running and stamped with started_at; a
	// stopped row whose parent apply has a pending start request is resumable
	// and is transitioned to resuming (so the request-gated stopped predicate
	// stops matching once the row is claimed); already-active rows whose
	// heartbeat has been stale for more than one minute are re-leased without
	// changing their state. Other terminal rows
	// (completed/failed/cancelled/reverted) are never claimed.
	//
	// owner identifies the claiming driver and is required; it is recorded as
	// the operation's lease owner. Returns the claimed row, or nil if nothing
	// needs work.
	FindNextApplyOperation(ctx context.Context, owner string) (*ApplyOperation, error)

	// FindNextApplyOperationCutover atomically claims the next operation parked
	// at the cutover barrier whose turn it is, in deployment order, and rotates a
	// fresh operation lease onto it in the same transaction. It is the cutover
	// counterpart to FindNextApplyOperation: that primitive gates the copy phase
	// (claims pending rows → running); this one gates the cutover phase.
	//
	// A waiting_for_cutover row is claimed and transitioned to cutting_over only
	// when every earlier deployment_order sibling has reached completed (the
	// cutover gate is completed-only, with the on_failure "continue" exemption
	// for a terminal-failed earlier sibling) and no pending stop control request
	// exists for the apply. Separately, a row already in cutting_over or
	// revert_window whose heartbeat has been stale for more than one minute is
	// re-leased without changing its state — recovering an in-flight cutover whose
	// driver died, which carries no ordering gate.
	//
	// owner identifies the claiming driver and is required. Returns the claimed
	// row, or nil if nothing is ready to cut over.
	FindNextApplyOperationCutover(ctx context.Context, owner string) (*ApplyOperation, error)

	// Heartbeat refreshes the child row's updated_at timestamp to extend the
	// claim's lease while a driver is acting on it. Mirrors ApplyStore.Heartbeat
	// semantics: silent no-op when the row no longer exists.
	Heartbeat(ctx context.Context, id int64) error

	// DeleteByApply removes all child rows for an apply (cleanup on apply delete).
	DeleteByApply(ctx context.Context, applyID int64) error

	// MarkPendingStoppedByApply transitions every still-pending operation of an
	// apply to stopped, returning the number of rows changed. Used by operator
	// stop reconciliation: once a stop is pending the claim gate keeps pending
	// siblings from starting, so they are terminalized here to let the apply
	// settle instead of stranding non-terminal under on_failure "continue". Only
	// pending rows are touched; running/terminal rows are left untouched. stopped
	// is resumable, so completed_at is left nil. Apply-lease guarded when a lease
	// is present in ctx.
	MarkPendingStoppedByApply(ctx context.Context, applyID int64) (int64, error)
}

// ApplyLogStore manages apply log entries for debugging and audit.
// Logs capture state transitions, errors, and events during schema changes.
// Logs are kept forever for audit purposes.
type ApplyLogStore interface {
	// Append adds a new log entry.
	Append(ctx context.Context, log *ApplyLog) error

	// GetByApply returns all logs for an apply, ordered by created_at.
	GetByApply(ctx context.Context, applyID int64) ([]*ApplyLog, error)

	// GetRecentByApply returns the newest limit logs for an apply, ordered by
	// created_at ascending so the result reads chronologically. The query is
	// bounded — long-running applies can accumulate far more log rows than a
	// caller rendering a tail needs.
	GetRecentByApply(ctx context.Context, applyID int64, limit int) ([]*ApplyLog, error)

	// List returns logs matching the filter criteria, ordered by created_at.
	List(ctx context.Context, filter ApplyLogFilter) ([]*ApplyLog, error)
}

// ControlRequestStore manages durable user control requests.
// A control request is behavioral state, not just audit: operator drivers use
// pending rows to recover accepted operations after process restarts.
type ControlRequestStore interface {
	// RequestPending records a pending request for an apply operation. If the
	// same operation is already pending for the apply, the existing request is
	// returned with alreadyPending=true.
	RequestPending(ctx context.Context, req *ApplyControlRequest) (*ApplyControlRequest, bool, error)

	// GetPending returns the pending request for an apply operation.
	GetPending(ctx context.Context, applyID int64, operation ControlOperation) (*ApplyControlRequest, error)

	// GetByOperation returns the request for an apply operation regardless of
	// status (nil if none). Unlike GetPending it does not filter on status, so
	// callers can observe a completed or failed latch — for example the release
	// latch that exempts a paused rollout (see ReleasesPausedRollout). At most
	// one row exists per (apply_id, operation).
	GetByOperation(ctx context.Context, applyID int64, operation ControlOperation) (*ApplyControlRequest, error)

	// CompletePending marks the pending request for an apply operation completed.
	CompletePending(ctx context.Context, applyID int64, operation ControlOperation) error

	// FailPending marks the pending request for an apply operation failed with an
	// operator-visible reason.
	FailPending(ctx context.Context, applyID int64, operation ControlOperation, errorMessage string) error
}
