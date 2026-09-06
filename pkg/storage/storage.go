// Package storage defines the storage interface for SchemaBot.
// Implemented by the MySQL backend (pkg/storage/mysqlstore) and the
// PostgreSQL backend (pkg/storage/postgresstore).
package storage

import (
	"context"
	"errors"
	"time"
)

// ApplyLeaseStaleAfter is how long an apply or apply_operation lease heartbeat
// (updated_at) may go unrefreshed before peer drivers treat the lease as
// expired and reclaim the row for crash recovery. The claim queries in
// mysqlstore derive their SQL staleness window from this constant.
//
// The window is also the driver-side presumed-lost bound: a driver whose
// heartbeats have been failing for this long must assume a peer can already
// have reclaimed the work, stop driving, and stop writing apply state. Storage
// writes remain lease-guarded either way; presuming the lease lost bounds how
// long two drivers can run the same engine work concurrently.
const ApplyLeaseStaleAfter = time.Minute

// ApplyDriveStallAfter bounds how long a drive may go without mirroring any
// task progress to storage before its driver presumes the drive goroutine is
// wedged and cancels the run. A healthy drive's poll loop writes every task row
// on every poll tick, so legitimate silence is far shorter than this window;
// the window is kept generous so slow pre-poll phases (target schema pulls,
// re-planning, engine acceptance) have the full window before their first
// mirror write.
//
// That makes tasks.updated_at the drive's liveness signal, and this the bound
// past which no drive the operator would still let run has spoken for a row.
// The stranded-active reaper derives its own window from it for that reason, so
// the two cannot drift: a row the operator would cancel a drive over is the
// same row the reaper may settle.
//
// The reaper waits a multiple of this window rather than matching it, because
// cancelling a run is recoverable and writing a verdict onto a row is not. Both
// read the same signal; only one of them can be wrong in a way an operator
// cannot undo.
const ApplyDriveStallAfter = 5 * time.Minute

// DefaultMaxDriversPerApply is the per-apply driver cap used when a deployment
// does not configure max_drivers_per_apply.
//
// The cap bounds how many operation leases one apply may hold at once. A wide
// fan-out claims its operations oldest-first like any other work, so without a
// bound a single apply takes every driver on the plane and holds them for as
// long as its slowest operation runs — starving every other database behind it
// with no limit. The cap is what keeps the plane available: capacity always
// remains to claim another tenant's work and to consume control requests.
//
// It gates every claim that starts work — a first start, a resume after a stop,
// and a deferred deploy — and no claim that recovers or controls. That split is
// the whole design: recovering a crashed operation and consuming a stop or
// cancel must never be capped, or the cap wedges the apply it is bounding, while
// leaving any start path uncapped is a way around the bound rather than an
// exception to it. A stop/start cycle is the case that makes this concrete: stop
// moves every pending sibling to stopped at once, so an uncapped resume would
// hand one apply the whole pool for the rest of its rollout.
//
// The count lives in storage, so the cap is plane-wide across pods with no
// extra coordination, and both planes enforce it because both run these claim
// queries against their own storage.
//
// It is deliberately soft, and softer than a ceiling of one over. Claims run at
// READ COMMITTED and count sibling rows rather than the row they lock, so
// concurrent claimers never exclude each other: every driver whose claim reads
// the count before the others commit sees the same pre-claim value and claims on
// it. The transient overshoot is therefore bounded by how many drivers claim in
// that window, not by one. It is self-correcting rather than durable — once
// those leases commit, the next claim counts them and the cap binds — so the
// steady-state occupancy is the cap and the excess drains as operations finish.
// Size the cap for the steady state; do not read it as a hard admission limit
// that some other guard will hold to cap+1. Paying for a hard bound would mean
// serializing every claim behind a per-apply lock, which is the contention the
// cap exists to avoid.
//
// It is configurable because the value only means something relative to a
// plane's pods x drivers: too high and it bounds nothing, too low and a wide
// sharded fan-out serializes. The default suits a plane running the default
// driver count on more than one pod; size it deliberately when either differs.
const DefaultMaxDriversPerApply = 2

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
// Methods accepting a *Lock canonicalize its repository, database name, and database type in place before persisting.
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

	// Update touches updated_at to mark liveness of the caller's own lock.
	// The touch is owner-scoped: it returns ErrLockNotFound when no lock
	// exists for the database, and ErrLockNotOwned when another owner holds
	// it — a caller whose lock was force-released and re-acquired elsewhere
	// must not refresh the new owner's row.
	Update(ctx context.Context, lock *Lock) error

	// GetByPR returns all locks associated with a PR (for cleanup on merge/close).
	GetByPR(ctx context.Context, repo string, pr int) ([]*Lock, error)
}

// CheckStore manages SchemaBot's stored check state.
// Per-database rows track internal status for a PR/environment/database.
// Aggregate rows store the GitHub check_run_id for the visible GitHub Check Run.
// Methods accepting a *Check canonicalize its repository, database name, database type, and environment in place before persisting.
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
	//
	// Returns stored=false only when the ownership guard refused the write. The
	// refusal is a correct outcome, not an error, but it leaves the stored row
	// on the apply's commit: callers must surface it, because a plan whose
	// result never landed cannot converge the PR's checks on its own.
	//
	// A write whose target row no longer exists — the PR closed and its check
	// state was cleaned up while the plan ran — returns ErrCheckNotFound rather
	// than a refusal, so callers never report a vanished gate as one an apply
	// is holding.
	UpsertPlanResult(ctx context.Context, check *Check, drift PlanDriftState) (stored bool, err error)

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

	// MarkActionRequiredForApply marks stored check state action_required for a
	// terminal apply only if no newer apply exists for the same target. The row
	// may be owned by this apply, an older apply, or no apply: completed rollbacks
	// must block stale success even when their claim never landed, and safely
	// cancelled forward applies must be able to release retained ownership.
	// Returns false when any newer apply exists for the target or a cancelled
	// forward apply has completed task history, whether or not a newer apply has
	// claimed the row.
	MarkActionRequiredForApply(ctx context.Context, check *Check, apply *Apply) (bool, error)

	// MarkCancelledApplyFailed marks stored check state as a terminal failure
	// owned by a cancelled apply when a completed forward task proves that apply
	// may have changed the target. The row may be owned by this apply, an older
	// apply, or no apply, so a cancelled apply whose claim never landed can still
	// block the stale check the owning apply left behind. It also accepts an
	// already-completed owned row so stale reconciliation can durably record the
	// decision. Returns false when the task evidence is absent or a newer apply
	// exists for the target.
	MarkCancelledApplyFailed(ctx context.Context, check *Check, apply *Apply) (bool, error)

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

	// List returns all settings, ordered by key ascending.
	List(ctx context.Context) ([]*Setting, error)

	// Delete removes a setting by key. It returns ErrSettingNotFound when no
	// setting with that key exists.
	Delete(ctx context.Context, key string) error
}

// WebhookEventStore manages durable webhook inbox rows. It is the storage
// primitive behind fast webhook acknowledgement: handlers can persist a delivery
// before returning 2xx, and drivers can claim/retry the stored event after the
// HTTP request has finished.
// Create canonicalizes the provided event's repository in place before
// persisting. The coalescing reads (HasCoveringSuccessor, SupersedeIfCovered)
// fold the repository only inside their SQL predicates and leave the caller's
// event untouched.
type WebhookEventStore interface {
	// Create records a webhook delivery in the pending state. Returns
	// inserted=false when provider + delivery GUID already exists, so callers
	// can deduplicate redeliveries. One exception: when the existing row is
	// terminal — failed, permanently failed, completed, or superseded —
	// Create re-opens it (pending, attempts reset, fresh payload) and returns
	// inserted=true, so GitHub's "Redeliver" button — which reuses the
	// original delivery GUID — is a real remediation for a terminal delivery
	// instead of a permanent no-op. Redeliver is also the only lever that
	// revives a permanently failed row — the reconciler never re-Creates its
	// GUID because HasEventForHead reports the dead-lettered head as covered —
	// and it exists only for organic GUIDs; a synthesized dead-lettered row
	// stays terminal, its head advancing only through a fresh delivery.
	//
	// A non-nil RetryAfter on the event is persisted as a not-before time: the
	// delivery is durable immediately but FindNext will not claim it until the
	// time passes. Producers of deferred work — a redundant convergence signal
	// that should lose the race to the primary delivery — set it to schedule
	// dispatch; nil means immediately claimable. A redelivery reopen clears it.
	//
	// Only a fresh insert populates event.ID; a reopen returns inserted=true
	// with event.ID left zero, so callers whose behavior differs between the
	// two — a deferred producer whose not-before time a reopen discards, for
	// example — can tell them apart.
	Create(ctx context.Context, event *WebhookEvent) (inserted bool, err error)

	// GetByDeliveryID returns a webhook event by provider + delivery GUID, or nil if not found.
	GetByDeliveryID(ctx context.Context, provider, deliveryID string) (*WebhookEvent, error)

	// HasEventForHead reports whether a plan-trigger delivery — an
	// auto-plannable pull_request row (AutoPlanPullRequestActions) — is
	// recorded for the given provider + repository + pull request + head
	// SHA. The reconciliation loop uses it to detect open PR heads whose
	// auto-plan delivery never reached the inbox. Rows of other event types
	// or actions can carry the same PR + head SHA without planning it, so
	// they do not cover the head. A terminally failed row covers its head
	// when it is organic (the operator's GitHub Redeliver lever exists for
	// it), but not when it is a plain failed synthesized row
	// (SynthesizedWebhookDeliveryIDPrefix) — there is no Redeliver lever for
	// a synthesized GUID, so the reconciler must be able to synthesize a
	// fresh recovery delivery that reopens it through Create's
	// duplicate-GUID branch. A permanently failed (dead-lettered) row covers
	// its head in either GUID form: the driver proved no retry can plan that
	// head, so no reconciler synthesis may resurrect it. A superseded row
	// never covers: it was discarded on the promise that a covering successor
	// performs its work, so it cannot itself attest that the head was
	// planned — if the successor's coverage lapses, the reconciler must be
	// able to synthesize afresh.
	HasEventForHead(ctx context.Context, provider, repository string, pullRequest int, headSHA string) (bool, error)

	// HasCoveringSuccessor reports whether a covering successor — as defined
	// by SupersedeIfCovered — currently exists for the claimed event's
	// (provider, repository, pull request). It is a read-only, advisory
	// probe with no lease semantics: the answer can change the moment it
	// returns, so a true result is a reason to verify the claimed head
	// against the live PR before discarding, never permission to discard by
	// itself — SupersedeIfCovered re-evaluates the predicate atomically with
	// its lease-guarded write.
	HasCoveringSuccessor(ctx context.Context, event *WebhookEvent) (bool, error)

	// SupersedeIfCovered marks a claimed auto-plannable pull_request event
	// superseded when a covering successor exists for the same
	// (provider, repository, pull request), and reports whether it did. The
	// caller must hold the claim: the write is guarded by the event's lease
	// token and its processing state, and returns ErrWebhookEventLeaseLost /
	// ErrWebhookEventNotFound like the other lease-guarded writes.
	//
	// A successor is a strictly newer pull_request delivery for the same PR —
	// strictly greater received_at, so a Redeliver-reopened row (which
	// refreshes received_at) is never superseded by rows that predate the
	// operator's redelivery, and deliveries whose received_at ties at the
	// column's timestamp precision never cover each other (they both
	// process, the safe direction for an optimization) — that either plans
	// the PR (AutoPlanPullRequestActions) or closes it
	// (PullRequestClosedAction). received_at is webhook arrival order, not
	// Git push order: GitHub does not guarantee delivery order and a
	// Redeliver refreshes received_at on an old-head row, so a newer
	// received_at proves only that newer-arriving work exists, never that
	// the claimed head is stale. Callers must independently confirm against
	// the live PR that the claimed head is no longer current before
	// invoking this — the durable dispatcher fetches the PR from GitHub and
	// skips coalescing when the claimed delivery carries the current head.
	//
	// A successor covers only when its own work will run, is running, or
	// has run: pending, processing (live lease, or reclaimable under the
	// attempt cap), retryable under the attempt cap, or completed.
	// Terminally failed and superseded successors never cover — discarding
	// old work on the strength of a successor that will not run would lose
	// the PR's plan. Coverage is a promise evaluated once, at supersede
	// time: a covering successor that later terminally fails does not
	// resurrect the superseded row. Recovery for that loss is the operator
	// Redeliver lever, plus reconciler synthesis when the lost work
	// targeted the PR's current head. Coalescing is keyed per PR, not per
	// head SHA: distinct PRs sharing a head remain independent.
	//
	// The asymmetry for closed deliveries: a newer closed delivery covers
	// older auto-plan work (planning a closed PR is pointless, and a reopen
	// arrives as its own auto-plannable delivery), but a closed delivery is
	// never itself superseded — its cleanup must always run, which the
	// auto-plan-only guard on the updated row enforces.
	SupersedeIfCovered(ctx context.Context, event *WebhookEvent) (bool, error)

	// FindNext atomically claims one pending, retryable, or lease-expired event.
	// The claim rotates lease_owner/lease_token, increments attempts, and sets a
	// lease expiry in the same transaction. A pending row with a future
	// retry_after (not-before time) is not claimable until it passes. Retryable
	// and lease-expired rows are only reclaimed while attempts <
	// MaxWebhookEventAttempts, so a poison event cannot be reclaimed forever.
	// Returns nil when no event is claimable.
	//
	// The claim consumes retry_after (the persisted row's is cleared); the
	// returned event carries ClaimableSince — the later of receipt and the
	// consumed not-before time — so the dispatcher can measure dispatch lag
	// from when the row became eligible rather than from receipt.
	//
	// Ordering is global FIFO (created_at, id); callers should not depend on
	// cross-key ordering. A row that spent time deferred re-enters dispatch
	// at its original insertion position once due — ahead of rows created
	// during its deferral — not at its due time. Stale auto-plan claims are
	// coalesced after the claim, not here: for each claimed auto-plannable
	// pull_request event the driver probes HasCoveringSuccessor, confirms
	// against the live PR that the claimed head is no longer current, and
	// only then calls SupersedeIfCovered.
	FindNext(ctx context.Context, owner string, leaseDuration time.Duration) (*WebhookEvent, error)

	// Heartbeat extends the current lease. Returns ErrWebhookEventNotFound when
	// the event no longer exists, and ErrWebhookEventLeaseLost when the token is
	// stale, so drivers can abandon work whose result has nowhere to land.
	Heartbeat(ctx context.Context, id int64, leaseToken string, leaseDuration time.Duration) error

	// MarkCompleted marks a claimed event terminal-successful.
	MarkCompleted(ctx context.Context, id int64, leaseToken string) error

	// MarkFailed marks a claimed event failed. A non-nil retryAfter keeps it
	// retryable after that time; nil makes the failure terminal but still
	// recoverable: the reconciler may synthesize a recovery delivery for its
	// head, and GitHub Redeliver reopens it.
	MarkFailed(ctx context.Context, id int64, leaseToken string, errMsg string, retryAfter *time.Time) error

	// MarkFailedPermanent dead-letters a claimed event: the driver proved the
	// delivery can never succeed for its head, so no retry, reconciler
	// synthesis, or attempt-budget reset may revive it. The row counts as head
	// coverage in HasEventForHead. Only an explicit GitHub Redeliver reopens
	// the row itself, and only an organic delivery has one — a synthesized
	// delivery has no GitHub delivery to redeliver, so its head moves forward
	// only through a fresh delivery (a new head push or a check re-run).
	MarkFailedPermanent(ctx context.Context, id int64, leaseToken string, errMsg string) error

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

// ListPlansOptions filters and bounds a plan listing. Zero-value fields are
// not applied, so an empty options value with a Limit lists the newest plans
// across every database and environment.
type ListPlansOptions struct {
	// Database, when set, restricts results to plans for that database name.
	Database string
	// Environment, when set, restricts results to plans for that environment.
	Environment string
	// Repository, when set, restricts results to plans generated for PRs in
	// that repository (owner/name).
	Repository string
	// PullRequest, when positive, restricts results to plans generated for
	// that PR number. Requires Repository — a PR number is only meaningful
	// within one repository, so List errors when it is set alone.
	PullRequest int
	// Since, when set, restricts results to plans created at or after this
	// instant.
	Since time.Time
	// Limit bounds the number of plans returned and must be positive.
	Limit int
}

// PlanStore manages schema change plans.
// Plans are created by Plan() and stored for Apply() and staleness detection.
// Both GRPCClient and LocalClient are stateless - SchemaBot owns plan storage.
// Create canonicalizes the provided plan's repository, environment, database, and database type in place before persisting.
type PlanStore interface {
	// Create stores a new plan and returns its ID. Returns error if plan_identifier already exists.
	Create(ctx context.Context, plan *Plan) (int64, error)

	// Get returns a plan by plan_identifier (external identifier), or nil if not found.
	Get(ctx context.Context, planIdentifier string) (*Plan, error)

	// GetByID returns a plan by ID, or nil if not found.
	GetByID(ctx context.Context, id int64) (*Plan, error)

	// GetByLock is not implemented: plans carry no direct lock association,
	// and every implementation returns ErrNotImplemented so a caller can
	// never mistake the missing capability for "no plans".
	GetByLock(ctx context.Context, lockID int64) ([]*Plan, error)

	// GetByPR returns all plans for a PR.
	GetByPR(ctx context.Context, repo string, pr int) ([]*Plan, error)

	// List returns plans matching opts, newest first. Ordering is
	// deterministic on created_at ties (see the sqlstore GetByPR ordering
	// rationale). Returned plans omit SchemaFiles — the full desired-schema
	// DDL is the bulk of a plan row and listings never need it; fetch a single
	// plan via Get for the complete record. Returns an error when opts.Limit
	// is not positive, or when opts.PullRequest is set without
	// opts.Repository.
	List(ctx context.Context, opts ListPlansOptions) ([]*Plan, error)

	// Delete removes a plan by ID.
	Delete(ctx context.Context, id int64) error

	// DeleteByPR removes all plans for a PR (cleanup on PR close/merge).
	DeleteByPR(ctx context.Context, repo string, pr int) error
}

// ApplyStore manages schema change execution state.
// Applies are created when Apply() is called and updated during execution.
// Methods accepting *Apply canonicalize repository, database, database type,
// and environment in place before persisting; Update rewrites these fields on
// the struct even though its SQL statement writes only state and progress.
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

	// AttachOperationWithTasks stores one additional apply_operations row and
	// its tasks under an existing apply in a single transaction. The apply's
	// state is re-read under a row lock inside the transaction and the attach
	// fails with ErrApplyNotActive when it is terminal — new work must never
	// land on an apply no drive will pick up again. The (apply_id, deployment,
	// operation_key) unique index is the idempotency guard: a concurrent attach
	// of the same operation loses with ErrApplyOperationExists, which the
	// caller resolves by re-reading the winner's row. On success the operation's
	// ID and every task's ID and ApplyOperationID are populated.
	AttachOperationWithTasks(ctx context.Context, apply *Apply, operation *ApplyOperation, tasks []*Task) error

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

	// GetRecent returns applies across all databases for `schemabot status` (no
	// args), in-flight work first.
	//
	// An apply is in flight when a driver is still on it: its latest lease
	// heartbeat — on the apply row or any of its operations — falls inside the
	// lease staleness window, and its state is not terminal. So a schema change
	// still copying rows stays on the first page however long ago it started,
	// while an abandoned or finished one sinks. In-flight applies are ordered
	// oldest-started first; everything else by when it last changed, which for a
	// finished apply is when it finished.
	GetRecent(ctx context.Context, filter RecentAppliesFilter) ([]*Apply, error)

	// CountRecentByState returns how many applies match the filter, grouped by
	// state. The filter's Limit is ignored: counts cover every matching row, so
	// a status summary is not truncated by list pagination.
	CountRecentByState(ctx context.Context, filter RecentAppliesFilter) (map[string]int, error)

	// GetInProgress returns all applies in non-terminal states.
	// Note: For recovery, use ClaimApplyByID which handles locking.
	GetInProgress(ctx context.Context) ([]*Apply, error)

	// FindStuckPendingApplies returns pending applies that a driver should
	// already have claimed — pending with child rows (the child-rows arm of
	// ClaimApplyByID's pending predicate) — but whose created_at is older than
	// olderThan, ordered oldest first and capped at limit. It is a read-only
	// diagnostic: apply creation rejects a second active apply for the same
	// target rather than queuing it, so a pending apply this old is not waiting
	// its turn — either no driver is claiming (a starved or crash-looping
	// operator pool) or the claim path is wedged. Returns an empty slice when
	// nothing is stuck. A non-positive limit means no cap.
	FindStuckPendingApplies(ctx context.Context, olderThan time.Duration, limit int) ([]*Apply, error)

	// ClaimApplyByID atomically claims one specific apply by ID when it needs a
	// driver: pending with child rows, stale active state, retryable within
	// budget, a pending start control request, or stopped with a pending cancel
	// control request. On a successful claim it rotates the lease (owner, token,
	// acquired_at) and refreshes the heartbeat so operator-owned writes can fail
	// closed after ownership changes.
	//
	// Terminal state is not by itself a refusal: stopped is terminal, and a
	// stopped apply is claimable both for a pending start (to resume it) and for
	// a pending cancel (to terminalize it). Every other terminal state is
	// refused. A claim for cancel leaves the apply stopped and rotates only the
	// lease, so it is admitted at most once per request until the lease goes
	// stale.
	//
	// Claiming a stopped apply settles a conflicting request as a side effect: a
	// claim for cancel fails any pending start request on the same apply, so the
	// cancel wins rather than racing a resume. A stopped claim refused because
	// the apply was superseded (see MarkSuperseded) or because another active
	// apply owns the target likewise fails the pending start request, with the
	// reason, instead of stranding it.
	//
	// Returns the claimed apply, or nil if the apply does not exist or is not
	// currently claimable — another driver holds a fresh lease, the apply is
	// terminal without a request that admits it, the target's exclusion lock
	// is held elsewhere, a stopped claim was refused as above, or the apply is
	// failed_retryable but superseded, which excludes it from automatic retry.
	// Used by the operation-level claim loop to acquire the parent apply lease
	// after claiming an apply_operations row.
	ClaimApplyByID(ctx context.Context, applyID int64, owner string) (*Apply, error)

	// FindNextApplyForStopReconciliation atomically claims one apply eligible for
	// stop reconciliation — pending or one of the active recovery-claimable states
	// (the claimableApplyStates set); the resumable non-terminal states
	// failed_retryable and stopped are excluded because they have their own resume
	// paths — that has a pending stop control request, at least one
	// pending operation, and no active operation (none being driven and none
	// awaiting stale recovery), rotating the lease onto it like ClaimApplyByID. It
	// is the trigger for stop reconciliation when no operation is claimable to
	// carry it: under on_failure "continue" a failed earlier sibling can leave the
	// apply with only terminal and pending operations, and the claim gate keeps
	// the pending ones from starting, so without this path the apply would strand
	// non-terminal with its stop request pending forever. Skipping applies with
	// any active operation leaves in-flight (and crash-recovered) drives to settle
	// the stop themselves. Returns nil when no such apply exists or it is locked
	// by a peer.
	FindNextApplyForStopReconciliation(ctx context.Context, owner string) (*Apply, error)

	// FindNextApplyForOperationProjection atomically claims one apply whose
	// operation rows have all settled while the apply itself is still
	// non-terminal and its heartbeat has gone stale, rotating the lease onto it
	// like ClaimApplyByID. It is the repair path for a parent left behind its own
	// children: an operation drive writes its terminal operation row and then
	// projects the parent, so a crash between those two writes leaves nothing to
	// finish the projection — every operation is terminal, so no operation-level
	// claim arm matches, and the one-active-apply guard keeps that target blocked
	// until the parent settles. Requiring a stale heartbeat is what keeps this off
	// live rollouts: a driver mid-projection still holds a fresh lease. Applies
	// with no operation rows are excluded (there is nothing to project from), as
	// are pending, stopped, and failed_retryable parents, which have their own
	// resume paths. Returns nil when no such apply exists or it is locked by a
	// peer.
	FindNextApplyForOperationProjection(ctx context.Context, owner string) (*Apply, error)

	// Heartbeat updates the apply's updated_at timestamp to maintain the lease.
	// Should be called every 10 seconds while working on an apply.
	// If not called for > 1 minute, another driver can claim the apply.
	Heartbeat(ctx context.Context, applyID int64) error

	// ReleaseClaim releases an apply lease the calling driver holds but will not
	// drive any further — a process shutting down hands its claimed applies back
	// rather than holding them until the lease goes stale on its own. It clears
	// the lease fields and backdates the heartbeat past the staleness window so
	// the row is re-claimable on the next poll. The apply's state is left
	// unchanged: the released row re-enters through the same stale-recovery path
	// that recovers a crashed driver's work. Mirrors
	// ApplyOperationStore.ReleaseClaim.
	//
	// The write is guarded on the lease token. Reports false when the lease no
	// longer matches (another writer rotated or cleared it), in which case the
	// row has moved on and needs nothing from the caller.
	ReleaseClaim(ctx context.Context, lease ApplyLease) (bool, error)

	// SetRevertSkipped records when skip-revert was dispatched for an apply, so
	// progress consumers can show that revert was skipped and finalization is in
	// progress. It is a targeted write of revert_skipped_at that preserves the
	// apply's updated_at lease heartbeat and touches no other fields; both the
	// control-plane skip-revert handler (no lease) and the data-plane finalizer
	// call it without disturbing recovery-claim staleness.
	SetRevertSkipped(ctx context.Context, applyID int64, at time.Time) error

	// MarkSuperseded records that successor took over the apply's unfinished
	// work, by ApplyIdentifier. It is a targeted write of superseded_by that
	// preserves the apply's updated_at lease heartbeat and touches no other
	// fields, so the apply that handed off keeps whatever terminal state it
	// settled in.
	//
	// The marker is write-once: it must outlive the successor, so it is never
	// cleared and never reassigned. Marking again with the same successor
	// succeeds, since a redelivered handoff records the same fact. Marking with
	// a different successor returns ErrApplyAlreadySuperseded — two applies
	// claiming the same handoff means the takeover decision is ambiguous, and an
	// ambiguous decision must not be resolved by overwriting. A successor equal
	// to the apply's own identifier is rejected: the marker is never cleared, so
	// a self-referential handoff would refuse the apply forever while pointing
	// the operator back at the refused apply itself.
	//
	// Callers recording a takeover mark as soon as the successor durably
	// exists — after its creation, so the marker never names an apply that
	// does not exist, and before any driver acts on it. The marker does not
	// carry that window alone: while the successor is active, a claim on the
	// handed-off apply is refused by the claim-time one-active-apply re-check;
	// the marker is what keeps refusing once the successor settles and that
	// re-check has nothing left to catch. A takeover whose mark never lands —
	// a failed write, or a crash between creating the successor and marking —
	// therefore leaves an apply that becomes startable again once its
	// successor settles; the failed write is logged with both applies named,
	// and neither case is retried.
	//
	// The refusal the marker backs is apply-granular even when the successor
	// took over only part of the apply's work: start is apply-wide and would
	// replay everything, including the part the successor now owns. Work the
	// successor did not take over reaches the database through a fresh
	// dispatch — the marked apply's hold on the database was already released
	// when the marker was earned.
	MarkSuperseded(ctx context.Context, applyID int64, successor string) error

	// CheckLease verifies that an operator apply lease is still current without
	// mutating the apply row.
	CheckLease(ctx context.Context, lease ApplyLease) error

	// ExpireRetryable transitions failed_retryable applies that exhausted their
	// retry budget or recovery freshness window to permanent failed. Returns the
	// applies updated.
	ExpireRetryable(ctx context.Context) ([]*RetryableApplyExpiration, error)

	// FindMissingSummaryComment returns GitHub-backed applies that recently
	// reached a terminal state (including stopped, judged by updated_at since a
	// resumable stop keeps completed_at NULL) but whose progress comment was
	// never followed by a summary comment — no summary marker exists, the
	// marker is superseded (a stop's summary consumed by a resume rotation, so
	// the current terminal state's summary was never posted), or only a claim
	// sentinel stale for longer than SummaryClaimStaleAfter remains from a
	// publisher that crashed before posting. Used by startup reconciliation to
	// post missing summary comments after restarts.
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
	Operation *ApplyOperation
	Tasks     []*Task
}

// RecentAppliesFilter controls recent apply queries for status views.
type RecentAppliesFilter struct {
	Limit       int
	Environment string
	Deployment  string
	States      []string
	// ActiveOnly restricts results to applies that have not reached a terminal
	// state — the ones that still own their target. It is expressed as "not
	// terminal" rather than as a list of live states so that a state the registry
	// does not know counts as active: a caller asking whether a target is busy
	// must not be told it is free because a state was missed.
	ActiveOnly bool
	// UpdatedSince, when set, restricts results to applies whose last activity
	// — the latest lease heartbeat on the apply row or any of its operations —
	// falls at or after this instant. Windowing on activity rather than start
	// time keeps two kinds of applies visible in a window: those that reached a
	// terminal state within it, and those started earlier but still running,
	// including a fanned-out apply whose heartbeats land only on its operations.
	UpdatedSince time.Time
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
// Create and UpsertShardProgress canonicalize repository, database, database
// type, and environment in place before persisting. Update leaves those
// identity fields untouched: its SQL statement writes only state and progress.
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

	// GetByApplyID returns all tasks for an apply, in creation order — the
	// plan's statement order. Apply-level drives execute tasks sequentially in
	// the order returned here, and state aggregation reads the same list.
	GetByApplyID(ctx context.Context, applyID int64) ([]*Task, error)

	// CountByApplyID returns the number of task rows an apply owns, with no
	// shard or operation filtering. It answers "does this apply own any task
	// work at all?" — the invariant behind the operator's claim gate — so a
	// drive can distinguish a genuinely task-less apply from one whose rows a
	// filtered loader did not return.
	CountByApplyID(ctx context.Context, applyID int64) (int64, error)

	// GetByApplyOperationID returns the drive tasks for a single apply_operation,
	// in creation order — the plan's statement order, which the sequential drive
	// executes as-is. Unsharded operations return their per-table tasks. Sharded
	// work operations return the task whose namespace/shard/table matches the
	// operation key so the drive can rebuild its shard selector. Reflected
	// per-shard progress rows that do not match a sharded work operation key are
	// excluded — read them via GetShardProgressByApplyOperationID.
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

	// FindTableOwners returns the pull requests that stored tasks attribute a
	// table to, most recently seen first. The plan path uses it to tell a drop
	// the planning PR proposes from a drop of a table another pull request
	// created and has not merged yet — SchemaBot's schema-first
	// workflow applies from the PR before the code merges, so the live database
	// legitimately carries tables no merged tree describes.
	//
	// Tasks from CLI-originated applies carry no pull request and are excluded;
	// every other state is included, since a task that named the table is an
	// ownership signal regardless of how its apply ended.
	//
	// The result is capped at a recency window rather than returning a table's
	// whole history, because the caller resolves each owner's state against
	// GitHub one at a time.
	FindTableOwners(ctx context.Context, ref TableRef) ([]TableOwner, error)

	// List returns tasks matching the filter criteria.
	List(ctx context.Context, filter TaskFilter) ([]*Task, error)

	// ReapStrandedRetryable hardens to failed the failed_retryable task rows
	// whose parent apply has already settled, returning what it reaped (at
	// most limit rows, oldest first). A failed_retryable task promises a retry, but
	// retries are dispatched by the parent's recovery path: once the parent has
	// settled, no driver will ever pick the row up again, so the promise is
	// dead. Left in place, the row poisons every reader that treats
	// failed_retryable as "a retry is coming" — most critically the control
	// plane's remote-progress snapshot, which copies the row verbatim and reads
	// it as a permanent retryable pause. Such rows are the residue of a partial
	// failure write: a task update that errored mid-transition, or a retryable
	// writer racing a concurrent failure writer.
	//
	// The row keeps its own error message — its failure is real; only the
	// retry promise is retired — so the task is hardened to failed rather than
	// mirroring the parent's state. Only settled parents (completed, failed,
	// cancelled, reverted) that have been quiescent past the retryable-task
	// reaper's own window qualify — longer than the retryable-recovery
	// freshness window (and than ReapStranded's), so a retry admitted at the
	// freshness window's edge, which refreshes the parent heartbeat on claim,
	// can never race the reap. Stopped and failed_retryable parents are
	// resumable, and their failed_retryable tasks belong to the resume/retry
	// path. A row whose apply_operation carries a live lease is left alone
	// whatever its parent says, because a driver holds it. Each write
	// re-verifies the row is still failed_retryable, the parent is still
	// settled, and the operation is still unleased, so a row a concurrent writer
	// advanced or a driver claimed is skipped rather than overwritten. The parent
	// apply row is never touched.
	//
	// One instance reaps per pass, guarded by an advisory lock;
	// ErrStrandedTaskReaperBusy reports that another instance holds it. As with
	// ReapStranded, the lock is an efficiency gate, not a safety one.
	ReapStrandedRetryable(ctx context.Context, limit int) ([]*ReapedTask, error)

	// ReapStrandedActive settles to their parent apply's recorded outcome the
	// task rows still in an active state under a settled parent, returning what
	// it reaped (at most limit rows, oldest first). A driver that records the
	// parent's verdict and exits before closing its task rows leaves them
	// describing work that will never resume: the verdict is final, so nothing
	// revisits the children, and the row reads as live work forever. That is
	// what makes a completed apply render a table still copying, and the only
	// writer that may correct it is one holding a lease — which a reader is not.
	//
	// The row takes the parent's verdict rather than a decided one: every
	// settled parent state is also a terminal task state, and a task whose own
	// outcome was never recorded is never assumed to have succeeded, so under a
	// failed parent it settles failed and carries the parent's explanation when
	// it has none of its own. completed_at is stamped because every settled
	// parent state is non-resumable.
	//
	// failed_retryable rows are excluded: they are ReapStrandedRetryable's, on a
	// far longer window, because the parent's recovery path may still dispatch
	// their retry. Only settled parents (completed, failed, cancelled, reverted)
	// quiescent past the reaper's window qualify — stopped and failed_retryable
	// parents are resumable, and their tasks belong to the resume path.
	//
	// A settled parent is not on its own a promise that nothing is running
	// underneath it: under a fan-out rollout one failed deployment settles the
	// apply while its siblings keep driving, and a sibling drive holds only an
	// operation lease, so it never touches the parent row. What excludes those
	// rows is the lease itself — a row whose apply_operation carries a
	// heartbeated lease belongs to the driver holding it, by the same test a
	// driver applies before taking an operation from a peer. Each write
	// re-verifies the row's state, the parent's, the row's own quiescence, and
	// that the operation is still unleased, so a row a concurrent writer advanced
	// or a driver claimed is skipped rather than overwritten. The parent apply row
	// is never touched.
	//
	// One instance reaps per pass, guarded by an advisory lock;
	// ErrStrandedActiveTaskReaperBusy reports that another instance holds it. As
	// with ReapStranded, the lock is an efficiency gate, not a safety one.
	ReapStrandedActive(ctx context.Context, limit int) ([]*ReapedTask, error)
}

// ErrStrandedTaskReaperBusy reports that another instance holds the stranded
// retryable-task reaper lock, so this pass did no work. It is an expected
// outcome on every instance but one, not a failure.
var ErrStrandedTaskReaperBusy = errors.New("another instance is reaping stranded retryable tasks")

// ErrStrandedActiveTaskReaperBusy reports that another instance holds the
// stranded active-task reaper lock, so this pass did no work. It is an expected
// outcome on every instance but one, not a failure.
var ErrStrandedActiveTaskReaperBusy = errors.New("another instance is reaping stranded active tasks")

// ReapedTask records one failed_retryable task row hardened to failed under a
// settled parent apply, carrying both rows so callers can log what the reaper
// did with the canonical triage attributes.
type ReapedTask struct {
	Task   *Task
	Parent *Apply
}

// TableRef names one table in a single deployment target. Namespace
// is deliberately absent: matching on the table name alone across a database's
// namespaces over-matches, and for the ownership lookup over-matching is the
// safe direction — it annotates a drop rather than presenting it bare.
type TableRef struct {
	Database     string
	DatabaseType string
	Environment  string
	TableName    string
}

// TableOwner is a pull request that stored tasks attribute a table to. LastSeen is the most recent task creation time for that pull request, so
// callers can present the newest attribution first.
type TableOwner struct {
	Repository  string
	PullRequest int
	LastSeen    time.Time
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

	// DeleteByApply removes all comment records for an apply. It is
	// deliberately lease-agnostic: it serves per-apply teardown where the
	// apply row itself is being removed, so no live drive holds a lease that
	// could fence it.
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

	// ClaimSummaryComment atomically claims the right to publish the terminal
	// summary comment for an apply by inserting the summary marker as a claim
	// sentinel (github_comment_id = 0). A superseded marker — a stop's summary
	// consumed by a resume rotation — does not block the claim: it is converted
	// back into a claim sentinel, since the summary it recorded belongs to an
	// earlier terminal state. Exactly one caller wins per terminal state: the
	// winner posts the summary and records the real comment ID via Upsert; every
	// loser skips. The claim — not the apply lease — is the exactly-once
	// authority for the summary, so a writer whose lease was re-claimed (stop
	// reconciliation, resume) can still be beaten to the post without a
	// duplicate. Returns true when this caller won the claim.
	ClaimSummaryComment(ctx context.Context, applyID int64) (bool, error)

	// ReclaimStaleSummaryClaim takes over a summary claim sentinel whose holder
	// crashed between claiming and posting: a sentinel (github_comment_id = 0)
	// not updated for at least SummaryClaimStaleAfter. Ownership transfers by
	// bumping updated_at, so concurrent reclaimers race on the same conditional
	// update and exactly one wins. Returns true when this caller now owns the
	// claim; false when the sentinel is missing, fresh, or already a real
	// comment.
	ReclaimStaleSummaryClaim(ctx context.Context, applyID int64) (bool, error)

	// ReleaseSummaryClaim releases a summary claim this caller won but could not
	// convert into a posted comment, so a later publisher (or startup
	// reconciliation) can retry immediately instead of waiting out the stale-
	// claim window. Deletes only the sentinel form of the marker — a recorded
	// real comment is never released.
	ReleaseSummaryClaim(ctx context.Context, applyID int64) error

	// ClaimProgressCommentAuthority atomically claims — or renews for its
	// current holder — the durable authority to edit the tracked progress
	// comment of an apply whose parent apply lease is legitimately unheld
	// because its work runs under operation leases. The claim is a conditional
	// update on the tracked progress comment row: it succeeds when the row has
	// no recorded observer, when the caller already holds it, or when the
	// recorded observer's heartbeat is older than
	// ProgressCommentAuthorityStaleAfter (a crashed holder). Exactly one of any
	// set of concurrent claimants wins the same handover, so two observers can
	// never both believe they own the comment. Returns true when the caller now
	// holds the authority; false when another observer holds it or no tracked
	// progress comment row exists to claim. Deliberately lease-agnostic — the
	// claim itself is the authority, mirroring the terminal summary claim.
	ClaimProgressCommentAuthority(ctx context.Context, applyID int64, owner string) (bool, error)
}

// ProgressCommentAuthorityStaleAfter is how long the progress-comment
// authority may go without a renewal before its holder is considered gone and
// another observer may take the authority over. Holders renew on every
// admitted GitHub side effect (at least once per progress poll tick), so
// anything older than this window is a stopped observer, not a slow one. It
// matches the apply lease staleness bound so comment ownership hands over on
// the same clock as drive ownership.
const ProgressCommentAuthorityStaleAfter = ApplyLeaseStaleAfter

// SummaryClaimStaleAfter is how long a summary claim sentinel
// (apply_comments row with github_comment_id = 0) may go without an update
// before it is considered abandoned by a crashed publisher and becomes
// reclaimable. Claims are normally held only for the duration of one GitHub
// post, so anything older than this window is a crash, not a slow post.
const SummaryClaimStaleAfter = 2 * time.Minute

// PlanCommentStore tracks plan comments posted on PRs so a newer plan comment
// for the same database can retire the ones it supersedes — minimizing a
// comment whose head an apply owns, deleting one no apply acted on. Rows exist
// only for comments actually posted; minimized_at and deleted_at are set only
// after the corresponding GitHub call succeeded, so an unretired row is always
// retried by the next supersede.
// Insert canonicalizes the provided comment's repository, database, and
// database type in place before persisting. EnvironmentScope is stored as
// given: no query predicate filters on it, and its consumers compare it in Go
// against a scope built from the configured environment names.
type PlanCommentStore interface {
	// Insert stores a newly posted plan comment and sets comment.ID.
	Insert(ctx context.Context, comment *PlanComment) error

	// ListUnretiredForSlot returns the comments neither minimized nor deleted
	// for a (repository, pull_request, database) slot, ordered by id
	// ascending. The caller decides which of them a newly posted comment
	// supersedes.
	ListUnretiredForSlot(ctx context.Context, repo string, pr int, database, databaseType string) ([]*PlanComment, error)

	// ListUnretiredForRepoPR returns the comments neither minimized nor
	// deleted for a whole pull request, across every database, ordered by id
	// ascending. A caller that resolved no database — a delivery that
	// discovers no schema config, or one whose discovery failed — still has
	// to retire the plan comments an earlier head left behind, and has no
	// slot to key.
	ListUnretiredForRepoPR(ctx context.Context, repo string, pr int) ([]*PlanComment, error)

	// MarkMinimized stamps minimized_at after the GitHub minimize call
	// succeeded. An already-minimized row is not an error.
	MarkMinimized(ctx context.Context, id int64) error

	// MarkDeleted stamps deleted_at after the GitHub delete call succeeded.
	// An already-deleted row is not an error.
	MarkDeleted(ctx context.Context, id int64) error
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
	// that owns the dispatch. The write is atomic with its deployment
	// invariant: in one transaction the store locks the apply's operation
	// rows, verifies the operation's deployment records no remote apply id
	// other than the one being stored, and only then writes. Sibling
	// operations of one deployment persist concurrently across the driver
	// pool, so a check outside the writing transaction cannot stop two of
	// them from recording divergent ids. Divergence returns an error wrapping
	// ErrRemoteApplyDeploymentIDConflict.
	SaveExternalID(ctx context.Context, applyID, operationID int64, externalID string) error

	// ApplyIdentifierForRemoteApply returns the identifier of the apply this
	// control plane dispatched as the given remote apply, or "" when it
	// dispatched no such thing. It answers the question an operator asks about
	// someone else's work: the data plane names the change holding a database by
	// its own identifier, which resolves nowhere the operator can reach, and this
	// turns that into the handle their CLI accepts.
	//
	// A remote apply this control plane did not start is the ordinary empty
	// answer, not an error — another control plane or a direct engine run owns
	// it, and there is no handle to offer. The correlation is read from the
	// operation rows because a multi-operation apply has no single authoritative
	// remote identifier; every operation carrying one remote apply id belongs to
	// the same parent, so more than one parent matching means the correlation
	// itself is broken and the store refuses to guess.
	ApplyIdentifierForRemoteApply(ctx context.Context, externalID string) (string, error)

	// SaveEngineResumeState stores opaque engine resume state on the operation.
	SaveEngineResumeState(ctx context.Context, operationID int64, resumeState *EngineResumeState) error

	// GetEngineResumeState returns opaque engine resume state for the operation.
	GetEngineResumeState(ctx context.Context, operationID int64) (*EngineResumeState, error)

	// FindNextApplyOperation atomically claims the next child row that needs
	// attention and rotates a fresh operation lease (owner + token) onto it in
	// the same transaction, returning the row populated with that lease.
	//
	// Pending rows are transitioned to running and stamped with started_at; a
	// stopped row whose parent apply has a pending start or cancel request is
	// claimable and is transitioned to resuming (so the request-gated stopped
	// predicate stops matching once the row is claimed); already-active rows whose
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

	// ReleaseClaim releases an operation lease the calling driver holds but
	// cannot use — typically because the parent apply lease it also needs was
	// transiently unclaimable. It clears the lease fields and backdates the
	// heartbeat past the staleness window so the row is re-claimable on the
	// next poll, instead of sitting on the fresh lease until it goes stale.
	// The row's state is left unchanged: the released row re-enters through
	// the stale-active recovery arm of FindNextApplyOperation, the same path
	// that recovers a crashed driver's work.
	//
	// The write is guarded on the lease token. Reports false when the lease no
	// longer matches (another writer rotated or cleared it), in which case the
	// row has moved on and needs nothing from the caller.
	ReleaseClaim(ctx context.Context, lease OperationLease) (bool, error)

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

	// ReapStranded mirrors the parent's outcome onto pending, unleased operation
	// rows whose parent apply settled long enough ago to be quiescent, returning
	// what it reaped (at most limit rows, oldest first). A pending row under a
	// settled parent describes work that will never run — the rollout's verdict was
	// recorded on the parent — so leaving it pending makes that state mean two
	// different things and hides genuinely stranded work behind harmless history.
	//
	// Only completed, failed, cancelled, and reverted parents qualify. stopped and
	// failed_retryable parents are resumable: their pending rows belong to the
	// resume path, which claims them once the parent is active again. Each write
	// is guarded on the row still being pending and unleased, so a row a driver
	// claimed or advanced in the meantime is skipped rather than overwritten, and
	// the parent apply row is never touched.
	//
	// One instance reaps per pass, guarded by an advisory lock;
	// ErrStrandedReaperBusy reports that another instance holds it. The lock is an
	// efficiency gate, not a safety one: every write is already guarded and
	// idempotent, so concurrent reapers would be correct but would each pay the
	// full scan to find rows the first one already settled.
	ReapStranded(ctx context.Context, limit int) ([]*ReapedOperation, error)
}

// ErrStrandedReaperBusy reports that another instance holds the stranded-operation
// reaper lock, so this pass did no work. It is an expected outcome on every
// instance but one, not a failure.
var ErrStrandedReaperBusy = errors.New("another instance is reaping stranded apply operations")

// ReapedOperation records one operation row settled from its parent apply's
// outcome, carrying both rows so callers can log what the reaper did with the
// canonical triage attributes.
type ReapedOperation struct {
	Operation *ApplyOperation
	Parent    *Apply
}

// ApplyLogStore manages apply log entries for debugging and audit.
// Logs capture state transitions, errors, and events during schema changes.
// Logs are kept forever for audit purposes.
//
// Ordering contract: every read returns entries ordered by created_at
// ascending, with ties on created_at broken by ascending id so entries keep
// their insertion order. Callers rendering log tails (PR comments, the logs
// API) depend on same-timestamp bursts reading in the order they were
// appended, so implementations must provide the tie-break regardless of the
// backing store's timestamp precision.
type ApplyLogStore interface {
	// Append adds a new log entry.
	Append(ctx context.Context, log *ApplyLog) error

	// GetByApply returns all logs for an apply, ordered by created_at
	// ascending with ties broken by ascending id.
	GetByApply(ctx context.Context, applyID int64) ([]*ApplyLog, error)

	// GetRecentByApply returns the newest limit logs for an apply, ordered by
	// created_at ascending (ties broken by ascending id) so the result reads
	// chronologically. The query is bounded — long-running applies can
	// accumulate far more log rows than a caller rendering a tail needs.
	GetRecentByApply(ctx context.Context, applyID int64, limit int) ([]*ApplyLog, error)

	// List returns the newest Limit logs matching the filter criteria,
	// ordered by created_at ascending (ties broken by ascending id) so the
	// result reads chronologically.
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

	// ListSettled returns every control request for an apply that has reached a
	// terminal status, ordered by operation ascending, so the plane that
	// accepted a control RPC can learn whether the operation took effect:
	// accepting a request only queues it. The ordering is a varchar sort and
	// therefore collation-dependent in principle; every ControlOperation value
	// is lowercase ASCII, which all supported dialects order identically.
	ListSettled(ctx context.Context, applyID int64) ([]*ApplyControlRequest, error)

	// RecordRemoteFailure records the terminal failure another plane reported
	// for a control request, and creates the row when the local plane never held
	// one. A locally completed row means the request was queued there, which a
	// later rejection overtakes; a locally pending row is a live request this
	// plane has not forwarded yet, so a settled report necessarily describes a
	// superseded attempt and is ignored rather than dropping the command. It
	// reports whether the stored row changed, so a caller polling the same
	// rejection every tick surfaces it exactly once.
	RecordRemoteFailure(ctx context.Context, req *ApplyControlRequest) (bool, error)

	// ClearRemoteFailure retires a rejection this plane mirrored from another,
	// for use when that plane later reports the same operation succeeded. It
	// only touches a failed row the mirror itself created: rows this plane
	// queued are cleared by their own request lifecycle, and a row with no
	// lifecycle here — an operation this plane only proxies — would otherwise
	// keep warning about a command the operator has since re-issued
	// successfully. It reports whether the stored row changed.
	ClearRemoteFailure(ctx context.Context, applyID int64, operation ControlOperation) (bool, error)
}
