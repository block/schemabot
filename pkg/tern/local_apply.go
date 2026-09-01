package tern

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/engine/spirit"
	"github.com/block/schemabot/pkg/metrics"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// checkActiveTaskConflict verifies there's no active schema change for this
// database that would conflict with a new apply targeting dispatchShard.
// Uses retry loop and engine verification to handle stale storage state.
//
// dispatchShard scopes the conflict to a single shard: a sharded apply is
// dispatched one shard at a time, and different shards of the same database are
// distinct physical primaries that run concurrently by design, so a task on
// another shard is not a conflict. dispatchShard is "" for a non-sharded apply,
// which conflicts with any active task on the database (today's behaviour).
//
// attachApplyID names the keyed apply a sibling dispatch is attaching into (0
// when the dispatch creates a new apply). That apply's own tasks are the
// dispatch's siblings, not conflicts: the apply already holds the database's
// reservation and the driver orders sibling work, so only other applies'
// active work blocks the attach.
//
// The conflict that refused the dispatch is returned alongside the error, so a
// caller that can resolve into the holding apply rather than be refused by it
// has the apply in hand without repeating the scan.
//
// The stopped applies whose resting tasks the check released are returned with
// it: their work is what a dispatch for the same table takes over, and the scan
// that frees the database is the one place that already knows who they are.
//
// environment is the dispatch's own environment. It plays no part in what
// blocks — a namesake's active work on the shared database name still refuses
// the dispatch — but it decides which released holders the dispatch may later
// mark superseded (see markSupersededHolders): only work attributable to the
// same environment and target is the dispatch's to take over.
func (c *LocalClient) checkActiveTaskConflict(ctx context.Context, plan *storage.Plan, environment, dispatchShard string, attachApplyID int64) (blockingTask, []supersededHolder, error) {
	memo := newConflictScanMemo()
	for attempt := range 10 {
		existingTasks, err := c.storage.Tasks().GetByDatabase(ctx, plan.Database)
		if err != nil {
			return blockingTask{}, nil, fmt.Errorf("check existing tasks: %w", err)
		}

		c.logger.Debug("conflict check: found tasks", "count", len(existingTasks), "database", plan.Database, "shard", dispatchShard, "attempt", attempt)

		blocking, released := c.findBlockingTask(ctx, existingTasks, plan, environment, dispatchShard, attachApplyID, memo)
		if !blocking.blocks() {
			return blockingTask{}, released, nil
		}

		// Retry: 10 attempts with 100ms sleep gives 1 second total wait.
		// Handles the race where storage is updated but Spirit hasn't fully finished.
		if attempt < 9 {
			c.logger.Debug("found potentially stale active task, retrying",
				"task_id", blocking.taskIdentifier, "table", blocking.table, "shard", blocking.shard,
				"apply_id", blocking.applyIdentifier(), "apply_state", blocking.applyStateName(),
				"attempt", attempt)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// The refusal is what the operator sees when an apply dies on arrival, so
		// it names the apply holding the database rather than only the task, which
		// on its own gives them nothing to act on.
		return blocking, nil, fmt.Errorf("schema change already in progress for database %q (plan %s): %s",
			plan.Database, plan.PlanIdentifier, blocking.describe())
	}
	return blockingTask{}, nil, nil
}

// findBlockingTask checks if any non-terminal task for this database is truly active.
// Returns the conflict, or a zero blockingTask when no conflict exists, along
// with the stopped applies whose resting tasks it released along the way.
// As a side effect, resolves stale tasks by checking engine state.
//
// dispatchShard scopes the conflict to a single shard (see checkActiveTaskConflict):
// when both the candidate apply and an existing task target a non-empty shard,
// a different shard does not conflict, so a sharded fan-out runs its shards
// concurrently instead of serializing on the first one.
func (c *LocalClient) findBlockingTask(ctx context.Context, tasks []*storage.Task, plan *storage.Plan, environment, dispatchShard string, attachApplyID int64, memo *conflictScanMemo) (blockingTask, []supersededHolder) {
	var released []supersededHolder
	for _, t := range tasks {
		c.logger.Debug("conflict check: checking task", "task_id", t.TaskIdentifier, "state", t.State, "shard", t.Shard, "is_terminal", state.IsTerminalTaskState(t.State))
		if t.DatabaseType != plan.DatabaseType || state.IsTerminalTaskState(t.State) {
			continue
		}

		// A task of the apply this dispatch is attaching into is sibling work
		// under the same reservation, not a conflict (see checkActiveTaskConflict).
		if attachApplyID != 0 && t.ApplyID == attachApplyID {
			c.logger.Debug("conflict check: skipping sibling task of the attach-target apply", "task_id", t.TaskIdentifier, "shard", t.Shard)
			continue
		}

		// A task on a different shard targets a different physical primary, so it
		// does not block this shard's apply. Only same-shard work, or work where
		// either side is non-sharded (database-wide), can conflict.
		if dispatchShard != "" && t.Shard != "" && t.Shard != dispatchShard {
			continue
		}

		// Both resolvers below decide based on the task's parent apply — its
		// state and its lease ownership. Any uncertainty (a storage failure or a
		// missing apply row) means the task cannot be proven resolvable, so it
		// keeps blocking (fail closed).
		apply, ok := c.applyForConflictCheck(ctx, t)
		if !ok {
			return newBlockingTask(t, nil), nil
		}

		// A task of a terminal apply waits for a driver that will never arrive;
		// settle it so it stops blocking the database as phantom active work.
		c.settleOrphanedTask(ctx, t, apply)

		// Settled terminal, the task is done and no longer blocks. Settled to
		// stopped, it stays in play for the resting checks below, which own the
		// takeover of its copy and the superseding of its apply.
		if state.IsTerminalTaskState(t.State) {
			continue
		}

		// A stopped task rests without ending. Nothing reaches for it until a
		// person asks, so it does not hold the database — and its apply's work
		// is what a dispatch for the same table takes over. The decision is
		// storage-only and memoized across the check's retry attempts: the
		// retries exist to absorb engine staleness, which cannot change this
		// answer (see conflictScanMemo).
		resting, decided := memo.resting[t.TaskIdentifier]
		if !decided {
			resting.releases = c.restingTaskReleasesDatabase(ctx, t, apply)
			if resting.releases {
				resting.attributable = c.releasedHolderAttributableToDispatch(ctx, t, apply, plan, environment, memo.operationTargets)
			}
			memo.resting[t.TaskIdentifier] = resting
		}
		if resting.releases {
			if resting.attributable {
				released = append(released, supersededHolder{
					applyID:         apply.ID,
					applyIdentifier: apply.ApplyIdentifier,
					namespace:       t.Namespace,
					table:           t.TableName,
				})
			}
			continue
		}

		// Storage says non-terminal — verify with engine before blocking.
		if c.tryResolveStaleTask(ctx, t, apply, plan.Database) {
			continue // Task was stale; engine confirmed it's done.
		}

		c.logger.Debug("conflict check: task is active", append(t.LogAttrs(),
			"apply_id", apply.ApplyIdentifier, "apply_state", apply.State)...)
		return newBlockingTask(t, apply), nil
	}
	return blockingTask{}, released
}

// conflictScanMemo carries one conflict check's decisions across its retry
// attempts. The retries exist to absorb engine staleness — storage already
// updated, the engine not yet settled — but the resting decision is
// storage-only: re-deciding it on every attempt would count one refusal in the
// ownership-block metrics up to once per attempt and re-read the apply's
// control requests for an answer that does not change within the check. Going
// stale inside the check's window is safe in both directions: a hold that
// lifts is picked up by the dispatch's own retry, and a release that an
// operator command overtakes is re-checked durably when the dispatch tries to
// create its apply against the one-active-apply gate.
//
// operationTargets caches the operation-row targets read while attributing
// released holders, exactly as runningCopyTables caches them across one scan.
type conflictScanMemo struct {
	resting          map[string]restingDecision
	operationTargets map[int64]string
}

// restingDecision is what one conflict check decided about a stopped task:
// whether it rests without holding the database, and — only when it does —
// whether its apply's work is attributable to the dispatch and so eligible to
// be marked superseded.
type restingDecision struct {
	releases     bool
	attributable bool
}

func newConflictScanMemo() *conflictScanMemo {
	return &conflictScanMemo{
		resting:          map[string]restingDecision{},
		operationTargets: map[int64]string{},
	}
}

// blockingTask names the active work that refuses a new apply on a database,
// in the terms an operator needs to clear it: what is being changed, which task
// tracks it, and the apply that owns that task.
//
// apply is nil when it could not be loaded. That task still blocks — the
// conflict check fails closed — so the conflict is reported by task alone
// rather than being suppressed.
type blockingTask struct {
	taskIdentifier string
	table          string
	shard          string
	apply          *storage.Apply
}

// newBlockingTask records the conflicting task, and the apply that owns it when
// that apply could be loaded.
func newBlockingTask(t *storage.Task, apply *storage.Apply) blockingTask {
	return blockingTask{
		taskIdentifier: t.TaskIdentifier,
		table:          t.TableName,
		shard:          t.Shard,
		apply:          apply,
	}
}

// blocks reports whether a conflicting task was found.
func (b blockingTask) blocks() bool { return b.taskIdentifier != "" }

// applyIdentifier and applyStateName name the holding apply for logs, empty
// when it could not be loaded, so a triage line reports the conflict by task
// alone rather than dropping the log or dereferencing a missing apply.
func (b blockingTask) applyIdentifier() string {
	if b.apply == nil {
		return ""
	}
	return b.apply.ApplyIdentifier
}

func (b blockingTask) applyStateName() string {
	if b.apply == nil {
		return ""
	}
	return b.apply.State
}

// describe renders the conflict for an operator: what holds the database, who
// owns it, and what has to happen before the database is free.
//
// The holding change is named by its pull request or its caller, not by the
// engine's apply identifier. That identifier belongs to the data plane alone:
// the control plane resolves an apply only by its own identifier, so offering
// the engine's as the handle sends an operator to a command that refuses them.
// It stays in the logs, where every triage attribute already carries it.
func (b blockingTask) describe() string {
	if b.apply == nil {
		return b.subject() + " is held by a schema change that could not be loaded"
	}

	described := fmt.Sprintf("%s is held by a schema change (%s)%s",
		b.subject(), b.apply.State, b.holder())
	if hold := state.Hold(b.apply.State); hold != "" {
		return described + "; " + hold
	}
	return described
}

// holder names who owns the blocking change, preferring the pull request an
// operator can open over the caller string. Both are empty for a change whose
// provenance was not recorded, which leaves the conflict described by its work
// and its state alone rather than by an identifier that resolves nowhere.
func (b blockingTask) holder() string {
	switch {
	case b.apply.Repository != "" && b.apply.PullRequest > 0:
		return fmt.Sprintf(" on %s#%d", b.apply.Repository, b.apply.PullRequest)
	case b.apply.Caller != "":
		return fmt.Sprintf(" started by %s", b.apply.Caller)
	default:
		return ""
	}
}

// conflict renders the refusal as the structured facts a caller can present on
// its own surfaces: the work being held, the state holding it, and who owns the
// holding change.
//
// The holding apply's own identifier crosses too, but as a lookup key rather
// than as text. A caller that dispatched this work recorded that identifier
// against its own apply, so it can turn the key into a handle that resolves on
// its side. That is the opposite of what describe() omits — this identifier is
// never rendered, it is what lets the caller avoid rendering it.
//
// Returns nil when there is no conflict to report, or when the blocking task's
// apply could not be loaded — the dispatch is still refused (the check fails
// closed), but with nothing proven about the holder there is nothing to render
// beyond the sanitized error the caller already has.
func (b blockingTask) conflict() *ternv1.ApplyConflict {
	if !b.blocks() || b.apply == nil {
		return nil
	}
	return &ternv1.ApplyConflict{
		Table:            b.table,
		Shard:            b.shard,
		BlockingState:    state.NormalizeState(b.apply.State),
		Repository:       b.apply.Repository,
		PullRequest:      int32(b.apply.PullRequest),
		Caller:           b.apply.Caller,
		HolderExternalId: b.apply.ApplyIdentifier,
	}
}

// subject names the work being done, leading with the table an operator
// recognizes and keeping the task identifier as the handle for the CLI. A
// multi-table atomic change records no table, so it is named by task alone —
// but it still names its shard when it has one, because only that shard is
// held and an operator reading the refusal needs to know which.
func (b blockingTask) subject() string {
	switch {
	case b.table == "" && b.shard == "":
		return fmt.Sprintf("task %s", b.taskIdentifier)
	case b.table == "":
		return fmt.Sprintf("shard %s (task %s)", b.shard, b.taskIdentifier)
	case b.shard == "":
		return fmt.Sprintf("table %s (task %s)", b.table, b.taskIdentifier)
	default:
		return fmt.Sprintf("table %s shard %s (task %s)", b.table, b.shard, b.taskIdentifier)
	}
}

// applyForConflictCheck loads the parent apply the conflict-check resolvers
// decide on. Returns ok=false when the apply cannot be loaded or the row is
// missing — the caller must keep the task blocking, because without the apply
// neither the task's orphan status nor its lease ownership can be proven.
func (c *LocalClient) applyForConflictCheck(ctx context.Context, t *storage.Task) (*storage.Apply, bool) {
	apply, err := c.storage.Applies().Get(ctx, t.ApplyID)
	if err != nil {
		c.logger.Warn("conflict check: failed to load the task's apply; the task keeps blocking the database",
			append(t.LogAttrs(), "error", err)...)
		return nil, false
	}
	if apply == nil {
		c.logger.Warn("conflict check: task's apply row is missing; the task keeps blocking the database",
			t.LogAttrs()...)
		return nil, false
	}
	return apply, true
}

// orphanedTaskSettlement returns the state an orphaned task settles to under a
// terminal apply, and whether the task is one that can be settled at all.
//
// Only the states that wait for a driver settle here. Pending and
// failed_retryable work is claimed unprompted, which is precisely what a
// terminal apply guarantees will never happen again, so a task left in either
// one has no way out on its own. Running and its phases may still own live
// engine work, and stopped is already a resting state; both stay with the
// checks that can tell.
//
// A stopped apply settles its retryable work to stopped rather than cancelled.
// The operator asked for a stop, the copy on the target may still be resumable,
// and stopped is the one state both start and the takeover path can act on —
// cancelling would discard a copy the apply never decided to discard. Every
// other terminal apply has ended for good, so its tasks are cancelled.
func orphanedTaskSettlement(taskState, applyState string) (string, bool) {
	switch {
	case state.IsState(taskState, state.Task.Pending):
		// Pending is provably unstarted: no engine work and no checkpoint
		// exists to preserve, so there is nothing for a later start to resume.
		return state.Task.Cancelled, true
	case state.IsState(taskState, state.Task.FailedRetryable):
		if state.IsState(applyState, state.Apply.Stopped) {
			return state.Task.Stopped, true
		}
		return state.Task.Cancelled, true
	default:
		return "", false
	}
}

// orphanedTaskSettlementLogMessage is the durable apply-log entry a settlement
// records. Operators read it on the holding apply to see why a task moved
// without a driver ever touching it.
const orphanedTaskSettlementLogMessage = "Settled orphaned task: its apply was already terminal, so no driver could ever claim it"

// settleOrphanedTask settles a task whose parent apply has already reached a
// terminal state. Such a task is waiting for a driver that will never arrive: a
// terminal apply is not claimable, so nothing picks the task up, and left alone
// it blocks every later apply targeting its database as phantom active work.
//
// The sweep is written against the invariant — a terminal apply's tasks must be
// settled — rather than against the single task state it was first needed for,
// so a state that reaches a terminal apply by some other route is settled too
// instead of moving the hole.
//
// The task is mutated in place and the caller re-reads its state: settled
// terminal it is done, and settled to stopped it stays in play for the resting
// checks that own the takeover of its copy. A refused write leaves the task
// untouched and blocking, because reporting it settled would admit a new apply
// while storage still records the orphan as active work.
func (c *LocalClient) settleOrphanedTask(ctx context.Context, t *storage.Task, apply *storage.Apply) {
	settledState, settles := orphanedTaskSettlement(t.State, apply.State)
	if !settles {
		return
	}
	if !state.IsTerminalApplyState(apply.State) {
		c.logger.Debug("conflict check: orphan candidate's apply is still active; the task blocks normally",
			append(t.LogAttrs(), "apply_id", apply.ApplyIdentifier, "apply_state", apply.State)...)
		return
	}
	// A terminal apply with a fresh lease is a driver mid-settlement of this
	// same apply — its own task writes may still be in flight, and racing them
	// leaves the row to whichever write lands last. This check holds no lease,
	// so it defers: the task keeps blocking, and the sweep settles it on a
	// later check once the lease has aged out.
	if apply.HasFreshLease(time.Now()) {
		c.logger.Debug("conflict check: orphan candidate's apply still holds a fresh lease; a driver may be settling it, so the task blocks normally",
			append(t.LogAttrs(), "apply_id", apply.ApplyIdentifier, "lease_owner", apply.LeaseOwner)...)
		return
	}
	c.logger.Info("conflict check: settling orphaned task; its apply is terminal so no driver will ever claim the task",
		append(t.LogAttrs(), "apply_id", apply.ApplyIdentifier, "apply_state", apply.State, "settled_state", settledState)...)

	previous := *t
	now := time.Now()
	t.State = settledState
	t.UpdatedAt = now
	// Every settled state is at rest, so the row must not carry a frozen ETA
	// or render as paused with no copy in flight (the same clearing
	// persistTaskStateTransition does). The rows this sweep repairs are
	// precisely the ones written by paths that never got to clear them.
	t.ETASeconds = 0
	t.Throttled = false
	t.ThrottleReason = ""
	// A settlement that ends the task records why it ended. A settlement to
	// stopped keeps the engine's own failure message instead: the change is
	// resumable, and that message is why the copy stopped where it did.
	if state.IsTerminalTaskState(settledState) {
		t.ErrorMessage = "Task orphaned: its apply reached a terminal state before the task could be driven"
		t.CompletedAt = &now
	}
	if err := c.storage.Tasks().Update(ctx, t); err != nil {
		*t = previous
		c.logger.Error("conflict check: failed to persist orphaned task settlement; the task keeps blocking the database",
			append(t.LogAttrs(), "apply_id", apply.ApplyIdentifier, "settled_state", settledState, "error", err)...)
		return
	}
	taskID := t.ID
	c.logApplyEvent(ctx, apply.ID, &taskID, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
		orphanedTaskSettlementLogMessage, previous.State, settledState)
}

// restingTaskReleasesDatabase reports whether a stopped task has stopped
// holding its database.
//
// A task holds the database only while a driver will pick it up on its own:
// running work is being driven now, and pending or failed_retryable work is
// claimed unprompted. Stopped is the one state where a task still exists and
// nothing reaches for it until a person asks. That is what makes it safe to
// ignore, and it is exactly what a terminal-task test cannot express, because
// the same fact that makes it inert also makes it resumable.
//
// Three conditions must hold, each closing off a way a driver could still
// arrive:
//
//	apply is terminal        a claimable apply is work someone will drive
//	no fresh lease           a live driver is mid-settlement of this apply
//	no pending start/cancel  an operator already asked, and the claim
//	                         predicate admits a stopped apply for either
//
// Uncertainty keeps the hold: a control-request read that fails cannot prove no
// driver is coming, so the task keeps blocking.
func (c *LocalClient) restingTaskReleasesDatabase(ctx context.Context, t *storage.Task, apply *storage.Apply) bool {
	if !state.IsState(t.State, state.Task.Stopped) {
		// Every other non-terminal task state is either being driven now or
		// waiting for a driver, so it stays with the engine-backed checks.
		return false
	}

	if !state.IsTerminalApplyState(apply.State) {
		c.logger.Debug("conflict check: stopped task's apply is still claimable; the task blocks normally",
			append(t.LogAttrs(), "apply_id", apply.ApplyIdentifier, "apply_state", apply.State)...)
		return false
	}

	// A terminal apply with a fresh lease is a driver mid-settlement of this
	// same apply, so the task falls through to tryResolveStaleTask, which
	// checks the lease again and owns the fresh_lease ownership-block record —
	// recording it here too would count one refusal twice.
	if apply.HasFreshLease(time.Now()) {
		c.logger.Debug("conflict check: stopped task's apply still holds a fresh lease; the engine-backed checks decide whether it blocks",
			append(t.LogAttrs(), "apply_id", apply.ApplyIdentifier, "lease_owner", apply.LeaseOwner)...)
		return false
	}

	operation, err := c.pendingDriverRequest(ctx, apply)
	if err != nil {
		c.logger.Warn("conflict check: failed to read the stopped apply's control requests; the task keeps blocking the database",
			append(t.LogAttrs(), "apply_id", apply.ApplyIdentifier, "error", err)...)
		metrics.RecordConflictCheckOwnershipBlock(ctx, t.Database, t.DatabaseType, "control_request_unreadable")
		return false
	}
	if operation != "" {
		c.logger.Info("conflict check: stopped task's apply has an operator command waiting for a driver; the task keeps blocking until that command is delivered",
			append(t.LogAttrs(), "apply_id", apply.ApplyIdentifier, "pending_operation", string(operation))...)
		metrics.RecordConflictCheckOwnershipBlock(ctx, t.Database, t.DatabaseType, "pending_control_request")
		return false
	}

	c.logger.Info("conflict check: stopped task rests with no driver coming for it; it no longer holds the database",
		append(t.LogAttrs(), "apply_id", apply.ApplyIdentifier, "apply_state", apply.State)...)
	return true
}

// releasedHolderAttributableToDispatch reports whether a released resting
// task's work provably belongs to the environment and target this dispatch is
// for. Task rows are keyed by database name alone, which is not a target: the
// name can be shared by this deployment's staging and production databases
// (see runningCopyTables). Releasing the hold is safe either way — a resting
// task holds nothing regardless of whose it is — but the superseded marker is
// a write-once, permanent refusal of start on the holder, so a holder that
// cannot be attributed to this dispatch's own environment and target must not
// be marked: refusing a namesake's apply forever is worse than leaving its
// marker to the dispatch that actually meets its copy. Every unattributable
// shape — a mismatched or absent environment, a target that cannot be
// resolved, a failed attribution read — fails toward not marking, the same
// direction taskRunsOnPlanTarget fails for plan disclosures.
func (c *LocalClient) releasedHolderAttributableToDispatch(ctx context.Context, t *storage.Task, apply *storage.Apply, plan *storage.Plan, environment string, operationTargets map[int64]string) bool {
	if !c.taskDescribesPlanTarget(t, environment) {
		c.logger.Info("conflict check: released resting task belongs to a namesake environment, so this dispatch takes over none of its work",
			append(t.LogAttrs(), "apply_id", apply.ApplyIdentifier, "dispatch_environment", environment)...)
		return false
	}
	onTarget, err := c.taskRunsOnPlanTarget(ctx, t, plan.Target, operationTargets)
	if err != nil {
		c.logger.Warn("conflict check: failed to attribute a released resting task to a target, so this dispatch will not mark its apply superseded",
			append(t.LogAttrs(), "apply_id", apply.ApplyIdentifier, "plan_target", plan.Target, "error", err)...)
		return false
	}
	if !onTarget {
		c.logger.Info("conflict check: released resting task runs on another target, so this dispatch takes over none of its work",
			append(t.LogAttrs(), "apply_id", apply.ApplyIdentifier, "plan_target", plan.Target)...)
		return false
	}
	return true
}

// pendingDriverRequest names the operator command that will bring a driver back
// to a stopped apply, empty when none is pending. Start and cancel are the two
// the apply claim predicate admits on a stopped apply: a start resumes it and a
// cancel is delivered by a drive, so either one means a driver arrives without
// anyone asking again.
func (c *LocalClient) pendingDriverRequest(ctx context.Context, apply *storage.Apply) (storage.ControlOperation, error) {
	requests := c.storage.ControlRequests()
	if requests == nil {
		// A storage without a control request store cannot answer whether a
		// command is waiting, and an unanswerable question is not a "no".
		return "", fmt.Errorf("read pending requests for apply %s: control request store is not configured", apply.ApplyIdentifier)
	}

	for _, operation := range []storage.ControlOperation{storage.ControlOperationStart, storage.ControlOperationCancel} {
		request, err := requests.GetPending(ctx, apply.ID, operation)
		if err != nil {
			return "", fmt.Errorf("read pending %s request for apply %s: %w", operation, apply.ApplyIdentifier, err)
		}
		if request != nil {
			return operation, nil
		}
	}
	return "", nil
}

// tryResolveStaleTask checks the engine to see if a non-terminal task is actually done.
// If the engine reports a terminal state, or reports no active work for a task that
// storage believes is in-flight, the task is updated in storage and no longer blocks.
// Resting tasks (Stopped, FailedRetryable) are left untouched.
//
// The engine probe is in-memory and database-scoped: it reports this process's
// last run on the database, not the task's actual cross-process state. The
// task's parent apply lease decides whether that memory is authoritative — a
// fresh lease means a live driver owns the work and the task keeps blocking,
// and a terminal report is only trusted when the last lease belongs to this
// process (the completing process's own report).
// Returns true if the task was resolved (no longer blocking).
func (c *LocalClient) tryResolveStaleTask(ctx context.Context, t *storage.Task, apply *storage.Apply, database string) bool {
	eng := c.getEngine()
	if eng == nil {
		c.logger.Error("tryResolveStaleTask: engine is nil", t.LogAttrs()...)
		return false
	}

	if apply.HasFreshLease(time.Now()) {
		c.logger.Info("conflict check: task's apply is actively driven; the task keeps blocking until its lease owner settles it",
			append(t.LogAttrs(), "apply_id", apply.ApplyIdentifier, "lease_owner", apply.LeaseOwner)...)
		metrics.RecordConflictCheckOwnershipBlock(ctx, t.Database, t.DatabaseType, "fresh_lease")
		return false
	}

	// The raw target credentials (no namespace mapping) are safe here only
	// because Spirit's Progress is purely in-memory and never queries by
	// request database or connection schema. An engine whose Progress inspects
	// the database must resolve credentials per task (credentialsForTask)
	// before this probe, or under schema overrides it would address the
	// canonical name instead of the physical schema.
	//
	// The task identifier rides along for engines that key progress by apply
	// identity (postgres): a probe about work the engine is still running
	// must see its live state and keep blocking, while an unrecognized
	// identity reads the idle sentinel and resolves as abandoned. Engines
	// that ignore identity (Spirit) or require resume metadata instead
	// (PlanetScale) behave exactly as before.
	result, err := eng.Progress(ctx, &engine.ProgressRequest{
		Database:    database,
		Credentials: c.credentials(),
		ResumeState: &engine.ResumeState{MigrationContext: t.TaskIdentifier},
	})
	if err != nil {
		// result may be nil when err is non-nil, so it must not be dereferenced here.
		c.logger.Warn("conflict check: engine progress failed", append(t.LogAttrs(), "err", err)...)
		return false
	}
	c.logger.Debug("conflict check: engine progress", "task_id", t.TaskIdentifier, "engine_state", result.State, "engine_message", result.Message)

	// Engine says terminal — update storage and unblock.
	// IMPORTANT: Only trust terminal states, NOT "No active schema change".
	// "No active schema change" just means Spirit has no runningSchemaChange,
	// which could mean completed, never started, or crashed.
	if result.State.IsTerminal() {
		// A terminal report for work last leased to another process is this
		// process's memory of an older run on the same database, not the
		// completing driver's own report — stamping it would mark someone
		// else's task done with state that says nothing about it. Leave the
		// task blocking until driver stale-claim recovery settles it.
		if apply.LeaseOwner != "" && !storage.LeaseOwnedByThisProcess(apply.LeaseOwner) {
			c.logger.Warn("conflict check: engine reports terminal state, but the apply's last lease belongs to another process; the task keeps blocking until driver recovery settles it",
				append(t.LogAttrs(), "apply_id", apply.ApplyIdentifier, "lease_owner", apply.LeaseOwner,
					"engine_state", result.State, "engine_message", result.Message)...)
			metrics.RecordConflictCheckOwnershipBlock(ctx, t.Database, t.DatabaseType, "foreign_terminal_report")
			return false
		}
		c.logger.Info("conflict check: engine reports terminal state",
			"task_id", t.TaskIdentifier, "engine_state", result.State,
			"engine_message", result.Message, "storage_state", t.State)
		now := time.Now()
		t.CompletedAt = &now
		c.transitionTaskState(ctx, t, 0, engineStateToStorage(result.State), "")
		return true
	}

	// The engine has no active work. For in-flight states this means the task was
	// abandoned (e.g. a server crash) and must be failed so it stops blocking.
	// Resting states (Stopped, FailedRetryable) also have no active engine work,
	// but that is expected — Spirit keeps the checkpoint until an operator resumes
	// or retries. Failing them here would destroy resumable work and void the
	// operator retry budget, so leave them untouched and let the conflict/lock
	// logic decide whether the new apply proceeds.
	if result.Message == "No active schema change" {
		if !state.IsInFlightTaskState(t.State) {
			c.logger.Debug("conflict check: leaving resting task untouched (no active engine work expected)",
				"task_id", t.TaskIdentifier, "storage_state", t.State)
			return false
		}
		c.logger.Info("conflict check: cleaning up stale task (no active schema change in engine)",
			"task_id", t.TaskIdentifier, "storage_state", t.State, "started_at", t.StartedAt)
		now := time.Now()
		t.ErrorMessage = "Task abandoned: engine has no active schema change (server may have crashed)"
		t.CompletedAt = &now
		c.transitionTaskState(ctx, t, 0, state.Task.Failed, "")
		return true
	}

	return false
}

// logApplyEvent appends a log entry for an apply operation.
func (c *LocalClient) logApplyEvent(ctx context.Context, applyID int64, taskID *int64, level, eventType, source, message string, oldState, newState string) {
	log := &storage.ApplyLog{
		ApplyID:   applyID,
		TaskID:    taskID,
		Level:     level,
		EventType: eventType,
		Source:    source,
		Message:   message,
		OldState:  oldState,
		NewState:  newState,
		CreatedAt: time.Now(),
	}
	if err := c.storage.ApplyLogs().Append(ctx, log); err != nil {
		c.logger.Warn("failed to log apply event", "error", err, "event", eventType, "event_message", message)
	}
}

// logEngineResumeOnce records a timeline event the first time a drive claim
// observes the engine reattached to a durable checkpoint instead of starting
// the copy fresh. One event per drive claim is intentional: every claim that
// resumes (pod restart, lease handover, operator start after a stop) is a
// real engine resume the operator should see on the timeline. eventLogged is
// the per-drive latch; the durable checkpoint itself lives in the engine.
func (c *LocalClient) logEngineResumeOnce(ctx context.Context, logger *slog.Logger, apply *storage.Apply, resumed bool, eventLogged *bool) {
	if !resumed || *eventLogged {
		return
	}
	*eventLogged = true
	const msg = "Engine resumed from checkpoint; row copy continues from durable progress"
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventInfo, storage.LogSourceSchemaBot,
		msg, "", "")
	logger.Info("engine resumed from checkpoint")
}

// setupSpiritLogging wires up Spirit's log callback to route engine logs to the apply_logs table.
// Returns a cleanup function that must be deferred.
func (c *LocalClient) setupSpiritLogging(ctx context.Context, apply *storage.Apply, tasks []*storage.Task) func() {
	spiritEng, ok := c.spiritEngine.(*spirit.Engine)
	if !ok {
		return func() {}
	}
	spiritEng.SetLogCallback(c.spiritApplyLogFunc(ctx, apply, tasks))
	return func() { spiritEng.SetLogCallback(nil) }
}

// spiritApplyLogFunc builds the callback that records one Spirit log line in
// the apply log stream. It attributes each line to its task by table name and
// embeds the table in the stored message so operators can tell interleaved
// lines apart during multi-table applies.
func (c *LocalClient) spiritApplyLogFunc(ctx context.Context, apply *storage.Apply, tasks []*storage.Task) func(level slog.Level, tableName, msg string) {
	taskByTable := make(map[string]*storage.Task)
	for _, task := range tasks {
		taskByTable[task.TableName] = task
	}

	return func(level slog.Level, tableName, msg string) {
		logLevel := storage.LogLevelInfo
		if level >= slog.LevelWarn {
			logLevel = storage.LogLevelWarn
		}
		if level >= slog.LevelError {
			logLevel = storage.LogLevelError
		}
		// Embed the table in the stored message so it survives every log
		// surface — CLI output, deployment log fetches, and PR comment log
		// folds render the message text only.
		if tableName != "" {
			msg = fmt.Sprintf("[%s] %s", tableName, msg)
		}
		// Run-level lines (or lines spanning several tables) carry no single
		// task's table name; attribute those to the apply, not to an
		// arbitrary task.
		var taskID *int64
		if task := taskByTable[tableName]; task != nil {
			id := task.ID
			taskID = &id
		}
		c.logApplyEvent(ctx, apply.ID, taskID, logLevel, storage.LogEventInfo, storage.LogSourceSpirit, msg, "", "")
	}
}

// transitionTaskState updates a task's state, persists it, and optionally logs a state transition.
// Fields like CompletedAt, StartedAt, ErrorMessage, or progress must be set on the task BEFORE calling this.
// A persistence failure is logged, not returned: callers on best-effort
// progress paths keep driving and the next poll retries the write. Callers
// whose control flow depends on the write durably landing use
// persistTaskStateTransition directly and fail closed on its error.
func (c *LocalClient) transitionTaskState(ctx context.Context, task *storage.Task, applyID int64, newState string, logMsg string) {
	if err := c.persistTaskStateTransition(ctx, task, applyID, newState, logMsg); err != nil {
		c.logger.Error("failed to update task state", append(task.LogAttrs(), "error", err)...)
	}
}

// persistTaskStateTransition updates a task's state, persists it, and — once the
// write has landed — optionally records the transition in the apply's durable
// log. A storage failure (including a lease-guarded write refused because the
// drive's lease was lost to a peer) is returned without recording the log
// event, so the durable log never claims a transition the task row does not
// carry, and the task is rolled back to the values it arrived with, so nothing
// downstream reads a state transition that did not happen. The rollback is to
// the caller's own pre-call task rather than to storage: a caller that
// refreshed display fields such as progress before calling keeps them, since
// only the state gates control flow.
func (c *LocalClient) persistTaskStateTransition(ctx context.Context, task *storage.Task, applyID int64, newState string, logMsg string) error {
	previous := *task
	oldState := task.State
	task.State = newState
	task.UpdatedAt = time.Now()
	// An ETA and throttle state are only meaningful while an engine is
	// actively driving the task and refreshing them. Clear them when the task
	// comes to rest (terminal, stopped, retryable, pending) so the stored row
	// never carries a frozen estimate or renders as paused with no copy in
	// flight; a resumed or retried task gets fresh figures from the first
	// engine poll.
	if !state.IsInFlightTaskState(newState) {
		task.ETASeconds = 0
		task.Throttled = false
		task.ThrottleReason = ""
	}
	if err := c.storage.Tasks().Update(ctx, task); err != nil {
		*task = previous
		return fmt.Errorf("update task %s state %s -> %s: %w", task.TaskIdentifier, oldState, newState, err)
	}
	if logMsg != "" && applyID > 0 {
		taskID := task.ID
		c.logApplyEvent(ctx, applyID, &taskID, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
			logMsg, oldState, newState)
	}
	return nil
}

// markTasksRunning sets DDL tasks to running state with a start timestamp.
func (c *LocalClient) markTasksRunning(ctx context.Context, tasks []*storage.Task) {
	now := time.Now()
	for _, task := range tasks {
		task.State = state.Task.Running
		task.StartedAt = &now
		task.UpdatedAt = now
		if err := c.storage.Tasks().Update(ctx, task); err != nil {
			c.logger.Error("failed to update task state", append(task.LogAttrs(), "error", err)...)
		}
	}
}

// runWithRecovery wraps an apply function with panic recovery so a single panic
// doesn't crash the entire process. On panic, all tasks and the apply are marked failed.
func (c *LocalClient) runWithRecovery(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			errMsg := fmt.Sprintf("panic in apply goroutine: %v", r)
			c.logger.Error(errMsg, apply.LogAttrs()...)
			c.failApplyWithTasks(ctx, apply, tasks, errMsg)
		}
	}()
	fn()
}

// groupedApplyMode classifies the grouped-apply strategy for a drive, for logs
// and operator-facing descriptions. It reads DeferCutover from the effective
// options map (which may carry an automatic barrier-park decision, see
// effectiveCopyDriveOptions) rather than from apply.GetOptions(), so an
// operation-scoped copy drive that must park at the cutover barrier takes the
// atomic-cutover label. Whether a drive groups at all is
// storage.GroupsEngineExecution's call; this only names the engine-specific
// strategy behind a grouping it selected.
func groupedApplyMode(apply *storage.Apply, options map[string]string) string {
	opts := storage.ApplyOptionsFromMap(options)
	if !storage.GroupsEngineExecution(apply.DatabaseType, opts.DeferCutover) {
		return "grouped_engine_apply"
	}
	if apply.DatabaseType == storage.DatabaseTypeVitess {
		return "vitess_deploy_request"
	}
	return "spirit_atomic_cutover"
}

func groupedApplyModeDescription(apply *storage.Apply, options map[string]string) string {
	switch groupedApplyMode(apply, options) {
	case "spirit_atomic_cutover":
		return "Spirit atomic cutover"
	case "vitess_deploy_request":
		return "Vitess deploy request"
	default:
		return "grouped engine apply"
	}
}

func (c *LocalClient) usesGroupedApply(apply *storage.Apply, options map[string]string) bool {
	return storage.GroupsEngineExecution(apply.DatabaseType, storage.ApplyOptionsFromMap(options).DeferCutover)
}

func (c *LocalClient) setApplyCancel(cancel context.CancelFunc) uint64 {
	c.cancelMu.Lock()
	c.cancelApplyGeneration++
	generation := c.cancelApplyGeneration
	c.cancelApply = cancel
	c.cancelMu.Unlock()
	return generation
}

func (c *LocalClient) clearApplyCancel(generation uint64) {
	c.cancelMu.Lock()
	if c.cancelApplyGeneration == generation {
		c.cancelApply = nil
	}
	c.cancelMu.Unlock()
}

func (c *LocalClient) currentApplyCancel() applyCancelHandle {
	c.cancelMu.Lock()
	defer c.cancelMu.Unlock()
	return applyCancelHandle{generation: c.cancelApplyGeneration, cancel: c.cancelApply}
}

func (c *LocalClient) cancelApplyHandle(handle applyCancelHandle) {
	if handle.cancel != nil {
		handle.cancel()
	}
	c.cancelMu.Lock()
	if c.cancelApplyGeneration == handle.generation {
		c.cancelApply = nil
	}
	c.cancelMu.Unlock()
}

func (c *LocalClient) runApplyExecution(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, plan *storage.Plan, options map[string]string, releaseAtCutoverBarrier bool) {
	if c.usesGroupedApply(apply, options) {
		c.runWithRecovery(ctx, apply, tasks, func() {
			c.executeGroupedApply(ctx, apply, tasks, plan, options, releaseAtCutoverBarrier)
		})
		return
	}

	c.runWithRecovery(ctx, apply, tasks, func() {
		c.executeApplySequential(ctx, apply, tasks, plan, options)
	})
}

// executeGroupedApply runs all DDLs in one engine operation. For Spirit with
// defer_cutover, this is atomic cutover; for Vitess, this is one deploy request.

// deriveOverallState determines the overall state from a list of tasks.
// Priority order:
//  1. Active work: CUTTING_OVER, then the least-advanced active phase
//     (RUNNING, CATCHING_UP, CHECKSUMMING, POST_CHECKSUM — the post-copy
//     phases surfacing only once every table has started), then
//     WAITING_FOR_CUTOVER once nothing is still working
//  2. FAILED - at least one task failed (CANCELLED tasks also indicate failure)
//  3. FAILED_RETRYABLE - operator recovery may retry failed task work
//  4. PENDING - more work queued
//  5. STOPPED - apply was stopped (even if some tasks completed)
//  6. COMPLETED - all tasks completed successfully
func deriveOverallState(tasks []*storage.Task) string {
	if len(tasks) == 0 {
		return state.Task.Pending
	}

	var hasRunning, hasCatchingUp, hasChecksumming, hasPostChecksum, hasWaitingForCutover, hasCuttingOver bool
	var hasPending, hasStopped, hasFailed, hasRetryableFailed, hasCancelled, hasCompleted, hasRevertWindow bool

	for _, t := range tasks {
		switch t.State {
		case state.Task.Running:
			hasRunning = true
		case state.Task.CatchingUp:
			hasCatchingUp = true
		case state.Task.Checksumming:
			hasChecksumming = true
		case state.Task.PostChecksum:
			hasPostChecksum = true
		case state.Task.WaitingForCutover:
			hasWaitingForCutover = true
		case state.Task.CuttingOver:
			hasCuttingOver = true
		case state.Task.Pending:
			hasPending = true
		case state.Task.Stopped:
			hasStopped = true
		case state.Task.Failed:
			hasFailed = true
		case state.Task.FailedRetryable:
			hasRetryableFailed = true
		case state.Task.Cancelled:
			hasCancelled = true
		case state.Task.Completed:
			hasCompleted = true
		case state.Task.RevertWindow:
			hasRevertWindow = true
		}
	}

	// Active work, mirroring state.DeriveApplyState: once any table starts
	// its cutover the apply is transitioning; otherwise surface the
	// least-advanced active phase — while any table still copies rows the
	// apply is running, the post-copy phases surface only once every table
	// has started and is draining or verifying, and waiting_for_cutover only
	// when nothing is still working. A queued table still has its whole copy
	// ahead of it, so naming a sibling's post-copy phase would overstate
	// progress.
	switch {
	case hasCuttingOver:
		return state.Task.CuttingOver
	case hasRunning:
		return state.Task.Running
	case hasPending && (hasCatchingUp || hasChecksumming || hasPostChecksum):
		return state.Task.Running
	case hasCatchingUp:
		return state.Task.CatchingUp
	case hasChecksumming:
		return state.Task.Checksumming
	case hasPostChecksum:
		return state.Task.PostChecksum
	case hasWaitingForCutover:
		return state.Task.WaitingForCutover
	}
	if hasFailed || hasCancelled {
		// Cancelled implies a prior task failed (sequential mode), so overall is failed.
		// For Vitess cancellation (user-initiated), the apply state is set directly.
		return state.Task.Failed
	}
	if hasRetryableFailed {
		return state.Task.FailedRetryable
	}
	if hasPending {
		return state.Task.Pending
	}
	if hasStopped {
		return state.Task.Stopped
	}
	if hasRevertWindow {
		return state.Task.RevertWindow
	}
	if hasCompleted {
		return state.Task.Completed
	}

	// Fallback to first task's state
	return tasks[0].State
}

// deriveApplyPhase returns the apply state transition from an engine event.
// Returns empty string if the event is informational (no state transition).
func deriveApplyPhase(event engine.ApplyEvent) string {
	return event.NewState
}

// applyEventStateTransition updates an apply's state based on an engine event.
// Skips the write if the state hasn't changed. On DB write failure, rolls back
// the in-memory state so the next event with the same NewState retries.
// Returns the new state if a transition occurred, or empty string if skipped.
// The logger is expected to carry the apply's identity attributes already
// bound, so the failure line appends only the mutable snapshot.
func applyEventStateTransition(apply *storage.Apply, event engine.ApplyEvent, updateFn func(*storage.Apply) error, logger *slog.Logger) string {
	oldState := apply.State
	newState := deriveApplyPhase(event)
	if newState == "" || newState == oldState {
		return ""
	}
	apply.State = newState
	apply.UpdatedAt = time.Now()
	if err := updateFn(apply); err != nil {
		logger.Error("failed to update apply phase", append(apply.MutableLogAttrs(), "error", err)...)
		apply.State = oldState
		return ""
	}
	return newState
}
