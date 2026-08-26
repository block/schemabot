package tern

import (
	"context"
	"fmt"

	"github.com/block/schemabot/pkg/metrics"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// adoptLiveApplyForDispatch resolves a dispatch into the apply that is already
// running its exact change set, instead of refusing it as a conflict.
//
// A copy that outlives the apply that started it is otherwise unreachable. The
// work is still running on the target and still owns the database, but nothing
// addresses it: re-applying the same schema change mints a fresh identity that
// never resolves to the running work, and the conflict check then refuses it on
// behalf of the very copy the operator is trying to rejoin. Adopting turns that
// dead end into the obvious recovery — apply the same change again and the
// dispatch lands on the work already in flight, keeping every row it has copied.
//
// Adoption is deliberately narrow. It resolves an operator to work that is
// already theirs; it never starts, alters, or reinterprets that work, and it
// never lets a second runner onto a live checkpoint. Every gate below fails
// closed to the existing refusal, which is always a safe outcome: the operator
// still sees which apply holds the database and what clears it.
//
//	dispatch ─▶ conflict check ─▶ blocked ─▶ adoptable?
//	                                          ├─ yes ─▶ resolve into the live apply
//	                                          └─ no  ─▶ refuse (unchanged)
//
// Returns ok=false to decline adoption, leaving the caller's refusal intact.
func (c *LocalClient) adoptLiveApplyForDispatch(ctx context.Context, req *ternv1.ApplyRequest, plan *storage.Plan, scope dispatchScope, blocking blockingTask) (*ternv1.ApplyResponse, bool) {
	apply, ok := c.adoptableApply(req, plan, scope, blocking)
	if !ok {
		return nil, false
	}
	if !c.dispatchMatchesApplyChangeSet(ctx, apply, scope, blocking) {
		return nil, false
	}

	// The dispatch must address the adopted apply through a real operation row,
	// so the caller receives a handle that resolves on the next status or
	// progress call. An identical change set derives an identical operation key,
	// so a missing row means the two dispatches disagree about the work's shape
	// and adoption is not provable.
	operationKey, _, err := operationIdentityForDispatch(scope)
	if err != nil {
		c.logger.Warn("adopt: cannot derive the dispatch's operation identity; the live apply keeps blocking the database",
			append(apply.LogAttrs(), "error", err)...)
		return nil, false
	}
	op, err := c.findApplyOperationByKey(ctx, apply, operationKey)
	if err != nil {
		c.logger.Warn("adopt: failed to load the live apply's operation; the live apply keeps blocking the database",
			append(apply.LogAttrs(), "operation_key", operationKey, "error", err)...)
		return nil, false
	}
	if op == nil {
		c.logger.Warn("adopt: live apply has no operation for this dispatch's key; the live apply keeps blocking the database",
			append(apply.LogAttrs(), "operation_key", operationKey)...)
		return nil, false
	}

	// The apply row was read at the start of the conflict check, and its work can
	// settle while the change sets are compared. Re-read it before answering so a
	// dispatch is never resolved into an apply that terminalized in the meantime
	// and would hand the operator a handle that never moves.
	fresh, err := c.storage.Applies().Get(ctx, apply.ID)
	if err != nil {
		c.logger.Warn("adopt: failed to re-read the live apply before resolving into it; it keeps blocking the database",
			append(apply.LogAttrs(), "error", err)...)
		return nil, false
	}
	if fresh == nil {
		c.logger.Warn("adopt: live apply row disappeared before the dispatch could resolve into it; it keeps blocking the database",
			apply.LogAttrs()...)
		return nil, false
	}
	if state.IsTerminalApplyState(fresh.State) {
		c.logger.Warn("adopt: live apply terminalized while its change set was being compared; it keeps blocking the database",
			fresh.LogAttrs()...)
		return nil, false
	}

	c.logger.Info("adopt: dispatch resolved into the live apply already running its change set",
		append(fresh.LogAttrs(),
			"operation_key", operationKey,
			"operation_deployment", op.Deployment,
			"dispatch_plan_id", plan.PlanIdentifier,
			"dispatch_shard", scope.shard,
			"blocking_task_id", blocking.taskIdentifier)...)
	metrics.RecordRemoteApplyAttach(ctx, req.Database, req.Environment, "adopted")
	return dispatchApplyResponse(fresh, op.ID, operationKey), true
}

// adoptableApply returns the live apply a dispatch may resolve into, or
// ok=false when the conflict is not one adoption can answer.
//
// The gates are about what adoption assumes rather than what the change set
// says: there is one live apply to rejoin, it is still running its work, and
// it is running it against the same database type and environment the dispatch
// names. The dispatch's own environment is authoritative — a stored plan may
// predate the environment it is being applied to.
func (c *LocalClient) adoptableApply(req *ternv1.ApplyRequest, plan *storage.Plan, scope dispatchScope, blocking blockingTask) (*storage.Apply, bool) {
	// A conflict whose apply could not be loaded is the conflict check's
	// fail-closed shape: without the apply there is nothing to prove adoptable.
	if blocking.apply == nil {
		c.logger.Warn("adopt: blocking task's apply could not be loaded; the task keeps blocking the database",
			"task_id", blocking.taskIdentifier, "database", plan.Database)
		return nil, false
	}
	apply := blocking.apply

	// A terminal apply is not running anything to rejoin. Its work may still be
	// alive on the target, but the control plane has stopped driving it, so
	// resolving a dispatch into it would hand back a handle that never moves.
	if state.IsTerminalApplyState(apply.State) {
		c.logger.Warn("adopt: blocking apply is terminal, so there is no live work to resolve into; it keeps blocking the database",
			apply.LogAttrs()...)
		return nil, false
	}

	// A VSchema-only dispatch is a task-less finalizer: it carries no table DDL,
	// so it has no change set to prove identical and cannot stand in for the
	// table work an apply holding the database is running.
	if scope.finalizer {
		c.logger.Info("adopt: dispatch is a task-less finalizer with no change set to match; the live apply keeps blocking the database",
			append(apply.LogAttrs(), "finalizer_namespace", scope.finalizerNamespace)...)
		return nil, false
	}

	if apply.DatabaseType != plan.DatabaseType {
		c.logger.Warn("adopt: blocking apply targets a different database type; it keeps blocking the database",
			append(apply.LogAttrs(), "dispatch_database_type", plan.DatabaseType)...)
		return nil, false
	}
	if apply.Environment != req.Environment {
		c.logger.Warn("adopt: blocking apply targets a different environment; it keeps blocking the database",
			append(apply.LogAttrs(), "dispatch_environment", req.Environment)...)
		return nil, false
	}
	return apply, true
}

// dispatchMatchesApplyChangeSet reports whether the dispatch would admit
// exactly the change set the live apply is already running.
//
// Identity is the whole safety argument. Adoption hands an operator a live row
// copy they did not start in this dispatch, so anything short of an exact match
// would resolve them to work that is not the change they asked for — and, worse,
// would report a schema change as under way that nothing is actually applying.
// A superset, a subset, and a same-table-different-DDL change are all refused
// for that reason, and so is a comparison that could not be performed.
func (c *LocalClient) dispatchMatchesApplyChangeSet(ctx context.Context, apply *storage.Apply, scope dispatchScope, blocking blockingTask) bool {
	dispatched, err := c.driftMultisetFromDispatchScope(scope)
	if err != nil {
		c.logger.Warn("adopt: dispatch change set could not be reconstructed; the live apply keeps blocking the database",
			append(apply.LogAttrs(), "error", err)...)
		return false
	}
	// An empty change set matches every other empty one, so a dispatch with
	// nothing to apply must never be read as agreeing with a live apply.
	if len(dispatched) == 0 {
		c.logger.Warn("adopt: dispatch carries no table changes to match against the live apply; it keeps blocking the database",
			apply.LogAttrs()...)
		return false
	}

	tasks, err := c.storage.Tasks().GetByApplyID(ctx, apply.ID)
	if err != nil {
		c.logger.Warn("adopt: failed to load the live apply's tasks; it keeps blocking the database",
			append(apply.LogAttrs(), "error", err)...)
		return false
	}
	owned, err := c.driftMultisetFromTasks(tasks, scope.shard)
	if err != nil {
		c.logger.Warn("adopt: live apply's change set could not be reconstructed from its tasks; it keeps blocking the database",
			append(apply.LogAttrs(), "error", err)...)
		return false
	}
	if err := compareDriftMultisets(owned, dispatched); err != nil {
		c.logger.Info("adopt: dispatch is a different change set than the live apply is running; it keeps blocking the database",
			append(apply.LogAttrs(),
				"blocking_task_id", blocking.taskIdentifier,
				"dispatch_shard", scope.shard,
				"difference", err.Error())...)
		return false
	}
	return true
}

// driftMultisetFromTasks builds the table DDL multiset an existing apply owns,
// reconstructed from its stored task rows. A task row is the durable record of
// one (table, shard) change the apply admitted, so the rows together are the
// change set that apply is executing — the only representation of it that
// outlives the dispatch which created it.
//
// Rows are counted regardless of task state. An apply that has already finished
// copying one table still owns that table's change, so excluding finished work
// would make a partly-done apply look like it covers less than it does.
//
// shardScope restricts the reconstruction to the shard a dispatch targets, the
// same scoping rule the conflict check applies: a sharded apply's other shards
// are separate physical primaries whose work is not part of this comparison,
// and their reflected per-shard progress rows are not changes the apply
// admitted at dispatch. A non-sharded dispatch scopes to "", the shard every
// non-sharded task carries.
//
// It fails closed on anything it cannot key exactly — a row with no table, no
// recorded operation, or DDL that does not canonicalize to exactly one DDL
// statement — so an apply whose change set cannot be reconstructed is never
// mistaken for one that matches.
func (c *LocalClient) driftMultisetFromTasks(tasks []*storage.Task, shardScope string) (driftChangeMultiset, error) {
	ms := driftChangeMultiset{}
	for _, t := range tasks {
		if t == nil {
			return nil, fmt.Errorf("apply carries a nil task row")
		}
		if t.Shard != shardScope {
			continue
		}
		if t.TableName == "" {
			return nil, fmt.Errorf("task %s records no table", t.TaskIdentifier)
		}
		if t.DDLAction == "" {
			return nil, fmt.Errorf("task %s (table %q) records no DDL operation", t.TaskIdentifier, t.TableName)
		}
		canon, err := canonicalDDLForDrift(t.DDL)
		if err != nil {
			return nil, fmt.Errorf("task %s (table %q): %w", t.TaskIdentifier, t.TableName, err)
		}
		ms[driftChangeKey{c.planNamespace(t.Namespace), t.Shard, t.TableName, t.DDLAction, canon}]++
	}
	return ms, nil
}

// driftMultisetFromDispatchScope builds the table DDL multiset a dispatch would
// admit, keyed to the dispatch's shard exactly as buildDispatchTasks would tag
// its task rows. Keying both sides the way a task row is written is what makes
// a dispatch's change set directly comparable with one reconstructed from an
// existing apply's rows.
//
// It fails closed on the same input driftMultisetFromTasks rejects, so the two
// sides of the comparison are built to the same standard.
func (c *LocalClient) driftMultisetFromDispatchScope(scope dispatchScope) (driftChangeMultiset, error) {
	ms := driftChangeMultiset{}
	for _, ch := range scope.ddlChanges {
		if ch.Table == "" {
			return nil, fmt.Errorf("dispatch carries a change with no table")
		}
		if ch.Operation == "" {
			return nil, fmt.Errorf("dispatch change for table %q records no operation", ch.Table)
		}
		canon, err := canonicalDDLForDrift(ch.DDL)
		if err != nil {
			return nil, fmt.Errorf("dispatch change for table %q: %w", ch.Table, err)
		}
		ms[driftChangeKey{c.planNamespace(ch.Namespace), scope.shard, ch.Table, ch.Operation, canon}]++
	}
	return ms, nil
}
