package api

import (
	"log/slog"
	"sort"
	"time"

	"github.com/block/schemabot/pkg/storage"
)

// runsTableByTable reports whether an apply of the per-member shape rolls out
// table by table: some deployment addresses more than one target, and no
// member's plan changes a VSchema (see memberPlanChangesVSchema).
func runsTableByTable(keys memberOperationKeys, members []applyMember) bool {
	return len(keys.multiTargetDeployments) > 0 && !memberPlanChangesVSchema(members)
}

// memberPlanChangesVSchema reports whether any member's plan changes a
// VSchema. A VSchema change rides inside a member's one work operation, which
// applies it with the member's tables, so a rollout carrying one keeps each
// member's operation whole rather than split it across steps that would each
// apply the document, or leave it to whichever step ran first.
func memberPlanChangesVSchema(members []applyMember) bool {
	for _, member := range members {
		if len(member.Plan.VSchemaNamespaces()) > 0 {
			return true
		}
	}
	return false
}

// tableSteps lists the tables of a rollout in the order it runs them, each
// table a step every target finishes before any target starts the next: the
// apply plan's tables in plan order, then the tables only other members' plans
// change, sorted, so a member planned against its own live schema adds its
// extra tables after the reviewed ones in an order every build agrees on. A
// table's position in the result is its step, counted from 1.
//
// A step is a table name, not a (namespace, table) pair. Targets that each
// hold their own namespaces hold the same table under different namespaces,
// and it is that table, across every namespace of every target, that lands
// before the next one starts. A target holding the table in two namespaces
// runs both in its one operation for the step.
func tableSteps(applyPlan *storage.Plan, members []applyMember) []string {
	var steps []string
	seen := map[string]bool{}
	for _, change := range applyTaskChanges(applyPlan) {
		if !seen[change.Table] {
			seen[change.Table] = true
			steps = append(steps, change.Table)
		}
	}
	var extra []string
	for _, member := range members {
		for _, change := range applyTaskChanges(member.Plan) {
			if !seen[change.Table] {
				seen[change.Table] = true
				extra = append(extra, change.Table)
			}
		}
	}
	sort.Strings(extra)
	return append(steps, extra...)
}

// buildTableStepOperationGroups lays a multi-target rollout out table by
// table: one work operation per (target, table step), stamped with its step
// and keyed by it behind the target ("orders-002/step-2"). Operations are
// created step by step and, within a step, in member order, so the claim's
// creation order is (step, member): the step gate holds a table until every
// target has finished the one before it, cutover_policy orders the targets
// within a step, and cutovers run in the same order. Two statements on one
// table, or one table in two of a target's namespaces, are one operation with
// two tasks. A member whose plan does not change a table has no operation in
// that step, so nothing waits on it there.
//
// A member with nothing left to run still belongs to the rollout. It gets one
// operation in the first step, keyed like its siblings so a deployment's keys
// keep one shape, and settleConvergedMemberOperations records it as already
// settled.
func buildTableStepOperationGroups(
	plan *storage.Plan,
	members []applyMember,
	keys memberOperationKeys,
	environment string,
	applyOpts storage.ApplyOptions,
	cutoverPolicy string,
	onFailure string,
	now time.Time,
) ([]*storage.ApplyOperationWithTasks, error) {
	steps := tableSteps(plan, members)
	stepOf := make(map[string]int, len(steps))
	for i, table := range steps {
		stepOf[table] = i + 1
	}
	changesByMember := make([]map[int][]storage.TableChange, len(members))
	for m, member := range members {
		changesByMember[m] = map[int][]storage.TableChange{}
		for _, change := range applyTaskChanges(member.Plan) {
			step := stepOf[change.Table]
			changesByMember[m][step] = append(changesByMember[m][step], change)
		}
	}

	var groups []*storage.ApplyOperationWithTasks
	for step := 1; step <= max(len(steps), 1); step++ {
		for m, member := range members {
			changes := changesByMember[m][step]
			convergedPlaceholder := step == 1 && len(changesByMember[m]) == 0
			if len(changes) == 0 && !convergedPlaceholder {
				continue
			}
			operationKey, err := keys.qualify(member, storage.RolloutStepOperationKey(step))
			if err != nil {
				return nil, err
			}
			operation := newPendingApplyOperation(member, plan, operationKey, cutoverPolicy, onFailure, now)
			operation.RolloutStep = step
			groups = append(groups, &storage.ApplyOperationWithTasks{
				Operation: operation,
				Tasks:     buildApplyTasks(member.Plan, changes, environment, applyOpts, "", now),
			})
		}
	}
	settleConvergedMemberOperations(groups, now)
	return groups, nil
}

// logRolloutShape records how a multi-target rollout was laid out: table by
// table, with how many steps, or member by member and why. An operator reading a rollout that runs every table of a
// target at once can tell from this which case it was.
func logRolloutShape(logger *slog.Logger, plan *storage.Plan, environment string, members []applyMember, groups []*storage.ApplyOperationWithTasks, shardedFanout bool) {
	if len(newMemberOperationKeys(members).multiTargetDeployments) == 0 {
		return
	}
	attrs := []any{
		"plan_id", plan.PlanIdentifier,
		"database", plan.Database,
		"database_type", plan.DatabaseType,
		"environment", environment,
		"member_count", len(members),
		"operation_group_count", len(groups),
	}
	steps := 0
	for _, group := range groups {
		steps = max(steps, group.Operation.RolloutStep)
	}
	switch {
	case steps > 0:
		logger.Info("createStoredApply: queueing a multi-target rollout table by table", append(attrs, "table_steps", steps)...)
	case shardedFanout:
		logger.Info("createStoredApply: queueing a multi-target rollout member by member: its plans fan out per shard", attrs...)
	case memberPlanChangesVSchema(members):
		logger.Info("createStoredApply: queueing a multi-target rollout member by member: a member's plan changes a VSchema, which applies with its member's tables", attrs...)
	default:
		logger.Info("createStoredApply: queueing a multi-target rollout member by member: its plans change no table to step through", attrs...)
	}
}
