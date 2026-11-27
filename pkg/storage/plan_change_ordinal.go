package storage

import (
	"strings"
)

// PlanChangeOrdinals records on each task which of a plan's changes it runs,
// as that change's 1-based position among the plan's changes to the same
// namespace and table. A shard's task is numbered within its shard's own
// changes instead (see PlanShardChangesByTable), since a sharded namespace
// keeps one change per table while its shards can each need several. A plan can hold the same statement more than once on
// one table, for example a PostgreSQL SET NOT NULL that first drops a
// leftover helper constraint and drops the new helper again at the end. The
// statement text alone cannot tell those changes apart, and they can carry
// different engine metadata, so the drive reads a task's metadata from the
// change at its ordinal.
//
// It indexes the plan once, so a builder that splits one plan's tasks into
// many groups assigns every group from the same index.
type PlanChangeOrdinals struct {
	positions map[string][]int
}

// NewPlanChangeOrdinals indexes plan's changes by namespace, table, and
// statement.
func NewPlanChangeOrdinals(plan *Plan) *PlanChangeOrdinals {
	return &PlanChangeOrdinals{positions: planChangePositions(plan)}
}

// Assign sets each task's PlanChangeOrdinal. tasks must list each table's
// tasks in the order of the plan's changes, as every task builder does, and
// must hold all of a table's tasks for a shard, since occurrences are counted
// within the call. Within one namespace, table, and shard, the k-th task
// carrying a statement is matched to the k-th change on that table with the
// same statement. A task whose statement no change carries keeps ordinal 0,
// which the drive treats as unknown and matches by statement.
func (o *PlanChangeOrdinals) Assign(tasks []*Task) {
	if o == nil || len(o.positions) == 0 || len(tasks) == 0 {
		return
	}
	seen := map[string]int{}
	for _, task := range tasks {
		if task == nil {
			continue
		}
		statement := strings.TrimSpace(task.DDL)
		occurrenceKey := planChangeKey(task.Namespace, task.TableName, task.Shard, statement)
		occurrence := seen[occurrenceKey]
		seen[occurrenceKey] = occurrence + 1
		candidates := o.positions[planChangeKey(task.Namespace, task.TableName, "", statement)]
		if occurrence < len(candidates) {
			task.PlanChangeOrdinal = candidates[occurrence]
		}
	}
}

// AssignPlanChangeOrdinals assigns tasks their ordinals in plan, for a builder
// that assigns one plan's tasks in a single call. See PlanChangeOrdinals.
func AssignPlanChangeOrdinals(plan *Plan, tasks []*Task) {
	NewPlanChangeOrdinals(plan).Assign(tasks)
}

// PlanChangesByTable returns plan's changes grouped by namespace and table, in
// the order the plan lists them, so the change at a task's ordinal is
// PlanChangesByTable(plan)[key][ordinal-1]. The key is PlanChangeTableKey.
func PlanChangesByTable(plan *Plan) map[string][]TableChange {
	byTable := map[string][]TableChange{}
	if plan == nil {
		return byTable
	}
	for namespace, data := range plan.Namespaces {
		if data == nil {
			continue
		}
		for _, change := range data.Tables {
			if change.Namespace == "" {
				change.Namespace = namespace
			}
			key := PlanChangeTableKey(change.Namespace, change.Table)
			byTable[key] = append(byTable[key], change)
		}
	}
	return byTable
}

// PlanShardChangesByTable returns each shard's own changes grouped by
// namespace, shard, and table, in the order the shard lists them. A shard's
// task runs the change at its ordinal in this list:
// PlanShardChangesByTable(plan)[PlanShardChangeTableKey(...)][ordinal-1].
// The namespace's own list cannot serve a shard: a sharded namespace keeps
// only the first change it saw per table, and two shards can need the same
// statement with different engine records.
func PlanShardChangesByTable(plan *Plan) map[string][]TableChange {
	byShard := map[string][]TableChange{}
	if plan == nil {
		return byShard
	}
	for _, shard := range plan.Shards {
		for _, change := range shard.Changes {
			namespace := change.Namespace
			if namespace == "" {
				namespace = shard.Namespace
			}
			key := PlanShardChangeTableKey(namespace, shard.Shard, change.Table)
			byShard[key] = append(byShard[key], change)
		}
	}
	return byShard
}

// PlanShardChangeTableKey is the key PlanShardChangesByTable groups a
// shard's changes to one table under.
func PlanShardChangeTableKey(namespace, shard, table string) string {
	return planChangeKey(namespace, table, shard, "")
}

// PlanChangeTableKey is the key PlanChangesByTable groups a table's changes
// under.
func PlanChangeTableKey(namespace, table string) string {
	return namespace + "\x00" + table
}

// planChangePositions maps each namespace, table, and statement to the 1-based
// positions, among that table's changes, of the changes carrying it.
func planChangePositions(plan *Plan) map[string][]int {
	positions := map[string][]int{}
	for _, changes := range PlanChangesByTable(plan) {
		for i, change := range changes {
			key := planChangeKey(change.Namespace, change.Table, "", strings.TrimSpace(change.DDL))
			positions[key] = append(positions[key], i+1)
		}
	}
	return positions
}

func planChangeKey(namespace, table, shard, statement string) string {
	return namespace + "\x00" + table + "\x00" + shard + "\x00" + statement
}
