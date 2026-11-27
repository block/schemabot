package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// A plan that drops the same constraint twice on one table, around other
// changes, gives each task the position of the change it was built from. Each
// shard's tasks count their own occurrences, a task whose statement the plan
// does not carry keeps 0, and so does every task of a nil plan.
func TestAssignPlanChangeOrdinals(t *testing.T) {
	const (
		drop = `ALTER TABLE notes DROP CONSTRAINT notes_body_not_null`
		add  = `ALTER TABLE notes ADD CONSTRAINT notes_body_not_null CHECK (body IS NOT NULL) NOT VALID`
	)
	plan := &Plan{Namespaces: map[string]*NamespacePlanData{
		"public": {Tables: []TableChange{
			{Table: "notes", DDL: drop},
			{Table: "users", DDL: drop},
			{Table: "notes", DDL: add},
			{Table: "notes", DDL: drop},
		}},
	}}
	task := func(table, shard, ddl string) *Task {
		return &Task{Namespace: "public", TableName: table, Shard: shard, DDL: ddl}
	}
	tasks := []*Task{
		task("notes", "-80", drop),
		task("notes", "80-", drop),
		task("users", "-80", drop),
		task("notes", "-80", add),
		task("notes", "-80", "  "+drop+"\n"),
		task("notes", "80-", drop),
		task("notes", "-80", `ALTER TABLE notes DROP COLUMN legacy`),
		task("notes", "-80", drop),
	}
	AssignPlanChangeOrdinals(plan, tasks)

	got := make([]int, len(tasks))
	for i, task := range tasks {
		got[i] = task.PlanChangeOrdinal
	}
	assert.Equal(t, []int{1, 1, 1, 2, 3, 3, 0, 0}, got,
		"the first and second drop on notes are changes 1 and 3; a third has no change, nor does an unplanned statement")

	unplanned := task("notes", "", drop)
	AssignPlanChangeOrdinals(nil, []*Task{unplanned})
	assert.Zero(t, unplanned.PlanChangeOrdinal)
}

// A sharded keyspace whose shards diverge lists, per shard, only the changes
// that shard still needs. The namespace drops a constraint twice with
// different engine records, and shard -80 needs only the second. Its one task
// runs the second change, not the first copy of the same statement. A shard
// that needs both takes them in order, and a change whose record no plan
// change carries keeps 0.
func TestAssignFromChangesFollowsEachShardsOwnChanges(t *testing.T) {
	const drop = `ALTER TABLE notes DROP CONSTRAINT notes_body_not_null`
	first := map[string]string{"helper": "leftover"}
	second := map[string]string{"helper": "new"}
	plan := &Plan{Namespaces: map[string]*NamespacePlanData{
		"ks": {Tables: []TableChange{
			{Table: "notes", DDL: drop, Metadata: first},
			{Table: "notes", DDL: drop, Metadata: second},
		}},
	}}
	ordinals := NewPlanChangeOrdinals(plan)
	build := func(shard string, changes ...TableChange) []int {
		tasks := make([]*Task, len(changes))
		for i, change := range changes {
			tasks[i] = &Task{Namespace: "ks", TableName: "notes", Shard: shard, DDL: change.DDL}
		}
		ordinals.AssignFromChanges(tasks, changes)
		got := make([]int, len(tasks))
		for i, task := range tasks {
			got[i] = task.PlanChangeOrdinal
		}
		return got
	}

	assert.Equal(t, []int{2}, build("-80", TableChange{Table: "notes", DDL: drop, Metadata: second}),
		"the shard that needs only the second drop runs change 2")
	assert.Equal(t, []int{1, 2}, build("80-",
		TableChange{Table: "notes", DDL: drop, Metadata: first},
		TableChange{Table: "notes", DDL: drop, Metadata: second}))
	assert.Equal(t, []int{0}, build("c0-", TableChange{Table: "notes", DDL: drop, Metadata: map[string]string{"helper": "other"}}),
		"a record no plan change carries matches none of them")
}
