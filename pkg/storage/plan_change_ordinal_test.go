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

// A sharded keyspace keeps one change per table in its namespace, while each
// shard keeps its own. A shard's changes to a table are listed in the shard's
// order, under the shard's key, so a shard that needs the same statement twice
// with different engine records keeps both.
func TestPlanShardChangesByTableKeepsEachShardsOwnChanges(t *testing.T) {
	const drop = `ALTER TABLE notes DROP CONSTRAINT notes_body_not_null`
	first := TableChange{Table: "notes", DDL: drop, Metadata: map[string]string{"helper": "leftover"}}
	second := TableChange{Table: "notes", DDL: drop, Metadata: map[string]string{"helper": "new"}}
	plan := &Plan{
		Namespaces: map[string]*NamespacePlanData{"ks": {Tables: []TableChange{first}}},
		Shards: []ShardPlan{
			{Namespace: "ks", Shard: "-80", Changes: []TableChange{second}},
			{Namespace: "ks", Shard: "80-", Changes: []TableChange{first, second}},
		},
	}

	byShard := PlanShardChangesByTable(plan)
	assert.Equal(t, []TableChange{second}, byShard[PlanShardChangeTableKey("ks", "-80", "notes")])
	assert.Equal(t, []TableChange{first, second}, byShard[PlanShardChangeTableKey("ks", "80-", "notes")])
	assert.Len(t, byShard, 2)
	assert.Empty(t, PlanShardChangesByTable(nil))
}
