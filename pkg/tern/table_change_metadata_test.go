package tern

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

const (
	metadataTestAdd  = `ALTER TABLE "public"."users" ADD COLUMN "email" text`
	metadataTestDrop = `ALTER TABLE "public"."users" DROP COLUMN "nickname"`
)

func metadataTestRecord() map[string]string {
	return map[string]string{"postgres_reviewed_drop": `{"kind":"column","name":"nickname"}`}
}

func metadataTestPlanOf(tables ...storage.TableChange) *storage.Plan {
	return &storage.Plan{ID: 7, PlanIdentifier: "plan-7", Namespaces: map[string]*storage.NamespacePlanData{
		"public": {Tables: tables},
	}}
}

func metadataTestPlan() (*storage.Plan, map[string]string) {
	recorded := metadataTestRecord()
	return metadataTestPlanOf(
		storage.TableChange{Table: "users", DDL: metadataTestAdd},
		storage.TableChange{Table: "users", DDL: metadataTestDrop, Metadata: recorded},
	), recorded
}

func metadataTestTasks() []*storage.Task {
	return []*storage.Task{
		{TaskIdentifier: "task-1", PlanID: 7, Namespace: "public", TableName: "users", DDL: metadataTestAdd, DDLAction: "alter"},
		{TaskIdentifier: "task-2", PlanID: 7, Namespace: "public", TableName: "users", DDL: metadataTestDrop, DDLAction: "alter"},
	}
}

func metadataTestCanon(t *testing.T) StatementCanonicalizer {
	t.Helper()
	canon, err := StatementCanonicalizerForDatabaseType(storage.DatabaseTypePostgres, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	return canon
}

// Every path that hands a planned change to the engine for apply carries the
// metadata the engine recorded at plan time, matched to the task by its
// namespace, table, and DDL: the gRPC dispatch, the sequential drive, and the
// grouped drive and its resume.
func TestApplyPathsCarryPlanTimeEngineMetadata(t *testing.T) {
	plan, recorded := metadataTestPlan()
	canon := metadataTestCanon(t)

	t.Run("gRPC dispatch", func(t *testing.T) {
		changes, err := tasksToProtoTableChanges(metadataTestTasks(), plan, canon)
		require.NoError(t, err)
		require.Len(t, changes, 2)
		assert.Empty(t, changes[0].GetMetadata(), "a change the engine recorded nothing for carries nothing")
		assert.Equal(t, recorded, changes[1].GetMetadata())
	})

	t.Run("sequential drive", func(t *testing.T) {
		c := &LocalClient{logger: slog.New(slog.DiscardHandler)}
		task := metadataTestTasks()[1]
		got, err := c.taskChangeMetadata(t.Context(), &storage.Apply{ApplyIdentifier: "apply-1", DatabaseType: storage.DatabaseTypePostgres}, task, plan, nil)
		require.NoError(t, err)
		assert.Equal(t, recorded, got)
	})

	t.Run("grouped drive", func(t *testing.T) {
		changes, err := groupedResumeChanges(metadataTestTasks(), plan, canon)
		require.NoError(t, err)
		require.Len(t, changes, 1)
		require.Len(t, changes[0].TableChanges, 2)
		assert.Empty(t, changes[0].TableChanges[0].Metadata)
		assert.Equal(t, recorded, changes[0].TableChanges[1].Metadata)
	})

	t.Run("rollout step's whole plan", func(t *testing.T) {
		changes := planToProtoTableChanges(plan)
		require.Len(t, changes, 2)
		assert.Empty(t, changes[0].GetMetadata())
		assert.Equal(t, recorded, changes[1].GetMetadata(), "a member that builds its plan from a step's dispatch keeps the reviewed record")
	})
}

// A resumed or synced task can carry a deployment's rendering of the reviewed
// statement, qualified with its own physical schema, while the stored plan
// keeps the reviewed text. The task still receives the record the operator
// reviewed, and a different statement on the same table still receives none.
func TestPlanChangeMetadataMatchesARenderingOfTheReviewedStatement(t *testing.T) {
	plan, recorded := metadataTestPlan()
	index := newPlanChangeMetadata(plan, metadataTestCanon(t))

	rendered := &storage.Task{Namespace: "public", TableName: "users", DDL: `ALTER TABLE "tenant_a"."users" DROP COLUMN "nickname"`}
	got, err := index.forTask(rendered)
	require.NoError(t, err)
	assert.Equal(t, recorded, got)

	other := &storage.Task{Namespace: "public", TableName: "users", DDL: `ALTER TABLE "public"."users" DROP COLUMN "legacy"`}
	got, err = index.forTask(other)
	require.NoError(t, err)
	assert.Nil(t, got, "a record is never borrowed from a different statement")

	byText, err := newPlanChangeMetadata(plan, nil).forTask(rendered)
	require.NoError(t, err)
	assert.Nil(t, byText, "without a dialect only the reviewed text matches")
}

func TestPlanChangeMetadataLookup(t *testing.T) {
	recorded := metadataTestRecord()
	task := func(namespace, table, ddl string) *storage.Task {
		return &storage.Task{Namespace: namespace, TableName: table, DDL: ddl}
	}

	t.Run("returns a copy of the metadata of the change the task runs", func(t *testing.T) {
		index := newPlanChangeMetadata(metadataTestPlanOf(
			storage.TableChange{Table: "users", DDL: metadataTestAdd},
			storage.TableChange{Table: "users", DDL: metadataTestDrop, Metadata: recorded},
		), nil)
		got, err := index.forTask(task("public", "users", metadataTestDrop))
		require.NoError(t, err)
		assert.Equal(t, recorded, got)

		got["postgres_reviewed_drop"] = "changed"
		again, err := index.forTask(task("public", "users", metadataTestDrop))
		require.NoError(t, err)
		assert.Equal(t, metadataTestRecord(), again, "the caller gets a copy, never the stored map")
	})

	t.Run("returns nil when no change matches or the match carries none", func(t *testing.T) {
		index := newPlanChangeMetadata(metadataTestPlanOf(
			storage.TableChange{Table: "users", DDL: metadataTestAdd},
			storage.TableChange{Table: "users", DDL: metadataTestDrop, Metadata: recorded},
		), nil)
		for name, args := range map[string][3]string{
			"other namespace": {"app", "users", metadataTestDrop},
			"other table":     {"public", "orders", metadataTestDrop},
			"other DDL":       {"public", "users", `ALTER TABLE "public"."users" DROP COLUMN "legacy"`},
			"no metadata":     {"public", "users", metadataTestAdd},
		} {
			got, err := index.forTask(task(args[0], args[1], args[2]))
			require.NoError(t, err, name)
			assert.Nil(t, got, name)
		}
	})

	t.Run("a nil plan answers with no metadata", func(t *testing.T) {
		got, err := newPlanChangeMetadata(nil, nil).forTask(task("public", "users", metadataTestDrop))
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("identical duplicates resolve to their shared metadata", func(t *testing.T) {
		index := newPlanChangeMetadata(metadataTestPlanOf(
			storage.TableChange{Table: "users", DDL: metadataTestDrop, Metadata: recorded},
			storage.TableChange{Table: "users", DDL: metadataTestDrop, Metadata: metadataTestRecord()},
		), nil)
		got, err := index.forTask(task("public", "users", metadataTestDrop))
		require.NoError(t, err)
		assert.Equal(t, recorded, got)
	})
}

// A plan holding two changes with the same namespace, table, and DDL but
// different engine metadata cannot say which one a task was created from, so
// the dispatch refuses rather than hand the engine either record.
func TestApplyPathsRefuseAmbiguousEngineMetadata(t *testing.T) {
	plan, _ := metadataTestPlan()
	ns := plan.Namespaces["public"]
	ns.Tables = append(ns.Tables, storage.TableChange{
		Table: "users", DDL: metadataTestDrop, Metadata: map[string]string{"postgres_reviewed_drop": `{"kind":"column","name":"other"}`},
	})
	canon := metadataTestCanon(t)

	_, err := tasksToProtoTableChanges(metadataTestTasks(), plan, canon)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read engine metadata for task task-2")
	assert.Contains(t, err.Error(), `plan plan-7 holds more than one change to table "users" in namespace "public"`)

	_, err = groupedResumeChanges(metadataTestTasks(), plan, canon)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read engine metadata for task task-2")
}

// A sequential drive reads a plan's metadata through one cache, so a task that
// names a plan other than the drive's loads and indexes that plan once, however
// many attempts the drive makes.
func TestSequentialDriveIndexesEachPlanOnce(t *testing.T) {
	plan, recorded := metadataTestPlan()
	loads := 0
	store := &fakePlanStore{getByIDFn: func(id int64) (*storage.Plan, error) {
		loads++
		require.Equal(t, plan.ID, id)
		return plan, nil
	}}
	c := newPlanMaterializeClient(store)
	apply := &storage.Apply{ApplyIdentifier: "apply-1", DatabaseType: storage.DatabaseTypePostgres}
	cache := c.newPlanMetadataCache(apply)
	drivePlan := &storage.Plan{ID: 9, PlanIdentifier: "plan-9"}
	task := metadataTestTasks()[1]

	for range 3 {
		got, err := c.taskChangeMetadata(t.Context(), apply, task, drivePlan, cache)
		require.NoError(t, err)
		assert.Equal(t, recorded, got)
	}
	assert.Equal(t, 1, loads, "the task's plan is loaded once per drive")
}

// A PostgreSQL SET NOT NULL re-planned after a failed attempt first drops the
// helper constraint the failure left behind, then adds, validates, and uses a
// new helper, and drops it again last. The two drops are the same statement,
// but only the first drops a constraint the operator reviewed, so only it
// carries the reviewed-drop record. Each task records which change it runs,
// and every apply path hands each drop its own change's metadata.
func TestApplyPathsTellApartRepeatedStatementsOnOneTable(t *testing.T) {
	const (
		dropHelper = `ALTER TABLE "public"."notes" DROP CONSTRAINT "notes_body_not_null"`
		addHelper  = `ALTER TABLE "public"."notes" ADD CONSTRAINT "notes_body_not_null" CHECK ("body" IS NOT NULL) NOT VALID`
		validate   = `ALTER TABLE "public"."notes" VALIDATE CONSTRAINT "notes_body_not_null"`
		setNotNull = `ALTER TABLE "public"."notes" ALTER COLUMN "body" SET NOT NULL`
	)
	reviewed := map[string]string{"postgres_reviewed_drop": `{"kind":"constraint","name":"notes_body_not_null"}`}
	plan := metadataTestPlanOf(
		storage.TableChange{Table: "notes", DDL: dropHelper, Metadata: reviewed},
		storage.TableChange{Table: "notes", DDL: addHelper},
		storage.TableChange{Table: "notes", DDL: validate},
		storage.TableChange{Table: "notes", DDL: setNotNull},
		storage.TableChange{Table: "notes", DDL: dropHelper},
	)
	tasks := make([]*storage.Task, 0, 5)
	for i, change := range plan.Namespaces["public"].Tables {
		tasks = append(tasks, &storage.Task{
			TaskIdentifier: fmt.Sprintf("task-%d", i+1), PlanID: plan.ID,
			Namespace: "public", TableName: "notes", DDL: change.DDL, DDLAction: "alter",
		})
	}
	storage.AssignPlanChangeOrdinals(plan, tasks)
	canon := metadataTestCanon(t)

	t.Run("gRPC dispatch", func(t *testing.T) {
		changes, err := tasksToProtoTableChanges(tasks, plan, canon)
		require.NoError(t, err)
		require.Len(t, changes, 5)
		assert.Equal(t, reviewed, changes[0].GetMetadata(), "the leftover helper's drop keeps its reviewed record")
		assert.Empty(t, changes[4].GetMetadata(), "the new helper's cleanup drop carries none")
	})

	t.Run("sequential drive", func(t *testing.T) {
		c := &LocalClient{logger: slog.New(slog.DiscardHandler)}
		apply := &storage.Apply{ApplyIdentifier: "apply-1", DatabaseType: storage.DatabaseTypePostgres}
		first, err := c.taskChangeMetadata(t.Context(), apply, tasks[0], plan, nil)
		require.NoError(t, err)
		assert.Equal(t, reviewed, first)
		last, err := c.taskChangeMetadata(t.Context(), apply, tasks[4], plan, nil)
		require.NoError(t, err)
		assert.Empty(t, last)
	})

	t.Run("grouped drive", func(t *testing.T) {
		changes, err := groupedResumeChanges(tasks, plan, canon)
		require.NoError(t, err)
		require.Len(t, changes, 1)
		require.Len(t, changes[0].TableChanges, 5)
		assert.Equal(t, reviewed, changes[0].TableChanges[0].Metadata)
		assert.Empty(t, changes[0].TableChanges[4].Metadata)
	})
}

// A task's ordinal names the change it was created from. When the change at
// that position is missing or carries another statement, the plan is not the
// one the task was created from, and the lookup refuses rather than hand the
// engine another change's record.
func TestPlanChangeMetadataRefusesAnOrdinalThatNamesAnotherChange(t *testing.T) {
	plan, _ := metadataTestPlan()
	index := newPlanChangeMetadata(plan, metadataTestCanon(t))

	_, err := index.forTask(&storage.Task{Namespace: "public", TableName: "users", DDL: metadataTestDrop, PlanChangeOrdinal: 3})
	require.Error(t, err)
	assert.Equal(t, `plan plan-7 has 2 change(s) to table "users" in namespace "public", but the task runs change 3`, err.Error())

	_, err = index.forTask(&storage.Task{Namespace: "public", TableName: "users", DDL: metadataTestDrop, PlanChangeOrdinal: 1})
	require.Error(t, err)
	assert.Equal(t, `plan plan-7 change 1 to table "users" in namespace "public" carries a different statement than the task`, err.Error())

	rendered := &storage.Task{Namespace: "public", TableName: "users", DDL: `ALTER TABLE "tenant_a"."users" DROP COLUMN "nickname"`, PlanChangeOrdinal: 2}
	got, err := index.forTask(rendered)
	require.NoError(t, err)
	assert.Equal(t, metadataTestRecord(), got, "a deployment's rendering of the change's statement still matches")
}

// A sharded plan keeps one change per table in its namespace, so a shard
// whose change list repeats a statement with a different record is found only
// in that shard's own list. Shard -80 drops only the new helper, while 80-
// drops a leftover helper first and the new one after. Each task gets the
// record of the change its own shard planned, and an unsharded task is still
// matched within the namespace.
func TestPlanChangeMetadataMatchesAShardTaskWithinItsShard(t *testing.T) {
	const dropHelper = `ALTER TABLE "ks"."notes" DROP CONSTRAINT "notes_body_not_null"`
	leftover := map[string]string{"helper": "leftover"}
	fresh := map[string]string{"helper": "new"}
	first := storage.TableChange{Table: "notes", DDL: dropHelper, Metadata: leftover}
	second := storage.TableChange{Table: "notes", DDL: dropHelper, Metadata: fresh}
	plan := &storage.Plan{
		ID: 7, PlanIdentifier: "plan-7",
		Namespaces: map[string]*storage.NamespacePlanData{"ks": {Tables: []storage.TableChange{first}}},
		Shards: []storage.ShardPlan{
			{Namespace: "ks", Shard: "-80", Changes: []storage.TableChange{second}},
			{Namespace: "ks", Shard: "80-", Changes: []storage.TableChange{first, second}},
		},
	}
	index := newPlanChangeMetadata(plan, nil)
	task := func(shard string, ordinal int) *storage.Task {
		return &storage.Task{Namespace: "ks", TableName: "notes", Shard: shard, DDL: dropHelper, PlanChangeOrdinal: ordinal}
	}

	for _, tc := range []struct {
		name string
		task *storage.Task
		want map[string]string
	}{
		{"-80 change 1", task("-80", 1), fresh},
		{"80- change 1", task("80-", 1), leftover},
		{"80- change 2", task("80-", 2), fresh},
		{"unsharded change 1", task("", 1), leftover},
	} {
		got, err := index.forTask(tc.task)
		require.NoError(t, err, tc.name)
		assert.Equal(t, tc.want, got, tc.name)
	}

	_, err := index.forTask(task("-80", 2))
	require.Error(t, err)
	assert.Equal(t, `plan plan-7 has 1 change(s) to table "notes" in namespace "ks", but the task runs change 2`, err.Error())
}
