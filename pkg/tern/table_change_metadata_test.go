package tern

import (
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
		got, err := c.taskChangeMetadata(t.Context(), &storage.Apply{ApplyIdentifier: "apply-1", DatabaseType: storage.DatabaseTypePostgres}, task, plan)
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
