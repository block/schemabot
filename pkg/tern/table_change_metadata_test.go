package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

const metadataTestDrop = `ALTER TABLE "public"."users" DROP COLUMN "nickname"`

func metadataTestPlan() (*storage.Plan, map[string]string) {
	recorded := map[string]string{"postgres_reviewed_drop": `{"kind":"column","name":"nickname"}`}
	return &storage.Plan{ID: 7, PlanIdentifier: "plan-7", Namespaces: map[string]*storage.NamespacePlanData{
		"public": {Tables: []storage.TableChange{
			{Table: "users", DDL: `ALTER TABLE "public"."users" ADD COLUMN "email" text`},
			{Table: "users", DDL: metadataTestDrop, Metadata: recorded},
		}},
	}}, recorded
}

func metadataTestTasks() []*storage.Task {
	return []*storage.Task{
		{TaskIdentifier: "task-1", PlanID: 7, Namespace: "public", TableName: "users", DDL: `ALTER TABLE "public"."users" ADD COLUMN "email" text`, DDLAction: "alter"},
		{TaskIdentifier: "task-2", PlanID: 7, Namespace: "public", TableName: "users", DDL: metadataTestDrop, DDLAction: "alter"},
	}
}

// Every path that hands a planned change to the engine for apply carries the
// metadata the engine recorded at plan time, matched to the task by its
// namespace, table, and DDL: the gRPC dispatch, the sequential drive, and the
// grouped drive and its resume.
func TestApplyPathsCarryPlanTimeEngineMetadata(t *testing.T) {
	plan, recorded := metadataTestPlan()

	t.Run("gRPC dispatch", func(t *testing.T) {
		changes, err := tasksToProtoTableChanges(metadataTestTasks(), plan)
		require.NoError(t, err)
		require.Len(t, changes, 2)
		assert.Empty(t, changes[0].GetMetadata(), "a change the engine recorded nothing for carries nothing")
		assert.Equal(t, recorded, changes[1].GetMetadata())
	})

	t.Run("sequential drive", func(t *testing.T) {
		c := &LocalClient{}
		task := metadataTestTasks()[1]
		got, err := c.taskChangeMetadata(t.Context(), &storage.Apply{ApplyIdentifier: "apply-1"}, task, plan)
		require.NoError(t, err)
		assert.Equal(t, recorded, got)
	})

	t.Run("grouped drive", func(t *testing.T) {
		changes, err := groupedResumeChanges(metadataTestTasks(), plan)
		require.NoError(t, err)
		require.Len(t, changes, 1)
		require.Len(t, changes[0].TableChanges, 2)
		assert.Empty(t, changes[0].TableChanges[0].Metadata)
		assert.Equal(t, recorded, changes[0].TableChanges[1].Metadata)
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

	_, err := tasksToProtoTableChanges(metadataTestTasks(), plan)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read engine metadata for task task-2")

	_, err = groupedResumeChanges(metadataTestTasks(), plan)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read engine metadata for task task-2")
}
