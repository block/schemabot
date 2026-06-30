package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/statement"
)

// A sharded re-plan repeats the same table across shards (and across keyspaces),
// each with its own DDL. replanShardTableDDL must key by (namespace, shard,
// table) so one shard's — or one keyspace's — remaining diff never reconciles
// another's task. Keying by less than the full tuple would collapse these
// entries and conflate tasks.
func TestReplanShardTableDDLKeysPerNamespaceAndShard(t *testing.T) {
	ddlA := "ALTER TABLE `mutes` ADD INDEX (`created_at`)"
	ddlB := "ALTER TABLE `mutes` ADD INDEX (`updated_at`)" // 80- has drifted differently
	ddlC := "ALTER TABLE `mutes` ADD INDEX (`deleted_at`)" // a second keyspace, same shard+table
	result := &engine.PlanResult{
		Changes: []engine.SchemaChange{
			{Namespace: "ks1", Shard: engine.Shard{Name: "-80"}, TableChanges: []engine.TableChange{{Table: "mutes", DDL: ddlA}}},
			{Namespace: "ks1", Shard: engine.Shard{Name: "80-"}, TableChanges: []engine.TableChange{{Table: "mutes", DDL: ddlB}}},
			{Namespace: "ks2", Shard: engine.Shard{Name: "-80"}, TableChanges: []engine.TableChange{{Table: "mutes", DDL: ddlC}}},
		},
	}

	got := replanShardTableDDL(result)

	require.Len(t, got, 3, "same table across shards and keyspaces must produce three distinct keys")
	assert.Equal(t, ddlA, got[shardTableKey{namespace: "ks1", shard: "-80", table: "mutes"}])
	assert.Equal(t, ddlB, got[shardTableKey{namespace: "ks1", shard: "80-", table: "mutes"}])
	assert.Equal(t, ddlC, got[shardTableKey{namespace: "ks2", shard: "-80", table: "mutes"}], "the same shard+table in another keyspace is not conflated")
}

// For a non-sharded engine the shard name is empty, so keying degrades to
// (namespace, table) and matches the pre-sharding lookup.
func TestReplanShardTableDDLNonShardedDegradesToTable(t *testing.T) {
	ddl := "ALTER TABLE `mutes` ADD INDEX (`created_at`)"
	result := &engine.PlanResult{
		Changes: []engine.SchemaChange{
			{Namespace: "commerce", TableChanges: []engine.TableChange{{Table: "mutes", DDL: ddl}}},
		},
	}

	got := replanShardTableDDL(result)

	require.Len(t, got, 1)
	assert.Equal(t, ddl, got[shardTableKey{namespace: "commerce", table: "mutes"}])
}

// On resume, replanAndFilterTasks recomputes each deployment's delta against its
// live schema and overwrites task.DDL with it. verifyReplannedTaskDDL is the
// gate that keeps a drifted deployment from silently applying that recomputed
// DDL: it must pass only when the re-plan matches what the task was reviewed
// with, tolerating incidental formatting, and fail closed otherwise.
func TestVerifyReplannedTaskDDL(t *testing.T) {
	task := func(reviewed string) *storage.Task {
		return &storage.Task{
			TaskIdentifier: "task_abc123",
			Namespace:      "commerce",
			Shard:          "-80",
			TableName:      "users",
			DDLAction:      "alter",
			DDL:            reviewed,
		}
	}

	t.Run("matching re-plan passes", func(t *testing.T) {
		tk := task("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		err := verifyReplannedTaskDDL(tk, "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		require.NoError(t, err)
	})

	t.Run("incidental formatting differences pass", func(t *testing.T) {
		// Unquoted identifiers and extra whitespace canonicalize to the same form
		// as the reviewed DDL, so they are not drift.
		tk := task("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		err := verifyReplannedTaskDDL(tk, "ALTER TABLE   users   ADD COLUMN email varchar(255)")
		require.NoError(t, err)
	})

	t.Run("divergent re-plan fails closed", func(t *testing.T) {
		// The deployment drifted: the re-plan would apply a different column type
		// than the one reviewed. This unreviewed DDL must be refused.
		tk := task("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		err := verifyReplannedTaskDDL(tk, "ALTER TABLE `users` ADD COLUMN `email` varchar(100)")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "drifted from the reviewed plan")
		assert.Contains(t, err.Error(), "commerce[-80].users/alter")
	})

	t.Run("empty reviewed DDL is left to the caller", func(t *testing.T) {
		// Only legacy synthetic VSchema tasks carry no reviewed DDL; they have no
		// reference to compare against and are handled downstream, not here.
		tk := task("")
		err := verifyReplannedTaskDDL(tk, "ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		require.NoError(t, err)
	})

	t.Run("unparseable re-planned DDL fails closed", func(t *testing.T) {
		tk := task("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		err := verifyReplannedTaskDDL(tk, "this is not valid sql")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "re-planned DDL for task task_abc123")
	})
}

// replanAndFilterTasks recomputes the delta against live schema and overwrites
// each still-needed task's DDL with it. When the deployment has drifted, that
// recomputed DDL diverges from what was reviewed; the resume must fail closed
// rather than silently apply the unreviewed DDL.
func TestReplanAndFilterTasks_FailsClosedOnDrift(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	drifted := &engine.PlanResult{
		Changes: []engine.SchemaChange{{
			Namespace: "testapp",
			TableChanges: []engine.TableChange{{
				Table:     "users",
				Operation: statement.StatementAlterTable,
				DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(100)",
			}},
		}},
	}
	c := newPlanMaterializeClientWithPlan(store, drifted)

	apply := &storage.Apply{Database: "testapp"}
	plan := &storage.Plan{}
	tasks := []*storage.Task{{
		TaskIdentifier: "task_1",
		Namespace:      "testapp",
		TableName:      "users",
		DDLAction:      "alter",
		DDL:            "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
	}}

	_, err := c.replanAndFilterTasks(t.Context(), apply, tasks, plan)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "drifted from the reviewed plan")
	assert.Contains(t, err.Error(), "testapp.users/alter")
}

// When the re-plan matches the reviewed DDL the deployment has not drifted, so
// the task stays active and its DDL is refreshed from the re-plan.
func TestReplanAndFilterTasks_MatchKeepsTaskActive(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	c := newPlanMaterializeClientWithPlan(store, alterUsersEmailPlan())

	apply := &storage.Apply{Database: "testapp"}
	plan := &storage.Plan{}
	tasks := []*storage.Task{{
		TaskIdentifier: "task_1",
		Namespace:      "testapp",
		TableName:      "users",
		DDLAction:      "alter",
		DDL:            "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
	}}

	rp, err := c.replanAndFilterTasks(t.Context(), apply, tasks, plan)
	require.NoError(t, err)
	require.Len(t, rp.ActiveTasks, 1)
	assert.Equal(t, "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", rp.ActiveTasks[0].DDL)
	assert.Zero(t, rp.CompletedCount)
}
