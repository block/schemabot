package tern

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
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
	assert.Equal(t, []string{ddlA}, got[shardTableKey{namespace: "ks1", shard: "-80", table: "mutes"}])
	assert.Equal(t, []string{ddlB}, got[shardTableKey{namespace: "ks1", shard: "80-", table: "mutes"}])
	assert.Equal(t, []string{ddlC}, got[shardTableKey{namespace: "ks2", shard: "-80", table: "mutes"}], "the same shard+table in another keyspace is not conflated")
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
	assert.Equal(t, []string{ddl}, got[shardTableKey{namespace: "commerce", table: "mutes"}])
}

// A table can carry several statements in one plan, each its own task. The
// re-plan index must keep every statement for the table, in plan order, so each
// task can be matched against its own statement rather than the table's last.
func TestReplanShardTableDDLKeepsEveryStatementForATable(t *testing.T) {
	createDDL := "CREATE TABLE public.users (id bigint PRIMARY KEY)"
	indexDDL := "CREATE INDEX users_email_idx ON public.users (email)"
	result := &engine.PlanResult{
		Changes: []engine.SchemaChange{{
			Namespace: "app",
			TableChanges: []engine.TableChange{
				{Table: "users", DDL: createDDL},
				{Table: "users", DDL: indexDDL},
			},
		}},
	}

	got := replanShardTableDDL(result)

	require.Len(t, got, 1)
	assert.Equal(t, []string{createDDL, indexDDL}, got[shardTableKey{namespace: "app", table: "users"}])
}

// On resume, replanAndFilterTasks recomputes each deployment's delta against its
// live schema and overwrites task.DDL with it. verifyReplannedTaskDDL is the
// gate that keeps a drifted deployment from silently applying that recomputed
// DDL: it must pass only when the re-plan matches what the task was reviewed
// with, tolerating incidental formatting, and fail closed otherwise.
func TestVerifyReplannedTaskDDL(t *testing.T) {
	c := &LocalClient{config: LocalConfig{Type: storage.DatabaseTypeMySQL}}
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
		ddl, _, err := c.verifyReplannedTaskDDL(tk, []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"}, nil)
		require.NoError(t, err)
		assert.Equal(t, "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ddl)
	})

	t.Run("incidental formatting differences pass", func(t *testing.T) {
		// Unquoted identifiers and extra whitespace canonicalize to the same form
		// as the reviewed DDL, so they are not drift.
		tk := task("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		ddl, _, err := c.verifyReplannedTaskDDL(tk, []string{"ALTER TABLE   users   ADD COLUMN email varchar(255)"}, nil)
		require.NoError(t, err)
		assert.Equal(t, "ALTER TABLE   users   ADD COLUMN email varchar(255)", ddl, "the re-planned text is what the task will run")
	})

	t.Run("divergent re-plan fails closed", func(t *testing.T) {
		// The deployment drifted: the re-plan would apply a different column type
		// than the one reviewed. This unreviewed DDL must be refused.
		tk := task("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		_, _, err := c.verifyReplannedTaskDDL(tk, []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(100)"}, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "drifted from the reviewed plan")
		assert.Contains(t, err.Error(), "commerce[-80].users/alter")
	})

	t.Run("the task's statement is matched among the table's several statements", func(t *testing.T) {
		// Three statements remain for the table; this task's is the middle
		// one, in different formatting. It must resolve to its own statement,
		// not the table's first or last.
		tk := task("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		ddl, _, err := c.verifyReplannedTaskDDL(tk, []string{
			"ALTER TABLE `users` ADD COLUMN `name` varchar(255)",
			"ALTER TABLE users ADD COLUMN email varchar(255)",
			"ALTER TABLE `users` ADD INDEX (`email`)",
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, "ALTER TABLE users ADD COLUMN email varchar(255)", ddl)
	})

	sibling := func(id, reviewed, taskState string) *storage.Task {
		s := task(reviewed)
		s.TaskIdentifier = id
		s.State = taskState
		return s
	}

	t.Run("no match with no pending siblings is drift", func(t *testing.T) {
		// The table still has pending statements, none is this task's, and no
		// other pending task was reviewed with them either: unreviewed DDL.
		tk := task("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		_, landed, err := c.verifyReplannedTaskDDL(tk, []string{
			"ALTER TABLE `users` ADD COLUMN `name` varchar(255)",
			"ALTER TABLE `users` ADD INDEX (`name`)",
		}, []*storage.Task{tk})
		require.Error(t, err)
		assert.False(t, landed)
		assert.Contains(t, err.Error(), "drifted from the reviewed plan")
		assert.Contains(t, err.Error(), "lists 2 pending statements for commerce[-80].users/alter")
		assert.Contains(t, err.Error(), "including 2 statements that neither task task_abc123 nor any pending sibling task was reviewed with")
	})

	t.Run("statement absent while pending siblings vouch for every remaining statement is landed", func(t *testing.T) {
		// The table still has two pending statements and neither is this
		// task's, but both are the reviewed DDL of sibling tasks that have not
		// run yet. The only statement that can have left the diff is this
		// task's own, so it landed and there is nothing to run.
		tk := task("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		siblings := []*storage.Task{
			tk,
			sibling("task_name", "ALTER TABLE users ADD COLUMN name varchar(255)", state.Task.Stopped),
			sibling("task_idx", "ALTER TABLE `users` ADD INDEX (`name`)", state.Task.Pending),
		}
		ddl, landed, err := c.verifyReplannedTaskDDL(tk, []string{
			"ALTER TABLE `users` ADD COLUMN `name` varchar(255)",
			"ALTER TABLE `users` ADD INDEX (`name`)",
		}, siblings)
		require.NoError(t, err)
		assert.True(t, landed)
		assert.Empty(t, ddl, "a landed task has no statement to run")
	})

	t.Run("a terminal sibling's leftover statement is refused without calling it drift", func(t *testing.T) {
		// The only remaining statement is the reviewed DDL of a sibling that
		// already settled (here it failed and never ran). The schema has not
		// drifted, but no pending task will run that statement, and this task
		// must not run another task's DDL in place of its own. The refusal
		// names the sibling and its state so the operator examines that task
		// instead of hunting for drift.
		tk := task("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		siblings := []*storage.Task{
			tk,
			sibling("task_name", "ALTER TABLE `users` ADD COLUMN `name` varchar(255)", state.Task.Failed),
		}
		_, landed, err := c.verifyReplannedTaskDDL(tk, []string{
			"ALTER TABLE `users` ADD COLUMN `name` varchar(255)",
		}, siblings)
		require.Error(t, err)
		assert.False(t, landed)
		assert.Contains(t, err.Error(), "has not drifted from the reviewed plan")
		assert.Contains(t, err.Error(), "cannot run task task_abc123")
		assert.Contains(t, err.Error(), "absent from the re-plan for commerce[-80].users/alter")
		assert.Contains(t, err.Error(), "terminal sibling task task_name (failed)")
		assert.NotContains(t, err.Error(), "has drifted")
	})

	t.Run("a terminal sibling explains one statement but unreviewed DDL beside it is still drift", func(t *testing.T) {
		// One remaining statement is a completed sibling's reviewed DDL, the
		// other was reviewed by nobody. The unreviewed statement decides: this
		// is drift, and only the unreviewed statement is reported as such.
		tk := task("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		siblings := []*storage.Task{
			tk,
			sibling("task_name", "ALTER TABLE `users` ADD COLUMN `name` varchar(255)", state.Task.Completed),
		}
		_, landed, err := c.verifyReplannedTaskDDL(tk, []string{
			"ALTER TABLE `users` ADD COLUMN `name` varchar(255)",
			"ALTER TABLE `users` ADD COLUMN `nickname` varchar(255)",
		}, siblings)
		require.Error(t, err)
		assert.False(t, landed)
		assert.Contains(t, err.Error(), "has drifted from the reviewed plan")
		assert.Contains(t, err.Error(), "including 1 statement that neither task task_abc123 nor any pending sibling task was reviewed with")
		assert.Contains(t, err.Error(), "nickname")
		assert.NotContains(t, err.Error(), "`name`", "the terminal sibling's statement is not reported as unreviewed")
	})

	t.Run("one unvouched statement among vouched siblings is drift, not landed", func(t *testing.T) {
		// Two statements remain: one is a pending sibling's, the other was
		// reviewed by nobody. The task's own statement may well have landed,
		// but the diff also carries unreviewed DDL, so the resume refuses.
		tk := task("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		siblings := []*storage.Task{
			tk,
			sibling("task_name", "ALTER TABLE `users` ADD COLUMN `name` varchar(255)", state.Task.Stopped),
		}
		_, landed, err := c.verifyReplannedTaskDDL(tk, []string{
			"ALTER TABLE `users` ADD COLUMN `name` varchar(255)",
			"ALTER TABLE `users` ADD COLUMN `nickname` varchar(255)",
		}, siblings)
		require.Error(t, err)
		assert.False(t, landed)
		assert.Contains(t, err.Error(), "drifted from the reviewed plan")
		assert.Contains(t, err.Error(), "lists 2 pending statements for commerce[-80].users/alter")
		assert.Contains(t, err.Error(), "including 1 statement that neither task task_abc123 nor its pending sibling task task_name was reviewed with")
		assert.Contains(t, err.Error(), "nickname")
		assert.NotContains(t, err.Error(), "`name`", "the vouched statement is not reported as unreviewed")
	})

	t.Run("several pending siblings are named in the drift refusal", func(t *testing.T) {
		tk := task("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		siblings := []*storage.Task{
			tk,
			sibling("task_name", "ALTER TABLE `users` ADD COLUMN `name` varchar(255)", state.Task.Stopped),
			sibling("task_idx", "ALTER TABLE `users` ADD INDEX (`name`)", state.Task.Pending),
		}
		_, landed, err := c.verifyReplannedTaskDDL(tk, []string{
			"ALTER TABLE `users` ADD COLUMN `name` varchar(255)",
			"ALTER TABLE `users` ADD INDEX (`name`)",
			"ALTER TABLE `users` ADD COLUMN `nickname` varchar(255)",
		}, siblings)
		require.Error(t, err)
		assert.False(t, landed)
		assert.Contains(t, err.Error(), "lists 3 pending statements for commerce[-80].users/alter")
		assert.Contains(t, err.Error(), "neither task task_abc123 nor its 2 pending sibling tasks task_name, task_idx was reviewed with")
	})

	t.Run("a pending sibling vouches for one occurrence of its statement", func(t *testing.T) {
		// The re-plan lists the sibling's statement twice. The sibling was
		// reviewed with it once, so the second occurrence is unreviewed.
		tk := task("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		siblings := []*storage.Task{
			tk,
			sibling("task_name", "ALTER TABLE `users` ADD COLUMN `name` varchar(255)", state.Task.Stopped),
		}
		_, landed, err := c.verifyReplannedTaskDDL(tk, []string{
			"ALTER TABLE `users` ADD COLUMN `name` varchar(255)",
			"ALTER TABLE `users` ADD COLUMN `name` varchar(255)",
		}, siblings)
		require.Error(t, err)
		assert.False(t, landed)
		assert.Contains(t, err.Error(), "has drifted from the reviewed plan")
		assert.Contains(t, err.Error(), "including 1 statement that neither task task_abc123 nor its pending sibling task task_name was reviewed with")
	})

	t.Run("a sibling in the same apply operation vouches", func(t *testing.T) {
		// A multi-deployment apply scopes its tasks by operation. Both tasks
		// carry the same operation, so the sibling's reviewed DDL vouches for
		// the remaining statement and this task is landed.
		opID := int64(7)
		tk := task("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		tk.ApplyOperationID = &opID
		sameOp := sibling("task_name", "ALTER TABLE `users` ADD COLUMN `name` varchar(255)", state.Task.Stopped)
		sameOpID := opID
		sameOp.ApplyOperationID = &sameOpID
		_, landed, err := c.verifyReplannedTaskDDL(tk, []string{
			"ALTER TABLE `users` ADD COLUMN `name` varchar(255)",
		}, []*storage.Task{tk, sameOp})
		require.NoError(t, err)
		assert.True(t, landed)
	})

	t.Run("siblings on another shard, operation or apply do not vouch", func(t *testing.T) {
		// Same table name, but the other tasks belong to a different shard, a
		// different apply operation, or a different apply altogether. None
		// shares this task's scope, so the remaining statement is unreviewed
		// here.
		tk := task("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		otherShard := sibling("task_shard", "ALTER TABLE `users` ADD COLUMN `name` varchar(255)", state.Task.Stopped)
		otherShard.Shard = "80-"
		otherOp := sibling("task_op", "ALTER TABLE `users` ADD COLUMN `name` varchar(255)", state.Task.Stopped)
		opID := int64(7)
		otherOp.ApplyOperationID = &opID
		otherApply := sibling("task_apply", "ALTER TABLE `users` ADD COLUMN `name` varchar(255)", state.Task.Stopped)
		otherApply.ApplyID = tk.ApplyID + 1
		_, landed, err := c.verifyReplannedTaskDDL(tk, []string{
			"ALTER TABLE `users` ADD COLUMN `name` varchar(255)",
		}, []*storage.Task{tk, otherShard, otherOp, otherApply})
		require.Error(t, err)
		assert.False(t, landed)
		assert.Contains(t, err.Error(), "drifted from the reviewed plan")
		assert.Contains(t, err.Error(), "re-planned \"ALTER TABLE `users` ADD COLUMN `name` VARCHAR(255)\"",
			"with no sibling in scope the refusal reads as plain single-statement drift")
	})

	t.Run("a terminal sibling explains one occurrence of its statement", func(t *testing.T) {
		// The re-plan lists a completed sibling's statement twice. The sibling
		// accounts for one; the other was reviewed by nobody and is drift.
		tk := task("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		siblings := []*storage.Task{
			tk,
			sibling("task_name", "ALTER TABLE `users` ADD COLUMN `name` varchar(255)", state.Task.Completed),
		}
		_, landed, err := c.verifyReplannedTaskDDL(tk, []string{
			"ALTER TABLE `users` ADD COLUMN `name` varchar(255)",
			"ALTER TABLE `users` ADD COLUMN `name` varchar(255)",
		}, siblings)
		require.Error(t, err)
		assert.False(t, landed)
		assert.Contains(t, err.Error(), "has drifted from the reviewed plan")
	})

	t.Run("each terminal sibling explains its own occurrence of a shared statement", func(t *testing.T) {
		// Two failed siblings were each reviewed with the same statement and
		// the re-plan lists it twice. Both occurrences are reviewed plan DDL
		// that nothing pending will run, so the refusal names both siblings
		// and does not call the schema drifted.
		tk := task("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		siblings := []*storage.Task{
			tk,
			sibling("task_t1", "ALTER TABLE `users` ADD COLUMN `name` varchar(255)", state.Task.Failed),
			sibling("task_t2", "ALTER TABLE `users` ADD COLUMN `name` varchar(255)", state.Task.Failed),
		}
		_, landed, err := c.verifyReplannedTaskDDL(tk, []string{
			"ALTER TABLE `users` ADD COLUMN `name` varchar(255)",
			"ALTER TABLE `users` ADD COLUMN `name` varchar(255)",
		}, siblings)
		require.Error(t, err)
		assert.False(t, landed)
		assert.Contains(t, err.Error(), "has not drifted from the reviewed plan")
		assert.Contains(t, err.Error(), "terminal sibling tasks task_t1 (failed), task_t2 (failed)")
		assert.NotContains(t, err.Error(), "has drifted")
	})

	t.Run("a shared statement listed once names every terminal sibling reviewed with it", func(t *testing.T) {
		// Two terminal siblings were reviewed with the same statement and the
		// re-plan lists it once. The one occurrence is reviewed plan DDL, not
		// drift, and the operator cannot tell from the statement alone which
		// sibling's outcome left it behind, so the refusal names both.
		tk := task("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		siblings := []*storage.Task{
			tk,
			sibling("task_t1", "ALTER TABLE `users` ADD COLUMN `name` varchar(255)", state.Task.Failed),
			sibling("task_t2", "ALTER TABLE `users` ADD COLUMN `name` varchar(255)", state.Task.Cancelled),
		}
		_, landed, err := c.verifyReplannedTaskDDL(tk, []string{
			"ALTER TABLE `users` ADD COLUMN `name` varchar(255)",
		}, siblings)
		require.Error(t, err)
		assert.False(t, landed)
		assert.Contains(t, err.Error(), "has not drifted from the reviewed plan")
		assert.Contains(t, err.Error(), "terminal sibling tasks task_t1 (failed), task_t2 (cancelled)")
		assert.NotContains(t, err.Error(), "has drifted")
	})

	t.Run("a sibling with no reviewed DDL neither vouches nor blocks", func(t *testing.T) {
		// A legacy synthetic sibling carries no DDL. It has nothing to compare
		// against, so it is left out of the sibling set: it must not fail the
		// resume on an empty statement, and the pending sibling that does
		// carry the remaining statement still vouches for it.
		tk := task("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		siblings := []*storage.Task{
			tk,
			sibling("task_vschema", "", state.Task.Stopped),
			sibling("task_name", "ALTER TABLE `users` ADD COLUMN `name` varchar(255)", state.Task.Stopped),
		}
		_, landed, err := c.verifyReplannedTaskDDL(tk, []string{
			"ALTER TABLE `users` ADD COLUMN `name` varchar(255)",
		}, siblings)
		require.NoError(t, err)
		assert.True(t, landed)
	})

	t.Run("no re-planned statements is an error", func(t *testing.T) {
		tk := task("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		_, _, err := c.verifyReplannedTaskDDL(tk, nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "re-plan emitted no statements")
	})

	t.Run("empty reviewed DDL is left to the caller", func(t *testing.T) {
		// Only legacy synthetic VSchema tasks carry no reviewed DDL; they have no
		// reference to compare against and are handled downstream, not here.
		tk := task("")
		ddl, _, err := c.verifyReplannedTaskDDL(tk, []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"}, nil)
		require.NoError(t, err)
		assert.Equal(t, "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", ddl)
	})

	t.Run("unparseable re-planned DDL fails closed", func(t *testing.T) {
		tk := task("ALTER TABLE `users` ADD COLUMN `email` varchar(255)")
		_, _, err := c.verifyReplannedTaskDDL(tk, []string{"this is not valid sql"}, nil)
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
				Operation: ddl.StatementAlterTable,
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

// The sequential resume loop re-plans each table right before applying it to
// catch a cutover that raced the resume. tableStillNeedsChange must return the
// DDL that re-plan would now apply so the loop can confirm it still matches the
// reviewed DDL before applying — closing the window between the resume-entry
// re-plan and this later per-task apply.
func TestTableStillNeedsChange_ReturnsReplannedDDL(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	c := newPlanMaterializeClientWithPlan(store, alterUsersEmailPlan())

	apply := &storage.Apply{Database: "testapp"}
	plan := &storage.Plan{}
	task := &storage.Task{
		TaskIdentifier: "task_1",
		Namespace:      "testapp",
		TableName:      "users",
		DDLAction:      "alter",
		DDL:            "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
	}

	replanned, needsChange, err := c.tableStillNeedsChange(t.Context(), apply, plan, task)
	require.NoError(t, err)
	assert.True(t, needsChange)
	assert.Equal(t, []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"}, replanned)
	_, _, err = c.verifyReplannedTaskDDL(task, replanned, []*storage.Task{task})
	require.NoError(t, err, "matching re-plan is not drift")
}

// When the table has dropped out of the re-plan diff (its cutover completed) the
// sequential loop treats it as already applied, so tableStillNeedsChange must
// report that no change remains.
func TestTableStillNeedsChange_TableAbsentReportsDone(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	// Re-plan for a different table only: the task's table is no longer in the diff.
	otherTablePlan := &engine.PlanResult{
		Changes: []engine.SchemaChange{{
			Namespace: "testapp",
			TableChanges: []engine.TableChange{{
				Table:     "orders",
				Operation: ddl.StatementAlterTable,
				DDL:       "ALTER TABLE `orders` ADD COLUMN `total` int",
			}},
		}},
	}
	c := newPlanMaterializeClientWithPlan(store, otherTablePlan)

	apply := &storage.Apply{Database: "testapp"}
	plan := &storage.Plan{}
	task := &storage.Task{
		TaskIdentifier: "task_1",
		Namespace:      "testapp",
		TableName:      "users",
		DDLAction:      "alter",
		DDL:            "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
	}

	replanned, needsChange, err := c.tableStillNeedsChange(t.Context(), apply, plan, task)
	require.NoError(t, err)
	assert.False(t, needsChange)
	assert.Empty(t, replanned)
}

// If live drifts between resume entry and a later per-task apply, the re-plan the
// sequential loop performs returns DDL that no longer matches the reviewed DDL.
// tableStillNeedsChange surfaces that DDL and verifyReplannedTaskDDL fails closed
// so the loop refuses to apply unreviewed DDL.
func TestTableStillNeedsChange_DriftFailsClosed(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	drifted := &engine.PlanResult{
		Changes: []engine.SchemaChange{{
			Namespace: "testapp",
			TableChanges: []engine.TableChange{{
				Table:     "users",
				Operation: ddl.StatementAlterTable,
				DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(100)",
			}},
		}},
	}
	c := newPlanMaterializeClientWithPlan(store, drifted)

	apply := &storage.Apply{Database: "testapp"}
	plan := &storage.Plan{}
	task := &storage.Task{
		TaskIdentifier: "task_1",
		Namespace:      "testapp",
		TableName:      "users",
		DDLAction:      "alter",
		DDL:            "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
	}

	replanned, needsChange, err := c.tableStillNeedsChange(t.Context(), apply, plan, task)
	require.NoError(t, err)
	require.True(t, needsChange)
	_, _, err = c.verifyReplannedTaskDDL(task, replanned, []*storage.Task{task})
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

// An apply reclaimed while its engine holds or unwinds a revert (task states
// revert_window, reverting) reports a live schema that matches the reviewed
// target until the revert cutover lands, so a schema match is not evidence the
// task settled — completing it would terminalize the apply as a success while
// the engine reverts the schema change underneath it. The resume re-plan must
// keep such tasks active, with their reviewed DDL untouched, so the resume
// reattaches to the engine and the terminal state comes from engine progress.
func TestReplanAndFilterTasks_RevertPhaseTaskStaysActive(t *testing.T) {
	reviewed := "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"
	revertPhaseTask := func(taskState string) []*storage.Task {
		return []*storage.Task{{
			TaskIdentifier: "task_1",
			Namespace:      "testapp",
			TableName:      "users",
			DDLAction:      "alter",
			DDL:            reviewed,
			State:          taskState,
		}}
	}
	replans := []struct {
		name   string
		replan *engine.PlanResult
	}{
		// Revert not yet landed: live still matches the reviewed target, so the
		// re-plan finds no remaining diff for the table.
		{name: "live schema still matches the reviewed target", replan: &engine.PlanResult{}},
		// Revert already landed: live is back on the original schema, so the
		// re-plan reproduces a forward change. Its DDL deliberately differs
		// from the reviewed DDL so the subtest is discriminating: the guard
		// must keep the task active with the reviewed DDL untouched without
		// consulting the re-plan at all — a re-plan comparison would fail
		// closed on the divergence, and a re-plan adoption would overwrite
		// the DDL.
		{name: "revert already landed", replan: &engine.PlanResult{
			Changes: []engine.SchemaChange{{
				Namespace: "testapp",
				TableChanges: []engine.TableChange{{
					Table:     "users",
					Operation: ddl.StatementAlterTable,
					DDL:       "ALTER TABLE `users` ADD COLUMN `email` varchar(500)",
				}},
			}},
		}},
	}

	for _, taskState := range []string{state.Task.RevertWindow, state.Task.Reverting} {
		for _, tc := range replans {
			t.Run(taskState+"/"+tc.name, func(t *testing.T) {
				store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
				c := newPlanMaterializeClientWithPlan(store, tc.replan)

				apply := &storage.Apply{Database: "testapp"}
				tasks := revertPhaseTask(taskState)

				rp, err := c.replanAndFilterTasks(t.Context(), apply, tasks, &storage.Plan{})
				require.NoError(t, err)
				require.Len(t, rp.ActiveTasks, 1, "a revert-phase task must stay active for the engine reattach")
				active := rp.ActiveTasks[0]
				assert.Equal(t, taskState, active.State, "the engine-monitored state is preserved for the reattach")
				assert.Equal(t, reviewed, active.DDL, "the reviewed DDL is kept, not overwritten from the re-plan")
				assert.Nil(t, active.CompletedAt)
				assert.Zero(t, rp.CompletedCount)
			})
		}
	}
}

// scriptedPlanStore scripts plan reads for recovery tests: a nil plan with a
// nil error models a confirmed-missing plan row, while a non-nil error models
// a storage read failure.
type scriptedPlanStore struct {
	storage.PlanStore
	plan *storage.Plan
	err  error
}

func (s *scriptedPlanStore) GetByID(context.Context, int64) (*storage.Plan, error) {
	return s.plan, s.err
}

// terminalRecordingObserver records terminal notifications so recovery tests
// can assert whether an apply's registered waiter (e.g. the PR check/comment)
// was told the apply reached a terminal state.
type terminalRecordingObserver struct {
	terminal []*storage.Apply
}

func (o *terminalRecordingObserver) OnProgress(*storage.Apply, []*storage.Task) {}
func (o *terminalRecordingObserver) OnTerminal(apply *storage.Apply, _ []*storage.Task) {
	o.terminal = append(o.terminal, apply)
}

// recoveryPlanLoadFixture builds an in-flight Vitess apply whose recovery is
// about to load its plan, with the plan store scripted by the caller.
func recoveryPlanLoadFixture(plans storage.PlanStore) (*LocalClient, *storage.Apply, []*storage.Task, *exactProgressApplyStore) {
	operationID := int64(3)
	apply := &storage.Apply{
		ID:              21,
		ApplyIdentifier: "apply-recover-plan",
		PlanID:          5,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeVitess,
		Engine:          storage.EnginePlanetScale,
		State:           state.Apply.Running,
	}
	tasks := []*storage.Task{{
		ID:               2,
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TaskIdentifier:   "task-recover-plan",
		Database:         "testdb",
		DatabaseType:     storage.DatabaseTypeVitess,
		Namespace:        "commerce",
		TableName:        "users",
		DDL:              "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
		DDLAction:        "alter",
		State:            state.Task.Running,
	}}
	applyStore := &exactProgressApplyStore{apply: apply}
	client := &LocalClient{
		config: LocalConfig{Database: "testdb", Type: storage.DatabaseTypeVitess},
		storage: &exactProgressStorage{
			applies:         applyStore,
			tasks:           &exactProgressTaskStore{tasks: tasks},
			controlRequests: &testControlRequestStore{},
			plans:           plans,
		},
		logger: slog.Default(),
	}
	return client, apply, tasks, applyStore
}

// Recovery must not convert a transient storage failure on the plan load into
// terminal apply state: the engine-side work (a checkpointed copy or a live
// deploy request) is untouched. The recovery attempt exits with an error so
// the claim is released and a later attempt retries against intact storage,
// and no terminal notification reaches the apply's observer.
func TestResumeApplyPlanLoadStorageErrorStaysRecoverable(t *testing.T) {
	storageErr := errors.New("storage unavailable")
	client, apply, tasks, applyStore := recoveryPlanLoadFixture(&scriptedPlanStore{err: storageErr})
	observer := &terminalRecordingObserver{}
	client.SetObserver(apply.ID, observer)

	err := client.resumeApplyWithTasks(t.Context(), apply, tasks, nil, false, false)

	require.ErrorIs(t, err, storageErr)
	assert.ErrorContains(t, err, "apply-recover-plan")
	assert.True(t, state.IsState(applyStore.apply.State, state.Apply.Running),
		"in-flight apply must stay recoverable, not terminal: got %s", applyStore.apply.State)
	assert.False(t, state.IsTerminalApplyState(applyStore.apply.State))
	assert.Empty(t, applyStore.apply.ErrorMessage)
	assert.Empty(t, observer.terminal, "a transient plan-load failure must not notify the terminal observer")
}

// A confirmed-missing plan row (a nil plan with no read error) is
// unrecoverable — the reviewed DDL cannot be rebuilt — so recovery fails the
// apply with an operator-facing reason and notifies its terminal observer.
func TestResumeApplyMissingPlanFailsApply(t *testing.T) {
	client, apply, tasks, applyStore := recoveryPlanLoadFixture(&scriptedPlanStore{})
	observer := &terminalRecordingObserver{}
	client.SetObserver(apply.ID, observer)

	err := client.resumeApplyWithTasks(t.Context(), apply, tasks, nil, false, false)

	require.NoError(t, err)
	assert.True(t, state.IsState(applyStore.apply.State, state.Apply.Failed),
		"apply with no plan row must fail, got %s", applyStore.apply.State)
	assert.Equal(t, "plan not found during recovery", applyStore.apply.ErrorMessage)
	assert.NotNil(t, applyStore.apply.CompletedAt, "a failed apply must record its completion time")
	assert.True(t, state.IsState(tasks[0].State, state.Task.Failed),
		"in-flight task must fail with its apply, got %s", tasks[0].State)
	assert.Equal(t, "plan not found during recovery", tasks[0].ErrorMessage)
	require.Len(t, observer.terminal, 1)
	assert.True(t, state.IsState(observer.terminal[0].State, state.Apply.Failed))
}

// When another actor settles the apply between the recovery claim and the
// plan-missing terminalization (e.g. a raced Stop()), the stored terminal
// state wins: it is not overwritten, and the observer is notified with the
// settled verdict rather than the stale in-flight state this recovery
// attempt was holding.
func TestResumeApplyMissingPlanAdoptsConcurrentTerminalState(t *testing.T) {
	client, apply, tasks, applyStore := recoveryPlanLoadFixture(&scriptedPlanStore{})
	settled := *apply
	settled.State = state.Apply.Stopped
	settled.ErrorMessage = "stopped by operator"
	applyStore.apply = &settled
	observer := &terminalRecordingObserver{}
	client.SetObserver(apply.ID, observer)

	err := client.resumeApplyWithTasks(t.Context(), apply, tasks, nil, false, false)

	require.NoError(t, err)
	assert.True(t, state.IsState(applyStore.apply.State, state.Apply.Stopped),
		"concurrently-settled state must not be overwritten, got %s", applyStore.apply.State)
	assert.Equal(t, "stopped by operator", applyStore.apply.ErrorMessage)
	require.Len(t, observer.terminal, 1)
	assert.True(t, state.IsState(observer.terminal[0].State, state.Apply.Stopped),
		"observer must see the settled verdict, got %s", observer.terminal[0].State)
}

// A task whose table no longer appears in the resume re-plan diff has no
// remaining work: the live schema already matches the reviewed target (the
// engine finished the change before the previous drive lost the apply). The
// re-plan settles it — the task row is durably marked completed with full
// progress — and reports it in CompletedCount so the caller can terminalize
// the apply from settled rows.
func TestReplanAndFilterTasks_SettledTaskPersistsCompleted(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	c := newPlanMaterializeClientWithPlan(store, &engine.PlanResult{})
	logs := &mockApplyLogStore{}
	c.storage = &exactProgressStorage{plans: store, tasks: &exactProgressTaskStore{}, logs: logs}

	apply := &storage.Apply{ID: 21, ApplyIdentifier: "apply-replan-settle", Database: "testapp"}
	tasks := []*storage.Task{{
		TaskIdentifier: "task_1",
		Namespace:      "testapp",
		TableName:      "users",
		DDLAction:      "alter",
		DDL:            "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
		State:          state.Task.FailedRetryable,
	}}

	rp, err := c.replanAndFilterTasks(t.Context(), apply, tasks, &storage.Plan{})
	require.NoError(t, err)
	assert.Empty(t, rp.ActiveTasks)
	assert.Equal(t, int64(1), rp.CompletedCount)
	assert.True(t, state.IsState(tasks[0].State, state.Task.Completed),
		"a task whose table left the diff is settled completed, got %s", tasks[0].State)
	assert.EqualValues(t, 100, tasks[0].ProgressPercent)
	assert.NotNil(t, tasks[0].CompletedAt)
	assert.True(t, hasLogMessageContaining(logs.logs, "Task task_1 already completed (live schema matches the reviewed target)"),
		"a landed settlement records its transition in the apply's durable log")
}

// A multi-statement table whose driver crashed after executing one statement
// but before recording its outcome resumes with that statement gone from the
// re-plan diff while its sibling statements remain. Because every remaining
// statement is the reviewed DDL of a sibling task that has not run, the
// re-plan settles the landed task completed without re-executing it and keeps
// only the siblings active, each on its own reviewed statement.
func TestReplanAndFilterTasks_LandedStatementSettlesCompleted(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	remaining := &engine.PlanResult{
		Changes: []engine.SchemaChange{{
			Namespace: "testapp",
			TableChanges: []engine.TableChange{
				{Table: "users", Operation: ddl.StatementAlterTable, DDL: "ALTER TABLE `users` ADD COLUMN `name` varchar(255)"},
				{Table: "users", Operation: ddl.StatementAlterTable, DDL: "ALTER TABLE `users` ADD INDEX (`name`)"},
			},
		}},
	}
	c := newPlanMaterializeClientWithPlan(store, remaining)
	logs := &mockApplyLogStore{}
	c.storage = &exactProgressStorage{plans: store, tasks: &exactProgressTaskStore{}, logs: logs}

	apply := &storage.Apply{ID: 21, ApplyIdentifier: "apply-replan-landed", Database: "testapp"}
	usersTask := func(id, reviewed string) *storage.Task {
		return &storage.Task{
			TaskIdentifier: id,
			Namespace:      "testapp",
			TableName:      "users",
			DDLAction:      "alter",
			DDL:            reviewed,
			State:          state.Task.Stopped,
		}
	}
	tasks := []*storage.Task{
		usersTask("task_email", "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"),
		usersTask("task_name", "ALTER TABLE `users` ADD COLUMN `name` varchar(255)"),
		usersTask("task_idx", "ALTER TABLE `users` ADD INDEX (`name`)"),
	}

	rp, err := c.replanAndFilterTasks(t.Context(), apply, tasks, &storage.Plan{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), rp.CompletedCount)
	require.Len(t, rp.ActiveTasks, 2)
	assert.Equal(t, "task_name", rp.ActiveTasks[0].TaskIdentifier)
	assert.Equal(t, "ALTER TABLE `users` ADD COLUMN `name` varchar(255)", rp.ActiveTasks[0].DDL)
	assert.Equal(t, "task_idx", rp.ActiveTasks[1].TaskIdentifier)
	assert.Equal(t, "ALTER TABLE `users` ADD INDEX (`name`)", rp.ActiveTasks[1].DDL)

	assert.True(t, state.IsState(tasks[0].State, state.Task.Completed),
		"the task whose statement landed is settled completed, got %s", tasks[0].State)
	assert.EqualValues(t, 100, tasks[0].ProgressPercent)
	assert.NotNil(t, tasks[0].CompletedAt)
	assert.True(t, hasLogMessageContaining(logs.logs, "Task task_email already completed (its statement landed before its outcome was recorded)"),
		"a landed settlement records its transition in the apply's durable log")
}

// Settling a no-remaining-work task is only real once its completed state
// durably lands. When the task store refuses the write — here a lease-guarded
// update that lost the drive's lease to a peer driver — the re-plan must fail
// closed instead of counting the task settled: the caller terminalizes the
// parent apply from this partition, and completing the apply over a task row
// that durably stays non-terminal would strand the pair in contradictory
// states. The returned error aborts the resume so a later claim redoes the
// settlement under a current lease.
func TestReplanAndFilterTasks_FailsClosedWhenCompletedWriteRefused(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	c := newPlanMaterializeClientWithPlan(store, &engine.PlanResult{})
	logs := &mockApplyLogStore{}
	c.storage = &exactProgressStorage{
		plans: store,
		tasks: &exactProgressTaskStore{err: storage.ErrApplyLeaseLost},
		logs:  logs,
	}

	apply := &storage.Apply{ID: 21, ApplyIdentifier: "apply-replan-lease-lost", Database: "testapp"}
	tasks := []*storage.Task{{
		TaskIdentifier: "task_1",
		Namespace:      "testapp",
		TableName:      "users",
		DDLAction:      "alter",
		DDL:            "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
		State:          state.Task.FailedRetryable,
	}}

	rp, err := c.replanAndFilterTasks(t.Context(), apply, tasks, &storage.Plan{})
	require.ErrorIs(t, err, storage.ErrApplyLeaseLost)
	assert.ErrorContains(t, err, "task_1")
	assert.Nil(t, rp, "a refused settlement write must not yield a partition the caller could terminalize from")
	assert.Empty(t, logs.logs, "the durable log must not claim a transition the task row does not carry")
}

// The sequential resume loop settles a task whose table already has the
// desired schema (its cutover raced the re-plan) by writing the task row
// completed. That settlement is only real once the write durably lands: the
// loop's finalization derives the apply's terminal state from these task rows,
// so a refused write — here a lease-guarded update that lost the drive's lease
// to a peer driver — must abort the resume without finalizing. The apply stays
// active for a later claim to redo the settlement under a current lease, and
// the durable log records no transition the task row does not carry.
func TestResumeApplySequential_AbortsWhenRacedCutoverSettlementRefused(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	c := newPlanMaterializeClientWithPlan(store, &engine.PlanResult{})
	c.heartbeatInterval = time.Hour

	apply := &storage.Apply{
		ID:              21,
		ApplyIdentifier: "apply-sequential-settle-refused",
		Database:        "testapp",
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:             1,
		ApplyID:        apply.ID,
		TaskIdentifier: "task_1",
		Database:       "testapp",
		Namespace:      "testapp",
		TableName:      "users",
		DDLAction:      "alter",
		DDL:            "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
		State:          state.Task.Running,
	}
	applies := &snapshotApplyStore{stored: *apply}
	logs := &mockApplyLogStore{}
	c.storage = &exactProgressStorage{
		plans:   store,
		applies: applies,
		tasks: &updateFailingTaskStore{
			exactProgressTaskStore: &exactProgressTaskStore{tasks: []*storage.Task{task}},
			updateErr:              storage.ErrApplyLeaseLost,
		},
		controlRequests: &testControlRequestStore{},
		logs:            logs,
	}

	c.resumeApplySequential(t.Context(), apply, []*storage.Task{task}, &storage.Plan{}, nil)

	stored, err := applies.Get(t.Context(), apply.ID)
	require.NoError(t, err)
	assert.True(t, state.IsState(stored.State, state.Apply.Running),
		"a refused settlement write must abort the resume before finalization, leaving the apply claimable; stored state was %q", stored.State)
	assert.False(t, state.IsTerminalApplyState(stored.State),
		"the apply must not terminalize over a task row that durably stays non-terminal")
	assert.Nil(t, stored.CompletedAt)
	assert.Empty(t, logs.logs, "the durable log must not claim a transition the task row does not carry")
}

// landedSiblingEngine re-plans to a fixed remaining diff and completes any
// statement it is asked to apply on the first progress poll, recording what it
// was asked to run so a test can assert which statements executed.
type landedSiblingEngine struct {
	fakePlanEngine
	applied []string
}

func (e *landedSiblingEngine) Apply(_ context.Context, req *engine.ApplyRequest) (*engine.ApplyResult, error) {
	for _, change := range req.Changes {
		for _, tc := range change.TableChanges {
			e.applied = append(e.applied, tc.DDL)
		}
	}
	return &engine.ApplyResult{Accepted: true}, nil
}

func (e *landedSiblingEngine) Progress(context.Context, *engine.ProgressRequest) (*engine.ProgressResult, error) {
	return &engine.ProgressResult{State: engine.StateCompleted}, nil
}

// newLandedSiblingResume builds a sequential resume over two tasks on one
// table whose per-task re-plan lists only the second task's statement: the
// first task's statement landed on the table after the resume's initial
// re-plan handed it over as active.
func newLandedSiblingResume(t *testing.T, taskStore storage.TaskStore, logs *mockApplyLogStore) (*LocalClient, *landedSiblingEngine, *storage.Apply, *snapshotApplyStore, []*storage.Task) {
	t.Helper()
	const (
		emailDDL = "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"
		nameDDL  = "ALTER TABLE `users` ADD COLUMN `name` varchar(255)"
	)
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	c := newPlanMaterializeClientWithPlan(store, &engine.PlanResult{
		Changes: []engine.SchemaChange{{
			Namespace:    "testapp",
			TableChanges: []engine.TableChange{{Table: "users", Operation: ddl.StatementAlterTable, DDL: nameDDL}},
		}},
	})
	eng := &landedSiblingEngine{fakePlanEngine: c.spiritEngine.(fakePlanEngine)}
	c.spiritEngine = eng
	c.heartbeatInterval = time.Hour
	c.taskPollIntervalOverride = time.Millisecond

	apply := &storage.Apply{
		ID:              21,
		ApplyIdentifier: "apply-sequential-landed",
		Database:        "testapp",
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	usersTask := func(id int64, identifier, reviewed string) *storage.Task {
		return &storage.Task{
			ID:             id,
			ApplyID:        apply.ID,
			TaskIdentifier: identifier,
			Database:       "testapp",
			Namespace:      "testapp",
			TableName:      "users",
			DDLAction:      "alter",
			DDL:            reviewed,
			State:          state.Task.Running,
		}
	}
	tasks := []*storage.Task{
		usersTask(1, "task_email", emailDDL),
		usersTask(2, "task_name", nameDDL),
	}
	applies := &snapshotApplyStore{stored: *apply}
	c.storage = &exactProgressStorage{
		plans:           store,
		applies:         applies,
		tasks:           taskStore,
		controlRequests: &testControlRequestStore{},
		logs:            logs,
	}
	return c, eng, apply, applies, tasks
}

// The sequential resume loop re-plans freshly before each task, so a statement
// can leave the diff between the resume's initial re-plan and the task's own
// turn. When the table's remaining statements are all the reviewed DDL of a
// sibling that has not run, the task's own statement is what landed: the loop
// settles it completed without handing it to the engine, records the
// settlement in the apply log, and runs only the sibling's statement, so the
// apply completes with each statement executed at most once.
func TestResumeApplySequential_SettlesLandedStatementWithoutReexecution(t *testing.T) {
	logs := &mockApplyLogStore{}
	taskStore := &exactProgressTaskStore{}
	c, eng, apply, applies, tasks := newLandedSiblingResume(t, taskStore, logs)
	taskStore.tasks = tasks

	c.resumeApplySequential(t.Context(), apply, tasks, &storage.Plan{}, nil)

	landed, sibling := tasks[0], tasks[1]
	assert.True(t, state.IsState(landed.State, state.Task.Completed),
		"the task whose statement landed is settled completed, got %s", landed.State)
	assert.EqualValues(t, 100, landed.ProgressPercent)
	assert.NotNil(t, landed.CompletedAt)
	assert.Equal(t, "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", landed.DDL, "a landed task keeps the DDL it was reviewed with")
	assert.True(t, hasLogMessageContaining(logs.logs, "Task task_email already completed (its statement landed before its outcome was recorded)"),
		"the landed settlement records its transition in the apply's durable log")
	assert.False(t, hasLogMessageContaining(logs.logs, "Task task_email resumed"),
		"a landed task is never handed to the engine")

	assert.True(t, state.IsState(sibling.State, state.Task.Completed), "the sibling runs to completion, got %s", sibling.State)
	assert.Equal(t, []string{"ALTER TABLE `users` ADD COLUMN `name` varchar(255)"}, eng.applied,
		"only the sibling's statement reaches the engine")

	stored, err := applies.Get(t.Context(), apply.ID)
	require.NoError(t, err)
	assert.True(t, state.IsState(stored.State, state.Apply.Completed), "stored apply state was %q", stored.State)
	assert.NotNil(t, stored.CompletedAt)
}

// A landed-statement settlement on the sequential path is only real once the
// task row's completed state durably lands. When the task store refuses the
// write — a lease-guarded update that lost the drive's lease to a peer — the
// loop must abort before running the sibling or finalizing, leaving the apply
// claimable so a later drive redoes the settlement under a current lease, and
// the durable log records no transition the task row does not carry.
func TestResumeApplySequential_AbortsWhenLandedStatementSettlementRefused(t *testing.T) {
	logs := &mockApplyLogStore{}
	taskStore := &updateFailingTaskStore{exactProgressTaskStore: &exactProgressTaskStore{}, updateErr: storage.ErrApplyLeaseLost}
	c, eng, apply, applies, tasks := newLandedSiblingResume(t, taskStore, logs)
	taskStore.tasks = tasks

	c.resumeApplySequential(t.Context(), apply, tasks, &storage.Plan{}, nil)

	assert.False(t, state.IsTerminalTaskState(tasks[0].State),
		"a refused settlement write restores the task's in-memory state, got %s", tasks[0].State)
	assert.Empty(t, eng.applied, "the resume must abort before handing the sibling to the engine")
	stored, err := applies.Get(t.Context(), apply.ID)
	require.NoError(t, err)
	assert.True(t, state.IsState(stored.State, state.Apply.Running),
		"a refused settlement write must abort the resume before finalization, leaving the apply claimable; stored state was %q", stored.State)
	assert.Nil(t, stored.CompletedAt)
	assert.Empty(t, logs.logs, "the durable log must not claim a transition the task row does not carry")
}

// A task in an engine-monitored revert phase carries no evidence a schema
// comparison can settle: post-cutover the live schema matches the reviewed
// target until the revert lands. Revert-phase states never reach a sequential
// resume today, so if one does the resume refuses before it writes the apply
// running, compares schemas, runs the engine, or finalizes — leaving the task
// in its revert-phase state and the apply row exactly as the claim found it,
// so the apply stays claimable and each re-claim fails the same way — rather
// than settle the task completed and report a reverting change as applied.
func TestResumeApplyWithTasks_RefusesSequentialResumeOfRevertPhaseTask(t *testing.T) {
	cases := []struct {
		name       string
		taskState  string
		applyState string
	}{
		{name: "revert window open", taskState: state.Task.RevertWindow, applyState: state.Apply.RevertWindow},
		{name: "revert in flight", taskState: state.Task.Reverting, applyState: state.Apply.Reverting},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logs := &mockApplyLogStore{}
			taskStore := &exactProgressTaskStore{}
			c, eng, apply, applies, tasks := newLandedSiblingResume(t, taskStore, logs)
			plan := &storage.Plan{ID: 5}
			apply.PlanID = plan.ID
			apply.State = tc.applyState
			applies.stored = *apply
			c.storage.(*exactProgressStorage).plans = &fakePlanStore{
				getByIDFn: func(int64) (*storage.Plan, error) { return plan, nil },
			}
			tasks[0].State = tc.taskState
			taskStore.tasks = tasks

			err := c.resumeApplyWithTasks(t.Context(), apply, tasks, nil, false, false)

			require.ErrorIs(t, err, errRevertPhaseTaskInSequentialResume)
			assert.True(t, state.IsState(tasks[0].State, tc.taskState),
				"a revert-phase task keeps its state through the refused resume, got %s", tasks[0].State)
			assert.Nil(t, tasks[0].CompletedAt)
			assert.True(t, state.IsState(tasks[1].State, state.Task.Running),
				"the sibling task is left as found, got %s", tasks[1].State)
			assert.Empty(t, eng.applied, "the resume must refuse before handing any task to the engine")
			stored, err := applies.Get(t.Context(), apply.ID)
			require.NoError(t, err)
			assert.True(t, state.IsState(stored.State, tc.applyState),
				"the resume must refuse before writing the apply running; stored state was %q", stored.State)
			assert.Nil(t, stored.CompletedAt)
			for _, entry := range logs.logs {
				assert.NotEqual(t, storage.LogEventStateTransition, entry.EventType,
					"the durable log must not record a transition the rows do not carry: %q", entry.Message)
			}
		})
	}
}

// sameApplyOperation scopes sibling vouching to one apply operation. Two legacy
// tasks with no operation share the same (absent) operation; a task with an
// operation shares it only with tasks carrying the same identifier, never with
// a legacy task or a task from another operation.
func TestSameApplyOperation(t *testing.T) {
	seven, alsoSeven, eight := int64(7), int64(7), int64(8)
	assert.True(t, sameApplyOperation(nil, nil), "two legacy tasks share the absent operation")
	assert.True(t, sameApplyOperation(&seven, &alsoSeven), "equal identifiers held in different pointers are the same operation")
	assert.False(t, sameApplyOperation(&seven, &eight), "different operations")
	assert.False(t, sameApplyOperation(&seven, nil), "an operation-scoped task never shares with a legacy task")
	assert.False(t, sameApplyOperation(nil, &seven), "a legacy task never shares with an operation-scoped task")
}

// A reverted task is terminal: the revert already landed for it, so the resume
// re-plan leaves it untouched instead of re-activating it. It contributes no
// remaining resume work, and its state must survive the re-plan so the apply's
// terminal state can be derived from it.
func TestReplanAndFilterTasks_RevertedTaskStaysTerminal(t *testing.T) {
	store := &fakePlanStore{getFn: func(string) (*storage.Plan, error) { return nil, nil }}
	c := newPlanMaterializeClientWithPlan(store, alterUsersEmailPlan())

	apply := &storage.Apply{Database: "testapp"}
	tasks := []*storage.Task{{
		TaskIdentifier: "task_1",
		Namespace:      "testapp",
		TableName:      "users",
		DDLAction:      "alter",
		DDL:            "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
		State:          state.Task.Reverted,
	}}

	rp, err := c.replanAndFilterTasks(t.Context(), apply, tasks, &storage.Plan{})
	require.NoError(t, err)
	assert.Empty(t, rp.ActiveTasks, "a reverted task carries no resume work")
	assert.Zero(t, rp.CompletedCount)
	assert.Equal(t, state.Task.Reverted, tasks[0].State, "terminal state is preserved")
}

// A reclaimed revert-phase apply reattaches to the engine like any other
// resume, but its persisted revert-phase states are the durable marker that
// revert-phase handling owns the outcome. The post-reattach persistence must
// not rewrite them to running: a driver death after that write would hand the
// next reclaim a forward-running apply whose live schema matches the reviewed
// target, and the resume re-plan would terminalize it as a success while the
// engine reverts the schema change underneath it.
func TestPersistReattachedResumeStates_PreservesRevertPhase(t *testing.T) {
	cases := []struct {
		name       string
		applyState string
		taskState  string
	}{
		{name: "revert window", applyState: state.Apply.RevertWindow, taskState: state.Task.RevertWindow},
		{name: "reverting", applyState: state.Apply.Reverting, taskState: state.Task.Reverting},
		{name: "skipping revert", applyState: state.Apply.SkippingRevert, taskState: state.Task.RevertWindow},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			apply := &storage.Apply{
				ID:              42,
				ApplyIdentifier: "apply-revert-phase",
				Database:        "testdb",
				DatabaseType:    storage.DatabaseTypeVitess,
				State:           tc.applyState,
			}
			tasks := []*storage.Task{{
				ID:             7,
				ApplyID:        apply.ID,
				TaskIdentifier: "task-revert-phase",
				State:          tc.taskState,
			}}
			applyStore := &exactProgressApplyStore{apply: apply}
			client := &LocalClient{
				config:  LocalConfig{Database: "testdb", Type: storage.DatabaseTypeVitess},
				storage: &exactProgressStorage{applies: applyStore, tasks: &exactProgressTaskStore{tasks: tasks}},
				logger:  slog.Default(),
			}

			err := client.persistReattachedResumeStates(t.Context(), apply, tasks, false, false, "Resumed after reclaim")

			require.NoError(t, err)
			assert.Equal(t, tc.applyState, applyStore.apply.State, "apply keeps its revert-phase state")
			assert.Equal(t, tc.taskState, tasks[0].State, "task keeps its revert-phase state")
		})
	}
}

// Outside a revert phase, the post-reattach persistence projects the resume
// forward: tasks and the apply move to running, or to recovering during a
// deferred-cutover recovery.
func TestPersistReattachedResumeStates_ProjectsForwardStates(t *testing.T) {
	cases := []struct {
		name       string
		applyState string
		taskState  string
		wantApply  string
		wantTask   string
	}{
		{name: "resuming moves to running", applyState: state.Apply.Resuming, taskState: state.Task.Stopped, wantApply: state.Apply.Running, wantTask: state.Task.Running},
		{name: "recovering stays recovering", applyState: state.Apply.Recovering, taskState: state.Task.Recovering, wantApply: state.Apply.Recovering, wantTask: state.Task.Recovering},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			apply := &storage.Apply{
				ID:              42,
				ApplyIdentifier: "apply-forward",
				Database:        "testdb",
				DatabaseType:    storage.DatabaseTypeVitess,
				State:           tc.applyState,
			}
			tasks := []*storage.Task{{
				ID:             7,
				ApplyID:        apply.ID,
				TaskIdentifier: "task-forward",
				State:          tc.taskState,
			}}
			applyStore := &exactProgressApplyStore{apply: apply}
			client := &LocalClient{
				config:  LocalConfig{Database: "testdb", Type: storage.DatabaseTypeVitess},
				storage: &exactProgressStorage{applies: applyStore, tasks: &exactProgressTaskStore{tasks: tasks}},
				logger:  slog.Default(),
			}

			err := client.persistReattachedResumeStates(t.Context(), apply, tasks, false, false, "Resumed after reclaim")

			require.NoError(t, err)
			assert.Equal(t, tc.wantApply, applyStore.apply.State)
			assert.Equal(t, tc.wantTask, tasks[0].State)
		})
	}
}

// deferredDeployGuardEngine satisfies the engine presence check in
// startDeferredDeploy; the mixed-namespace guard must reject the deploy
// before any engine method is reached, so none are implemented.
type deferredDeployGuardEngine struct{ engine.Engine }

// A deferred deploy resolves credentials once from tasks[0] and drives every
// task with them. On an engine that resolves credentials per namespace, tasks
// spanning multiple namespaces must fail closed before the engine is asked to
// start — proceeding would silently run every task against tasks[0]'s schema.
func TestStartDeferredDeployRejectsMixedNamespaces(t *testing.T) {
	apply := &storage.Apply{
		ID:              11,
		ApplyIdentifier: "apply-mixed-namespaces",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		State:           state.Apply.WaitingForDeploy,
	}
	tasks := []*storage.Task{
		{ID: 1, ApplyID: apply.ID, TaskIdentifier: "task-orders", Namespace: "orders", TableName: "users"},
		{ID: 2, ApplyID: apply.ID, TaskIdentifier: "task-billing", Namespace: "billing", TableName: "invoices"},
	}
	client := &LocalClient{
		config:       LocalConfig{Database: "testdb", Type: storage.DatabaseTypeMySQL},
		spiritEngine: deferredDeployGuardEngine{},
		storage: &controlTestStorage{
			tasks: &controlTestTaskStore{tasks: tasks},
		},
		logger: slog.Default(),
	}

	_, err := client.startDeferredDeploy(t.Context(), apply, "")

	require.Error(t, err)
	assert.ErrorContains(t, err, "tasks span multiple namespaces")
	assert.ErrorContains(t, err, apply.ApplyIdentifier)
}
