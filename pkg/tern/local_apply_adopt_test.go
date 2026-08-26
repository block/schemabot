package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

// newMultisetClient builds the minimal client the change-set reconstruction
// needs: the configured database and type are what normalize a namespace, so
// both sides of a comparison agree on the name of an unnamed schema.
func newMultisetClient(database string, dbType string) *LocalClient {
	return &LocalClient{config: LocalConfig{Database: database, Type: dbType}}
}

func alterTask(identifier, namespace, shard, table, ddl string) *storage.Task {
	return &storage.Task{
		TaskIdentifier: identifier,
		Namespace:      namespace,
		Shard:          shard,
		TableName:      table,
		DDL:            ddl,
		DDLAction:      "alter",
	}
}

func alterChange(namespace, table, ddl string) storage.TableChange {
	return storage.TableChange{
		Namespace: namespace,
		Table:     table,
		DDL:       ddl,
		Operation: "alter",
	}
}

// A non-sharded apply's stored task rows reconstruct to the same change set the
// dispatch that created them would admit, so re-applying the same schema change
// against the apply already running it compares as identical rather than as a
// different change that happens to overlap.
func TestChangeSetReconstructionMatchesDispatchNonSharded(t *testing.T) {
	client := newMultisetClient("testdb", storage.DatabaseTypeMySQL)

	tasks := []*storage.Task{
		alterTask("task-1", "", "", "users", "ALTER TABLE users ADD COLUMN email VARCHAR(255)"),
		alterTask("task-2", "", "", "orders", "ALTER TABLE orders ADD COLUMN total INT"),
	}
	scope := dispatchScope{ddlChanges: []storage.TableChange{
		alterChange("testdb", "users", "ALTER TABLE users ADD COLUMN email VARCHAR(255)"),
		alterChange("testdb", "orders", "ALTER TABLE orders ADD COLUMN total INT"),
	}}

	fromTasks, err := client.driftMultisetFromTasks(tasks, "")
	require.NoError(t, err)
	fromScope, err := client.driftMultisetFromDispatchScope(scope)
	require.NoError(t, err)

	require.Len(t, fromTasks, 2)
	assert.Equal(t, fromTasks, fromScope)
	assert.NoError(t, compareDriftMultisets(fromTasks, fromScope))
}

// A DDL statement that differs only in formatting is the same change, so an
// apply's rows and a dispatch that writes the statement differently still
// compare as identical: both sides canonicalize before keying.
func TestChangeSetReconstructionIgnoresDDLFormatting(t *testing.T) {
	client := newMultisetClient("testdb", storage.DatabaseTypeMySQL)

	fromTasks, err := client.driftMultisetFromTasks([]*storage.Task{
		alterTask("task-1", "", "", "users", "ALTER TABLE users ADD COLUMN email VARCHAR(255)"),
	}, "")
	require.NoError(t, err)
	fromScope, err := client.driftMultisetFromDispatchScope(dispatchScope{
		ddlChanges: []storage.TableChange{
			alterChange("testdb", "users", "alter table `users`\n  add column `email` varchar(255)"),
		},
	})
	require.NoError(t, err)

	assert.NoError(t, compareDriftMultisets(fromTasks, fromScope))
}

// A dispatch that alters a different column than the apply already running is a
// different change set, so it must not compare as identical — the comparison
// keys on canonicalized DDL, not just on the table being altered.
func TestChangeSetReconstructionDetectsDifferentDDL(t *testing.T) {
	client := newMultisetClient("testdb", storage.DatabaseTypeMySQL)

	fromTasks, err := client.driftMultisetFromTasks([]*storage.Task{
		alterTask("task-1", "", "", "users", "ALTER TABLE users ADD COLUMN email VARCHAR(255)"),
	}, "")
	require.NoError(t, err)
	fromScope, err := client.driftMultisetFromDispatchScope(dispatchScope{
		ddlChanges: []storage.TableChange{
			alterChange("testdb", "users", "ALTER TABLE users ADD COLUMN phone VARCHAR(32)"),
		},
	})
	require.NoError(t, err)

	assert.Error(t, compareDriftMultisets(fromTasks, fromScope))
}

// A dispatch covering fewer tables than the apply already running is not the
// same change set, so a partial overlap does not compare as identical.
func TestChangeSetReconstructionDetectsPartialOverlap(t *testing.T) {
	client := newMultisetClient("testdb", storage.DatabaseTypeMySQL)

	fromTasks, err := client.driftMultisetFromTasks([]*storage.Task{
		alterTask("task-1", "", "", "users", "ALTER TABLE users ADD COLUMN email VARCHAR(255)"),
		alterTask("task-2", "", "", "orders", "ALTER TABLE orders ADD COLUMN total INT"),
	}, "")
	require.NoError(t, err)
	fromScope, err := client.driftMultisetFromDispatchScope(dispatchScope{
		ddlChanges: []storage.TableChange{
			alterChange("testdb", "users", "ALTER TABLE users ADD COLUMN email VARCHAR(255)"),
		},
	})
	require.NoError(t, err)

	assert.Error(t, compareDriftMultisets(fromTasks, fromScope))
}

// A sharded apply is dispatched one shard at a time, so a shard-scoped dispatch
// reconstructs only the shard it targets. The apply's other shards are separate
// physical primaries whose rows, including reflected per-shard progress, are not
// part of this comparison.
func TestChangeSetReconstructionScopesToDispatchShard(t *testing.T) {
	client := newMultisetClient("commerce", storage.DatabaseTypeVitess)

	tasks := []*storage.Task{
		alterTask("task-80-", "commerce", "80-", "users", "ALTER TABLE users ADD COLUMN email VARCHAR(255)"),
		alterTask("task--80", "commerce", "-80", "users", "ALTER TABLE users ADD COLUMN email VARCHAR(255)"),
	}
	scope := dispatchScope{
		shard: "-80",
		ddlChanges: []storage.TableChange{
			alterChange("commerce", "users", "ALTER TABLE users ADD COLUMN email VARCHAR(255)"),
		},
	}

	fromTasks, err := client.driftMultisetFromTasks(tasks, scope.shard)
	require.NoError(t, err)
	fromScope, err := client.driftMultisetFromDispatchScope(scope)
	require.NoError(t, err)

	require.Len(t, fromTasks, 1)
	assert.NoError(t, compareDriftMultisets(fromTasks, fromScope))
}

// A non-sharded dispatch and a sharded apply describe work on different
// targets, so they never compare as the same change set even when the table and
// DDL match.
func TestChangeSetReconstructionRefusesShardShapeMismatch(t *testing.T) {
	client := newMultisetClient("commerce", storage.DatabaseTypeVitess)

	fromTasks, err := client.driftMultisetFromTasks([]*storage.Task{
		alterTask("task--80", "commerce", "-80", "users", "ALTER TABLE users ADD COLUMN email VARCHAR(255)"),
	}, "")
	require.NoError(t, err)
	fromScope, err := client.driftMultisetFromDispatchScope(dispatchScope{
		ddlChanges: []storage.TableChange{
			alterChange("commerce", "users", "ALTER TABLE users ADD COLUMN email VARCHAR(255)"),
		},
	})
	require.NoError(t, err)

	assert.Empty(t, fromTasks)
	assert.Error(t, compareDriftMultisets(fromTasks, fromScope))
}

// A VSchema-only dispatch is a task-less finalizer: it carries no table DDL, so
// it reconstructs to an empty change set that describes nothing and can never
// stand in for an apply's work.
func TestChangeSetReconstructionOfVSchemaOnlyDispatchIsEmpty(t *testing.T) {
	client := newMultisetClient("commerce", storage.DatabaseTypeVitess)

	fromScope, err := client.driftMultisetFromDispatchScope(dispatchScope{
		finalizer:          true,
		finalizerNamespace: "commerce",
	})
	require.NoError(t, err)

	assert.Empty(t, fromScope)
}

// The reconstruction fails closed on any row it cannot key exactly, so an apply
// whose change set cannot be rebuilt is never mistaken for one that matches.
func TestChangeSetReconstructionFailsClosedOnUnkeyableTask(t *testing.T) {
	client := newMultisetClient("testdb", storage.DatabaseTypeMySQL)

	cases := []struct {
		name string
		task *storage.Task
	}{
		{"nil row", nil},
		{"no table", &storage.Task{TaskIdentifier: "task-1", DDL: "ALTER TABLE users ADD COLUMN email VARCHAR(255)", DDLAction: "alter"}},
		{"no operation", &storage.Task{TaskIdentifier: "task-1", TableName: "users", DDL: "ALTER TABLE users ADD COLUMN email VARCHAR(255)"}},
		{"unparseable DDL", alterTask("task-1", "", "", "users", "ALTER TABLE users ADD COLUMN")},
		{"multi-statement DDL", alterTask("task-1", "", "", "users", "ALTER TABLE users ADD COLUMN a INT; ALTER TABLE users ADD COLUMN b INT")},
		{"non-DDL", alterTask("task-1", "", "", "users", "SELECT 1")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.driftMultisetFromTasks([]*storage.Task{tc.task}, "")
			assert.Error(t, err)
		})
	}
}

// The dispatch side fails closed on the same input the task side rejects, so
// both sides of a comparison are built to one standard.
func TestChangeSetReconstructionFailsClosedOnUnkeyableDispatchChange(t *testing.T) {
	client := newMultisetClient("testdb", storage.DatabaseTypeMySQL)

	cases := []struct {
		name   string
		change storage.TableChange
	}{
		{"no table", storage.TableChange{DDL: "ALTER TABLE users ADD COLUMN email VARCHAR(255)", Operation: "alter"}},
		{"no operation", storage.TableChange{Table: "users", DDL: "ALTER TABLE users ADD COLUMN email VARCHAR(255)"}},
		{"unparseable DDL", alterChange("testdb", "users", "ALTER TABLE users ADD COLUMN")},
		{"non-DDL", alterChange("testdb", "users", "SELECT 1")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.driftMultisetFromDispatchScope(dispatchScope{ddlChanges: []storage.TableChange{tc.change}})
			assert.Error(t, err)
		})
	}
}

// A task row that records no namespace belongs to the configured database, the
// same normalization the dispatch side applies, so an unnamed schema on one
// side and the database name on the other are the same namespace.
func TestChangeSetReconstructionNormalizesNamespace(t *testing.T) {
	client := newMultisetClient("testdb", storage.DatabaseTypeMySQL)

	fromTasks, err := client.driftMultisetFromTasks([]*storage.Task{
		alterTask("task-1", "", "", "users", "ALTER TABLE users ADD COLUMN email VARCHAR(255)"),
	}, "")
	require.NoError(t, err)
	fromScope, err := client.driftMultisetFromDispatchScope(dispatchScope{
		ddlChanges: []storage.TableChange{
			alterChange("default", "users", "ALTER TABLE users ADD COLUMN email VARCHAR(255)"),
		},
	})
	require.NoError(t, err)

	assert.NoError(t, compareDriftMultisets(fromTasks, fromScope))
}
