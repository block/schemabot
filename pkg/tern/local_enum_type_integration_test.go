//go:build integration

package tern

import (
	"database/sql"
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/testutil"
)

// enumTypeFiles declares order_status with two labels the live type lacks,
// one placed before a live label and one appended, next to the table that
// uses it, which gains a column. The table's file sorts ahead of the types
// file.
var enumTypeFiles = map[string]*ternv1.SchemaFiles{"logical": {Files: map[string]string{
	"orders.sql": `CREATE TABLE orders (
    id bigint PRIMARY KEY,
    status order_status NOT NULL,
    note text
);`,
	"status_types.sql": `CREATE TYPE order_status AS ENUM ('new', 'packed', 'shipped', 'delivered');`,
}}}

// newEnumTypeClient starts storage and a PostgreSQL target that holds the
// live type with labels ('new', 'shipped') and a table that uses it, and
// returns a local client whose logical namespace routes to public.
func newEnumTypeClient(t *testing.T, database string) (*LocalClient, storage.Storage, *sql.DB) {
	t.Helper()
	_, storageDSN := setupMySQLContainer(t)
	setupStorageSchema(t, storageDSN)
	cleanupTasks(t, storageDSN)
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TYPE public.order_status AS ENUM ('new', 'shipped');
		CREATE TABLE public.orders (id bigint PRIMARY KEY, status public.order_status NOT NULL)`)
	require.NoError(t, err)
	stor := createStorage(t, storageDSN)
	t.Cleanup(func() { utils.CloseAndLog(stor) })
	client, err := NewLocalClient(LocalConfig{Database: database, Type: storage.DatabaseTypePostgres, TargetDSN: dsn,
		SchemaOverrides: map[string]string{"logical": "public"}}, stor, slog.Default())
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(client) })
	return client, stor, db
}

// orderStatusLabels returns the live labels of public.order_status in sort
// order.
func orderStatusLabels(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `
		SELECT enumlabel FROM pg_enum
		WHERE enumtypid = 'public.order_status'::regtype
		ORDER BY enumsortorder`)
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)
	var labels []string
	for rows.Next() {
		var label string
		require.NoError(t, rows.Scan(&label))
		labels = append(labels, label)
	}
	require.NoError(t, rows.Err())
	return labels
}

// An enum types file and a table change go through stored plan, task
// creation, and the sequential drive against a real PostgreSQL target. The
// label additions are tasks ahead of the table change, run in planned order,
// so the label placed BEFORE a live label and the appended one both land
// where the file lists them, the table gains its column, and a fresh plan
// is empty.
func TestLocalClientEnumTypeFilePlanAndApply(t *testing.T) {
	const database = "enum_apply"
	client, stor, db := newEnumTypeClient(t, database)

	plan, err := client.Plan(t.Context(), &ternv1.PlanRequest{Database: database, Type: storage.DatabaseTypePostgres, SchemaFiles: enumTypeFiles})
	require.NoError(t, err)
	require.Len(t, plan.Changes, 1)
	changes := plan.Changes[0].TableChanges
	require.Len(t, changes, 3)
	wantDDL := []string{
		"ALTER TYPE public.order_status ADD VALUE 'packed' BEFORE 'shipped'",
		"ALTER TYPE public.order_status ADD VALUE 'delivered'",
		"ALTER TABLE public.orders ADD COLUMN note text",
	}
	wantType := []ternv1.ChangeType{ternv1.ChangeType_CHANGE_TYPE_ALTER_TYPE, ternv1.ChangeType_CHANGE_TYPE_ALTER_TYPE, ternv1.ChangeType_CHANGE_TYPE_ALTER}
	for i, change := range changes {
		assert.Equal(t, wantDDL[i], change.Ddl)
		assert.Equal(t, wantType[i], change.ChangeType)
	}

	result, err := client.Apply(t.Context(), &ternv1.ApplyRequest{PlanId: plan.PlanId, Environment: localClientTestEnvironment, DdlChanges: changes})
	require.NoError(t, err)
	require.True(t, result.Accepted, result.ErrorMessage)
	driveQueuedApply(t, stor, client, result.ApplyId)

	apply, err := stor.Applies().GetByApplyIdentifier(t.Context(), result.ApplyId)
	require.NoError(t, err)
	assert.Equal(t, state.Apply.Completed, apply.State, apply.ErrorMessage)
	tasks, err := stor.Tasks().GetByApplyID(t.Context(), apply.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 3)
	wantAction := []string{"alter_type", "alter_type", "alter"}
	for i, task := range tasks {
		assert.Equal(t, state.Task.Completed, task.State, task.ErrorMessage)
		assert.Equal(t, wantDDL[i], task.DDL, "tasks run in planned order")
		assert.Equal(t, wantAction[i], task.DDLAction)
	}
	assert.Equal(t, []string{"new", "packed", "shipped", "delivered"}, orderStatusLabels(t, db))
	_, err = db.ExecContext(t.Context(), `INSERT INTO public.orders (id, status, note) VALUES (1, 'packed', 'first')`)
	require.NoError(t, err, "the new labels and the new column are usable")

	again, err := client.Plan(t.Context(), &ternv1.PlanRequest{Database: database, Type: storage.DatabaseTypePostgres, SchemaFiles: enumTypeFiles})
	require.NoError(t, err)
	assert.Empty(t, again.Changes)
}

// Another session adds a planned label between the plan and the apply. The
// drive stops at the step that would add it again: that task fails with a
// refusal naming the label and saying to re-plan, the apply fails rather
// than retrying the same statement, and the later steps never run. A fresh
// plan shows only what is still missing.
func TestLocalClientEnumTypeFileLabelAddedAfterPlanning(t *testing.T) {
	const database = "enum_apply_stale"
	client, stor, db := newEnumTypeClient(t, database)

	plan, err := client.Plan(t.Context(), &ternv1.PlanRequest{Database: database, Type: storage.DatabaseTypePostgres, SchemaFiles: enumTypeFiles})
	require.NoError(t, err)
	require.Len(t, plan.Changes, 1)
	require.Len(t, plan.Changes[0].TableChanges, 3)

	_, err = db.ExecContext(t.Context(), `ALTER TYPE public.order_status ADD VALUE 'packed' BEFORE 'shipped'`)
	require.NoError(t, err)

	result, err := client.Apply(t.Context(), &ternv1.ApplyRequest{PlanId: plan.PlanId, Environment: localClientTestEnvironment, DdlChanges: plan.Changes[0].TableChanges})
	require.NoError(t, err)
	require.True(t, result.Accepted, result.ErrorMessage)
	driveQueuedApply(t, stor, client, result.ApplyId)

	apply, err := stor.Applies().GetByApplyIdentifier(t.Context(), result.ApplyId)
	require.NoError(t, err)
	assert.Equal(t, state.Apply.Failed, apply.State, "a refused step fails the apply instead of retrying it")
	tasks, err := stor.Tasks().GetByApplyID(t.Context(), apply.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 3)
	assert.Equal(t, state.Task.Failed, tasks[0].State)
	assert.Contains(t, tasks[0].ErrorMessage, `enum type "order_status" already has label "packed", which another change added after planning; re-plan against the current schema`)
	for _, task := range tasks[1:] {
		assert.NotEqual(t, state.Task.Completed, task.State, "steps after the refused one never run")
	}
	assert.Equal(t, []string{"new", "packed", "shipped"}, orderStatusLabels(t, db))

	fresh, err := client.Plan(t.Context(), &ternv1.PlanRequest{Database: database, Type: storage.DatabaseTypePostgres, SchemaFiles: enumTypeFiles})
	require.NoError(t, err)
	require.Len(t, fresh.Changes, 1)
	var freshDDL []string
	for _, change := range fresh.Changes[0].TableChanges {
		freshDDL = append(freshDDL, change.Ddl)
	}
	assert.Equal(t, []string{
		"ALTER TYPE public.order_status ADD VALUE 'delivered'",
		"ALTER TABLE public.orders ADD COLUMN note text",
	}, freshDDL)
}
