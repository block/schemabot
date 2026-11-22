//go:build integration

package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

// ordersTableFile declares a table whose status column uses the enum type,
// matching the live table each test creates, so the table plans clean and
// only the types file can produce a change.
const ordersTableFile = `CREATE TABLE orders (
    id bigint PRIMARY KEY,
    status order_status NOT NULL
);`

// planEnumFiles plans the given schema files for namespace public against a
// fresh database seeded with the live enum type and a table that uses it.
func planEnumFiles(t *testing.T, database, liveLabels string, files map[string]string) (*engine.PlanResult, error) {
	t.Helper()
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TYPE public.order_status AS ENUM `+liveLabels+`;
		CREATE TABLE public.orders (id bigint PRIMARY KEY, status public.order_status NOT NULL)`)
	require.NoError(t, err)
	return New().Plan(t.Context(), &engine.PlanRequest{
		Database:    database,
		SchemaFiles: schema.SchemaFiles{"public": {Files: files}},
		Credentials: &engine.Credentials{DSN: dsn},
	})
}

// A schema directory keeps its enum type in a file next to the table that
// uses it. When the live type already carries every label the file lists,
// the plan is empty: the types file is reviewed like any other schema file,
// not refused.
func TestEnginePlanConvergedEnumTypeFile(t *testing.T) {
	result, err := planEnumFiles(t, "plan_enum_converged_test", `('new', 'shipped')`, map[string]string{
		"order_status.sql": `CREATE TYPE order_status AS ENUM ('new', 'shipped');`,
		"orders.sql":       ordersTableFile,
	})
	require.NoError(t, err)
	assert.True(t, result.NoChanges, "a converged types file plans no change, got %+v", result.Changes)
}

// A types file that lists labels the live type lacks plans one ALTER TYPE
// ... ADD VALUE per label, in declared order and placed before the next live
// label, named by the type. SchemaBot does not apply them yet, so each step
// is blocked with a reason that says how to run it, and the merge gate holds
// until the type carries the labels. The table that uses the type is
// untouched.
func TestEnginePlanEnumTypeFileAddsLabelsBlocked(t *testing.T) {
	result, err := planEnumFiles(t, "plan_enum_add_test", `('new', 'shipped')`, map[string]string{
		"order_status.sql": `CREATE TYPE order_status AS ENUM ('new', 'paid', 'shipped', 'delivered');`,
		"orders.sql":       ordersTableFile,
	})
	require.NoError(t, err)
	require.False(t, result.NoChanges)
	require.Len(t, result.Changes, 1)
	assert.Equal(t, "public", result.Changes[0].Namespace)

	changes := result.Changes[0].TableChanges
	require.Len(t, changes, 2, "one change per added label and nothing for the table")
	wantDDL := []string{
		"ALTER TYPE public.order_status ADD VALUE 'paid' BEFORE 'shipped'",
		"ALTER TYPE public.order_status ADD VALUE 'delivered'",
	}
	wantLabel := []string{"paid", "delivered"}
	for i, change := range changes {
		assert.Equal(t, "order_status", change.Table)
		assert.Equal(t, ddl.StatementAlterType, change.Operation)
		assert.Equal(t, wantDDL[i], change.DDL)
		assert.False(t, change.IsUnsafe, "adding a label destroys nothing")
		assert.Equal(t, engine.ExecutionModeBlocked, change.ExecutionMode)
		assert.Equal(t, `adds label "`+wantLabel[i]+`" to enum type "order_status", which SchemaBot does not apply yet; run its ALTER TYPE outside SchemaBot, then re-plan`, change.ModeReason)
	}
}

// PostgreSQL cannot drop an enum label, so a types file that no longer lists
// a live label has no plan that reaches it. The plan fails, naming the file
// and the label, rather than planning the labels it could add.
func TestEnginePlanEnumTypeFileRefusesADroppedLabel(t *testing.T) {
	_, err := planEnumFiles(t, "plan_enum_drop_test", `('new', 'paid', 'shipped')`, map[string]string{
		"order_status.sql": `CREATE TYPE order_status AS ENUM ('new', 'shipped', 'delivered');`,
		"orders.sql":       ordersTableFile,
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, `plan PostgreSQL schema in "public"/"order_status.sql"`)
	assert.ErrorContains(t, err, `plan PostgreSQL enum type "order_status"`)
	assert.ErrorContains(t, err, "carries ('paid'), which the file no longer lists")
}

// Several types files in one namespace each declare their own type, and none
// declares a table, so they never read as one table declared twice. Two
// files that declare the same type fail the plan, naming both files.
func TestEnginePlanEnumTypeFileDeclarations(t *testing.T) {
	t.Run("distinct types plan side by side", func(t *testing.T) {
		dsn, db := testutil.StartPostgres(t, "plan_enum_distinct_test")
		_, err := db.ExecContext(t.Context(), `
			CREATE TYPE public.order_status AS ENUM ('new', 'shipped');
			CREATE TYPE public.payment_state AS ENUM ('pending', 'settled')`)
		require.NoError(t, err)
		result, err := New().Plan(t.Context(), &engine.PlanRequest{
			Database: "plan_enum_distinct_test",
			SchemaFiles: schema.SchemaFiles{"public": {Files: map[string]string{
				"order_status.sql":  `CREATE TYPE order_status AS ENUM ('new', 'shipped');`,
				"payment_state.sql": `CREATE TYPE payment_state AS ENUM ('pending', 'settled', 'refunded');`,
			}}},
			Credentials: &engine.Credentials{DSN: dsn},
		})
		require.NoError(t, err)
		require.Len(t, result.Changes, 1)
		changes := result.Changes[0].TableChanges
		require.Len(t, changes, 1, "only the type that lacks a label changes")
		assert.Equal(t, "payment_state", changes[0].Table)
		assert.Equal(t, "ALTER TYPE public.payment_state ADD VALUE 'refunded'", changes[0].DDL)
	})

	t.Run("one type declared twice fails the plan", func(t *testing.T) {
		_, err := planEnumFiles(t, "plan_enum_twice_test", `('new', 'shipped')`, map[string]string{
			"order_status.sql":    `CREATE TYPE order_status AS ENUM ('new', 'shipped');`,
			"order_status_v2.sql": `CREATE TYPE order_status AS ENUM ('new', 'paid', 'shipped');`,
			"orders.sql":          ordersTableFile,
		})
		require.EqualError(t, err, `plan PostgreSQL namespace "public": enum type "order_status" is declared by both schema files "order_status.sql" and "order_status_v2.sql". Declare each type in exactly one schema file`)
	})
}
