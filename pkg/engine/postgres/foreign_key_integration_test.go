//go:build integration

package postgres

import (
	"testing"

	"github.com/block/pg-sprite/pkg/schemadiff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

// A schema whose files declare foreign keys plans: the change a foreign key
// does not touch is planned to run, while each foreign-key change, and a new
// table that declares one, is shown for review and blocked, since SchemaBot
// does not apply foreign keys on PostgreSQL yet. A foreign key on a column the
// same plan adds is planned as the column add and a blocked constraint, and
// its remedy has the rest of the plan applied first, because the constraint
// cannot run before its column exists. A dropped foreign key's remedy names
// the table's foreign keys into another namespace only when it has some, since
// a schema file cannot declare those.
func TestEnginePlanForeignKeys(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "plan_foreign_keys_test")
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.orders (id bigint PRIMARY KEY, user_id bigint NOT NULL);
		CREATE TABLE public.payments (id bigint PRIMARY KEY, order_id bigint NOT NULL REFERENCES public.orders (id));
		CREATE TABLE public.refunds (id bigint PRIMARY KEY);
		CREATE SCHEMA billing;
		CREATE TABLE billing.accounts (id bigint PRIMARY KEY);
		CREATE TABLE public.invoices (id bigint PRIMARY KEY, account_id bigint NOT NULL REFERENCES billing.accounts (id))`)
	require.NoError(t, err)
	req := &engine.PlanRequest{
		Database: "plan_foreign_keys_test",
		SchemaFiles: schema.SchemaFiles{
			"public": {Files: map[string]string{
				"users.sql":     "CREATE TABLE users (id bigint PRIMARY KEY)",
				"orders.sql":    "CREATE TABLE orders (id bigint PRIMARY KEY, user_id bigint NOT NULL REFERENCES users (id), note text)",
				"payments.sql":  "CREATE TABLE payments (id bigint PRIMARY KEY, order_id bigint NOT NULL)",
				"refunds.sql":   "CREATE TABLE refunds (id bigint PRIMARY KEY, order_id bigint REFERENCES orders (id))",
				"invoices.sql":  "CREATE TABLE invoices (id bigint PRIMARY KEY, account_id bigint NOT NULL)",
				"shipments.sql": "CREATE TABLE shipments (id bigint PRIMARY KEY, order_id bigint NOT NULL REFERENCES orders (id))",
			}},
		},
		Credentials: &engine.Credentials{DSN: dsn},
	}

	result, err := New().Plan(t.Context(), req)
	require.NoError(t, err)

	byDDL := map[string]engine.TableChange{}
	assert.Len(t, result.Changes, 1)
	for _, sc := range result.Changes {
		for _, tc := range sc.TableChanges {
			byDDL[tc.DDL] = tc
		}
	}

	addColumn, ok := byDDL["ALTER TABLE public.orders ADD COLUMN note text"]
	require.True(t, ok, "the column add is planned")
	assert.NotEqual(t, engine.ExecutionModeBlocked, addColumn.ExecutionMode)

	addFK, ok := byDDL["ALTER TABLE public.orders ADD CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users (id)"]
	require.True(t, ok, "the foreign-key add is planned")
	assert.Equal(t, engine.ExecutionModeBlocked, addFK.ExecutionMode)
	assert.Equal(t, `statement for table "orders" adds a foreign key, which SchemaBot plans but does not apply on PostgreSQL yet; leave the foreign key out of the schema file to apply the rest of the plan, then run this statement outside SchemaBot and add the foreign key back to the file`, addFK.ModeReason)

	addRefundColumn, ok := byDDL["ALTER TABLE public.refunds ADD COLUMN order_id bigint"]
	require.True(t, ok, "the new column is planned on its own")
	assert.NotEqual(t, engine.ExecutionModeBlocked, addRefundColumn.ExecutionMode)
	addRefundFK, ok := byDDL["ALTER TABLE public.refunds ADD CONSTRAINT refunds_order_id_fkey FOREIGN KEY (order_id) REFERENCES public.orders (id)"]
	require.True(t, ok, "the foreign key on the new column is planned as its own statement")
	assert.Equal(t, engine.ExecutionModeBlocked, addRefundFK.ExecutionMode)
	assert.Contains(t, addRefundFK.ModeReason, "leave the foreign key out of the schema file to apply the rest of the plan")

	dropFK, ok := byDDL["ALTER TABLE public.payments DROP CONSTRAINT payments_order_id_fkey"]
	require.True(t, ok, "the undeclared live foreign key is planned as a drop")
	assert.Equal(t, engine.ExecutionModeBlocked, dropFK.ExecutionMode)
	assert.Equal(t, `statement for table "payments" drops a foreign key, which SchemaBot plans but does not apply on PostgreSQL yet; to keep the foreign key, declare it in the schema file and re-plan; to remove it, run this statement outside SchemaBot, then re-plan`, dropFK.ModeReason)

	dropCrossNamespaceFK, ok := byDDL["ALTER TABLE public.invoices DROP CONSTRAINT invoices_account_id_fkey"]
	require.True(t, ok, "the undeclared foreign key into another namespace is planned as a drop")
	assert.Equal(t, engine.ExecutionModeBlocked, dropCrossNamespaceFK.ExecutionMode)
	assert.Equal(t, `statement for table "invoices" drops a foreign key, which SchemaBot plans but does not apply on PostgreSQL yet; to keep the foreign key, declare it in the schema file and re-plan; to remove it, run this statement outside SchemaBot, then re-plan; foreign key constraint(s) "invoices_account_id_fkey" reference another namespace and cannot be declared, so the plan stays blocked until they are removed through a separately reviewed process`, dropCrossNamespaceFK.ModeReason)

	create, ok := byDDL["CREATE TABLE public.shipments (id bigint PRIMARY KEY, order_id bigint NOT NULL REFERENCES orders (id))"]
	require.True(t, ok, "the new table is planned")
	assert.Equal(t, engine.ExecutionModeBlocked, create.ExecutionMode)
	assert.Contains(t, create.ModeReason, `statement for table "shipments" declares a foreign key, which SchemaBot plans but does not apply on PostgreSQL yet`)
}

// A schema file whose foreign key references a table that does not exist on
// the target fails the plan, even when another file in the same plan creates
// that table, because the planner binds each foreign key to the live
// referenced table's shape. The error names the fix: leave the foreign key
// out of the file, apply the referenced table first, then add the foreign key
// outside SchemaBot.
func TestEnginePlanForeignKeyToMissingTable(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "plan_foreign_key_missing_test")
	_, err := db.ExecContext(t.Context(), `CREATE TABLE public.orders (id bigint PRIMARY KEY, customer_id bigint NOT NULL)`)
	require.NoError(t, err)
	req := &engine.PlanRequest{
		Database: "plan_foreign_key_missing_test",
		SchemaFiles: schema.SchemaFiles{
			"public": {Files: map[string]string{
				"customers.sql": "CREATE TABLE customers (id bigint PRIMARY KEY)",
				"orders.sql":    "CREATE TABLE orders (id bigint PRIMARY KEY, customer_id bigint NOT NULL REFERENCES customers (id))",
			}},
		},
		Credentials: &engine.Credentials{DSN: dsn},
	}

	_, err = New().Plan(t.Context(), req)
	require.Error(t, err)
	assert.ErrorIs(t, err, schemadiff.ErrReferencedTableNotFound)
	assert.Contains(t, err.Error(), `plan PostgreSQL schema in "public"/"orders.sql"`)
	assert.Contains(t, err.Error(), "public.customers")
	assert.Contains(t, err.Error(), "a foreign key can reference only a table that already exists on the target, so leave the foreign key out of the schema file, apply the referenced table first, then run the ADD CONSTRAINT outside SchemaBot and add the foreign key back to the file")
}
