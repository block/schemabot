//go:build integration

package postgres

import (
	"database/sql"
	"testing"

	"github.com/block/pg-sprite/pkg/schemadiff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

// A schema whose files declare foreign keys plans: an added foreign key, on
// an existing column or one the same plan adds, is planned to run, and so is a
// new table that declares one. A live foreign key the file leaves out is shown
// for review and blocked, since SchemaBot does not drop foreign keys on
// PostgreSQL yet. A dropped foreign key's remedy names the table's foreign
// keys into another namespace only when it has some, since a schema file
// cannot declare those.
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

	// An added foreign key is two steps, in this order: the key is added
	// without checking existing rows, then validated while writes flow.
	assert.Equal(t, []string{
		"ALTER TABLE public.orders ADD COLUMN note text",
		"ALTER TABLE public.orders ADD CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users (id) NOT VALID",
		`ALTER TABLE "public"."orders" VALIDATE CONSTRAINT "orders_user_id_fkey"`,
	}, executableDDL(t, result, "orders"))
	assert.Equal(t, []string{
		"ALTER TABLE public.refunds ADD COLUMN order_id bigint",
		"ALTER TABLE public.refunds ADD CONSTRAINT refunds_order_id_fkey FOREIGN KEY (order_id) REFERENCES public.orders (id) NOT VALID",
		`ALTER TABLE "public"."refunds" VALIDATE CONSTRAINT "refunds_order_id_fkey"`,
	}, executableDDL(t, result, "refunds"), "the foreign key on a new column runs after the column is added")

	dropFK, ok := byDDL["ALTER TABLE public.payments DROP CONSTRAINT payments_order_id_fkey"]
	require.True(t, ok, "the undeclared live foreign key is planned as a drop")
	assert.Equal(t, engine.ExecutionModeBlocked, dropFK.ExecutionMode)
	assert.Equal(t, `statement for table "payments" drops a foreign key, which SchemaBot plans but does not apply on PostgreSQL yet; to keep the foreign key, declare it in the schema file and re-plan; to remove it, run this statement outside SchemaBot, then re-plan`, dropFK.ModeReason)

	dropCrossNamespaceFK, ok := byDDL["ALTER TABLE public.invoices DROP CONSTRAINT invoices_account_id_fkey"]
	require.True(t, ok, "the undeclared foreign key into another namespace is planned as a drop")
	assert.Equal(t, engine.ExecutionModeBlocked, dropCrossNamespaceFK.ExecutionMode)
	assert.Equal(t, `statement for table "invoices" drops a foreign key, which SchemaBot plans but does not apply on PostgreSQL yet; to keep the foreign key, declare it in the schema file and re-plan; to remove it, run this statement outside SchemaBot, then re-plan; foreign key constraint(s) "invoices_account_id_fkey" reference another namespace and cannot be declared, so the plan stays blocked until they are removed through a separately reviewed process`, dropCrossNamespaceFK.ModeReason)

	assert.Equal(t, []string{
		"CREATE TABLE public.shipments (id bigint PRIMARY KEY, order_id bigint NOT NULL REFERENCES orders (id))",
	}, executableDDL(t, result, "shipments"), "the new table is created with its foreign key")
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
	assert.True(t, engine.IsPlanRefused(err))
	assert.Contains(t, err.Error(), `plan PostgreSQL schema in "public"/"orders.sql"`)
	assert.Contains(t, err.Error(), "public.customers")
	assert.Contains(t, err.Error(), "a foreign key can reference only a table that already exists on the target, so apply the referenced table first, then add the foreign key to the file in a later change")
}

// executableDDL returns the DDL of the table's planned changes, in plan order,
// and fails the test if any of them is blocked.
func executableDDL(t *testing.T, result *engine.PlanResult, table string) []string {
	t.Helper()
	var ddl []string
	for _, sc := range result.Changes {
		for _, tc := range sc.TableChanges {
			if tc.Table != table {
				continue
			}
			assert.NotEqual(t, engine.ExecutionModeBlocked, tc.ExecutionMode, "%s: %s", tc.DDL, tc.ModeReason)
			ddl = append(ddl, tc.DDL)
		}
	}
	return ddl
}

// planForeignKeyFiles plans files against the database behind dsn.
func planForeignKeyFiles(t *testing.T, eng *Engine, database, dsn string, files map[string]string) *engine.PlanResult {
	t.Helper()
	result, err := eng.Plan(t.Context(), &engine.PlanRequest{
		Database:    database,
		SchemaFiles: schema.SchemaFiles{"public": {Files: files}},
		Credentials: &engine.Credentials{DSN: dsn},
	})
	require.NoError(t, err)
	return result
}

// foreignKeyValidated reports whether the named constraint exists and has been
// validated.
func foreignKeyValidated(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var validated bool
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT convalidated FROM pg_constraint WHERE conname = $1 AND contype = 'f'`, name).Scan(&validated))
	return validated
}

// An owner declares foreign keys in the schema files: one on an existing
// table, and one on a new table. The apply adds the existing table's key
// without checking existing rows under a lock, then validates it while writes
// keep flowing, and creates the new table with its key. Both keys are
// validated and enforced afterward, and a fresh plan is empty.
func TestEngineApplyForeignKeys(t *testing.T) {
	const database = "apply_foreign_keys_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.orders (id bigint PRIMARY KEY, user_id bigint NOT NULL);
		INSERT INTO public.users VALUES (1);
		INSERT INTO public.orders VALUES (10, 1)`)
	require.NoError(t, err)
	files := map[string]string{
		"users.sql":     "CREATE TABLE users (id bigint PRIMARY KEY)",
		"orders.sql":    "CREATE TABLE orders (id bigint PRIMARY KEY, user_id bigint NOT NULL REFERENCES users (id))",
		"shipments.sql": "CREATE TABLE shipments (id bigint PRIMARY KEY, order_id bigint NOT NULL REFERENCES orders (id))",
	}
	eng := New()

	plan := planForeignKeyFiles(t, eng, database, dsn, files)
	outcomes := applyPlannedSteps(t, eng, dsn, plan)
	require.Len(t, outcomes, 3, "add the orders key, validate it, create shipments")
	for _, progress := range outcomes {
		assert.Equal(t, engine.StateCompleted, progress.State, progress.ErrorMessage)
	}

	assert.True(t, foreignKeyValidated(t, db, "orders_user_id_fkey"))
	assert.True(t, foreignKeyValidated(t, db, "shipments_order_id_fkey"))
	_, err = db.ExecContext(t.Context(), `INSERT INTO public.orders VALUES (11, 2)`)
	require.Error(t, err, "the added key is enforced")
	assert.Contains(t, err.Error(), "orders_user_id_fkey")

	replan := planForeignKeyFiles(t, eng, database, dsn, files)
	assert.True(t, replan.NoChanges, "the applied keys match the files, got %+v", replan.Changes)
}

// An owner adds a foreign key that an existing row breaks. The apply adds the
// key without checking existing rows, then fails at the validation, naming the
// key and the fix. The key stays in place unvalidated, so new writes are
// already checked. Once the owner fixes the row, the next plan is only the
// validation, and applying it completes.
func TestEngineApplyForeignKeyOverABrokenRow(t *testing.T) {
	const database = "apply_foreign_key_orphan_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.orders (id bigint PRIMARY KEY, user_id bigint NOT NULL);
		INSERT INTO public.users VALUES (1);
		INSERT INTO public.orders VALUES (10, 1), (11, 99)`)
	require.NoError(t, err)
	files := map[string]string{
		"users.sql":  "CREATE TABLE users (id bigint PRIMARY KEY)",
		"orders.sql": "CREATE TABLE orders (id bigint PRIMARY KEY, user_id bigint NOT NULL REFERENCES users (id))",
	}
	eng := New()

	outcomes := applyPlannedSteps(t, eng, dsn, planForeignKeyFiles(t, eng, database, dsn, files))
	require.Len(t, outcomes, 2, "the add completes and the validation fails")
	assert.Equal(t, engine.StateCompleted, outcomes[0].State, outcomes[0].ErrorMessage)
	assert.Equal(t, engine.StateFailed, outcomes[1].State)
	assert.Equal(t, `existing rows in "orders" break foreign key "orders_user_id_fkey"; the key is in place and checks new writes; fix those rows, then retry`, outcomes[1].ErrorMessage)
	assert.False(t, foreignKeyValidated(t, db, "orders_user_id_fkey"), "the key stays in place, unvalidated")
	_, err = db.ExecContext(t.Context(), `INSERT INTO public.orders VALUES (12, 98)`)
	require.Error(t, err, "new writes are checked before the key is validated")

	_, err = db.ExecContext(t.Context(), `UPDATE public.orders SET user_id = 1 WHERE id = 11`)
	require.NoError(t, err)
	replan := planForeignKeyFiles(t, eng, database, dsn, files)
	require.Len(t, replan.Changes, 1)
	require.Len(t, replan.Changes[0].TableChanges, 1)
	assert.Equal(t, "ALTER TABLE public.orders VALIDATE CONSTRAINT orders_user_id_fkey", replan.Changes[0].TableChanges[0].DDL)

	outcomes = applyPlannedSteps(t, eng, dsn, replan)
	require.Len(t, outcomes, 1)
	assert.Equal(t, engine.StateCompleted, outcomes[0].State, outcomes[0].ErrorMessage)
	assert.True(t, foreignKeyValidated(t, db, "orders_user_id_fkey"))
}
