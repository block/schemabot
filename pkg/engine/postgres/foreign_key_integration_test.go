//go:build integration

package postgres

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/block/pg-sprite/pkg/schemadiff"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

// A schema whose files declare foreign keys plans: an added foreign key, on
// an existing column or one the same plan adds, is planned to run, and so is a
// new table that declares one. A live foreign key the file leaves out is
// planned to run as an unsafe drop. A live foreign key into another namespace
// is blocked instead, since a schema file cannot declare it and every plan
// would drop it whatever the file says; the reason names the key.
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

	assert.Equal(t, []string{
		"ALTER TABLE public.payments DROP CONSTRAINT payments_order_id_fkey",
	}, executableDDL(t, result, "payments"), "the undeclared live foreign key is planned to run as a drop")
	dropFK := byDDL["ALTER TABLE public.payments DROP CONSTRAINT payments_order_id_fkey"]
	assert.True(t, dropFK.IsUnsafe, "a foreign-key drop needs --allow-unsafe")
	assert.Equal(t, `statement removes live structure from table "payments"`, dropFK.UnsafeReason)
	assert.Contains(t, dropFK.Metadata, reviewedDropMetadataKey, "the plan records the key it reviewed, so the apply can check it")

	dropCrossNamespaceFK, ok := byDDL["ALTER TABLE public.invoices DROP CONSTRAINT invoices_account_id_fkey"]
	require.True(t, ok, "the undeclared foreign key into another namespace is planned as a drop")
	assert.Equal(t, engine.ExecutionModeBlocked, dropCrossNamespaceFK.ExecutionMode)
	assert.Equal(t, `statement for table "invoices" drops foreign key "invoices_account_id_fkey" into another namespace, which a schema file cannot declare, so SchemaBot does not apply the drop; remove the key outside SchemaBot through a separately reviewed process, then re-plan`, dropCrossNamespaceFK.ModeReason)

	assert.Equal(t, []string{
		"CREATE TABLE public.shipments (id bigint PRIMARY KEY, order_id bigint NOT NULL REFERENCES public.orders (id))",
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
// key and the fix, and ends the apply rather than retrying it. The engine's
// steps leave the key unvalidated, checking new writes; the drive's release
// of a failed apply is what removes it. When the key stays, because it was
// already on the table, the next plan once the owner fixes the row is only the
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
	assert.Equal(t, `existing rows in "orders" break foreign key "orders_user_id_fkey"; fix those rows, then apply again`, outcomes[1].ErrorMessage)
	assert.False(t, outcomes[1].Retryable, "a retry cannot pass until someone fixes the row, so the apply ends at once")
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

// An owner adds a foreign key to a table whose validation scan outlasts the
// deployment's validation bound. The apply adds the key, then the scan is
// cancelled at the bound and the step is refused, naming the option that
// grants more time. The key stays in place unvalidated and checks new writes.
// Once the bound is raised, the next plan is only the validation, and
// applying it completes. The bound is one the catalog drift check ahead of the
// scan fits inside, so the refusal is the scan's own cut-off.
func TestEngineApplyForeignKeyValidationPastItsBound(t *testing.T) {
	const bound = 100 * time.Millisecond
	const database = "apply_foreign_key_bound_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.orders (id bigint PRIMARY KEY, user_id bigint NOT NULL);
		INSERT INTO public.users SELECT g FROM generate_series(1, 1000) g;
		INSERT INTO public.orders SELECT g, 1 + g % 1000 FROM generate_series(1, 2000000) g;
		ANALYZE public.orders`)
	require.NoError(t, err)
	files := map[string]string{
		"users.sql":  "CREATE TABLE users (id bigint PRIMARY KEY)",
		"orders.sql": "CREATE TABLE orders (id bigint PRIMARY KEY, user_id bigint NOT NULL REFERENCES users (id))",
	}
	eng := New().WithValidateMaxDuration(bound)

	outcomes := applyPlannedSteps(t, eng, dsn, planForeignKeyFiles(t, eng, database, dsn, files))
	require.Len(t, outcomes, 2, "the add completes and the validation is refused")
	assert.Equal(t, engine.StateCompleted, outcomes[0].State, outcomes[0].ErrorMessage)
	assert.Equal(t, engine.StateFailed, outcomes[1].State)
	assert.Contains(t, outcomes[1].ErrorMessage, `constraint "orders_user_id_fkey" ran past postgres.validate_max_duration (100ms)`)
	assert.Contains(t, outcomes[1].ErrorMessage, "raise postgres.validate_max_duration and re-run")
	assert.False(t, foreignKeyValidated(t, db, "orders_user_id_fkey"), "the key stays in place, unvalidated")
	_, err = db.ExecContext(t.Context(), `INSERT INTO public.orders VALUES (2000001, 5000)`)
	require.Error(t, err, "new writes are checked before the key is validated")

	raised := New()
	replan := planForeignKeyFiles(t, raised, database, dsn, files)
	require.Len(t, replan.Changes, 1)
	require.Len(t, replan.Changes[0].TableChanges, 1)
	assert.Equal(t, "ALTER TABLE public.orders VALIDATE CONSTRAINT orders_user_id_fkey", replan.Changes[0].TableChanges[0].DDL)
	outcomes = applyPlannedSteps(t, raised, dsn, replan)
	require.Len(t, outcomes, 1)
	assert.Equal(t, engine.StateCompleted, outcomes[0].State, outcomes[0].ErrorMessage)
	assert.True(t, foreignKeyValidated(t, db, "orders_user_id_fkey"))
}

// A table past the deployment's native-safe size ceiling has a foreign key
// left NOT VALID. The plan validates it in place, and that step is not
// blocked for the table's size: the scan lets reads and writes continue and
// runs under the validation bound instead. Applying it validates the key.
func TestEnginePlanValidationOnATablePastTheSizeCeiling(t *testing.T) {
	const database = "plan_validation_size_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.orders (id bigint PRIMARY KEY, user_id bigint NOT NULL);
		INSERT INTO public.users SELECT g FROM generate_series(1, 100) g;
		INSERT INTO public.orders SELECT g, 1 + g % 100 FROM generate_series(1, 1000) g;
		ALTER TABLE public.orders ADD CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users (id) NOT VALID`)
	require.NoError(t, err)
	files := map[string]string{
		"users.sql":  "CREATE TABLE users (id bigint PRIMARY KEY)",
		"orders.sql": "CREATE TABLE orders (id bigint PRIMARY KEY, user_id bigint NOT NULL REFERENCES users (id))",
	}
	eng := NewWithTableSizeLimit(1)

	plan := planForeignKeyFiles(t, eng, database, dsn, files)
	require.Len(t, plan.Changes, 1)
	require.Len(t, plan.Changes[0].TableChanges, 1, "only the validation is left to run")
	step := plan.Changes[0].TableChanges[0]
	assert.Contains(t, step.DDL, "VALIDATE CONSTRAINT")
	assert.Empty(t, step.ExecutionMode, step.ModeReason)

	outcomes := applyPlannedSteps(t, eng, dsn, plan)
	require.Len(t, outcomes, 1)
	assert.Equal(t, engine.StateCompleted, outcomes[0].State, outcomes[0].ErrorMessage)
	assert.True(t, foreignKeyValidated(t, db, "orders_user_id_fkey"))
}

// A deployment sets a validation bound shorter than the lock budget. The
// validation queues behind another session's lock on the table, and its lock
// wait, half the bound, ends before the bound's statement timer, so the step
// fails as lock contention the drive retries, not as a scan that ran past its
// bound. Once the lock is released, applying the validation again completes.
func TestEngineApplyValidationBehindALockIsRetriedNotRefused(t *testing.T) {
	const database = "apply_foreign_key_lock_wait_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.orders (id bigint PRIMARY KEY, user_id bigint NOT NULL);
		INSERT INTO public.users SELECT g FROM generate_series(1, 100) g;
		INSERT INTO public.orders SELECT g, 1 + g % 100 FROM generate_series(1, 1000) g`)
	require.NoError(t, err)
	files := map[string]string{
		"users.sql":  "CREATE TABLE users (id bigint PRIMARY KEY)",
		"orders.sql": "CREATE TABLE orders (id bigint PRIMARY KEY, user_id bigint NOT NULL REFERENCES users (id))",
	}
	plan := planForeignKeyFiles(t, New(), database, dsn, files)
	require.Len(t, plan.Changes, 1)
	require.Len(t, plan.Changes[0].TableChanges, 2, "the key is added NOT VALID, then validated")
	add, validate := *plan, *plan
	add.Changes = []engine.SchemaChange{{Namespace: plan.Changes[0].Namespace, TableChanges: plan.Changes[0].TableChanges[:1]}}
	validate.Changes = []engine.SchemaChange{{Namespace: plan.Changes[0].Namespace, TableChanges: plan.Changes[0].TableChanges[1:]}}
	outcomes := applyPlannedSteps(t, New(), dsn, &add)
	require.Len(t, outcomes, 1)
	require.Equal(t, engine.StateCompleted, outcomes[0].State, outcomes[0].ErrorMessage)

	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	defer utils.CloseAndLog(conn)
	tx, err := conn.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(t.Context(), "LOCK TABLE public.orders IN SHARE UPDATE EXCLUSIVE MODE")
	require.NoError(t, err)

	short := New().WithValidateMaxDuration(600 * time.Millisecond)
	outcomes = applyPlannedSteps(t, short, dsn, &validate)
	require.Len(t, outcomes, 1)
	assert.Equal(t, engine.StateFailed, outcomes[0].State)
	assert.True(t, outcomes[0].Retryable, "lock contention is retried: %s", outcomes[0].ErrorMessage)
	assert.Contains(t, outcomes[0].ErrorMessage, "retry once lock contention subsides")
	assert.NotContains(t, outcomes[0].ErrorMessage, "validate_max_duration", "the scan never started, so the bound did not end it")
	assert.False(t, foreignKeyValidated(t, db, "orders_user_id_fkey"))

	require.NoError(t, tx.Rollback())
	outcomes = applyPlannedSteps(t, short, dsn, &validate)
	require.Len(t, outcomes, 1)
	assert.Equal(t, engine.StateCompleted, outcomes[0].State, outcomes[0].ErrorMessage)
	assert.True(t, foreignKeyValidated(t, db, "orders_user_id_fkey"))
}

// A plan validates a live foreign key left NOT VALID. Before the apply runs,
// another session replaces the key with a different one under the same name.
// The apply reads the key again under the validation's lock, finds it is not
// the key the plan reviewed, and validates nothing; the operator is told to
// re-plan.
func TestEngineApplyRefusesToValidateAReplacedForeignKey(t *testing.T) {
	const database = "apply_foreign_key_replaced_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.accounts (id bigint PRIMARY KEY);
		CREATE TABLE public.orders (id bigint PRIMARY KEY, user_id bigint NOT NULL);
		INSERT INTO public.users VALUES (1);
		INSERT INTO public.accounts VALUES (1);
		INSERT INTO public.orders VALUES (10, 1);
		ALTER TABLE public.orders ADD CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users (id) NOT VALID`)
	require.NoError(t, err)
	files := map[string]string{
		"users.sql":    "CREATE TABLE users (id bigint PRIMARY KEY)",
		"accounts.sql": "CREATE TABLE accounts (id bigint PRIMARY KEY)",
		"orders.sql":   "CREATE TABLE orders (id bigint PRIMARY KEY, user_id bigint NOT NULL REFERENCES users (id))",
	}
	eng := New()
	planned := planForeignKeyFiles(t, eng, database, dsn, files)
	require.Len(t, planned.Changes, 1)
	require.Len(t, planned.Changes[0].TableChanges, 1)
	validate := planned.Changes[0].TableChanges[0]
	assert.Equal(t, "ALTER TABLE public.orders VALIDATE CONSTRAINT orders_user_id_fkey", validate.DDL)
	assert.Contains(t, validate.Metadata, reviewedValidationMetadataKey, "the plan records the key it reviewed")

	_, err = db.ExecContext(t.Context(), `
		ALTER TABLE public.orders DROP CONSTRAINT orders_user_id_fkey;
		ALTER TABLE public.orders ADD CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.accounts (id) NOT VALID`)
	require.NoError(t, err)

	outcomes := applyPlannedSteps(t, eng, dsn, planned)
	require.Len(t, outcomes, 1)
	assert.Equal(t, engine.StateFailed, outcomes[0].State)
	assert.Contains(t, outcomes[0].ErrorMessage, `the index or constraint the planned statement for "orders" names changed after planning`)
	assert.Contains(t, outcomes[0].ErrorMessage, "re-plan against the current schema")
	assert.False(t, foreignKeyValidated(t, db, "orders_user_id_fkey"), "the replacement key is not validated")
}

// An owner adds a foreign key. The apply adds it NOT VALID, and then the
// apply stops before the validation runs. Before it resumes, another session
// replaces the key with a different one under the same name. The resumed
// validation reads the key again under its lock, finds it is not the key this
// plan added, and validates nothing; the operator is told to re-plan.
func TestEngineApplyResumedValidationRefusesAReplacedForeignKey(t *testing.T) {
	const database = "apply_foreign_key_resume_replaced_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.accounts (id bigint PRIMARY KEY);
		CREATE TABLE public.orders (id bigint PRIMARY KEY, user_id bigint NOT NULL);
		INSERT INTO public.users VALUES (1);
		INSERT INTO public.accounts VALUES (1);
		INSERT INTO public.orders VALUES (10, 1)`)
	require.NoError(t, err)
	files := map[string]string{
		"users.sql":    "CREATE TABLE users (id bigint PRIMARY KEY)",
		"accounts.sql": "CREATE TABLE accounts (id bigint PRIMARY KEY)",
		"orders.sql":   "CREATE TABLE orders (id bigint PRIMARY KEY, user_id bigint NOT NULL REFERENCES users (id))",
	}
	eng := New()
	planned := planForeignKeyFiles(t, eng, database, dsn, files)
	require.Len(t, planned.Changes, 1)
	steps := planned.Changes[0].TableChanges
	require.Len(t, steps, 2, "add the key NOT VALID, then validate it")
	add, validate := steps[0], steps[1]
	assert.Equal(t, `ALTER TABLE "public"."orders" VALIDATE CONSTRAINT "orders_user_id_fkey"`, validate.DDL)
	assert.Contains(t, validate.Metadata, reviewedValidationMetadataKey, "the plan records the key its own ADD creates")

	only := func(tc engine.TableChange) *engine.PlanResult {
		return &engine.PlanResult{Changes: []engine.SchemaChange{{Namespace: planned.Changes[0].Namespace, TableChanges: []engine.TableChange{tc}}}}
	}
	outcomes := applyPlannedSteps(t, eng, dsn, only(add))
	require.Len(t, outcomes, 1)
	require.Equal(t, engine.StateCompleted, outcomes[0].State, outcomes[0].ErrorMessage)

	_, err = db.ExecContext(t.Context(), `
		ALTER TABLE public.orders DROP CONSTRAINT orders_user_id_fkey;
		ALTER TABLE public.orders ADD CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.accounts (id) NOT VALID`)
	require.NoError(t, err)

	outcomes = applyPlannedSteps(t, eng, dsn, only(validate))
	require.Len(t, outcomes, 1)
	assert.Equal(t, engine.StateFailed, outcomes[0].State)
	assert.Contains(t, outcomes[0].ErrorMessage, `the index or constraint the planned statement for "orders" names changed after planning`)
	assert.Contains(t, outcomes[0].ErrorMessage, "re-plan against the current schema")
	assert.False(t, foreignKeyValidated(t, db, "orders_user_id_fkey"), "the replacement key is not validated")
}

// An owner adds a column to orders, which references users. The plan captures
// both tables with the key as rollback originals, and the apply adds the
// column. Rolling back plans only the column's drop; it runs, and the key is
// still in place and validated afterward.
func TestEngineRollbackKeepsForeignKeys(t *testing.T) {
	const database = "rollback_foreign_keys_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.orders (id bigint PRIMARY KEY, user_id bigint NOT NULL REFERENCES public.users (id));
		INSERT INTO public.users VALUES (1);
		INSERT INTO public.orders VALUES (10, 1)`)
	require.NoError(t, err)
	eng := New()

	forward := planForeignKeyFiles(t, eng, database, dsn, map[string]string{
		"users.sql":  "CREATE TABLE users (id bigint PRIMARY KEY)",
		"orders.sql": "CREATE TABLE orders (id bigint PRIMARY KEY, user_id bigint NOT NULL REFERENCES users (id), note text)",
	})
	require.Len(t, forward.Changes, 1)
	require.True(t, forward.Changes[0].OriginalFilesCaptured, "a namespace with foreign keys can be rolled back")
	originals := forward.Changes[0].OriginalFiles
	assert.Equal(t, []string{"orders.sql", "users.sql"}, sortedKeys(originals))
	assert.Contains(t, originals["orders.sql"], `CONSTRAINT "orders_user_id_fkey" FOREIGN KEY (user_id) REFERENCES users(id)`)
	outcomes := applyPlannedSteps(t, eng, dsn, forward)
	require.Len(t, outcomes, 1)
	assert.Equal(t, engine.StateCompleted, outcomes[0].State, outcomes[0].ErrorMessage)

	rollback := planForeignKeyFiles(t, eng, database, dsn, originals)
	assert.Equal(t, []string{"ALTER TABLE public.orders DROP note"}, executableDDL(t, rollback, "orders"))
	assert.Empty(t, executableDDL(t, rollback, "users"))
	outcomes = applyPlannedSteps(t, eng, dsn, rollback)
	require.Len(t, outcomes, 1)
	assert.Equal(t, engine.StateCompleted, outcomes[0].State, outcomes[0].ErrorMessage)

	converged := planForeignKeyFiles(t, eng, database, dsn, originals)
	assert.True(t, converged.NoChanges, "the rollback converged, got %+v", converged.Changes)
	assert.True(t, foreignKeyValidated(t, db, "orders_user_id_fkey"))
}

// An owner adds a foreign key and the apply runs it, and then the change is
// reverted, the way a revert of the pull request does: rolling back plans the
// key's drop as an unsafe change, and the apply drops it. A row the key
// rejected now lands, and a fresh plan of the originals is empty.
func TestEngineRollbackOfAnAddedForeignKeyDropsIt(t *testing.T) {
	const database = "rollback_added_foreign_key_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.orders (id bigint PRIMARY KEY, user_id bigint NOT NULL);
		INSERT INTO public.users VALUES (1);
		INSERT INTO public.orders VALUES (10, 1)`)
	require.NoError(t, err)
	eng := New()

	forward := planForeignKeyFiles(t, eng, database, dsn, map[string]string{
		"users.sql":  "CREATE TABLE users (id bigint PRIMARY KEY)",
		"orders.sql": "CREATE TABLE orders (id bigint PRIMARY KEY, user_id bigint NOT NULL REFERENCES users (id))",
	})
	require.Len(t, forward.Changes, 1)
	require.True(t, forward.Changes[0].OriginalFilesCaptured)
	outcomes := applyPlannedSteps(t, eng, dsn, forward)
	require.Len(t, outcomes, 2, "add the key, then validate it")
	for _, progress := range outcomes {
		assert.Equal(t, engine.StateCompleted, progress.State, progress.ErrorMessage)
	}

	originals := forward.Changes[0].OriginalFiles
	rollback := planForeignKeyFiles(t, eng, database, dsn, originals)
	assert.Equal(t, []string{"ALTER TABLE public.orders DROP CONSTRAINT orders_user_id_fkey"}, executableDDL(t, rollback, "orders"))
	require.Len(t, rollback.Changes, 1)
	require.Len(t, rollback.Changes[0].TableChanges, 1)
	assert.True(t, rollback.Changes[0].TableChanges[0].IsUnsafe)

	outcomes = applyPlannedSteps(t, eng, dsn, rollback)
	require.Len(t, outcomes, 1)
	assert.Equal(t, engine.StateCompleted, outcomes[0].State, outcomes[0].ErrorMessage)
	_, err = db.ExecContext(t.Context(), `INSERT INTO public.orders VALUES (11, 2)`)
	require.NoError(t, err, "the dropped key no longer rejects the row")

	converged := planForeignKeyFiles(t, eng, database, dsn, originals)
	assert.True(t, converged.NoChanges, "the rollback converged, got %+v", converged.Changes)
}

// An owner changes a live foreign key to cascade deletes. The plan replaces
// the key in one statement that drops it and adds the new one without
// checking existing rows, so writes are never checked against no key, then
// validates it; the apply runs both, and the key cascades afterward.
func TestEngineApplyChangedForeignKey(t *testing.T) {
	const database = "apply_changed_foreign_key_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.orders (id bigint PRIMARY KEY, user_id bigint NOT NULL REFERENCES public.users (id));
		INSERT INTO public.users VALUES (1);
		INSERT INTO public.orders VALUES (10, 1)`)
	require.NoError(t, err)
	files := map[string]string{
		"users.sql":  "CREATE TABLE users (id bigint PRIMARY KEY)",
		"orders.sql": "CREATE TABLE orders (id bigint PRIMARY KEY, user_id bigint NOT NULL REFERENCES users (id) ON DELETE CASCADE)",
	}
	eng := New()

	plan := planForeignKeyFiles(t, eng, database, dsn, files)
	assert.Equal(t, []string{
		"ALTER TABLE public.orders DROP CONSTRAINT orders_user_id_fkey, ADD CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users (id) ON DELETE CASCADE NOT VALID",
		"ALTER TABLE public.orders VALIDATE CONSTRAINT orders_user_id_fkey",
	}, executableDDL(t, plan, "orders"))
	outcomes := applyPlannedSteps(t, eng, dsn, plan)
	require.Len(t, outcomes, 2)
	for _, progress := range outcomes {
		assert.Equal(t, engine.StateCompleted, progress.State, progress.ErrorMessage)
	}

	assert.True(t, foreignKeyValidated(t, db, "orders_user_id_fkey"))
	_, err = db.ExecContext(t.Context(), `DELETE FROM public.users WHERE id = 1`)
	require.NoError(t, err)
	var orders int
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT count(*) FROM public.orders`).Scan(&orders))
	assert.Zero(t, orders, "the changed key cascades the delete")
	replan := planForeignKeyFiles(t, eng, database, dsn, files)
	assert.True(t, replan.NoChanges, "the changed key matches the file, got %+v", replan.Changes)
}

// A plan drops a live foreign key the file left out. Before the apply runs,
// another session replaces the key with a different one under the same name.
// The apply reads the key again under the drop's lock, finds it is not the key
// the plan reviewed, and drops nothing.
func TestEngineApplyRefusesToDropAReplacedForeignKey(t *testing.T) {
	const database = "apply_foreign_key_drop_replaced_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.accounts (id bigint PRIMARY KEY);
		CREATE TABLE public.orders (id bigint PRIMARY KEY, user_id bigint NOT NULL REFERENCES public.users (id));
		INSERT INTO public.users VALUES (1);
		INSERT INTO public.accounts VALUES (1);
		INSERT INTO public.orders VALUES (10, 1)`)
	require.NoError(t, err)
	eng := New()
	planned := planForeignKeyFiles(t, eng, database, dsn, map[string]string{
		"users.sql":    "CREATE TABLE users (id bigint PRIMARY KEY)",
		"accounts.sql": "CREATE TABLE accounts (id bigint PRIMARY KEY)",
		"orders.sql":   "CREATE TABLE orders (id bigint PRIMARY KEY, user_id bigint NOT NULL)",
	})
	assert.Equal(t, []string{"ALTER TABLE public.orders DROP CONSTRAINT orders_user_id_fkey"}, executableDDL(t, planned, "orders"))

	_, err = db.ExecContext(t.Context(), `
		ALTER TABLE public.orders DROP CONSTRAINT orders_user_id_fkey;
		ALTER TABLE public.orders ADD CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.accounts (id)`)
	require.NoError(t, err)

	outcomes := applyPlannedSteps(t, eng, dsn, planned)
	require.Len(t, outcomes, 1)
	assert.Equal(t, engine.StateFailed, outcomes[0].State)
	assert.Equal(t, `the object the planned drop on "orders" removes changed after planning; re-plan against the current schema`, outcomes[0].ErrorMessage)
	var references string
	require.NoError(t, db.QueryRowContext(t.Context(),
		`SELECT confrelid::regclass::text FROM pg_constraint WHERE conname = 'orders_user_id_fkey'`).Scan(&references))
	assert.Equal(t, "accounts", references, "the replacement key stays")
}

// A schema file cannot declare a foreign key into another namespace, so the
// originals of a table holding one, or referenced by one, would leave the
// key out. Such a namespace plans rollback-incapable, and the plan itself
// still proceeds.
func TestEnginePlanForeignKeyAcrossNamespacesIsRollbackIncapable(t *testing.T) {
	tests := []struct {
		name     string
		database string
		setup    string
	}{
		{
			name:     "key into another namespace",
			database: "rollback_fk_outgoing_test",
			setup: `CREATE SCHEMA billing;
				CREATE TABLE billing.accounts (id bigint PRIMARY KEY);
				CREATE TABLE public.users (id bigint PRIMARY KEY, account_id bigint REFERENCES billing.accounts (id))`,
		},
		{
			name:     "key from another namespace",
			database: "rollback_fk_incoming_test",
			setup: `CREATE SCHEMA billing;
				CREATE TABLE public.users (id bigint PRIMARY KEY, account_id bigint);
				CREATE TABLE billing.invoices (id bigint PRIMARY KEY, user_id bigint REFERENCES public.users (id))`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn, db := testutil.StartPostgres(t, tt.database)
			_, err := db.ExecContext(t.Context(), tt.setup)
			require.NoError(t, err)

			result, err := New().Plan(t.Context(), &engine.PlanRequest{
				Database: tt.database,
				SchemaFiles: schema.SchemaFiles{"public": {Files: map[string]string{
					"users.sql": "CREATE TABLE users (id bigint PRIMARY KEY, account_id bigint, email text)",
				}}},
				Credentials: &engine.Credentials{DSN: dsn},
			})
			require.NoError(t, err)
			require.Len(t, result.Changes, 1)
			assert.False(t, result.Changes[0].OriginalFilesCaptured)
			assert.Nil(t, result.Changes[0].OriginalFiles)
		})
	}
}

// A namespace already holds a foreign key added NOT VALID, and a pull request
// adds a column to another table. A schema file cannot declare the key as NOT
// VALID, so originals captured from the live namespace would write it as a
// validated key, and a rollback would run a VALIDATE CONSTRAINT the forward
// apply never ran. The plan proceeds with the column and is
// rollback-incapable.
func TestEnginePlanBesideNotValidForeignKeyIsRollbackIncapable(t *testing.T) {
	const database = "rollback_fk_not_valid_test"
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.orders (id bigint PRIMARY KEY, user_id bigint);
		ALTER TABLE public.orders ADD CONSTRAINT orders_user_id_fkey
			FOREIGN KEY (user_id) REFERENCES public.users (id) NOT VALID`)
	require.NoError(t, err)

	result, err := New().Plan(t.Context(), &engine.PlanRequest{
		Database: database,
		SchemaFiles: schema.SchemaFiles{"public": {Files: map[string]string{
			"users.sql":  "CREATE TABLE users (id bigint PRIMARY KEY, email text)",
			"orders.sql": "CREATE TABLE orders (id bigint PRIMARY KEY, user_id bigint, CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES users (id))",
		}}},
		Credentials: &engine.Credentials{DSN: dsn},
	})
	require.NoError(t, err)
	require.Len(t, result.Changes, 1)
	var addsEmail bool
	for _, change := range result.Changes[0].TableChanges {
		if change.Table == "users" && strings.Contains(change.DDL, "ADD COLUMN") && strings.Contains(change.DDL, "email") {
			addsEmail = true
		}
	}
	assert.True(t, addsEmail, "the plan still adds the column: %+v", result.Changes[0].TableChanges)
	assert.False(t, result.Changes[0].OriginalFilesCaptured)
	assert.Nil(t, result.Changes[0].OriginalFiles)
	assert.Equal(t, []engine.RollbackBlocker{{
		Table:  "orders",
		Reason: "holds a foreign key added NOT VALID, which a schema file cannot declare",
	}}, result.Changes[0].RollbackBlockers, "the plan names the table that keeps it from being rolled back, and why")
}
