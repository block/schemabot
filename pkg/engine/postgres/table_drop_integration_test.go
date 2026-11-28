//go:build integration

package postgres

import (
	"database/sql"
	"fmt"
	"net/url"
	"testing"

	"github.com/block/spirit/pkg/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

// planUndeclared plans the target against a schema that declares only
// users, so every other live table in public is planned as a drop.
func planUndeclared(t *testing.T, dsn string) *engine.PlanResult {
	t.Helper()
	result, err := New().Plan(t.Context(), &engine.PlanRequest{
		Database: "app",
		SchemaFiles: schema.SchemaFiles{"public": {Files: map[string]string{
			"users.sql": "CREATE TABLE users (id bigint PRIMARY KEY)",
		}}},
		Credentials: &engine.Credentials{DSN: dsn},
	})
	require.NoError(t, err)
	require.Len(t, result.Changes, 1)
	return result
}

// planUndeclaredDrops is planUndeclared for a target whose every drop the
// plan can run.
func planUndeclaredDrops(t *testing.T, dsn string) *engine.PlanResult {
	t.Helper()
	result := planUndeclared(t, dsn)
	for _, change := range result.Changes[0].TableChanges {
		require.Empty(t, change.ExecutionMode, "%s: %s", change.Table, change.ModeReason)
	}
	return result
}

// limitedRoleDSN creates a role that can connect, plan in a scratch schema,
// and read the public schema, but owns no table and belongs to no role, and
// returns the DSN that logs in as it.
func limitedRoleDSN(t *testing.T, dsn, database string) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	_, err = db.ExecContext(t.Context(), fmt.Sprintf(`
		CREATE ROLE drop_limited LOGIN PASSWORD 'drop_limited';
		GRANT CONNECT, CREATE ON DATABASE %s TO drop_limited;
		GRANT USAGE ON SCHEMA public TO drop_limited`, database))
	require.NoError(t, err)
	limited, err := url.Parse(dsn)
	require.NoError(t, err)
	limited.User = url.UserPassword("drop_limited", "drop_limited")
	return limited.String()
}

// An operator deletes the schema files of three tables, two of which a
// foreign key ties together, and applies the plan with the unsafe
// acknowledgment. The drive runs the drops in the plan's order, so the
// referencing table goes before the table it references, and every
// undeclared table is gone while the declared one stays.
func TestEngineApplyDropsUndeclaredTablesInForeignKeyOrder(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "table_drop_order")
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.regions (id bigint PRIMARY KEY);
		CREATE TABLE public.warehouses (id bigint PRIMARY KEY, region_id bigint REFERENCES public.regions (id));
		CREATE TABLE public.categories (id bigint PRIMARY KEY, parent_id bigint REFERENCES public.categories (id));
		INSERT INTO public.regions VALUES (1);
		INSERT INTO public.warehouses VALUES (1, 1)`)
	require.NoError(t, err)

	result := planUndeclaredDrops(t, dsn)
	var order []string
	for _, change := range result.Changes[0].TableChanges {
		order = append(order, change.Table)
	}
	assert.Equal(t, []string{"categories", "warehouses", "regions"}, order)

	outcomes := applyPlannedSteps(t, New(), dsn, result)
	require.Len(t, outcomes, 3)
	for _, outcome := range outcomes {
		assert.Equal(t, engine.StateCompleted, outcome.State, outcome.ErrorMessage)
	}
	for _, table := range order {
		assert.False(t, testutil.PostgresTableExists(t, db, "public", table), "%s must be dropped", table)
	}
	assert.True(t, testutil.PostgresTableExists(t, db, "public", "users"))
}

// Between the review and the apply, someone adds a column to the table the
// plan drops. The table is no longer the one the operator accepted losing,
// so the apply refuses with a curated message, drops nothing, and asks for a
// re-plan.
func TestEngineApplyRefusesATableDropWhoseTableChangedAfterReview(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "table_drop_drift")
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.legacy_users (id bigint PRIMARY KEY)`)
	require.NoError(t, err)

	result := planUndeclaredDrops(t, dsn)
	_, err = db.ExecContext(t.Context(), "ALTER TABLE public.legacy_users ADD COLUMN email text")
	require.NoError(t, err)

	outcomes := applyPlannedSteps(t, New(), dsn, result)
	require.Len(t, outcomes, 1)
	assert.Equal(t, engine.StateFailed, outcomes[0].State)
	assert.False(t, outcomes[0].Retryable, "a retry would meet the same changed table")
	assert.Contains(t, outcomes[0].ErrorMessage, `the object the planned drop on "legacy_users" removes changed after planning; re-plan against the current schema`)
	assert.True(t, testutil.PostgresTableExists(t, db, "public", "legacy_users"), "the changed table must be left in place")
}

// A view reads a table whose schema file was deleted. PostgreSQL refuses to
// drop a table a view depends on, and SchemaBot never cascades a drop into
// objects the plan did not show, so the plan blocks the drop and names the
// view, rather than leave the apply to fail on it.
func TestEnginePlanBlocksATableDropAViewDependsOn(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "table_drop_view_plan")
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.legacy_users (id bigint PRIMARY KEY, email text);
		CREATE VIEW public.legacy_emails AS SELECT email FROM public.legacy_users`)
	require.NoError(t, err)

	changes := planUndeclared(t, dsn).Changes[0].TableChanges
	require.Len(t, changes, 1)
	assert.Equal(t, engine.ExecutionModeBlocked, changes[0].ExecutionMode)
	assert.Contains(t, changes[0].ModeReason, `"view public.legacy_emails" depends on it, so PostgreSQL refuses to drop it`)
}

// A table SchemaBot does not manage draws its ids from the sequence of an
// undeclared table. Dropping the table would drop its sequence, so
// PostgreSQL refuses the drop, and the plan blocks it and names the default.
func TestEnginePlanBlocksATableDropWhoseSequenceAnotherTableUses(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "table_drop_sequence_plan")
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.legacy_users (id bigserial PRIMARY KEY);
		CREATE SCHEMA reporting;
		CREATE TABLE reporting.imports (id bigint DEFAULT nextval('public.legacy_users_id_seq') PRIMARY KEY)`)
	require.NoError(t, err)

	changes := planUndeclared(t, dsn).Changes[0].TableChanges
	require.Len(t, changes, 1)
	assert.Equal(t, "legacy_users", changes[0].Table)
	assert.Equal(t, engine.ExecutionModeBlocked, changes[0].ExecutionMode)
	assert.Contains(t, changes[0].ModeReason, `"default value for column id of table reporting.imports" depends on it, so PostgreSQL refuses to drop it`)
}

// A view that counts the table's rows is created after the plan was
// reviewed. It reads no column, so the table's reviewed definition does not
// change, but the server refuses the drop, because SchemaBot never
// cascades. The apply fails, names the cause, and both the table and the
// view remain.
func TestEngineApplyRefusesATableDropAViewDependsOn(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "table_drop_view")
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.legacy_users (id bigint PRIMARY KEY, email text)`)
	require.NoError(t, err)

	result := planUndeclaredDrops(t, dsn)
	_, err = db.ExecContext(t.Context(), "CREATE VIEW public.legacy_count AS SELECT pg_catalog.count(*) FROM public.legacy_users")
	require.NoError(t, err)

	outcomes := applyPlannedSteps(t, New(), dsn, result)
	require.Len(t, outcomes, 1)
	assert.Equal(t, engine.StateFailed, outcomes[0].State)
	assert.Contains(t, outcomes[0].ErrorMessage, `a view or other object depends on table "legacy_users"`)
	assert.True(t, testutil.PostgresTableExists(t, db, "public", "legacy_users"))
}

// The engine role can read the target but does not own the table whose
// schema file was deleted, and only a table's owner can drop it. The plan
// blocks the drop with the grant that would let the role drop it.
func TestEnginePlanBlocksATableDropTheRoleCannotRun(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "table_drop_privilege_plan")
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.legacy_users (id bigint PRIMARY KEY)`)
	require.NoError(t, err)

	changes := planUndeclared(t, limitedRoleDSN(t, dsn, "table_drop_privilege_plan")).Changes[0].TableChanges
	require.Len(t, changes, 1)
	assert.Equal(t, engine.ExecutionModeBlocked, changes[0].ExecutionMode)
	assert.Contains(t, changes[0].ModeReason, "the engine role cannot drop it, because only the table's owner can drop a table")
	assert.Contains(t, changes[0].ModeReason, "provision with: GRANT")
	assert.Contains(t, changes[0].ModeReason, `TO "drop_limited"`, "the provisioning statement grants the access to the engine role")
}

// A plan made by a role that owns the table is applied by one that does
// not. The apply checks the role's access before it drops the table, fails
// with the grant that would let the role drop it, and the table remains.
func TestEngineApplyRefusesATableDropTheRoleCannotRun(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "table_drop_privilege")
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.legacy_users (id bigint PRIMARY KEY)`)
	require.NoError(t, err)

	result := planUndeclaredDrops(t, dsn)
	outcomes := applyPlannedSteps(t, New(), limitedRoleDSN(t, dsn, "table_drop_privilege"), result)
	require.Len(t, outcomes, 1)
	assert.Equal(t, engine.StateFailed, outcomes[0].State)
	assert.Contains(t, outcomes[0].ErrorMessage, "provision with: GRANT")
	assert.True(t, testutil.PostgresTableExists(t, db, "public", "legacy_users"))
}

// A table that is already gone when the apply reaches it has reached the
// state the plan asked for, as when a resumed apply meets a table its earlier
// attempt dropped. The apply completes, as the MySQL engine's does, and logs
// a warning rather than failing.
func TestEngineApplySkipsATableDropWhoseTableIsAlreadyGone(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "table_drop_absent")
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.legacy_users (id bigint PRIMARY KEY)`)
	require.NoError(t, err)

	result := planUndeclaredDrops(t, dsn)
	_, err = db.ExecContext(t.Context(), "DROP TABLE public.legacy_users")
	require.NoError(t, err)

	outcomes := applyPlannedSteps(t, New(), dsn, result)
	require.Len(t, outcomes, 1)
	assert.Equal(t, engine.StateCompleted, outcomes[0].State, outcomes[0].ErrorMessage)
}

// An operator deletes the schema file of a table that a kept table's
// foreign key references, and removes that foreign key from the kept
// table's file in the same change. The plan drops the foreign key in the
// kept table's own steps, which run before the table drops, so the table it
// referenced is no longer kept by it: the plan drops it, and the apply runs
// both. The kept table stays, with its column.
func TestEngineApplyDropsATableWhoseLastReferenceThePlanDrops(t *testing.T) {
	for _, tt := range []struct {
		name, users string
		steps       []string
	}{
		{
			name:  "the file keeps the column",
			users: "CREATE TABLE users (id bigint PRIMARY KEY, region_id bigint)",
			steps: []string{
				"users: ALTER TABLE public.users DROP CONSTRAINT users_region_id_fkey",
				"regions: DROP TABLE public.regions",
			},
		},
		{
			name:  "the file drops the column",
			users: "CREATE TABLE users (id bigint PRIMARY KEY)",
			steps: []string{
				"users: ALTER TABLE public.users DROP CONSTRAINT users_region_id_fkey",
				"users: ALTER TABLE public.users DROP region_id",
				"regions: DROP TABLE public.regions",
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dsn, db := testutil.StartPostgres(t, "table_drop_planned_reference_drop")
			_, err := db.ExecContext(t.Context(), `
				CREATE TABLE public.regions (id bigint PRIMARY KEY);
				CREATE TABLE public.users (id bigint PRIMARY KEY, region_id bigint REFERENCES public.regions (id));
				INSERT INTO public.regions VALUES (1);
				INSERT INTO public.users VALUES (1, 1)`)
			require.NoError(t, err)

			result, err := New().Plan(t.Context(), &engine.PlanRequest{
				Database:    "app",
				SchemaFiles: schema.SchemaFiles{"public": {Files: map[string]string{"users.sql": tt.users}}},
				Credentials: &engine.Credentials{DSN: dsn},
			})
			require.NoError(t, err)
			require.Len(t, result.Changes, 1)
			var steps []string
			for _, change := range result.Changes[0].TableChanges {
				require.Empty(t, change.ExecutionMode, "%s: %s", change.Table, change.ModeReason)
				steps = append(steps, change.Table+": "+change.DDL)
			}
			assert.Equal(t, tt.steps, steps)

			outcomes := applyPlannedSteps(t, New(), dsn, result)
			require.Len(t, outcomes, len(tt.steps))
			for _, outcome := range outcomes {
				assert.Equal(t, engine.StateCompleted, outcome.State, outcome.ErrorMessage)
			}
			assert.False(t, testutil.PostgresTableExists(t, db, "public", "regions"))
			assert.True(t, testutil.PostgresTableExists(t, db, "public", "users"))
		})
	}
}

// An operator deletes the schema file of a table while a kept table's file
// still declares a foreign key that references it: the key unchanged,
// replaced under its name, or new. The kept table references the table once
// the plan's other steps run, so PostgreSQL would refuse its drop, and the
// plan blocks the drop and names the file to change instead.
func TestEnginePlanBlocksATableDropADeclaredForeignKeyReferences(t *testing.T) {
	for _, tt := range []struct{ name, live, orders string }{
		{
			name:   "the key is unchanged",
			live:   "CREATE TABLE public.orders (id bigint PRIMARY KEY, user_id bigint REFERENCES public.users (id))",
			orders: "CREATE TABLE orders (id bigint PRIMARY KEY, user_id bigint REFERENCES users (id))",
		},
		{
			name:   "the key is replaced under its name",
			live:   "CREATE TABLE public.orders (id bigint PRIMARY KEY, user_id bigint REFERENCES public.users (id))",
			orders: "CREATE TABLE orders (id bigint PRIMARY KEY, user_id bigint REFERENCES users (id) ON DELETE CASCADE)",
		},
		{
			name:   "the key is new",
			live:   "CREATE TABLE public.orders (id bigint PRIMARY KEY, user_id bigint)",
			orders: "CREATE TABLE orders (id bigint PRIMARY KEY, user_id bigint REFERENCES users (id))",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dsn, db := testutil.StartPostgres(t, "table_drop_declared_reference")
			_, err := db.ExecContext(t.Context(), "CREATE TABLE public.users (id bigint PRIMARY KEY); "+tt.live)
			require.NoError(t, err)

			result, err := New().Plan(t.Context(), &engine.PlanRequest{
				Database:    "app",
				SchemaFiles: schema.SchemaFiles{"public": {Files: map[string]string{"orders.sql": tt.orders}}},
				Credentials: &engine.Credentials{DSN: dsn},
			})
			require.NoError(t, err)
			require.Len(t, result.Changes, 1)
			var usersDrop *engine.TableChange
			for i, change := range result.Changes[0].TableChanges {
				if change.Table == "users" {
					usersDrop = &result.Changes[0].TableChanges[i]
				}
			}
			require.NotNil(t, usersDrop, "the undeclared table is planned as a drop")
			assert.Equal(t, engine.ExecutionModeBlocked, usersDrop.ExecutionMode)
			assert.Contains(t, usersDrop.ModeReason, `the schema file of table "orders" declares a foreign key that references it, so PostgreSQL refuses to drop it`)
		})
	}
}
