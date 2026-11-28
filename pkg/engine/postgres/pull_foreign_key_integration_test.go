//go:build integration

package postgres

import (
	"testing"

	"github.com/block/pg-sprite/pkg/statement"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

// pullForeignKeyTarget starts a database seeded with ddl and returns an engine
// pointed at it, with the credentials a plan against it needs.
func pullForeignKeyTarget(t *testing.T, database, ddl string) (*Engine, *engine.Credentials) {
	t.Helper()
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), ddl)
	require.NoError(t, err)
	credentials := &engine.Credentials{DSN: dsn}
	return NewForTarget(0, 0, database, credentials), credentials
}

// pulledSchemaFiles turns a pull response into the schema files an owner would
// commit, one file per table.
func pulledSchemaFiles(response *ternv1.PullSchemaResponse) schema.SchemaFiles {
	files := schema.SchemaFiles{}
	for namespace, pulled := range response.GetNamespaces() {
		tables := map[string]string{}
		for table, content := range pulled.GetTables() {
			tables[table+".sql"] = content
		}
		files[namespace] = &schema.Namespace{Files: tables}
	}
	return files
}

// An application schema whose tables reference each other pulls: each table's
// file declares its own foreign keys, with their actions and deferrability,
// a self-reference stays in its own table's file, and a key declared on a
// partitioned parent stays in the parent's file. Planning the pulled
// files against the same database plans no changes, so the baseline an owner
// commits is the live schema, relationships included.
func TestEnginePullSchemaWritesForeignKeys(t *testing.T) {
	eng, credentials := pullForeignKeyTarget(t, "pull_foreign_keys_test", `
		CREATE SCHEMA app;
		CREATE TABLE app.users (id bigint PRIMARY KEY, email text NOT NULL UNIQUE);
		CREATE TABLE app.orders (
			id bigint PRIMARY KEY,
			user_id bigint NOT NULL REFERENCES app.users (id) ON DELETE CASCADE,
			contact_email text REFERENCES app.users (email) ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED);
		CREATE TABLE app.employees (id bigint PRIMARY KEY, manager_id bigint REFERENCES app.employees (id));
		CREATE TABLE app.events (id bigint, user_id bigint REFERENCES app.users (id), day date) PARTITION BY RANGE (day);
		CREATE TABLE app.events_2026 PARTITION OF app.events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')`)

	response, err := eng.PullSchema(t.Context(), &ternv1.PullSchemaRequest{Namespace: "app"})
	require.NoError(t, err)
	tables := response.Namespaces["app"].GetTables()
	assert.Equal(t, []string{"employees", "events", "orders", "users"}, sortedKeys(tables))
	assert.Contains(t, tables["events"], `CONSTRAINT "events_user_id_fkey" FOREIGN KEY (user_id) REFERENCES users(id)`,
		"a key declared on a partitioned parent is written in the parent's file, and its copy on each partition does not refuse the referenced table")
	assert.Contains(t, tables["orders"], `CONSTRAINT "orders_user_id_fkey" FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE`)
	assert.Contains(t, tables["orders"], `CONSTRAINT "orders_contact_email_fkey" FOREIGN KEY (contact_email) REFERENCES users(email) ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED`)
	assert.Contains(t, tables["employees"], `CONSTRAINT "employees_manager_id_fkey" FOREIGN KEY (manager_id) REFERENCES employees(id)`)
	assert.NotContains(t, tables["users"], "FOREIGN KEY", "an incoming foreign key is declared in the referencing table's file")

	result, err := New().Plan(t.Context(), &engine.PlanRequest{
		Database:    "pull_foreign_keys_test",
		SchemaFiles: pulledSchemaFiles(response),
		Credentials: credentials,
	})
	require.NoError(t, err)
	assert.Empty(t, result.Changes, "the pulled files must describe the live schema exactly")
}

// A referenced table is refused when a foreign key that references it lives on
// a table the pull does not write: a partition or an extension-owned table,
// which has no schema file of its own, or a table in another schema when the
// pull is scoped to one. The refusal says what to do for each. A
// baseline without that key's file would not carry the relationship, so the
// whole pull is refused rather than written partially.
func TestEnginePullSchemaRefusesForeignKeysOutsideThePull(t *testing.T) {
	t.Run("partition", func(t *testing.T) {
		eng, _ := pullForeignKeyTarget(t, "pull_partition_fk_test", `
			CREATE SCHEMA app;
			CREATE TABLE app.users (id bigint PRIMARY KEY);
			CREATE TABLE app.events (id bigint, user_id bigint, day date) PARTITION BY RANGE (day);
			CREATE TABLE app.events_2026 PARTITION OF app.events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
			ALTER TABLE app.events_2026 ADD CONSTRAINT events_2026_user_id_fkey FOREIGN KEY (user_id) REFERENCES app.users (id)`)

		response, err := eng.PullSchema(t.Context(), &ternv1.PullSchemaRequest{Namespace: "app"})

		require.ErrorIs(t, err, errUnwrittenReferencingTable)
		assert.Nil(t, response)
		assert.Contains(t, err.Error(), `schema "app" table "users"`)
		assert.Contains(t, err.Error(), "events_2026.events_2026_user_id_fkey (on a partition; declare the key on the partitioned parent)")
	})

	t.Run("extension-owned table", func(t *testing.T) {
		eng, _ := pullForeignKeyTarget(t, "pull_extension_fk_test", `
			CREATE SCHEMA app;
			CREATE TABLE app.users (id bigint PRIMARY KEY);
			CREATE EXTENSION hstore SCHEMA app;
			CREATE TABLE app.ext_settings (key text PRIMARY KEY, user_id bigint REFERENCES app.users (id));
			ALTER EXTENSION hstore ADD TABLE app.ext_settings`)

		response, err := eng.PullSchema(t.Context(), &ternv1.PullSchemaRequest{Namespace: "app"})

		require.ErrorIs(t, err, errUnwrittenReferencingTable)
		assert.Nil(t, response)
		assert.Contains(t, err.Error(), `schema "app" table "users"`)
		assert.Contains(t, err.Error(), "ext_settings.ext_settings_user_id_fkey (on a table the pull cannot write, such as one an extension owns; remove the key)")
	})

	t.Run("another schema", func(t *testing.T) {
		eng, _ := pullForeignKeyTarget(t, "pull_cross_schema_fk_test", `
			CREATE SCHEMA billing;
			CREATE TABLE billing.accounts (id bigint PRIMARY KEY);
			CREATE SCHEMA app;
			CREATE TABLE app.invoices (id bigint PRIMARY KEY, account_id bigint REFERENCES billing.accounts (id))`)

		response, err := eng.PullSchema(t.Context(), &ternv1.PullSchemaRequest{Namespace: "billing"})

		require.ErrorIs(t, err, errUnwrittenReferencingTable)
		assert.Nil(t, response)
		assert.Contains(t, err.Error(), `schema "billing" table "accounts"`)
		assert.Contains(t, err.Error(), "app.invoices.invoices_account_id_fkey (in another schema; a schema file cannot reference it)")
	})
}

// A foreign key into another schema cannot be declared in a schema file, so
// pulling every schema refuses both sides of the relationship: the
// referencing table, which the renderer cannot write, and the referenced
// table, whose incoming key would otherwise have no file.
func TestEnginePullSchemaRefusesCrossSchemaForeignKeys(t *testing.T) {
	eng, _ := pullForeignKeyTarget(t, "pull_all_cross_schema_fk_test", `
		CREATE SCHEMA billing;
		CREATE TABLE billing.accounts (id bigint PRIMARY KEY);
		CREATE SCHEMA app;
		CREATE TABLE app.invoices (id bigint PRIMARY KEY, account_id bigint REFERENCES billing.accounts (id))`)

	response, err := eng.PullSchema(t.Context(), &ternv1.PullSchemaRequest{})

	require.Error(t, err)
	assert.Nil(t, response)
	assert.Contains(t, err.Error(), `schema "app" table "invoices": render`)
	assert.Contains(t, err.Error(), `schema "billing" table "accounts": `+errUnwrittenReferencingTable.Error())
}

// A foreign key added NOT VALID is pulled as an enforced one, because a
// schema file declares what the table should be and cannot declare that
// existing rows are unchecked. Planning the pulled files therefore plans the
// key's validation, which runs while writes keep flowing, rather than leaving
// the key unchecked silently.
func TestEnginePullSchemaPlansValidationOfANotValidForeignKey(t *testing.T) {
	eng, credentials := pullForeignKeyTarget(t, "pull_not_valid_fk_test", `
		CREATE SCHEMA app;
		CREATE TABLE app.users (id bigint PRIMARY KEY);
		CREATE TABLE app.orders (id bigint PRIMARY KEY, user_id bigint);
		ALTER TABLE app.orders ADD CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES app.users (id) NOT VALID`)

	response, err := eng.PullSchema(t.Context(), &ternv1.PullSchemaRequest{Namespace: "app"})
	require.NoError(t, err)
	assert.Contains(t, response.Namespaces["app"].GetTables()["orders"], `CONSTRAINT "orders_user_id_fkey" FOREIGN KEY (user_id) REFERENCES users(id)`)
	assert.NotContains(t, response.Namespaces["app"].GetTables()["orders"], "NOT VALID")

	result, err := New().Plan(t.Context(), &engine.PlanRequest{
		Database:    "pull_not_valid_fk_test",
		SchemaFiles: pulledSchemaFiles(response),
		Credentials: credentials,
	})
	require.NoError(t, err)
	require.Len(t, result.Changes, 1)
	require.Len(t, result.Changes[0].TableChanges, 1)
	validate := result.Changes[0].TableChanges[0]
	assert.Equal(t, "ALTER TABLE app.orders VALIDATE CONSTRAINT orders_user_id_fkey", validate.DDL)
	assert.Empty(t, validate.ExecutionMode, "the validation runs as planned: %s", validate.ModeReason)
}

// A schema file cannot declare row-level security and a foreign key together
// yet, so a table with both is refused, and with it the pull, rather than
// written without one of them.
func TestEnginePullSchemaRefusesRowSecurityWithAForeignKey(t *testing.T) {
	eng, _ := pullForeignKeyTarget(t, "pull_row_security_fk_test", `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.orders (id bigint PRIMARY KEY, user_id bigint REFERENCES public.users (id));
		ALTER TABLE public.orders ENABLE ROW LEVEL SECURITY;
		CREATE POLICY orders_all ON public.orders USING (true)`)

	response, err := eng.PullSchema(t.Context(), &ternv1.PullSchemaRequest{})

	require.ErrorIs(t, err, statement.ErrRowSecurityForeignKey)
	assert.Nil(t, response)
	assert.Contains(t, err.Error(), `schema "public" table "orders": render`)
	assert.NotContains(t, err.Error(), `table "users"`, "only the referencing table is refused")
}

// A plan's rollback baseline carries each table's foreign keys, so a
// namespace with an ordinary foreign key pair captures both tables. A managed
// table referenced by a foreign key on an ignored table is refused, since no
// captured file would hold that key, and the plan is rollback-incapable.
func TestCaptureOriginalFilesCarriesForeignKeys(t *testing.T) {
	const originalOrders = "CREATE TABLE \"orders\" (\n    \"id\" bigint NOT NULL,\n    \"user_id\" bigint,\n    CONSTRAINT \"orders_pkey\" PRIMARY KEY (id),\n    CONSTRAINT \"orders_user_id_fkey\" FOREIGN KEY (user_id) REFERENCES users(id)\n);\n"
	const originalUsers = "CREATE TABLE \"users\" (\n    \"id\" bigint NOT NULL,\n    CONSTRAINT \"users_pkey\" PRIMARY KEY (id)\n);\n"
	tests := []struct {
		name         string
		database     string
		setup        string
		ignored      []string
		wantBlockers []engine.RollbackBlocker
		wantFiles    map[string]string
	}{
		{
			name:     "foreign key pair",
			database: "capture_fk_test",
			setup: `CREATE SCHEMA app;
				CREATE TABLE app.users (id bigint PRIMARY KEY);
				CREATE TABLE app.orders (id bigint PRIMARY KEY, user_id bigint REFERENCES app.users (id))`,
			wantFiles: map[string]string{"orders.sql": originalOrders, "users.sql": originalUsers},
		},
		{
			name:     "referenced by an ignored table",
			database: "capture_fk_ignored_test",
			setup: `CREATE SCHEMA app;
				CREATE TABLE app.users (id bigint PRIMARY KEY);
				CREATE TABLE app.audit_events (id bigint PRIMARY KEY, user_id bigint REFERENCES app.users (id))`,
			ignored: []string{"audit_events"},
			wantBlockers: []engine.RollbackBlocker{{
				Table:  "users",
				Reason: "referenced by a foreign key on a table without a schema file: audit_events.audit_events_user_id_fkey (on a table without a schema file, such as one an extension owns or one the repository ignores; declare that table in a schema file or remove the key)",
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn, db := testutil.StartPostgres(t, tt.database)
			_, err := db.ExecContext(t.Context(), tt.setup)
			require.NoError(t, err)
			pool, err := pgxpool.New(t.Context(), dsn)
			require.NoError(t, err)
			defer pool.Close()

			files, blockers, err := captureOriginalFiles(t.Context(), pool, tt.database, "app", nil, nil, mustIgnoredTables(t, tt.ignored))

			require.NoError(t, err)
			assert.Equal(t, tt.wantBlockers, blockers)
			assert.Equal(t, tt.wantFiles, files)
		})
	}
}
