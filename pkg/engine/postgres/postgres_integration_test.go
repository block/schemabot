//go:build integration

package postgres

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/block/pg-sprite/pkg/dbconn"
	"github.com/block/pg-sprite/pkg/statement"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

const postgresApplyDeadline = 10 * time.Second

// TestEnginePullSchema exports every ordinary table in a requested schema as
// an independently parseable declarative file, including constraints and
// secondary indexes.
func TestEnginePullSchema(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "pull_test")
	_, err := db.ExecContext(t.Context(), `
		CREATE SCHEMA app;
		CREATE TABLE app.accounts (id bigint PRIMARY KEY, balance bigint NOT NULL CHECK (balance >= 0));
		CREATE INDEX accounts_balance_idx ON app.accounts (balance);
		CREATE TABLE app.events (id bigint PRIMARY KEY, message text NOT NULL)`)
	require.NoError(t, err)

	eng := NewForTarget(0, "pull_test", &engine.Credentials{DSN: dsn})
	response, err := eng.PullSchema(t.Context(), &ternv1.PullSchemaRequest{
		Database: "pull_test", Type: "postgres", Environment: "test", Namespace: "app",
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), response.TableCount)
	require.Len(t, response.Namespaces, 1)
	require.Len(t, response.Namespaces["app"].Tables, 2)
	for _, table := range []string{"accounts", "events"} {
		ddl := response.Namespaces["app"].Tables[table]
		parsed, parseErr := statement.ParseDesired(ddl)
		require.NoError(t, parseErr)
		assert.Equal(t, table, parsed.Table())
	}
	assert.Contains(t, response.Namespaces["app"].Tables["accounts"], "CHECK")
	assert.Contains(t, response.Namespaces["app"].Tables["accounts"], "accounts_balance_idx")
}

// TestEnginePullSchemaRejectsMissingSchema proves a request for a schema that
// does not exist fails instead of producing an empty baseline that a typo
// could be mistaken for a schema with no tables.
func TestEnginePullSchemaRejectsMissingSchema(t *testing.T) {
	dsn, _ := testutil.StartPostgres(t, "pull_missing_test")

	eng := NewForTarget(0, "pull_missing_test", &engine.Credentials{DSN: dsn})
	response, err := eng.PullSchema(t.Context(), &ternv1.PullSchemaRequest{Namespace: "missing"})

	require.Error(t, err)
	assert.Nil(t, response)
	assert.Contains(t, err.Error(), `schema "missing" does not exist`)
}

// TestEnginePullSchemaAggregatesUnrenderableTables proves a pull never returns
// a partial baseline and identifies every table that needs manual resolution.
func TestEnginePullSchemaAggregatesUnrenderableTables(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "pull_refusal_test")
	_, err := db.ExecContext(t.Context(), `
		CREATE SCHEMA app;
		CREATE UNLOGGED TABLE app.audit_log (id bigint PRIMARY KEY);
		CREATE UNLOGGED TABLE app.delivery_log (id bigint PRIMARY KEY)`)
	require.NoError(t, err)

	eng := NewForTarget(0, "pull_refusal_test", &engine.Credentials{DSN: dsn})
	response, err := eng.PullSchema(t.Context(), &ternv1.PullSchemaRequest{Namespace: "app"})

	require.Error(t, err)
	assert.Nil(t, response)
	assert.Contains(t, err.Error(), `schema "app" table "audit_log"`)
	assert.Contains(t, err.Error(), `schema "app" table "delivery_log"`)
}

// A table carrying trigger behavior or descriptive metadata is refused so the
// printed schema never suggests that its declarative table definition is the
// whole live object.
func TestEnginePullSchemaRejectsUnmodeledTableObjects(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "pull_objects_test")
	_, err := db.ExecContext(t.Context(), `
		CREATE SCHEMA app;
		CREATE TABLE app.accounts (id bigint PRIMARY KEY);
		CREATE FUNCTION app.touch_account() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
		CREATE TRIGGER touch_account BEFORE INSERT ON app.accounts FOR EACH ROW EXECUTE FUNCTION app.touch_account();
		COMMENT ON TABLE app.accounts IS 'customer accounts'`)
	require.NoError(t, err)

	eng := NewForTarget(0, "pull_objects_test", &engine.Credentials{DSN: dsn})
	response, err := eng.PullSchema(t.Context(), &ternv1.PullSchemaRequest{Namespace: "app"})

	require.Error(t, err)
	assert.Nil(t, response)
	assert.Contains(t, err.Error(), `schema "app" table "accounts"`)
	assert.Contains(t, err.Error(), "trigger")
	assert.Contains(t, err.Error(), "comment")
}

// A child table created with PostgreSQL table inheritance is refused because
// flattening inherited columns would lose the relationship to its parent.
func TestEnginePullSchemaRejectsTableInheritance(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "pull_inheritance_test")
	_, err := db.ExecContext(t.Context(), `
		CREATE SCHEMA app;
		CREATE TABLE app.parent (id bigint PRIMARY KEY);
		CREATE TABLE app.child (detail text) INHERITS (app.parent)`)
	require.NoError(t, err)

	eng := NewForTarget(0, "pull_inheritance_test", &engine.Credentials{DSN: dsn})
	response, err := eng.PullSchema(t.Context(), &ternv1.PullSchemaRequest{Namespace: "app"})

	require.Error(t, err)
	assert.Nil(t, response)
	assert.Contains(t, err.Error(), `schema "app" table "child"`)
	assert.Contains(t, err.Error(), "table inheritance")
}

// Omitting the namespace exports each application schema while keeping server
// and information schemas outside the response.
func TestEnginePullSchemaDiscoversNonReservedSchemas(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "pull_discovery_test")
	_, err := db.ExecContext(t.Context(), `
		CREATE SCHEMA billing;
		CREATE TABLE billing.invoices (id bigint PRIMARY KEY);
		CREATE SCHEMA shipping;
		CREATE TABLE shipping.parcels (id bigint PRIMARY KEY)`)
	require.NoError(t, err)

	eng := NewForTarget(0, "pull_discovery_test", &engine.Credentials{DSN: dsn})
	response, err := eng.PullSchema(t.Context(), &ternv1.PullSchemaRequest{})

	require.NoError(t, err)
	assert.Contains(t, response.Namespaces, "billing")
	assert.Contains(t, response.Namespaces, "shipping")
	assert.Contains(t, response.Namespaces["billing"].Tables, "invoices")
	assert.Contains(t, response.Namespaces["shipping"].Tables, "parcels")
	assert.NotContains(t, response.Namespaces, "pg_catalog")
	assert.NotContains(t, response.Namespaces, "information_schema")
}

// A partitioned hierarchy selects only its parent for rendering; the child is
// neither exported nor reported as an independently unrenderable table.
func TestEnginePullSchemaExcludesPartitionChildren(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "pull_partition_test")
	_, err := db.ExecContext(t.Context(), `
		CREATE SCHEMA app;
		CREATE TABLE app.events (id bigint, created_at date) PARTITION BY RANGE (created_at);
		CREATE TABLE app.events_2026 PARTITION OF app.events FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')`)
	require.NoError(t, err)

	eng := NewForTarget(0, "pull_partition_test", &engine.Credentials{DSN: dsn})
	response, err := eng.PullSchema(t.Context(), &ternv1.PullSchemaRequest{Namespace: "app"})

	require.Error(t, err)
	assert.Nil(t, response)
	assert.Contains(t, err.Error(), `schema "app" table "events"`)
	assert.NotContains(t, err.Error(), "events_2026")
}

// Views are outside the table baseline, so ordinary and materialized views do
// not become declarative tables in a successful pull.
func TestEnginePullSchemaExcludesViews(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "pull_views_test")
	_, err := db.ExecContext(t.Context(), `
		CREATE SCHEMA app;
		CREATE TABLE app.accounts (id bigint PRIMARY KEY);
		CREATE VIEW app.account_view AS SELECT id FROM app.accounts;
		CREATE MATERIALIZED VIEW app.account_snapshot AS SELECT id FROM app.accounts`)
	require.NoError(t, err)

	eng := NewForTarget(0, "pull_views_test", &engine.Credentials{DSN: dsn})
	response, err := eng.PullSchema(t.Context(), &ternv1.PullSchemaRequest{Namespace: "app"})

	require.NoError(t, err)
	require.Len(t, response.Namespaces["app"].Tables, 1)
	assert.Contains(t, response.Namespaces["app"].Tables, "accounts")
	assert.NotContains(t, response.Namespaces["app"].Tables, "account_view")
	assert.NotContains(t, response.Namespaces["app"].Tables, "account_snapshot")
}

// A cancelled request remains a context failure and is never presented as a
// refusal caused by an unrepresentable table.
func TestEnginePullSchemaReturnsCancelledContext(t *testing.T) {
	dsn, _ := testutil.StartPostgres(t, "pull_cancel_test")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	eng := NewForTarget(0, "pull_cancel_test", &engine.Credentials{DSN: dsn})
	response, err := eng.PullSchema(ctx, &ternv1.PullSchemaRequest{Namespace: "public"})

	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, response)
	assert.Contains(t, err.Error(), `PostgreSQL database "pull_cancel_test" for schema pull`)
	assert.NotContains(t, err.Error(), "refused incomplete baseline")
}

// TestEnginePlanCreateTable proves a greenfield CREATE TABLE derived from
// desired state plans as an executable change: the role's schema CREATE
// access is verified at plan time, so the plan the operator reviews carries
// the same verdict the apply path will enforce.
func TestEnginePlanCreateTable(t *testing.T) {
	dsn, _ := testutil.StartPostgres(t, "plan_test")
	req := &engine.PlanRequest{
		Database: "plan_test",
		SchemaFiles: schema.SchemaFiles{
			"public": {Files: map[string]string{
				"users.sql": "CREATE TABLE users (id bigint PRIMARY KEY, email text NOT NULL)",
			}},
		},
		Credentials: &engine.Credentials{DSN: dsn},
	}

	result, err := New().Plan(t.Context(), req)
	require.NoError(t, err)
	assert.False(t, result.NoChanges)
	require.Len(t, result.Changes, 1)
	require.Len(t, result.Changes[0].TableChanges, 1)
	change := result.Changes[0].TableChanges[0]
	assert.Equal(t, "public", result.Changes[0].Namespace)
	assert.Equal(t, "users", change.Table)
	assert.Contains(t, change.DDL, "CREATE TABLE public.users")
	assert.Empty(t, change.ExecutionMode, "a greenfield create the role can run must plan executable")
	assert.Empty(t, change.ModeReason)
}

// TestEnginePlanPrivilegeRefusal proves a role that cannot alter the target
// gets a blocked plan naming the exact provisioning statement, instead of an
// executable plan that deterministically fails at apply. The plan itself
// still succeeds: the operator needs the review surface to carry the grant.
func TestEnginePlanPrivilegeRefusal(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "plan_privilege_test")
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE ROLE plan_limited LOGIN PASSWORD 'plan_limited';
		GRANT CONNECT, CREATE ON DATABASE plan_privilege_test TO plan_limited;
		GRANT USAGE ON SCHEMA public TO plan_limited`)
	require.NoError(t, err)

	limitedDSN, err := url.Parse(dsn)
	require.NoError(t, err)
	limitedDSN.User = url.UserPassword("plan_limited", "plan_limited")
	req := &engine.PlanRequest{
		Database: "plan_privilege_test",
		SchemaFiles: schema.SchemaFiles{
			"public": {Files: map[string]string{
				"users.sql": "CREATE TABLE users (id bigint PRIMARY KEY, email text)",
			}},
		},
		Credentials: &engine.Credentials{DSN: limitedDSN.String()},
	}

	result, err := New().Plan(t.Context(), req)
	require.NoError(t, err)
	require.Len(t, result.Changes, 1)
	require.Len(t, result.Changes[0].TableChanges, 1)
	change := result.Changes[0].TableChanges[0]
	assert.Equal(t, engine.ExecutionModeBlocked, change.ExecutionMode)
	assert.Contains(t, change.ModeReason, "in-place ALTER TABLE",
		"the reason must name the tier whose access is missing")
	assert.Contains(t, change.ModeReason, "provision with: GRANT",
		"the reason must carry the exact provisioning statement")
	assert.Contains(t, change.ModeReason, "pg_has_role(plan_limited,",
		"the reason must carry the exact failed catalog check")
}

// TestEnginePlanPrivilegeRefusalPerTier proves a privilege gap blocks only
// the plan steps that need the missing tier: a role with owner membership but
// no index-build access gets an executable ALTER step alongside a blocked
// CREATE INDEX step whose reason names the index-build tier's grant, so an
// operator fixes exactly the access the failing step needs.
func TestEnginePlanPrivilegeRefusalPerTier(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "plan_tier_test")
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE ROLE mixed_owner NOLOGIN;
		ALTER TABLE public.users OWNER TO mixed_owner;
		CREATE ROLE mixed_limited LOGIN PASSWORD 'mixed_limited';
		GRANT mixed_owner TO mixed_limited;
		GRANT CONNECT, CREATE ON DATABASE plan_tier_test TO mixed_limited;
		GRANT USAGE ON SCHEMA public TO mixed_limited`)
	require.NoError(t, err)

	limitedDSN, err := url.Parse(dsn)
	require.NoError(t, err)
	limitedDSN.User = url.UserPassword("mixed_limited", "mixed_limited")
	req := &engine.PlanRequest{
		Database: "plan_tier_test",
		SchemaFiles: schema.SchemaFiles{
			"public": {Files: map[string]string{
				"users.sql": "CREATE TABLE users (id bigint PRIMARY KEY, email text);\nCREATE INDEX idx_users_email ON users (email)",
			}},
		},
		Credentials: &engine.Credentials{DSN: limitedDSN.String()},
	}

	result, err := New().Plan(t.Context(), req)
	require.NoError(t, err)
	require.Len(t, result.Changes, 1)
	require.Len(t, result.Changes[0].TableChanges, 2)

	alter := result.Changes[0].TableChanges[0]
	assert.Contains(t, alter.DDL, "ADD COLUMN")
	assert.NotEqual(t, engine.ExecutionModeBlocked, alter.ExecutionMode,
		"owner membership satisfies the in-place ALTER tier; that step must stay executable")

	index := result.Changes[0].TableChanges[1]
	assert.Contains(t, index.DDL, "CREATE INDEX")
	assert.Equal(t, engine.ExecutionModeBlocked, index.ExecutionMode)
	assert.Contains(t, index.ModeReason, "index builds",
		"the reason must name the tier whose access is missing")
	assert.Contains(t, index.ModeReason, "provision with: GRANT",
		"the reason must carry the exact provisioning statement")
}

// TestEnginePlanTableSizeRefusal proves the configured native-safe ceiling
// blocks an otherwise executable plan when the target table exceeds it.
func TestEnginePlanTableSizeRefusal(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "plan_size_limit_test")
	_, err := db.ExecContext(t.Context(), "CREATE TABLE public.users (id bigint PRIMARY KEY)")
	require.NoError(t, err)
	req := &engine.PlanRequest{
		Database: "plan_size_limit_test",
		SchemaFiles: schema.SchemaFiles{
			"public": {Files: map[string]string{
				"users.sql": "CREATE TABLE users (id bigint PRIMARY KEY, email text)",
			}},
		},
		Credentials: &engine.Credentials{DSN: dsn},
	}

	result, err := NewWithTableSizeLimit(1).Plan(t.Context(), req)
	require.NoError(t, err)
	require.Len(t, result.Changes, 1)
	require.Len(t, result.Changes[0].TableChanges, 1)
	change := result.Changes[0].TableChanges[0]
	assert.Equal(t, engine.ExecutionModeBlocked, change.ExecutionMode)
	assert.Contains(t, change.ModeReason, `statement for table "users":`)
	assert.Contains(t, change.ModeReason, "1-byte threshold")
	assert.Contains(t, change.ModeReason, "SchemaBot's ceiling for a native-safe apply")
}

// TestEnginePlanCreateTableIgnoresSizeCeiling proves the native-safe table
// size ceiling never blocks a greenfield CREATE TABLE: the ceiling bounds
// rewrites of existing data, and a table that does not exist yet has none —
// even a one-byte ceiling must leave the create executable.
func TestEnginePlanCreateTableIgnoresSizeCeiling(t *testing.T) {
	dsn, _ := testutil.StartPostgres(t, "create_size_limit_test")
	req := &engine.PlanRequest{
		Database: "create_size_limit_test",
		SchemaFiles: schema.SchemaFiles{
			"public": {Files: map[string]string{
				"users.sql": "CREATE TABLE users (id bigint PRIMARY KEY, email text NOT NULL)",
			}},
		},
		Credentials: &engine.Credentials{DSN: dsn},
	}

	result, err := NewWithTableSizeLimit(1).Plan(t.Context(), req)
	require.NoError(t, err)
	require.Len(t, result.Changes, 1)
	require.Len(t, result.Changes[0].TableChanges, 1)
	change := result.Changes[0].TableChanges[0]
	assert.Empty(t, change.ExecutionMode, "a greenfield create has no existing data for the ceiling to bound")
	assert.Empty(t, change.ModeReason)
}

// TestEnginePlanUndeclaredTableIsBlockedDrop proves a live table that no
// schema file declares never vanishes from the plan: the declarative diff
// surfaces it as a DROP TABLE that is both destructive and blocked, so the
// reviewer sees the divergence and the apply path can never run the drop. The
// verdict prescribes only remedies the operator can follow — a table that owns
// a foreign key cannot be declared, and a table that another table's foreign
// key references cannot be declared by a pulled file, so each reason names the
// constraint and the side it lives on instead of a file that cannot be
// written. A declared table that already matches its file contributes nothing,
// even when foreign keys elsewhere reference it. Tables whose definition lives
// elsewhere are not reported on their own: a partition belongs to its parent
// and an extension-owned table to its extension. Archive tables are exempt by
// naming convention, as they are on MySQL, while an underscore prefix means
// nothing on PostgreSQL. An inheritance child and an unlogged table are
// ordinary tables that must carry their own file.
func TestEnginePlanUndeclaredTableIsBlockedDrop(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "plan_undeclared_test")
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY, email text NOT NULL);
		CREATE TABLE public.legacy_users (id bigint PRIMARY KEY);
		CREATE TABLE public.orders (id bigint PRIMARY KEY, user_id bigint REFERENCES public.users (id));
		CREATE TABLE public.regions (id bigint PRIMARY KEY);
		CREATE TABLE public.warehouses (id bigint PRIMARY KEY, region_id bigint REFERENCES public.regions (id));
		CREATE TABLE public.events (id bigint, day date) PARTITION BY RANGE (day);
		CREATE TABLE public.events_2026 PARTITION OF public.events
			FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
		CREATE TABLE public.users_history () INHERITS (public.users);
		CREATE UNLOGGED TABLE public.scratch (id bigint PRIMARY KEY);
		CREATE TABLE public._settings (id bigint PRIMARY KEY);
		CREATE TABLE public.audit_log_archive_2019 (id bigint PRIMARY KEY);
		CREATE EXTENSION hstore;
		CREATE TABLE public.ext_owned_config (key text PRIMARY KEY);
		ALTER EXTENSION hstore ADD TABLE public.ext_owned_config`)
	require.NoError(t, err)
	req := &engine.PlanRequest{
		Database: "plan_undeclared_test",
		SchemaFiles: schema.SchemaFiles{
			"public": {Files: map[string]string{
				"users.sql": "CREATE TABLE users (id bigint PRIMARY KEY, email text NOT NULL)",
			}},
		},
		Credentials: &engine.Credentials{DSN: dsn},
	}

	result, err := New().Plan(t.Context(), req)
	require.NoError(t, err)
	assert.False(t, result.NoChanges, "an undeclared live table is a change the reviewer must see")
	require.Len(t, result.Changes, 1)
	assert.Equal(t, "public", result.Changes[0].Namespace)
	require.Len(t, result.Changes[0].TableChanges, 8,
		"every undeclared table with a definition of its own is reported; the declared table, its partition, the extension-owned table and the archive table are not")

	const declarable = "declare the table in a schema file to keep it under management, or drop it through a separately reviewed process"
	const ownsForeignKey = `cannot be declared while it carries foreign key constraint(s) "orders_user_id_fkey", which schema files do not support — drop the table, or remove its foreign keys before declaring it, through a separately reviewed process`
	const referencedByForeignKey = `foreign key constraint(s) "warehouses_region_id_fkey" on other tables reference it, and schema files do not support foreign keys, so the schema pull cannot write a file for it — declare the table by hand in a schema file to keep it under management, or drop it together with the referencing constraints through a separately reviewed process`
	for i, want := range []struct{ table, ddl, remedy string }{
		{"_settings", "DROP TABLE public._settings", declarable},
		{"events", "DROP TABLE public.events", declarable},
		{"legacy_users", "DROP TABLE public.legacy_users", declarable},
		{"orders", "DROP TABLE public.orders", ownsForeignKey},
		{"regions", "DROP TABLE public.regions", referencedByForeignKey},
		{"scratch", "DROP TABLE public.scratch", declarable},
		{"users_history", "DROP TABLE public.users_history", declarable},
		{"warehouses", "DROP TABLE public.warehouses", `cannot be declared while it carries foreign key constraint(s) "warehouses_region_id_fkey"`},
	} {
		change := result.Changes[0].TableChanges[i]
		assert.Equal(t, want.table, change.Table)
		assert.Equal(t, ddl.StatementDropTable, change.Operation)
		assert.Equal(t, want.ddl, change.DDL)
		assert.True(t, change.IsUnsafe, "a drop removes live data and must carry the unsafe flag")
		assert.Equal(t, `DROP TABLE removes all data from table "`+want.table+`"`, change.UnsafeReason)
		assert.Equal(t, engine.ExecutionModeBlocked, change.ExecutionMode,
			"the native-safe path never executes a drop, so the verdict must say so at plan time")
		assert.Contains(t, change.ModeReason, `table "`+want.table+`" exists on the target but no schema file in namespace "public" declares it`)
		assert.Contains(t, change.ModeReason, want.remedy)
	}
	for _, table := range []string{"legacy_users", "orders", "regions", "warehouses", "scratch", "users_history", "_settings", "audit_log_archive_2019", "ext_owned_config"} {
		assert.True(t, testutil.PostgresTableExists(t, db, "public", table), "planning must never touch the target")
	}
}

// TestEnginePlanUndeclaredTableInMissingSchema proves a namespace whose
// schema does not exist yet plans as a greenfield create: the live-table
// enumeration finds nothing to reconcile against, and the declared table is
// created rather than the plan failing on the absent schema.
func TestEnginePlanUndeclaredTableInMissingSchema(t *testing.T) {
	dsn, _ := testutil.StartPostgres(t, "plan_missing_schema_test")
	req := &engine.PlanRequest{
		Database: "plan_missing_schema_test",
		SchemaFiles: schema.SchemaFiles{
			"app": {Files: map[string]string{
				"users.sql": "CREATE TABLE users (id bigint PRIMARY KEY)",
			}},
		},
		Credentials: &engine.Credentials{DSN: dsn},
	}

	result, err := New().Plan(t.Context(), req)
	require.NoError(t, err)
	require.Len(t, result.Changes, 1)
	require.Len(t, result.Changes[0].TableChanges, 1)
	assert.Equal(t, ddl.StatementCreateTable, result.Changes[0].TableChanges[0].Operation)
}

// TestEnginePlanUndeclaredTablesAcrossNamespaces proves every namespace in
// the request is reconciled against its own live tables: an undeclared table
// in one namespace is reported there and nowhere else, so a repository with
// several namespaces sees each divergence under the namespace that owns it.
func TestEnginePlanUndeclaredTablesAcrossNamespaces(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "plan_namespaces_test")
	_, err := db.ExecContext(t.Context(), `
		CREATE SCHEMA app;
		CREATE SCHEMA billing;
		CREATE TABLE app.users (id bigint PRIMARY KEY);
		CREATE TABLE app.legacy_users (id bigint PRIMARY KEY);
		CREATE TABLE billing.invoices (id bigint PRIMARY KEY);
		CREATE TABLE billing.legacy_invoices (id bigint PRIMARY KEY)`)
	require.NoError(t, err)
	req := &engine.PlanRequest{
		Database: "plan_namespaces_test",
		SchemaFiles: schema.SchemaFiles{
			"app":     {Files: map[string]string{"users.sql": "CREATE TABLE users (id bigint PRIMARY KEY)"}},
			"billing": {Files: map[string]string{"invoices.sql": "CREATE TABLE invoices (id bigint PRIMARY KEY)"}},
		},
		Credentials: &engine.Credentials{DSN: dsn},
	}

	result, err := New().Plan(t.Context(), req)
	require.NoError(t, err)
	assert.False(t, result.NoChanges)
	require.Len(t, result.Changes, 2)
	dropsByNamespace := make(map[string][]string, len(result.Changes))
	for _, change := range result.Changes {
		for _, tc := range change.TableChanges {
			assert.Equal(t, ddl.StatementDropTable, tc.Operation)
			assert.Equal(t, engine.ExecutionModeBlocked, tc.ExecutionMode)
			assert.Contains(t, tc.ModeReason, `no schema file in namespace "`+change.Namespace+`" declares it`)
			dropsByNamespace[change.Namespace] = append(dropsByNamespace[change.Namespace], tc.Table)
		}
	}
	assert.Equal(t, map[string][]string{
		"app":     {"legacy_users"},
		"billing": {"legacy_invoices"},
	}, dropsByNamespace, "each namespace reports exactly its own undeclared table")
}

// TestLiveTablesUnderShadowingSearchPath proves the catalog reads behind the
// undeclared-table verdict and the schema pull fail closed when the target's
// search_path lists a user schema ahead of pg_catalog. Decoy relations named
// after every catalog table the queries touch, decoy "=", "<>", ">" and ">="
// operators over every operand type the queries compare, and decoy array_agg
// and cardinality routines must not shadow the catalog: a shadowed
// enumeration would come back empty and an undeclared table would linger
// silently, and a shadowed pull would hand the operator a wrong baseline. The
// fixture includes a partition and an extension-owned table so the exemption
// subqueries have rows to get wrong, not a vacuous NOT EXISTS.
func TestLiveTablesUnderShadowingSearchPath(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "plan_shadow_test")
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.orders (id bigint PRIMARY KEY, user_id bigint REFERENCES public.users (id));
		CREATE TABLE public.events (id bigint, day date) PARTITION BY RANGE (day);
		CREATE TABLE public.events_2026 PARTITION OF public.events
			FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');
		CREATE EXTENSION hstore;
		CREATE TABLE public.ext_owned_config (key text PRIMARY KEY);
		ALTER EXTENSION hstore ADD TABLE public.ext_owned_config;
		CREATE SCHEMA hostile;
		CREATE TABLE hostile.pg_class (oid oid, relname name, relnamespace oid, relkind "char", relispartition boolean,
			relrowsecurity boolean, relforcerowsecurity boolean, reloptions text[]);
		CREATE TABLE hostile.pg_namespace (oid oid, nspname name);
		CREATE TABLE hostile.pg_inherits (inhrelid oid);
		CREATE TABLE hostile.pg_depend (classid oid, objid oid, deptype "char");
		CREATE TABLE hostile.pg_constraint (conrelid oid, confrelid oid, conname name, contype "char");
		CREATE TABLE hostile.pg_trigger (tgrelid oid, tgisinternal boolean);
		CREATE TABLE hostile.pg_policy (polrelid oid);
		CREATE TABLE hostile.pg_description (objoid oid, objsubid integer);
		CREATE FUNCTION hostile.never(oid, oid) RETURNS boolean LANGUAGE sql IMMUTABLE AS 'SELECT false';
		CREATE FUNCTION hostile.never("char", "char") RETURNS boolean LANGUAGE sql IMMUTABLE AS 'SELECT false';
		CREATE FUNCTION hostile.never(name, name) RETURNS boolean LANGUAGE sql IMMUTABLE AS 'SELECT false';
		CREATE FUNCTION hostile.never(name, text) RETURNS boolean LANGUAGE sql IMMUTABLE AS 'SELECT false';
		CREATE FUNCTION hostile.never(integer, integer) RETURNS boolean LANGUAGE sql IMMUTABLE AS 'SELECT false';
		CREATE OPERATOR hostile.= (LEFTARG = oid, RIGHTARG = oid, FUNCTION = hostile.never);
		CREATE OPERATOR hostile.<> (LEFTARG = oid, RIGHTARG = oid, FUNCTION = hostile.never);
		CREATE OPERATOR hostile.= (LEFTARG = "char", RIGHTARG = "char", FUNCTION = hostile.never);
		CREATE OPERATOR hostile.= (LEFTARG = name, RIGHTARG = name, FUNCTION = hostile.never);
		CREATE OPERATOR hostile.= (LEFTARG = name, RIGHTARG = text, FUNCTION = hostile.never);
		CREATE OPERATOR hostile.>= (LEFTARG = integer, RIGHTARG = integer, FUNCTION = hostile.never);
		CREATE OPERATOR hostile.> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = hostile.never);
		CREATE FUNCTION hostile.empty_agg(anyarray, anyelement) RETURNS anyarray LANGUAGE sql IMMUTABLE AS 'SELECT $1';
		CREATE AGGREGATE hostile.array_agg(anyelement) (SFUNC = hostile.empty_agg, STYPE = anyarray);
		CREATE FUNCTION hostile.cardinality(anyarray) RETURNS integer LANGUAGE sql IMMUTABLE AS 'SELECT 1';
		ALTER DATABASE plan_shadow_test SET search_path = hostile, pg_catalog, public`)
	require.NoError(t, err)
	poolCfg, err := spritePoolConfig(dsn, "")
	require.NoError(t, err)
	pool, err := dbconn.NewPool(t.Context(), poolCfg)
	require.NoError(t, err)
	defer pool.Close()

	var searchPath string
	require.NoError(t, pool.QueryRow(t.Context(), "SHOW search_path").Scan(&searchPath))
	require.Equal(t, "hostile, pg_catalog, public", searchPath, "the pool must see the hostile search_path for the test to prove anything")

	tables, err := liveTables(t.Context(), pool, "public")
	require.NoError(t, err)
	assert.Equal(t, []liveTable{
		{name: "events", foreignKeys: []string{}, referencedBy: []string{}},
		{name: "orders", foreignKeys: []string{"orders_user_id_fkey"}, referencedBy: []string{}},
		{name: "users", foreignKeys: []string{}, referencedBy: []string{"orders_user_id_fkey"}},
	}, tables, "every live table and its foreign keys must be enumerated, and only the partition and the extension-owned table left out, despite the shadowing search_path")

	pulled, err := pullTables(t.Context(), pool, "public")
	require.NoError(t, err)
	assert.Equal(t, []string{"events", "orders", "users"}, pulled, "the pull must render the same set the plan holds files accountable for")

	namespaces, err := pullNamespaces(t.Context(), pool, "")
	require.NoError(t, err)
	assert.Equal(t, []string{"hostile", "public"}, namespaces, "namespace discovery must read the real catalog")
	namespaces, err = pullNamespaces(t.Context(), pool, "public")
	require.NoError(t, err)
	assert.Equal(t, []string{"public"}, namespaces, "the existence check for a requested namespace must read the real catalog")

	objects, err := pullUnmodeledTableObjects(t.Context(), pool, "public", "users")
	require.NoError(t, err)
	assert.Equal(t, unmodeledTableObjects{}, objects, "a plain table must not acquire unmodeled objects from decoy catalog rows")
}

// TestEngineApplyNativeSafe proves a planned metadata-only ALTER runs through
// pg-sprite's preflight and bounded optimistic executor.
func TestEngineApplyNativeSafe(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "apply_test")
	_, err := db.ExecContext(t.Context(), "CREATE TABLE public.users (id bigint PRIMARY KEY)")
	require.NoError(t, err)

	eng := New()
	result, err := eng.Apply(t.Context(), applyRequest(dsn, "users", "ALTER TABLE public.users ADD COLUMN email text"))
	require.NoError(t, err)
	assert.True(t, result.Accepted)
	progress := awaitPostgresProgress(t, eng, "users")
	assert.Equal(t, engine.StateCompleted, progress.State)
	assert.Equal(t, 100, progress.Progress)
	assert.Equal(t, "completed", progress.Metadata["phase"])
	assert.Equal(t, "1", progress.Metadata["step"])

	var exists bool
	err = db.QueryRowContext(t.Context(), `SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'email')`).Scan(&exists)
	require.NoError(t, err)
	assert.True(t, exists)
}

// TestEngineApplyCreateTable proves a greenfield CREATE TABLE executes
// through the native-safe path end to end: absence and schema CREATE access
// are proved in the executing session, the table lands with its declared
// shape, and the drive reports completion.
func TestEngineApplyCreateTable(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "create_test")

	eng := New()
	result, err := eng.Apply(t.Context(), applyRequest(dsn, "widgets",
		"CREATE TABLE public.widgets (id bigint PRIMARY KEY, name text NOT NULL)"))
	require.NoError(t, err)
	assert.True(t, result.Accepted)
	progress := awaitPostgresProgress(t, eng, "widgets")
	assert.Equal(t, engine.StateCompleted, progress.State)
	assert.Equal(t, 100, progress.Progress)
	assert.Equal(t, "completed", progress.Metadata["phase"])

	var exists bool
	err = db.QueryRowContext(t.Context(), `SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'widgets' AND column_name = 'name')`).Scan(&exists)
	require.NoError(t, err)
	assert.True(t, exists)
}

// TestEngineApplyGreenfieldCreateSet proves a new table and its declared indexes
// execute as one unit before the table can carry traffic, then converge cleanly.
func TestEngineApplyGreenfieldCreateSet(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "create_set_test")
	desired := "CREATE TABLE widgets (id bigint PRIMARY KEY, name text); CREATE UNIQUE INDEX widgets_name_key ON widgets (name); CREATE INDEX widgets_id_idx ON widgets (id);"
	planRequest := &engine.PlanRequest{
		Database:    "create_set_test",
		SchemaFiles: schema.SchemaFiles{"public": {Files: map[string]string{"widgets.sql": desired}}},
		Credentials: &engine.Credentials{DSN: dsn},
	}

	eng := New()
	plan, err := eng.Plan(t.Context(), planRequest)
	require.NoError(t, err)
	require.Len(t, plan.Changes, 1)
	require.Len(t, plan.Changes[0].TableChanges, 1)
	change := plan.Changes[0].TableChanges[0]
	assert.Empty(t, change.ExecutionMode)
	assert.Contains(t, change.DDL, ";\nCREATE UNIQUE INDEX widgets_name_key")
	assert.Contains(t, change.DDL, ";\nCREATE INDEX widgets_id_idx")

	result, err := eng.Apply(t.Context(), applyRequest(dsn, "widgets", change.DDL))
	require.NoError(t, err)
	assert.True(t, result.Accepted)
	progress := awaitPostgresProgress(t, eng, "widgets")
	assert.Equal(t, engine.StateCompleted, progress.State)

	rows, err := db.QueryContext(t.Context(), "SELECT indexname FROM pg_indexes WHERE schemaname = 'public' AND tablename = 'widgets' ORDER BY indexname")
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)
	var indexes []string
	for rows.Next() {
		var index string
		require.NoError(t, rows.Scan(&index))
		indexes = append(indexes, index)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []string{"widgets_id_idx", "widgets_name_key", "widgets_pkey"}, indexes)

	converged, err := eng.Plan(t.Context(), planRequest)
	require.NoError(t, err)
	assert.True(t, converged.NoChanges)
}

// TestEngineApplyCreateSetDuplicateNameRefusal proves name ownership across the
// table and index declarations is validated before any object is created.
func TestEngineApplyCreateSetDuplicateNameRefusal(t *testing.T) {
	dsn, _ := testutil.StartPostgres(t, "duplicate_create_name_test")
	eng := New()
	_, err := eng.Apply(t.Context(), applyRequest(dsn, "widgets",
		"CREATE TABLE public.widgets (id int PRIMARY KEY);\nCREATE INDEX widgets_pkey ON public.widgets (id)"))
	require.NoError(t, err)
	progress := awaitPostgresProgress(t, eng, "widgets")
	assert.Equal(t, engine.StateFailed, progress.State)
	assert.Equal(t, "refused", progress.Metadata["phase"])
	assert.Equal(t, `the create set for "widgets" claims the same relation name twice (a CREATE INDEX name repeats the table's implicit constraint-index name or another index); fix the schema file and re-plan`, progress.ErrorMessage)
}

// TestEngineApplyCreateCollisionRefusal proves a CREATE TABLE whose name is
// already occupied on the target is a permanent refusal directing a re-plan —
// the apply must never guess whether the occupying relation is the desired
// one — not a retryable failure.
func TestEngineApplyCreateCollisionRefusal(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "collision_test")
	_, err := db.ExecContext(t.Context(), "CREATE TABLE public.widgets (id bigint PRIMARY KEY)")
	require.NoError(t, err)

	eng := New()
	_, err = eng.Apply(t.Context(), applyRequest(dsn, "widgets",
		"CREATE TABLE public.widgets (id bigint PRIMARY KEY, name text NOT NULL)"))
	require.NoError(t, err)
	progress := awaitPostgresProgress(t, eng, "widgets")
	assert.Equal(t, engine.StateFailed, progress.State)
	assert.Equal(t, "refused", progress.Metadata["phase"])
	assert.False(t, progress.Retryable, "a collision refusal is permanent until a re-plan; the drive must not offer a retry")
	assert.Equal(t, `a relation already occupies a name the create set for "widgets" claims (the table, or one of its index names); re-plan against the current schema`, progress.ErrorMessage)
}

// TestEngineApplyCreateSetCommittedPrefixNotRetryable proves a create set
// that fails after its CREATE TABLE committed is published as a failure the
// drive must not retry: the table now exists, so a retry can only land on a
// collision refusal for a state the operator did not author. The second
// statement fails on the server because its operator class does not accept
// the column's type — a shape the desired-schema parse admits, so nothing
// refuses it before the first step commits.
func TestEngineApplyCreateSetCommittedPrefixNotRetryable(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "committed_prefix_test")

	eng := New()
	_, err := eng.Apply(t.Context(), applyRequest(dsn, "widgets",
		"CREATE TABLE public.widgets (id bigint PRIMARY KEY, name integer);\nCREATE INDEX widgets_name_idx ON public.widgets (name text_pattern_ops)"))
	require.NoError(t, err)
	progress := awaitPostgresProgress(t, eng, "widgets")
	assert.Equal(t, engine.StateFailed, progress.State)
	assert.Equal(t, "failed", progress.Metadata["phase"])
	assert.False(t, progress.Retryable, "the CREATE TABLE committed; a retry cannot succeed, so the drive must not offer one")
	assert.Equal(t, `step 2 of 2 failed after the CREATE TABLE for "widgets" committed; re-plan against the current schema`, progress.ErrorMessage)

	var exists bool
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT to_regclass('public.widgets') IS NOT NULL").Scan(&exists))
	assert.True(t, exists, "the committed CREATE TABLE stays for the next plan to reconcile")
}

// TestEngineApplyCreateIfNotExistsRefusal proves a CREATE TABLE carrying
// IF NOT EXISTS is a permanent refusal naming the clause: the native-safe
// path cannot prove what the clause's silent no-op would mean, so the drive
// must direct the operator to drop it and re-plan — never offer a retry that
// refails identically.
func TestEngineApplyCreateIfNotExistsRefusal(t *testing.T) {
	dsn, _ := testutil.StartPostgres(t, "if_not_exists_test")

	eng := New()
	_, err := eng.Apply(t.Context(), applyRequest(dsn, "widgets",
		"CREATE TABLE IF NOT EXISTS public.widgets (id bigint PRIMARY KEY)"))
	require.NoError(t, err)
	progress := awaitPostgresProgress(t, eng, "widgets")
	assert.Equal(t, engine.StateFailed, progress.State)
	assert.Equal(t, "refused", progress.Metadata["phase"])
	assert.False(t, progress.Retryable, "an IF NOT EXISTS refusal is permanent until the clause is dropped and the change re-planned")
	assert.Contains(t, progress.ErrorMessage, "IF NOT EXISTS")
	assert.Contains(t, progress.ErrorMessage, "re-plan")
}

// TestEngineApplyPrivilegeRefusal proves a role that cannot alter the target
// is blocked before execution and receives exact provisioning SQL.
func TestEngineApplyPrivilegeRefusal(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "privilege_test")
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE ROLE limited LOGIN PASSWORD 'limited';
		GRANT CONNECT ON DATABASE privilege_test TO limited;
		GRANT USAGE ON SCHEMA public TO limited`)
	require.NoError(t, err)

	limitedDSN, err := url.Parse(dsn)
	require.NoError(t, err)
	limitedDSN.User = url.UserPassword("limited", "limited")
	eng := New()
	_, err = eng.Apply(t.Context(), applyRequest(limitedDSN.String(), "users", "ALTER TABLE public.users ADD COLUMN email text"))
	require.NoError(t, err)
	progress := awaitPostgresProgress(t, eng, "users")
	assert.Equal(t, engine.StateFailed, progress.State)
	assert.Equal(t, "refused", progress.Metadata["phase"])
	assert.False(t, progress.Retryable, "a refusal is permanent; the drive must not offer a retry")
	assert.Contains(t, progress.ErrorMessage, "GRANT")
}

// TestEngineApplyTableNotFoundRefusal proves a change targeting a table that
// does not exist on the target is a permanent refusal — retrying cannot
// succeed until the schema change is re-planned — not a retryable failure.
func TestEngineApplyTableNotFoundRefusal(t *testing.T) {
	dsn, _ := testutil.StartPostgres(t, "failure_test")
	eng := New()
	_, err := eng.Apply(t.Context(), applyRequest(dsn, "missing_users", "ALTER TABLE public.missing_users ADD COLUMN email text"))
	require.NoError(t, err)
	progress := awaitPostgresProgress(t, eng, "missing_users")
	assert.Equal(t, engine.StateFailed, progress.State)
	assert.Equal(t, "refused", progress.Metadata["phase"])
	assert.False(t, progress.Retryable, "a refusal is permanent; the drive must not offer a retry")
	assert.Contains(t, progress.ErrorMessage, "missing_users")
}

// TestEngineApplyTableSizeRefusal proves the configured native-safe ceiling
// reaches preflight and permanently refuses an ALTER when the table exceeds it.
func TestEngineApplyTableSizeRefusal(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "size_limit_test")
	_, err := db.ExecContext(t.Context(), "CREATE TABLE public.users (id bigint PRIMARY KEY)")
	require.NoError(t, err)

	eng := NewWithTableSizeLimit(1)
	_, err = eng.Apply(t.Context(), applyRequest(dsn, "users", "ALTER TABLE public.users ADD COLUMN email text"))
	require.NoError(t, err)
	progress := awaitPostgresProgress(t, eng, "users")
	assert.Equal(t, engine.StateFailed, progress.State)
	assert.Equal(t, "refused", progress.Metadata["phase"])
	assert.False(t, progress.Retryable, "a size refusal is permanent until the ceiling or target changes")
	assert.Contains(t, progress.ErrorMessage, "1-byte threshold")
}

// TestEngineApplyOperationalFailure proves an execution-path error — here an
// unreachable target — remains a failed operation rather than being
// misclassified as a safety refusal.
func TestEngineApplyOperationalFailure(t *testing.T) {
	eng := New()
	unreachableDSN := "postgres://postgres:postgres@127.0.0.1:1/failure_test?sslmode=disable"
	_, err := eng.Apply(t.Context(), applyRequest(unreachableDSN, "users", "ALTER TABLE public.users ADD COLUMN email text"))
	require.NoError(t, err)
	progress := awaitPostgresProgress(t, eng, "users")
	assert.Equal(t, engine.StateFailed, progress.State)
	assert.Equal(t, "failed", progress.Metadata["phase"])
	assert.True(t, progress.Retryable, "an operational failure must reach the retryable task state, not cancel the apply's remaining work")
}

// TestEngineSharedAcrossAppliesKeepsProgressPerApply proves the engine
// answers Progress for the apply the caller identifies. One engine lives for
// a target's lifetime, so after a second apply claims it, the first apply's
// identity must read the idle sentinel — never the other apply's state — and
// Drain must leave the engine idle for the next drive's clean poll.
func TestEngineSharedAcrossAppliesKeepsProgressPerApply(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "shared_test")
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.t_a (id bigint PRIMARY KEY);
		CREATE TABLE public.t_b (id bigint PRIMARY KEY)`)
	require.NoError(t, err)

	eng := New()
	_, err = eng.Apply(t.Context(), applyRequest(dsn, "t_a", "ALTER TABLE public.t_a ADD COLUMN a text"))
	require.NoError(t, err)
	first := awaitPostgresProgress(t, eng, "t_a")
	assert.Equal(t, engine.StateCompleted, first.State)

	// Before the second apply exists, polling its identity must not surface
	// the first apply's terminal state.
	other, err := eng.Progress(t.Context(), progressRequestFor("t_b"))
	require.NoError(t, err)
	assert.Equal(t, engine.StatePending, other.State)
	assert.Equal(t, "No active schema change", other.Message)

	_, err = eng.Apply(t.Context(), applyRequest(dsn, "t_b", "ALTER TABLE public.t_b ADD COLUMN b text"))
	require.NoError(t, err)
	second := awaitPostgresProgress(t, eng, "t_b")
	assert.Equal(t, engine.StateCompleted, second.State)
	require.Len(t, second.Tables, 1)
	assert.Equal(t, "t_b", second.Tables[0].Table)

	// The superseded apply's identity now reads idle, and a drain leaves the
	// engine idle for every identity.
	stale, err := eng.Progress(t.Context(), progressRequestFor("t_a"))
	require.NoError(t, err)
	assert.Equal(t, engine.StatePending, stale.State)
	eng.Drain()
	drained, err := eng.Progress(t.Context(), progressRequestFor("t_b"))
	require.NoError(t, err)
	assert.Equal(t, engine.StatePending, drained.State)
	assert.Equal(t, "No active schema change", drained.Message)
}

// TestEngineApplyRefusesNonNativeShape proves a statement shape the
// native-safe path cannot execute is refused synchronously at acceptance,
// before any background work starts.
func TestEngineApplyRefusesNonNativeShape(t *testing.T) {
	dsn, _ := testutil.StartPostgres(t, "shape_test")
	eng := New()
	_, err := eng.Apply(t.Context(), applyRequest(dsn, "users", "DROP TABLE public.users"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not execute this statement shape yet")
}

// TestEngineApplyConcurrentIndexBuild proves a CREATE INDEX CONCURRENTLY
// routes to the dedicated concurrent-build executor and completes: the
// statement must never reach the transactional optimistic executor, whose
// transaction block PostgreSQL refuses, and the built index must be valid
// on the target when the drive reports completion.
func TestEngineApplyConcurrentIndexBuild(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "index_build_test")
	_, err := db.ExecContext(t.Context(), "CREATE TABLE public.users (id bigint PRIMARY KEY, email text)")
	require.NoError(t, err)

	eng := New()
	result, err := eng.Apply(t.Context(), applyRequest(dsn, "users",
		"CREATE INDEX CONCURRENTLY users_email_idx ON public.users (email)"))
	require.NoError(t, err)
	assert.True(t, result.Accepted)
	progress := awaitPostgresProgress(t, eng, "users")
	assert.Equal(t, engine.StateCompleted, progress.State)
	assert.Equal(t, 100, progress.Progress)
	assert.Equal(t, "completed", progress.Metadata["phase"])

	var valid bool
	err = db.QueryRowContext(t.Context(),
		`SELECT i.indisvalid FROM pg_index i WHERE i.indexrelid = 'public.users_email_idx'::regclass`).Scan(&valid)
	require.NoError(t, err)
	assert.True(t, valid, "the built index must be catalog-valid, not merely present")
}

// TestEngineApplyPartitionedParentConcurrentIndexRefusal proves the
// partition admission policy runs at apply time: a concurrent build against
// a partitioned parent is permanently refused with the typed fixed-sentence
// detail, never attempted as a raw statement the server would fail.
func TestEngineApplyPartitionedParentConcurrentIndexRefusal(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "partition_test")
	_, err := db.ExecContext(t.Context(),
		"CREATE TABLE public.events (id bigint, created date) PARTITION BY RANGE (created)")
	require.NoError(t, err)

	eng := New()
	_, err = eng.Apply(t.Context(), applyRequest(dsn, "events",
		"CREATE INDEX CONCURRENTLY events_created_idx ON public.events (created)"))
	require.NoError(t, err)
	progress := awaitPostgresProgress(t, eng, "events")
	assert.Equal(t, engine.StateFailed, progress.State)
	assert.Equal(t, "refused", progress.Metadata["phase"])
	assert.False(t, progress.Retryable, "a partitioned-parent refusal is permanent until the plan or target changes")
	assert.Contains(t, progress.ErrorMessage, "cannot build parent-level indexes concurrently")
}

// TestEngineApplyConcurrentIndexPreexistingInvalidRetryable proves an
// invalid index already occupying the target name fails the build as a
// retryable operational failure whose detail names the index and the
// investigation step — the entry may be another actor's build still in
// progress, so the advice is to check for one before any recovery, never a
// statement to run.
func TestEngineApplyConcurrentIndexPreexistingInvalidRetryable(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "invalid_index_test")
	_, err := db.ExecContext(t.Context(), "CREATE TABLE public.orders (id bigint PRIMARY KEY, ref text)")
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "CREATE INDEX orders_ref_idx ON public.orders (ref)")
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(),
		"UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'public.orders_ref_idx'::regclass")
	require.NoError(t, err)

	eng := New()
	_, err = eng.Apply(t.Context(), applyRequest(dsn, "orders",
		"CREATE INDEX CONCURRENTLY orders_ref_idx ON public.orders (ref)"))
	require.NoError(t, err)
	progress := awaitPostgresProgress(t, eng, "orders")
	assert.Equal(t, engine.StateFailed, progress.State)
	assert.Equal(t, "failed", progress.Metadata["phase"])
	assert.True(t, progress.Retryable, "an operator can clear the invalid index; the drive must offer the retry")
	assert.Contains(t, progress.ErrorMessage, "orders_ref_idx")
	assert.Contains(t, progress.ErrorMessage, "another actor's build")
	assert.Contains(t, progress.ErrorMessage, "pg_stat_activity")
}

// applyRequest builds a single-statement apply request with the same identity
// shape the drive layer uses: the task identifier stamped into
// ResumeState.MigrationContext keys the engine's progress to this apply.
func applyRequest(dsn, table, statement string) *engine.ApplyRequest {
	return &engine.ApplyRequest{
		Database: "app",
		Changes: []engine.SchemaChange{{
			Namespace:    "public",
			TableChanges: []engine.TableChange{{Table: table, DDL: statement}},
		}},
		Credentials: &engine.Credentials{DSN: dsn},
		ResumeState: &engine.ResumeState{MigrationContext: applyTaskID(table)},
	}
}

func applyTaskID(table string) string {
	return "task-" + table
}

func progressRequestFor(table string) *engine.ProgressRequest {
	return &engine.ProgressRequest{ResumeState: &engine.ResumeState{MigrationContext: applyTaskID(table)}}
}

func awaitPostgresProgress(t *testing.T, eng *Engine, table string) *engine.ProgressResult {
	t.Helper()
	var result *engine.ProgressResult
	// assert then require so the failure message formats the last polled
	// progress instead of the pre-poll nil value.
	finished := assert.Eventually(t, func() bool {
		var err error
		result, err = eng.Progress(t.Context(), progressRequestFor(table))
		require.NoError(t, err)
		return result.State.IsTerminal()
	}, postgresApplyDeadline, 10*time.Millisecond)
	require.True(t, finished, "PostgreSQL apply did not finish; last progress: %+v", result)
	return result
}
