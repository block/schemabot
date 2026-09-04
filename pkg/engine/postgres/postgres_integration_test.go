//go:build integration

package postgres

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/block/pg-sprite/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	assert.Contains(t, progress.ErrorMessage, "re-plan")
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
