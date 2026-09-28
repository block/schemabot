//go:build integration

package postgres

import (
	"context"
	"database/sql"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
)

// Keep the image pin in .github/workflows/test.yaml in sync. This is the same
// PostgreSQL image used by pg-sprite's Supabase compatibility suite.
const supabasePostgresImage = "supabase/postgres:17.6.1.136@sha256:f371b5f3f2ac0a05703f33d6e6134515fb2498cab708fb948a0aeb7481467c00"

const supabaseOperationDeadline = 30 * time.Second

// TestSupabaseEngine exercises SchemaBot's adapter against Supabase's real
// PostgreSQL image as its non-superuser postgres owner. Native changes converge;
// unsupported changes and incomplete schema exports are refused explicitly.
// This is database coverage, not hosted API, Auth service, or Realtime coverage.
func TestSupabaseEngine(t *testing.T) {
	dsn, db := startSupabasePostgres(t)

	t.Run("add nullable column", func(t *testing.T) {
		testSupabaseNativeChange(t, dsn, db, `
			CREATE TABLE documents (
				id bigint PRIMARY KEY,
				title text,
				summary text
			);`, "ADD COLUMN summary text")
	})

	t.Run("add concurrent index", func(t *testing.T) {
		testSupabaseNativeChange(t, dsn, db, `
			CREATE TABLE documents (
				id bigint PRIMARY KEY,
				title text
			);
			CREATE INDEX documents_title_idx ON documents (title);`, "CREATE INDEX CONCURRENTLY")
	})

	t.Run("RLS pull refuses incomplete export", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), supabaseOperationDeadline)
		defer cancel()
		seedSupabaseDocuments(t, ctx, db)
		_, err := db.ExecContext(ctx, `
			ALTER TABLE public.documents ADD COLUMN owner_id uuid;
			UPDATE public.documents SET owner_id = '11111111-1111-1111-1111-111111111111' WHERE id = 1;
			UPDATE public.documents SET owner_id = '22222222-2222-2222-2222-222222222222' WHERE id = 2;
			ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;
			CREATE POLICY own_document ON public.documents
				FOR SELECT TO authenticated
				USING (owner_id = auth.uid());
		`)
		require.NoError(t, err)
		eng := NewForTarget(0, 0, "postgres", &engine.Credentials{DSN: dsn})
		response, err := eng.PullSchema(ctx, supabasePullRequest())
		require.Error(t, err)
		assert.Nil(t, response, "must not return a baseline that loses access rules")
		assert.Contains(t, err.Error(), "row-level security")
		assert.Contains(t, err.Error(), "policy")

		// Native DDL keeps the original relation and its access rules. This
		// direct adapter apply does not claim declarative RLS management.
		result, err := eng.Apply(ctx, applyRequest(dsn, "documents",
			"ALTER TABLE public.documents ADD COLUMN summary text"))
		require.NoError(t, err)
		require.True(t, result.Accepted)
		require.Equal(t, engine.StateCompleted, awaitPostgresProgress(t, eng, "documents").State)
		var columnType string
		require.NoError(t, db.QueryRowContext(ctx, `
			SELECT data_type FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = 'documents' AND column_name = 'summary'
		`).Scan(&columnType))
		assert.Equal(t, "text", columnType, "the native apply must add the requested column")
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { assert.NoError(t, tx.Rollback()) }()
		_, err = tx.ExecContext(ctx, `
			SET LOCAL ROLE authenticated;
			SET LOCAL request.jwt.claim.sub = '11111111-1111-1111-1111-111111111111';
		`)
		require.NoError(t, err)
		var role string
		var superuser, bypassRLS bool
		require.NoError(t, tx.QueryRowContext(ctx, `
			SELECT current_user, rolsuper, rolbypassrls
			FROM pg_roles WHERE rolname = current_user
		`).Scan(&role, &superuser, &bypassRLS))
		assert.Equal(t, "authenticated", role)
		assert.False(t, superuser)
		assert.False(t, bypassRLS, "visibility must be checked as an application reader")
		var count int
		require.NoError(t, tx.QueryRowContext(ctx, "SELECT count(*) FROM public.documents").Scan(&count))
		assert.Equal(t, 1, count, "authenticated readers still see only their permitted row")
	})

	t.Run("copy and swap is blocked", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), supabaseOperationDeadline)
		defer cancel()
		seedSupabaseDocuments(t, ctx, db)
		plan, err := New().Plan(ctx, supabasePlanRequest(dsn, `
			CREATE TABLE documents (
				id bigint PRIMARY KEY,
				title varchar(10)
			);`))
		require.NoError(t, err)
		require.Len(t, plan.Changes, 1)
		require.Len(t, plan.Changes[0].TableChanges, 1)
		change := plan.Changes[0].TableChanges[0]
		assert.Equal(t, engine.ExecutionModeBlocked, change.ExecutionMode)
		assert.Contains(t, change.DDL, "title")
		assert.Contains(t, change.ModeReason, "copy-and-swap")
		var dataType string
		require.NoError(t, db.QueryRowContext(ctx, `
			SELECT data_type FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = 'documents' AND column_name = 'title'
		`).Scan(&dataType))
		assert.Equal(t, "text", dataType)
	})
}

func testSupabaseNativeChange(t *testing.T, dsn string, db *sql.DB, desired, expectedDDL string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), supabaseOperationDeadline)
	defer cancel()
	seedSupabaseDocuments(t, ctx, db)
	eng := NewForTarget(0, 0, "postgres", &engine.Credentials{DSN: dsn})
	pulled, err := eng.PullSchema(ctx, supabasePullRequest())
	require.NoError(t, err)
	require.Contains(t, pulled.Namespaces, "public")
	assert.Contains(t, pulled.Namespaces["public"].Tables["documents"], "CREATE TABLE")

	req := supabasePlanRequest(dsn, desired)
	plan, err := eng.Plan(ctx, req)
	require.NoError(t, err)
	require.Len(t, plan.Changes, 1)
	require.Len(t, plan.Changes[0].TableChanges, 1)
	change := plan.Changes[0].TableChanges[0]
	require.Empty(t, change.ExecutionMode, change.ModeReason)
	assert.Contains(t, change.DDL, expectedDDL)
	apply := applyRequest(dsn, "documents", change.DDL)
	apply.Changes = plan.Changes
	result, err := eng.Apply(ctx, apply)
	require.NoError(t, err)
	require.True(t, result.Accepted)
	progress := awaitPostgresProgress(t, eng, "documents")
	require.Equal(t, engine.StateCompleted, progress.State, "%+v", progress)
	assert.Equal(t, 100, progress.Progress)
	plan, err = eng.Plan(ctx, req)
	require.NoError(t, err)
	assert.True(t, plan.NoChanges, "applying the plan must converge to the declared schema")
	var rows int
	var granted bool
	require.NoError(t, db.QueryRowContext(ctx, "SELECT count(*) FROM public.documents").Scan(&rows))
	assert.Equal(t, 2, rows)
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT has_table_privilege('authenticated', 'public.documents', 'SELECT')").Scan(&granted))
	assert.True(t, granted, "native DDL must retain the application's table grant")
}

func seedSupabaseDocuments(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	_, err := db.ExecContext(ctx, `
		CREATE TABLE public.documents (
			id bigint PRIMARY KEY,
			title text
		);
		INSERT INTO public.documents VALUES (1, 'First document'), (2, 'Second document');
		GRANT SELECT ON public.documents TO authenticated;
	`)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), supabaseOperationDeadline)
		defer cancel()
		_, err := db.ExecContext(ctx, "DROP TABLE public.documents")
		require.NoError(t, err)
	})
}

func supabasePlanRequest(dsn, desired string) *engine.PlanRequest {
	return &engine.PlanRequest{
		Database:    "postgres",
		Credentials: &engine.Credentials{DSN: dsn},
		SchemaFiles: schema.SchemaFiles{"public": {Files: map[string]string{"documents.sql": desired}}},
	}
}

func supabasePullRequest() *ternv1.PullSchemaRequest {
	return &ternv1.PullSchemaRequest{Database: "postgres", Type: "postgres", Environment: "test", Namespace: "public"}
}

func startSupabasePostgres(t *testing.T) (string, *sql.DB) {
	t.Helper()
	// Image downloads use the test deadline; only readiness has a 30-second
	// startup budget, so a cold Docker cache does not consume that budget.
	container, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        supabasePostgresImage,
			Env:          map[string]string{"POSTGRES_PASSWORD": "schemabot_test_only", "POSTGRES_DB": "postgres"},
			ExposedPorts: []string{"5432/tcp"},
			Cmd:          []string{"postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"},
			// The entrypoint runs a temporary initialization server, which
			// listens only on the Unix socket and announces readiness once,
			// then stops it and starts the final server, which announces
			// readiness again on TCP. Docker's port proxy accepts a TCP
			// handshake before anything listens inside the container, so a
			// port wait can pass during the restart and the first connection
			// is reset. Wait for the final server's own readiness line.
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(supabaseOperationDeadline),
		},
		Started: true,
	})
	if container != nil {
		t.Cleanup(func() { require.NoError(t, testcontainers.TerminateContainer(container)) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), supabaseOperationDeadline)
	defer cancel()
	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "5432/tcp")
	require.NoError(t, err)
	dsn := (&url.URL{Scheme: "postgres", User: url.UserPassword("postgres", "schemabot_test_only"),
		Host: net.JoinHostPort(host, port.Port()), Path: "/postgres", RawQuery: "sslmode=disable"}).String()
	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(db) })
	require.NoError(t, db.PingContext(ctx))
	var superuser bool
	require.NoError(t, db.QueryRowContext(ctx, "SELECT rolsuper FROM pg_roles WHERE rolname = current_user").Scan(&superuser))
	require.False(t, superuser, "the fixture must exercise Supabase's restricted postgres role")
	return dsn, db
}
