//go:build integration

package postgres

import (
	"context"
	"net/url"
	"testing"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A held reader must not block a no-op plan, even for a planner that cannot
// alter the table. A changed definition still needs locked executor admission.
func TestEngineUnchangedRowSecurityNeedsNeitherExclusiveLockNorOwner(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "rls_noop")
	_, err := db.ExecContext(t.Context(), `
 CREATE TABLE public.documents (id bigint PRIMARY KEY);
 ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;
 CREATE POLICY readers ON public.documents FOR SELECT USING (id = 1);
 CREATE ROLE plan_reader LOGIN PASSWORD 'plan_reader';
 GRANT CONNECT, CREATE ON DATABASE rls_noop TO plan_reader;
 GRANT USAGE ON SCHEMA public TO plan_reader;
 `)
	require.NoError(t, err)
	limited, err := url.Parse(dsn)
	require.NoError(t, err)
	limited.User = url.UserPassword("plan_reader", "plan_reader")
	reader, err := db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Rollback()) }()
	_, err = reader.ExecContext(t.Context(), `LOCK TABLE public.documents IN ACCESS SHARE MODE`)
	require.NoError(t, err)
	const definition = `
 CREATE TABLE documents (id bigint PRIMARY KEY);
 ALTER TABLE documents ENABLE ROW LEVEL SECURITY;
 CREATE POLICY readers ON documents FOR SELECT USING (id = 1);
 `
	for name, conn := range map[string]string{"owner": dsn, "non-owner": limited.String()} {
		t.Run(name, func(t *testing.T) {
			// The reader stays open until planning returns. An exclusive lock
			// would time out and fail this plan, regardless of machine speed.
			ctx, cancel := context.WithTimeout(t.Context(), postgresApplyDeadline)
			defer cancel()
			result, err := New().Plan(ctx, &engine.PlanRequest{
				Database: "rls_noop", Credentials: &engine.Credentials{DSN: conn},
				SchemaFiles: schema.SchemaFiles{"public": {Files: map[string]string{"documents.sql": definition}}},
			})
			require.NoError(t, err)
			assert.True(t, result.NoChanges)
		})
	}
	t.Run("table-only syntax error names schema, not RLS", func(t *testing.T) {
		_, err := New().Plan(t.Context(), &engine.PlanRequest{
			Database: "rls_noop", Credentials: &engine.Credentials{DSN: dsn},
			SchemaFiles: schema.SchemaFiles{"public": {Files: map[string]string{"users.sql": "CREATE TABLE users ("}}},
		})
		require.ErrorContains(t, err, `plan PostgreSQL schema in "public"/"users.sql"`)
		assert.NotContains(t, err.Error(), "plan PostgreSQL row security")
	})
}
