//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/block/pg-sprite/pkg/executor"
	"github.com/block/pg-sprite/pkg/schemadiff"
	"github.com/block/pg-sprite/pkg/statement"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/lint"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

// Pull keeps settings, policies, and policy comments. The exported definition
// converges without executable changes; altered policies form one atomic operation.
func TestEngineRowSecurityRoundTrip(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "rls_roundtrip")
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	_, err := db.ExecContext(ctx, `
		CREATE TABLE public.documents (
			id bigint PRIMARY KEY,
			owner_id bigint NOT NULL
		);
		ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;
		ALTER TABLE public.documents FORCE ROW LEVEL SECURITY;
		CREATE POLICY readers ON public.documents FOR SELECT USING (owner_id = 1);
		COMMENT ON POLICY readers ON public.documents IS 'Read your documents';
	`)
	require.NoError(t, err)
	eng := NewForTarget(0, 0, "rls_roundtrip", &engine.Credentials{DSN: dsn})
	pulled, err := eng.PullSchema(ctx, &ternv1.PullSchemaRequest{Namespace: "public"})
	require.NoError(t, err)
	require.Contains(t, pulled.Namespaces, "public")
	definition := pulled.Namespaces["public"].Tables["documents"]
	assert.Contains(t, definition, "ENABLE ROW LEVEL SECURITY")
	assert.Contains(t, definition, "FORCE ROW LEVEL SECURITY")
	assert.Contains(t, definition, "CREATE POLICY")
	assert.Contains(t, definition, "Read your documents")
	_, err = statement.ParseDesiredWithRowSecurity(definition)
	require.NoError(t, err)
	findings, err := lint.New().LintPostgresSchema(map[string]string{"documents": definition})
	require.NoError(t, err)
	assert.Empty(t, findings)
	req := &engine.PlanRequest{Database: "rls_roundtrip", Credentials: &engine.Credentials{DSN: dsn},
		SchemaFiles: schema.SchemaFiles{"public": {Files: map[string]string{"documents.sql": definition}}}}
	plan, err := eng.Plan(ctx, req)
	require.NoError(t, err)
	assert.True(t, plan.NoChanges)

	t.Run("changed policy produces one unsafe operation", func(t *testing.T) {
		req.SchemaFiles["public"].Files["documents.sql"] = `
			CREATE TABLE documents (
				id bigint PRIMARY KEY,
				owner_id bigint NOT NULL
			);
			ALTER TABLE documents ENABLE ROW LEVEL SECURITY;
			ALTER TABLE documents FORCE ROW LEVEL SECURITY;
			CREATE POLICY readers ON documents FOR SELECT USING (true);
		`
		result, err := eng.Plan(ctx, req)
		require.NoError(t, err)
		require.Len(t, result.Changes, 1)
		require.Len(t, result.Changes[0].TableChanges, 1)
		assert.True(t, result.Changes[0].TableChanges[0].IsUnsafe)

	})

	t.Run("direct RLS apply is refused", func(t *testing.T) {
		result, err := eng.Apply(ctx, applyRequest(dsn, "documents",
			"ALTER TABLE public.documents DISABLE ROW LEVEL SECURITY"))
		require.ErrorContains(t, err, "requires the reviewed desired schema files")
		assert.Nil(t, result)
		var enabled, forced bool
		require.NoError(t, db.QueryRowContext(ctx, `
			SELECT relrowsecurity, relforcerowsecurity FROM pg_class
			WHERE oid = 'public.documents'::regclass
		`).Scan(&enabled, &forced))
		assert.True(t, enabled)
		assert.True(t, forced)
	})
}

// Policies still matter while RLS is disabled: pull must retain them rather
// than silently exporting a table with no access definitions.
func TestEnginePullDisabledRowSecurityPolicies(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "rls_disabled")
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	_, err := db.ExecContext(ctx, `
		CREATE TABLE public.documents (id bigint PRIMARY KEY);
		CREATE POLICY readers ON public.documents FOR SELECT USING (id = 1);
	`)
	require.NoError(t, err)
	eng := NewForTarget(0, 0, "rls_disabled", &engine.Credentials{DSN: dsn})
	pulled, err := eng.PullSchema(ctx, &ternv1.PullSchemaRequest{Namespace: "public"})
	require.NoError(t, err)
	definition := pulled.Namespaces["public"].Tables["documents"]
	assert.Contains(t, definition, "DISABLE ROW LEVEL SECURITY")
	assert.Contains(t, definition, "CREATE POLICY")
	plan, err := eng.Plan(ctx, &engine.PlanRequest{Database: "rls_disabled", Credentials: &engine.Credentials{DSN: dsn},
		SchemaFiles: schema.SchemaFiles{"public": {Files: map[string]string{"documents.sql": definition}}}})
	require.NoError(t, err)
	assert.True(t, plan.NoChanges)
}

// Legacy files and rollback originals manage table shape without claiming
// ownership of access rules. Exercise every live security state.
func TestEngineTableOnlyDefinitionPreservesRowSecurity(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "rls_legacy")
	for _, tc := range []struct{ name, security string }{
		{"enabled", `ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;`},
		{"forced", `ALTER TABLE public.documents FORCE ROW LEVEL SECURITY;`},
		{"policy while disabled", `CREATE POLICY readers ON public.documents FOR SELECT USING (id = 1);`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			_, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS public.documents; CREATE TABLE public.documents (id bigint PRIMARY KEY);`)
			require.NoError(t, err)
			_, err = db.ExecContext(ctx, tc.security)
			require.NoError(t, err)
			securityState := func() string {
				var state string
				require.NoError(t, db.QueryRowContext(ctx, `
     SELECT json_build_array(c.relrowsecurity, c.relforcerowsecurity,
      (SELECT json_agg(row_to_json(p) ORDER BY p.polname) FROM pg_policy p WHERE p.polrelid = c.oid))::text
     FROM pg_class c WHERE c.oid = 'public.documents'::regclass
    `).Scan(&state))
				return state
			}
			before := securityState()
			eng := NewForTarget(0, 0, "rls_legacy", &engine.Credentials{DSN: dsn})
			req := &engine.PlanRequest{Database: "rls_legacy", Credentials: &engine.Credentials{DSN: dsn},
				SchemaFiles: schema.SchemaFiles{"public": {Files: map[string]string{"documents.sql": `CREATE TABLE documents (id bigint PRIMARY KEY);`}}}}
			result, err := eng.Plan(ctx, req)
			require.NoError(t, err)
			require.True(t, result.NoChanges)
			for _, desired := range []string{
				`CREATE TABLE documents (id bigint PRIMARY KEY, summary text);`,
				`CREATE TABLE documents (id bigint PRIMARY KEY, summary text); CREATE INDEX summary_idx ON documents (summary);`,
			} {
				req.SchemaFiles["public"].Files["documents.sql"] = desired
				result, err = eng.Plan(ctx, req)
				require.NoError(t, err)
				require.Len(t, result.Changes, 1)
				require.Len(t, result.Changes[0].TableChanges, 1)
				change := result.Changes[0].TableChanges[0]
				require.Empty(t, change.ExecutionMode, change.ModeReason)
				apply := applyRequest(dsn, "documents", change.DDL)
				apply.Changes = result.Changes
				applied, err := eng.Apply(ctx, apply)
				require.NoError(t, err)
				require.True(t, applied.Accepted)
				require.Equal(t, engine.StateCompleted, awaitPostgresProgress(t, eng, "documents").State)
				result, err = eng.Plan(ctx, req)
				require.NoError(t, err)
				require.True(t, result.NoChanges)
				assert.Equal(t, before, securityState())
			}
			// A pre-upgrade rollback original has no security clauses. Replanning
			// that shape must still work, without generating policy or RLS changes.
			req.SchemaFiles["public"].Files["documents.sql"] = `CREATE TABLE documents (id bigint PRIMARY KEY);`
			result, err = eng.Plan(ctx, req)
			require.NoError(t, err)
			assert.False(t, result.NoChanges)
			for _, ns := range result.Changes {
				for _, change := range ns.TableChanges {
					assert.NotContains(t, change.DDL, "POLICY")
					assert.NotContains(t, change.DDL, "ROW LEVEL SECURITY")
				}
			}
			assert.Equal(t, before, securityState())
		})
	}
}

func TestRowSecurityComparisonOperationalError(t *testing.T) {
	dsn, _ := testutil.StartPostgres(t, "rls_operational")
	pool, err := pgxpool.New(t.Context(), dsn)
	require.NoError(t, err)
	defer pool.Close()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, _, err = planRowSecurityOperation(ctx, pool, "public", `
  CREATE TABLE documents (id bigint PRIMARY KEY);
  ALTER TABLE documents ENABLE ROW LEVEL SECURITY;
 `)
	require.ErrorIs(t, err, context.Canceled)
	assert.NotContains(t, err.Error(), "does not yet execute")
	assert.NotErrorIs(t, err, schemadiff.ErrUnsupportedChange)
}

// Greenfield RLS must refuse before any ordinary CREATE TABLE can be planned.
func TestEngineRowSecurityMissingTableRefuses(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "rls_missing")
	ctx, cancel := context.WithTimeout(t.Context(), postgresApplyDeadline)
	defer cancel()
	result, err := New().Plan(ctx, &engine.PlanRequest{
		Database: "rls_missing", Credentials: &engine.Credentials{DSN: dsn},
		SchemaFiles: schema.SchemaFiles{"public": {Files: map[string]string{"documents.sql": `
   CREATE TABLE documents (
    id bigint PRIMARY KEY
   );
   ALTER TABLE documents ENABLE ROW LEVEL SECURITY;
   CREATE POLICY readers ON documents FOR SELECT USING (id = 1);
  `}}},
	})
	require.ErrorIs(t, err, executor.ErrTableNotFound)
	assert.Nil(t, result)
	var absent bool
	require.NoError(t, db.QueryRowContext(ctx, "SELECT to_regclass('public.documents') IS NULL").Scan(&absent))
	assert.True(t, absent)
}

// Unsupported relation dependencies must refuse the whole pull, not omit a policy.
func TestEnginePullRefusesPolicyRelationDependency(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "rls_dependency")
	ctx, cancel := context.WithTimeout(t.Context(), postgresApplyDeadline)
	defer cancel()
	_, err := db.ExecContext(ctx, `
  CREATE TABLE public.accounts (id bigint PRIMARY KEY);
  CREATE TABLE public.documents (id bigint PRIMARY KEY);
  ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;
  CREATE POLICY readers ON public.documents FOR SELECT
   USING (EXISTS (SELECT 1 FROM public.accounts WHERE accounts.id = documents.id));
 `)
	require.NoError(t, err)
	eng := NewForTarget(0, 0, "rls_dependency", &engine.Credentials{DSN: dsn})
	result, err := eng.PullSchema(ctx, &ternv1.PullSchemaRequest{Namespace: "public"})
	require.ErrorIs(t, err, statement.ErrPolicyRelationDependency)
	assert.Nil(t, result)
}
