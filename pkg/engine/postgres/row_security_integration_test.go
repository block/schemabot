//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/block/pg-sprite/pkg/diffplan"
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
// converges without executable changes, while altered policies remain refused.
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

	t.Run("changed policy refuses planning", func(t *testing.T) {
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
		require.Error(t, err)
		assert.Nil(t, result)
		var review *diffplan.RowSecurityReviewRequired
		require.ErrorAs(t, err, &review)
		require.Len(t, review.Review.Changes, 1)
		assert.Equal(t, schemadiff.SecurityPolicyChanged, review.Review.Changes[0].Kind)
		assert.Equal(t, "readers", review.Review.Changes[0].Policy)
	})

	t.Run("direct RLS apply is refused", func(t *testing.T) {
		result, err := eng.Apply(ctx, applyRequest(dsn, "documents",
			"ALTER TABLE public.documents DISABLE ROW LEVEL SECURITY"))
		require.ErrorContains(t, err, "require an atomic apply path")
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

// Omitting all security clauses must refuse even if the table shape is unchanged.
func TestEngineRowSecurityDeclarationRemoved(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "rls_removed")
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
			eng := NewForTarget(0, 0, "rls_removed", &engine.Credentials{DSN: dsn})
			result, err := eng.Plan(ctx, &engine.PlanRequest{Database: "rls_removed", Credentials: &engine.Credentials{DSN: dsn},
				SchemaFiles: schema.SchemaFiles{"public": {Files: map[string]string{"documents.sql": `CREATE TABLE documents (id bigint PRIMARY KEY);`}}}})
			require.ErrorIs(t, err, schemadiff.ErrUnsupportedChange)
			require.ErrorContains(t, err, "missing from its declaration")
			assert.Nil(t, result)
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
	_, _, err = planPostgresDefinition(ctx, pool, "public", `
  CREATE TABLE documents (id bigint PRIMARY KEY);
  ALTER TABLE documents ENABLE ROW LEVEL SECURITY;
 `)
	require.ErrorIs(t, err, context.Canceled)
	assert.NotContains(t, err.Error(), "does not yet execute")
	assert.NotErrorIs(t, err, schemadiff.ErrUnsupportedChange)
}
