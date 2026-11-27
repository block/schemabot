//go:build integration

package postgres

import (
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

func TestEngineAtomicRowSecurityPlanAndApply(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "rls_apply")
	_, err := db.ExecContext(t.Context(), `
  CREATE TABLE public.documents (
   id bigint PRIMARY KEY,
   owner_name text NOT NULL,
   published boolean NOT NULL
  );
  CREATE ROLE reader_a;
  CREATE ROLE reader_b;
  GRANT SELECT ON public.documents TO reader_a, reader_b;
  INSERT INTO public.documents VALUES (1, 'reader_a', true), (2, 'reader_a', false), (3, 'reader_b', true);
  ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;
  CREATE POLICY readers ON public.documents FOR SELECT USING (owner_name = current_user);
 `)
	require.NoError(t, err)
	files := schema.SchemaFiles{"public": {Files: map[string]string{"documents.sql": `
  CREATE TABLE documents (
   id bigint PRIMARY KEY,
   owner_name text NOT NULL,
   published boolean NOT NULL
  );
  ALTER TABLE documents ENABLE ROW LEVEL SECURITY;
  CREATE POLICY readers ON documents FOR SELECT USING (owner_name = current_user AND published);
 `}}}
	eng := New()
	plan, err := eng.Plan(t.Context(), &engine.PlanRequest{Database: "rls_apply", Credentials: &engine.Credentials{DSN: dsn}, SchemaFiles: files})
	require.NoError(t, err)
	require.Len(t, plan.Changes, 1)
	require.Len(t, plan.Changes[0].TableChanges, 1)
	change := plan.Changes[0].TableChanges[0]
	assert.Equal(t, ddl.StatementAlterTable, change.Operation)
	assert.True(t, change.IsUnsafe, "access changes require the existing unsafe consent gate")
	req := applyRequest(dsn, "documents", change.DDL)
	req.SchemaFiles = files
	result, err := eng.Apply(t.Context(), req)
	require.NoError(t, err)
	require.True(t, result.Accepted)
	progress := awaitPostgresProgress(t, eng, "documents")
	require.Equal(t, engine.StateCompleted, progress.State, progress.ErrorMessage)
	for _, role := range []string{"reader_a", "reader_b"} {
		tx, err := db.BeginTx(t.Context(), nil)
		require.NoError(t, err)
		// Fixture role names are constant; this query is not built from user input.
		_, err = tx.ExecContext(t.Context(), "SET LOCAL ROLE "+pgx.Identifier{role}.Sanitize())
		require.NoError(t, err)
		var visible int
		require.NoError(t, tx.QueryRowContext(t.Context(), "SELECT count(*) FROM public.documents").Scan(&visible))
		assert.Equal(t, 1, visible)
		require.NoError(t, tx.Rollback())
	}
	again, err := eng.Plan(t.Context(), &engine.PlanRequest{Database: "rls_apply", Credentials: &engine.Credentials{DSN: dsn}, SchemaFiles: files})
	require.NoError(t, err)
	assert.True(t, again.NoChanges)
}

func TestEngineAtomicRowSecurityRefusesLatePolicyAddition(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "rls_late")
	_, err := db.ExecContext(t.Context(), `
  CREATE TABLE public.documents (id bigint PRIMARY KEY);
  ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;
  CREATE POLICY readers ON public.documents FOR SELECT USING (id = 1);
 `)
	require.NoError(t, err)
	files := schema.SchemaFiles{"public": {Files: map[string]string{"documents.sql": `
  CREATE TABLE documents (id bigint PRIMARY KEY);
  ALTER TABLE documents DISABLE ROW LEVEL SECURITY;
 `}}}
	eng := New()
	plan, err := eng.Plan(t.Context(), &engine.PlanRequest{Database: "rls_late", Credentials: &engine.Credentials{DSN: dsn}, SchemaFiles: files})
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), `CREATE POLICY writers ON public.documents FOR INSERT WITH CHECK (id = 1);`)
	require.NoError(t, err)
	req := applyRequest(dsn, "documents", plan.Changes[0].TableChanges[0].DDL)
	req.SchemaFiles = files
	_, err = eng.Apply(t.Context(), req)
	require.NoError(t, err)
	progress := awaitPostgresProgress(t, eng, "documents")
	assert.Equal(t, engine.StateFailed, progress.State)
	assert.False(t, progress.Retryable)
	var enabled bool
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT relrowsecurity FROM pg_class WHERE oid='public.documents'::regclass").Scan(&enabled))
	assert.True(t, enabled)
	var policies int
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT count(*) FROM pg_policy WHERE polrelid='public.documents'::regclass").Scan(&policies))
	assert.Equal(t, 2, policies)
}

func TestEnginePlanRefusesDuplicateRowSecurityDeclarations(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "rls_duplicates")
	_, err := db.ExecContext(t.Context(), `
  CREATE TABLE public.documents (id bigint PRIMARY KEY);
  ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;
  CREATE POLICY readers ON public.documents FOR SELECT USING (id = 1);
 `)
	require.NoError(t, err)
	const changed = `
  CREATE TABLE documents (id bigint PRIMARY KEY);
  ALTER TABLE documents ENABLE ROW LEVEL SECURITY;
  CREATE POLICY readers ON documents FOR SELECT USING (id = 2);
 `
	const unchanged = `
  CREATE TABLE documents (id bigint PRIMARY KEY);
  ALTER TABLE documents ENABLE ROW LEVEL SECURITY;
  CREATE POLICY readers ON documents FOR SELECT USING (id = 1);
 `
	const tableOnly = `CREATE TABLE documents (id bigint PRIMARY KEY);`
	for _, tt := range []struct{ name, first, second string }{
		{"two changed RLS files", changed, changed},
		{"RLS then table only", changed, tableOnly},
		{"table only then RLS", tableOnly, changed},
		{"two unchanged RLS files", unchanged, unchanged},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result, err := New().Plan(t.Context(), &engine.PlanRequest{
				Database: "rls_duplicates", Credentials: &engine.Credentials{DSN: dsn},
				SchemaFiles: schema.SchemaFiles{"public": {Files: map[string]string{
					"a.sql": tt.first, "b.sql": tt.second,
				}}},
			})
			require.ErrorContains(t, err, `table "documents" is declared by both schema files "a.sql" and "b.sql"`)
			require.Nil(t, result, "a duplicate declaration must never publish an unsafe or no-change plan")
		})
	}
	var predicate string
	require.NoError(t, db.QueryRowContext(t.Context(), `
  SELECT pg_get_expr(polqual, polrelid) FROM pg_policy
  WHERE polrelid='public.documents'::regclass AND polname='readers'
 `).Scan(&predicate))
	assert.Equal(t, "(id = 1)", predicate)
}

// A schema file that declares a new table with row security and a policy
// plans as one change: the CREATE TABLE and its security statements, which
// apply in one transaction, so the table never exists without its policy.
// The table is new and holds no rows, so the change is not unsafe. Rolling
// the plan back drops the table, which is unsafe.
func TestEngineCreateWithRowSecurityPlanAndApply(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "rls_create")
	files := schema.SchemaFiles{"public": {Files: map[string]string{"documents.sql": `
  CREATE TABLE documents (
   id bigint PRIMARY KEY
  );
  ALTER TABLE documents ENABLE ROW LEVEL SECURITY;
  CREATE POLICY readers ON documents FOR SELECT USING (id = 1);
 `}}}
	eng := New()
	plan, err := eng.Plan(t.Context(), &engine.PlanRequest{Database: "rls_create", Credentials: &engine.Credentials{DSN: dsn}, SchemaFiles: files})
	require.NoError(t, err)
	require.Len(t, plan.Changes, 1)
	require.Len(t, plan.Changes[0].TableChanges, 1)
	change := plan.Changes[0].TableChanges[0]
	assert.Equal(t, "documents", change.Table)
	assert.Equal(t, ddl.StatementCreateTable, change.Operation)
	assert.False(t, change.IsUnsafe, "a new table holds no rows to expose")
	assert.Contains(t, change.DDL, "CREATE TABLE")
	assert.Contains(t, change.DDL, "ENABLE ROW LEVEL SECURITY")
	assert.Contains(t, change.DDL, `CREATE POLICY "readers" ON "public"."documents"`)
	var absent bool
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT to_regclass('public.documents') IS NULL").Scan(&absent))
	assert.True(t, absent, "planning previews the create and rolls it back")

	req := applyRequest(dsn, "documents", change.DDL)
	req.SchemaFiles = files
	result, err := eng.Apply(t.Context(), req)
	require.NoError(t, err)
	require.True(t, result.Accepted)
	progress := awaitPostgresProgress(t, eng, "documents")
	require.Equal(t, engine.StateCompleted, progress.State, progress.ErrorMessage)
	var enabled bool
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT relrowsecurity FROM pg_class WHERE oid='public.documents'::regclass").Scan(&enabled))
	assert.True(t, enabled)
	var policy string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT polname FROM pg_policy WHERE polrelid='public.documents'::regclass").Scan(&policy))
	assert.Equal(t, "readers", policy)
	again, err := eng.Plan(t.Context(), &engine.PlanRequest{Database: "rls_create", Credentials: &engine.Credentials{DSN: dsn}, SchemaFiles: files})
	require.NoError(t, err)
	assert.True(t, again.NoChanges)

	// The namespace had no table with row security when the plan captured
	// its rollback originals, so the plan can be rolled back. The rollback
	// drops the new table and its policies with it, which never exposes a
	// row, and is unsafe, so it needs the operator's acknowledgement.
	require.True(t, plan.Changes[0].OriginalFilesCaptured)
	assert.Empty(t, plan.Changes[0].RollbackBlockers)
	assert.Empty(t, plan.Changes[0].OriginalFiles, "the namespace had no table before the plan")
	rollback, err := eng.Plan(t.Context(), &engine.PlanRequest{Database: "rls_create", Credentials: &engine.Credentials{DSN: dsn},
		SchemaFiles: schema.SchemaFiles{"public": {Files: plan.Changes[0].OriginalFiles}}})
	require.NoError(t, err)
	require.Len(t, rollback.Changes, 1)
	require.Len(t, rollback.Changes[0].TableChanges, 1)
	assert.Equal(t, "DROP TABLE public.documents", rollback.Changes[0].TableChanges[0].DDL)
	assert.True(t, rollback.Changes[0].TableChanges[0].IsUnsafe)
}

// An owner declares a table with row security in a namespace the target does
// not have yet. The plan shows the create blocked with the same cause a table
// without row security gets, naming the schema to create, instead of failing
// the plan.
func TestEngineCreateWithRowSecurityInAMissingSchemaIsBlocked(t *testing.T) {
	dsn, _ := testutil.StartPostgres(t, "rls_create_missing_schema")
	eng := New()
	plan := func(t *testing.T, file string) engine.TableChange {
		t.Helper()
		result, err := eng.Plan(t.Context(), &engine.PlanRequest{Database: "rls_create_missing_schema", Credentials: &engine.Credentials{DSN: dsn},
			SchemaFiles: schema.SchemaFiles{"rls1": {Files: map[string]string{"documents.sql": file}}}})
		require.NoError(t, err)
		require.Len(t, result.Changes, 1)
		require.Len(t, result.Changes[0].TableChanges, 1)
		return result.Changes[0].TableChanges[0]
	}

	withRowSecurity := plan(t, `
  CREATE TABLE documents (id bigint PRIMARY KEY);
  ALTER TABLE documents ENABLE ROW LEVEL SECURITY;
  CREATE POLICY readers ON documents FOR SELECT USING (id = 1);
 `)
	without := plan(t, `CREATE TABLE documents (id bigint PRIMARY KEY);`)

	assert.Equal(t, "documents", withRowSecurity.Table)
	assert.Equal(t, ddl.StatementCreateTable, withRowSecurity.Operation)
	assert.Equal(t, engine.ExecutionModeBlocked, withRowSecurity.ExecutionMode)
	assert.Equal(t, `the schema that would hold table "documents" does not exist on the target; create the schema first`, withRowSecurity.ModeReason)
	assert.Equal(t, without.ModeReason, withRowSecurity.ModeReason, "row security does not change the verdict on a missing schema")
	assert.Contains(t, withRowSecurity.DDL, "CREATE TABLE documents")
	assert.Contains(t, withRowSecurity.DDL, "CREATE POLICY readers ON documents")
}

// An apply runs the reviewed create only when the reviewed desired file still
// derives the same statements. A schema file edited after the review derives
// a different sequence, so the apply refuses and creates nothing.
func TestEngineCreateWithRowSecurityRefusesAnUnreviewedSequence(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "rls_create_changed")
	reviewed := schema.SchemaFiles{"public": {Files: map[string]string{"documents.sql": `
  CREATE TABLE documents (id bigint PRIMARY KEY);
  ALTER TABLE documents ENABLE ROW LEVEL SECURITY;
  CREATE POLICY readers ON documents FOR SELECT USING (id = 1);
 `}}}
	eng := New()
	plan, err := eng.Plan(t.Context(), &engine.PlanRequest{Database: "rls_create_changed", Credentials: &engine.Credentials{DSN: dsn}, SchemaFiles: reviewed})
	require.NoError(t, err)
	require.Len(t, plan.Changes, 1)
	change := plan.Changes[0].TableChanges[0]
	req := applyRequest(dsn, "documents", change.DDL)
	req.SchemaFiles = schema.SchemaFiles{"public": {Files: map[string]string{"documents.sql": `
  CREATE TABLE documents (id bigint PRIMARY KEY);
  ALTER TABLE documents ENABLE ROW LEVEL SECURITY;
  CREATE POLICY readers ON documents FOR SELECT USING (id = 2);
 `}}}
	_, err = eng.Apply(t.Context(), req)
	require.NoError(t, err)
	progress := awaitPostgresProgress(t, eng, "documents")
	assert.Equal(t, engine.StateFailed, progress.State)
	assert.False(t, progress.Retryable)
	var absent bool
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT to_regclass('public.documents') IS NULL").Scan(&absent))
	assert.True(t, absent)
}
