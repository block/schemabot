//go:build integration

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/testutil"
)

const (
	plainTable       = `CREATE TABLE h_plain (id bigint PRIMARY KEY);`
	plainWithPolicy  = plainTable + ` ALTER TABLE h_plain ENABLE ROW LEVEL SECURITY; CREATE POLICY "read all" ON h_plain FOR SELECT USING (true);`
	plainRLSDisabled = plainTable + ` ALTER TABLE h_plain DISABLE ROW LEVEL SECURITY;`
)

// A pull request reverts the one that turned on row security for h_plain and
// added the "read all" policy: the file is back to the bare table, while the
// table still has both. SchemaBot applied those clauses, so the plan refuses
// rather than report no changes, and names how to remove the row security or
// keep it. A file that never declared row security, or whose table has none
// left, still plans as the table definition alone.
func TestEnginePlanRefusesRowSecurityTheFileNoLongerDeclares(t *testing.T) {
	dsn, db := testutil.StartPostgres(t, "rls_removed")
	eng := NewForTarget(0, 0, "rls_removed", &engine.Credentials{DSN: dsn})
	reset := func(t *testing.T, ctx context.Context) {
		t.Helper()
		_, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS public.h_plain; CREATE TABLE public.h_plain (id bigint PRIMARY KEY);
			ALTER TABLE public.h_plain ENABLE ROW LEVEL SECURITY;
			CREATE POLICY "read all" ON public.h_plain FOR SELECT USING (true);`)
		require.NoError(t, err)
	}
	plan := func(t *testing.T, ctx context.Context, desired string, applied schema.SchemaFiles) (*engine.PlanResult, error) {
		t.Helper()
		return eng.Plan(ctx, &engine.PlanRequest{
			Database: "rls_removed", Credentials: &engine.Credentials{DSN: dsn},
			SchemaFiles:        schema.SchemaFiles{"public": {Files: map[string]string{"h_plain.sql": desired}}},
			AppliedSchemaFiles: applied,
		})
	}
	appliedWith := func(file string) schema.SchemaFiles {
		return schema.SchemaFiles{"public": {Files: map[string]string{"h_plain.sql": file}}}
	}

	t.Run("the revert is refused", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		reset(t, ctx)
		_, err := plan(t, ctx, plainTable, appliedWith(plainWithPolicy))
		require.Error(t, err)
		var refused *engine.PlanRefusedError
		require.True(t, errors.As(err, &refused), "a removed declaration is a refusal the file's author fixes: %v", err)
		assert.Contains(t, err.Error(), `"public"/"h_plain.sql"`)
		assert.Contains(t, err.Error(), `add ALTER TABLE "h_plain" DISABLE ROW LEVEL SECURITY; to remove row level security enabled and policy "read all" from table "h_plain", or put the file's row security clauses back to keep them`)
		assert.Contains(t, err.Error(), `SchemaBot applied them, and a file without row security clauses leaves them in place`)
	})

	t.Run("a file under a new name is matched by its table", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		reset(t, ctx)
		_, err := plan(t, ctx, plainTable, schema.SchemaFiles{"public": {Files: map[string]string{"plain.sql": plainWithPolicy}}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), `from table "h_plain"`)
	})

	t.Run("the removal it names plans", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		reset(t, ctx)
		result, err := plan(t, ctx, plainRLSDisabled, appliedWith(plainWithPolicy))
		require.NoError(t, err)
		require.False(t, result.NoChanges)
		require.Len(t, result.Changes, 1)
		require.Len(t, result.Changes[0].TableChanges, 1)
		ddl := result.Changes[0].TableChanges[0].DDL
		assert.Contains(t, ddl, `DROP POLICY "read all"`)
		assert.Contains(t, ddl, "DISABLE ROW LEVEL SECURITY")
	})

	t.Run("row security SchemaBot never declared is left alone", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		reset(t, ctx)
		for name, applied := range map[string]schema.SchemaFiles{
			"no apply has completed":    nil,
			"the applied file had none": appliedWith(plainTable),
		} {
			result, err := plan(t, ctx, plainTable, applied)
			require.NoError(t, err, name)
			assert.True(t, result.NoChanges, name)
		}
	})

	t.Run("a table with no row security left plans as before", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		reset(t, ctx)
		_, err := db.ExecContext(ctx, `DROP POLICY "read all" ON public.h_plain; ALTER TABLE public.h_plain DISABLE ROW LEVEL SECURITY;`)
		require.NoError(t, err)
		for name, applied := range map[string]schema.SchemaFiles{
			"after the disable was applied": appliedWith(plainRLSDisabled),
			"after it was removed by hand":  appliedWith(plainWithPolicy),
		} {
			result, err := plan(t, ctx, plainTable, applied)
			require.NoError(t, err, name)
			assert.True(t, result.NoChanges, name)
		}
	})
}
