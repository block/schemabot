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

// recordedHistory serves fixed schema files last applied, counting the
// lookups an engine makes, or fails every lookup with err.
type recordedHistory struct {
	files   schema.SchemaFiles
	err     error
	lookups int
}

func (h *recordedHistory) LastApplied(_ context.Context, namespace string) (*schema.Namespace, error) {
	h.lookups++
	if h.err != nil {
		return nil, h.err
	}
	return h.files[namespace], nil
}

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
	planWith := func(t *testing.T, ctx context.Context, desired string, history engine.AppliedSchemaFiles) (*engine.PlanResult, error) {
		t.Helper()
		return eng.Plan(ctx, &engine.PlanRequest{
			Database: "rls_removed", Credentials: &engine.Credentials{DSN: dsn},
			SchemaFiles:        schema.SchemaFiles{"public": {Files: map[string]string{"h_plain.sql": desired}}},
			AppliedSchemaFiles: history,
		})
	}
	plan := func(t *testing.T, ctx context.Context, desired string, applied schema.SchemaFiles) (*engine.PlanResult, error) {
		t.Helper()
		var history engine.AppliedSchemaFiles
		if applied != nil {
			history = (&recordedHistory{files: applied}).LastApplied
		}
		return planWith(t, ctx, desired, history)
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
		assert.Contains(t, err.Error(), `add ALTER TABLE "h_plain" DISABLE ROW LEVEL SECURITY; to turn off row level security and drop policy "read all" on table "h_plain", or declare the row security the table keeps`)
		assert.Contains(t, err.Error(), `The schema file SchemaBot last applied declared this table's row security, and a file without row security clauses leaves it as it is`)
	})

	// Each form of row security declaration counts: ENABLE with FORCE, and
	// a DISABLE the table has since lost to a hand-enabled policy. The
	// refusal claims only that the file declared the table's row security,
	// which holds for both.
	t.Run("every row security declaration counts", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		reset(t, ctx)
		for name, file := range map[string]string{
			"ENABLE with FORCE": plainTable + ` ALTER TABLE h_plain ENABLE ROW LEVEL SECURITY; ALTER TABLE h_plain FORCE ROW LEVEL SECURITY;`,
			"DISABLE alone":     plainRLSDisabled,
		} {
			_, err := plan(t, ctx, plainTable, appliedWith(file))
			var refused *engine.PlanRefusedError
			require.True(t, errors.As(err, &refused), "%s: %v", name, err)
			assert.Contains(t, err.Error(), `on table "h_plain"`, name)
		}
	})

	t.Run("a file under a new name is matched by its table", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		reset(t, ctx)
		_, err := plan(t, ctx, plainTable, schema.SchemaFiles{"public": {Files: map[string]string{"plain.sql": plainWithPolicy}}})
		require.Error(t, err)
		assert.Contains(t, err.Error(), `on table "h_plain"`)
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

	// The history is looked up only for a table that still has row
	// security and whose file declares none, the one case the refusal can
	// hit, so every other plan reads none of it.
	t.Run("the history is read only when the refusal could apply", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		reset(t, ctx)
		declared := &recordedHistory{files: appliedWith(plainWithPolicy)}
		_, err := planWith(t, ctx, plainWithPolicy, declared.LastApplied)
		require.NoError(t, err)
		assert.Zero(t, declared.lookups, "a file that declares the table's row security needs no history")

		left := &recordedHistory{files: appliedWith(plainWithPolicy)}
		_, err = planWith(t, ctx, plainTable, left.LastApplied)
		require.Error(t, err)
		assert.Equal(t, 1, left.lookups)

		_, err = db.ExecContext(ctx, `DROP POLICY "read all" ON public.h_plain; ALTER TABLE public.h_plain DISABLE ROW LEVEL SECURITY;`)
		require.NoError(t, err)
		none := &recordedHistory{files: appliedWith(plainWithPolicy)}
		_, err = planWith(t, ctx, plainTable, none.LastApplied)
		require.NoError(t, err)
		assert.Zero(t, none.lookups, "a table with no row security left needs no history")
	})

	// History that cannot be read fails the plan as an error, never as a
	// refusal or as a file that was never applied.
	t.Run("unreadable history fails the plan", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		reset(t, ctx)
		_, err := planWith(t, ctx, plainTable, (&recordedHistory{err: errors.New("apply history unavailable")}).LastApplied)
		require.ErrorContains(t, err, "apply history unavailable")
		var refused *engine.PlanRefusedError
		assert.False(t, errors.As(err, &refused), "a storage failure is not a schema refusal: %v", err)
	})
}
