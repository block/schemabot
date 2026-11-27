//go:build integration

package postgres

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/testutil"
)

// brokenForeignKeyFiles adds orders.user_id REFERENCES users (id).
var brokenForeignKeyFiles = map[string]string{
	"users.sql":  "CREATE TABLE users (id bigint PRIMARY KEY)",
	"orders.sql": "CREATE TABLE orders (id bigint PRIMARY KEY, user_id bigint NOT NULL REFERENCES users (id))",
}

// failForeignKeyOverAnOrphanRow plans orders_user_id_fkey over a table where
// one row references a missing user, applies the plan, and checks it stops at
// the key's validation with the key in place, unvalidated.
func failForeignKeyOverAnOrphanRow(t *testing.T, database string) (*sql.DB, string, *Engine) {
	t.Helper()
	dsn, db := testutil.StartPostgres(t, database)
	_, err := db.ExecContext(t.Context(), `
		CREATE TABLE public.users (id bigint PRIMARY KEY);
		CREATE TABLE public.orders (id bigint PRIMARY KEY, user_id bigint NOT NULL);
		INSERT INTO public.users VALUES (1);
		INSERT INTO public.orders VALUES (10, 1), (11, 99)`)
	require.NoError(t, err)
	eng := New()

	outcomes := applyPlannedSteps(t, eng, dsn, planForeignKeyFiles(t, eng, database, dsn, brokenForeignKeyFiles))
	require.Len(t, outcomes, 2, "the add completes and the validation fails")
	require.Equal(t, engine.StateCompleted, outcomes[0].State, outcomes[0].ErrorMessage)
	require.Equal(t, engine.StateFailed, outcomes[1].State)
	assert.Equal(t, `existing rows in "orders" break foreign key "orders_user_id_fkey"; fix those rows, then apply again`, outcomes[1].ErrorMessage)
	require.False(t, foreignKeyValidated(t, db, "orders_user_id_fkey"), "the key is in place, unvalidated")
	return db, dsn, eng
}

// An owner adds a foreign key that an existing row breaks, and the apply ends
// at the key's validation, or is cancelled there. The key, added NOT VALID,
// would go on rejecting the application's writes that break it for a change
// nobody is driving. Either release drops the key when the apply added it, so
// those writes land again and the next plan adds the key again. It leaves a
// key the plan validates but the apply did not add, naming the statement that
// drops it, and it does not look at a key the plan never validates, which the
// desired schema leaves unvalidated.
func TestEngineReleaseDropsTheUnvalidatedForeignKeyTheApplyAdded(t *testing.T) {
	const key = "orders_user_id_fkey"
	for _, tc := range []struct {
		name    string
		release func(*Engine, context.Context, *engine.ReleaseArtifactsRequest) (*engine.ReleaseArtifactsResult, error)
	}{
		{name: "cancelled", release: (*Engine).ReleaseCancelledArtifacts},
		{name: "failed", release: (*Engine).ReleaseFailedArtifacts},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, dsn, eng := failForeignKeyOverAnOrphanRow(t, "release_foreign_key_"+tc.name+"_test")
			req := &engine.ReleaseArtifactsRequest{
				Database:         "public",
				Tables:           []string{"orders"},
				Credentials:      &engine.Credentials{DSN: dsn},
				AddedConstraints: map[string][]string{"orders": {key}},
			}

			result, err := tc.release(eng, t.Context(), req)
			require.NoError(t, err)
			assert.Empty(t, result.RemovedConstraints, "the plan does not validate the key, so the release does not look at it")
			assert.Empty(t, result.PreexistingConstraints)
			assert.Equal(t, 1, constraintCount(t, db, "orders", key))

			req.AddedConstraints = nil
			req.ValidatedConstraints = map[string][]string{"orders": {key}}
			result, err = tc.release(eng, t.Context(), req)
			require.NoError(t, err)
			assert.Empty(t, result.RemovedConstraints, "the apply did not add the key, so the release keeps it")
			require.Len(t, result.PreexistingConstraints, 1)
			assert.Equal(t, `ALTER TABLE "public"."orders" DROP CONSTRAINT "`+key+`"`, result.PreexistingConstraints[0].Removal)
			assert.Equal(t, "rejects writes that break FOREIGN KEY (user_id) REFERENCES users(id)", result.PreexistingConstraints[0].Effect)
			assert.Equal(t, 1, constraintCount(t, db, "orders", key))

			req.AddedConstraints = map[string][]string{"orders": {key}}
			result, err = tc.release(eng, t.Context(), req)
			require.NoError(t, err)
			require.Len(t, result.RemovedConstraints, 1)
			assert.Equal(t, key, result.RemovedConstraints[0].Name)
			assert.Equal(t, "public.orders", result.RemovedConstraints[0].Table)
			assert.Empty(t, result.PreexistingConstraints)
			assert.Zero(t, constraintCount(t, db, "orders", key), "the release drops the key the apply added")
			_, err = db.ExecContext(t.Context(), `INSERT INTO public.orders VALUES (12, 98)`)
			require.NoError(t, err, "a write the key would reject lands again, as it did before the apply")

			replan := planForeignKeyFiles(t, eng, "release_foreign_key_"+tc.name+"_test", dsn, brokenForeignKeyFiles)
			assert.Equal(t, []string{
				"ALTER TABLE public.orders ADD CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users (id) NOT VALID",
				`ALTER TABLE "public"."orders" VALIDATE CONSTRAINT "orders_user_id_fkey"`,
			}, executableDDL(t, replan, "orders"), "the next plan adds the key again")
		})
	}
}
