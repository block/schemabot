//go:build integration

package tern

import (
	"log/slog"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/testutil"
)

// Exercise stored review, unsafe consent, replan, task creation and the real
// PostgreSQL engine together. The replacement must remain exactly one task.
func TestLocalClientAtomicRowSecurityPlanAndApply(t *testing.T) {
	_, storageDSN := setupMySQLContainer(t)
	setupStorageSchema(t, storageDSN)
	cleanupTasks(t, storageDSN)
	dsn, db := testutil.StartPostgres(t, "atomic_rls")
	_, err := db.ExecContext(t.Context(), `
  CREATE TABLE public.documents (id bigint PRIMARY KEY);
  ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY;
  CREATE POLICY readers ON public.documents FOR SELECT USING (id = 1);
 `)
	require.NoError(t, err)
	stor := createStorage(t, storageDSN)
	defer utils.CloseAndLog(stor)
	client, err := NewLocalClient(LocalConfig{Database: "atomic_rls", Type: storage.DatabaseTypePostgres, TargetDSN: dsn, SchemaOverrides: map[string]string{"logical": "public"}}, stor, slog.Default())
	require.NoError(t, err)
	defer utils.CloseAndLog(client)
	files := map[string]*ternv1.SchemaFiles{"logical": {Files: map[string]string{"documents.sql": `
  CREATE TABLE documents (id bigint PRIMARY KEY);
  ALTER TABLE documents ENABLE ROW LEVEL SECURITY;
  CREATE POLICY readers ON documents FOR SELECT USING (id = 2);
 `}}}
	plan, err := client.Plan(t.Context(), &ternv1.PlanRequest{Database: "atomic_rls", Type: storage.DatabaseTypePostgres, SchemaFiles: files})
	require.NoError(t, err)
	require.Len(t, plan.Changes, 1)
	require.Len(t, plan.Changes[0].TableChanges, 1)
	assert.Equal(t, ternv1.ChangeType_CHANGE_TYPE_ALTER, plan.Changes[0].TableChanges[0].ChangeType)
	request := &ternv1.ApplyRequest{PlanId: plan.PlanId, Environment: localClientTestEnvironment, DdlChanges: plan.Changes[0].TableChanges}
	refused, err := client.Apply(t.Context(), request)
	require.NoError(t, err)
	require.False(t, refused.Accepted)
	require.Contains(t, refused.ErrorMessage, "allow_unsafe")
	request.Options = map[string]string{"allow_unsafe": "true"}
	// A newly added policy changes the reviewed SQL. The locked executor must refuse it.
	_, err = db.ExecContext(t.Context(), `
  CREATE POLICY writers ON public.documents FOR INSERT WITH CHECK (id = 1);
 `)
	require.NoError(t, err)
	drifted, err := client.Apply(t.Context(), request)
	require.NoError(t, err)
	require.True(t, drifted.Accepted, drifted.ErrorMessage)
	driveQueuedApply(t, stor, client, drifted.ApplyId)
	failed, err := stor.Applies().GetByApplyIdentifier(t.Context(), drifted.ApplyId)
	require.NoError(t, err)
	require.Equal(t, state.Apply.Failed, failed.State, failed.ErrorMessage)
	var unchanged string
	require.NoError(t, db.QueryRowContext(t.Context(), `
  SELECT pg_get_expr(polqual, polrelid) FROM pg_policy
  WHERE polrelid='public.documents'::regclass AND polname='readers'
 `).Scan(&unchanged))
	require.Equal(t, "(id = 1)", unchanged)
	var policyCount int
	require.NoError(t, db.QueryRowContext(t.Context(), `
  SELECT count(*) FROM pg_policy WHERE polrelid='public.documents'::regclass
 `).Scan(&policyCount))
	require.Equal(t, 2, policyCount)
	_, err = db.ExecContext(t.Context(), `DROP POLICY writers ON public.documents;`)
	require.NoError(t, err)
	fresh, err := client.Plan(t.Context(), &ternv1.PlanRequest{Database: "atomic_rls", Type: storage.DatabaseTypePostgres, SchemaFiles: files})
	require.NoError(t, err)
	request.PlanId = fresh.PlanId
	request.DdlChanges = fresh.Changes[0].TableChanges
	result, err := client.Apply(t.Context(), request)
	require.NoError(t, err)
	require.True(t, result.Accepted, result.ErrorMessage)
	driveQueuedApply(t, stor, client, result.ApplyId)
	apply, err := stor.Applies().GetByApplyIdentifier(t.Context(), result.ApplyId)
	require.NoError(t, err)
	tasks, err := stor.Tasks().GetByApplyID(t.Context(), apply.ID)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, state.Task.Completed, tasks[0].State, tasks[0].ErrorMessage)
	assert.Equal(t, "alter", tasks[0].DDLAction)
	assert.Equal(t, "logical", tasks[0].Namespace)
	var predicate string
	require.NoError(t, db.QueryRowContext(t.Context(), `
  SELECT pg_get_expr(polqual, polrelid) FROM pg_policy
  WHERE polrelid='public.documents'::regclass AND polname='readers'
 `).Scan(&predicate))
	assert.Equal(t, "(id = 2)", predicate)
	again, err := client.Plan(t.Context(), &ternv1.PlanRequest{Database: "atomic_rls", Type: storage.DatabaseTypePostgres, SchemaFiles: files})
	require.NoError(t, err)
	assert.Empty(t, again.Changes)
}
