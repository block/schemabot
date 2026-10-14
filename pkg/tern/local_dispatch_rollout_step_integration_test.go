//go:build integration

package tern

import (
	"database/sql"
	"log/slog"
	"os"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

// rolloutStepDispatchRequest is the dispatch the control plane sends for one
// table step of payments-001 in a rollout run table by table: the step's own
// table under the deployment's idempotency key, with the manifest naming both
// of payments-001's steps.
func rolloutStepDispatchRequest(planID, key, step, table string) *ternv1.ApplyRequest {
	req := memberTargetDispatchRequest(planID, key, "payments-001")
	req.GenerationOperationKeys = []string{"payments-001/step-1", "payments-001/step-2"}
	req.DdlChanges[0].TableName = table
	if step != "" {
		req.Options[dispatchRolloutStepOption] = step
	}
	return req
}

// payments-001's plan alters `users` and `accounts`, and the rollout runs them
// as two table steps. Each step's dispatch attaches its own operation to the
// deployment's one apply, keyed by its step behind the target and stamped with
// the step, carrying only its own table's task. A data plane that ignored the
// step would key the dispatch by the target alone, which the manifest does not
// name, so that dispatch is refused before any operation is attached rather
// than run both tables as one.
func TestLocalClient_Apply_RolloutStepsAttachOneOperationPerStep(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	_, dsn := setupMySQLContainer(t)
	setupStorageSchema(t, dsn)
	cleanupTestTables(t, dsn)
	cleanupTasks(t, dsn)
	ctx := t.Context()

	db, err := sql.Open("block-mysql", dsn)
	require.NoError(t, err)
	defer utils.CloseAndLog(db)
	require.NoError(t, db.PingContext(ctx))
	for _, ddl := range []string{"CREATE TABLE users (id INT PRIMARY KEY)", "CREATE TABLE accounts (id INT PRIMARY KEY)"} {
		_, err = db.ExecContext(ctx, ddl)
		require.NoError(t, err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	stor := createStorage(t, dsn)
	client, err := NewLocalClient(LocalConfig{Database: "testdb", Type: "mysql", TargetDSN: dsn}, stor, logger)
	require.NoError(t, err)
	t.Cleanup(func() { utils.CloseAndLog(client) })

	planResp, err := client.Plan(ctx, &ternv1.PlanRequest{
		Type:     "mysql",
		Database: "testdb",
		SchemaFiles: map[string]*ternv1.SchemaFiles{"testdb": {Files: buildSchemaWithAllTables(t, dsn, map[string]string{
			"users":    "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255))",
			"accounts": "CREATE TABLE accounts (id INT PRIMARY KEY, name VARCHAR(255))",
		})}},
	})
	require.NoError(t, err)
	planID := storeMemberTargetPlan(t, stor, planResp.PlanId, "payments-001")
	const key = "schemabot:v1:rollout-step-test"

	unstepped, err := client.Apply(ctx, rolloutStepDispatchRequest(planID, key, "", "users"))
	require.NoError(t, err)
	assert.False(t, unstepped.Accepted, "a dispatch keyed by the target alone is outside the stepped manifest")
	assert.Contains(t, unstepped.ErrorMessage, `does not include its own operation key "payments-001"`)

	first, err := client.Apply(ctx, rolloutStepDispatchRequest(planID, key, "1", "users"))
	require.NoError(t, err)
	require.True(t, first.Accepted, "the first step must be accepted: %s", first.ErrorMessage)
	assert.Equal(t, "payments-001/step-1", first.OperationKey)

	second, err := client.Apply(ctx, rolloutStepDispatchRequest(planID, key, "2", "accounts"))
	require.NoError(t, err)
	require.True(t, second.Accepted, "the second step must attach to the deployment's apply: %s", second.ErrorMessage)
	assert.Equal(t, first.ApplyId, second.ApplyId)
	assert.Equal(t, "payments-001/step-2", second.OperationKey)

	apply, err := stor.Applies().GetByApplyIdentifier(ctx, first.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)
	ops, err := stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, ops, 2, "the refused dispatch attached nothing")
	tasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	tablesByOperation := map[int64][]string{}
	for _, task := range tasks {
		require.NotNil(t, task.ApplyOperationID)
		tablesByOperation[*task.ApplyOperationID] = append(tablesByOperation[*task.ApplyOperationID], task.TableName)
	}
	byKey := map[string]*storage.ApplyOperation{}
	for _, op := range ops {
		byKey[op.OperationKey] = op
	}
	for key, want := range map[string]struct {
		step  int
		table string
	}{
		"payments-001/step-1": {1, "users"},
		"payments-001/step-2": {2, "accounts"},
	} {
		op := byKey[key]
		require.NotNil(t, op, "operation %s", key)
		assert.Equal(t, want.step, op.RolloutStep, "%s carries its step", key)
		assert.Equal(t, "payments-001", op.Target)
		assert.Equal(t, []string{want.table}, tablesByOperation[op.ID], "%s runs only its own table", key)
	}
}
