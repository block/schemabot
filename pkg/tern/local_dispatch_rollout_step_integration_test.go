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

	"github.com/block/schemabot/pkg/engine"
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

// payments-002 is a rollout member other than the primary, so the control
// plane planned it with the non-persisting diff and stored its plan only in
// its own storage: the data plane holds no plan for it. Its rollout runs
// table by table, and each step's dispatch carries payments-002's whole plan
// beside the step's one table. The first step builds payments-002's plan on
// the data plane from that whole plan, verified against payments-002's live
// schema, and the second step runs from the plan the first stored. Each step
// attaches its own operation carrying only its own table, and the stored plan
// holds both tables, so neither step is refused for want of a plan.
func TestLocalClient_Apply_MemberStepsBuildTheMembersPlanFromTheDispatch(t *testing.T) {
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

	schemaFiles := map[string]*ternv1.SchemaFiles{"testdb": {Files: buildSchemaWithAllTables(t, dsn, map[string]string{
		"users":    "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255))",
		"accounts": "CREATE TABLE accounts (id INT PRIMARY KEY, name VARCHAR(255))",
	})}}
	diff, err := client.PlanDiff(ctx, &ternv1.PlanRequest{Type: "mysql", Database: "testdb", Target: "payments-002", SchemaFiles: schemaFiles})
	require.NoError(t, err)
	var memberPlan []*ternv1.TableChange
	for _, change := range diff.Changes {
		for _, table := range change.TableChanges {
			memberPlan = append(memberPlan, &ternv1.TableChange{Namespace: change.Namespace, TableName: table.TableName, Ddl: table.Ddl, ChangeType: table.ChangeType})
		}
	}
	require.Len(t, memberPlan, 2, "payments-002's own plan alters both tables")

	planID := engine.NewPlanID()
	stored, err := stor.Plans().Get(ctx, planID)
	require.NoError(t, err)
	require.Nil(t, stored, "the data plane holds no plan for a member planned with the diff")

	const key = "schemabot:v1:member-step-test"
	stepDispatch := func(step, table string) *ternv1.ApplyRequest {
		req := memberTargetDispatchRequest(planID, key, "payments-002")
		req.GenerationOperationKeys = []string{"payments-002/step-1", "payments-002/step-2"}
		req.Options[dispatchRolloutStepOption] = step
		req.SchemaFiles = schemaFiles
		req.DdlChanges = nil
		for _, change := range memberPlan {
			if change.TableName == table {
				req.DdlChanges = append(req.DdlChanges, change)
			}
		}
		require.Len(t, req.DdlChanges, 1, "step %s names its one table", step)
		req.PlanDdlChanges = memberPlan
		return req
	}

	first, err := client.Apply(ctx, stepDispatch("1", "users"))
	require.NoError(t, err)
	require.True(t, first.Accepted, "the member's first step must be accepted: %s", first.ErrorMessage)
	assert.Equal(t, "payments-002/step-1", first.OperationKey)

	second, err := client.Apply(ctx, stepDispatch("2", "accounts"))
	require.NoError(t, err)
	require.True(t, second.Accepted, "the member's second step must run from the plan its first stored: %s", second.ErrorMessage)
	assert.Equal(t, first.ApplyId, second.ApplyId)
	assert.Equal(t, "payments-002/step-2", second.OperationKey)

	stored, err = stor.Plans().Get(ctx, planID)
	require.NoError(t, err)
	require.NotNil(t, stored, "the first step stores payments-002's plan on the data plane")
	assert.Equal(t, "payments-002", stored.Target)
	var storedTables []string
	for _, change := range stored.FlatDDLChanges() {
		storedTables = append(storedTables, change.Table)
	}
	assert.ElementsMatch(t, []string{"users", "accounts"}, storedTables, "the stored plan is payments-002's whole plan, not its first step's table")

	apply, err := stor.Applies().GetByApplyIdentifier(ctx, first.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)
	ops, err := stor.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	require.Len(t, ops, 2)
	tasks, err := stor.Tasks().GetByApplyID(ctx, apply.ID)
	require.NoError(t, err)
	tablesByOperation := map[int64][]string{}
	for _, task := range tasks {
		require.NotNil(t, task.ApplyOperationID)
		tablesByOperation[*task.ApplyOperationID] = append(tablesByOperation[*task.ApplyOperationID], task.TableName)
	}
	for _, op := range ops {
		want := map[string]string{"payments-002/step-1": "users", "payments-002/step-2": "accounts"}[op.OperationKey]
		require.NotEmpty(t, want, "unexpected operation %s", op.OperationKey)
		assert.Equal(t, "payments-002", op.Target)
		assert.Equal(t, []string{want}, tablesByOperation[op.ID], "%s runs only its own table", op.OperationKey)
	}
}
