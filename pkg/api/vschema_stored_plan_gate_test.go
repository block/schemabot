package api

import (
	"log/slog"
	"os"
	"testing"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// vschemaGateTestPlan returns the shared stored-plan fixture with a VSchema
// change attached to its namespace: the desired-VSchema artifact plus the
// persisted change-metadata the apply-time gate reads.
func vschemaGateTestPlan(metadata map[string]string) *storage.Plan {
	plan := executeApplyTestPlan()
	nsData := plan.Namespaces["testdb"]
	nsData.Artifacts = map[string]string{storage.VSchemaArtifactName: `{"sharded": true}`}
	nsData.Metadata = metadata
	return plan
}

func newVSchemaGateTestService(plan *storage.Plan) (*Service, *capturingApplyStore, *capturingTaskStore) {
	applies := &capturingApplyStore{}
	tasks := &capturingTaskStore{}
	applies.taskStore = tasks
	svc := New(&mockStorageWithApplyStores{
		plans:     &staticPlanStore{plan: plan},
		applies:   applies,
		tasks:     tasks,
		locks:     &emptyLockStore{},
		applyLogs: &noopApplyLogStore{},
	}, testServerConfig(), map[string]tern.Client{
		"default/staging": &mockTernClient{},
	}, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))
	return svc, applies, tasks
}

// TestExecuteApplyRejectsStoredVSchemaDeletionWithoutOptIn proves a stored
// plan whose VSchema change removes a routing entry never queues an apply
// without explicit unsafe opt-in — the same bar destructive DDL meets.
func TestExecuteApplyRejectsStoredVSchemaDeletionWithoutOptIn(t *testing.T) {
	plan := vschemaGateTestPlan(map[string]string{
		storage.PlanMetadataVSchemaChanged:   "true",
		storage.PlanMetadataVSchemaDeletions: `[{"kind":"vindex","name":"email_idx","reason":"removing vindex email_idx changes query routing"}]`,
	})
	svc, applies, tasks := newVSchemaGateTestService(plan)

	resp, applyID, err := svc.ExecuteApply(t.Context(), ApplyRequest{
		PlanID:      "plan-1",
		Environment: "staging",
	})

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Zero(t, applyID)
	assert.Contains(t, err.Error(), "unsafe VSchema change")
	assert.Contains(t, err.Error(), `namespace "testdb"`)
	assert.Contains(t, err.Error(), "removing vindex email_idx changes query routing")
	assert.Contains(t, err.Error(), "allow_unsafe=true")
	assert.Nil(t, applies.apply)
	assert.Empty(t, tasks.tasks)
}

// TestExecuteApplyQueuesStoredVSchemaDeletionWithOptIn proves the unsafe
// opt-in unlocks a stored plan with a recorded VSchema deletion, and the
// opt-in is carried onto the queued apply.
func TestExecuteApplyQueuesStoredVSchemaDeletionWithOptIn(t *testing.T) {
	plan := vschemaGateTestPlan(map[string]string{
		storage.PlanMetadataVSchemaChanged:   "true",
		storage.PlanMetadataVSchemaDeletions: `[{"kind":"vindex","name":"email_idx","reason":"removing vindex email_idx changes query routing"}]`,
	})
	svc, applies, tasks := newVSchemaGateTestService(plan)

	resp, applyID, err := svc.ExecuteApply(t.Context(), ApplyRequest{
		PlanID:      "plan-1",
		Environment: "staging",
		Options:     map[string]string{"allow_unsafe": "true"},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Accepted)
	assert.Equal(t, int64(123), applyID)
	require.NotNil(t, applies.apply)
	require.Len(t, tasks.tasks, 1)
	assert.True(t, applies.apply.GetOptions().AllowUnsafe)
}

// TestExecuteApplyRejectsStoredVSchemaMutationWithoutOptIn proves a stored
// plan whose VSchema change mutates a vindex in place — same name, different
// routing behavior — never queues an apply without explicit unsafe opt-in.
func TestExecuteApplyRejectsStoredVSchemaMutationWithoutOptIn(t *testing.T) {
	plan := vschemaGateTestPlan(map[string]string{
		storage.PlanMetadataVSchemaChanged:   "true",
		storage.PlanMetadataVSchemaMutations: `[{"kind":"vindex_type","name":"user_idx","reason":"changing vindex user_idx type re-computes keyspace ids"}]`,
	})
	svc, applies, tasks := newVSchemaGateTestService(plan)

	resp, applyID, err := svc.ExecuteApply(t.Context(), ApplyRequest{
		PlanID:      "plan-1",
		Environment: "staging",
	})

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Zero(t, applyID)
	assert.Contains(t, err.Error(), "unsafe VSchema change")
	assert.Contains(t, err.Error(), "changing vindex user_idx type re-computes keyspace ids")
	assert.Contains(t, err.Error(), "allow_unsafe=true")
	assert.Nil(t, applies.apply)
	assert.Empty(t, tasks.tasks)
}

// TestExecuteApplyRejectsStoredVSchemaChangeWithoutMetadata proves the gate
// fails closed on a stored plan that changes its VSchema but carries no
// persisted change-metadata: with no deletion record to consult, the plan is
// rejected with guidance to re-plan rather than assumed additive.
func TestExecuteApplyRejectsStoredVSchemaChangeWithoutMetadata(t *testing.T) {
	plan := vschemaGateTestPlan(nil)
	svc, applies, tasks := newVSchemaGateTestService(plan)

	resp, applyID, err := svc.ExecuteApply(t.Context(), ApplyRequest{
		PlanID:      "plan-1",
		Environment: "staging",
	})

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Zero(t, applyID)
	assert.Contains(t, err.Error(), "unsafe VSchema change")
	assert.Contains(t, err.Error(), "re-plan")
	assert.Nil(t, applies.apply)
	assert.Empty(t, tasks.tasks)
}

// TestExecuteApplyQueuesAdditiveStoredVSchemaChange proves an additive VSchema
// change — the changed flag persisted, no deletions recorded — queues without
// unsafe opt-in, so the gate blocks only removals and ambiguity.
func TestExecuteApplyQueuesAdditiveStoredVSchemaChange(t *testing.T) {
	plan := vschemaGateTestPlan(map[string]string{
		storage.PlanMetadataVSchemaChanged: "true",
	})
	svc, applies, tasks := newVSchemaGateTestService(plan)

	resp, applyID, err := svc.ExecuteApply(t.Context(), ApplyRequest{
		PlanID:      "plan-1",
		Environment: "staging",
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Accepted)
	assert.Equal(t, int64(123), applyID)
	require.NotNil(t, applies.apply)
	require.Len(t, tasks.tasks, 1)
}
