package api

import (
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// Live-remote progress renders the same operation string for a table change
// that the storage-served path persists as the task's DDL action, so a client
// polling an apply sees a stable change type across the two sources. Only
// CHANGE_TYPE_OTHER differs by design: live progress has no DDL parser to
// recover an operation from, so it renders the change type as empty.
func TestChangeTypeToStringMatchesStorageOperation(t *testing.T) {
	for _, ct := range []ternv1.ChangeType{
		ternv1.ChangeType_CHANGE_TYPE_CREATE,
		ternv1.ChangeType_CHANGE_TYPE_ALTER,
		ternv1.ChangeType_CHANGE_TYPE_DROP,
		ternv1.ChangeType_CHANGE_TYPE_CREATE_INDEX,
		ternv1.ChangeType_CHANGE_TYPE_DROP_INDEX,
		ternv1.ChangeType_CHANGE_TYPE_RENAME,
		ternv1.ChangeType_CHANGE_TYPE_TRUNCATE,
		ternv1.ChangeType_CHANGE_TYPE_CREATE_VIEW,
		ternv1.ChangeType_CHANGE_TYPE_VSCHEMA,
	} {
		assert.Equal(t, protoChangeTypeToOperation(ct), changeTypeToString(ct), "change type %v", ct)
	}
	assert.Empty(t, changeTypeToString(ternv1.ChangeType_CHANGE_TYPE_OTHER))
}

// A table's ETA travels from the progress proto through the HTTP response and
// its JSON encoding unchanged, so CLI and API consumers see the same figure
// the driver stored.
func TestProgressResponseFromProtoCarriesTableETA(t *testing.T) {
	resp := progressResponseFromProto(&ternv1.ProgressResponse{
		State:  ternv1.State_STATE_RUNNING,
		Engine: ternv1.Engine_ENGINE_SPIRIT,
		Tables: []*ternv1.TableProgress{
			{
				TableName:       "users",
				Namespace:       "testdb",
				Status:          "running",
				RowsCopied:      250000,
				RowsTotal:       500000,
				PercentComplete: 50,
				EtaSeconds:      540,
			},
		},
	})

	require.Len(t, resp.Tables, 1)
	assert.Equal(t, int64(540), resp.Tables[0].ETASeconds)

	encoded, err := json.Marshal(resp)
	require.NoError(t, err)
	var decoded apitypes.ProgressResponse
	require.NoError(t, json.Unmarshal(encoded, &decoded))

	require.Len(t, decoded.Tables, 1)
	assert.Equal(t, "users", decoded.Tables[0].TableName)
	assert.Equal(t, state.Apply.Running, decoded.State)
	assert.Equal(t, int64(250000), decoded.Tables[0].RowsCopied)
	assert.Equal(t, int64(500000), decoded.Tables[0].RowsTotal)
	assert.Equal(t, int32(50), decoded.Tables[0].PercentComplete)
	assert.Equal(t, int64(540), decoded.Tables[0].ETASeconds)
}

func statusFoldService() *Service {
	return &Service{logger: slog.New(slog.DiscardHandler)}
}

// A deployment applied per shard folds into one status row. All of the
// deployment's operations attach into the deployment's single data-plane
// apply, so the folded row carries that shared apply id as its external id —
// the operator sees one deployment, one data-plane apply. Per-operation
// external ids stay in the per-shard detail views, so the fold leaves the
// external operation id empty.
func TestStatusFoldCarriesDeploymentSharedDataPlaneApplyID(t *testing.T) {
	s := statusFoldService()
	apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-a"}
	ops := []*storage.ApplyOperation{
		{ID: 1, ApplyID: 7, Deployment: "west", State: state.ApplyOperation.Completed,
			ExternalID: "apply-remote-west", ExternalOperationID: "901"},
		{ID: 2, ApplyID: 7, Deployment: "west", State: state.ApplyOperation.Running,
			ExternalID: "apply-remote-west", ExternalOperationID: "902"},
	}

	summary := s.statusOperationForDeployment(apply, ops, "west")

	require.NotNil(t, summary)
	assert.Equal(t, "apply-remote-west", summary.ExternalID)
	assert.Empty(t, summary.ExternalOperationID)
	assert.Equal(t, state.Apply.Running, summary.State)
}

// Two data-plane apply ids on one deployment's operations means the planes
// have diverged; the status fold must not pick one, so it omits the external
// id and leaves the divergence to server logs.
func TestStatusFoldOmitsDivergentDataPlaneApplyIDs(t *testing.T) {
	s := statusFoldService()
	apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-a"}
	ops := []*storage.ApplyOperation{
		{ID: 1, ApplyID: 7, Deployment: "west", State: state.ApplyOperation.Running, ExternalID: "apply-remote-a"},
		{ID: 2, ApplyID: 7, Deployment: "west", State: state.ApplyOperation.Running, ExternalID: "apply-remote-b"},
	}

	summary := s.statusOperationForDeployment(apply, ops, "west")

	require.NotNil(t, summary)
	assert.Empty(t, summary.ExternalID)
	assert.Equal(t, state.Apply.Running, summary.State)
}

// Locally driven operations carry engine-owned resume state, not a data-plane
// apply id; the status fold must never surface that state as an external id.
func TestStatusFoldIgnoresEngineResumeState(t *testing.T) {
	s := statusFoldService()
	apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-a"}
	ops := []*storage.ApplyOperation{
		{ID: 1, ApplyID: 7, Deployment: "west", State: state.ApplyOperation.Running, EngineResumeContext: "vt-ctx-1"},
		{ID: 2, ApplyID: 7, Deployment: "west", State: state.ApplyOperation.Running, EngineResumeContext: "vt-ctx-2"},
	}

	summary := s.statusOperationForDeployment(apply, ops, "west")

	require.NotNil(t, summary)
	assert.Empty(t, summary.ExternalID)
}

// A deployment with a single operation is returned as its own row, external
// ids intact — no fold, no synthetic summary.
func TestStatusSingleOperationRowKeepsItsExternalIDs(t *testing.T) {
	s := statusFoldService()
	apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-a"}
	op := &storage.ApplyOperation{ID: 1, ApplyID: 7, Deployment: "west",
		State: state.ApplyOperation.Running, ExternalID: "apply-remote-west", ExternalOperationID: "901"}

	summary := s.statusOperationForDeployment(apply, []*storage.ApplyOperation{op}, "west")

	require.Same(t, op, summary)
}

// Each stored apply_operation's operation key travels through the progress
// response and its JSON encoding, so an operator triaging a multi-operation
// apply with `status --json` can correlate each JSON operation row with the
// stored apply_operation and its data-plane work.
func TestProgressOperationsCarryOperationKey(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{
			ID:           1,
			Deployment:   "region-a",
			OperationKey: "commerce/-80/users",
			State:        state.Apply.Running,
		},
		{
			ID:           2,
			Deployment:   "region-a",
			OperationKey: "commerce/80-/users",
			State:        state.Apply.Completed,
		},
	}

	responses, _ := progressOperationsFromRows(ops)
	require.Len(t, responses, 2)
	assert.Equal(t, "commerce/-80/users", responses[0].OperationKey)
	assert.Equal(t, "commerce/80-/users", responses[1].OperationKey)

	encoded, err := json.Marshal(&apitypes.ProgressResponse{
		State:      state.Apply.Running,
		Operations: responses,
	})
	require.NoError(t, err)
	assert.Contains(t, string(encoded), `"operation_key":"commerce/-80/users"`)

	var decoded apitypes.ProgressResponse
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Len(t, decoded.Operations, 2)
	assert.Equal(t, "commerce/-80/users", decoded.Operations[0].OperationKey)
	assert.Equal(t, "commerce/80-/users", decoded.Operations[1].OperationKey)
}

// The operation-id map that attributes tasks to rollout members carries the
// whole routing pair. One deployment can address several targets, each copying
// the same tables against its own schema, so a task attributed to the deployment
// alone would not say which member's progress it reports.
func TestProgressOperationsMapCarriesTheRoutingPair(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "primary", Target: "testapp-001", State: state.Apply.Running},
		{ID: 2, Deployment: "primary", Target: "testapp-002", State: state.Apply.Pending},
		{ID: 3, Deployment: "eu-west", Target: "orders-eu", State: state.Apply.Pending},
	}

	_, memberByOperationID := progressOperationsFromRows(ops)
	require.Len(t, memberByOperationID, 3)
	assert.Equal(t, routing.ExecutionTarget{Deployment: "primary", Target: "testapp-001"}, memberByOperationID[1])
	assert.Equal(t, routing.ExecutionTarget{Deployment: "primary", Target: "testapp-002"}, memberByOperationID[2])
	assert.Equal(t, routing.ExecutionTarget{Deployment: "eu-west", Target: "orders-eu"}, memberByOperationID[3])
}

// A table's target survives the API's JSON encoding, so the CLI can scope each
// member's section to the copies that member actually ran.
func TestTableProgressResponseEncodesTarget(t *testing.T) {
	encoded, err := json.Marshal(&apitypes.ProgressResponse{
		State: state.Apply.Running,
		Tables: []*apitypes.TableProgressResponse{
			{TableName: "users", Deployment: "primary", Target: "testapp-002", Status: state.Task.Running},
		},
	})
	require.NoError(t, err)
	assert.Contains(t, string(encoded), `"target":"testapp-002"`)

	var decoded apitypes.ProgressResponse
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Len(t, decoded.Tables, 1)
	assert.Equal(t, "testapp-002", decoded.Tables[0].Target)
}
