package api

import (
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/presentation"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// Live-remote progress renders the same operation string for a table change
// that the storage-served path persists as the task's DDL action, so a client
// polling an apply sees a stable change type across the two sources, for
// every value of the proto enum. Only CHANGE_TYPE_OTHER differs by design:
// live progress has no DDL parser to recover an operation from, so it
// renders the change type as empty.
func TestChangeTypeToStringMatchesStorageOperation(t *testing.T) {
	for value, name := range ternv1.ChangeType_name {
		ct := ternv1.ChangeType(value)
		if ct == ternv1.ChangeType_CHANGE_TYPE_OTHER {
			continue
		}
		assert.Equal(t, protoChangeTypeToOperation(ct), changeTypeToString(ct), "change type %s", name)
		assert.NotEmpty(t, changeTypeToString(ct), "change type %s", name)
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

// A table's planned size and row count travel from the progress proto through
// the HTTP response as estimated_bytes and estimated_rows, and a table without
// them omits the fields rather than reporting an empty table.
func TestProgressResponseFromProtoCarriesPlanEstimates(t *testing.T) {
	bytes := int64(23_400_000_000)
	rows := int64(48_200_000)
	resp := progressResponseFromProto(&ternv1.ProgressResponse{
		State:  ternv1.State_STATE_RUNNING,
		Engine: ternv1.Engine_ENGINE_SPIRIT,
		Tables: []*ternv1.TableProgress{
			{TableName: "orders", Namespace: "testdb", Status: "running", RowsTotal: 48_200_000, EstimatedBytes: &bytes, EstimatedRows: &rows},
			{TableName: "users", Namespace: "testdb", Status: "running", RowsTotal: 1000},
		},
	})

	require.Len(t, resp.Tables, 2)
	require.NotNil(t, resp.Tables[0].EstimatedBytes)
	assert.Equal(t, bytes, *resp.Tables[0].EstimatedBytes)
	require.NotNil(t, resp.Tables[0].EstimatedRows)
	assert.Equal(t, rows, *resp.Tables[0].EstimatedRows)
	assert.Nil(t, resp.Tables[1].EstimatedBytes)
	assert.Nil(t, resp.Tables[1].EstimatedRows)

	encoded, err := json.Marshal(resp.Tables)
	require.NoError(t, err)
	var decoded []map[string]any
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Len(t, decoded, 2)
	assert.InDelta(t, float64(bytes), decoded[0]["estimated_bytes"], 0)
	assert.InDelta(t, float64(rows), decoded[0]["estimated_rows"], 0)
	assert.NotContains(t, decoded[1], "estimated_bytes")
	assert.NotContains(t, decoded[1], "estimated_rows")
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

// A target the apply recorded as already holding the change keeps that mark in
// the progress response and its JSON encoding, so a client tells it apart from
// a completed operation that never started for another reason, such as one a
// reaper settled to its apply's outcome.
func TestProgressOperationsCarryAlreadyConverged(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "primary", Target: "testapp-001", State: state.ApplyOperation.Completed, AlreadyConverged: true},
		{ID: 2, Deployment: "primary", Target: "testapp-002", State: state.ApplyOperation.Completed},
	}

	responses, _ := progressOperationsFromRows(ops)
	require.Len(t, responses, 2)
	assert.True(t, responses[0].AlreadyConverged)
	assert.False(t, responses[1].AlreadyConverged)

	encoded, err := json.Marshal(responses)
	require.NoError(t, err)
	assert.Equal(t, 1, strings.Count(string(encoded), `"already_converged":true`), string(encoded))
}

// A shard SchemaBot failed at the cutover for another shard's failure names
// that shard's operation by deployment and key in the progress response, so a
// client reads it as stopped at cutover. A row whose ended operation the
// apply does not list still carries an empty reference, and a row that failed
// on its own carries none.
func TestProgressOperationsCarryEndedBy(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "primary", OperationKey: "shop/-40/users", State: state.ApplyOperation.Failed},
		{ID: 2, Deployment: "primary", OperationKey: "shop/40-80/users", State: state.ApplyOperation.Failed, EndedByOperationID: 1},
		{ID: 3, Deployment: "primary", OperationKey: "shop/80-/users", State: state.ApplyOperation.Failed, EndedByOperationID: 99},
	}

	responses, _ := progressOperationsFromRows(ops)
	require.Len(t, responses, 3)
	assert.Nil(t, responses[0].EndedBy)
	assert.Equal(t, &apitypes.ProgressOperationRef{Deployment: "primary", OperationKey: "shop/-40/users"}, responses[1].EndedBy)
	assert.Equal(t, &apitypes.ProgressOperationRef{}, responses[2].EndedBy)

	encoded, err := json.Marshal(responses[1])
	require.NoError(t, err)
	assert.Contains(t, string(encoded), `"ended_by":{"deployment":"primary","operation_key":"shop/-40/users"}`)
}

// A row of a rollout run table by table keeps its table step in the progress
// response and its JSON encoding, which is what folds a target's rows into one
// member and counts the rollout in tables. A row of a member's whole change
// omits the field.
func TestProgressOperationsCarryRolloutStep(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "primary", Target: "testapp-001", State: state.ApplyOperation.Running, RolloutStep: 2},
		{ID: 2, Deployment: "primary", Target: "testapp-002", State: state.ApplyOperation.Pending},
	}

	responses, _ := progressOperationsFromRows(ops)
	require.Len(t, responses, 2)
	assert.Equal(t, 2, responses[0].RolloutStep)
	assert.Equal(t, 0, responses[1].RolloutStep)

	encoded, err := json.Marshal(responses)
	require.NoError(t, err)
	assert.Equal(t, 1, strings.Count(string(encoded), `"rollout_step":2`), string(encoded))
	assert.Equal(t, 1, strings.Count(string(encoded), `"rollout_step"`), "a row of a member's whole change omits the step: %s", encoded)
}

// Each table names the operation that runs it, and each operation carries its
// own progress metadata. One target can run several operations, and the
// response's top-level metadata speaks for one operation only, so a rollout's
// table reads the work in flight from its own operation rather than from
// whichever operation the top level happened to describe.
func TestProgressAttributesEachTableToItsOperation(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, Deployment: "primary", Target: "testapp-001", OperationKey: "step-1", State: state.Apply.Completed},
		{ID: 2, Deployment: "primary", Target: "testapp-001", OperationKey: "step-2", State: state.Apply.Running,
			ProgressMetadata: `{"executor_operation":"concurrent-index-build","server_phase":"building index: scanning table"}`},
		{ID: 3, Deployment: "eu-west", Target: "orders-eu", State: state.Apply.Pending},
	}

	responses, operationByID := progressOperationsFromRows(ops)
	require.Len(t, responses, 3)
	assert.Nil(t, responses[0].Metadata, "an operation that recorded no progress metadata carries none")
	assert.Equal(t, map[string]string{
		"executor_operation": "concurrent-index-build",
		"server_phase":       "building index: scanning table",
	}, responses[1].Metadata)

	opID := int64(2)
	var tpr apitypes.TableProgressResponse
	attributeStoredTask(&tpr, &storage.Task{TaskIdentifier: "task-users", ApplyOperationID: &opID}, operationByID)
	assert.Equal(t, apitypes.TableProgressResponse{TaskID: "task-users", Deployment: "primary", Target: "testapp-001", OperationKey: "step-2"}, tpr)
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

// Every engine the data plane can report has a display name, so a plan or
// progress response never names its engine "Unknown".
func TestEngineNameNamesEveryEngine(t *testing.T) {
	want := map[ternv1.Engine]string{
		ternv1.Engine_ENGINE_SPIRIT:      "Spirit",
		ternv1.Engine_ENGINE_PLANETSCALE: "PlanetScale",
		ternv1.Engine_ENGINE_STRATA:      "Strata",
		ternv1.Engine_ENGINE_POSTGRES:    "PostgreSQL",
	}
	require.Len(t, want, len(ternv1.Engine_name), "every proto engine needs an expected display name here")
	for value := range ternv1.Engine_name {
		engine := ternv1.Engine(value)
		assert.Equal(t, want[engine], engineName(engine), "engine %s", engine)
	}
}

// The PR comment names an apply's engine as storage records it and the CLI as
// the progress API reports it. Both names must choose the same stop or cancel
// command, or one surface offers a command the engine refuses while the other
// offers the one it accepts.
func TestProgressEngineNamesChooseTheStorageEnginesStopCommand(t *testing.T) {
	build := apitypes.BuildWork{Operation: "concurrent-index-build"}
	for engine, stored := range map[ternv1.Engine]string{
		ternv1.Engine_ENGINE_SPIRIT:      storage.EngineSpirit,
		ternv1.Engine_ENGINE_PLANETSCALE: storage.EnginePlanetScale,
		ternv1.Engine_ENGINE_STRATA:      storage.EngineStrata,
		ternv1.Engine_ENGINE_POSTGRES:    storage.EnginePostgres,
	} {
		t.Run(stored, func(t *testing.T) {
			for _, work := range []apitypes.BuildWork{{}, build} {
				assert.Equal(t, presentation.StopCommand(stored, work, 1), presentation.StopCommand(engineName(engine), work, 1),
					"%s reported as %q", stored, engineName(engine))
			}
		})
	}
}

// A shard that failed remotely records its error on its tasks, not its
// operation row, so the progress response names the first task error for it,
// the reason the PR comment names, while a row's own error stands as it is.
func TestProgressOperationsCarryTheirFirstTaskError(t *testing.T) {
	ops := []*storage.ApplyOperation{
		{ID: 1, OperationKey: "shop/-80/orders", State: state.ApplyOperation.Failed},
		{ID: 2, OperationKey: "shop/80-/orders", State: state.ApplyOperation.Failed, ErrorMessage: "copy failed on orders"},
		{ID: 3, OperationKey: "shop/c0-/orders", State: state.ApplyOperation.Completed},
	}
	tasks := []*storage.Task{
		{ApplyOperationID: new(int64(1)), TableName: "orders", ErrorMessage: "dial tcp 10.0.0.11:3306: connect: connection refused"},
		{ApplyOperationID: new(int64(2)), TableName: "orders", ErrorMessage: "lock wait timeout exceeded"},
		{ApplyOperationID: new(int64(3)), TableName: "orders"},
	}
	responses, _ := progressOperationsFromRows(ops)

	setProgressOperationErrors(responses, ops, tasks)

	assert.Equal(t, "dial tcp 10.0.0.11:3306: connect: connection refused", responses[0].ErrorMessage)
	assert.Equal(t, deriveErrorCode(state.ApplyOperation.Failed, responses[0].ErrorMessage), responses[0].ErrorCode)
	assert.Equal(t, "copy failed on orders", responses[1].ErrorMessage, "the row's own error stands")
	assert.Empty(t, responses[2].ErrorMessage)
}
