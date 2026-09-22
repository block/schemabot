package api

import (
	"fmt"
	"io"
	"log/slog"
	"testing"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// engineLogsService builds a service whose apply resolves to the given
// operations and whose named deployments answer with the given clients.
func engineLogsService(apply *storage.Apply, operations []*storage.ApplyOperation, clients map[string]tern.Client) *Service {
	return New(&mockStorageWithApplyStores{
		applies:    &staticApplyStore{apply: apply},
		operations: &staticApplyOperationStore{operations: operations},
	}, testServerConfig(), clients, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func engineLogsApply() *storage.Apply {
	return &storage.Apply{ID: 71, ApplyIdentifier: "apply-control", Database: "commerce", DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging"}
}

// A data plane's apply log carries both streams: the lines SchemaBot wrote
// about its own state machine, and the lines the engine wrote about the
// change it was running. Only the engine's lines belong in the engine-logs
// fold — the control plane already has its own account of the apply, and
// repeating it would cost the fold the room the engine's lines need.
func TestEngineApplyLogsKeepsOnlyEngineAuthoredLines(t *testing.T) {
	apply := engineLogsApply()
	operations := []*storage.ApplyOperation{
		{ApplyID: apply.ID, Deployment: "region-a", OperationKey: "commerce/orders", Target: "cluster-a", ExternalID: "remote-a"},
	}
	client := &mockTernClient{isRemote: true}
	client.logsHook = func(req *ternv1.LogsRequest) (*ternv1.LogsResponse, error) {
		return &ternv1.LogsResponse{ApplyId: req.ApplyId, Logs: []*ternv1.ApplyLog{
			{Id: 1, Level: "info", Source: storage.LogSourceSchemaBot, Message: "Apply claimed by driver", CreatedAt: "2026-07-18T18:33:10Z"},
			{Id: 2, Level: "info", Source: storage.LogSourceSpirit, Message: "[orders] copy starting", CreatedAt: "2026-07-18T18:33:11Z"},
			{Id: 3, Level: "warn", Source: storage.LogSourceSpirit, Message: "[orders] unsafe warning 1265", CreatedAt: "2026-07-18T18:33:12Z"},
			// An unset source stores as the SchemaBot source, so it is the
			// control plane speaking, not the engine.
			{Id: 4, Level: "error", Message: "Apply failed", CreatedAt: "2026-07-18T18:33:13Z"},
		}}, nil
	}
	service := engineLogsService(apply, operations, map[string]tern.Client{"region-a/staging": client})

	sources, err := service.EngineApplyLogs(t.Context(), apply, 50)
	require.NoError(t, err)
	require.Len(t, sources, 1)
	assert.Equal(t, "region-a", sources[0].Deployment)
	assert.Equal(t, "cluster-a", sources[0].Target)
	assert.False(t, sources[0].HasOlder)
	require.Len(t, sources[0].Entries, 2)
	assert.Equal(t, "[orders] copy starting", sources[0].Entries[0].Message)
	assert.Equal(t, "[orders] unsafe warning 1265", sources[0].Entries[1].Message)
	assert.Equal(t, "remote-a", sources[0].Entries[0].ApplyID)

	require.Len(t, client.logsReqs, 1)
	assert.Equal(t, "remote-a", client.logsReqs[0].ApplyId, "the data plane is addressed by its own apply id, never the control plane's")
	assert.Equal(t, "cluster-a", client.logsReqs[0].Target)
	assert.Equal(t, int32(tern.MaxLogsLimit), client.logsReqs[0].Limit,
		"the read takes the data plane's whole window, because the engine lines wanted are filtered out of it here")
}

// An apply driven in this process writes the engine's lines into
// control-plane storage, where the summary's own recent-logs fold already
// shows them. Reading them back as a second stream would print every engine
// line twice, so a deployment with no data plane produces no source at all.
func TestEngineApplyLogsSkipsDeploymentWithNoDataPlane(t *testing.T) {
	apply := engineLogsApply()
	operations := []*storage.ApplyOperation{
		{ApplyID: apply.ID, Deployment: "local", OperationKey: "commerce/orders", ExternalID: "remote-a"},
	}
	client := &mockTernClient{isRemote: false}
	service := engineLogsService(apply, operations, map[string]tern.Client{"local/staging": client})

	sources, err := service.EngineApplyLogs(t.Context(), apply, 50)
	require.NoError(t, err)
	assert.Empty(t, sources)
	assert.Empty(t, client.logsReqs, "a deployment with no data plane is never read")
}

// An operation that has not been dispatched yet has no data-plane apply id to
// address, so there is nothing to read. It must not be turned into a read
// against the control plane's own apply id, which names a different apply on
// the other side.
func TestEngineApplyLogsSkipsUndispatchedOperation(t *testing.T) {
	apply := engineLogsApply()
	operations := []*storage.ApplyOperation{
		{ApplyID: apply.ID, Deployment: "region-a", OperationKey: "commerce/orders", Target: "cluster-a"},
		{ApplyID: apply.ID, Deployment: "region-a", OperationKey: "commerce/users", Target: "cluster-b"},
	}
	client := &mockTernClient{isRemote: true}
	service := engineLogsService(apply, operations, map[string]tern.Client{"region-a/staging": client})

	sources, err := service.EngineApplyLogs(t.Context(), apply, 50)
	require.NoError(t, err)
	assert.Empty(t, sources)
	assert.Empty(t, client.logsReqs)
}

// A fan-out reads every data plane the apply ran on, in a stable order, and
// one data plane that cannot be read costs the caller that source alone. A
// failed apply's comment is the surface an operator triages from, so losing
// the whole fold because one region is unreachable would take away the
// evidence that is still available.
func TestEngineApplyLogsFansOutAndSurvivesOneUnreadableDataPlane(t *testing.T) {
	apply := engineLogsApply()
	operations := []*storage.ApplyOperation{
		{ApplyID: apply.ID, Deployment: "region-b", OperationKey: "commerce/users", Target: "cluster-b", ExternalID: "remote-b"},
		{ApplyID: apply.ID, Deployment: "region-a", OperationKey: "commerce/orders", Target: "cluster-a", ExternalID: "remote-a"},
	}
	readable := &mockTernClient{isRemote: true}
	readable.logsHook = func(req *ternv1.LogsRequest) (*ternv1.LogsResponse, error) {
		return &ternv1.LogsResponse{ApplyId: req.ApplyId, Logs: []*ternv1.ApplyLog{
			{Id: 1, Level: "info", Source: storage.LogSourceSpirit, Message: "[orders] copy starting", CreatedAt: "2026-07-18T18:33:11Z"},
		}}, nil
	}
	unreachable := &mockTernClient{isRemote: true}
	unreachable.logsHook = func(*ternv1.LogsRequest) (*ternv1.LogsResponse, error) {
		return nil, fmt.Errorf("connection refused")
	}
	service := engineLogsService(apply, operations, map[string]tern.Client{
		"region-a/staging": readable,
		"region-b/staging": unreachable,
	})

	sources, err := service.EngineApplyLogs(t.Context(), apply, 50)
	require.NoError(t, err)
	require.Len(t, sources, 1)
	assert.Equal(t, "region-a", sources[0].Deployment, "deployments are read in a stable order")
	require.Len(t, sources[0].Entries, 1)
}

// A source with more engine lines than the caller asked for keeps the newest
// and says so: the lines leading up to the failure are the ones that explain
// it, and a window shown without the marker reads as the engine's complete
// account of the change.
func TestEngineApplyLogsKeepsTheNewestLinesAndReportsOlderOnes(t *testing.T) {
	apply := engineLogsApply()
	operations := []*storage.ApplyOperation{
		{ApplyID: apply.ID, Deployment: "region-a", OperationKey: "commerce/orders", Target: "cluster-a", ExternalID: "remote-a"},
	}
	client := &mockTernClient{isRemote: true}
	client.logsHook = func(req *ternv1.LogsRequest) (*ternv1.LogsResponse, error) {
		resp := &ternv1.LogsResponse{ApplyId: req.ApplyId}
		for id := 1; id <= 5; id++ {
			resp.Logs = append(resp.Logs, &ternv1.ApplyLog{Id: int64(id), Level: "info", Source: storage.LogSourceSpirit, Message: fmt.Sprintf("line %d", id), CreatedAt: "2026-07-18T18:33:11Z"})
		}
		return resp, nil
	}
	service := engineLogsService(apply, operations, map[string]tern.Client{"region-a/staging": client})

	sources, err := service.EngineApplyLogs(t.Context(), apply, 2)
	require.NoError(t, err)
	require.Len(t, sources, 1)
	assert.True(t, sources[0].HasOlder)
	require.Len(t, sources[0].Entries, 2)
	assert.Equal(t, "line 4", sources[0].Entries[0].Message)
	assert.Equal(t, "line 5", sources[0].Entries[1].Message)
}

// A data plane that answered with nothing the engine wrote is not a failure —
// a change that failed before the engine ran leaves exactly that — so it
// yields no source rather than an empty one a renderer would have to drop.
func TestEngineApplyLogsReturnsNoSourceWhenTheEngineSaidNothing(t *testing.T) {
	apply := engineLogsApply()
	operations := []*storage.ApplyOperation{
		{ApplyID: apply.ID, Deployment: "region-a", OperationKey: "commerce/orders", Target: "cluster-a", ExternalID: "remote-a"},
	}
	client := &mockTernClient{isRemote: true}
	client.logsHook = func(req *ternv1.LogsRequest) (*ternv1.LogsResponse, error) {
		return &ternv1.LogsResponse{ApplyId: req.ApplyId, Logs: []*ternv1.ApplyLog{
			{Id: 1, Level: "error", Source: storage.LogSourceSchemaBot, Message: "Apply failed: credentials rejected", CreatedAt: "2026-07-18T18:33:10Z"},
		}}, nil
	}
	service := engineLogsService(apply, operations, map[string]tern.Client{"region-a/staging": client})

	sources, err := service.EngineApplyLogs(t.Context(), apply, 50)
	require.NoError(t, err)
	assert.Empty(t, sources)
}

// The operation rows are what say where the change ran, so a read that cannot
// load them has no way to tell "no data plane" from "a data plane we failed to
// look up". It reports the failure rather than answering as though the apply
// ran nowhere.
func TestEngineApplyLogsFailsWhenTheOperationRowsCannotBeLoaded(t *testing.T) {
	apply := engineLogsApply()
	service := New(&mockStorageWithApplyStores{
		applies:    &staticApplyStore{apply: apply},
		operations: &staticApplyOperationStore{err: fmt.Errorf("storage unavailable")},
	}, testServerConfig(), nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	sources, err := service.EngineApplyLogs(t.Context(), apply, 50)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "apply-control")
	assert.Empty(t, sources)
}
