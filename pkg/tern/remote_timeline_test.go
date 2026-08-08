package tern

import (
	"context"
	"log/slog"
	"net"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// logServingTernServer serves a fixed data-plane timeline, or an error.
type logServingTernServer struct {
	ternv1.UnimplementedTernServer
	logs    []*ternv1.ApplyLog
	logsErr error
	req     *ternv1.LogsRequest
}

func (s *logServingTernServer) Logs(_ context.Context, req *ternv1.LogsRequest) (*ternv1.LogsResponse, error) {
	s.req = req
	if s.logsErr != nil {
		return nil, s.logsErr
	}
	return &ternv1.LogsResponse{Logs: s.logs}, nil
}

func logMirrorClient(t *testing.T, server *logServingTernServer, logs *mockApplyLogStore) *GRPCClient {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "localhost:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	ternv1.RegisterTernServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	client := &GRPCClient{conn: conn, client: ternv1.NewTernClient(conn), storage: &mockStorage{logs: logs}, logger: slog.Default()}
	t.Cleanup(func() {
		utils.CloseAndLog(client)
		grpcServer.Stop()
		utils.CloseAndLog(lis)
	})
	return client
}

func mirroredApply() *storage.Apply {
	return &storage.Apply{ID: 3, ApplyIdentifier: "apply-3", Database: "appdb", DatabaseType: storage.DatabaseTypeVitess, Deployment: "appdb", Environment: "staging"}
}

// The control plane learns remote state by asking on an interval, so a state
// the data plane occupies only briefly between two polls leaves no trace and the
// timeline reads as a transition that never happened. Once the schema change is
// finished the data plane's own record is complete, so the states that came and
// went are recovered from it.
func TestMirrorMissingRemoteTransitions(t *testing.T) {
	remoteTimeline := []*ternv1.ApplyLog{
		{EventType: storage.LogEventStateTransition, OldState: state.Apply.CuttingOver, NewState: state.Apply.RevertWindow},
		{EventType: storage.LogEventStateTransition, OldState: state.Apply.RevertWindow, NewState: state.Apply.SkippingRevert},
		{EventType: storage.LogEventStateTransition, OldState: state.Apply.SkippingRevert, NewState: state.Apply.Completed},
		{EventType: storage.LogEventProgress, Message: "50% copied"},
	}

	t.Run("records the state polling never sampled", func(t *testing.T) {
		logs := &mockApplyLogStore{logs: []*storage.ApplyLog{
			{EventType: storage.LogEventStateTransition, NewState: state.Apply.RevertWindow},
			{EventType: storage.LogEventStateTransition, NewState: state.Apply.Completed},
		}}
		server := &logServingTernServer{logs: remoteTimeline}
		client := logMirrorClient(t, server, logs)

		client.mirrorMissingRemoteTransitions(t.Context(), mirroredApply(), "remote-apply-1")

		require.Len(t, logs.logs, 3, "only the unsampled state is added")
		added := logs.logs[2]
		assert.Equal(t, state.Apply.SkippingRevert, added.NewState)
		assert.Equal(t, state.Apply.RevertWindow, added.OldState)
		assert.Contains(t, added.Message, "observed on the data plane")
		assert.Equal(t, "remote-apply-1", server.req.ApplyId)
	})

	t.Run("adds nothing when polling saw every state", func(t *testing.T) {
		logs := &mockApplyLogStore{logs: []*storage.ApplyLog{
			{EventType: storage.LogEventStateTransition, NewState: state.Apply.RevertWindow},
			{EventType: storage.LogEventStateTransition, NewState: state.Apply.SkippingRevert},
			{EventType: storage.LogEventStateTransition, NewState: state.Apply.Completed},
		}}
		client := logMirrorClient(t, &logServingTernServer{logs: remoteTimeline}, logs)

		client.mirrorMissingRemoteTransitions(t.Context(), mirroredApply(), "remote-apply-1")

		assert.Len(t, logs.logs, 3)
	})

	// The terminal state is already persisted by the time this runs, so an
	// unreadable data plane leaves the timeline incomplete rather than turning a
	// finished schema change into a failed reconcile.
	t.Run("an unreadable data plane leaves the timeline as it is", func(t *testing.T) {
		logs := &mockApplyLogStore{}
		client := logMirrorClient(t, &logServingTernServer{logsErr: status.Error(codes.Unimplemented, "no log reads")}, logs)

		client.mirrorMissingRemoteTransitions(t.Context(), mirroredApply(), "remote-apply-1")

		assert.Empty(t, logs.logs)
	})

	t.Run("a drive with no remote apply id reads nothing", func(t *testing.T) {
		logs := &mockApplyLogStore{}
		server := &logServingTernServer{logs: remoteTimeline}
		client := logMirrorClient(t, server, logs)

		client.mirrorMissingRemoteTransitions(t.Context(), mirroredApply(), "")

		assert.Empty(t, logs.logs)
		assert.Nil(t, server.req)
	})
}
