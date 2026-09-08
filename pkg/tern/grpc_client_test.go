package tern

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// mockTernServer implements TernServer for testing.
type mockTernServer struct {
	ternv1.UnimplementedTernServer
	healthErr error
}

func (s *mockTernServer) Health(ctx context.Context, req *ternv1.HealthRequest) (*ternv1.HealthResponse, error) {
	if s.healthErr != nil {
		return nil, s.healthErr
	}
	return &ternv1.HealthResponse{Status: "ok"}, nil
}

func (s *mockTernServer) Plan(ctx context.Context, req *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	if req.Database == "" {
		return nil, status.Error(codes.InvalidArgument, "database is required")
	}
	return &ternv1.PlanResponse{
		PlanId: "test-plan-id",
		Engine: ternv1.Engine_ENGINE_PLANETSCALE,
	}, nil
}

func (s *mockTernServer) Apply(ctx context.Context, req *ternv1.ApplyRequest) (*ternv1.ApplyResponse, error) {
	if req.PlanId == "" {
		return nil, status.Error(codes.InvalidArgument, "plan_id is required")
	}
	return &ternv1.ApplyResponse{Accepted: true}, nil
}

func (s *mockTernServer) Progress(ctx context.Context, req *ternv1.ProgressRequest) (*ternv1.ProgressResponse, error) {
	if req.ApplyId == "" {
		return nil, status.Error(codes.InvalidArgument, "apply_id is required")
	}
	return &ternv1.ProgressResponse{
		State:  ternv1.State_STATE_RUNNING,
		Engine: ternv1.Engine_ENGINE_SPIRIT,
	}, nil
}
func (s *mockTernServer) Logs(context.Context, *ternv1.LogsRequest) (*ternv1.LogsResponse, error) {
	return &ternv1.LogsResponse{}, nil
}

func (s *mockTernServer) Cutover(ctx context.Context, req *ternv1.CutoverRequest) (*ternv1.CutoverResponse, error) {
	if req.ApplyId == "" {
		return nil, status.Error(codes.InvalidArgument, "apply_id is required")
	}
	return &ternv1.CutoverResponse{Accepted: true}, nil
}

func (s *mockTernServer) Stop(ctx context.Context, req *ternv1.StopRequest) (*ternv1.StopResponse, error) {
	if req.ApplyId == "" {
		return nil, status.Error(codes.InvalidArgument, "apply_id is required")
	}
	return &ternv1.StopResponse{Accepted: true}, nil
}

func (s *mockTernServer) Cancel(ctx context.Context, req *ternv1.CancelRequest) (*ternv1.CancelResponse, error) {
	if req.ApplyId == "" {
		return nil, status.Error(codes.InvalidArgument, "apply_id is required")
	}
	return &ternv1.CancelResponse{Accepted: true}, nil
}

func (s *mockTernServer) Start(ctx context.Context, req *ternv1.StartRequest) (*ternv1.StartResponse, error) {
	if req.ApplyId == "" {
		return nil, status.Error(codes.InvalidArgument, "apply_id is required")
	}
	return &ternv1.StartResponse{Accepted: true}, nil
}

func (s *mockTernServer) Revert(ctx context.Context, req *ternv1.RevertRequest) (*ternv1.RevertResponse, error) {
	if req.ApplyId == "" {
		return nil, status.Error(codes.InvalidArgument, "apply_id is required")
	}
	return &ternv1.RevertResponse{Accepted: true}, nil
}

func (s *mockTernServer) SkipRevert(ctx context.Context, req *ternv1.SkipRevertRequest) (*ternv1.SkipRevertResponse, error) {
	if req.ApplyId == "" {
		return nil, status.Error(codes.InvalidArgument, "apply_id is required")
	}
	return &ternv1.SkipRevertResponse{Accepted: true}, nil
}

// flakyTernServer fails the first N calls of an RPC with UNAVAILABLE and then
// succeeds, simulating a transient transport blip in front of a healthy
// remote deployment.
type flakyTernServer struct {
	ternv1.UnimplementedTernServer
	mu          sync.Mutex
	planCalls   int
	applyCalls  int
	pullCalls   int
	healthCalls int
	failPlans   int
	failApplies int
	failPulls   int
	failHealths int
}

func (s *flakyTernServer) Plan(context.Context, *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.planCalls++
	if s.planCalls <= s.failPlans {
		return nil, status.Error(codes.Unavailable, "upstream connect error")
	}
	return &ternv1.PlanResponse{PlanId: "plan-after-retry"}, nil
}

func (s *flakyTernServer) Apply(context.Context, *ternv1.ApplyRequest) (*ternv1.ApplyResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applyCalls++
	if s.applyCalls <= s.failApplies {
		return nil, status.Error(codes.Unavailable, "upstream connect error")
	}
	return &ternv1.ApplyResponse{Accepted: true, ApplyId: "remote-apply-1"}, nil
}

func (s *flakyTernServer) PullSchema(context.Context, *ternv1.PullSchemaRequest) (*ternv1.PullSchemaResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pullCalls++
	if s.pullCalls <= s.failPulls {
		return nil, status.Error(codes.Unavailable, "upstream connect error")
	}
	return &ternv1.PullSchemaResponse{Database: "testdb"}, nil
}

func (s *flakyTernServer) Health(context.Context, *ternv1.HealthRequest) (*ternv1.HealthResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.healthCalls++
	if s.healthCalls <= s.failHealths {
		return nil, status.Error(codes.Unavailable, "upstream connect error")
	}
	return &ternv1.HealthResponse{Status: "ok"}, nil
}

func (s *flakyTernServer) calls() (plans, applies int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.planCalls, s.applyCalls
}

func (s *flakyTernServer) readCalls() (pulls, healths int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pullCalls, s.healthCalls
}

// newRetryTestClient starts an in-process Tern gRPC server and connects a
// GRPCClient through the production constructor so the client's retry policy
// is exercised.
func newRetryTestClient(t *testing.T, server ternv1.TernServer) *GRPCClient {
	t.Helper()

	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "localhost:0")
	require.NoError(t, err, "failed to listen")

	grpcServer := grpc.NewServer()
	ternv1.RegisterTernServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(lis)
	}()

	client, err := NewGRPCClient(Config{Address: lis.Addr().String()})
	require.NoError(t, err, "failed to create client")

	t.Cleanup(func() {
		utils.CloseAndLog(client)
		grpcServer.Stop()
	})
	return client
}

// staticPullErrTernServer answers PullSchema with a fixed error, standing in
// for a remote data plane whose pull verdict the client must classify.
type staticPullErrTernServer struct {
	ternv1.UnimplementedTernServer
	pullErr error
}

func (s *staticPullErrTernServer) PullSchema(context.Context, *ternv1.PullSchemaRequest) (*ternv1.PullSchemaResponse, error) {
	return nil, s.pullErr
}

// A remote data plane that answers pull with its own unsupported-type verdict
// (the tern server maps ErrPullSchemaUnsupportedType to codes.Unimplemented
// with the sentinel text in the status message) is re-derived client-side as
// the same sentinel, so callers classify local and remote pulls identically.
func TestGRPCClientPullSchemaRederivesRemoteUnsupportedVerdict(t *testing.T) {
	server := &staticPullErrTernServer{
		pullErr: status.Error(codes.Unimplemented, "pull schema for database orders type strata: engine does not support schema pull: "+ErrPullSchemaUnsupportedType.Error()),
	}
	client := newRetryTestClient(t, server)

	_, err := client.PullSchema(t.Context(), &ternv1.PullSchemaRequest{Database: "orders"})

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrPullSchemaUnsupportedType)
	assert.ErrorContains(t, err, "orders")
}

// An Unimplemented without the data plane's unsupported-type verdict — a
// proxy mapping an HTTP 404, or a data plane too old to serve the RPC — says
// nothing about the database type and must surface as an ordinary failure,
// never as the unsupported-type sentinel.
func TestGRPCClientPullSchemaKeepsInfrastructureUnimplemented(t *testing.T) {
	server := &staticPullErrTernServer{
		pullErr: status.Error(codes.Unimplemented, "unexpected HTTP status code received from server: 404 (Not Found)"),
	}
	client := newRetryTestClient(t, server)

	_, err := client.PullSchema(t.Context(), &ternv1.PullSchemaRequest{Database: "orders"})

	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrPullSchemaUnsupportedType)
	assert.Equal(t, codes.Unimplemented, status.Code(err))
}

// A transient UNAVAILABLE on the network path in front of a remote deployment
// must not fail a plan request: the client retries idempotent RPCs and
// returns the successful response.
func TestGRPCClientRetriesPlanOnUnavailable(t *testing.T) {
	server := &flakyTernServer{failPlans: 1}
	client := newRetryTestClient(t, server)

	resp, err := client.Plan(t.Context(), &ternv1.PlanRequest{Database: "testdb"})

	require.NoError(t, err)
	assert.Equal(t, "plan-after-retry", resp.GetPlanId())
	plans, _ := server.calls()
	assert.Equal(t, 2, plans, "client should retry the failed plan attempt")
}

// A data-plane pod restart or mesh drain lasts seconds, not milliseconds. A
// caller-facing read must ride out a sustained blip — several consecutive
// UNAVAILABLE attempts — and still return the response a human is waiting on.
func TestGRPCClientPullSchemaRidesOutSustainedUnavailable(t *testing.T) {
	server := &flakyTernServer{failPulls: 3}
	client := newRetryTestClient(t, server)

	resp, err := client.PullSchema(t.Context(), &ternv1.PullSchemaRequest{Database: "testdb"})

	require.NoError(t, err)
	assert.Equal(t, "testdb", resp.GetDatabase())
	pulls, _ := server.readCalls()
	assert.Equal(t, 4, pulls, "the caller-facing read budget must survive a sustained blip, not just a single reset")
}

// Retries are bounded, and the fast-poll budget stays small: Health feeds the
// remote-deployment outage monitor, which must observe an outage promptly
// rather than ride it out.
func TestGRPCClientHealthRetriesFailFast(t *testing.T) {
	server := &flakyTernServer{failHealths: 10}
	client := newRetryTestClient(t, server)

	err := client.Health(t.Context())

	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
	_, healths := server.readCalls()
	assert.Equal(t, 3, healths, "fast polls must stop after the small attempt budget so outages surface promptly")
}

// Apply is state-changing, so the client surfaces a transient UNAVAILABLE
// instead of re-sending the request; the operator's durable queue owns
// redelivery for dispatch failures.
func TestGRPCClientDoesNotRetryApplyOnUnavailable(t *testing.T) {
	server := &flakyTernServer{failApplies: 1}
	client := newRetryTestClient(t, server)

	_, err := client.Apply(t.Context(), &ternv1.ApplyRequest{PlanId: "plan-1"})

	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
	_, applies := server.calls()
	assert.Equal(t, 1, applies, "state-changing RPCs must not be retried")
}

// testClient creates a test server and returns a connected GRPCClient.
func testClient(t *testing.T, server *mockTernServer) (*GRPCClient, func()) {
	t.Helper()

	// Silence logs during tests
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))

	// Start server on random port
	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "localhost:0")
	require.NoError(t, err, "failed to listen")

	grpcServer := grpc.NewServer()
	ternv1.RegisterTernServer(grpcServer, server)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	// Create client
	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err, "failed to dial")

	client := &GRPCClient{
		conn:   conn,
		client: ternv1.NewTernClient(conn),
	}

	cleanup := func() {
		utils.CloseAndLog(client)
		grpcServer.Stop()
	}

	return client, cleanup
}

func TestGRPCClient_Health(t *testing.T) {
	t.Run("healthy", func(t *testing.T) {
		client, cleanup := testClient(t, &mockTernServer{})
		defer cleanup()

		err := client.Health(t.Context())
		require.NoError(t, err)
	})

	t.Run("unhealthy", func(t *testing.T) {
		client, cleanup := testClient(t, &mockTernServer{
			healthErr: status.Error(codes.Unavailable, "database unavailable"),
		})
		defer cleanup()

		err := client.Health(t.Context())
		require.Error(t, err)
	})
}

func TestGRPCClient_Plan(t *testing.T) {
	client, cleanup := testClient(t, &mockTernServer{})
	defer cleanup()

	t.Run("valid request", func(t *testing.T) {
		resp, err := client.Plan(t.Context(), &ternv1.PlanRequest{
			Database: "testdb",
			Type:     "vitess",
		})
		require.NoError(t, err)
		assert.Equal(t, "test-plan-id", resp.PlanId)
	})

	t.Run("missing database", func(t *testing.T) {
		_, err := client.Plan(t.Context(), &ternv1.PlanRequest{})
		require.Error(t, err)
		assert.Equal(t, codes.InvalidArgument, status.Code(err))
	})
}

func TestGRPCClient_Apply(t *testing.T) {
	client, cleanup := testClient(t, &mockTernServer{})
	defer cleanup()

	t.Run("valid request", func(t *testing.T) {
		resp, err := client.Apply(t.Context(), &ternv1.ApplyRequest{
			PlanId: "test-plan-id",
		})
		require.NoError(t, err)
		assert.True(t, resp.Accepted)
	})

	t.Run("missing plan_id", func(t *testing.T) {
		_, err := client.Apply(t.Context(), &ternv1.ApplyRequest{})
		require.Error(t, err)
	})
}

func TestGRPCClient_Progress(t *testing.T) {
	client, cleanup := testClient(t, &mockTernServer{})
	defer cleanup()

	t.Run("valid request", func(t *testing.T) {
		resp, err := client.Progress(t.Context(), &ternv1.ProgressRequest{
			ApplyId:     "apply-progress123",
			Environment: "staging",
		})
		require.NoError(t, err)
		assert.Equal(t, ternv1.State_STATE_RUNNING, resp.State)
	})
}

func TestGRPCClient_Cutover(t *testing.T) {
	client, cleanup := testClient(t, &mockTernServer{})
	defer cleanup()

	t.Run("valid request", func(t *testing.T) {
		resp, err := client.Cutover(t.Context(), &ternv1.CutoverRequest{
			ApplyId:     "apply-cut123",
			Environment: "staging",
		})
		require.NoError(t, err)
		assert.True(t, resp.Accepted)
	})
}

func TestGRPCClient_Stop(t *testing.T) {
	client, cleanup := testClient(t, &mockTernServer{})
	defer cleanup()

	t.Run("valid request", func(t *testing.T) {
		resp, err := client.Stop(t.Context(), &ternv1.StopRequest{
			ApplyId:     "apply-stop123",
			Environment: "staging",
		})
		require.NoError(t, err)
		assert.True(t, resp.Accepted)
	})

	t.Run("missing apply_id", func(t *testing.T) {
		_, err := client.Stop(t.Context(), &ternv1.StopRequest{})
		require.Error(t, err)
	})
}

func TestGRPCClient_Start(t *testing.T) {
	client, cleanup := testClient(t, &mockTernServer{})
	defer cleanup()

	t.Run("valid request", func(t *testing.T) {
		resp, err := client.Start(t.Context(), &ternv1.StartRequest{
			ApplyId:     "apply-start123",
			Environment: "staging",
		})
		require.NoError(t, err)
		assert.True(t, resp.Accepted)
	})

	t.Run("missing apply_id", func(t *testing.T) {
		_, err := client.Start(t.Context(), &ternv1.StartRequest{})
		require.Error(t, err)
	})
}

func TestGRPCClient_Revert(t *testing.T) {
	client, cleanup := testClient(t, &mockTernServer{})
	defer cleanup()

	t.Run("valid request", func(t *testing.T) {
		resp, err := client.Revert(t.Context(), &ternv1.RevertRequest{
			ApplyId:     "apply-rev123",
			Environment: "staging",
		})
		require.NoError(t, err)
		assert.True(t, resp.Accepted)
	})
}

func TestGRPCClient_SkipRevert(t *testing.T) {
	client, cleanup := testClient(t, &mockTernServer{})
	defer cleanup()

	t.Run("valid request", func(t *testing.T) {
		resp, err := client.SkipRevert(t.Context(), &ternv1.SkipRevertRequest{
			ApplyId:     "apply-skip123",
			Environment: "staging",
		})
		require.NoError(t, err)
		assert.True(t, resp.Accepted)
	})
}

func TestGRPCClient_Close(t *testing.T) {
	client, cleanup := testClient(t, &mockTernServer{})
	defer cleanup()

	err := client.Close()
	require.NoError(t, err)
}

// capturingTernServer records the apply_id from Start and Progress requests.
// progressState is returned only when progressStateSet is true; otherwise the
// server defaults to STATE_COMPLETED.
type capturingTernServer struct {
	ternv1.UnimplementedTernServer
	mu                sync.Mutex
	applyReq          *ternv1.ApplyRequest
	applyErr          error
	remoteApplyID     string
	remoteOperationID string
	cancelApplyID     string
	cancelCaller      string
	stopApplyID       string
	stopCaller        string
	startApplyID      string
	cutoverApplyID    string
	cutoverCaller     string
	skipRevertApplyID string
	revertApplyID     string
	progressApplyID   string
	progressReq       *ternv1.ProgressRequest
	progressState     ternv1.State // state returned by Progress; 0 = STATE_COMPLETED
	progressStateSet  bool
	progressStates    []ternv1.State
	progressTables    []*ternv1.TableProgress
	progressTableSets [][]*ternv1.TableProgress
	progressError     string
	progressSettled   []*ternv1.SettledControlRequest
	progressErr       error
	startErr          error
	stopErr           error
	cancelErr         error
	cutoverErr        error
	cutoverAccepted   bool
	cutoverMessage    string
	startCalled       bool // tracks whether Start was actually invoked
	stopCalls         int
	cancelCalls       int
	stopRefusal       string // when set, Stop answers Accepted=false with this reason
	cancelRefusal     string // when set, Cancel answers Accepted=false with this reason
	omitOperationKey  bool   // emulate a data plane that does not echo the operation key
}

// dispatchOperationKeyEcho mirrors the data plane's operation key derivation
// for the dispatch shapes these tests exercise, minus the stored-plan
// validation a real data plane performs before answering.
func dispatchOperationKeyEcho(req *ternv1.ApplyRequest) string {
	if len(req.TargetShards) == 1 {
		changes, err := scopedDispatchDDLChanges(req.DdlChanges)
		if err != nil {
			return ""
		}
		key, err := shardScopedDispatchOperationKey(changes, req.TargetShards[0])
		if err != nil {
			return ""
		}
		return key
	}
	if namespaces := vschemaOnlyDispatchNamespaces(req.DdlChanges); len(namespaces) > 0 {
		if len(namespaces) == 1 {
			return namespaces[0] + finalizerOperationKeySuffix
		}
		return finalizerDeploymentScopedKey
	}
	return ""
}

func (s *capturingTernServer) Apply(_ context.Context, req *ternv1.ApplyRequest) (*ternv1.ApplyResponse, error) {
	s.mu.Lock()
	s.applyReq = req
	applyID := s.remoteApplyID
	if applyID == "" {
		applyID = "remote-apply-123"
	}
	operationID := s.remoteOperationID
	omitOperationKey := s.omitOperationKey
	err := s.applyErr
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	operationKey := ""
	if !omitOperationKey {
		operationKey = dispatchOperationKeyEcho(req)
	}
	return &ternv1.ApplyResponse{Accepted: true, ApplyId: applyID, ApplyOperationId: operationID, OperationKey: operationKey}, nil
}

func (s *capturingTernServer) Start(_ context.Context, req *ternv1.StartRequest) (*ternv1.StartResponse, error) {
	s.mu.Lock()
	s.startApplyID = req.ApplyId
	s.startCalled = true
	err := s.startErr
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	// After Start succeeds, transition to COMPLETED so the poller exits.
	s.progressState = ternv1.State_STATE_COMPLETED
	s.mu.Unlock()
	return &ternv1.StartResponse{Accepted: true}, nil
}

func (s *capturingTernServer) Stop(_ context.Context, req *ternv1.StopRequest) (*ternv1.StopResponse, error) {
	s.mu.Lock()
	s.stopApplyID = req.ApplyId
	s.stopCaller = req.Caller
	s.stopCalls++
	err := s.stopErr
	refusal := s.stopRefusal
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if refusal != "" {
		return &ternv1.StopResponse{Accepted: false, ErrorMessage: refusal}, nil
	}
	return &ternv1.StopResponse{Accepted: true}, nil
}

func (s *capturingTernServer) Cancel(_ context.Context, req *ternv1.CancelRequest) (*ternv1.CancelResponse, error) {
	s.mu.Lock()
	s.cancelApplyID = req.ApplyId
	s.cancelCaller = req.Caller
	s.cancelCalls++
	err := s.cancelErr
	refusal := s.cancelRefusal
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if refusal != "" {
		return &ternv1.CancelResponse{Accepted: false, ErrorMessage: refusal}, nil
	}
	return &ternv1.CancelResponse{Accepted: true}, nil
}

func (s *capturingTernServer) Cutover(_ context.Context, req *ternv1.CutoverRequest) (*ternv1.CutoverResponse, error) {
	s.mu.Lock()
	s.cutoverApplyID = req.ApplyId
	s.cutoverCaller = req.Caller
	err := s.cutoverErr
	accepted := s.cutoverAccepted
	message := s.cutoverMessage
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &ternv1.CutoverResponse{Accepted: accepted, ErrorMessage: message}, nil
}

func (s *capturingTernServer) SkipRevert(_ context.Context, req *ternv1.SkipRevertRequest) (*ternv1.SkipRevertResponse, error) {
	s.mu.Lock()
	s.skipRevertApplyID = req.ApplyId
	s.mu.Unlock()
	return &ternv1.SkipRevertResponse{Accepted: true}, nil
}

func (s *capturingTernServer) Revert(_ context.Context, req *ternv1.RevertRequest) (*ternv1.RevertResponse, error) {
	s.mu.Lock()
	s.revertApplyID = req.ApplyId
	s.mu.Unlock()
	return &ternv1.RevertResponse{Accepted: true}, nil
}

func (s *capturingTernServer) Progress(_ context.Context, req *ternv1.ProgressRequest) (*ternv1.ProgressResponse, error) {
	s.mu.Lock()
	s.progressReq = &ternv1.ProgressRequest{
		ApplyId:     req.ApplyId,
		Environment: req.Environment,
	}
	s.progressApplyID = req.ApplyId
	ps := s.progressState
	psSet := s.progressStateSet
	if len(s.progressStates) > 0 {
		ps = s.progressStates[0]
		s.progressStates = s.progressStates[1:]
		psSet = true
	}
	tables := s.progressTables
	if len(s.progressTableSets) > 0 {
		tables = s.progressTableSets[0]
		s.progressTableSets = s.progressTableSets[1:]
	}
	errorMessage := s.progressError
	settled := s.progressSettled
	err := s.progressErr
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if !psSet {
		ps = ternv1.State_STATE_COMPLETED
	}
	return &ternv1.ProgressResponse{
		State:                  ps,
		Tables:                 tables,
		ErrorMessage:           errorMessage,
		SettledControlRequests: settled,
	}, nil
}
func (s *capturingTernServer) Logs(context.Context, *ternv1.LogsRequest) (*ternv1.LogsResponse, error) {
	return &ternv1.LogsResponse{}, nil
}

func (s *capturingTernServer) getStartApplyID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.startApplyID
}

func (s *capturingTernServer) startWasCalled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.startCalled
}

func (s *capturingTernServer) getStopApplyID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopApplyID
}

func (s *capturingTernServer) getCancelApplyID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cancelApplyID
}

func (s *capturingTernServer) getCutoverCaller() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cutoverCaller
}

func (s *capturingTernServer) getStopCaller() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopCaller
}

func (s *capturingTernServer) getCancelCaller() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cancelCaller
}

func (s *capturingTernServer) getStopCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopCalls
}

func (s *capturingTernServer) getCancelCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cancelCalls
}

func (s *capturingTernServer) getSkipRevertApplyID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.skipRevertApplyID
}

func (s *capturingTernServer) getRevertApplyID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.revertApplyID
}

func (s *capturingTernServer) getCutoverApplyID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cutoverApplyID
}

func (s *capturingTernServer) getProgressApplyID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.progressApplyID
}

func (s *capturingTernServer) getProgressRequest() *ternv1.ProgressRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.progressReq == nil {
		return nil
	}
	return &ternv1.ProgressRequest{
		ApplyId:     s.progressReq.ApplyId,
		Environment: s.progressReq.Environment,
	}
}

func (s *capturingTernServer) getApplyRequest() *ternv1.ApplyRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applyReq
}

// mockApplyStore is a minimal ApplyStore for testing ResumeApply.
type mockApplyStore struct {
	storage.ApplyStore
	apply           *storage.Apply
	updateErr       error
	updates         []*storage.Apply
	revertSkippedAt *time.Time
}

func (m *mockApplyStore) SetRevertSkipped(_ context.Context, _ int64, at time.Time) error {
	m.revertSkippedAt = &at
	return nil
}

func (m *mockApplyStore) GetByApplyIdentifier(context.Context, string) (*storage.Apply, error) {
	if m.apply == nil {
		return nil, nil
	}
	apply := *m.apply
	return &apply, nil
}

func (m *mockApplyStore) Get(context.Context, int64) (*storage.Apply, error) {
	if m.apply == nil {
		return nil, nil
	}
	apply := *m.apply
	return &apply, nil
}
func (m *mockApplyStore) Update(_ context.Context, apply *storage.Apply) error {
	if m.updateErr != nil {
		return m.updateErr
	}
	stored := *apply
	m.apply = &stored
	m.updates = append(m.updates, &stored)
	return nil
}
func (m *mockApplyStore) Heartbeat(context.Context, int64) error               { return nil }
func (m *mockApplyStore) CheckLease(context.Context, storage.ApplyLease) error { return nil }

// mockTaskStore is a minimal TaskStore for testing pollForCompletion.
type mockTaskStore struct {
	storage.TaskStore
	tasks               []*storage.Task
	getTask             *storage.Task
	getErr              error
	getByApplyIDErr     error
	getByOperationIDErr error
	updateErr           error
	lastOperationID     int64
	updated             []*storage.Task
	upsertedShards      []*storage.Task
	upsertShardErr      error
}

func (m *mockTaskStore) Get(context.Context, string) (*storage.Task, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	return m.getTask, nil
}

func (m *mockTaskStore) GetByApplyID(context.Context, int64) ([]*storage.Task, error) {
	if m.getByApplyIDErr != nil {
		return nil, m.getByApplyIDErr
	}
	return m.tasks, nil
}

func (m *mockTaskStore) CountByApplyID(context.Context, int64) (int64, error) {
	return int64(len(m.tasks)), nil
}

func (m *mockTaskStore) GetByApplyOperationID(_ context.Context, applyOperationID int64) ([]*storage.Task, error) {
	m.lastOperationID = applyOperationID
	if m.getByOperationIDErr != nil {
		return nil, m.getByOperationIDErr
	}
	// The real store returns a non-nil empty slice when an operation owns no
	// tasks, and callers are entitled to rely on that.
	scoped := make([]*storage.Task, 0, len(m.tasks))
	for _, task := range m.tasks {
		if task.ApplyOperationID != nil && *task.ApplyOperationID == applyOperationID {
			scoped = append(scoped, task)
		}
	}
	return scoped, nil
}
func (m *mockTaskStore) Update(_ context.Context, task *storage.Task) error {
	if m.updateErr != nil {
		return m.updateErr
	}
	stored := *task
	m.updated = append(m.updated, &stored)
	return nil
}

func (m *mockTaskStore) UpsertShardProgress(_ context.Context, task *storage.Task) error {
	if m.upsertShardErr != nil {
		return m.upsertShardErr
	}
	stored := *task
	m.upsertedShards = append(m.upsertedShards, &stored)
	return nil
}

type mockApplyLogStore struct {
	storage.ApplyLogStore
	logs        []*storage.ApplyLog
	recentLimit int
}

// Append records the line the way the real store does: a cancelled context is
// a failed write, not a silently dropped one.
func (m *mockApplyLogStore) Append(ctx context.Context, log *storage.ApplyLog) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("append apply log for apply %d: %w", log.ApplyID, err)
	}
	stored := *log
	m.logs = append(m.logs, &stored)
	return nil
}

func (m *mockApplyLogStore) GetByApply(context.Context, int64) ([]*storage.ApplyLog, error) {
	return m.logs, nil
}

func (m *mockApplyLogStore) GetRecentByApply(_ context.Context, _ int64, limit int) ([]*storage.ApplyLog, error) {
	m.recentLimit = limit
	return m.logs, nil
}

type mockPlanStore struct {
	storage.PlanStore
	plan *storage.Plan
}

func (m *mockPlanStore) GetByID(context.Context, int64) (*storage.Plan, error) {
	return m.plan, nil
}

// mockStorage wires together the mock stores.
type mockStorage struct {
	storage.Storage
	applies         *mockApplyStore
	tasks           *mockTaskStore
	plans           *mockPlanStore
	logs            *mockApplyLogStore
	controlRequests *testControlRequestStore
	operations      *mockApplyOperationStore
}

// mockApplyOperationStore is an in-memory ApplyOperationStore for the remote
// drive tests. It backs the operation lookups (Get/ListByApply) and the per-op
// remote identifier writes that the fan-out drive uses.
type mockApplyOperationStore struct {
	storage.ApplyOperationStore
	ops          map[int64]*storage.ApplyOperation
	saveErr      error
	savedResumes []*storage.EngineResumeState
}

func (m *mockApplyOperationStore) Get(_ context.Context, id int64) (*storage.ApplyOperation, error) {
	op, ok := m.ops[id]
	if !ok {
		return nil, storage.ErrApplyOperationNotFound
	}
	return op, nil
}

func (m *mockApplyOperationStore) ListByApply(_ context.Context, applyID int64) ([]*storage.ApplyOperation, error) {
	var ops []*storage.ApplyOperation
	for _, op := range m.ops {
		if op != nil && op.ApplyID == applyID {
			ops = append(ops, op)
		}
	}
	return ops, nil
}

func (m *mockApplyOperationStore) ListByApplies(_ context.Context, applyIDs []int64) ([]*storage.ApplyOperation, error) {
	applyIDSet := make(map[int64]bool, len(applyIDs))
	for _, applyID := range applyIDs {
		applyIDSet[applyID] = true
	}
	var ops []*storage.ApplyOperation
	for _, op := range m.ops {
		if op != nil && applyIDSet[op.ApplyID] {
			ops = append(ops, op)
		}
	}
	return ops, nil
}

func (m *mockApplyOperationStore) UpdateState(_ context.Context, id int64, newState string) error {
	op, ok := m.ops[id]
	if !ok {
		return storage.ErrApplyOperationNotFound
	}
	op.State = newState
	return nil
}

func (m *mockApplyOperationStore) MarkTerminal(_ context.Context, id int64, newState string) error {
	op, ok := m.ops[id]
	if !ok {
		return storage.ErrApplyOperationNotFound
	}
	now := time.Now()
	op.State = newState
	op.CompletedAt = &now
	return nil
}

func (m *mockApplyOperationStore) MarkCompleted(_ context.Context, id int64) error {
	op, ok := m.ops[id]
	if !ok {
		return storage.ErrApplyOperationNotFound
	}
	now := time.Now()
	op.State = state.ApplyOperation.Completed
	op.CompletedAt = &now
	return nil
}

func (m *mockApplyOperationStore) MarkFailed(_ context.Context, id int64, errMsg string) error {
	op, ok := m.ops[id]
	if !ok {
		return storage.ErrApplyOperationNotFound
	}
	now := time.Now()
	op.State = state.ApplyOperation.Failed
	op.ErrorMessage = errMsg
	op.CompletedAt = &now
	return nil
}

func (m *mockApplyOperationStore) SaveExternalOperationID(_ context.Context, operationID int64, externalOperationID string) error {
	if m.saveErr != nil {
		return m.saveErr
	}
	op, ok := m.ops[operationID]
	if !ok {
		return storage.ErrApplyOperationNotFound
	}
	op.ExternalOperationID = externalOperationID
	return nil
}

func (m *mockApplyOperationStore) SaveExternalID(_ context.Context, applyID, operationID int64, externalID string) error {
	if m.saveErr != nil {
		return m.saveErr
	}
	op, ok := m.ops[operationID]
	if !ok {
		return storage.ErrApplyOperationNotFound
	}
	if op.ApplyID != applyID {
		return fmt.Errorf("apply_operation %d does not belong to apply %d: %w", operationID, applyID, storage.ErrApplyOperationNotFound)
	}
	op.ExternalID = externalID
	return nil
}

func (m *mockApplyOperationStore) SaveEngineResumeState(_ context.Context, operationID int64, resumeState *storage.EngineResumeState) error {
	if m.saveErr != nil {
		return m.saveErr
	}
	op, ok := m.ops[operationID]
	if !ok {
		return storage.ErrApplyOperationNotFound
	}
	op.EngineResumeContext = resumeState.MigrationContext
	op.EngineResumeMetadata = resumeState.Metadata
	m.savedResumes = append(m.savedResumes, resumeState)
	return nil
}

// For a remote PlanetScale apply the engine runs in the data plane, so the
// control plane mirrors the progress display fields (deploy-request URL, VSchema)
// onto the operation's engine_resume_metadata — the source the PR comment reads.
// The mirror must preserve a multi-operation drive's remote-apply-id context,
// resolve the operation for a whole-apply (single-operation) drive, skip
// redundant writes, and stay a no-op for non-PlanetScale applies.
func TestGRPCClientMirrorsRemoteDisplayMetadata(t *testing.T) {
	display := map[string]string{
		"deploy_request_url": "https://app.planetscale.com/org/db/deploy-requests/106",
		"branch_name":        "schemabot-db",
		"revert_expires_at":  "2026-06-29T18:30:00Z",
	}

	t.Run("multi-operation preserves the remote-apply-id context", func(t *testing.T) {
		ops := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
			7: {ID: 7, ApplyID: 100, EngineResumeContext: "remote-apply-xyz"},
		}}
		client := &GRPCClient{storage: &mockStorage{operations: ops}}
		apply := &storage.Apply{ID: 100, ApplyIdentifier: "apply-x", Engine: storage.EnginePlanetScale}
		scope := applyTaskScope{applyOperationID: 7, operation: ops.ops[7], multiOperation: true}

		blob := client.mirrorRemoteDisplayMetadata(t.Context(), apply, scope, display, "")
		require.NotEmpty(t, blob)

		got, err := PSDisplayMetadata(ops.ops[7].EngineResumeMetadata)
		require.NoError(t, err)
		assert.Equal(t, "https://app.planetscale.com/org/db/deploy-requests/106", got["deploy_request_url"])
		assert.Equal(t, "2026-06-29T18:30:00Z", got["revert_expires_at"], "the revert-window deadline must survive the mirror so the PR comment can show the countdown")
		assert.Equal(t, "remote-apply-xyz", ops.ops[7].EngineResumeContext, "the remote apply id must survive the display-metadata write")

		// A second poll with the same metadata skips the redundant write.
		before := len(ops.savedResumes)
		blob2 := client.mirrorRemoteDisplayMetadata(t.Context(), apply, scope, display, blob)
		assert.Equal(t, blob, blob2)
		assert.Len(t, ops.savedResumes, before, "unchanged metadata must not re-write")
	})

	t.Run("whole-apply drive resolves the sole operation", func(t *testing.T) {
		ops := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
			9: {ID: 9, ApplyID: 200},
		}}
		client := &GRPCClient{storage: &mockStorage{operations: ops}}
		apply := &storage.Apply{ID: 200, ApplyIdentifier: "apply-y", Engine: storage.EnginePlanetScale}

		blob := client.mirrorRemoteDisplayMetadata(t.Context(), apply, wholeApplyTaskScope(), display, "")
		require.NotEmpty(t, blob)
		got, err := PSDisplayMetadata(ops.ops[9].EngineResumeMetadata)
		require.NoError(t, err)
		assert.Equal(t, "https://app.planetscale.com/org/db/deploy-requests/106", got["deploy_request_url"])
	})

	t.Run("non-PlanetScale apply is a no-op", func(t *testing.T) {
		ops := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{9: {ID: 9, ApplyID: 200}}}
		client := &GRPCClient{storage: &mockStorage{operations: ops}}
		apply := &storage.Apply{ID: 200, Engine: storage.EngineSpirit}
		assert.Empty(t, client.mirrorRemoteDisplayMetadata(t.Context(), apply, wholeApplyTaskScope(), display, ""))
		assert.Empty(t, ops.savedResumes)
	})

	// If the operation can't be loaded, the mirror must skip the write rather than
	// persist with an empty context — clobbering the remote apply id would break
	// resuming the remote apply.
	t.Run("skips the write when the operation cannot be loaded", func(t *testing.T) {
		ops := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{}} // op 7 absent
		client := &GRPCClient{storage: &mockStorage{operations: ops}}
		apply := &storage.Apply{ID: 100, ApplyIdentifier: "apply-x", Engine: storage.EnginePlanetScale}
		scope := applyTaskScope{applyOperationID: 7, multiOperation: true}

		blob := client.mirrorRemoteDisplayMetadata(t.Context(), apply, scope, display, "")
		assert.Empty(t, blob, "a failed operation load returns the unchanged lastBlob")
		assert.Empty(t, ops.savedResumes, "no write when the context can't be preserved")
	})
}

func (m *mockStorage) Applies() storage.ApplyStore {
	if m.applies != nil {
		return m.applies
	}
	return &mockApplyStore{}
}
func (m *mockStorage) Tasks() storage.TaskStore {
	if m.tasks != nil {
		return m.tasks
	}
	return &mockTaskStore{}
}
func (m *mockStorage) Plans() storage.PlanStore {
	if m.plans != nil {
		return m.plans
	}
	return &mockPlanStore{}
}
func (m *mockStorage) ApplyLogs() storage.ApplyLogStore {
	if m.logs != nil {
		return m.logs
	}
	return &mockApplyLogStore{}
}
func (m *mockStorage) ControlRequests() storage.ControlRequestStore {
	if m.controlRequests != nil {
		return m.controlRequests
	}
	return &testControlRequestStore{}
}
func (m *mockStorage) ApplyOperations() storage.ApplyOperationStore {
	if m.operations != nil {
		return m.operations
	}
	return &mockApplyOperationStore{}
}

func testCapturingGRPCClient(t *testing.T, server *capturingTernServer) (*GRPCClient, func()) {
	t.Helper()

	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "localhost:0")
	require.NoError(t, err, "failed to listen")

	grpcServer := grpc.NewServer()
	ternv1.RegisterTernServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err, "failed to dial")

	client := &GRPCClient{
		conn:    conn,
		client:  ternv1.NewTernClient(conn),
		storage: &mockStorage{},
	}
	cleanup := func() {
		utils.CloseAndLog(client)
		grpcServer.Stop()
		utils.CloseAndLog(lis)
	}
	return client, cleanup
}

func TestGRPCClient_ResumeApplyDispatchesQueuedRemoteApply(t *testing.T) {
	// Operator claims start with a stored control-plane apply row and pending
	// tasks but no external_id. ResumeApply dispatches the queued work to
	// remote Tern, stores the returned data-plane ID, then polls it to terminal.
	server := &capturingTernServer{
		remoteApplyID: "remote-dispatched-123",
		progressTables: []*ternv1.TableProgress{{
			Namespace:       "default",
			TableName:       "users",
			Status:          state.Task.Completed,
			PercentComplete: 100,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-control-queued",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{
		Target:       "testdb-target",
		DeferCutover: true,
	})
	task := &storage.Task{
		ID:             11,
		TaskIdentifier: "task-users",
		ApplyID:        apply.ID,
		TableName:      "users",
		DDL:            "ALTER TABLE users ADD COLUMN email varchar(255)",
		DDLAction:      "alter",
		Namespace:      "default",
		State:          state.Task.Pending,
	}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-remote-queued",
		}},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.ResumeApply(ctx, apply)
	require.NoError(t, err)

	assert.Equal(t, "remote-dispatched-123", apply.ExternalID)
	assert.Equal(t, state.Apply.Completed, apply.State)
	require.NotNil(t, apply.StartedAt)
	require.NotNil(t, apply.CompletedAt)

	req := server.getApplyRequest()
	require.NotNil(t, req, "expected queued apply to be dispatched to remote Tern")
	assert.Equal(t, "plan-remote-queued", req.PlanId)
	assert.Equal(t, "testdb", req.Database)
	assert.Equal(t, "testdb-target", req.Target)
	assert.Equal(t, "true", req.Options["defer_cutover"])
	require.Len(t, req.DdlChanges, 1)
	assert.Equal(t, "users", req.DdlChanges[0].TableName)
	assert.Equal(t, ternv1.ChangeType_CHANGE_TYPE_ALTER, req.DdlChanges[0].ChangeType)
	assert.Equal(t, "remote-dispatched-123", server.getProgressApplyID())
	progressReq := server.getProgressRequest()
	require.NotNil(t, progressReq)
	assert.Equal(t, "remote-dispatched-123", progressReq.ApplyId)
	assert.Equal(t, "staging", progressReq.Environment)
}

// A shard work operation (key "namespace/shard/table") whose tasks carry no
// shard must fail closed at the control plane with a clear error, rather than
// dispatching an empty TargetShards that the data plane rejects opaquely as
// "expected exactly one target shard, got 0". This makes a version/data skew
// self-diagnosing instead of surfacing as a confusing data-plane failure.
func TestGRPCClient_ResumeApplyOperationFailsClosedOnShardOpMissingShard(t *testing.T) {
	server := &capturingTernServer{remoteApplyID: "remote-skew"}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID: 7, ApplyIdentifier: "apply-skew", PlanID: 99, Database: "cdb_resolute",
		DatabaseType: storage.DatabaseTypeStrata, Environment: "staging", State: state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "cdb-resolute-target"})
	operationID := int64(42)
	// The task is missing its shard — the skew this guard catches.
	task := &storage.Task{
		ID: 11, TaskIdentifier: "task-mutes", ApplyID: apply.ID, ApplyOperationID: &operationID,
		TableName: "mutes", Shard: "", Namespace: "cdb_resolute_sharded",
		DDL: "ALTER TABLE mutes ADD INDEX created_at (created_at)", DDLAction: "alter", State: state.Task.Pending,
	}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
		plans:   &mockPlanStore{plan: &storage.Plan{ID: apply.PlanID, PlanIdentifier: "plan-skew"}},
		operations: &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
			operationID: {ID: operationID, ApplyID: apply.ID, Deployment: "cdb-resolute-deployment", OperationKey: "cdb_resolute_sharded/-40/mutes", State: state.ApplyOperation.Pending},
		}},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.ResumeApplyOperation(ctx, apply, operationID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected exactly one target shard")
	assert.Nil(t, server.getApplyRequest(), "must not dispatch to the data plane when the shard scope is missing")
}

func TestGRPCClient_ResumeApplyOperationDispatchesScopedTasks(t *testing.T) {
	// An operator driver resumes a single apply_operation over the remote path.
	// The drive loads tasks scoped to that operation (GetByApplyOperationID) and
	// dispatches only those, never widening to the whole apply's tasks.
	server := &capturingTernServer{
		remoteApplyID: "remote-op-dispatched-1",
		progressTables: []*ternv1.TableProgress{{
			Namespace:       "default",
			TableName:       "users",
			Status:          state.Task.Completed,
			PercentComplete: 100,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-op-scoped",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "testdb-target", DeferCutover: true})
	operationID := int64(42)
	task := &storage.Task{
		ID:               11,
		TaskIdentifier:   "task-users",
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TableName:        "users",
		Shard:            "-80",
		DDL:              "ALTER TABLE users ADD COLUMN email varchar(255)",
		DDLAction:        "alter",
		Namespace:        "default",
		State:            state.Task.Pending,
	}
	// Fail any whole-apply task load so the test proves the drive stays scoped to
	// the operation rather than falling back to GetByApplyID.
	taskStore := &mockTaskStore{
		tasks:           []*storage.Task{task},
		getByApplyIDErr: errors.New("whole-apply task load must not be used for operation-scoped resume"),
	}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   taskStore,
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-op-scoped",
		}},
		// The real task query joins a work operation to its tasks on
		// operation_key = namespace/shard/table_name, so the operation carries the
		// key its task derives — a keyless operation owns no task rows.
		operations: &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
			operationID: {
				ID: operationID, ApplyID: apply.ID, Deployment: "testdb-deployment",
				OperationKind: storage.ApplyOperationKindWork, OperationKey: "default/-80/users",
				State: state.ApplyOperation.Pending,
			},
		}},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.ResumeApplyOperation(ctx, apply, operationID)
	require.NoError(t, err)

	assert.Equal(t, operationID, taskStore.lastOperationID)
	assert.Equal(t, "remote-op-dispatched-1", apply.ExternalID)
	assert.Equal(t, state.Apply.Completed, apply.State)

	req := server.getApplyRequest()
	require.NotNil(t, req, "expected operation-scoped apply to be dispatched to remote Tern")
	require.Len(t, req.DdlChanges, 1)
	assert.Equal(t, "users", req.DdlChanges[0].TableName)
	assert.Equal(t, []string{"-80"}, req.TargetShards)
}

// A deployment applied per shard dispatches each shard's operation to the
// remote data plane under one deployment-keyed idempotency key, so the data
// plane lands every sibling in the deployment's single data-plane apply: the
// first dispatch creates it, each later sibling attaches its own operation.
// Both operation rows record that one remote apply id, and the parent apply's
// external_id stays untouched — one deployment, one data-plane apply.
func TestGRPCClient_SiblingShardOperationsRecordOneRemoteApply(t *testing.T) {
	server := &capturingTernServer{
		remoteApplyID: "remote-shared-1",
		progressTables: []*ternv1.TableProgress{{
			Namespace:       "commerce",
			TableName:       "users",
			Status:          state.Task.Completed,
			PercentComplete: 100,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-sharded",
		PlanID:          99,
		Database:        "commerce",
		DatabaseType:    storage.DatabaseTypeStrata,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "commerce-target"})
	opA, opB := int64(41), int64(42)
	taskA := &storage.Task{
		ID: 11, TaskIdentifier: "task-users-a", ApplyID: apply.ID, ApplyOperationID: &opA,
		TableName: "users", Shard: "-80", Namespace: "commerce",
		DDL: "ALTER TABLE users ADD COLUMN email varchar(255)", DDLAction: "alter", State: state.Task.Pending,
	}
	taskB := &storage.Task{
		ID: 12, TaskIdentifier: "task-users-b", ApplyID: apply.ID, ApplyOperationID: &opB,
		TableName: "users", Shard: "80-", Namespace: "commerce",
		DDL: "ALTER TABLE users ADD COLUMN email varchar(255)", DDLAction: "alter", State: state.Task.Pending,
	}
	operationStore := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
		opA: {ID: opA, ApplyID: apply.ID, Deployment: "commerce-deployment", OperationKey: "commerce/-80/users", OperationKind: storage.ApplyOperationKindWork, State: state.ApplyOperation.Pending},
		opB: {ID: opB, ApplyID: apply.ID, Deployment: "commerce-deployment", OperationKey: "commerce/80-/users", OperationKind: storage.ApplyOperationKindWork, State: state.ApplyOperation.Pending},
	}}
	client.storage = &mockStorage{
		applies:    &mockApplyStore{apply: apply},
		tasks:      &mockTaskStore{tasks: []*storage.Task{taskA, taskB}},
		plans:      &mockPlanStore{plan: &storage.Plan{ID: apply.PlanID, PlanIdentifier: "plan-sharded"}},
		operations: operationStore,
	}

	// Each dispatch is poll-driven and costs at least one progress tick, so
	// give every drive its own deadline rather than sharing one across both.
	driveCtx := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(t.Context(), 2*time.Second)
	}

	firstCtx, cancelFirst := driveCtx()
	defer cancelFirst()
	require.NoError(t, client.ResumeApplyOperation(firstCtx, apply, opA))
	firstReq := server.getApplyRequest()
	require.NotNil(t, firstReq, "the first shard operation must dispatch to the data plane")
	assert.Equal(t, []string{"-80"}, firstReq.TargetShards)
	assert.Equal(t, "remote-shared-1", operationStore.ops[opA].ExternalID)

	secondCtx, cancelSecond := driveCtx()
	defer cancelSecond()
	require.NoError(t, client.ResumeApplyOperation(secondCtx, apply, opB))
	secondReq := server.getApplyRequest()
	require.NotNil(t, secondReq, "the sibling shard operation must dispatch to the data plane")
	assert.Equal(t, []string{"80-"}, secondReq.TargetShards)

	require.NotEmpty(t, firstReq.IdempotencyKey)
	assert.Equal(t, firstReq.IdempotencyKey, secondReq.IdempotencyKey,
		"sibling dispatches must carry the deployment-keyed idempotency key so the data plane attaches them into one apply")

	// The idempotency key routes a sibling into the shared apply; the generation
	// manifest is what makes that apply wait for the siblings still to come, so
	// both dispatches must declare the deployment's whole operation set.
	expectedManifest := []string{"commerce/-80/users", "commerce/80-/users"}
	assert.Equal(t, expectedManifest, firstReq.GenerationOperationKeys,
		"the dispatch that creates the shared apply must declare every sibling it will wait for")
	assert.Equal(t, expectedManifest, secondReq.GenerationOperationKeys,
		"an attaching sibling must declare the same generation manifest so the data plane can verify agreement")

	assert.Equal(t, "remote-shared-1", operationStore.ops[opA].ExternalID)
	assert.Equal(t, "remote-shared-1", operationStore.ops[opB].ExternalID,
		"every operation of the deployment must record the deployment's one remote apply id")
	assert.Empty(t, apply.ExternalID,
		"a multi-operation dispatch must not write the parent apply external_id")
}

func TestGRPCClient_ResumeApplyOperationDispatchesGroupFinalizerAsVSchemaOnly(t *testing.T) {
	// A task-less group_finalizer operation is driven over the remote path by
	// dispatching the namespace VSchema as a VSchema-only apply (no DDL, no target
	// shards) rather than failing closed on the empty task set. The data plane
	// applies it via its task-less VSchema-only path.
	server := &capturingTernServer{remoteApplyID: "remote-finalizer-1"} // default Progress = COMPLETED
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              8,
		ApplyIdentifier: "apply-finalizer",
		PlanID:          99,
		Database:        "commerce",
		DatabaseType:    storage.DatabaseTypeStrata,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "commerce-target"})
	operationID := int64(51)
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   &mockTaskStore{getByApplyIDErr: errors.New("finalizer drive must not load tasks")},
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-finalizer",
			SchemaFiles: schema.SchemaFiles{
				"commerce": {Files: map[string]string{storage.VSchemaArtifactName: `{"sharded":true}`}},
			},
			Namespaces: map[string]*storage.NamespacePlanData{
				"commerce": {Artifacts: map[string]string{storage.VSchemaArtifactName: `{"sharded":true}`}},
			},
		}},
		operations: &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
			operationID: {
				ID:            operationID,
				ApplyID:       apply.ID,
				Deployment:    "commerce-deployment",
				OperationKey:  "commerce/group_finalizer",
				OperationKind: storage.ApplyOperationKindGroupFinalizer,
				State:         state.ApplyOperation.Pending,
			},
		}},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.ResumeApplyOperation(ctx, apply, operationID)
	require.NoError(t, err, "a group_finalizer must dispatch, not fail closed on no tasks")

	req := server.getApplyRequest()
	require.NotNil(t, req, "expected the finalizer to dispatch a VSchema apply to remote Tern")
	require.Len(t, req.DdlChanges, 1)
	assert.Equal(t, ternv1.ChangeType_CHANGE_TYPE_VSCHEMA, req.DdlChanges[0].ChangeType)
	assert.Equal(t, "commerce", req.DdlChanges[0].Namespace)
	assert.Empty(t, req.TargetShards, "a namespace VSchema apply targets no shard")
	assert.Contains(t, req.SchemaFiles, "commerce", "the vschema.json must be carried for a materializing remote")
}

func TestGRPCClient_ResumeApplyOperationDispatchesDeploymentScopedGroupFinalizer(t *testing.T) {
	// A VSchema-only apply is shaped as one deployment-scoped group_finalizer
	// (operation key "group_finalizer", no namespace prefix). Its remote drive
	// dispatches every VSchema-changed namespace in the plan as one VSchema-only
	// apply, because a branch-based engine stands up one branch covering the whole
	// deployment and validates every keyspace in it.
	server := &capturingTernServer{remoteApplyID: "remote-finalizer-deployment-1"} // default Progress = COMPLETED
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              8,
		ApplyIdentifier: "apply-finalizer-deployment",
		PlanID:          99,
		Database:        "commerce",
		DatabaseType:    storage.DatabaseTypeVitess,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "commerce-target"})
	operationID := int64(51)
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   &mockTaskStore{getByApplyIDErr: errors.New("finalizer drive must not load tasks")},
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-finalizer-deployment",
			SchemaFiles: schema.SchemaFiles{
				"commerce":         {Files: map[string]string{storage.VSchemaArtifactName: `{"sharded":true}`}},
				"commerce_unshard": {Files: map[string]string{storage.VSchemaArtifactName: `{"tables":{}}`}},
			},
			Namespaces: map[string]*storage.NamespacePlanData{
				"commerce":         {Artifacts: map[string]string{storage.VSchemaArtifactName: `{"sharded":true}`}},
				"commerce_unshard": {Artifacts: map[string]string{storage.VSchemaArtifactName: `{"tables":{}}`}},
			},
		}},
		operations: &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
			operationID: {
				ID:            operationID,
				ApplyID:       apply.ID,
				Deployment:    "commerce-deployment",
				OperationKey:  "group_finalizer",
				OperationKind: storage.ApplyOperationKindGroupFinalizer,
				State:         state.ApplyOperation.Pending,
			},
		}},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.ResumeApplyOperation(ctx, apply, operationID)
	require.NoError(t, err, "a deployment-scoped group_finalizer must dispatch every VSchema-changed namespace")

	req := server.getApplyRequest()
	require.NotNil(t, req, "expected the finalizer to dispatch a VSchema apply to remote Tern")
	require.Len(t, req.DdlChanges, 2, "one VSchema change per VSchema-changed namespace, in one dispatch")
	gotNamespaces := []string{req.DdlChanges[0].Namespace, req.DdlChanges[1].Namespace}
	assert.ElementsMatch(t, []string{"commerce", "commerce_unshard"}, gotNamespaces)
	for _, change := range req.DdlChanges {
		assert.Equal(t, ternv1.ChangeType_CHANGE_TYPE_VSCHEMA, change.ChangeType)
	}
	assert.Empty(t, req.TargetShards, "a VSchema apply targets no shard")
	assert.Contains(t, req.SchemaFiles, "commerce", "the vschema.json must be carried for a materializing remote")
	assert.Contains(t, req.SchemaFiles, "commerce_unshard")
}

func TestGRPCClient_ResumeApplyOperationFailsClosedOnMalformedFinalizerKey(t *testing.T) {
	// A group_finalizer key is either namespace-scoped ("<ns>/group_finalizer") or
	// the deployment-scoped "group_finalizer". Anything else is a malformed row;
	// the drive must fail closed before dispatching a scope it cannot name.
	server := &capturingTernServer{remoteApplyID: "remote-finalizer-malformed-1"}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              8,
		ApplyIdentifier: "apply-finalizer-malformed",
		PlanID:          99,
		Database:        "commerce",
		DatabaseType:    storage.DatabaseTypeStrata,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "commerce-target"})
	operationID := int64(51)
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   &mockTaskStore{getByApplyIDErr: errors.New("finalizer drive must not load tasks")},
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-finalizer-malformed",
			Namespaces: map[string]*storage.NamespacePlanData{
				"commerce": {Artifacts: map[string]string{storage.VSchemaArtifactName: `{"sharded":true}`}},
			},
		}},
		operations: &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
			operationID: {
				ID:            operationID,
				ApplyID:       apply.ID,
				Deployment:    "commerce-deployment",
				OperationKey:  "banana",
				OperationKind: storage.ApplyOperationKindGroupFinalizer,
				State:         state.ApplyOperation.Pending,
			},
		}},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.ResumeApplyOperation(ctx, apply, operationID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "malformed operation key")
	assert.Nil(t, server.getApplyRequest(), "a malformed finalizer key must not reach the data plane")
}

func TestGRPCClient_ResumeApplyOperationDispatchesTasklessVSchemaOnlyWork(t *testing.T) {
	// A VSchema-only change carries no DDL, so its apply gets one whole-deployment
	// work operation and no task rows. The remote drive must dispatch that
	// operation's namespaces as a VSchema-only apply — every VSchema-changing
	// namespace in one dispatch, since the data plane deploys the whole branch as
	// one operation — rather than reading the empty task set as a stale claim and
	// failing the apply before it reaches the data plane.
	server := &capturingTernServer{remoteApplyID: "remote-vschema-only-1"} // default Progress = COMPLETED
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              9,
		ApplyIdentifier: "apply-vschema-only",
		PlanID:          77,
		Database:        "commerce",
		DatabaseType:    storage.DatabaseTypeVitess,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "commerce-target"})
	operationID := int64(61)
	vschema := `{"sharded":true,"vindexes":{"xxhash":{"type":"xxhash"}}}`
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   &mockTaskStore{},
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-vschema-only",
			SchemaFiles: schema.SchemaFiles{
				"commerce":         {Files: map[string]string{storage.VSchemaArtifactName: vschema}},
				"commerce_sharded": {Files: map[string]string{storage.VSchemaArtifactName: vschema}},
			},
			Namespaces: map[string]*storage.NamespacePlanData{
				"commerce":         {Artifacts: map[string]string{storage.VSchemaArtifactName: vschema}},
				"commerce_sharded": {Artifacts: map[string]string{storage.VSchemaArtifactName: vschema}},
			},
		}},
		operations: &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
			operationID: {
				ID:            operationID,
				ApplyID:       apply.ID,
				Deployment:    "commerce-deployment",
				OperationKind: storage.ApplyOperationKindWork,
				State:         state.ApplyOperation.Pending,
			},
		}},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.ResumeApplyOperation(ctx, apply, operationID)
	require.NoError(t, err, "a task-less VSchema-only work operation must dispatch, not fail closed on no tasks")

	req := server.getApplyRequest()
	require.NotNil(t, req, "expected the work operation to dispatch a VSchema apply to remote Tern")
	require.Len(t, req.DdlChanges, 2, "every VSchema-changing namespace travels in one dispatch")
	assert.Equal(t, ternv1.ChangeType_CHANGE_TYPE_VSCHEMA, req.DdlChanges[0].ChangeType)
	assert.Equal(t, "commerce", req.DdlChanges[0].Namespace)
	assert.Equal(t, ternv1.ChangeType_CHANGE_TYPE_VSCHEMA, req.DdlChanges[1].ChangeType)
	assert.Equal(t, "commerce_sharded", req.DdlChanges[1].Namespace)
	assert.Empty(t, req.TargetShards, "a namespace VSchema apply targets no shard")
	assert.Contains(t, req.SchemaFiles, "commerce", "the vschema.json must be carried for a materializing remote")
	assert.Contains(t, req.SchemaFiles, "commerce_sharded")
}

func TestGRPCClient_TasklessVSchemaOnlyWorkSettlesOnItsOperationRow(t *testing.T) {
	// The operator claims an operation under an operation lease and no parent
	// apply lease, whether or not the apply has siblings. Such a drive owns its
	// operation row and nothing else: it records the remote apply id there so a
	// later claim resumes the dispatch instead of repeating it, writes the
	// terminal outcome there so the rollout projection can settle the parent, and
	// never writes the parent applies row — a write storage refuses, which would
	// end the drive and strand the apply non-terminal with its target blocked.
	// A task-less operation makes the last part load-bearing: with no task rows,
	// the operation row is the only place the outcome can land.
	server := &capturingTernServer{remoteApplyID: "remote-vschema-only-settle"} // default Progress = COMPLETED
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              11,
		ApplyIdentifier: "apply-vschema-only-settle",
		PlanID:          79,
		Database:        "commerce",
		DatabaseType:    storage.DatabaseTypeVitess,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "commerce-target"})
	operationID := int64(63)
	vschema := `{"sharded":true,"vindexes":{"xxhash":{"type":"xxhash"}}}`
	// The stored row is a separate value from the one the drive carries, so a
	// reload during the drive returns what storage holds rather than the drive's
	// own in-memory progress.
	storedApply := *apply
	applies := &mockApplyStore{apply: &storedApply}
	operations := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
		operationID: {
			ID:            operationID,
			ApplyID:       apply.ID,
			Deployment:    "commerce-deployment",
			OperationKind: storage.ApplyOperationKindWork,
			State:         state.ApplyOperation.Pending,
		},
	}}
	client.storage = &mockStorage{
		applies: applies,
		tasks:   &mockTaskStore{},
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-vschema-only-settle",
			SchemaFiles: schema.SchemaFiles{
				"commerce": {Files: map[string]string{storage.VSchemaArtifactName: vschema}},
			},
			Namespaces: map[string]*storage.NamespacePlanData{
				"commerce": {Artifacts: map[string]string{storage.VSchemaArtifactName: vschema}},
			},
		}},
		operations: operations,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	ctx = storage.WithOperationLease(ctx, storage.OperationLease{
		ApplyID:     apply.ID,
		OperationID: operationID,
		Owner:       "host/1/driver-0",
		Token:       "operation-token",
	})
	require.NoError(t, client.ResumeApplyOperation(ctx, apply, operationID))

	require.NotNil(t, server.getApplyRequest(), "expected the work operation to dispatch a VSchema apply to remote Tern")

	op := operations.ops[operationID]
	assert.Equal(t, "remote-vschema-only-settle", op.ExternalID,
		"the remote apply id belongs on the operation row, or a later claim dispatches the work a second time")
	assert.True(t, state.IsState(op.State, state.ApplyOperation.Completed),
		"the operation row carries the terminal outcome, but is %q", op.State)
	assert.Empty(t, applies.updates,
		"the drive holds no parent apply lease, so the parent state is the rollout projection's to move")
}

func TestGRPCClient_ResumeApplyOperationFailsClosedOnTasklessWorkWithDDLPlan(t *testing.T) {
	// The task-less dispatch is gated on the plan carrying nothing but VSchema. A
	// work operation whose plan has DDL should have produced task rows, so an
	// empty task set is an invalid or stale claim: fail closed before any dispatch
	// or state mutation rather than sending the data plane an apply with no work.
	server := &capturingTernServer{remoteApplyID: "remote-must-not-dispatch"}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              10,
		ApplyIdentifier: "apply-stale-claim",
		PlanID:          78,
		Database:        "commerce",
		DatabaseType:    storage.DatabaseTypeVitess,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	operationID := int64(62)
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   &mockTaskStore{},
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-with-ddl",
			Namespaces: map[string]*storage.NamespacePlanData{
				"commerce": {
					Tables:    []storage.TableChange{{Table: "games", DDL: "ALTER TABLE games ADD COLUMN played_at timestamp NULL", Operation: "alter"}},
					Artifacts: map[string]string{storage.VSchemaArtifactName: `{"sharded":true}`},
				},
			},
		}},
		operations: &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
			operationID: {
				ID:            operationID,
				ApplyID:       apply.ID,
				Deployment:    "commerce-deployment",
				OperationKind: storage.ApplyOperationKindWork,
				State:         state.ApplyOperation.Pending,
			},
		}},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.ResumeApplyOperation(ctx, apply, operationID)
	require.ErrorIs(t, err, ErrNoTasksForApplyOperation)
	assert.Nil(t, server.getApplyRequest(), "must not dispatch a task-less work operation whose plan carries DDL")
}

func TestGRPCClient_ResumeApplyOperationReflectsFinalizerTerminalStateOntoOperationRow(t *testing.T) {
	// A task-less group_finalizer in a multi-operation apply carries no task rows,
	// so the operator's task-derived projection can never move its operation row.
	// The remote drive must therefore reflect the remote terminal state onto the
	// operation row itself, or the operation is stranded in pending and the
	// operator retries forever (and a terminal failure is silently lost).
	cases := []struct {
		name        string
		progress    ternv1.State
		progressSet bool
		wantState   string
	}{
		{name: "completed", wantState: state.ApplyOperation.Completed}, // default Progress = COMPLETED
		{name: "failed", progress: ternv1.State_STATE_FAILED, progressSet: true, wantState: state.ApplyOperation.Failed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := &capturingTernServer{
				remoteApplyID:    "remote-finalizer-terminal",
				progressState:    tc.progress,
				progressStateSet: tc.progressSet,
			}
			client, cleanup := testCapturingGRPCClient(t, server)
			defer cleanup()

			apply := &storage.Apply{
				ID:              8,
				ApplyIdentifier: "apply-finalizer-terminal",
				PlanID:          99,
				Database:        "commerce",
				DatabaseType:    storage.DatabaseTypeStrata,
				Environment:     "staging",
				State:           state.Apply.Pending,
			}
			apply.SetOptions(storage.ApplyOptions{Target: "commerce-target"})
			// The drive mutates its in-memory parent apply but, for a multi-operation
			// scope, must not persist it (the operator owns parent state). Store an
			// independent copy so the terminal reload reflects the persisted pending
			// parent, not the drive's in-memory mutation.
			storedParent := *apply
			finalizerID := int64(51)
			siblingID := int64(52) // a second operation makes this a multi-operation apply
			operations := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
				finalizerID: {
					ID:            finalizerID,
					ApplyID:       apply.ID,
					Deployment:    "commerce-deployment",
					OperationKey:  "commerce/group_finalizer",
					OperationKind: storage.ApplyOperationKindGroupFinalizer,
					State:         state.ApplyOperation.Pending,
				},
				siblingID: {
					ID:           siblingID,
					ApplyID:      apply.ID,
					Deployment:   "commerce-deployment",
					OperationKey: "commerce/-80/mutes",
					State:        state.ApplyOperation.Completed,
				},
			}}
			client.storage = &mockStorage{
				applies: &mockApplyStore{apply: &storedParent},
				tasks:   &mockTaskStore{getByApplyIDErr: errors.New("finalizer drive must not load tasks")},
				plans: &mockPlanStore{plan: &storage.Plan{
					ID:             apply.PlanID,
					PlanIdentifier: "plan-finalizer",
					SchemaFiles: schema.SchemaFiles{
						"commerce": {Files: map[string]string{storage.VSchemaArtifactName: `{"sharded":true}`}},
					},
					Namespaces: map[string]*storage.NamespacePlanData{
						"commerce": {Artifacts: map[string]string{storage.VSchemaArtifactName: `{"sharded":true}`}},
					},
				}},
				operations: operations,
			}

			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			// Reaching a terminal remote state — success or failure — is a successful
			// drive: the outcome lives in the persisted operation state, not a Go
			// error (mirroring the non-suppressed terminal path).
			require.NoError(t, client.ResumeApplyOperation(ctx, apply, finalizerID))

			assert.Equal(t, tc.wantState, operations.ops[finalizerID].State,
				"the finalizer operation row must reflect the remote terminal state, not stay pending")
			assert.NotNil(t, operations.ops[finalizerID].CompletedAt, "a terminal operation row is stamped completed_at")
		})
	}
}

func TestGRPCClient_ResumeApplyOperationDispatchParksBarrierCutoverRemotely(t *testing.T) {
	// On a multi-deployment apply under the barrier cutover policy, the remote
	// copy drive must instruct Tern to park at the cutover barrier (defer_cutover)
	// so the deployment-ordered cutover claim can drive each operation's swap in
	// turn. The apply itself was not started with manual --defer-cutover, so the
	// instruction is the per-operation automatic barrier decision, derived at
	// dispatch and never persisted onto the shared apply.
	server := &capturingTernServer{
		remoteApplyID: "remote-op-barrier-1",
		progressTables: []*ternv1.TableProgress{{
			Namespace:       "default",
			TableName:       "users",
			Status:          state.Task.Completed,
			PercentComplete: 100,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-barrier",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "testdb-target"})
	operationID := int64(42)
	siblingID := int64(43)
	task := &storage.Task{
		ID:               11,
		TaskIdentifier:   "task-users",
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TableName:        "users",
		DDL:              "ALTER TABLE users ADD COLUMN email varchar(255)",
		DDLAction:        "alter",
		Namespace:        "default",
		State:            state.Task.Pending,
	}
	taskStore := &mockTaskStore{
		tasks:           []*storage.Task{task},
		getByApplyIDErr: errors.New("whole-apply task load must not be used for operation-scoped resume"),
	}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   taskStore,
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-barrier",
		}},
		operations: &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
			operationID: {ID: operationID, ApplyID: apply.ID, Deployment: "west", State: state.ApplyOperation.Pending, CutoverPolicy: storage.CutoverPolicyBarrier},
			siblingID:   {ID: siblingID, ApplyID: apply.ID, Deployment: "east", State: state.ApplyOperation.Pending, CutoverPolicy: storage.CutoverPolicyBarrier},
		}},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.ResumeApplyOperation(ctx, apply, operationID)
	require.NoError(t, err)

	req := server.getApplyRequest()
	require.NotNil(t, req, "expected barrier operation to be dispatched to remote Tern")
	assert.Equal(t, "true", req.Options["defer_cutover"],
		"a multi-op barrier operation must dispatch with defer_cutover so the remote engine parks at the cutover barrier")
	assert.False(t, apply.GetOptions().DeferCutover,
		"the automatic barrier decision must not be persisted onto the shared apply options")
}

func TestGRPCClient_ResumeApplyOperationDispatchDoesNotDeferRollingCutover(t *testing.T) {
	// A multi-deployment apply under the default rolling policy serializes copy
	// and cutover per deployment, so its copy drive must NOT defer cutover — the
	// barrier park is specific to the barrier policy.
	server := &capturingTernServer{
		remoteApplyID: "remote-op-rolling-1",
		progressTables: []*ternv1.TableProgress{{
			Namespace:       "default",
			TableName:       "users",
			Status:          state.Task.Completed,
			PercentComplete: 100,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-rolling",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "testdb-target"})
	operationID := int64(42)
	siblingID := int64(43)
	task := &storage.Task{
		ID:               11,
		TaskIdentifier:   "task-users",
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TableName:        "users",
		DDL:              "ALTER TABLE users ADD COLUMN email varchar(255)",
		DDLAction:        "alter",
		Namespace:        "default",
		State:            state.Task.Pending,
	}
	taskStore := &mockTaskStore{
		tasks:           []*storage.Task{task},
		getByApplyIDErr: errors.New("whole-apply task load must not be used for operation-scoped resume"),
	}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   taskStore,
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-rolling",
		}},
		operations: &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
			operationID: {ID: operationID, ApplyID: apply.ID, Deployment: "west", State: state.ApplyOperation.Pending, CutoverPolicy: storage.CutoverPolicyRolling},
			siblingID:   {ID: siblingID, ApplyID: apply.ID, Deployment: "east", State: state.ApplyOperation.Pending, CutoverPolicy: storage.CutoverPolicyRolling},
		}},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.NoError(t, client.ResumeApplyOperation(ctx, apply, operationID))

	req := server.getApplyRequest()
	require.NotNil(t, req)
	assert.NotEqual(t, "true", req.Options["defer_cutover"],
		"a rolling-policy operation must not defer cutover at dispatch")
}

func TestGRPCClient_ResumeApplyOperationStoresRemoteIDOnOperationForMultiOpApply(t *testing.T) {
	// On a multi-deployment apply, each deployment gets its own remote Tern apply
	// id and remote apply_operation id. Dispatching one operation must store the
	// remote apply id on that operation's external_id, store the remote
	// operation id separately for operator UX, and must NOT touch the parent
	// applies.external_id, which has no single authoritative value across
	// deployments.
	server := &capturingTernServer{
		remoteApplyID:     "remote-op-west-1",
		remoteOperationID: "remote-operation-west-1",
		progressTables: []*ternv1.TableProgress{{
			Namespace:       "default",
			TableName:       "users",
			Status:          state.Task.Completed,
			PercentComplete: 100,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-multi-op",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "testdb-target", DeferCutover: true})
	operationID := int64(42)
	siblingID := int64(43)
	task := &storage.Task{
		ID:               11,
		TaskIdentifier:   "task-users",
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TableName:        "users",
		DDL:              "ALTER TABLE users ADD COLUMN email varchar(255)",
		DDLAction:        "alter",
		Namespace:        "default",
		State:            state.Task.Pending,
	}
	taskStore := &mockTaskStore{
		tasks:           []*storage.Task{task},
		getByApplyIDErr: errors.New("whole-apply task load must not be used for operation-scoped resume"),
	}
	operationStore := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
		operationID: {ID: operationID, ApplyID: apply.ID, Deployment: "west", State: state.ApplyOperation.Pending},
		siblingID:   {ID: siblingID, ApplyID: apply.ID, Deployment: "east", State: state.ApplyOperation.Pending},
	}}
	applyStore := &mockApplyStore{apply: apply}
	client.storage = &mockStorage{
		applies: applyStore,
		tasks:   taskStore,
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-multi-op",
		}},
		operations: operationStore,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.ResumeApplyOperation(ctx, apply, operationID)
	require.NoError(t, err)

	assert.Empty(t, applyStore.updates, "a multi-op drive must never write the parent applies row directly; parent state is owned by the projection CAS")
	assert.Empty(t, apply.ExternalID, "multi-op dispatch must not write the parent apply external_id")
	assert.Equal(t, "remote-op-west-1", operationStore.ops[operationID].ExternalID,
		"the remote apply id must be stored on the claimed operation")
	assert.Empty(t, operationStore.ops[operationID].EngineResumeContext,
		"new remote dispatches must not store data-plane apply ids in opaque engine resume context")
	assert.Equal(t, "remote-operation-west-1", operationStore.ops[operationID].ExternalOperationID,
		"the remote apply_operation id must be stored on the claimed operation")
	assert.Empty(t, operationStore.ops[siblingID].ExternalID,
		"the sibling operation's remote id must be untouched")
	assert.Empty(t, operationStore.ops[siblingID].ExternalOperationID,
		"the sibling operation's remote operation id must be untouched")
	assert.Empty(t, operationStore.savedResumes)

	req := server.getApplyRequest()
	require.NotNil(t, req)
	require.Len(t, req.DdlChanges, 1)
	assert.Equal(t, "users", req.DdlChanges[0].TableName)
}

func TestGRPCClient_ResumeApplyOperationRefusesExternalOperationIDMismatch(t *testing.T) {
	server := &capturingTernServer{
		remoteApplyID:     "remote-op-west-1",
		remoteOperationID: "remote-operation-west-2",
		progressTables: []*ternv1.TableProgress{{
			Namespace:       "default",
			TableName:       "users",
			Status:          state.Task.Completed,
			PercentComplete: 100,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-multi-op-mismatch",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "testdb-target"})
	operationID := int64(42)
	task := &storage.Task{
		ID:               11,
		TaskIdentifier:   "task-users",
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TableName:        "users",
		DDL:              "ALTER TABLE users ADD COLUMN email varchar(255)",
		DDLAction:        "alter",
		Namespace:        "default",
		State:            state.Task.Pending,
	}
	operationStore := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
		operationID: {
			ID:                  operationID,
			ApplyID:             apply.ID,
			Deployment:          "west",
			State:               state.ApplyOperation.Pending,
			ExternalOperationID: "remote-operation-west-1",
		},
		43: {ID: 43, ApplyID: apply.ID, Deployment: "east", State: state.ApplyOperation.Pending},
	}}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks: &mockTaskStore{
			tasks:           []*storage.Task{task},
			getByApplyIDErr: errors.New("whole-apply task load must not be used for operation-scoped resume"),
		},
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-multi-op-mismatch",
		}},
		operations: operationStore,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.ResumeApplyOperation(ctx, apply, operationID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already has remote apply_operation id")
	assert.Equal(t, "remote-operation-west-1", operationStore.ops[operationID].ExternalOperationID)
}

func TestGRPCClient_ResumeApplyOperationStartsQueuedRemoteWithoutWritingParent(t *testing.T) {
	// A multi-deployment operation that was already dispatched (its remote apply
	// id lives on the operation) and whose parent apply is still pending must
	// start its own remote apply by the operation's id and must NOT write the
	// parent applies row: parent state is owned by the operator's projection CAS.
	server := &capturingTernServer{
		progressTables: []*ternv1.TableProgress{{
			Namespace:       "default",
			TableName:       "users",
			Status:          state.Task.Completed,
			PercentComplete: 100,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-multi-op-start",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "testdb-target"})
	operationID := int64(42)
	siblingID := int64(43)
	task := &storage.Task{
		ID:               11,
		TaskIdentifier:   "task-users",
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TableName:        "users",
		DDL:              "ALTER TABLE users ADD COLUMN email varchar(255)",
		DDLAction:        "alter",
		Namespace:        "default",
		State:            state.Task.Pending,
	}
	taskStore := &mockTaskStore{
		tasks:           []*storage.Task{task},
		getByApplyIDErr: errors.New("whole-apply task load must not be used for operation-scoped resume"),
	}
	operationStore := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
		operationID: {ID: operationID, ApplyID: apply.ID, Deployment: "west", State: state.ApplyOperation.Pending, EngineResumeContext: "remote-op-west-1"},
		siblingID:   {ID: siblingID, ApplyID: apply.ID, Deployment: "east", State: state.ApplyOperation.Pending},
	}}
	applyStore := &mockApplyStore{apply: apply}
	client.storage = &mockStorage{
		applies: applyStore,
		tasks:   taskStore,
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-multi-op-start",
		}},
		operations: operationStore,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.NoError(t, client.ResumeApplyOperation(ctx, apply, operationID))

	assert.Equal(t, "remote-op-west-1", server.getStartApplyID(),
		"the operation's own remote apply id must be started, not the parent external_id")
	assert.Empty(t, applyStore.updates,
		"a multi-op drive must never write the parent applies row directly; parent state is owned by the projection CAS")
}

func TestGRPCClient_ResumeApplyOperationStopsUndispatchedOperationWithoutCompletingApplyStop(t *testing.T) {
	// A multi-deployment apply has one durable stop request shared by every
	// deployment. Stopping a claimed-but-undispatched operation (no remote apply
	// id yet) must terminalize only that operation, leave the parent apply
	// untouched, and keep the apply-level stop request pending so sibling
	// deployments that already dispatched still observe the stop.
	server := &capturingTernServer{}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-multi-op-stop",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	operationID := int64(42)
	siblingID := int64(43)
	task := &storage.Task{
		ID:               11,
		TaskIdentifier:   "task-users",
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TableName:        "users",
		State:            state.Task.Running,
	}
	operationStore := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
		operationID: {ID: operationID, ApplyID: apply.ID, Deployment: "west", State: state.ApplyOperation.Running},
		siblingID:   {ID: siblingID, ApplyID: apply.ID, Deployment: "east", State: state.ApplyOperation.Running, EngineResumeContext: "remote-east"},
	}}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: apply},
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
		operations:      operationStore,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.ResumeApplyOperation(ctx, apply, operationID)
	require.NoError(t, err)

	assert.Empty(t, server.getStopApplyID(), "no remote stop should be sent for an undispatched operation")
	assert.Equal(t, state.Task.Stopped, task.State, "the operation's task should be stopped")
	assert.Equal(t, state.ApplyOperation.Stopped, operationStore.ops[operationID].State, "the claimed operation should be stopped")
	assert.Equal(t, state.Apply.Running, apply.State, "the parent apply must not be terminalized by one undispatched operation")
	assert.Equal(t, "remote-east", operationStore.ops[siblingID].EngineResumeContext, "the sibling's remote id must be untouched")

	stopReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
	require.NoError(t, err)
	assert.NotNil(t, stopReq, "the apply-level stop request must remain pending for sibling operations")
}

func TestGRPCClient_ResumeApplyCancelsUndispatchedApply(t *testing.T) {
	// An apply cancelled before it was ever dispatched has no remote apply to
	// address. The drive settles it in control-plane storage — tasks and apply
	// row cancelled, the durable request completed — sends nothing to the data
	// plane, and returns cleanly so the drive does not re-claim a driver.
	server := &capturingTernServer{}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              5,
		ApplyIdentifier: "apply-undispatched-cancel",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	task := &storage.Task{
		ID:             11,
		TaskIdentifier: "task-users",
		ApplyID:        apply.ID,
		TableName:      "users",
		State:          state.Task.Pending,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: apply},
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.NoError(t, client.ResumeApply(ctx, apply))

	assert.Empty(t, server.getCancelApplyID(), "no remote cancel should be sent for an undispatched apply")
	assert.Empty(t, server.getApplyRequest(), "an apply cancelled before dispatch must never be dispatched")
	assert.Equal(t, state.Apply.Cancelled, apply.State)
	assert.Equal(t, state.Task.Cancelled, task.State)

	cancelReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	assert.Nil(t, cancelReq, "the cancel request must be completed, not left pending for the next claim")
}

func TestGRPCClient_ResumeApplyOperationCancelsUndispatchedOperationWithoutCompletingApplyCancel(t *testing.T) {
	// A multi-deployment apply has one durable cancel request shared by every
	// deployment. Cancelling a claimed-but-undispatched operation must
	// terminalize only that operation, leave the parent apply untouched, and keep
	// the apply-level cancel request pending so sibling deployments that already
	// dispatched still observe the cancel.
	server := &capturingTernServer{}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-multi-op-cancel",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	operationID := int64(42)
	siblingID := int64(43)
	task := &storage.Task{
		ID:               11,
		TaskIdentifier:   "task-users",
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TableName:        "users",
		State:            state.Task.Running,
	}
	operationStore := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
		operationID: {ID: operationID, ApplyID: apply.ID, Deployment: "west", State: state.ApplyOperation.Running},
		siblingID:   {ID: siblingID, ApplyID: apply.ID, Deployment: "east", State: state.ApplyOperation.Running, EngineResumeContext: "remote-east"},
	}}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: apply},
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
		operations:      operationStore,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.NoError(t, client.ResumeApplyOperation(ctx, apply, operationID))

	assert.Empty(t, server.getCancelApplyID(), "no remote cancel should be sent for an undispatched operation")
	assert.Equal(t, state.Task.Cancelled, task.State, "the operation's task should be cancelled")
	assert.Equal(t, state.ApplyOperation.Cancelled, operationStore.ops[operationID].State, "the claimed operation should be cancelled")
	assert.Equal(t, state.Apply.Running, apply.State, "the parent apply must not be terminalized by one undispatched operation")
	assert.Equal(t, "remote-east", operationStore.ops[siblingID].EngineResumeContext, "the sibling's remote id must be untouched")

	cancelReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	assert.NotNil(t, cancelReq, "the apply-level cancel request must remain pending for sibling operations")
}

func TestGRPCClient_ResumeApplyCancelsApplyStoppedBeforeDispatch(t *testing.T) {
	// An apply stopped before it was ever dispatched is stopped with no remote
	// apply id, and none is ever recorded after the stop, so a later cancel has
	// no remote work to address. The drive settles the cancel in control-plane
	// storage — tasks and apply row cancelled, the durable request completed —
	// and sends nothing to the data plane: no dispatch of work the operator
	// already stopped, and no probe against an empty remote apply id that would
	// fail the apply instead of cancelling it.
	server := &capturingTernServer{}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              5,
		ApplyIdentifier: "apply-stopped-cancel",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Stopped,
	}
	task := &storage.Task{
		ID:             11,
		TaskIdentifier: "task-users",
		ApplyID:        apply.ID,
		TableName:      "users",
		State:          state.Task.Stopped,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: apply},
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.NoError(t, client.ResumeApply(ctx, apply))

	assert.Empty(t, server.getCancelApplyID(), "no remote cancel should be sent for an apply stopped before dispatch")
	assert.Empty(t, server.getApplyRequest(), "an apply stopped before dispatch must never be dispatched by its cancel")
	assert.Equal(t, state.Apply.Cancelled, apply.State, "the stopped apply settles as cancelled, not failed by a probe it cannot answer")
	assert.Equal(t, state.Task.Cancelled, task.State)

	cancelReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	assert.Nil(t, cancelReq, "the cancel request must be completed, not left pending")
}

func TestGRPCClient_ResumeApplyOperationCancelsOperationStoppedBeforeDispatch(t *testing.T) {
	// One deployment of a multi-deployment apply was stopped before its first
	// dispatch, so its operation row is stopped with no remote apply id and none
	// is ever recorded after the stop. A later cancel settles that operation
	// locally — its task and row cancelled — while the parent apply, the
	// sibling's remote work, and the shared apply-level cancel request are
	// untouched, so sibling deployments that did dispatch still observe the
	// command.
	server := &capturingTernServer{}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-multi-op-stopped-cancel",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	operationID := int64(42)
	siblingID := int64(43)
	task := &storage.Task{
		ID:               11,
		TaskIdentifier:   "task-users",
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TableName:        "users",
		State:            state.Task.Stopped,
	}
	operationStore := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
		operationID: {ID: operationID, ApplyID: apply.ID, Deployment: "west", State: state.ApplyOperation.Stopped},
		siblingID:   {ID: siblingID, ApplyID: apply.ID, Deployment: "east", State: state.ApplyOperation.Running, EngineResumeContext: "remote-east"},
	}}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: apply},
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
		operations:      operationStore,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.NoError(t, client.ResumeApplyOperation(ctx, apply, operationID))

	assert.Empty(t, server.getCancelApplyID(), "no remote cancel should be sent for an operation stopped before dispatch")
	assert.Equal(t, state.Task.Cancelled, task.State, "the operation's task should be cancelled")
	assert.Equal(t, state.ApplyOperation.Cancelled, operationStore.ops[operationID].State, "the stopped operation should be cancelled")
	assert.Equal(t, state.Apply.Running, apply.State, "the parent apply must not be terminalized by one stopped operation")
	assert.Equal(t, "remote-east", operationStore.ops[siblingID].EngineResumeContext, "the sibling's remote id must be untouched")

	cancelReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	assert.NotNil(t, cancelReq, "the apply-level cancel request must remain pending for sibling operations")
}

func TestGRPCClient_ResumeApplyAmbiguousDispatchResolvesPendingCancel(t *testing.T) {
	// An apply that is running with no remote apply id may or may not have
	// created a remote apply whose dispatch response was lost, so a pending
	// cancel cannot be satisfied locally without risking an orphan. The drive
	// leaves the cancel to the dispatch ambiguity guard, which fails the apply
	// closed and fails the pending cancel with the same message — a failed apply
	// is never claimed again, so nothing later could answer the request, and the
	// operator must see the rejection rather than a command pending forever.
	server := &capturingTernServer{}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              9,
		ApplyIdentifier: "apply-ambiguous-cancel",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:             11,
		TaskIdentifier: "task-users",
		ApplyID:        apply.ID,
		TableName:      "users",
		State:          state.Task.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: apply},
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	err := client.ResumeApply(ctx, apply)
	require.Error(t, err, "the ambiguity guard fails the apply closed rather than cancelling work it cannot account for")
	assert.Empty(t, server.getCancelApplyID(), "no remote cancel is addressable without a remote apply id")
	assert.Equal(t, state.Apply.Failed, apply.State)

	cancelReq, err := controlRequests.GetByOperation(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	require.NotNil(t, cancelReq)
	assert.Equal(t, storage.ControlRequestFailed, cancelReq.Status, "the guard must fail the pending cancel so the operator sees the rejection")
	assert.Contains(t, cancelReq.ErrorMessage, "remote dispatch state is ambiguous")
}

func TestGRPCClient_ResumeApplyAmbiguousDispatchResolvesPendingStop(t *testing.T) {
	// A stop carries the same orphan risk as a cancel: an apply running with no
	// remote apply id may already have a remote apply the control plane cannot
	// see, and settling the stop locally would report the change stopped while it
	// ran on to cutover — with the apply left terminal, nothing would ever
	// revisit it. The drive leaves the stop to the dispatch ambiguity guard,
	// which fails the apply closed and fails the pending stop with the same
	// message, since a failed apply is never claimed again.
	server := &capturingTernServer{}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              9,
		ApplyIdentifier: "apply-ambiguous-stop",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:             11,
		TaskIdentifier: "task-users",
		ApplyID:        apply.ID,
		TableName:      "users",
		State:          state.Task.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: apply},
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	err := client.ResumeApply(ctx, apply)
	require.Error(t, err, "the ambiguity guard fails the apply closed rather than reporting a stop it cannot account for")
	assert.Empty(t, server.getStopApplyID(), "no remote stop is addressable without a remote apply id")
	assert.Equal(t, state.Apply.Failed, apply.State, "the apply must not be recorded stopped while remote work may still be running")
	assert.Equal(t, state.Task.Failed, task.State)

	stopReq, err := controlRequests.GetByOperation(t.Context(), apply.ID, storage.ControlOperationStop)
	require.NoError(t, err)
	require.NotNil(t, stopReq)
	assert.Equal(t, storage.ControlRequestFailed, stopReq.Status, "the guard must fail the pending stop so the operator sees the rejection")
	assert.Contains(t, stopReq.ErrorMessage, "remote dispatch state is ambiguous")
}

func TestGRPCClient_ResumeApplyAmbiguousDispatchResolvesPendingCancelAndStop(t *testing.T) {
	// An operator who cancels an unresponsive apply and then stops it leaves both
	// requests pending. The drive processes cancel first, so a stop path that
	// settled locally would take over from the unhandled cancel and route around
	// the dispatch ambiguity guard entirely. Both requests must defer to the
	// guard, and the guard must fail both once it has failed the apply closed.
	server := &capturingTernServer{}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              9,
		ApplyIdentifier: "apply-ambiguous-cancel-stop",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:             11,
		TaskIdentifier: "task-users",
		ApplyID:        apply.ID,
		TableName:      "users",
		State:          state.Task.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{
		{ApplyID: apply.ID, Operation: storage.ControlOperationCancel, Status: storage.ControlRequestPending, RequestedBy: "cli:alice"},
		{ApplyID: apply.ID, Operation: storage.ControlOperationStop, Status: storage.ControlRequestPending, RequestedBy: "cli:alice"},
	}}
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: apply},
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	err := client.ResumeApply(ctx, apply)
	require.Error(t, err, "a second pending request must not carry the drive past the ambiguity guard")
	assert.Equal(t, state.Apply.Failed, apply.State)
	assert.Empty(t, server.getCancelApplyID())
	assert.Empty(t, server.getStopApplyID())

	for _, op := range []storage.ControlOperation{storage.ControlOperationCancel, storage.ControlOperationStop} {
		req, err := controlRequests.GetByOperation(t.Context(), apply.ID, op)
		require.NoError(t, err)
		require.NotNilf(t, req, "the %s request must exist", op)
		assert.Equalf(t, storage.ControlRequestFailed, req.Status, "the guard must fail the pending %s request so the operator sees the rejection", op)
		assert.Contains(t, req.ErrorMessage, "remote dispatch state is ambiguous")
	}
}

func TestGRPCClient_ResumeApplyOperationAmbiguousDispatchLeavesApplyCancelPending(t *testing.T) {
	// An operation recorded past its first dispatch with no remote apply id is
	// the ambiguous lost-dispatch shape, and the guard fails that operation
	// closed. The apply-level cancel request is shared across deployments, so
	// the guard must leave it pending: a sibling with its own remote apply still
	// needs to observe the command, and the operator projection settles the
	// request once the parent apply derives terminal.
	server := &capturingTernServer{}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-multi-op-ambiguous-cancel",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	operationID := int64(42)
	siblingID := int64(43)
	task := &storage.Task{
		ID:               11,
		TaskIdentifier:   "task-users",
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TableName:        "users",
		State:            state.Task.Running,
	}
	operationStore := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
		operationID: {ID: operationID, ApplyID: apply.ID, Deployment: "west", State: state.ApplyOperation.CatchingUp},
		siblingID:   {ID: siblingID, ApplyID: apply.ID, Deployment: "east", State: state.ApplyOperation.Running, EngineResumeContext: "remote-east"},
	}}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: apply},
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
		operations:      operationStore,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	err := client.ResumeApplyOperation(ctx, apply, operationID)
	require.Error(t, err, "the ambiguity guard fails the operation closed rather than cancelling work it cannot account for")
	assert.Empty(t, server.getCancelApplyID(), "no remote cancel is addressable without a remote apply id")

	cancelReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	assert.NotNil(t, cancelReq, "the apply-level cancel request must remain pending for sibling operations")
}

func TestGRPCClient_ResumeApplyOperationStopReachingTerminalLeavesApplyStopPending(t *testing.T) {
	// A multi-deployment apply has one durable stop request shared by every
	// deployment. When a dispatched operation observes its own remote apply
	// reaching a terminal state, it must NOT complete the apply-level stop
	// request: sibling deployments still in flight need to keep observing the
	// stop. The rollout projection completes the shared request once the
	// aggregate settles.
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_COMPLETED,
		progressStateSet: true,
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-multi-op-stop-terminal",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	operationID := int64(42)
	siblingID := int64(43)
	task := &storage.Task{
		ID:               11,
		TaskIdentifier:   "task-users",
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TableName:        "users",
		State:            state.Task.Running,
	}
	operationStore := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
		operationID: {ID: operationID, ApplyID: apply.ID, Deployment: "west", State: state.ApplyOperation.Running, EngineResumeContext: "remote-op-west"},
		siblingID:   {ID: siblingID, ApplyID: apply.ID, Deployment: "east", State: state.ApplyOperation.Running, EngineResumeContext: "remote-east"},
	}}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	// Keep the stored parent row a distinct copy so mutating the in-memory apply
	// during the drive does not leak a terminal state into stored reads.
	storedApply := *apply
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: &storedApply},
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
		operations:      operationStore,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.NoError(t, client.ResumeApplyOperation(ctx, apply, operationID))

	assert.Equal(t, "remote-op-west", server.getStopApplyID(),
		"the operation's own remote apply id must be stopped, not the parent external_id")
	assert.Equal(t, state.Apply.Running, apply.State,
		"one operation reaching terminal must not leak its terminal remote state onto the shared parent apply")

	stopReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
	require.NoError(t, err)
	assert.NotNil(t, stopReq, "the apply-level stop request must remain pending for sibling operations after one operation terminalizes")
}

// The control plane's immediate stop or cancel attempt carries the operator's
// name, but when that attempt fails the durable request is what reaches the data
// plane — forwarded later by the driver holding the apply. The data plane records
// the requester from the RPC, so the driver has to carry the same name the
// operator's command was queued under, or the row it stores names the forwarding
// path and the PR notice credits an internal caller for a person's command.
func TestGRPCClient_DriverForwardsTheOperatorOnStopAndCancel(t *testing.T) {
	newApply := func(identifier, remoteID string) *storage.Apply {
		return &storage.Apply{
			ID:              7,
			ApplyIdentifier: identifier,
			ExternalID:      remoteID,
			PlanID:          99,
			Database:        "testdb",
			DatabaseType:    storage.DatabaseTypeMySQL,
			Environment:     "staging",
			State:           state.Apply.Running,
		}
	}
	newStorage := func(apply *storage.Apply, operation storage.ControlOperation) storage.Storage {
		storedApply := *apply
		return &mockStorage{
			applies: &mockApplyStore{apply: &storedApply},
			tasks: &mockTaskStore{tasks: []*storage.Task{{
				ID:             11,
				TaskIdentifier: "task-users",
				ApplyID:        apply.ID,
				Namespace:      "default",
				TableName:      "users",
				State:          state.Task.Running,
			}}},
			logs: &mockApplyLogStore{},
			controlRequests: &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
				ApplyID:     apply.ID,
				Operation:   operation,
				Status:      storage.ControlRequestPending,
				RequestedBy: "cli:alice",
				CreatedAt:   time.Now(),
			}}},
		}
	}

	t.Run("stop", func(t *testing.T) {
		server := &capturingTernServer{
			progressState:    ternv1.State_STATE_RUNNING,
			progressStateSet: true,
			progressTables: []*ternv1.TableProgress{{
				Namespace: "default",
				TableName: "users",
				Status:    state.Task.Running,
			}},
		}
		client, cleanup := testCapturingGRPCClient(t, server)
		defer cleanup()

		apply := newApply("apply-grpc-stop-caller", "remote-grpc-stop-caller")
		client.storage = newStorage(apply, storage.ControlOperationStop)

		_, err := client.processPendingStopControlRequest(t.Context(), apply, wholeApplyTaskScope())
		require.NoError(t, err)
		assert.Equal(t, "cli:alice", server.getStopCaller(),
			"the data plane must record the operator who issued the stop, not the forwarding path")
	})

	t.Run("cancel", func(t *testing.T) {
		server := &capturingTernServer{
			progressState:    ternv1.State_STATE_CANCELLED,
			progressStateSet: true,
			progressTables: []*ternv1.TableProgress{{
				Namespace: "default",
				TableName: "users",
				Status:    state.Task.Cancelled,
			}},
		}
		client, cleanup := testCapturingGRPCClient(t, server)
		defer cleanup()

		apply := newApply("apply-grpc-cancel-caller", "remote-grpc-cancel-caller")
		client.storage = newStorage(apply, storage.ControlOperationCancel)

		handled, err := client.processPendingCancelControlRequest(t.Context(), apply, wholeApplyTaskScope())
		require.NoError(t, err)
		require.True(t, handled)
		assert.Equal(t, "cli:alice", server.getCancelCaller(),
			"the data plane must record the operator who issued the cancel, not the forwarding path")
	})
}

func TestGRPCClient_ProcessPendingCancelControlRequestCompletesWholeApply(t *testing.T) {
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_CANCELLED,
		progressStateSet: true,
		progressTables: []*ternv1.TableProgress{{
			Namespace: "default",
			TableName: "users",
			Status:    state.Task.Cancelled,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-grpc-cancel",
		ExternalID:      "remote-grpc-cancel",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:             11,
		TaskIdentifier: "task-users",
		ApplyID:        apply.ID,
		Namespace:      "default",
		TableName:      "users",
		State:          state.Task.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	storedApply := *apply
	applyStore := &mockApplyStore{apply: &storedApply}
	logs := &mockApplyLogStore{}
	client.storage = &mockStorage{
		applies:         applyStore,
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            logs,
		controlRequests: controlRequests,
	}

	handled, err := client.processPendingCancelControlRequest(t.Context(), apply, wholeApplyTaskScope())
	require.NoError(t, err)
	assert.True(t, handled)
	assert.Equal(t, "remote-grpc-cancel", server.getCancelApplyID())
	assert.Equal(t, state.Apply.Cancelled, applyStore.apply.State)
	assert.Equal(t, state.Task.Cancelled, task.State)
	assert.True(t, hasLogEvent(logs.logs, storage.LogEventCancelRequested))
	cancelReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	assert.Nil(t, cancelReq)
}

// A data plane still on a release that recognized a since-retired control
// operation reports its settled requests on every progress poll for the life
// of the drive. There is nothing to mirror for an operation this release
// removed, so the entry is skipped at debug level — warning would recur on
// every poll of a pre-upgrade apply — and no rejection row is recorded.
func TestGRPCClient_MirrorSkipsRetiredControlOperations(t *testing.T) {
	var records []capturedLog
	controlRequests := &testControlRequestStore{}
	client := &GRPCClient{
		logger:  slog.New(captureHandler{records: &records}),
		storage: &mockStorage{controlRequests: controlRequests},
	}
	apply := &storage.Apply{
		ID: 7, ApplyIdentifier: "apply-retired-mirror",
		Database: "testdb", Environment: "staging",
	}

	client.mirrorRemoteControlRejections(t.Context(), apply, "remote-apply", []*ternv1.SettledControlRequest{{
		Operation:    "volume",
		Status:       string(storage.ControlRequestFailed),
		ErrorMessage: "the engine rejected the volume change",
	}})

	require.Len(t, records, 1, "the skip must be visible in logs, exactly once per report")
	assert.Equal(t, slog.LevelDebug, records[0].level,
		"a retired operation recurs on every poll; it must not warn")
	assert.Contains(t, records[0].msg, "retired operation")
	assert.Empty(t, controlRequests.requests, "a retired operation records no rejection row")
}

// A cancel that reconciles a terminal remote ends the drive, so the regular
// poll loop never runs again. A control command the data plane settled after
// the last regular poll — here a revert its engine refused — reaches the
// operator only if the cancel path's own progress read mirrors it; otherwise the
// operator is left believing a command they were told was accepted took effect.
func TestGRPCClient_CancelPathMirrorsSettledControlRejections(t *testing.T) {
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_CANCELLED,
		progressStateSet: true,
		progressTables: []*ternv1.TableProgress{{
			Namespace: "default",
			TableName: "users",
			Status:    state.Task.Cancelled,
		}},
		progressSettled: []*ternv1.SettledControlRequest{{
			Operation:    string(storage.ControlOperationRevert),
			Status:       string(storage.ControlRequestFailed),
			ErrorMessage: "deploy request is outside its revert window",
			RequestedBy:  "cli:alice",
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-grpc-cancel-mirror",
		ExternalID:      "remote-grpc-cancel-mirror",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:             11,
		TaskIdentifier: "task-users",
		ApplyID:        apply.ID,
		Namespace:      "default",
		TableName:      "users",
		State:          state.Task.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	storedApply := *apply
	logs := &mockApplyLogStore{}
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: &storedApply},
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            logs,
		controlRequests: controlRequests,
	}

	handled, err := client.processPendingCancelControlRequest(t.Context(), apply, wholeApplyTaskScope())
	require.NoError(t, err)
	require.True(t, handled)

	rejected, err := controlRequests.GetByOperation(t.Context(), apply.ID, storage.ControlOperationRevert)
	require.NoError(t, err)
	require.NotNil(t, rejected, "the rejection the data plane settled must survive the drive that ends here")
	assert.Equal(t, storage.ControlRequestFailed, rejected.Status)
	assert.Contains(t, rejected.ErrorMessage, "deploy request is outside its revert window")
	assert.Equal(t, "cli:alice", rejected.RequestedBy)
	assert.True(t, hasLogMessageContaining(logs.logs, "Revert was accepted but not applied"),
		"the operator must find the rejection on the schema change's log")
}

func TestGRPCClient_ProcessPendingCancelOperationLeavesApplyCancelPending(t *testing.T) {
	// A multi-deployment apply has one durable cancel request shared by every
	// deployment. One operation reaching a terminal remote state must not
	// complete the apply-level cancel request while sibling deployments still
	// need to observe it.
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_CANCELLED,
		progressStateSet: true,
		progressTables: []*ternv1.TableProgress{{
			Namespace: "default",
			TableName: "users",
			Status:    state.Task.Cancelled,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-multi-op-cancel",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	operationID := int64(42)
	task := &storage.Task{
		ID:               11,
		TaskIdentifier:   "task-users",
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		Namespace:        "default",
		TableName:        "users",
		State:            state.Task.Running,
	}
	operation := &storage.ApplyOperation{ID: operationID, ApplyID: apply.ID, Deployment: "west", State: state.ApplyOperation.Running, EngineResumeContext: "remote-op-west"}
	operationStore := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
		operationID: operation,
		43:          {ID: 43, ApplyID: apply.ID, Deployment: "east", State: state.ApplyOperation.Running, EngineResumeContext: "remote-op-east"},
	}}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	storedApply := *apply
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: &storedApply},
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
		operations:      operationStore,
	}
	scope := applyTaskScope{applyOperationID: operationID, operation: operation, multiOperation: true}

	handled, err := client.processPendingCancelControlRequest(t.Context(), apply, scope)
	require.NoError(t, err)
	assert.True(t, handled)
	assert.Equal(t, "remote-op-west", server.getCancelApplyID())
	assert.Equal(t, state.Task.Cancelled, task.State)
	assert.Equal(t, state.Apply.Running, apply.State)
	cancelReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	assert.NotNil(t, cancelReq)
}

func TestGRPCClient_ProcessPendingCancelReconcilesAlreadyTerminalRemote(t *testing.T) {
	// A remote apply whose cancel was already consumed (for example on a prior
	// drive that lost its lease before persisting) rejects a re-sent Cancel.
	// The drive must treat that rejection as "the requested outcome already
	// happened": reconcile the stored apply to the remote terminal state and
	// complete the durable cancel request, so the apply reaches a terminal
	// state instead of failing every future resume.
	server := &capturingTernServer{
		cancelErr:        status.Error(codes.Internal, "apply remote-grpc-cancel is already terminal (state: cancelled)"),
		progressState:    ternv1.State_STATE_CANCELLED,
		progressStateSet: true,
		progressTables: []*ternv1.TableProgress{{
			Namespace: "default",
			TableName: "users",
			Status:    state.Task.Cancelled,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-grpc-cancel",
		ExternalID:      "remote-grpc-cancel",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:             11,
		TaskIdentifier: "task-users",
		ApplyID:        apply.ID,
		Namespace:      "default",
		TableName:      "users",
		State:          state.Task.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	storedApply := *apply
	applyStore := &mockApplyStore{apply: &storedApply}
	logs := &mockApplyLogStore{}
	client.storage = &mockStorage{
		applies:         applyStore,
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            logs,
		controlRequests: controlRequests,
	}

	handled, err := client.processPendingCancelControlRequest(t.Context(), apply, wholeApplyTaskScope())
	require.NoError(t, err)
	assert.True(t, handled)
	assert.Equal(t, state.Apply.Cancelled, applyStore.apply.State)
	assert.Equal(t, state.Task.Cancelled, task.State)
	assert.True(t, hasLogEvent(logs.logs, storage.LogEventCancelRequested))
	cancelReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	assert.Nil(t, cancelReq)
}

func TestGRPCClient_ProcessPendingCancelKeepsRequestWhenRemoteStillActive(t *testing.T) {
	// A cancel rejection with the remote still actively running is a real
	// failure: the durable cancel request must stay pending for the next drive
	// to retry, and the stored apply must not be marked terminal.
	server := &capturingTernServer{
		cancelErr:        status.Error(codes.Unavailable, "data plane restarting"),
		progressState:    ternv1.State_STATE_RUNNING,
		progressStateSet: true,
		progressTables: []*ternv1.TableProgress{{
			Namespace: "default",
			TableName: "users",
			Status:    state.Task.Running,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-grpc-cancel",
		ExternalID:      "remote-grpc-cancel",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	storedApply := *apply
	applyStore := &mockApplyStore{apply: &storedApply}
	client.storage = &mockStorage{
		applies:         applyStore,
		tasks:           &mockTaskStore{},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
	}

	handled, err := client.processPendingCancelControlRequest(t.Context(), apply, wholeApplyTaskScope())
	require.Error(t, err)
	assert.True(t, handled)
	assert.Equal(t, state.Apply.Running, applyStore.apply.State)
	cancelReq, getErr := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, getErr)
	assert.NotNil(t, cancelReq)
}

func TestGRPCClient_ProcessPendingCancelAlreadyTerminalRemoteLeavesApplyCancelPendingForOperation(t *testing.T) {
	// The already-terminal reconcile must respect operation-drive ownership: a
	// multi-deployment apply has one durable cancel request shared by every
	// deployment, so an operation drive reconciling its own already-cancelled
	// remote must cancel only its operation's tasks and leave both the parent
	// apply state and the shared cancel request for the siblings.
	server := &capturingTernServer{
		cancelErr:        status.Error(codes.Internal, "apply remote-op-west is already terminal (state: cancelled)"),
		progressState:    ternv1.State_STATE_CANCELLED,
		progressStateSet: true,
		progressTables: []*ternv1.TableProgress{{
			Namespace: "default",
			TableName: "users",
			Status:    state.Task.Cancelled,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-multi-op-cancel",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	operationID := int64(42)
	task := &storage.Task{
		ID:               11,
		TaskIdentifier:   "task-users",
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		Namespace:        "default",
		TableName:        "users",
		State:            state.Task.Running,
	}
	operation := &storage.ApplyOperation{ID: operationID, ApplyID: apply.ID, Deployment: "west", State: state.ApplyOperation.Running, EngineResumeContext: "remote-op-west"}
	operationStore := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
		operationID: operation,
		43:          {ID: 43, ApplyID: apply.ID, Deployment: "east", State: state.ApplyOperation.Running, EngineResumeContext: "remote-op-east"},
	}}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	storedApply := *apply
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: &storedApply},
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
		operations:      operationStore,
	}
	scope := applyTaskScope{applyOperationID: operationID, operation: operation, multiOperation: true}

	handled, err := client.processPendingCancelControlRequest(t.Context(), apply, scope)
	require.NoError(t, err)
	assert.True(t, handled)
	assert.Equal(t, state.Task.Cancelled, task.State)
	assert.Equal(t, state.Apply.Running, apply.State)
	cancelReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	assert.NotNil(t, cancelReq)
}

func TestGRPCClient_ProcessPendingCancelKeepsRequestWhenRemoteStopped(t *testing.T) {
	// A stopped remote is terminal but remains cancellable, so a cancel
	// rejection with the remote stopped is not an already-consumed cancel.
	// The durable cancel request must stay pending for the next drive to
	// deliver — completing it here would let a pending start resume a schema
	// change the user cancelled.
	server := &capturingTernServer{
		cancelErr:        status.Error(codes.Unavailable, "data plane restarting"),
		progressState:    ternv1.State_STATE_STOPPED,
		progressStateSet: true,
		progressTables: []*ternv1.TableProgress{{
			Namespace: "default",
			TableName: "users",
			Status:    state.Task.Stopped,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-grpc-cancel",
		ExternalID:      "remote-grpc-cancel",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Stopped,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	storedApply := *apply
	applyStore := &mockApplyStore{apply: &storedApply}
	client.storage = &mockStorage{
		applies:         applyStore,
		tasks:           &mockTaskStore{},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
	}

	handled, err := client.processPendingCancelControlRequest(t.Context(), apply, wholeApplyTaskScope())
	require.Error(t, err)
	assert.True(t, handled)
	assert.Equal(t, state.Apply.Stopped, applyStore.apply.State)
	cancelReq, getErr := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, getErr)
	assert.NotNil(t, cancelReq)
}

func TestGRPCClient_ProcessPendingCancelStaysPendingWhileAcceptedRemoteStillStopped(t *testing.T) {
	// The success-path counterpart of the stopped-remote guard: the remote
	// accepts the Cancel and stores it durably, but its progress still reads
	// stopped — the cancel has not taken effect there yet. The request must
	// stay pending (handled=false) so a later drive reconciles once the
	// remote's own driver consumes the cancel; completing it here would
	// freeze the stored apply at stopped after the remote cancels.
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_STOPPED,
		progressStateSet: true,
		progressTables: []*ternv1.TableProgress{{
			Namespace: "default",
			TableName: "users",
			Status:    state.Task.Stopped,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-grpc-cancel-stopped-remote",
		ExternalID:      "remote-grpc-cancel-stopped-remote",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Stopped,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	storedApply := *apply
	applyStore := &mockApplyStore{apply: &storedApply}
	client.storage = &mockStorage{
		applies:         applyStore,
		tasks:           &mockTaskStore{},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
	}

	handled, err := client.processPendingCancelControlRequest(t.Context(), apply, wholeApplyTaskScope())
	require.NoError(t, err)
	assert.False(t, handled, "a stopped remote keeps the accepted cancel pending; the drive must not complete it")
	assert.Equal(t, 1, server.getCancelCalls(), "the cancel must be forwarded to the remote")
	assert.Equal(t, state.Apply.Stopped, applyStore.apply.State)
	cancelReq, getErr := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, getErr)
	assert.NotNil(t, cancelReq, "the durable cancel request must remain pending until the remote settles")
}

// TestGRPCClient_ProcessPendingCancelSyncsTasksWhenCancelStepObservesRemoteStop
// verifies the cancel step is a full observer when it is the first to see the
// remote go stopped: the stored apply still reads running with a running task,
// the remote reports stopped, and the cancel stays pending. The stored task
// rows must be synced along with the parent — persisting only the applies row
// would leave the progress surfaces showing a stopped apply with its tasks
// still running until the next poll.
func TestGRPCClient_ProcessPendingCancelSyncsTasksWhenCancelStepObservesRemoteStop(t *testing.T) {
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_STOPPED,
		progressStateSet: true,
		progressTables: []*ternv1.TableProgress{{
			Namespace: "default",
			TableName: "users",
			Status:    state.Task.Stopped,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-grpc-cancel-first-observer",
		ExternalID:      "remote-grpc-cancel-first-observer",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:             11,
		TaskIdentifier: "task-users",
		ApplyID:        apply.ID,
		Namespace:      "default",
		TableName:      "users",
		State:          state.Task.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	storedApply := *apply
	applyStore := &mockApplyStore{apply: &storedApply}
	client.storage = &mockStorage{
		applies:         applyStore,
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
	}

	handled, err := client.processPendingCancelControlRequest(t.Context(), apply, wholeApplyTaskScope())
	require.NoError(t, err)
	assert.False(t, handled, "a stopped remote keeps the accepted cancel pending; the drive must not complete it")
	assert.Equal(t, state.Apply.Stopped, applyStore.apply.State, "the parent apply must be persisted with the stopped snapshot")
	assert.Equal(t, state.Task.Stopped, task.State, "the stored task must be synced to the remote's stopped state alongside the parent")
	cancelReq, getErr := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, getErr)
	assert.NotNil(t, cancelReq, "the durable cancel request must remain pending until the remote settles")
}

func TestGRPCClient_ResumeApplyStoppedWithPendingCancelNeverStartsRemote(t *testing.T) {
	// A stopped apply claimed to deliver a pending cancel, with the remote
	// still stopped after the forward: the drive must exit without requesting
	// a remote start — starting would resume a copy the operator asked to
	// discard — leaving the apply stopped and the cancel pending for a later
	// drive to reconcile once the remote's own driver consumes it.
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_STOPPED,
		progressStateSet: true,
		progressTables: []*ternv1.TableProgress{{
			Namespace: "default",
			TableName: "users",
			Status:    state.Task.Stopped,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-grpc-stopped-cancel-gate",
		ExternalID:      "remote-grpc-stopped-cancel-gate",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Stopped,
	}
	task := &storage.Task{
		ID:             11,
		TaskIdentifier: "task-grpc-stopped-cancel-gate",
		ApplyID:        apply.ID,
		TableName:      "users",
		State:          state.Task.Stopped,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	storedApply := *apply
	applyStore := &mockApplyStore{apply: &storedApply}
	client.storage = &mockStorage{
		applies:         applyStore,
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
	}

	err := client.ResumeApply(t.Context(), apply)
	require.NoError(t, err)

	assert.False(t, server.startWasCalled(), "a stopped apply without a pending start must never receive a remote start")
	assert.Equal(t, 1, server.getCancelCalls(), "the pending cancel must be forwarded to the remote")
	assert.Equal(t, state.Apply.Stopped, applyStore.apply.State, "the stored apply stays stopped until the remote settles")
	assert.Equal(t, state.Task.Stopped, task.State, "the stopped task must not be requeued for a start")
	cancelReq, getErr := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, getErr)
	assert.NotNil(t, cancelReq, "the durable cancel request must remain pending for retry")
}

func TestGRPCClient_ProcessPendingCancelReconcilesCompletedRemote(t *testing.T) {
	// A cancel can lose the race against the remote finishing: the remote
	// settles completed before the cancel lands, and the re-sent Cancel is
	// rejected as already terminal. The reconcile must adopt the remote's
	// actual outcome — completed, not cancelled — and record which remote
	// terminal state settled the request so operators reading the apply log
	// are not misled into thinking the cancel won.
	server := &capturingTernServer{
		cancelErr:        status.Error(codes.Internal, "apply remote-grpc-cancel is already terminal (state: completed)"),
		progressState:    ternv1.State_STATE_COMPLETED,
		progressStateSet: true,
		progressTables: []*ternv1.TableProgress{{
			Namespace: "default",
			TableName: "users",
			Status:    state.Task.Completed,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-grpc-cancel",
		ExternalID:      "remote-grpc-cancel",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:             11,
		TaskIdentifier: "task-users",
		ApplyID:        apply.ID,
		Namespace:      "default",
		TableName:      "users",
		State:          state.Task.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	storedApply := *apply
	applyStore := &mockApplyStore{apply: &storedApply}
	logs := &mockApplyLogStore{}
	client.storage = &mockStorage{
		applies:         applyStore,
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            logs,
		controlRequests: controlRequests,
	}

	handled, err := client.processPendingCancelControlRequest(t.Context(), apply, wholeApplyTaskScope())
	require.NoError(t, err)
	assert.True(t, handled)
	assert.Equal(t, state.Apply.Completed, applyStore.apply.State)
	cancelReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	assert.Nil(t, cancelReq)
	var eventMessage string
	for _, log := range logs.logs {
		if log.EventType == storage.LogEventCancelRequested {
			eventMessage = log.Message
		}
	}
	assert.Contains(t, eventMessage, fmt.Sprintf("remote state: %s", state.Apply.Completed))
}

func TestGRPCClient_ProcessPendingCancelReconcilesFailedRemoteWithErrorMessage(t *testing.T) {
	// A remote that failed before the cancel landed settles the apply failed;
	// the reconcile must carry the remote's error message onto the stored
	// apply so the failure is triageable from the apply row alone.
	server := &capturingTernServer{
		cancelErr:        status.Error(codes.Internal, "apply remote-grpc-cancel is already terminal (state: failed)"),
		progressState:    ternv1.State_STATE_FAILED,
		progressStateSet: true,
		progressError:    "copy row chunk: disk full",
		progressTables: []*ternv1.TableProgress{{
			Namespace: "default",
			TableName: "users",
			Status:    state.Task.Failed,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-grpc-cancel",
		ExternalID:      "remote-grpc-cancel",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:             11,
		TaskIdentifier: "task-users",
		ApplyID:        apply.ID,
		Namespace:      "default",
		TableName:      "users",
		State:          state.Task.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	storedApply := *apply
	applyStore := &mockApplyStore{apply: &storedApply}
	client.storage = &mockStorage{
		applies:         applyStore,
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
	}

	handled, err := client.processPendingCancelControlRequest(t.Context(), apply, wholeApplyTaskScope())
	require.NoError(t, err)
	assert.True(t, handled)
	assert.Equal(t, state.Apply.Failed, applyStore.apply.State)
	assert.Equal(t, "copy row chunk: disk full", applyStore.apply.ErrorMessage)
	cancelReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	assert.Nil(t, cancelReq)
}

func TestGRPCClient_ProcessPendingCancelProgressFailureKeepsRequestPending(t *testing.T) {
	// When the Progress read used to reconcile a failed cancel also fails,
	// the drive knows nothing about the remote's actual state. It must fall
	// through to the original cancel failure and keep the durable request
	// pending, never guess a terminal outcome.
	server := &capturingTernServer{
		cancelErr:   status.Error(codes.Unavailable, "data plane restarting"),
		progressErr: status.Error(codes.Unavailable, "data plane restarting"),
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-grpc-cancel",
		ExternalID:      "remote-grpc-cancel",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	storedApply := *apply
	applyStore := &mockApplyStore{apply: &storedApply}
	client.storage = &mockStorage{
		applies:         applyStore,
		tasks:           &mockTaskStore{},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
	}

	handled, err := client.processPendingCancelControlRequest(t.Context(), apply, wholeApplyTaskScope())
	require.Error(t, err)
	assert.True(t, handled)
	assert.Equal(t, state.Apply.Running, applyStore.apply.State)
	cancelReq, getErr := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, getErr)
	assert.NotNil(t, cancelReq)
}

func TestGRPCClient_ResumeApplyOperationStartLeavesApplyStartPending(t *testing.T) {
	// A multi-deployment apply has one durable start request shared by every
	// deployment. When one operation starts its own remote apply, it must NOT
	// complete the apply-level start request: stopped sibling deployments still
	// need it pending so they remain claimable and can resume. The rollout
	// projection completes the shared request once the aggregate settles.
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_STOPPED,
		progressStateSet: true,
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-multi-op-start-pending",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "testdb-target"})
	operationID := int64(42)
	siblingID := int64(43)
	task := &storage.Task{
		ID:               11,
		TaskIdentifier:   "task-users",
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TableName:        "users",
		State:            state.Task.Stopped,
	}
	taskStore := &mockTaskStore{
		tasks:           []*storage.Task{task},
		getByApplyIDErr: errors.New("whole-apply task load must not be used for operation-scoped resume"),
	}
	operationStore := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
		operationID: {ID: operationID, ApplyID: apply.ID, Deployment: "west", State: state.ApplyOperation.Stopped, EngineResumeContext: "remote-op-west"},
		siblingID:   {ID: siblingID, ApplyID: apply.ID, Deployment: "east", State: state.ApplyOperation.Stopped, EngineResumeContext: "remote-east"},
	}}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStart,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	storedApply := *apply
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: &storedApply},
		tasks:           taskStore,
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
		operations:      operationStore,
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-multi-op-start-pending",
		}},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.NoError(t, client.ResumeApplyOperation(ctx, apply, operationID))

	assert.Equal(t, "remote-op-west", server.getStartApplyID(),
		"the operation's own remote apply id must be started, not the parent external_id")

	startReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationStart)
	require.NoError(t, err)
	assert.NotNil(t, startReq, "the apply-level start request must remain pending for sibling operations after one operation starts")
}

func TestGRPCClient_ResumeApplyOperationDispatchedStopLeavesApplyStopPending(t *testing.T) {
	// A multi-deployment operation that already dispatched (its remote apply id
	// lives on the operation) stops its own remote work when it sees the shared
	// stop request, but must NOT complete the apply-level stop request even when
	// its own remote reaches a terminal state: sibling deployments may still be
	// live, so the operator's projection owns parent stop completion.
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_STOPPED,
		progressStateSet: true,
		progressTables: []*ternv1.TableProgress{{
			Namespace:       "default",
			TableName:       "users",
			Status:          state.Task.Stopped,
			PercentComplete: 50,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-multi-op-dispatched-stop",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	operationID := int64(42)
	siblingID := int64(43)
	task := &storage.Task{
		ID:               11,
		TaskIdentifier:   "task-users",
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TableName:        "users",
		Namespace:        "default",
		State:            state.Task.Running,
	}
	operationStore := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
		operationID: {ID: operationID, ApplyID: apply.ID, Deployment: "west", State: state.ApplyOperation.Running, EngineResumeContext: "remote-op-west-1"},
		siblingID:   {ID: siblingID, ApplyID: apply.ID, Deployment: "east", State: state.ApplyOperation.Running, EngineResumeContext: "remote-op-east-1"},
	}}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	applyStore := &mockApplyStore{apply: apply}
	client.storage = &mockStorage{
		applies:         applyStore,
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
		operations:      operationStore,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.NoError(t, client.ResumeApplyOperation(ctx, apply, operationID))

	assert.Equal(t, "remote-op-west-1", server.getStopApplyID(),
		"the operation's own remote apply id must be stopped, not the parent external_id")
	assert.Empty(t, applyStore.updates,
		"a multi-op drive must never write the parent applies row; parent state is owned by the projection CAS")

	stopReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
	require.NoError(t, err)
	assert.NotNil(t, stopReq, "the apply-level stop request must remain pending for the operator to complete")
}

func TestGRPCClient_ResumeApplyOperationDefersStartWhileApplyStopPending(t *testing.T) {
	// A multi-deployment operation with both a pending start and a pending stop
	// must not start its remote work or complete the parent stop: it stops its
	// own remote work and leaves both requests pending for the operator. It must
	// return promptly rather than spin waiting for a parent stop it never owns.
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_RUNNING,
		progressStateSet: true,
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-multi-op-defer-start",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	operationID := int64(42)
	siblingID := int64(43)
	task := &storage.Task{
		ID:               11,
		TaskIdentifier:   "task-users",
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TableName:        "users",
		Namespace:        "default",
		State:            state.Task.Running,
	}
	operationStore := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
		operationID: {ID: operationID, ApplyID: apply.ID, Deployment: "west", State: state.ApplyOperation.Running, EngineResumeContext: "remote-op-west-1"},
		siblingID:   {ID: siblingID, ApplyID: apply.ID, Deployment: "east", State: state.ApplyOperation.Running, EngineResumeContext: "remote-op-east-1"},
	}}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{
		{
			ApplyID:     apply.ID,
			Operation:   storage.ControlOperationStop,
			Status:      storage.ControlRequestPending,
			RequestedBy: "cli:alice",
		},
		{
			ApplyID:     apply.ID,
			Operation:   storage.ControlOperationStart,
			Status:      storage.ControlRequestPending,
			RequestedBy: "cli:bob",
		},
	}}
	applyStore := &mockApplyStore{apply: apply}
	client.storage = &mockStorage{
		applies:         applyStore,
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
		operations:      operationStore,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.NoError(t, client.ResumeApplyOperation(ctx, apply, operationID))

	assert.Empty(t, server.getStartApplyID(),
		"a multi-op drive must not start remote work while the apply-level stop is pending")
	assert.Empty(t, applyStore.updates,
		"a multi-op drive must never write the parent applies row; parent state is owned by the projection CAS")

	stopReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
	require.NoError(t, err)
	assert.NotNil(t, stopReq, "the apply-level stop request must remain pending for the operator to complete")
	startReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationStart)
	require.NoError(t, err)
	assert.NotNil(t, startReq, "the apply-level start request must remain pending until the stop resolves")
}

func TestGRPCClient_ResumeApplyOperationFailureRecordsTasksOnlyForMultiOpApply(t *testing.T) {
	// When a multi-deployment operation's remote dispatch is rejected, the drive
	// records the failure on that operation's own tasks and returns the error,
	// but must NOT write the parent applies row: the operator derives the failed
	// operation from its tasks and moves the parent via the projection CAS.
	server := &capturingTernServer{
		applyErr: errors.New("remote rejected the apply"),
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-multi-op-fail",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "testdb-target", DeferCutover: true})
	operationID := int64(42)
	siblingID := int64(43)
	task := &storage.Task{
		ID:               11,
		TaskIdentifier:   "task-users",
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TableName:        "users",
		DDL:              "ALTER TABLE users ADD COLUMN email varchar(255)",
		DDLAction:        "alter",
		Namespace:        "default",
		State:            state.Task.Pending,
	}
	taskStore := &mockTaskStore{
		tasks:           []*storage.Task{task},
		getByApplyIDErr: errors.New("whole-apply task load must not be used for operation-scoped resume"),
	}
	operationStore := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
		operationID: {ID: operationID, ApplyID: apply.ID, Deployment: "west", State: state.ApplyOperation.Pending},
		siblingID:   {ID: siblingID, ApplyID: apply.ID, Deployment: "east", State: state.ApplyOperation.Pending},
	}}
	applyStore := &mockApplyStore{apply: apply}
	client.storage = &mockStorage{
		applies: applyStore,
		tasks:   taskStore,
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-multi-op-fail",
		}},
		operations: operationStore,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.ResumeApplyOperation(ctx, apply, operationID)
	require.Error(t, err, "a rejected remote dispatch must return an error so the operator can project the failure")

	assert.True(t, state.IsState(task.State, state.Task.Failed, state.Task.FailedRetryable),
		"the operation's own task must be recorded as failed; got %q", task.State)
	assert.Empty(t, applyStore.updates, "a multi-op failure must not write the parent applies row directly")
}

// newCutoverDriveApply returns a multi-deployment parent apply for the remote
// ordered-cutover drive tests.
func newCutoverDriveApply() *storage.Apply {
	return &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-oc",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
}

// buildCutoverDriveStorage wires the storage a remote ordered-cutover drive
// needs: a multi-operation apply whose claimed operation carries the given state
// and remote apply id, plus an untouched parked sibling.
func buildCutoverDriveStorage(apply *storage.Apply, operationID, siblingID int64, opState, opRemoteID string) (*mockStorage, *mockApplyStore, *mockApplyOperationStore, *storage.Task) {
	task := &storage.Task{
		ID:               11,
		TaskIdentifier:   "task-users",
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TableName:        "users",
		Namespace:        "default",
		State:            state.Task.Running,
	}
	operationStore := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
		operationID: {ID: operationID, ApplyID: apply.ID, Deployment: "west", State: opState, EngineResumeContext: opRemoteID},
		siblingID:   {ID: siblingID, ApplyID: apply.ID, Deployment: "east", State: state.ApplyOperation.WaitingForCutover, EngineResumeContext: "remote-east"},
	}}
	// Keep the stored parent row a distinct copy so mutating the in-memory apply
	// during the drive does not leak a terminal state into stored reads.
	storedApply := *apply
	applyStore := &mockApplyStore{apply: &storedApply}
	st := &mockStorage{
		applies:         applyStore,
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            &mockApplyLogStore{},
		controlRequests: &testControlRequestStore{},
		operations:      operationStore,
	}
	return st, applyStore, operationStore, task
}

// A barrier-parked multi-deployment operation claimed for ordered cutover forces
// the swap against the operation's own remote apply id and polls it to terminal
// while the sibling stays parked. The parent applies row is never written — the
// operator projection CAS owns parent state.
func TestGRPCClient_ResumeApplyOperationCutoverDrivesParkedOperation(t *testing.T) {
	server := &capturingTernServer{
		cutoverAccepted: true,
		progressStates:  []ternv1.State{ternv1.State_STATE_WAITING_FOR_CUTOVER, ternv1.State_STATE_COMPLETED},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := newCutoverDriveApply()
	operationID, siblingID := int64(42), int64(43)
	st, applyStore, operationStore, task := buildCutoverDriveStorage(apply, operationID, siblingID, state.ApplyOperation.WaitingForCutover, "remote-op-west")
	client.storage = st

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.NoError(t, client.ResumeApplyOperationCutover(ctx, apply, operationID))

	assert.Equal(t, "remote-op-west", server.getCutoverApplyID(), "cutover must target the operation's own remote apply id")
	assert.Equal(t, "remote-op-west", server.getProgressApplyID(), "progress must poll the operation's own remote apply id")
	assert.Nil(t, server.getApplyRequest(), "a cutover drive must not dispatch a new remote apply")
	assert.Empty(t, applyStore.updates, "a multi-op cutover drive must not write the parent applies row")
	assert.True(t, state.IsState(task.State, state.Task.Completed),
		"the operation's task should reach completed; got %q", task.State)
	assert.Equal(t, "remote-east", operationStore.ops[siblingID].EngineResumeContext, "the sibling must be untouched")
}

// The deployment-ordered cutover is routed by the claim rather than by the
// durable request, so nothing hands the drive an operator's name. It reads the
// requester back from the apply-level cutover request the operator queued, and
// the swap is only attributed correctly if that name reaches the RPC. When no
// request is pending — a sibling's drive resolved it, or this is a recovery
// re-drive — the swap still proceeds, unattributed.
func TestGRPCClient_OperationCutoverForwardsTheOperator(t *testing.T) {
	driveCutover := func(t *testing.T, pending []*storage.ApplyControlRequest) *capturingTernServer {
		t.Helper()
		server := &capturingTernServer{
			cutoverAccepted:  true,
			progressState:    ternv1.State_STATE_WAITING_FOR_CUTOVER,
			progressStateSet: true,
		}
		client, cleanup := testCapturingGRPCClient(t, server)
		t.Cleanup(cleanup)

		apply := newCutoverDriveApply()
		operationID, siblingID := int64(42), int64(43)
		st, _, operationStore, _ := buildCutoverDriveStorage(apply, operationID, siblingID, state.ApplyOperation.WaitingForCutover, "remote-op-west")
		st.controlRequests = &testControlRequestStore{requests: pending}
		client.storage = st

		scope := applyTaskScope{applyOperationID: operationID, operation: operationStore.ops[operationID], multiOperation: true}
		poll, err := client.triggerRemoteOperationCutover(t.Context(), apply, scope, "remote-op-west")
		require.NoError(t, err)
		assert.True(t, poll, "a swap forced at the barrier must be polled to terminal")
		return server
	}

	t.Run("names the operator who queued the cutover", func(t *testing.T) {
		server := driveCutover(t, []*storage.ApplyControlRequest{{
			ApplyID:     7,
			Operation:   storage.ControlOperationCutover,
			Status:      storage.ControlRequestPending,
			RequestedBy: "octocat",
			CreatedAt:   time.Now(),
		}})
		assert.Equal(t, "remote-op-west", server.getCutoverApplyID())
		assert.Equal(t, "octocat", server.getCutoverCaller(),
			"the data plane must record the operator whose cutover this drive is carrying out")
	})

	t.Run("proceeds unattributed when no request is pending", func(t *testing.T) {
		server := driveCutover(t, nil)
		assert.Equal(t, "remote-op-west", server.getCutoverApplyID(),
			"a settled request must not stop the swap")
		assert.Empty(t, server.getCutoverCaller())
	})
}

// A stale-lease recovery whose remote already left the barrier must not re-issue
// Cutover; it polls the existing swap to terminal.
func TestGRPCClient_ResumeApplyOperationCutoverDoesNotResendWhenAlreadyCuttingOver(t *testing.T) {
	server := &capturingTernServer{
		cutoverAccepted: true,
		progressStates:  []ternv1.State{ternv1.State_STATE_CUTTING_OVER, ternv1.State_STATE_COMPLETED},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := newCutoverDriveApply()
	operationID, siblingID := int64(42), int64(43)
	st, applyStore, _, _ := buildCutoverDriveStorage(apply, operationID, siblingID, state.ApplyOperation.CuttingOver, "remote-op-west")
	client.storage = st

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.NoError(t, client.ResumeApplyOperationCutover(ctx, apply, operationID))

	assert.Empty(t, server.getCutoverApplyID(), "an in-flight cutover must not be re-sent")
	assert.Equal(t, "remote-op-west", server.getProgressApplyID(), "progress must poll the operation's own remote apply id")
	assert.Empty(t, applyStore.updates, "a multi-op cutover drive must not write the parent applies row")
}

// A remote that is already unwinding a revert has moved past cutover: the swap
// happened and the revert owns the outcome. The cutover drive must never send
// Cutover at a reverting remote — it polls the revert to its terminal state.
func TestGRPCClient_ResumeApplyOperationCutoverDoesNotResendWhenRemoteReverting(t *testing.T) {
	server := &capturingTernServer{
		cutoverAccepted: true,
		progressStates:  []ternv1.State{ternv1.State_STATE_REVERTING, ternv1.State_STATE_REVERTED},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := newCutoverDriveApply()
	operationID, siblingID := int64(42), int64(43)
	st, applyStore, _, task := buildCutoverDriveStorage(apply, operationID, siblingID, state.ApplyOperation.CuttingOver, "remote-op-west")
	client.storage = st

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.NoError(t, client.ResumeApplyOperationCutover(ctx, apply, operationID))

	assert.Empty(t, server.getCutoverApplyID(), "a reverting remote must never be cut over")
	assert.Equal(t, "remote-op-west", server.getProgressApplyID(), "progress must poll the operation's own remote apply id")
	assert.Empty(t, applyStore.updates, "a multi-op cutover drive must not write the parent applies row")
	assert.True(t, state.IsState(task.State, state.Task.Reverted),
		"the operation's task should reconcile to the revert's terminal state; got %q", task.State)
}

// The remote already being terminal on preflight reconciles from that poll
// without sending Cutover, and never writes the parent row.
func TestGRPCClient_ResumeApplyOperationCutoverReconcilesAlreadyTerminalRemote(t *testing.T) {
	server := &capturingTernServer{
		progressStates: []ternv1.State{ternv1.State_STATE_COMPLETED},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := newCutoverDriveApply()
	operationID, siblingID := int64(42), int64(43)
	st, applyStore, _, task := buildCutoverDriveStorage(apply, operationID, siblingID, state.ApplyOperation.CuttingOver, "remote-op-west")
	client.storage = st

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.NoError(t, client.ResumeApplyOperationCutover(ctx, apply, operationID))

	assert.Empty(t, server.getCutoverApplyID(), "a terminal remote must not be cut over again")
	assert.Empty(t, applyStore.updates, "a multi-op cutover drive must not write the parent applies row")
	assert.True(t, state.IsState(task.State, state.Task.Completed),
		"the operation's task should reconcile to completed; got %q", task.State)
}

// The cutover drive targets the operation's own remote apply id only; an empty
// per-operation remote id fails closed rather than falling back to the parent
// apply external id.
func TestGRPCClient_ResumeApplyOperationCutoverFailsClosedOnMissingRemoteID(t *testing.T) {
	server := &capturingTernServer{cutoverAccepted: true}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := newCutoverDriveApply()
	apply.ExternalID = "parent-remote"
	operationID, siblingID := int64(42), int64(43)
	st, applyStore, _, _ := buildCutoverDriveStorage(apply, operationID, siblingID, state.ApplyOperation.WaitingForCutover, "")
	client.storage = st

	err := client.ResumeApplyOperationCutover(t.Context(), apply, operationID)
	require.Error(t, err)
	assert.Empty(t, server.getCutoverApplyID(), "no cutover may be sent without a per-operation remote id")
	assert.Empty(t, applyStore.updates, "a failed precondition must not write the parent applies row")
}

// A claim that resolves to no tasks is an invalid or stale claim and must fail
// closed without touching the remote or the parent apply.
func TestGRPCClient_ResumeApplyOperationCutoverFailsClosedOnNoTasks(t *testing.T) {
	server := &capturingTernServer{cutoverAccepted: true}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := newCutoverDriveApply()
	operationID, siblingID := int64(42), int64(43)
	st, applyStore, _, _ := buildCutoverDriveStorage(apply, operationID, siblingID, state.ApplyOperation.WaitingForCutover, "remote-op-west")
	st.tasks = &mockTaskStore{tasks: nil}
	client.storage = st

	err := client.ResumeApplyOperationCutover(t.Context(), apply, operationID)
	require.ErrorIs(t, err, ErrNoTasksForApplyOperation)
	assert.Empty(t, server.getCutoverApplyID(), "no cutover may be sent for an empty claim")
	assert.Empty(t, applyStore.updates, "an empty claim must not write the parent applies row")
}

// An operation that is not in a cutover phase (e.g. still copying) must never be
// forced through the high-risk swap.
func TestGRPCClient_ResumeApplyOperationCutoverFailsClosedOnNonCutoverState(t *testing.T) {
	server := &capturingTernServer{cutoverAccepted: true}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := newCutoverDriveApply()
	operationID, siblingID := int64(42), int64(43)
	st, applyStore, _, _ := buildCutoverDriveStorage(apply, operationID, siblingID, state.ApplyOperation.Running, "remote-op-west")
	client.storage = st

	err := client.ResumeApplyOperationCutover(t.Context(), apply, operationID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), state.ApplyOperation.Running)
	assert.Empty(t, server.getCutoverApplyID(), "a copy-phase operation must not be cut over")
	assert.Empty(t, applyStore.updates, "a rejected claim must not write the parent applies row")
}

// A remote that rejects the cutover surfaces an error without writing the parent
// row.
func TestGRPCClient_ResumeApplyOperationCutoverFailsClosedOnRejectedCutover(t *testing.T) {
	server := &capturingTernServer{
		cutoverAccepted: false,
		cutoverMessage:  "engine busy",
		progressStates:  []ternv1.State{ternv1.State_STATE_WAITING_FOR_CUTOVER},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := newCutoverDriveApply()
	operationID, siblingID := int64(42), int64(43)
	st, applyStore, _, _ := buildCutoverDriveStorage(apply, operationID, siblingID, state.ApplyOperation.WaitingForCutover, "remote-op-west")
	client.storage = st

	err := client.ResumeApplyOperationCutover(t.Context(), apply, operationID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "engine busy")
	assert.Equal(t, "remote-op-west", server.getCutoverApplyID(), "the cutover was attempted against the operation remote id")
	assert.Empty(t, applyStore.updates, "a rejected cutover must not write the parent applies row")
}

func TestGRPCClient_ResumeApplyOperationRejectsMissingOperationID(t *testing.T) {
	client, cleanup := testCapturingGRPCClient(t, &capturingTernServer{})
	defer cleanup()

	err := client.ResumeApplyOperation(t.Context(), &storage.Apply{ApplyIdentifier: "apply-x"}, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "apply operation id is required")
}

func TestGRPCClient_ResumeApplyOperationRejectsTasksFromAnotherApply(t *testing.T) {
	// Guard the (apply, apply_operation) trust boundary: if the operation ID
	// resolves to tasks owned by a different apply (mismatched pair, stale
	// claim), the drive must refuse rather than dispatch/reconcile foreign tasks
	// under this apply's state.
	server := &capturingTernServer{remoteApplyID: "remote-should-not-dispatch"}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-op-scoped",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "testdb-target", DeferCutover: true})
	operationID := int64(42)
	foreignApplyID := apply.ID + 1
	foreignTask := &storage.Task{
		ID:               11,
		TaskIdentifier:   "task-foreign",
		ApplyID:          foreignApplyID,
		ApplyOperationID: &operationID,
		TableName:        "users",
		State:            state.Task.Pending,
	}
	taskStore := &mockTaskStore{tasks: []*storage.Task{foreignTask}}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   taskStore,
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-op-scoped",
		}},
		operations: &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
			operationID: {ID: operationID, ApplyID: apply.ID, Deployment: "testdb-deployment", State: state.ApplyOperation.Pending},
		}},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.ResumeApplyOperation(ctx, apply, operationID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "task-foreign")
	assert.Contains(t, err.Error(), "belongs to apply")

	assert.Nil(t, server.getApplyRequest(), "foreign tasks must not be dispatched to remote Tern")
}

func TestGRPCClient_ResumeApplyOperationFailsClosedOnNoTasks(t *testing.T) {
	// An operation that resolves to no tasks is an invalid or stale claim. The
	// remote drive must fail closed without dispatching or mutating the parent
	// apply — marking the whole apply failed would be wrong when only this one
	// operation's lookup came back empty. Mirrors LocalClient.
	server := &capturingTernServer{remoteApplyID: "remote-should-not-dispatch"}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-op-scoped",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "testdb-target", DeferCutover: true})
	operationID := int64(42)

	applyStore := &mockApplyStore{apply: apply}
	taskStore := &mockTaskStore{tasks: nil}
	client.storage = &mockStorage{
		applies: applyStore,
		tasks:   taskStore,
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-op-scoped",
		}},
		operations: &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
			operationID: {ID: operationID, ApplyID: apply.ID, Deployment: "testdb-deployment", State: state.ApplyOperation.Pending},
		}},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.ResumeApplyOperation(ctx, apply, operationID)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNoTasksForApplyOperation, "the empty-operation fail-closed signal must be matchable with errors.Is")
	assert.Contains(t, err.Error(), "apply_operation 42")

	assert.Equal(t, operationID, taskStore.lastOperationID, "drive must look up tasks scoped to the operation")
	assert.Nil(t, server.getApplyRequest(), "an operation with no tasks must not be dispatched to remote Tern")
	assert.Empty(t, applyStore.updates, "the parent apply must not be mutated when one operation lookup is empty")
	assert.Equal(t, state.Apply.Pending, apply.State, "the parent apply state must be left untouched")
}

func TestGRPCClient_ResumeApplyLogsRemoteLifecycle(t *testing.T) {
	// gRPC mode keeps the stored apply history in the control plane. When the
	// operator dispatches work to a remote Tern service, operators should still
	// see the dispatch and final state through SchemaBot apply logs.
	server := &capturingTernServer{
		remoteApplyID:  "remote-lifecycle-123",
		progressStates: []ternv1.State{ternv1.State_STATE_RUNNING, ternv1.State_STATE_COMPLETED},
		progressTables: []*ternv1.TableProgress{{
			Namespace:       "default",
			TableName:       "users",
			Status:          state.Task.Completed,
			PercentComplete: 100,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              17,
		ApplyIdentifier: "apply-control-lifecycle",
		PlanID:          109,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      "us-west",
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "testdb-target"})
	task := &storage.Task{
		ID:             21,
		TaskIdentifier: "task-lifecycle",
		ApplyID:        apply.ID,
		TableName:      "users",
		DDL:            "ALTER TABLE users ADD COLUMN email varchar(255)",
		DDLAction:      "alter",
		Namespace:      "default",
		State:          state.Task.Pending,
	}
	logs := &mockApplyLogStore{}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-remote-lifecycle",
		}},
		logs: logs,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	err := client.ResumeApply(ctx, apply)
	require.NoError(t, err)

	messages := make([]string, 0, len(logs.logs))
	for _, log := range logs.logs {
		messages = append(messages, log.Message)
	}
	assert.Contains(t, messages, "Apply dispatched to remote Tern: target=testdb-target deployment=us-west remote_apply_id=remote-lifecycle-123")
	assert.Contains(t, messages, "Remote task users changed state: pending -> completed")
	assert.True(t, hasLogMessageContaining(logs.logs, "Remote apply reached terminal state: completed"),
		"expected final remote apply state in logs: %v", messages)
	assert.Equal(t, state.Task.Completed, task.State)
	require.NotNil(t, task.CompletedAt)

	var dispatchLog *storage.ApplyLog
	for _, log := range logs.logs {
		if strings.Contains(log.Message, "Apply dispatched to remote Tern") {
			dispatchLog = log
			break
		}
	}
	require.NotNil(t, dispatchLog, "dispatch log should be present")
	assert.Equal(t, state.Apply.Pending, dispatchLog.OldState)
	assert.Equal(t, state.Apply.Running, dispatchLog.NewState)
}

func TestGRPCClient_ResumeApplyPersistsRemoteFailureMessage(t *testing.T) {
	// Remote Tern failures should be copied into control-plane storage and logs
	// so status and logs explain the failed schema change without data-plane logs.
	server := &capturingTernServer{
		remoteApplyID:    "remote-failed-123",
		progressState:    ternv1.State_STATE_FAILED,
		progressStateSet: true,
		progressError:    "failed to connect to target database",
		progressTables: []*ternv1.TableProgress{{
			Namespace: "default",
			TableName: "users",
			Status:    state.Task.Failed,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              18,
		ApplyIdentifier: "apply-control-failed",
		PlanID:          110,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeVitess,
		Deployment:      "us-west",
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	task := &storage.Task{
		ID:             22,
		TaskIdentifier: "task-failed",
		ApplyID:        apply.ID,
		TableName:      "users",
		DDL:            "ALTER TABLE users ADD COLUMN email varchar(255)",
		DDLAction:      "alter",
		Namespace:      "default",
		State:          state.Task.Pending,
	}
	logs := &mockApplyLogStore{}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-remote-failed",
		}},
		logs: logs,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	err := client.ResumeApply(ctx, apply)
	require.NoError(t, err)

	assert.Equal(t, state.Apply.Failed, apply.State)
	assert.Equal(t, "failed to connect to target database", apply.ErrorMessage)
	assert.Equal(t, state.Task.Failed, task.State)
	assert.Equal(t, "failed to connect to target database", task.ErrorMessage,
		"failed task without its own error must adopt the remote apply's failure reason")
	progressReq := server.getProgressRequest()
	require.NotNil(t, progressReq)
	assert.Equal(t, "remote-failed-123", progressReq.ApplyId)
	assert.Equal(t, "staging", progressReq.Environment)

	var terminalLog *storage.ApplyLog
	for _, log := range logs.logs {
		if strings.Contains(log.Message, "Remote apply reached terminal state: failed") {
			terminalLog = log
			break
		}
	}
	require.NotNil(t, terminalLog, "expected failed terminal state log")
	assert.Equal(t, storage.LogLevelError, terminalLog.Level)
	assert.Contains(t, terminalLog.Message, "failed to connect to target database")
}

func TestGRPCClient_ProgressPollTerminalErrorFailsApply(t *testing.T) {
	// Permanent progress RPC errors mean the control plane cannot observe the
	// remote apply. The apply should fail with that error instead of polling
	// forever and leaving operators with an in-progress status.
	server := &capturingTernServer{
		progressErr: status.Error(codes.InvalidArgument, `invalid apply_id "apply-remote": strconv.ParseInt: invalid syntax`),
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              24,
		ApplyIdentifier: "apply-terminal-progress-error",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeVitess,
		Deployment:      "us-west",
		Environment:     "staging",
		ExternalID:      "apply-remote",
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:             30,
		TaskIdentifier: "task-terminal-progress-error",
		ApplyID:        apply.ID,
		TableName:      "users",
		State:          state.Task.Running,
	}
	logs := &mockApplyLogStore{}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
		logs:    logs,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	err := client.pollForCompletion(ctx, apply, false, wholeApplyTaskScope(), false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid apply_id")

	assert.Equal(t, state.Apply.Failed, apply.State)
	assert.Contains(t, apply.ErrorMessage, "invalid apply_id")
	assert.Equal(t, state.Task.Failed, task.State)
	assert.Contains(t, task.ErrorMessage, "invalid apply_id")
	assert.True(t, hasLogMessageContaining(logs.logs, "Remote apply failed: remote progress failed"))
}

func TestGRPCClient_ProgressPollRepeatedRetryableErrorsPauseApply(t *testing.T) {
	// Retryable progress RPC errors can happen while the remote service is
	// unavailable. After repeated failures, the apply should pause for operator
	// recovery and expose the polling error through status and logs.
	server := &capturingTernServer{
		progressErr: status.Error(codes.Unavailable, "remote service unavailable"),
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              25,
		ApplyIdentifier: "apply-retryable-progress-error",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      "us-west",
		Environment:     "staging",
		ExternalID:      "remote-retryable",
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:             31,
		TaskIdentifier: "task-retryable-progress-error",
		ApplyID:        apply.ID,
		TableName:      "users",
		State:          state.Task.Running,
	}
	logs := &mockApplyLogStore{}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
		logs:    logs,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 7*time.Second)
	defer cancel()
	err := client.pollForCompletion(ctx, apply, false, wholeApplyTaskScope(), false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "remote service unavailable")

	assert.Equal(t, state.Apply.FailedRetryable, apply.State)
	assert.Contains(t, apply.ErrorMessage, "remote progress polling failed after 10 consecutive errors")
	assert.Nil(t, apply.CompletedAt)
	assert.Equal(t, state.Task.FailedRetryable, task.State)
	assert.Contains(t, task.ErrorMessage, "remote progress polling failed after 10 consecutive errors")
	assert.Nil(t, task.CompletedAt)
	assert.True(t, hasLogMessageContaining(logs.logs, "Remote apply failed: remote progress polling failed after 10 consecutive errors"))
}

func TestGRPCClient_ProgressPollBoundsStoppedAfterStart(t *testing.T) {
	// An operator-owned start may briefly see the remote stopped state from the
	// preceding stop, but that grace period must end with a stored stopped result
	// instead of an unbounded polling loop.
	originalGracePeriod := grpcStoppedAfterStartGracePeriod
	grpcStoppedAfterStartGracePeriod = 0
	t.Cleanup(func() { grpcStoppedAfterStartGracePeriod = originalGracePeriod })

	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_STOPPED,
		progressStateSet: true,
		progressTables: []*ternv1.TableProgress{{
			Namespace:       "default",
			TableName:       "users",
			Status:          state.Task.Stopped,
			PercentComplete: 40,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              26,
		ApplyIdentifier: "apply-stopped-after-start",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeVitess,
		Deployment:      "us-west",
		Environment:     "staging",
		ExternalID:      "remote-stopped-after-start",
		State:           state.Apply.Running,
	}
	storedApply := *apply
	task := &storage.Task{
		ID:             32,
		TaskIdentifier: "task-stopped-after-start",
		ApplyID:        apply.ID,
		Namespace:      "default",
		TableName:      "users",
		State:          state.Task.Running,
	}
	logs := &mockApplyLogStore{}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: &storedApply},
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
		logs:    logs,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.pollForCompletion(ctx, apply, true, wholeApplyTaskScope(), false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "start accepted")
	assert.Contains(t, err.Error(), "remained stopped after start grace period")

	assert.Equal(t, state.Apply.Stopped, apply.State)
	assert.Contains(t, apply.ErrorMessage, "remained stopped after start grace period")
	assert.Equal(t, state.Task.Stopped, task.State)
	assert.Equal(t, 40, task.ProgressPercent)
	assert.True(t, hasLogMessageContaining(logs.logs, "remote apply remote-stopped-after-start remained stopped after start grace period"))
	assert.True(t, hasLogMessageContaining(logs.logs, "Remote apply reached terminal state: stopped"))
}

func TestGRPCClient_ProgressPollAdoptsTerminalTablesAfterStart(t *testing.T) {
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_STOPPED,
		progressStateSet: true,
		progressTables: []*ternv1.TableProgress{{
			Namespace:       "default",
			TableName:       "users",
			Status:          state.Task.Completed,
			PercentComplete: 100,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              27,
		ApplyIdentifier: "apply-stopped-with-completed-table",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeVitess,
		Deployment:      "us-west",
		Environment:     "staging",
		ExternalID:      "remote-stopped-with-completed-table",
		State:           state.Apply.Running,
	}
	storedApply := *apply
	task := &storage.Task{
		ID:             33,
		TaskIdentifier: "task-stopped-with-completed-table",
		ApplyID:        apply.ID,
		Namespace:      "default",
		TableName:      "users",
		State:          state.Task.Running,
	}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: &storedApply},
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
		logs:    &mockApplyLogStore{},
	}

	err := client.pollForCompletion(t.Context(), apply, true, wholeApplyTaskScope(), false)
	require.NoError(t, err)
	assert.Equal(t, state.Apply.Completed, apply.State)
	assert.Equal(t, state.Task.Completed, task.State)
}

// An operation-scoped barrier copy drive must stop driving the moment the remote
// parks at the cutover barrier: it persists the operation's tasks at
// waiting_for_cutover and returns, releasing its lease so the operator can mark
// the operation row parked and free it for the deployment-ordered cutover claim.
func TestGRPCClient_PollForCompletionReleasesAtCutoverBarrier(t *testing.T) {
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_WAITING_FOR_CUTOVER,
		progressStateSet: true,
		progressTables: []*ternv1.TableProgress{{
			Namespace:       "default",
			TableName:       "users",
			Status:          state.Task.WaitingForCutover,
			PercentComplete: 100,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              60,
		ApplyIdentifier: "apply-barrier-park",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      "eu",
		Environment:     "production",
		ExternalID:      "remote-barrier-park",
		State:           state.Apply.Running,
	}
	storedApply := *apply
	task := &storage.Task{
		ID:             61,
		TaskIdentifier: "task-barrier-park",
		ApplyID:        apply.ID,
		Namespace:      "default",
		TableName:      "users",
		State:          state.Task.Running,
	}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: &storedApply},
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
		logs:    &mockApplyLogStore{},
	}

	// A generous deadline: the drive must return promptly at the barrier, not
	// run until the deadline.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	err := client.pollForCompletion(ctx, apply, false, wholeApplyTaskScope(), true)
	require.NoError(t, err)
	assert.Equal(t, state.Apply.WaitingForCutover, apply.State)
	assert.Equal(t, state.Task.WaitingForCutover, task.State)
}

// The cutover drive (and any non-barrier drive) must NOT release at the barrier:
// it keeps polling a remote parked at waiting_for_cutover so it can carry the
// swap past the barrier to terminal. With releaseAtCutoverBarrier false the
// drive polls until the context deadline rather than returning.
func TestGRPCClient_PollForCompletionDoesNotReleaseWhenBarrierReleaseDisabled(t *testing.T) {
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_WAITING_FOR_CUTOVER,
		progressStateSet: true,
		progressTables: []*ternv1.TableProgress{{
			Namespace:       "default",
			TableName:       "users",
			Status:          state.Task.WaitingForCutover,
			PercentComplete: 100,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              62,
		ApplyIdentifier: "apply-barrier-hold",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      "eu",
		Environment:     "production",
		ExternalID:      "remote-barrier-hold",
		State:           state.Apply.Running,
	}
	storedApply := *apply
	task := &storage.Task{
		ID:             63,
		TaskIdentifier: "task-barrier-hold",
		ApplyID:        apply.ID,
		Namespace:      "default",
		TableName:      "users",
		State:          state.Task.Running,
	}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: &storedApply},
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
		logs:    &mockApplyLogStore{},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.pollForCompletion(ctx, apply, false, wholeApplyTaskScope(), false)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, state.Apply.WaitingForCutover, apply.State)
}

func TestGRPCClient_ResumeApplyDoesNotRegressRunningApplyToPendingProgress(t *testing.T) {
	// A freshly dispatched remote apply can report pending before the remote
	// engine starts copying rows. SchemaBot has already claimed the queued apply
	// locally, so progress polling must not write pending back to the stored
	// apply row and make it claimable by another operator driver.
	server := &capturingTernServer{
		remoteApplyID: "remote-pending-first",
		progressStates: []ternv1.State{
			ternv1.State_STATE_PENDING,
			ternv1.State_STATE_COMPLETED,
		},
		progressTables: []*ternv1.TableProgress{{
			Namespace:       "default",
			TableName:       "users",
			Status:          state.Task.Completed,
			PercentComplete: 100,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              23,
		ApplyIdentifier: "apply-pending-progress",
		PlanID:          123,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	task := &storage.Task{
		ID:             29,
		TaskIdentifier: "task-pending-progress",
		ApplyID:        apply.ID,
		TableName:      "users",
		DDL:            "ALTER TABLE users ADD COLUMN email varchar(255)",
		DDLAction:      "alter",
		Namespace:      "default",
		State:          state.Task.Pending,
	}
	applyStore := &mockApplyStore{apply: apply}
	client.storage = &mockStorage{
		applies: applyStore,
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-pending-progress",
		}},
		logs: &mockApplyLogStore{},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	err := client.ResumeApply(ctx, apply)
	require.NoError(t, err)

	var storedStates []string
	for _, updatedApply := range applyStore.updates {
		storedStates = append(storedStates, updatedApply.State)
	}
	require.NotEmpty(t, storedStates)
	assert.Equal(t, state.Apply.Running, storedStates[0])
	assert.NotContains(t, storedStates[1:], state.Apply.Pending)
	assert.Equal(t, state.Apply.Completed, storedStates[len(storedStates)-1])
}

func TestApplyStateFromRemoteProgress(t *testing.T) {
	tests := []struct {
		name        string
		storedState string
		remoteState string
		remoteTasks []*ternv1.TableProgress
		expected    string
	}{
		{
			name:        "empty remote state keeps stored state",
			storedState: state.Apply.Running,
			remoteState: "",
			expected:    state.Apply.Running,
		},
		{
			name:        "remote terminal state wins",
			storedState: state.Apply.Running,
			remoteState: state.Apply.Completed,
			expected:    state.Apply.Completed,
		},
		{
			name:        "stored terminal state is final",
			storedState: state.Apply.Completed,
			remoteState: state.Apply.Running,
			expected:    state.Apply.Completed,
		},
		{
			name:        "stored stopped state is final without start ownership",
			storedState: state.Apply.Stopped,
			remoteState: state.Apply.Running,
			expected:    state.Apply.Stopped,
		},
		{
			name:        "stored retryable failure blocks active progress",
			storedState: state.Apply.FailedRetryable,
			remoteState: state.Apply.Running,
			expected:    state.Apply.FailedRetryable,
		},
		{
			name:        "remote retryable pause keeps stored active state",
			storedState: state.Apply.Running,
			remoteState: state.Apply.FailedRetryable,
			expected:    state.Apply.Running,
		},
		{
			name:        "remote retryable pause normalizes a pre-claim pending snapshot to running",
			storedState: state.Apply.Pending,
			remoteState: state.Apply.FailedRetryable,
			expected:    state.Apply.Running,
		},
		{
			name:        "remote retryable pause normalizes a pre-claim retryable snapshot to running",
			storedState: state.Apply.FailedRetryable,
			remoteState: state.Apply.FailedRetryable,
			expected:    state.Apply.Running,
		},
		{
			name:        "stale pending remote state does not reopen running apply",
			storedState: state.Apply.Running,
			remoteState: state.Apply.Pending,
			expected:    state.Apply.Running,
		},
		{
			name:        "newer active remote state advances stored state",
			storedState: state.Apply.Running,
			remoteState: state.Apply.WaitingForCutover,
			expected:    state.Apply.WaitingForCutover,
		},
		{
			name:        "deploy-request phase advances from the dispatched preparing-branch state",
			storedState: state.Apply.PreparingBranch,
			remoteState: state.Apply.ValidatingDeployRequest,
			expected:    state.Apply.ValidatingDeployRequest,
		},
		{
			name:        "deploy-request apply advances into the row-copy running phase",
			storedState: state.Apply.ValidatingDeployRequest,
			remoteState: state.Apply.Running,
			expected:    state.Apply.Running,
		},
		{
			name:        "stale pending remote state does not reopen a preparing-branch apply",
			storedState: state.Apply.PreparingBranch,
			remoteState: state.Apply.Pending,
			expected:    state.Apply.PreparingBranch,
		},
		{
			name:        "a lagging deploy-request poll does not rewind a running apply",
			storedState: state.Apply.Running,
			remoteState: state.Apply.PreparingBranch,
			expected:    state.Apply.Running,
		},
		{
			name:        "a report with queued tables corrects a stored cutting_over to running",
			storedState: state.Apply.CuttingOver,
			remoteState: state.Apply.Running,
			remoteTasks: []*ternv1.TableProgress{
				{TableName: "users", Status: state.Task.Completed},
				{TableName: "orders", Status: state.Task.Running},
				{TableName: "payments", Status: state.Task.Pending},
			},
			expected: state.Apply.Running,
		},
		{
			name:        "a report with a still-copying table corrects a stored cutting_over to running",
			storedState: state.Apply.CuttingOver,
			remoteState: state.Apply.Running,
			remoteTasks: []*ternv1.TableProgress{
				{TableName: "users", Status: state.Task.Completed},
				{TableName: "orders", Status: state.Task.Running},
			},
			expected: state.Apply.Running,
		},
		{
			name:        "a report with a verifying table corrects a stored cutting_over",
			storedState: state.Apply.CuttingOver,
			remoteState: state.Apply.CatchingUp,
			remoteTasks: []*ternv1.TableProgress{
				{TableName: "users", Status: state.Task.Completed},
				{TableName: "orders", Status: state.Task.CatchingUp},
			},
			expected: state.Apply.CatchingUp,
		},
		{
			name:        "a parked table does not contradict a stored cutting_over",
			storedState: state.Apply.CuttingOver,
			remoteState: state.Apply.WaitingForCutover,
			remoteTasks: []*ternv1.TableProgress{
				{TableName: "users", Status: state.Task.Completed},
				{TableName: "orders", Status: state.Task.WaitingForCutover},
			},
			expected: state.Apply.CuttingOver,
		},
		{
			name:        "a stored cutting_over holds once no table is in an earlier active phase",
			storedState: state.Apply.CuttingOver,
			remoteState: state.Apply.Running,
			remoteTasks: []*ternv1.TableProgress{
				{TableName: "users", Status: state.Task.Completed},
				{TableName: "orders", Status: state.Task.CuttingOver},
			},
			expected: state.Apply.CuttingOver,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, applyStateFromRemoteProgress(tc.storedState, tc.remoteState, tc.remoteTasks, false))
		})
	}

	assert.Equal(t, state.Apply.Running,
		applyStateFromRemoteProgress(state.Apply.Stopped, state.Apply.Running, nil, true),
		"an operator-owned start may adopt active remote progress after a stale stopped write")
}

func TestGRPCClient_SyncStoredTasksFromRemoteTasksUsesRemoteTaskState(t *testing.T) {
	// Remote task state is the source of truth for stored task rows. Apply-level
	// terminal state must not invent task results when the remote task snapshot
	// is missing or incomplete.
	now := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	testCases := []struct {
		name                string
		remoteTaskState     string
		remoteProgress      int32
		wantStoredTaskState string
		wantProgress        int
		wantCompletedAt     bool
	}{
		{
			name:                "completed remote task completes stored task row",
			remoteTaskState:     state.Task.Completed,
			remoteProgress:      42,
			wantStoredTaskState: state.Task.Completed,
			wantProgress:        100,
			wantCompletedAt:     true,
		},
		{
			name:                "failed remote task fails stored task row",
			remoteTaskState:     state.Task.Failed,
			wantStoredTaskState: state.Task.Failed,
			wantCompletedAt:     true,
		},
		{
			name:                "cancelled remote task cancels stored task row",
			remoteTaskState:     state.Task.Cancelled,
			wantStoredTaskState: state.Task.Cancelled,
			wantCompletedAt:     true,
		},
		{
			name:                "reverted remote task reverts stored task row",
			remoteTaskState:     state.Task.Reverted,
			wantStoredTaskState: state.Task.Reverted,
			wantCompletedAt:     true,
		},
		{
			name:                "stopped remote task leaves stored task row resumable",
			remoteTaskState:     state.Task.Stopped,
			wantStoredTaskState: state.Task.Stopped,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			storedApply := &storage.Apply{
				ID:              18,
				ApplyIdentifier: "apply-remote-terminal",
				State:           state.Apply.Running,
			}
			storedTask := &storage.Task{
				ID:             22,
				TaskIdentifier: "task-remote-terminal",
				ApplyID:        storedApply.ID,
				TableName:      "users",
				State:          state.Task.Running,
			}
			logs := &mockApplyLogStore{}
			client := &GRPCClient{
				storage: &mockStorage{
					tasks: &mockTaskStore{tasks: []*storage.Task{storedTask}},
					logs:  logs,
				},
			}

			err := client.syncStoredTasksFromRemoteTasks(t.Context(), storedApply, []*storage.Task{storedTask}, []*ternv1.TableProgress{{
				TableName:       "users",
				Status:          tc.remoteTaskState,
				PercentComplete: tc.remoteProgress,
			}}, now)
			require.NoError(t, err)

			assert.Equal(t, tc.wantStoredTaskState, storedTask.State)
			assert.Equal(t, tc.wantProgress, storedTask.ProgressPercent)
			assert.Equal(t, tc.wantCompletedAt, storedTask.CompletedAt != nil)
			assert.True(t, hasLogMessageContaining(logs.logs, "Remote task users changed state: running -> "+tc.wantStoredTaskState))
		})
	}
}

func TestRemoteProgressRetryablePauseDetection(t *testing.T) {
	// A current data plane reports a retryable pause directly as
	// STATE_FAILED_RETRYABLE. A data plane from before that wire state reports
	// the pause as STATE_FAILED and reveals the retryable truth only on the
	// per-table statuses, so a STATE_FAILED snapshot with a table still
	// reporting failed_retryable is also a pause — nonterminal — while a
	// settled failure (all tables terminal) or a task-less failure remains a
	// real verdict the drive must reconcile.
	retryableTable := &ternv1.TableProgress{TableName: "users", Status: state.Task.FailedRetryable}
	failedTable := &ternv1.TableProgress{TableName: "users", Status: state.Task.Failed}

	testCases := []struct {
		name           string
		protoState     ternv1.State
		tables         []*ternv1.TableProgress
		wantPaused     bool
		wantTerminal   bool
		wantApplyState string
	}{
		{
			name:           "failed_retryable wire state is a pause without table inspection",
			protoState:     ternv1.State_STATE_FAILED_RETRYABLE,
			tables:         nil,
			wantPaused:     true,
			wantTerminal:   false,
			wantApplyState: state.Apply.FailedRetryable,
		},
		{
			name:           "failed_retryable wire state is a pause even when tables read settled",
			protoState:     ternv1.State_STATE_FAILED_RETRYABLE,
			tables:         []*ternv1.TableProgress{failedTable},
			wantPaused:     true,
			wantTerminal:   false,
			wantApplyState: state.Apply.FailedRetryable,
		},
		{
			name:           "failed with a retryable table is a pause",
			protoState:     ternv1.State_STATE_FAILED,
			tables:         []*ternv1.TableProgress{retryableTable},
			wantPaused:     true,
			wantTerminal:   false,
			wantApplyState: state.Apply.FailedRetryable,
		},
		{
			name:           "failed with a retryable table among settled tables is a pause",
			protoState:     ternv1.State_STATE_FAILED,
			tables:         []*ternv1.TableProgress{failedTable, retryableTable},
			wantPaused:     true,
			wantTerminal:   false,
			wantApplyState: state.Apply.FailedRetryable,
		},
		{
			name:           "failed with settled tables is terminal",
			protoState:     ternv1.State_STATE_FAILED,
			tables:         []*ternv1.TableProgress{failedTable},
			wantPaused:     false,
			wantTerminal:   true,
			wantApplyState: state.Apply.Failed,
		},
		{
			name:           "failed without tables is terminal",
			protoState:     ternv1.State_STATE_FAILED,
			tables:         nil,
			wantPaused:     false,
			wantTerminal:   true,
			wantApplyState: state.Apply.Failed,
		},
		{
			name:           "nil table entries are skipped",
			protoState:     ternv1.State_STATE_FAILED,
			tables:         []*ternv1.TableProgress{nil, retryableTable},
			wantPaused:     true,
			wantTerminal:   false,
			wantApplyState: state.Apply.FailedRetryable,
		},
		{
			name:           "completed ignores table statuses",
			protoState:     ternv1.State_STATE_COMPLETED,
			tables:         []*ternv1.TableProgress{retryableTable},
			wantPaused:     false,
			wantTerminal:   true,
			wantApplyState: state.Apply.Completed,
		},
		{
			name:           "running is never a pause",
			protoState:     ternv1.State_STATE_RUNNING,
			tables:         []*ternv1.TableProgress{retryableTable},
			wantPaused:     false,
			wantTerminal:   false,
			wantApplyState: state.Apply.Running,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantPaused, remoteApplyPausedForDataPlaneRetry(tc.protoState, tc.tables))
			assert.Equal(t, tc.wantTerminal, remoteProgressIsTerminal(tc.protoState, tc.tables))
			assert.Equal(t, tc.wantApplyState, remoteProgressApplyState(tc.protoState, tc.tables))
		})
	}
}

func TestGRPCClient_SyncStoredTasksFollowsDataPlaneRetryOfRetryableTask(t *testing.T) {
	// A stored failed_retryable task mirrors a data-plane pause. The data plane
	// owns recovery of its retryable failures: when its next attempt reports the
	// task actively driving again, the mirror must follow so operator surfaces
	// show the live attempt instead of pinning "Retrying". A remote task still
	// paused or merely requeued to pending keeps the stored pause.
	testCases := []struct {
		name            string
		remoteTaskState string
		wantStoredState string
	}{
		{
			name:            "running remote task resumes the stored pause",
			remoteTaskState: state.Task.Running,
			wantStoredState: state.Task.Running,
		},
		{
			name:            "paused remote task keeps the stored pause",
			remoteTaskState: state.Task.FailedRetryable,
			wantStoredState: state.Task.FailedRetryable,
		},
		{
			name:            "requeued pending remote task keeps the stored pause",
			remoteTaskState: state.Task.Pending,
			wantStoredState: state.Task.FailedRetryable,
		},
		{
			name:            "completed remote task completes the stored task",
			remoteTaskState: state.Task.Completed,
			wantStoredState: state.Task.Completed,
		},
		{
			name:            "unrecognized remote status keeps the stored pause",
			remoteTaskState: "someNewEngineState",
			wantStoredState: state.Task.FailedRetryable,
		},
		{
			name:            "empty remote status keeps the stored pause",
			remoteTaskState: "",
			wantStoredState: state.Task.FailedRetryable,
		},
		{
			name:            "failed remote task fails the stored task",
			remoteTaskState: state.Task.Failed,
			wantStoredState: state.Task.Failed,
		},
	}

	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			storedApply := &storage.Apply{
				ID:              18,
				ApplyIdentifier: "apply-remote-retry-resume",
				State:           state.Apply.Running,
			}
			storedTask := &storage.Task{
				ID:             22,
				TaskIdentifier: "task-remote-retry-resume",
				ApplyID:        storedApply.ID,
				TableName:      "users",
				State:          state.Task.FailedRetryable,
			}
			client := &GRPCClient{
				storage: &mockStorage{
					tasks: &mockTaskStore{tasks: []*storage.Task{storedTask}},
					logs:  &mockApplyLogStore{},
				},
			}

			err := client.syncStoredTasksFromRemoteTasks(t.Context(), storedApply, []*storage.Task{storedTask}, []*ternv1.TableProgress{{
				TableName: "users",
				Status:    tc.remoteTaskState,
			}}, now)
			require.NoError(t, err)

			assert.Equal(t, tc.wantStoredState, storedTask.State)
		})
	}
}

func TestGRPCClient_ResumeApplyRejectsStartDuringDataPlaneRetryablePause(t *testing.T) {
	// An operator start against an apply whose data plane is mid-retry has
	// nothing to start: the data plane resumes on its own. The drive settles
	// the durable start request as rejected — leaving it pending would repeat
	// the same check on every later claim — never sends a remote Start, and
	// then holds through the pause like any other in-flight snapshot instead
	// of terminalizing the stored apply from the paused wire state.
	server := &capturingTernServer{
		remoteApplyID:  "remote-start-during-pause",
		progressStates: []ternv1.State{ternv1.State_STATE_FAILED, ternv1.State_STATE_RUNNING},
		progressTableSets: [][]*ternv1.TableProgress{
			{{
				Namespace:    "default",
				TableName:    "users",
				Status:       state.Task.FailedRetryable,
				ErrorMessage: "temporary engine failure",
			}},
			{{
				Namespace:       "default",
				TableName:       "users",
				Status:          state.Task.Running,
				PercentComplete: 50,
			}},
		},
		progressTables: []*ternv1.TableProgress{{
			Namespace:       "default",
			TableName:       "users",
			Status:          state.Task.Completed,
			PercentComplete: 100,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-start-during-pause",
		ExternalID:      "remote-start-during-pause",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:             11,
		TaskIdentifier: "task-users",
		ApplyID:        apply.ID,
		Namespace:      "default",
		TableName:      "users",
		State:          state.Task.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStart,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	logs := &mockApplyLogStore{}
	applyStore := &mockApplyStore{apply: apply}
	client.storage = &mockStorage{
		applies:         applyStore,
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            logs,
		controlRequests: controlRequests,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, client.ResumeApply(ctx, apply))

	assert.Equal(t, state.Apply.Completed, apply.State,
		"the drive must hold through the pause and follow the data plane to completion")
	assert.False(t, server.startCalled, "a paused remote is not startable; no remote Start may be sent")
	pendingStart, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationStart)
	require.NoError(t, err)
	assert.Nil(t, pendingStart, "the start request must settle during the pause instead of repeating on every claim")
	startReq, err := controlRequests.GetByOperation(t.Context(), apply.ID, storage.ControlOperationStart)
	require.NoError(t, err)
	require.NotNil(t, startReq)
	assert.Equal(t, storage.ControlRequestFailed, startReq.Status)
	assert.Contains(t, startReq.ErrorMessage, "already retrying automatically")
	for _, update := range applyStore.updates {
		assert.False(t, state.IsState(update.State, state.Apply.Failed, state.Apply.FailedRetryable),
			"a retryable pause must never persist a failed or failed_retryable apply row while the drive polls, got %s", update.State)
	}
}

func TestGRPCClient_RemoteRetryablePauseKeepsDrivePollingToCompletion(t *testing.T) {
	// A data plane from before the STATE_FAILED_RETRYABLE wire state reports a
	// retryable pause as STATE_FAILED with the retryable truth only on the
	// per-table statuses. The drive must still treat that snapshot as a pause:
	// keep polling, mirror the pause and its error onto the task row, and never
	// persist a failed or failed_retryable apply row — failed_retryable is
	// immediately claimable, so a mid-drive mirror would invite a second
	// driver. When the data plane's next recovery attempt succeeds, the stored
	// apply lands completed instead of a falsely failed row diverging forever
	// from a live remote.
	server := &capturingTernServer{
		remoteApplyID:  "remote-retryable-pause",
		progressStates: []ternv1.State{ternv1.State_STATE_FAILED, ternv1.State_STATE_RUNNING},
		progressTableSets: [][]*ternv1.TableProgress{
			{{
				Namespace:    "default",
				TableName:    "users",
				Status:       state.Task.FailedRetryable,
				ErrorMessage: "failed to execute chunklet insert: Error 1041 (HY000): Out of memory",
			}},
			{{
				Namespace:       "default",
				TableName:       "users",
				Status:          state.Task.Running,
				PercentComplete: 50,
			}},
		},
		progressTables: []*ternv1.TableProgress{{
			Namespace:       "default",
			TableName:       "users",
			Status:          state.Task.Completed,
			PercentComplete: 100,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-retryable-pause",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "testdb-target"})
	task := &storage.Task{
		ID:             11,
		TaskIdentifier: "task-users",
		ApplyID:        apply.ID,
		TableName:      "users",
		DDL:            "ALTER TABLE users ADD COLUMN email varchar(255)",
		DDLAction:      "alter",
		Namespace:      "default",
		State:          state.Task.Pending,
	}
	logs := &mockApplyLogStore{}
	applyStore := &mockApplyStore{apply: apply}
	client.storage = &mockStorage{
		applies: applyStore,
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-retryable-pause",
		}},
		logs: logs,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, client.ResumeApply(ctx, apply))

	assert.Equal(t, state.Apply.Completed, apply.State)
	require.NotNil(t, apply.CompletedAt)
	assert.Equal(t, state.Task.Completed, task.State)
	for _, update := range applyStore.updates {
		assert.False(t, state.IsState(update.State, state.Apply.Failed, state.Apply.FailedRetryable),
			"a retryable pause must never persist a failed or failed_retryable apply row while the drive polls, got %s", update.State)
	}
	assert.True(t, hasLogMessageContaining(logs.logs, "Remote task users changed state: pending -> failed_retryable"),
		"the task row must mirror the data-plane pause")
	assert.True(t, hasLogMessageContaining(logs.logs, "Remote task users changed state: failed_retryable -> running"),
		"the task row must follow the data plane's next recovery attempt")
}

func TestGRPCClient_FailedRetryableWireStateKeepsDrivePollingToCompletion(t *testing.T) {
	// A current data plane reports a retryable pause directly as
	// STATE_FAILED_RETRYABLE. The drive treats it exactly like a sniffed pause:
	// keep polling, mirror the pause onto the task row, never persist a failed
	// or failed_retryable apply row mid-drive, and land completed once the data
	// plane's next recovery attempt succeeds.
	server := &capturingTernServer{
		remoteApplyID:  "remote-retryable-wire-pause",
		progressStates: []ternv1.State{ternv1.State_STATE_FAILED_RETRYABLE, ternv1.State_STATE_RUNNING},
		progressTableSets: [][]*ternv1.TableProgress{
			{{
				Namespace:    "default",
				TableName:    "users",
				Status:       state.Task.FailedRetryable,
				ErrorMessage: "failed to execute chunklet insert: Error 1041 (HY000): Out of memory",
			}},
			{{
				Namespace:       "default",
				TableName:       "users",
				Status:          state.Task.Running,
				PercentComplete: 50,
			}},
		},
		progressTables: []*ternv1.TableProgress{{
			Namespace:       "default",
			TableName:       "users",
			Status:          state.Task.Completed,
			PercentComplete: 100,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-retryable-wire-pause",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "testdb-target"})
	task := &storage.Task{
		ID:             11,
		TaskIdentifier: "task-users",
		ApplyID:        apply.ID,
		TableName:      "users",
		DDL:            "ALTER TABLE users ADD COLUMN email varchar(255)",
		DDLAction:      "alter",
		Namespace:      "default",
		State:          state.Task.Pending,
	}
	logs := &mockApplyLogStore{}
	applyStore := &mockApplyStore{apply: apply}
	client.storage = &mockStorage{
		applies: applyStore,
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-retryable-wire-pause",
		}},
		logs: logs,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, client.ResumeApply(ctx, apply))

	assert.Equal(t, state.Apply.Completed, apply.State)
	require.NotNil(t, apply.CompletedAt)
	assert.Equal(t, state.Task.Completed, task.State)
	for _, update := range applyStore.updates {
		assert.False(t, state.IsState(update.State, state.Apply.Failed, state.Apply.FailedRetryable),
			"a retryable pause must never persist a failed or failed_retryable apply row while the drive polls, got %s", update.State)
	}
	assert.True(t, hasLogMessageContaining(logs.logs, "Remote task users changed state: pending -> failed_retryable"),
		"the task row must mirror the data-plane pause")
	assert.True(t, hasLogMessageContaining(logs.logs, "Remote task users changed state: failed_retryable -> running"),
		"the task row must follow the data plane's next recovery attempt")
}

func TestGRPCClient_RemoteRetryableExhaustionTerminalizesApply(t *testing.T) {
	// The pause guard releases when the data plane settles: once its retry
	// budget expires, the tables report terminal failed, the STATE_FAILED
	// snapshot is a real verdict, and the drive reconciles the stored apply to
	// failed with the remote's error message.
	server := &capturingTernServer{
		remoteApplyID:  "remote-retryable-exhausted",
		progressStates: []ternv1.State{ternv1.State_STATE_FAILED},
		progressTableSets: [][]*ternv1.TableProgress{
			{{
				Namespace: "default",
				TableName: "users",
				Status:    state.Task.FailedRetryable,
			}},
		},
		progressState:    ternv1.State_STATE_FAILED,
		progressStateSet: true,
		progressError:    "copy failed after retries",
		progressTables: []*ternv1.TableProgress{{
			Namespace: "default",
			TableName: "users",
			Status:    state.Task.Failed,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-retryable-exhausted",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "testdb-target"})
	task := &storage.Task{
		ID:             11,
		TaskIdentifier: "task-users",
		ApplyID:        apply.ID,
		TableName:      "users",
		DDL:            "ALTER TABLE users ADD COLUMN email varchar(255)",
		DDLAction:      "alter",
		Namespace:      "default",
		State:          state.Task.Pending,
	}
	applyStore := &mockApplyStore{apply: apply}
	client.storage = &mockStorage{
		applies: applyStore,
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-retryable-exhausted",
		}},
		logs: &mockApplyLogStore{},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, client.ResumeApply(ctx, apply))

	assert.Equal(t, state.Apply.Failed, apply.State)
	assert.Equal(t, "copy failed after retries", apply.ErrorMessage)
	assert.Equal(t, state.Task.Failed, task.State)
}

func TestGRPCClient_ProcessPendingCancelKeepsRequestPendingDuringRetryablePause(t *testing.T) {
	// A cancel that the data plane rejects as already-terminal while the remote
	// apply is really paused for a data-plane retry must not be consumed: the
	// remote will keep driving, so completing the cancel here would tell the
	// operator the change is settled while the data plane finishes it. The
	// durable request stays pending for retry against the live remote.
	server := &capturingTernServer{
		cancelErr:        status.Error(codes.Internal, "apply remote-grpc-cancel-paused is already terminal (state: failed)"),
		progressState:    ternv1.State_STATE_FAILED,
		progressStateSet: true,
		progressError:    "temporary engine failure",
		progressTables: []*ternv1.TableProgress{{
			Namespace: "default",
			TableName: "users",
			Status:    state.Task.FailedRetryable,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-grpc-cancel-paused",
		ExternalID:      "remote-grpc-cancel-paused",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:             11,
		TaskIdentifier: "task-users",
		ApplyID:        apply.ID,
		Namespace:      "default",
		TableName:      "users",
		State:          state.Task.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	storedApply := *apply
	applyStore := &mockApplyStore{apply: &storedApply}
	client.storage = &mockStorage{
		applies:         applyStore,
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
	}

	_, err := client.processPendingCancelControlRequest(t.Context(), apply, wholeApplyTaskScope())
	require.Error(t, err, "a cancel rejection during a retryable pause has no terminal progress to reconcile from")
	assert.False(t, state.IsTerminalApplyState(applyStore.apply.State),
		"the stored apply must not be terminalized from a paused remote snapshot, got %s", applyStore.apply.State)
	cancelReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	assert.NotNil(t, cancelReq, "the durable cancel request must stay pending for retry against the live remote")
}

func TestGRPCClient_ProcessPendingStopSyncDoesNotTerminalizeDuringRetryablePause(t *testing.T) {
	// A stop accepted by the data plane while the remote apply is paused for a
	// data-plane retry syncs against a STATE_FAILED snapshot whose tables still
	// report failed_retryable. That snapshot is a pause, not a verdict: the
	// sync must keep the durable stop pending for the remote to consume, mirror
	// the pause onto the task row, and never persist a failed or
	// failed_retryable apply row while the drive holds the lease.
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_FAILED,
		progressStateSet: true,
		progressError:    "temporary engine failure",
		progressTables: []*ternv1.TableProgress{{
			Namespace:    "default",
			TableName:    "users",
			Status:       state.Task.FailedRetryable,
			ErrorMessage: "temporary engine failure",
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-grpc-stop-paused",
		ExternalID:      "remote-grpc-stop-paused",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:             11,
		TaskIdentifier: "task-users",
		ApplyID:        apply.ID,
		Namespace:      "default",
		TableName:      "users",
		State:          state.Task.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	storedApply := *apply
	applyStore := &mockApplyStore{apply: &storedApply}
	client.storage = &mockStorage{
		applies:         applyStore,
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
	}

	handled, err := client.processPendingStopControlRequest(t.Context(), apply, wholeApplyTaskScope())
	require.NoError(t, err)
	assert.False(t, handled, "a paused remote is not settled: the drive must keep polling for the stop to land")
	assert.False(t, state.IsTerminalApplyState(apply.State),
		"the in-memory apply must not be terminalized from a paused remote snapshot, got %s", apply.State)
	for _, update := range applyStore.updates {
		assert.False(t, state.IsState(update.State, state.Apply.Failed, state.Apply.FailedRetryable),
			"a retryable pause must never persist a failed or failed_retryable apply row while the drive holds the lease, got %s", update.State)
	}
	assert.Equal(t, state.Task.FailedRetryable, task.State, "the task row must mirror the data-plane pause")
	stopReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
	require.NoError(t, err)
	assert.NotNil(t, stopReq, "the durable stop request must stay pending for the remote to consume")
}

func TestGRPCClient_ProcessPendingCancelSyncDoesNotTerminalizeDuringRetryablePause(t *testing.T) {
	// A cancel accepted by the data plane while the remote apply is paused for
	// a data-plane retry syncs against a STATE_FAILED snapshot whose tables
	// still report failed_retryable. Completing the cancel from that snapshot
	// would tell the operator the change is settled while the data plane keeps
	// driving it: the sync must keep the durable request pending and must not
	// terminalize the stored apply.
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_FAILED,
		progressStateSet: true,
		progressError:    "temporary engine failure",
		progressTables: []*ternv1.TableProgress{{
			Namespace: "default",
			TableName: "users",
			Status:    state.Task.FailedRetryable,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-grpc-cancel-sync-paused",
		ExternalID:      "remote-grpc-cancel-sync-paused",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCancel,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	storedApply := *apply
	applyStore := &mockApplyStore{apply: &storedApply}
	client.storage = &mockStorage{
		applies:         applyStore,
		tasks:           &mockTaskStore{},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
	}

	handled, err := client.processPendingCancelControlRequest(t.Context(), apply, wholeApplyTaskScope())
	require.NoError(t, err)
	assert.False(t, handled, "a paused remote is not settled: the drive must keep polling for the cancel to land")
	assert.Equal(t, 1, server.cancelCalls, "the cancel must still be relayed to the data plane")
	assert.False(t, state.IsTerminalApplyState(apply.State),
		"the in-memory apply must not be terminalized from a paused remote snapshot, got %s", apply.State)
	for _, update := range applyStore.updates {
		assert.False(t, state.IsState(update.State, state.Apply.Failed, state.Apply.FailedRetryable),
			"a retryable pause must never persist a failed or failed_retryable apply row while the drive holds the lease, got %s", update.State)
	}
	cancelReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	assert.NotNil(t, cancelReq, "the durable cancel request must stay pending for the remote to consume")
}

func TestGRPCClient_StopErrorReconcileKeepsRequestPendingDuringRetryablePause(t *testing.T) {
	// A stop the data plane rejects as already-terminal while the remote apply
	// is really paused for a data-plane retry must not be reconciled to a
	// terminal state: the remote will keep driving, so the terminal-progress
	// fallback must see the pause as nonterminal and leave the durable stop
	// pending for retry against the live remote.
	server := &capturingTernServer{
		stopErr:          status.Error(codes.Internal, "apply remote-grpc-stop-paused is already terminal (state: failed)"),
		progressState:    ternv1.State_STATE_FAILED,
		progressStateSet: true,
		progressError:    "temporary engine failure",
		progressTables: []*ternv1.TableProgress{{
			Namespace: "default",
			TableName: "users",
			Status:    state.Task.FailedRetryable,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-grpc-stop-paused",
		ExternalID:      "remote-grpc-stop-paused",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	storedApply := *apply
	applyStore := &mockApplyStore{apply: &storedApply}
	client.storage = &mockStorage{
		applies:         applyStore,
		tasks:           &mockTaskStore{},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
	}

	_, err := client.processPendingStopControlRequest(t.Context(), apply, wholeApplyTaskScope())
	require.Error(t, err, "a stop rejection during a retryable pause has no terminal progress to reconcile from")
	assert.False(t, state.IsTerminalApplyState(applyStore.apply.State),
		"the stored apply must not be terminalized from a paused remote snapshot, got %s", applyStore.apply.State)
	stopReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
	require.NoError(t, err)
	assert.NotNil(t, stopReq, "the durable stop request must stay pending for retry against the live remote")
}

func TestGRPCClient_CutoverPreflightDoesNotTerminalizeDuringRetryablePause(t *testing.T) {
	// A cutover preflight that polls a remote paused for a data-plane retry
	// sees STATE_FAILED with a failed_retryable table. That snapshot is a
	// pause, not a verdict: the preflight must not reconcile the apply to a
	// terminal state and must not send Cutover — it returns a retryable error
	// so the operator retries once the data plane's own recovery brings the
	// copy back to the barrier.
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_FAILED,
		progressStateSet: true,
		progressError:    "temporary engine failure",
		progressTables: []*ternv1.TableProgress{{
			Namespace: "default",
			TableName: "users",
			Status:    state.Task.FailedRetryable,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-grpc-cutover-paused",
		ExternalID:      "remote-grpc-cutover-paused",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	storedApply := *apply
	applyStore := &mockApplyStore{apply: &storedApply}
	client.storage = &mockStorage{
		applies: applyStore,
		tasks:   &mockTaskStore{},
		logs:    &mockApplyLogStore{},
	}

	poll, err := client.triggerRemoteOperationCutover(t.Context(), apply, wholeApplyTaskScope(), apply.ExternalID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not parked at the cutover barrier",
		"a paused remote must surface as not-ready so the operator retries the cutover")
	assert.False(t, poll)
	assert.Empty(t, server.cutoverApplyID, "Cutover must never be sent to a remote that is not parked at the barrier")
	assert.False(t, state.IsTerminalApplyState(apply.State),
		"the in-memory apply must not be terminalized from a paused remote snapshot, got %s", apply.State)
	for _, update := range applyStore.updates {
		assert.False(t, state.IsState(update.State, state.Apply.Failed, state.Apply.FailedRetryable),
			"a retryable pause must never persist a failed or failed_retryable apply row from a cutover preflight, got %s", update.State)
	}
}

func TestGRPCClient_StampRemoteApplyErrorOnFailedTasks(t *testing.T) {
	// A remote TableProgress snapshot carries no per-table error text, so a
	// failed task mirrored from remote progress has no error of its own. The
	// terminal reconcile falls back to the remote apply's failure reason on
	// exactly those tasks, so the operation row derived from them carries an
	// actionable message. Tasks with their own error, and tasks that did not
	// fail, must be left untouched.
	now := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	remoteErr := "engine preflight: enumReorder check failed for table users"
	testCases := []struct {
		name             string
		remoteApplyState string
		remoteApplyError string
		taskState        string
		taskError        string
		wantTaskError    string
	}{
		{
			name:             "failed task without error adopts remote apply error",
			remoteApplyState: state.Apply.Failed,
			remoteApplyError: remoteErr,
			taskState:        state.Task.Failed,
			wantTaskError:    remoteErr,
		},
		{
			name:             "failed task keeps its own more specific error",
			remoteApplyState: state.Apply.Failed,
			remoteApplyError: remoteErr,
			taskState:        state.Task.Failed,
			taskError:        "table-specific failure",
			wantTaskError:    "table-specific failure",
		},
		{
			name:             "cancelled task is not stamped",
			remoteApplyState: state.Apply.Failed,
			remoteApplyError: remoteErr,
			taskState:        state.Task.Cancelled,
		},
		{
			name:             "completed task is not stamped",
			remoteApplyState: state.Apply.Failed,
			remoteApplyError: remoteErr,
			taskState:        state.Task.Completed,
		},
		{
			name:             "non-failed remote apply stamps nothing",
			remoteApplyState: state.Apply.Stopped,
			remoteApplyError: remoteErr,
			taskState:        state.Task.Failed,
		},
		{
			name:             "failed remote apply without message stamps nothing",
			remoteApplyState: state.Apply.Failed,
			taskState:        state.Task.Failed,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			storedApply := &storage.Apply{
				ID:              31,
				ApplyIdentifier: "apply-error-stamp",
				State:           tc.remoteApplyState,
			}
			remoteApply := &storage.Apply{
				ApplyIdentifier: "apply-error-stamp",
				State:           tc.remoteApplyState,
				ErrorMessage:    tc.remoteApplyError,
			}
			storedTask := &storage.Task{
				ID:             33,
				TaskIdentifier: "task-error-stamp",
				ApplyID:        storedApply.ID,
				TableName:      "users",
				State:          tc.taskState,
				ErrorMessage:   tc.taskError,
			}
			client := &GRPCClient{
				storage: &mockStorage{
					tasks: &mockTaskStore{tasks: []*storage.Task{storedTask}},
					logs:  &mockApplyLogStore{},
				},
			}

			err := client.stampRemoteApplyErrorOnFailedTasks(t.Context(), storedApply, remoteApply, []*storage.Task{storedTask}, now)
			require.NoError(t, err)
			assert.Equal(t, tc.wantTaskError, storedTask.ErrorMessage)
		})
	}
}

func TestGRPCClient_SyncStoredTasksFromRemoteTasksMirrorsPerTableError(t *testing.T) {
	// A remote TableProgress row carries the table's own failure reason. The
	// control plane mirrors it onto the stored task row so the operation — and
	// the PR comment — can attribute the failure to the right table. An empty
	// remote error never clears a stored one: an older data plane omits the
	// field, and the stored message may already carry the apply-level error.
	now := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	testCases := []struct {
		name            string
		remoteTaskError string
		storedTaskError string
		wantTaskError   string
	}{
		{
			name:            "remote per-table error is mirrored onto the stored task",
			remoteTaskError: "engine preflight: enumReorder check failed for table users",
			wantTaskError:   "engine preflight: enumReorder check failed for table users",
		},
		{
			name:            "remote error replaces an older stored error",
			remoteTaskError: "engine preflight: enumReorder check failed for table users",
			storedTaskError: "previous attempt error",
			wantTaskError:   "engine preflight: enumReorder check failed for table users",
		},
		{
			name:            "empty remote error keeps the stored error",
			storedTaskError: "stamped from the apply-level failure reason",
			wantTaskError:   "stamped from the apply-level failure reason",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			storedApply := &storage.Apply{
				ID:              19,
				ApplyIdentifier: "apply-table-error-mirror",
				State:           state.Apply.Running,
			}
			storedTask := &storage.Task{
				ID:             23,
				TaskIdentifier: "task-table-error-mirror",
				ApplyID:        storedApply.ID,
				TableName:      "users",
				State:          state.Task.Running,
				ErrorMessage:   tc.storedTaskError,
			}
			client := &GRPCClient{
				storage: &mockStorage{
					tasks: &mockTaskStore{tasks: []*storage.Task{storedTask}},
					logs:  &mockApplyLogStore{},
				},
			}

			err := client.syncStoredTasksFromRemoteTasks(t.Context(), storedApply, []*storage.Task{storedTask}, []*ternv1.TableProgress{{
				TableName:    "users",
				Status:       state.Task.Failed,
				ErrorMessage: tc.remoteTaskError,
			}}, now)
			require.NoError(t, err)

			assert.Equal(t, tc.wantTaskError, storedTask.ErrorMessage)
			assert.Equal(t, state.Task.Failed, storedTask.State)
		})
	}
}

func TestGRPCClient_SyncStoredTasksFromRemoteTasksAttributesEachStatementToItsOwnTask(t *testing.T) {
	// A remote apply that runs two statements against one table reports one
	// TableProgress row per statement. Each stored task mirrors the row for
	// its own statement, so the finished statement reads completed while the
	// one still running reads running — instead of both tasks taking whichever
	// row for the table the data plane happened to list last.
	now := time.Date(2026, 8, 31, 9, 0, 0, 0, time.UTC)
	const (
		addEmailDDL = "ALTER TABLE public.users ADD COLUMN email text"
		addNameDDL  = "ALTER TABLE public.users ADD COLUMN name text"
	)
	storedApply := &storage.Apply{ID: 61, ApplyIdentifier: "apply-two-statements", State: state.Apply.Running}
	addEmail := &storage.Task{
		ID: 62, TaskIdentifier: "task-add-email", ApplyID: storedApply.ID,
		Namespace: "public", TableName: "users", DDL: addEmailDDL, State: state.Task.Running,
	}
	addName := &storage.Task{
		ID: 63, TaskIdentifier: "task-add-name", ApplyID: storedApply.ID,
		Namespace: "public", TableName: "users", DDL: addNameDDL, State: state.Task.Pending,
	}
	client := &GRPCClient{
		storage: &mockStorage{
			tasks: &mockTaskStore{tasks: []*storage.Task{addEmail, addName}},
			logs:  &mockApplyLogStore{},
		},
	}

	err := client.syncStoredTasksFromRemoteTasks(t.Context(), storedApply, []*storage.Task{addEmail, addName}, []*ternv1.TableProgress{
		{TaskId: "remote-1", Namespace: "public", TableName: "users", Ddl: addEmailDDL, Status: state.Task.Completed, PercentComplete: 100},
		{TaskId: "remote-2", Namespace: "public", TableName: "users", Ddl: addNameDDL, Status: state.Task.Running, PercentComplete: 40},
	}, now)
	require.NoError(t, err)

	assert.Equal(t, state.Task.Completed, addEmail.State)
	assert.Equal(t, 100, addEmail.ProgressPercent)
	assert.Equal(t, state.Task.Running, addName.State)
	assert.Equal(t, 40, addName.ProgressPercent)
}

func TestGRPCClient_SyncShardProgressFromRemote(t *testing.T) {
	// A remote Tern Progress response carries per-shard ShardProgress. The control
	// plane is a reader/mirror, so it must encode those into per-(table, shard) task
	// rows in its own storage — otherwise the PR comment / CLI can never show the
	// per-shard breakdown or shard drift for a remote apply. Writing is lease-gated:
	// the operator's drive holds the lease, read-path callers do not.
	now := time.Now()
	opID := int64(7)
	apply := &storage.Apply{ID: 40, ApplyIdentifier: "apply-shard-sync", State: state.Apply.Running}
	newStoredTask := func() *storage.Task {
		return &storage.Task{
			ID: 41, TaskIdentifier: "task-shard-sync", ApplyID: apply.ID,
			ApplyOperationID: &opID, Namespace: "commerce_sharded", TableName: "customers",
			State: state.Task.Running,
		}
	}
	remoteTables := func() []*ternv1.TableProgress {
		return []*ternv1.TableProgress{{
			Namespace:  "commerce_sharded",
			TableName:  "customers",
			Status:     state.Task.Running,
			RowsCopied: 150,
			RowsTotal:  300,
			Shards: []*ternv1.ShardProgress{
				{Shard: "-80", Status: state.Task.Running, RowsCopied: 100, RowsTotal: 100},
				{Shard: "80-", Status: state.Task.Running, RowsCopied: 50, RowsTotal: 200},
			},
		}}
	}

	t.Run("encodes per-shard rows under a lease", func(t *testing.T) {
		storedTask := newStoredTask()
		tasks := &mockTaskStore{tasks: []*storage.Task{storedTask}}
		client := &GRPCClient{storage: &mockStorage{tasks: tasks, logs: &mockApplyLogStore{}}}
		ctx := storage.WithApplyLease(t.Context(), storage.ApplyLease{ApplyID: apply.ID, Token: "lease-tok"})

		require.NoError(t, client.syncStoredTasksFromRemoteTasks(ctx, apply, []*storage.Task{storedTask}, remoteTables(), now))

		require.Len(t, tasks.upsertedShards, 2)
		byShard := map[string]*storage.Task{}
		for _, s := range tasks.upsertedShards {
			byShard[s.Shard] = s
		}
		require.Contains(t, byShard, "-80")
		require.Contains(t, byShard, "80-")
		// Identity carried from the table task; rows/percent from the shard.
		assert.Equal(t, apply.ID, byShard["-80"].ApplyID)
		require.NotNil(t, byShard["-80"].ApplyOperationID)
		assert.Equal(t, opID, *byShard["-80"].ApplyOperationID)
		assert.Equal(t, "commerce_sharded", byShard["-80"].Namespace)
		assert.Equal(t, "customers", byShard["-80"].TableName)
		assert.Equal(t, int64(100), byShard["-80"].RowsCopied)
		assert.Equal(t, 100, byShard["-80"].ProgressPercent) // 100/100
		assert.Equal(t, 25, byShard["80-"].ProgressPercent)  // 50/200
	})

	t.Run("skips when no lease is on the context", func(t *testing.T) {
		storedTask := newStoredTask()
		tasks := &mockTaskStore{tasks: []*storage.Task{storedTask}}
		client := &GRPCClient{storage: &mockStorage{tasks: tasks, logs: &mockApplyLogStore{}}}

		require.NoError(t, client.syncStoredTasksFromRemoteTasks(t.Context(), apply, []*storage.Task{storedTask}, remoteTables(), now))

		assert.Empty(t, tasks.upsertedShards, "no per-shard rows should be written without a lease")
	})

	t.Run("skips a shard-scoped drive task", func(t *testing.T) {
		// A per-shard work operation's drive task is itself the per-shard row for
		// its shard. Mirroring a table-level breakdown under that operation would
		// overwrite the drive task's row and attach rows for shards the operation
		// does not own, so a shard-scoped task never fans out.
		storedTask := newStoredTask()
		storedTask.Shard = "-80"
		tasks := &mockTaskStore{tasks: []*storage.Task{storedTask}}
		client := &GRPCClient{storage: &mockStorage{tasks: tasks, logs: &mockApplyLogStore{}}}
		ctx := storage.WithApplyLease(t.Context(), storage.ApplyLease{ApplyID: apply.ID, Token: "lease-tok"})

		require.NoError(t, client.syncStoredTasksFromRemoteTasks(ctx, apply, []*storage.Task{storedTask}, remoteTables(), now))

		assert.Empty(t, tasks.upsertedShards, "a shard-scoped drive task must not mirror a per-shard breakdown")
	})

	t.Run("skips a shard entry with an empty shard name", func(t *testing.T) {
		// An empty shard name would collide with the unsharded single-shard
		// sentinel, so the entry is dropped while the named shards still mirror.
		storedTask := newStoredTask()
		tasks := &mockTaskStore{tasks: []*storage.Task{storedTask}}
		client := &GRPCClient{storage: &mockStorage{tasks: tasks, logs: &mockApplyLogStore{}}}
		ctx := storage.WithApplyLease(t.Context(), storage.ApplyLease{ApplyID: apply.ID, Token: "lease-tok"})
		tables := remoteTables()
		tables[0].Shards = append(tables[0].Shards, &ternv1.ShardProgress{Status: state.Task.Running, RowsCopied: 10, RowsTotal: 10})

		require.NoError(t, client.syncStoredTasksFromRemoteTasks(ctx, apply, []*storage.Task{storedTask}, tables, now))

		require.Len(t, tasks.upsertedShards, 2)
		for _, s := range tasks.upsertedShards {
			assert.NotEmpty(t, s.Shard, "no per-shard row may be stored without a shard name")
		}
	})
}

func TestGRPCClient_SyncUnchangedProgressAdvancesUpdatedAt(t *testing.T) {
	// Every mirror tick stamps tasks.updated_at with the sync time, even when
	// the remote progress is byte-for-byte identical to what is already stored.
	// The operator consumes that timestamp as the drive's liveness signal
	// (ApplyDriveStallAfter): long-running phases such as checksums legitimately
	// hold row counts flat, so a poll over unchanged progress must still count
	// as liveness rather than letting the task row age into a stall verdict.
	now := time.Now()
	staleUpdatedAt := now.Add(-time.Hour)
	apply := &storage.Apply{ID: 51, ApplyIdentifier: "apply-liveness-stamp", State: state.Apply.Running}
	storedTask := &storage.Task{
		ID: 52, TaskIdentifier: "task-liveness-stamp", ApplyID: apply.ID,
		Namespace: "commerce", TableName: "users",
		State:      state.Task.Running,
		RowsCopied: 150, RowsTotal: 300, ProgressPercent: 50,
		UpdatedAt: staleUpdatedAt,
	}
	tasks := &mockTaskStore{tasks: []*storage.Task{storedTask}}
	client := &GRPCClient{storage: &mockStorage{tasks: tasks, logs: &mockApplyLogStore{}}}

	require.NoError(t, client.syncStoredTasksFromRemoteTasks(t.Context(), apply, []*storage.Task{storedTask}, []*ternv1.TableProgress{{
		Namespace:       "commerce",
		TableName:       "users",
		Status:          state.Task.Running,
		RowsCopied:      150,
		RowsTotal:       300,
		PercentComplete: 50,
	}}, now))

	require.Len(t, tasks.updated, 1)
	assert.True(t, tasks.updated[0].UpdatedAt.Equal(now), "unchanged progress must still stamp updated_at with the sync time")
	assert.Equal(t, state.Task.Running, tasks.updated[0].State)
	assert.Equal(t, int64(150), tasks.updated[0].RowsCopied)
	assert.Equal(t, 50, tasks.updated[0].ProgressPercent)
}

func TestGRPCClient_SyncRemoteProgressByNamespace(t *testing.T) {
	// Multi-keyspace Vitess applies can report the same table name from more
	// than one namespace. The control plane must update each stored task from
	// the matching namespace/table progress row.
	apply := &storage.Apply{
		ID:              19,
		ApplyIdentifier: "apply-namespace-progress",
		Database:        "testdb",
		Environment:     "staging",
		ExternalID:      "remote-namespace-progress",
		State:           state.Apply.Running,
	}
	firstTask := &storage.Task{
		ID:             31,
		TaskIdentifier: "task-commerce-orders",
		ApplyID:        apply.ID,
		Namespace:      "commerce_sharded",
		TableName:      "orders",
		State:          state.Task.Pending,
	}
	secondTask := &storage.Task{
		ID:             32,
		TaskIdentifier: "task-commerce-006-orders",
		ApplyID:        apply.ID,
		Namespace:      "commerce_sharded_006",
		TableName:      "orders",
		State:          state.Task.Pending,
	}
	client := &GRPCClient{
		storage: &mockStorage{
			tasks: &mockTaskStore{tasks: []*storage.Task{firstTask, secondTask}},
			logs:  &mockApplyLogStore{},
		},
	}

	err := client.syncStoredTasksFromRemoteTasks(t.Context(), apply, []*storage.Task{firstTask, secondTask}, []*ternv1.TableProgress{
		{
			Namespace:       "commerce_sharded",
			TableName:       "orders",
			Status:          state.Task.Running,
			RowsCopied:      100,
			RowsTotal:       1000,
			PercentComplete: 10,
		},
		{
			Namespace:       "commerce_sharded_006",
			TableName:       "orders",
			Status:          state.Task.Running,
			RowsCopied:      800,
			RowsTotal:       1000,
			PercentComplete: 80,
		},
	}, time.Now())
	require.NoError(t, err)

	assert.Equal(t, state.Task.Running, firstTask.State)
	assert.Equal(t, int64(100), firstTask.RowsCopied)
	assert.Equal(t, int64(1000), firstTask.RowsTotal)
	assert.Equal(t, 10, firstTask.ProgressPercent)
	assert.Equal(t, state.Task.Running, secondTask.State)
	assert.Equal(t, int64(800), secondTask.RowsCopied)
	assert.Equal(t, int64(1000), secondTask.RowsTotal)
	assert.Equal(t, 80, secondTask.ProgressPercent)
}

func TestGRPCClient_SyncRemoteProgressKeepsLastUsefulRows(t *testing.T) {
	// Remote progress is lossy when a data-plane pod can see the stored task but
	// does not own the in-memory engine runner. If that response omits row
	// totals, keep the last durable row-copy progress instead of clearing the
	// operator-facing progress bar.
	apply := &storage.Apply{
		ID:              20,
		ApplyIdentifier: "apply-progress-regression",
		Database:        "testdb",
		Environment:     "staging",
		ExternalID:      "remote-progress-regression",
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:              33,
		TaskIdentifier:  "task-progress-regression",
		ApplyID:         apply.ID,
		Namespace:       "default",
		TableName:       "orders",
		State:           state.Task.Running,
		RowsCopied:      950,
		RowsTotal:       1000,
		ProgressPercent: 95,
		ETASeconds:      120,
	}
	client := &GRPCClient{
		storage: &mockStorage{
			tasks: &mockTaskStore{tasks: []*storage.Task{task}},
			logs:  &mockApplyLogStore{},
		},
	}

	err := client.syncStoredTasksFromRemoteTasks(t.Context(), apply, []*storage.Task{task}, []*ternv1.TableProgress{
		{
			Namespace:       "default",
			TableName:       "orders",
			Status:          state.Task.Running,
			RowsCopied:      0,
			RowsTotal:       0,
			PercentComplete: 0,
		},
	}, time.Now())
	require.NoError(t, err)

	assert.Equal(t, state.Task.Running, task.State)
	assert.Equal(t, int64(950), task.RowsCopied)
	assert.Equal(t, int64(1000), task.RowsTotal)
	assert.Equal(t, 95, task.ProgressPercent)
	assert.Equal(t, 120, task.ETASeconds)
}

func TestGRPCClient_SyncRemoteProgressMirrorsRowCopySnapshot(t *testing.T) {
	// The control plane never polls a remote engine directly: the stored task
	// rows it mirrors from the remote Progress response are the only source the
	// PR progress comment and CLI render from. Every field of the remote
	// row-copy snapshot — rows, percent, ETA, checksum progress — must land in
	// storage, or the surfaces above render it as absent for the apply's whole
	// life.
	apply := &storage.Apply{
		ID:              21,
		ApplyIdentifier: "apply-progress-snapshot",
		Database:        "testdb",
		Environment:     "staging",
		ExternalID:      "remote-progress-snapshot",
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:             34,
		TaskIdentifier: "task-progress-snapshot",
		ApplyID:        apply.ID,
		Namespace:      "default",
		TableName:      "orders",
		State:          state.Task.Running,
	}
	client := &GRPCClient{
		storage: &mockStorage{
			tasks: &mockTaskStore{tasks: []*storage.Task{task}},
			logs:  &mockApplyLogStore{},
		},
	}

	err := client.syncStoredTasksFromRemoteTasks(t.Context(), apply, []*storage.Task{task}, []*ternv1.TableProgress{
		{
			Namespace:           "default",
			TableName:           "orders",
			Status:              state.Task.Running,
			RowsCopied:          450,
			RowsTotal:           1000,
			PercentComplete:     45,
			EtaSeconds:          340,
			ChecksumRowsChecked: 200,
			ChecksumRowsTotal:   1000,
			Throttled:           true,
			ThrottleReason:      "replica-lag 12s > 10s",
		},
	}, time.Now())
	require.NoError(t, err)

	assert.Equal(t, state.Task.Running, task.State)
	assert.Equal(t, int64(450), task.RowsCopied)
	assert.Equal(t, int64(1000), task.RowsTotal)
	assert.Equal(t, 45, task.ProgressPercent)
	assert.Equal(t, 340, task.ETASeconds)
	assert.Equal(t, int64(200), task.ChecksumRowsChecked)
	assert.Equal(t, int64(1000), task.ChecksumRowsTotal)
	assert.True(t, task.Throttled, "throttle state mirrors from the remote table progress")
	assert.Equal(t, "replica-lag 12s > 10s", task.ThrottleReason)
}

func TestGRPCClient_SyncRemoteProgressThrottleContract(t *testing.T) {
	// Throttle state is a point-in-time signal the PR comment and CLI render
	// verbatim, so the mirror must enforce its contract on every tick: a
	// lifted throttle clears even on a snapshot whose row totals are kept, a
	// reason never survives without the flag, and a remote reason is bounded
	// before it can reach an operator surface.
	apply := &storage.Apply{
		ID:              22,
		ApplyIdentifier: "apply-throttle-contract",
		Database:        "testdb",
		Environment:     "staging",
		ExternalID:      "remote-throttle-contract",
		State:           state.Apply.Running,
	}
	newThrottledTask := func() *storage.Task {
		return &storage.Task{
			ID:             35,
			TaskIdentifier: "task-throttle-contract",
			ApplyID:        apply.ID,
			Namespace:      "default",
			TableName:      "orders",
			State:          state.Task.Running,
			RowsCopied:     450,
			RowsTotal:      1000,
			Throttled:      true,
			ThrottleReason: "replica-lag 12s > 10s",
		}
	}
	sync := func(t *testing.T, task *storage.Task, remote *ternv1.TableProgress) {
		t.Helper()
		client := &GRPCClient{
			storage: &mockStorage{
				tasks: &mockTaskStore{tasks: []*storage.Task{task}},
				logs:  &mockApplyLogStore{},
			},
		}
		remote.Namespace = "default"
		remote.TableName = "orders"
		remote.Status = state.Task.Running
		err := client.syncStoredTasksFromRemoteTasks(t.Context(), apply, []*storage.Task{task}, []*ternv1.TableProgress{remote}, time.Now())
		require.NoError(t, err)
	}

	t.Run("a lifted throttle clears on a snapshot with omitted row totals", func(t *testing.T) {
		task := newThrottledTask()
		sync(t, task, &ternv1.TableProgress{})

		assert.Equal(t, int64(450), task.RowsCopied, "the stored row-copy progress survives the omitted totals")
		assert.Equal(t, int64(1000), task.RowsTotal)
		assert.False(t, task.Throttled, "the throttle flag mirrors the snapshot even when row totals are kept")
		assert.Empty(t, task.ThrottleReason)
	})

	t.Run("a reason without the flag is dropped", func(t *testing.T) {
		task := newThrottledTask()
		sync(t, task, &ternv1.TableProgress{
			RowsCopied:     500,
			RowsTotal:      1000,
			ThrottleReason: "replica-lag 12s > 10s",
		})

		assert.False(t, task.Throttled)
		assert.Empty(t, task.ThrottleReason, "an unthrottled task never carries a reason")
	})

	t.Run("a remote reason is sanitized before it is stored", func(t *testing.T) {
		task := newThrottledTask()
		sync(t, task, &ternv1.TableProgress{
			RowsCopied:     500,
			RowsTotal:      1000,
			Throttled:      true,
			ThrottleReason: "replica-lag\n12s | " + strings.Repeat("threads-running 130 > 128; ", 20),
		})

		assert.True(t, task.Throttled)
		assert.NotContains(t, task.ThrottleReason, "\n", "newlines collapse before storage")
		assert.NotContains(t, task.ThrottleReason, "|", "table separators are neutralized before storage")
		assert.LessOrEqual(t, len(task.ThrottleReason), 200, "an overlong remote reason is clamped")
		assert.True(t, strings.HasSuffix(task.ThrottleReason, "…"))
	})
}

func TestGRPCClient_PollSetsTerminalTaskMetadataFromRemoteTaskProgress(t *testing.T) {
	// Terminal remote task progress marks the stored task terminal and fills
	// local metadata before the stored apply row is marked completed.
	client, cleanup := testCapturingGRPCClient(t, &capturingTernServer{
		progressState:    ternv1.State_STATE_COMPLETED,
		progressStateSet: true,
		progressTables: []*ternv1.TableProgress{{
			TableName:       "users",
			Status:          state.Task.Completed,
			PercentComplete: 100,
		}},
	})
	defer cleanup()

	apply := &storage.Apply{
		ID:              18,
		ApplyIdentifier: "apply-remote-completed",
		Database:        "testdb",
		Environment:     "staging",
		ExternalID:      "remote-completed",
		State:           state.Apply.Running,
	}
	storedApply := *apply
	task := &storage.Task{
		ID:             22,
		TaskIdentifier: "task-remote-completed",
		ApplyID:        apply.ID,
		TableName:      "users",
		State:          state.Task.Running,
	}
	logs := &mockApplyLogStore{}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: &storedApply},
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
		logs:    logs,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.pollForCompletion(ctx, apply, false, wholeApplyTaskScope(), false)
	require.NoError(t, err)

	assert.Equal(t, state.Task.Completed, task.State)
	assert.Equal(t, 100, task.ProgressPercent)
	require.NotNil(t, task.CompletedAt)
}

func TestGRPCClient_PollReconcilesLaggingTaskWhenRemoteApplyCompletedAndTaskProgressOmitted(t *testing.T) {
	// A terminal remote apply is authoritative: the remote will send no more task
	// progress, so a stored task the remote no longer reports is reconciled to the
	// apply's terminal state and the apply finalizes — rather than looping forever
	// waiting for progress that will never arrive.
	client, cleanup := testCapturingGRPCClient(t, &capturingTernServer{
		progressState:    ternv1.State_STATE_COMPLETED,
		progressStateSet: true,
	})
	defer cleanup()

	apply := &storage.Apply{
		ID:              18,
		ApplyIdentifier: "apply-terminal-missing-task-state",
		Database:        "testdb",
		Environment:     "staging",
		ExternalID:      "remote-terminal-missing-task-state",
		State:           state.Apply.Running,
	}
	storedApply := *apply
	task := &storage.Task{
		ID:             22,
		TaskIdentifier: "task-terminal-missing-state",
		ApplyID:        apply.ID,
		TableName:      "users",
		State:          state.Task.Running,
	}
	applyStore := &mockApplyStore{apply: &storedApply}
	client.storage = &mockStorage{
		applies: applyStore,
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
		logs:    &mockApplyLogStore{},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.pollForCompletion(ctx, apply, false, wholeApplyTaskScope(), false)
	require.NoError(t, err)

	assert.Equal(t, state.Apply.Completed, applyStore.apply.State)
	assert.Equal(t, state.Task.Completed, task.State)
	assert.Equal(t, 100, task.ProgressPercent)
	require.NotNil(t, task.CompletedAt)
}

// A storage failure while reconciling a lagging task is genuinely transient, so
// the apply is kept active for operator retry rather than finalized.
func TestGRPCClient_PollKeepsApplyActiveWhenReconcilingLaggingTaskStorageFails(t *testing.T) {
	client, cleanup := testCapturingGRPCClient(t, &capturingTernServer{
		progressState:    ternv1.State_STATE_COMPLETED,
		progressStateSet: true,
	})
	defer cleanup()

	apply := &storage.Apply{
		ID:              19,
		ApplyIdentifier: "apply-terminal-task-update-fails",
		Database:        "testdb",
		Environment:     "staging",
		ExternalID:      "remote-terminal-task-update-fails",
		State:           state.Apply.Running,
	}
	storedApply := *apply
	task := &storage.Task{
		ID:             23,
		TaskIdentifier: "task-terminal-update-fails",
		ApplyID:        apply.ID,
		TableName:      "users",
		State:          state.Task.Running,
	}
	applyStore := &mockApplyStore{apply: &storedApply}
	client.storage = &mockStorage{
		applies: applyStore,
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}, updateErr: errors.New("storage unavailable")},
		logs:    &mockApplyLogStore{},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.pollForCompletion(ctx, apply, false, wholeApplyTaskScope(), false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "storage unavailable")
	assert.Equal(t, state.Apply.Running, applyStore.apply.State)
	assert.Nil(t, applyStore.apply.CompletedAt)
}

// When a stop drives the remote apply to a terminal state but the remote no
// longer reports the per-task progress (the remote apply is already terminal and
// drops the task from its payload), the terminal remote apply is authoritative:
// the lagging stored task must be reconciled to the matching terminal state so
// the stop finalizes. The control plane must not loop forever waiting for task
// progress the terminal remote will never send again.
func TestGRPCClient_PollReconcilesLaggingTaskWhenRemoteApplyStoppedAndTaskProgressOmitted(t *testing.T) {
	client, cleanup := testCapturingGRPCClient(t, &capturingTernServer{
		progressState:    ternv1.State_STATE_STOPPED,
		progressStateSet: true,
		// No progressTables: the terminal remote apply no longer reports the task.
	})
	defer cleanup()

	apply := &storage.Apply{
		ID:              31,
		ApplyIdentifier: "apply-stop-terminal-remote",
		Database:        "testdb",
		Environment:     "staging",
		ExternalID:      "remote-stop-terminal-apply",
		State:           state.Apply.Running,
	}
	storedApply := *apply
	task := &storage.Task{
		ID:             41,
		TaskIdentifier: "task-stop-terminal-remote",
		ApplyID:        apply.ID,
		TableName:      "users",
		State:          state.Task.Running,
	}
	applyStore := &mockApplyStore{apply: &storedApply}
	client.storage = &mockStorage{
		applies: applyStore,
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
		logs:    &mockApplyLogStore{},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.pollForCompletion(ctx, apply, false, wholeApplyTaskScope(), false)
	require.NoError(t, err)

	assert.Equal(t, state.Apply.Stopped, applyStore.apply.State)
	assert.Equal(t, state.Task.Stopped, task.State,
		"lagging stored task should be reconciled to the apply's stopped state, got %q", task.State)
}

// When a remote apply fails and no longer reports per-task progress, the
// lagging stored tasks resolve by what they were doing when the apply died: a
// task that had started fails with the apply, while a pending task that never
// started is cancelled — it was blocked behind the failure, and marking it
// failed would misattribute the failure to a table that did no work. Status
// surfaces then show the operator which table to investigate.
func TestGRPCClient_PollReconcilesPendingTaskToCancelledWhenRemoteApplyFailed(t *testing.T) {
	client, cleanup := testCapturingGRPCClient(t, &capturingTernServer{
		progressState:    ternv1.State_STATE_FAILED,
		progressStateSet: true,
		progressError:    "copy row chunk: disk full",
		// No progressTables: the terminal remote apply no longer reports tasks.
	})
	defer cleanup()

	apply := &storage.Apply{
		ID:              32,
		ApplyIdentifier: "apply-failed-terminal-remote",
		Database:        "testdb",
		Environment:     "staging",
		ExternalID:      "remote-failed-terminal-apply",
		State:           state.Apply.Running,
	}
	storedApply := *apply
	startedTask := &storage.Task{
		ID:             42,
		TaskIdentifier: "task-failed-terminal-started",
		ApplyID:        apply.ID,
		TableName:      "users",
		State:          state.Task.Running,
	}
	pendingTask := &storage.Task{
		ID:             43,
		TaskIdentifier: "task-failed-terminal-pending",
		ApplyID:        apply.ID,
		TableName:      "orders",
		State:          state.Task.Pending,
	}
	applyStore := &mockApplyStore{apply: &storedApply}
	client.storage = &mockStorage{
		applies: applyStore,
		tasks:   &mockTaskStore{tasks: []*storage.Task{startedTask, pendingTask}},
		logs:    &mockApplyLogStore{},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.pollForCompletion(ctx, apply, false, wholeApplyTaskScope(), false)
	require.NoError(t, err)

	assert.Equal(t, state.Apply.Failed, applyStore.apply.State)
	assert.Equal(t, state.Task.Failed, startedTask.State,
		"a task that was in flight when the apply died fails with it")
	require.NotNil(t, startedTask.CompletedAt)
	assert.Equal(t, state.Task.Cancelled, pendingTask.State,
		"a task that never started is cancelled, not failed")
	require.NotNil(t, pendingTask.CompletedAt)
}

// terminalTaskStateForApply must map every terminal apply state a remote can
// report — including the less-common cancelled and reverted outcomes — to the
// task state a lagging stored task adopts. A terminal apply state with no
// mapping would make terminal reconciliation error and re-poll the already
// terminal remote forever.
func TestTerminalTaskStateForApply(t *testing.T) {
	tests := []struct {
		name       string
		applyState string
		wantTask   string
		wantOK     bool
	}{
		{name: "completed", applyState: state.Apply.Completed, wantTask: state.Task.Completed, wantOK: true},
		{name: "stopped", applyState: state.Apply.Stopped, wantTask: state.Task.Stopped, wantOK: true},
		{name: "failed", applyState: state.Apply.Failed, wantTask: state.Task.Failed, wantOK: true},
		{name: "cancelled", applyState: state.Apply.Cancelled, wantTask: state.Task.Cancelled, wantOK: true},
		{name: "reverted", applyState: state.Apply.Reverted, wantTask: state.Task.Reverted, wantOK: true},
		{name: "running is not terminal", applyState: state.Apply.Running, wantOK: false},
		{name: "failed-retryable is not terminal", applyState: state.Apply.FailedRetryable, wantOK: false},
		{name: "pending is not terminal", applyState: state.Apply.Pending, wantOK: false},
		{name: "waiting-for-cutover is not terminal", applyState: state.Apply.WaitingForCutover, wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := terminalTaskStateForApply(tt.applyState)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantTask, got)
		})
	}

	// Every terminal apply state in the registry must have a task mapping so the
	// reconcile path can never error on a terminal remote apply.
	for _, applyState := range []string{
		state.Apply.Completed, state.Apply.Failed, state.Apply.Stopped,
		state.Apply.Cancelled, state.Apply.Reverted,
	} {
		require.True(t, state.IsTerminalApplyState(applyState), "%q should be a terminal apply state", applyState)
		_, ok := terminalTaskStateForApply(applyState)
		assert.True(t, ok, "terminal apply state %q must map to a task state", applyState)
	}
}

func hasLogMessageContaining(logs []*storage.ApplyLog, want string) bool {
	for _, log := range logs {
		if strings.Contains(log.Message, want) {
			return true
		}
	}
	return false
}

func hasLogEvent(logs []*storage.ApplyLog, eventType string) bool {
	for _, log := range logs {
		if log.EventType == eventType {
			return true
		}
	}
	return false
}

func TestGRPCClient_PollReturnsTerminalStorageUpdateError(t *testing.T) {
	// A terminal remote state is not enough by itself; the control plane must
	// persist that terminal state to storage before the operator driver exits.
	client, cleanup := testCapturingGRPCClient(t, &capturingTernServer{
		progressState:    ternv1.State_STATE_COMPLETED,
		progressStateSet: true,
	})
	defer cleanup()

	apply := &storage.Apply{
		ID:              18,
		ApplyIdentifier: "apply-terminal-storage-error",
		Database:        "testdb",
		Environment:     "staging",
		ExternalID:      "remote-terminal-storage-error",
		State:           state.Apply.Running,
	}
	storedApply := *apply
	client.storage = &mockStorage{
		applies: &mockApplyStore{
			apply:     &storedApply,
			updateErr: errors.New("storage unavailable"),
		},
		tasks: &mockTaskStore{},
		logs:  &mockApplyLogStore{},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.pollForCompletion(ctx, apply, false, wholeApplyTaskScope(), false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "update terminal remote gRPC apply")
	assert.Contains(t, err.Error(), "storage unavailable")
}

func TestGRPCClient_PollKeepsApplyActiveWhenTerminalTaskLoadFails(t *testing.T) {
	// Terminal remote progress is only fully reconciled once stored task rows are
	// updated too. If task storage fails, the apply should remain active so a
	// later operator attempt can finish reconciliation.
	client, cleanup := testCapturingGRPCClient(t, &capturingTernServer{
		progressState:    ternv1.State_STATE_COMPLETED,
		progressStateSet: true,
	})
	defer cleanup()

	apply := &storage.Apply{
		ID:              19,
		ApplyIdentifier: "apply-terminal-task-load-error",
		Database:        "testdb",
		Environment:     "staging",
		ExternalID:      "remote-terminal-task-load-error",
		State:           state.Apply.Running,
	}
	storedApply := *apply
	applyStore := &mockApplyStore{apply: &storedApply}
	client.storage = &mockStorage{
		applies: applyStore,
		tasks: &mockTaskStore{
			getByApplyIDErr: errors.New("task storage unavailable"),
		},
		logs: &mockApplyLogStore{},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.pollForCompletion(ctx, apply, false, wholeApplyTaskScope(), false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "load tasks to sync terminal gRPC progress")
	assert.Contains(t, err.Error(), "task storage unavailable")
	assert.Equal(t, state.Apply.Running, applyStore.apply.State)
	assert.Nil(t, applyStore.apply.CompletedAt)
}

func TestGRPCClient_PollSkipsTaskFinalizationWhenStoredApplyAlreadyTerminal(t *testing.T) {
	// A stale driver can receive a terminal remote state after another driver
	// has already terminalized the stored apply row. In that case the driver must
	// not rewrite tasks from its stale in-memory apply state.
	client, cleanup := testCapturingGRPCClient(t, &capturingTernServer{
		progressState:    ternv1.State_STATE_COMPLETED,
		progressStateSet: true,
	})
	defer cleanup()

	apply := &storage.Apply{
		ID:              19,
		ApplyIdentifier: "apply-terminal-race",
		Database:        "testdb",
		Environment:     "staging",
		ExternalID:      "remote-terminal-race",
		State:           state.Apply.Running,
	}
	storedApply := *apply
	storedApply.State = state.Apply.Failed
	task := &storage.Task{
		ID:             23,
		TaskIdentifier: "task-terminal-race",
		ApplyID:        apply.ID,
		TableName:      "users",
		State:          state.Task.Running,
	}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: &storedApply},
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
		logs:    &mockApplyLogStore{},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.pollForCompletion(ctx, apply, false, wholeApplyTaskScope(), false)
	require.NoError(t, err)

	assert.Equal(t, state.Apply.Failed, apply.State)
	assert.Equal(t, state.Task.Running, task.State)
	assert.Nil(t, task.CompletedAt)
}

func TestGRPCClient_MarkRemoteApplyFailedReturnsTaskLoadError(t *testing.T) {
	// A remote failure is only safe to store after the control plane can update
	// both apply and task rows. Task storage uncertainty should make the caller
	// retry instead of leaving unfinished tasks behind.
	apply := &storage.Apply{
		ID:              20,
		ApplyIdentifier: "apply-task-load-error",
		Database:        "testdb",
		Environment:     "staging",
		ExternalID:      "remote-task-load-error",
		State:           state.Apply.Running,
	}
	applyStore := &mockApplyStore{apply: apply}
	client := &GRPCClient{
		storage: &mockStorage{
			applies: applyStore,
			tasks: &mockTaskStore{
				getByApplyIDErr: errors.New("task storage unavailable"),
			},
			logs: &mockApplyLogStore{},
		},
	}

	err := client.markRemoteApplyFailed(t.Context(), apply, nil, "remote failed", false, wholeApplyTaskScope())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "load tasks after remote gRPC apply failed")
	assert.Contains(t, err.Error(), "task storage unavailable")
	assert.Equal(t, state.Apply.Running, applyStore.apply.State)
}

func TestGRPCClient_ResumeApplyRejectsAmbiguousRemoteDispatchState(t *testing.T) {
	// A stale active gRPC apply without an external_id is ambiguous: the prior
	// driver may have sent the remote Apply RPC and crashed before persisting the
	// returned data-plane ID. Fail closed instead of dispatching a duplicate
	// remote schema change.
	server := &capturingTernServer{remoteApplyID: "remote-duplicate"}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              8,
		ApplyIdentifier: "apply-ambiguous-dispatch",
		PlanID:          100,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	task := &storage.Task{
		ID:             12,
		TaskIdentifier: "task-ambiguous-dispatch",
		ApplyID:        apply.ID,
		TableName:      "users",
		State:          state.Task.Running,
	}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-ambiguous-dispatch",
		}},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.ResumeApply(ctx, apply)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "remote dispatch state is ambiguous")

	assert.Nil(t, server.getApplyRequest(), "ambiguous apply should not be dispatched to remote Tern")
	assert.Equal(t, state.Apply.Failed, apply.State)
	assert.Contains(t, apply.ErrorMessage, "remote dispatch state is ambiguous")
	assert.Equal(t, state.Task.Failed, task.State)
	assert.Contains(t, task.ErrorMessage, "remote dispatch state is ambiguous")
}

func TestGRPCClient_ResumeApplyDoesNotFailStateWhenRemoteDispatchOutcomeIsAmbiguous(t *testing.T) {
	// Cancellation or deadline from the remote Apply RPC does not prove whether
	// the data plane accepted the schema change. Leave stored state unchanged
	// so the operator does not record a false terminal failure.
	server := &capturingTernServer{
		applyErr: status.Error(codes.DeadlineExceeded, "deadline waiting for response"),
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              9,
		ApplyIdentifier: "apply-ambiguous-rpc",
		PlanID:          101,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	task := &storage.Task{
		ID:             13,
		TaskIdentifier: "task-ambiguous-rpc",
		ApplyID:        apply.ID,
		TableName:      "users",
		State:          state.Task.Pending,
	}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-ambiguous-rpc",
		}},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.ResumeApply(ctx, apply)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ambiguous remote dispatch outcome")

	require.NotNil(t, server.getApplyRequest(), "expected Apply RPC to be attempted")
	assert.Equal(t, state.Apply.Pending, apply.State)
	assert.Empty(t, apply.ErrorMessage)
	assert.Equal(t, state.Task.Pending, task.State)
	assert.Empty(t, task.ErrorMessage)
}

func TestGRPCClient_ResumeApplyClassifiesRemoteDispatchErrors(t *testing.T) {
	// When remote dispatch is rejected before the data plane accepts work, the
	// control plane records the failure using the gRPC status code. Retryable
	// status codes stay claimable for the operator; known-permanent status
	// codes become terminal failures.
	testCases := []struct {
		name            string
		code            codes.Code
		message         string
		wantApplyState  string
		wantTaskState   string
		wantCompletedAt bool
	}{
		{
			name:           "retryable remote error",
			code:           codes.Internal,
			message:        "remote apply rejected",
			wantApplyState: state.Apply.FailedRetryable,
			wantTaskState:  state.Task.FailedRetryable,
		},
		{
			name:            "permanent remote error",
			code:            codes.FailedPrecondition,
			message:         "remote apply rejected",
			wantApplyState:  state.Apply.Failed,
			wantTaskState:   state.Task.Failed,
			wantCompletedAt: true,
		},
		{
			name:            "permanent status with transient-looking message",
			code:            codes.FailedPrecondition,
			message:         "Too many requests for this deploy request",
			wantApplyState:  state.Apply.Failed,
			wantTaskState:   state.Task.Failed,
			wantCompletedAt: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server := &capturingTernServer{
				applyErr: status.Error(tc.code, tc.message),
			}
			client, cleanup := testCapturingGRPCClient(t, server)
			defer cleanup()

			apply := &storage.Apply{
				ID:              10,
				ApplyIdentifier: "apply-classify-remote-error",
				PlanID:          102,
				Database:        "testdb",
				DatabaseType:    storage.DatabaseTypeMySQL,
				Environment:     "staging",
				State:           state.Apply.Pending,
			}
			task := &storage.Task{
				ID:             14,
				TaskIdentifier: "task-classify-remote-error",
				ApplyID:        apply.ID,
				TableName:      "users",
				State:          state.Task.Pending,
			}
			client.storage = &mockStorage{
				applies: &mockApplyStore{apply: apply},
				tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
				plans: &mockPlanStore{plan: &storage.Plan{
					ID:             apply.PlanID,
					PlanIdentifier: "plan-classify-remote-error",
				}},
			}

			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			err := client.ResumeApply(ctx, apply)
			require.Error(t, err)

			require.NotNil(t, server.getApplyRequest(), "expected Apply RPC to be attempted")
			assert.Equal(t, tc.wantApplyState, apply.State)
			assert.Contains(t, apply.ErrorMessage, tc.message)
			assert.Equal(t, tc.wantCompletedAt, apply.CompletedAt != nil)
			assert.Equal(t, tc.wantTaskState, task.State)
			assert.Contains(t, task.ErrorMessage, tc.message)
			assert.Equal(t, tc.wantCompletedAt, task.CompletedAt != nil)
		})
	}
}

func TestGRPCClient_QueuedRemoteDispatchPredicate(t *testing.T) {
	tests := []struct {
		name       string
		state      string
		externalID string
		want       bool
	}{
		{name: "pending without remote id", state: state.Apply.Pending, want: true},
		{name: "retryable without remote id", state: state.Apply.FailedRetryable, want: true},
		{name: "running without remote id", state: state.Apply.Running, want: false},
		{name: "pending with remote id", state: state.Apply.Pending, externalID: "remote-apply-123", want: false},
		{name: "terminal without remote id", state: state.Apply.Completed, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			apply := &storage.Apply{
				State:      tt.state,
				ExternalID: tt.externalID,
			}
			assert.Equal(t, tt.want, shouldDispatchQueuedRemoteApply(apply, wholeApplyTaskScope()))
		})
	}
}

// TestGRPCClient_QueuedRemoteDispatchPredicate_OperationScope pins the dispatch
// gate for multi-operation drives. The operator claim transitions an operation
// pending→running in a separate transaction before the drive runs, so a freshly
// claimed operation reaches dispatch in running with no per-operation remote id
// yet. That first dispatch must proceed — a running operation with an empty
// remote id is not the ambiguous crash case the whole-apply path rejects.
func TestGRPCClient_QueuedRemoteDispatchPredicate_OperationScope(t *testing.T) {
	tests := []struct {
		name     string
		opState  string
		remoteID string
		multiOp  bool
		want     bool
	}{
		{name: "multi-op running without remote id dispatches", opState: state.ApplyOperation.Running, multiOp: true, want: true},
		{name: "multi-op pending without remote id dispatches", opState: state.ApplyOperation.Pending, multiOp: true, want: true},
		{name: "multi-op running with remote id does not dispatch", opState: state.ApplyOperation.Running, remoteID: "remote-apply-123", multiOp: true, want: false},
		{name: "multi-op terminal without remote id does not dispatch", opState: state.ApplyOperation.Completed, multiOp: true, want: false},
		{name: "single-op running without remote id does not dispatch", opState: state.ApplyOperation.Running, multiOp: false, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The parent apply is running because a sibling deployment is active;
			// the per-operation remote id (not apply.ExternalID) governs dispatch.
			apply := &storage.Apply{State: state.Apply.Running}
			scope := applyTaskScope{
				applyOperationID: 1,
				operation: &storage.ApplyOperation{
					State:               tt.opState,
					EngineResumeContext: tt.remoteID,
				},
				multiOperation: tt.multiOp,
			}
			assert.Equal(t, tt.want, shouldDispatchQueuedRemoteApply(apply, scope))
		})
	}
}

func TestGRPCClient_ResumeApply_ThreadsExternalID(t *testing.T) {
	// Progress returns STOPPED initially so ResumeApply checks remote state,
	// confirms the apply is stopped, and — with the operator's start request
	// pending — calls Start. After Start, the mock transitions to COMPLETED so
	// the poller exits.
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_STOPPED,
		progressStateSet: true,
	}

	// Start a test gRPC server
	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "localhost:0")
	require.NoError(t, err, "failed to listen")

	grpcServer := grpc.NewServer()
	ternv1.RegisterTernServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err, "failed to dial")

	apply := &storage.Apply{
		ID:          1,
		Database:    "testdb",
		Environment: "staging",
		ExternalID:  "remote_tern_xyz",
		State:       state.Apply.Stopped,
	}
	storedApply := *apply
	client := &GRPCClient{
		conn:   conn,
		client: ternv1.NewTernClient(conn),
		storage: &mockStorage{
			applies: &mockApplyStore{apply: &storedApply},
			tasks:   &mockTaskStore{},
			logs:    &mockApplyLogStore{},
			controlRequests: &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
				ApplyID:     apply.ID,
				Operation:   storage.ControlOperationStart,
				Status:      storage.ControlRequestPending,
				RequestedBy: "cli:alice",
			}}},
		},
	}
	defer utils.CloseAndLog(client)

	err = client.ResumeApply(t.Context(), apply)
	require.NoError(t, err)

	// Verify Start received the external_id as apply_id
	assert.Equal(t, "remote_tern_xyz", server.getStartApplyID())

	// Progress returns STATE_COMPLETED after Start, so ResumeApply exits after
	// syncing one terminal progress response.
	assert.Equal(t, "remote_tern_xyz", server.getProgressApplyID())
}

func TestGRPCClient_ResumeApplyStartsQueuedStartAfterClaim(t *testing.T) {
	// An operator claim can move the apply row before the driver calls remote
	// Start. The durable control request lets a later driver recover that
	// intent and validate the remote stopped state.
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_STOPPED,
		progressStateSet: true,
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              1,
		ApplyIdentifier: "apply-start-claimed",
		Database:        "testdb",
		Environment:     "staging",
		ExternalID:      "remote-start-claimed",
		State:           state.Apply.Running,
	}
	storedApply := *apply
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:   apply.ID,
		Operation: storage.ControlOperationStart,
		Status:    storage.ControlRequestPending,
	}}}
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: &storedApply},
		tasks:           &mockTaskStore{},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
	}

	err := client.ResumeApply(t.Context(), apply)
	require.NoError(t, err)

	assert.Equal(t, "remote-start-claimed", server.getStartApplyID())
	controlReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationStart)
	require.NoError(t, err)
	assert.Nil(t, controlReq)
}

func TestGRPCClient_ResumeApplyPublishesResumingUntilDataPlaneLeavesStopped(t *testing.T) {
	// A start accepted by the data plane may still report stopped for a short
	// window. The control plane must publish resuming, not running, during that
	// window so /api/status and /api/progress/apply/{id} stay consistent until
	// the data plane actually leaves stopped, then transition to running.
	server := &capturingTernServer{
		progressStates: []ternv1.State{
			ternv1.State_STATE_STOPPED,   // pre-start stopped-state check
			ternv1.State_STATE_STOPPED,   // first poll: still stopped (grace window)
			ternv1.State_STATE_RUNNING,   // data plane leaves stopped
			ternv1.State_STATE_COMPLETED, // terminal
		},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              1,
		ApplyIdentifier: "apply-resuming-window",
		Database:        "testdb",
		Environment:     "staging",
		ExternalID:      "remote-resuming-window",
		State:           state.Apply.Stopped,
	}
	storedApply := *apply
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:   apply.ID,
		Operation: storage.ControlOperationStart,
		Status:    storage.ControlRequestPending,
	}}}
	applyStore := &mockApplyStore{apply: &storedApply}
	client.storage = &mockStorage{
		applies:         applyStore,
		tasks:           &mockTaskStore{},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
	}

	err := client.ResumeApply(t.Context(), apply)
	require.NoError(t, err)

	persistedStates := make([]string, 0, len(applyStore.updates))
	for _, u := range applyStore.updates {
		persistedStates = append(persistedStates, u.State)
	}
	require.Contains(t, persistedStates, state.Apply.Resuming,
		"the resume must publish resuming before running while the data plane is still stopped")

	resumingIdx := -1
	runningIdx := -1
	for i, s := range persistedStates {
		if resumingIdx == -1 && state.IsState(s, state.Apply.Resuming) {
			resumingIdx = i
		}
		if runningIdx == -1 && state.IsState(s, state.Apply.Running) {
			runningIdx = i
		}
	}
	require.NotEqual(t, -1, runningIdx, "the resume must eventually publish running")
	assert.Less(t, resumingIdx, runningIdx, "resuming must be published before running")

	assert.Equal(t, "remote-resuming-window", server.getStartApplyID())
	startReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationStart)
	require.NoError(t, err)
	assert.Nil(t, startReq, "the start control request must be completed once the resume is driven")
}

func TestGRPCClient_RequeueStoppedTasksForRemoteStart(t *testing.T) {
	// When the data plane accepts a start, the gRPC drive must requeue the apply's
	// stopped task rows to pending. Otherwise taskStateWithNoBackwardProgress pins
	// them at stopped on every later progress poll (stopped blocks active engine
	// progress), so the resumed row copy never surfaces and the PR comment keeps
	// rendering "Stopped" while the data plane copies. Tasks in other states are
	// left untouched.
	client, cleanup := testCapturingGRPCClient(t, &capturingTernServer{})
	defer cleanup()

	completedAt := time.Now()
	apply := &storage.Apply{
		ID:              1,
		ApplyIdentifier: "apply-requeue-stopped",
		Database:        "testdb",
		Environment:     "staging",
		State:           state.Apply.Resuming,
	}
	taskStore := &mockTaskStore{tasks: []*storage.Task{
		{TaskIdentifier: "t-stopped", ApplyID: 1, State: state.Task.Stopped, TableName: "users", CompletedAt: &completedAt},
		{TaskIdentifier: "t-completed", ApplyID: 1, State: state.Task.Completed, TableName: "orders"},
	}}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   taskStore,
		logs:    &mockApplyLogStore{},
	}

	err := client.requeueStoppedTasksForRemoteStart(t.Context(), apply, wholeApplyTaskScope())
	require.NoError(t, err)

	assert.Equal(t, state.Task.Pending, taskStore.tasks[0].State, "stopped task must be requeued to pending on resume")
	assert.Nil(t, taskStore.tasks[0].CompletedAt, "requeued task must clear its completed timestamp")
	assert.Equal(t, state.Task.Completed, taskStore.tasks[1].State, "non-stopped task must be left untouched")
}

func TestGRPCClient_ResumeApplyCompletesQueuedStopBeforeQueuedStart(t *testing.T) {
	// Start can arrive immediately after stop progress is visible. The operator
	// should consume the resolved stop request and continue with the queued start
	// in the same claim instead of requiring another scheduler pass.
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_STOPPED,
		progressStateSet: true,
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              1,
		ApplyIdentifier: "apply-start-after-stop",
		Database:        "testdb",
		Environment:     "staging",
		ExternalID:      "remote-start-after-stop",
		State:           state.Apply.Running,
	}
	storedApply := *apply
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{
		{
			ApplyID:     apply.ID,
			Operation:   storage.ControlOperationStop,
			Status:      storage.ControlRequestPending,
			RequestedBy: "stop-caller",
		},
		{
			ApplyID:     apply.ID,
			Operation:   storage.ControlOperationStart,
			Status:      storage.ControlRequestPending,
			RequestedBy: "start-caller",
		},
	}}
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: &storedApply},
		tasks:           &mockTaskStore{},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
	}

	err := client.ResumeApply(t.Context(), apply)
	require.NoError(t, err)

	assert.Equal(t, "remote-start-after-stop", server.getStartApplyID())
	stopReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
	require.NoError(t, err)
	assert.Nil(t, stopReq)
	startReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationStart)
	require.NoError(t, err)
	assert.Nil(t, startReq)
}

func TestGRPCClient_ResumeApplyStartsDeferredDeployFromPendingRequest(t *testing.T) {
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_COMPLETED,
		progressStateSet: true,
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              1,
		ApplyIdentifier: "apply-start-deploy",
		Database:        "testdb",
		Environment:     "staging",
		ExternalID:      "remote-start-deploy",
		State:           state.Apply.WaitingForDeploy,
	}
	storedApply := *apply
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:   apply.ID,
		Operation: storage.ControlOperationStart,
		Status:    storage.ControlRequestPending,
	}}}
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: &storedApply},
		tasks:           &mockTaskStore{},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
	}

	err := client.ResumeApply(t.Context(), apply)
	require.NoError(t, err)

	assert.Equal(t, "remote-start-deploy", server.getStartApplyID())
	controlReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationStart)
	require.NoError(t, err)
	assert.Nil(t, controlReq)
}

func TestGRPCClient_ResumeApplyStartErrorLeavesApplyStopped(t *testing.T) {
	// When the operator accepts a stored start request but remote Tern rejects
	// the Start RPC, keep the apply stopped with a visible reason and leave the
	// start request pending for a later retry/reconciliation attempt.
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_STOPPED,
		progressStateSet: true,
		progressTables: []*ternv1.TableProgress{{
			Namespace:       "default",
			TableName:       "users",
			Status:          state.Task.Stopped,
			PercentComplete: 35,
		}},
		startErr: status.Error(codes.Unavailable, "remote start unavailable"),
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              1,
		ApplyIdentifier: "apply-start-error",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeVitess,
		Environment:     "staging",
		ExternalID:      "remote-start-error",
		State:           state.Apply.Running,
	}
	storedApply := *apply
	task := &storage.Task{
		ID:             2,
		TaskIdentifier: "task-start-error",
		ApplyID:        apply.ID,
		Namespace:      "default",
		TableName:      "users",
		State:          state.Task.Stopped,
	}
	logs := &mockApplyLogStore{}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:   apply.ID,
		Operation: storage.ControlOperationStart,
		Status:    storage.ControlRequestPending,
	}}}
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: &storedApply},
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            logs,
		controlRequests: controlRequests,
	}

	err := client.ResumeApply(t.Context(), apply)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "remote start unavailable")
	assert.Equal(t, state.Apply.Stopped, apply.State)
	assert.Contains(t, apply.ErrorMessage, "remote start failed")
	assert.Equal(t, state.Task.Stopped, task.State)
	assert.Equal(t, 35, task.ProgressPercent)
	assert.True(t, hasLogMessageContaining(logs.logs, "remote start failed for remote apply remote-start-error"))
	pendingStart, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationStart)
	require.NoError(t, err)
	assert.Nil(t, pendingStart)
	require.Len(t, controlRequests.requests, 1)
	assert.Equal(t, storage.ControlRequestFailed, controlRequests.requests[0].Status)
	assert.Contains(t, controlRequests.requests[0].ErrorMessage, "remote start failed")
}

func TestGRPCClient_ResumeApplyProcessesQueuedStop(t *testing.T) {
	// A pending durable stop is processed by the operator-owned driver before
	// resume/start work. The driver mirrors remote stopped progress to storage
	// before completing the durable request.
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_STOPPED,
		progressStateSet: true,
		progressTables: []*ternv1.TableProgress{{
			TableName: "users",
			Status:    state.Task.Stopped,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              1,
		ApplyIdentifier: "apply-stop-claimed",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		ExternalID:      "remote-stop-claimed",
		State:           state.Apply.Running,
	}
	storedApply := *apply
	task := &storage.Task{
		ID:             1,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-stop-claimed",
		TableName:      "users",
		State:          state.Task.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStop,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	logs := &mockApplyLogStore{}
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: &storedApply},
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            logs,
		controlRequests: controlRequests,
	}

	err := client.ResumeApply(t.Context(), apply)
	require.NoError(t, err)

	assert.Equal(t, "remote-stop-claimed", server.getStopApplyID())
	assert.Equal(t, "remote-stop-claimed", server.getProgressApplyID())
	assert.Equal(t, state.Apply.Stopped, apply.State)
	assert.Equal(t, state.Task.Stopped, task.State)
	controlReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
	require.NoError(t, err)
	assert.Nil(t, controlReq)
	assert.True(t, hasLogMessageContaining(logs.logs, "Remote stop accepted for apply remote-stop-claimed (caller: cli:alice)"))
}

func TestGRPCClient_ResumeApplyProcessesQueuedCutover(t *testing.T) {
	// A pending durable cutover is processed by the operator-owned driver using
	// the remote apply ID, then completed once remote Tern accepts the request.
	server := &capturingTernServer{
		cutoverAccepted:  true,
		progressState:    ternv1.State_STATE_COMPLETED,
		progressStateSet: true,
		progressTables: []*ternv1.TableProgress{{
			TableName:       "users",
			Status:          state.Task.Completed,
			PercentComplete: 100,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              1,
		ApplyIdentifier: "apply-cutover-claimed",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeVitess,
		Environment:     "staging",
		ExternalID:      "remote-cutover-claimed",
		State:           state.Apply.WaitingForCutover,
	}
	storedApply := *apply
	task := &storage.Task{
		ID:             1,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-cutover-claimed",
		TableName:      "users",
		State:          state.Task.WaitingForCutover,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCutover,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	logs := &mockApplyLogStore{}
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: &storedApply},
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            logs,
		controlRequests: controlRequests,
	}

	err := client.ResumeApply(t.Context(), apply)
	require.NoError(t, err)

	assert.Equal(t, "remote-cutover-claimed", server.getCutoverApplyID())
	controlReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCutover)
	require.NoError(t, err)
	assert.Nil(t, controlReq)
	assert.True(t, hasLogMessageContaining(logs.logs, "Remote cutover accepted for apply apply-cutover-claimed (remote remote-cutover-claimed) (caller: cli:alice)"))
}

func TestGRPCClient_ProcessPendingCutoverWaitsWhenNotReady(t *testing.T) {
	// A transient running sample after cutover was requested should not fail the
	// durable request. The operator will retry after the next progress sync.
	server := &capturingTernServer{}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              1,
		ApplyIdentifier: "apply-cutover-wait-grpc",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeVitess,
		Environment:     "staging",
		ExternalID:      "remote-cutover-wait-grpc",
		State:           state.Apply.Running,
	}
	storedApply := *apply
	task := &storage.Task{
		ID:             1,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-cutover-wait-grpc",
		TableName:      "users",
		State:          state.Task.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCutover,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: &storedApply},
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
	}

	err := client.processPendingCutoverControlRequest(t.Context(), apply, wholeApplyTaskScope())
	require.NoError(t, err)
	assert.Empty(t, server.getCutoverApplyID())
	pendingCutover, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCutover)
	require.NoError(t, err)
	require.NotNil(t, pendingCutover)
	assert.Equal(t, storage.ControlRequestPending, pendingCutover.Status)
}

// A pending skip-revert request on a revert-window apply is the operator's retry
// path: the drive proxies SkipRevert to the data plane, records revert_skipped,
// and completes the request. Once the apply has left the revert window, the
// request is moot and is completed without another engine call.
func TestGRPCClient_ProcessPendingSkipRevert(t *testing.T) {
	newApply := func(s string) *storage.Apply {
		return &storage.Apply{
			ID: 1, ApplyIdentifier: "apply-skiprevert-grpc",
			Database: "testdb", DatabaseType: storage.DatabaseTypeVitess,
			Environment: "staging", Engine: storage.EnginePlanetScale,
			ExternalID: "remote-skiprevert-grpc", State: s,
		}
	}
	newStore := func(apply *storage.Apply) (*mockStorage, *testControlRequestStore, *mockApplyStore) {
		controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
			ApplyID: apply.ID, Operation: storage.ControlOperationSkipRevert,
			Status: storage.ControlRequestPending, RequestedBy: "cli:alice",
		}}}
		stored := *apply
		applies := &mockApplyStore{apply: &stored}
		return &mockStorage{applies: applies, tasks: &mockTaskStore{}, logs: &mockApplyLogStore{}, controlRequests: controlRequests}, controlRequests, applies
	}

	t.Run("revert window: proxies skip and completes the request", func(t *testing.T) {
		server := &capturingTernServer{}
		client, cleanup := testCapturingGRPCClient(t, server)
		defer cleanup()
		apply := newApply(state.Apply.RevertWindow)
		store, controlRequests, applies := newStore(apply)
		client.storage = store

		err := client.processPendingSkipRevertControlRequest(t.Context(), apply, apply.ExternalID)
		require.NoError(t, err)
		assert.Equal(t, "remote-skiprevert-grpc", server.getSkipRevertApplyID(), "skip-revert proxied to the data plane")
		assert.NotNil(t, applies.revertSkippedAt, "revert_skipped recorded")
		pending, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationSkipRevert)
		require.NoError(t, err)
		assert.Nil(t, pending, "request completed after a successful skip")
	})

	t.Run("apply already left the revert window: completes without an engine call", func(t *testing.T) {
		server := &capturingTernServer{}
		client, cleanup := testCapturingGRPCClient(t, server)
		defer cleanup()
		apply := newApply(state.Apply.Completed)
		store, controlRequests, _ := newStore(apply)
		client.storage = store

		err := client.processPendingSkipRevertControlRequest(t.Context(), apply, apply.ExternalID)
		require.NoError(t, err)
		assert.Empty(t, server.getSkipRevertApplyID(), "no engine call once the revert window is gone")
		pending, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationSkipRevert)
		require.NoError(t, err)
		assert.Nil(t, pending, "moot request completed")
	})
}

func TestGRPCClient_ProcessPendingRevert(t *testing.T) {
	newApply := func(s string) *storage.Apply {
		return &storage.Apply{
			ID: 1, ApplyIdentifier: "apply-revert-grpc",
			Database: "testdb", DatabaseType: storage.DatabaseTypeVitess,
			Environment: "staging", Engine: storage.EnginePlanetScale,
			ExternalID: "remote-revert-grpc", State: s,
		}
	}
	newStore := func(apply *storage.Apply) (*mockStorage, *testControlRequestStore) {
		controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
			ApplyID: apply.ID, Operation: storage.ControlOperationRevert,
			Status: storage.ControlRequestPending, RequestedBy: "cli:alice",
		}}}
		stored := *apply
		applies := &mockApplyStore{apply: &stored}
		return &mockStorage{applies: applies, tasks: &mockTaskStore{}, logs: &mockApplyLogStore{}, controlRequests: controlRequests}, controlRequests
	}

	t.Run("revert window: proxies revert and completes the request", func(t *testing.T) {
		server := &capturingTernServer{}
		client, cleanup := testCapturingGRPCClient(t, server)
		defer cleanup()
		apply := newApply(state.Apply.RevertWindow)
		store, controlRequests := newStore(apply)
		client.storage = store

		err := client.processPendingRevertControlRequest(t.Context(), apply, apply.ExternalID)
		require.NoError(t, err)
		assert.Equal(t, "remote-revert-grpc", server.getRevertApplyID(), "revert proxied to the data plane")
		pending, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationRevert)
		require.NoError(t, err)
		assert.Nil(t, pending, "request completed after a successful revert")
	})

	t.Run("apply already left the revert window: completes without an engine call", func(t *testing.T) {
		server := &capturingTernServer{}
		client, cleanup := testCapturingGRPCClient(t, server)
		defer cleanup()
		apply := newApply(state.Apply.Completed)
		store, controlRequests := newStore(apply)
		client.storage = store

		err := client.processPendingRevertControlRequest(t.Context(), apply, apply.ExternalID)
		require.NoError(t, err)
		assert.Empty(t, server.getRevertApplyID(), "no engine call once the revert window is gone")
		pending, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationRevert)
		require.NoError(t, err)
		assert.Nil(t, pending, "moot request completed")
	})
}

func TestGRPCClient_ProcessPendingCutoverWaitsWhileRecovering(t *testing.T) {
	server := &capturingTernServer{}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              1,
		ApplyIdentifier: "apply-cutover-recovering-grpc",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeVitess,
		Environment:     "staging",
		ExternalID:      "remote-cutover-recovering-grpc",
		State:           state.Apply.Recovering,
	}
	storedApply := *apply
	task := &storage.Task{
		ID:             1,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-cutover-recovering-grpc",
		TableName:      "users",
		State:          state.Task.Recovering,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCutover,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: &storedApply},
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
	}

	err := client.processPendingCutoverControlRequest(t.Context(), apply, wholeApplyTaskScope())
	require.NoError(t, err)
	assert.Empty(t, server.getCutoverApplyID())
	pendingCutover, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCutover)
	require.NoError(t, err)
	require.NotNil(t, pendingCutover)
	assert.Equal(t, storage.ControlRequestPending, pendingCutover.Status)
}

func TestGRPCClient_ResumeApplyCutoverErrorFailsPendingRequest(t *testing.T) {
	// A cutover RPC failure leaves a visible failed control request so the
	// operator does not retry indefinitely without a new operator request.
	server := &capturingTernServer{
		cutoverErr: status.Error(codes.Unavailable, "remote cutover unavailable"),
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              1,
		ApplyIdentifier: "apply-cutover-error",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeVitess,
		Environment:     "staging",
		ExternalID:      "remote-cutover-error",
		State:           state.Apply.WaitingForCutover,
	}
	storedApply := *apply
	task := &storage.Task{
		ID:             1,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-cutover-error",
		TableName:      "users",
		State:          state.Task.WaitingForCutover,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationCutover,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	logs := &mockApplyLogStore{}
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: &storedApply},
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            logs,
		controlRequests: controlRequests,
	}

	err := client.ResumeApply(t.Context(), apply)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "remote cutover unavailable")
	assert.Equal(t, "remote-cutover-error", server.getCutoverApplyID())
	pendingCutover, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCutover)
	require.NoError(t, err)
	assert.Nil(t, pendingCutover)
	require.Len(t, controlRequests.requests, 1)
	assert.Equal(t, storage.ControlRequestFailed, controlRequests.requests[0].Status)
	assert.Contains(t, controlRequests.requests[0].ErrorMessage, "remote cutover failed")
	assert.True(t, hasLogMessageContaining(logs.logs, "Remote cutover failed for apply apply-cutover-error (remote remote-cutover-error) (caller: cli:alice)"))
}

func TestGRPCClient_ResumeApplyCompletesQueuedStartWhenRemoteAlreadyActive(t *testing.T) {
	// An operator can start the remote apply directly after SchemaBot records
	// durable start intent. The operator adopts the active remote state instead
	// of sending another Start request, then continues polling the exact apply ID.
	server := &capturingTernServer{
		progressStates: []ternv1.State{
			ternv1.State_STATE_RUNNING,
			ternv1.State_STATE_COMPLETED,
		},
		progressTables: []*ternv1.TableProgress{{
			Namespace:       "default",
			TableName:       "users",
			Status:          state.Task.Completed,
			PercentComplete: 100,
		}},
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              1,
		ApplyIdentifier: "apply-start-already-active",
		Database:        "testdb",
		Environment:     "staging",
		ExternalID:      "remote-start-already-active",
		State:           state.Apply.Running,
	}
	storedApply := *apply
	task := &storage.Task{
		ID:             31,
		TaskIdentifier: "task-start-already-active",
		ApplyID:        apply.ID,
		TableName:      "users",
		Namespace:      "default",
		State:          state.Task.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:   apply.ID,
		Operation: storage.ControlOperationStart,
		Status:    storage.ControlRequestPending,
	}}}
	client.storage = &mockStorage{
		applies:         &mockApplyStore{apply: &storedApply},
		tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
		logs:            &mockApplyLogStore{},
		controlRequests: controlRequests,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	err := client.ResumeApply(ctx, apply)
	require.NoError(t, err)

	server.mu.Lock()
	startCalled := server.startCalled
	server.mu.Unlock()
	assert.False(t, startCalled, "Start should not be called after remote progress reports active work")
	assert.Equal(t, "remote-start-already-active", server.getProgressApplyID())
	assert.Equal(t, state.Apply.Completed, apply.State)
	assert.Equal(t, state.Task.Completed, task.State)
	controlReq, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationStart)
	require.NoError(t, err)
	assert.Nil(t, controlReq)
}

func TestGRPCClient_ReconcileStoppedRemoteProgressKeepsQueuedStartPending(t *testing.T) {
	// A Start request can be accepted while an older driver is still recording
	// the remote stop. The stop sync must not consume the pending Start intent;
	// the operator needs that durable request to claim and resume the apply.
	now := time.Now()
	remoteApply := &storage.Apply{
		ID:              1,
		ApplyIdentifier: "apply-stop-with-pending-start",
		ExternalID:      "remote-stop-with-pending-start",
		Database:        "testdb",
		Environment:     "staging",
		State:           state.Apply.Stopped,
		StartedAt:       &now,
	}
	storedApply := *remoteApply
	storedApply.State = state.Apply.Running
	task := &storage.Task{
		ID:             31,
		TaskIdentifier: "task-stop-with-pending-start",
		ApplyID:        remoteApply.ID,
		TableName:      "users",
		Namespace:      "default",
		State:          state.Task.Running,
	}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ApplyID:   remoteApply.ID,
		Operation: storage.ControlOperationStart,
		Status:    storage.ControlRequestPending,
	}}}
	client := &GRPCClient{
		storage: &mockStorage{
			applies:         &mockApplyStore{apply: &storedApply},
			tasks:           &mockTaskStore{tasks: []*storage.Task{task}},
			logs:            &mockApplyLogStore{},
			controlRequests: controlRequests,
		},
	}

	err := client.reconcileTerminalRemoteProgress(t.Context(), remoteApply, []*ternv1.TableProgress{{
		Namespace: "default",
		TableName: "users",
		Status:    state.Task.Stopped,
	}}, now, wholeApplyTaskScope())
	require.NoError(t, err)

	assert.Equal(t, state.Apply.Stopped, remoteApply.State)
	assert.Equal(t, state.Task.Stopped, task.State)
	controlReq, err := controlRequests.GetPending(t.Context(), remoteApply.ID, storage.ControlOperationStart)
	require.NoError(t, err)
	assert.NotNil(t, controlReq)
}

// TestGRPCClient_ResumeApply_SkipsStartWhenNotStopped verifies that ResumeApply
// checks Tern's real state before calling Start. If Tern says the apply is already
// completed (stored state diverged), Start is skipped and terminal state is
// reconciled into stored rows.
func TestGRPCClient_ResumeApply_SkipsStartWhenNotStopped(t *testing.T) {
	// Progress returns COMPLETED — Tern already finished the apply even though
	// stored state says "stopped". Start should NOT be called.
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_COMPLETED,
		progressStateSet: true,
		progressTables: []*ternv1.TableProgress{{
			TableName:       "users",
			Status:          state.Task.Completed,
			PercentComplete: 100,
		}},
	}

	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "localhost:0")
	require.NoError(t, err, "failed to listen")

	grpcServer := grpc.NewServer()
	ternv1.RegisterTernServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err, "failed to dial")

	client := &GRPCClient{
		conn:    conn,
		client:  ternv1.NewTernClient(conn),
		storage: &mockStorage{},
	}
	defer utils.CloseAndLog(client)

	apply := &storage.Apply{
		ID:              1,
		ApplyIdentifier: "apply-stopped-remote-completed",
		Database:        "testdb",
		Environment:     "staging",
		ExternalID:      "remote_tern_xyz",
		State:           state.Apply.Stopped, // storage says stopped
	}
	storedApply := *apply
	task := &storage.Task{
		ID:             11,
		TaskIdentifier: "task-stopped-remote-completed",
		ApplyID:        apply.ID,
		TableName:      "users",
		State:          state.Task.Stopped,
	}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: &storedApply},
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
		logs:    &mockApplyLogStore{},
	}

	err = client.ResumeApply(t.Context(), apply)
	require.NoError(t, err)

	// Start should NOT have been called — Tern said the apply is completed.
	server.mu.Lock()
	startCalled := server.startCalled
	server.mu.Unlock()
	assert.False(t, startCalled, "Start should not be called when Tern reports apply is not stopped")

	// State should have been updated from Tern's response.
	assert.Equal(t, state.Apply.Completed, apply.State,
		"apply state should reflect Tern's real state")
	assert.NotNil(t, apply.CompletedAt)
	assert.Equal(t, state.Task.Completed, task.State)
	assert.NotNil(t, task.CompletedAt)
}

func TestGRPCClient_ResumeApplyDoesNotStartWhenStoppedStateCheckFails(t *testing.T) {
	// A stale stored stopped state is not enough to issue Start. If the remote
	// state check fails, leave the apply stopped and let a later attempt retry
	// with a fresh view of the data plane.
	server := &capturingTernServer{
		progressErr: status.Error(codes.Unavailable, "remote progress unavailable"),
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	logs := &mockApplyLogStore{}
	client.storage = &mockStorage{logs: logs}
	apply := &storage.Apply{
		ID:              1,
		ApplyIdentifier: "apply-stopped-check-error",
		Database:        "testdb",
		Environment:     "staging",
		ExternalID:      "remote_tern_xyz",
		State:           state.Apply.Stopped,
	}

	err := client.ResumeApply(t.Context(), apply)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "check stopped gRPC apply")

	server.mu.Lock()
	startCalled := server.startCalled
	server.mu.Unlock()
	assert.False(t, startCalled, "Start should not be called when the remote state check fails")
	assert.Equal(t, state.Apply.Stopped, apply.State)
	assert.True(t, hasLogMessageContaining(logs.logs, "Remote stopped-state check failed before operator start"))
}

func TestGRPCClient_ResumeApplyFailsWhenStoppedRemoteHasNoActiveProgress(t *testing.T) {
	// STATE_NO_ACTIVE_CHANGE is inconsistent for an exact stopped apply ID. The
	// operator should not fall back to Start because there is no remote stopped
	// state to resume.
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_NO_ACTIVE_CHANGE,
		progressStateSet: true,
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              1,
		ApplyIdentifier: "apply-stopped-no-active",
		Database:        "testdb",
		Environment:     "staging",
		ExternalID:      "remote-stopped-no-active",
		State:           state.Apply.Stopped,
	}
	task := &storage.Task{
		ID:             12,
		TaskIdentifier: "task-stopped-no-active",
		State:          state.Task.Stopped,
	}
	logs := &mockApplyLogStore{}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
		logs:    logs,
	}

	err := client.ResumeApply(t.Context(), apply)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no active schema change")

	server.mu.Lock()
	startCalled := server.startCalled
	server.mu.Unlock()
	assert.False(t, startCalled, "Start should not be called when the remote apply is missing")
	assert.Equal(t, state.Apply.Failed, apply.State)
	assert.Contains(t, apply.ErrorMessage, "no active schema change")
	assert.Equal(t, state.Task.Failed, task.State)
	assert.Contains(t, task.ErrorMessage, "no active schema change")
	assert.True(t, hasLogMessageContaining(logs.logs, "Remote apply failed: remote apply remote-stopped-no-active returned no active schema change"))
}

func TestGRPCClient_ResumeApplyFailsWhenStoppedRemoteIsNotFound(t *testing.T) {
	// A stored stopped apply with a missing exact remote apply ID is inconsistent
	// cross-plane state. Fail the stored apply instead of leaving it resumable.
	server := &capturingTernServer{
		progressErr: status.Error(codes.NotFound, "apply not found"),
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              1,
		ApplyIdentifier: "apply-stopped-not-found",
		Database:        "testdb",
		Environment:     "staging",
		ExternalID:      "remote-stopped-not-found",
		State:           state.Apply.Stopped,
	}
	task := &storage.Task{
		ID:             12,
		TaskIdentifier: "task-stopped-not-found",
		State:          state.Task.Stopped,
	}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
		logs:    &mockApplyLogStore{},
	}

	err := client.ResumeApply(t.Context(), apply)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "apply not found")

	server.mu.Lock()
	startCalled := server.startCalled
	server.mu.Unlock()
	assert.False(t, startCalled, "Start should not be called when the remote apply is missing")
	assert.Equal(t, state.Apply.Failed, apply.State)
	assert.Contains(t, apply.ErrorMessage, "remote-stopped-not-found")
	assert.Equal(t, state.Task.Failed, task.State)
	assert.Contains(t, task.ErrorMessage, "remote-stopped-not-found")
}

func TestGRPCClient_PollFailsWhenRemoteApplyIsNotFound(t *testing.T) {
	// A known remote apply ID returning NotFound means the data plane can no
	// longer report progress for work the control plane believes exists. The
	// stored apply fails so drivers do not keep polling a stale remote ID.
	client, cleanup := testCapturingGRPCClient(t, &capturingTernServer{
		progressErr: status.Error(codes.NotFound, "apply not found"),
	})
	defer cleanup()

	task := &storage.Task{
		ID:             11,
		TaskIdentifier: "task-missing-remote",
		State:          state.Task.Running,
	}
	apply := &storage.Apply{
		ID:              1,
		ApplyIdentifier: "apply-control-plane",
		Database:        "testdb",
		Environment:     "development",
		ExternalID:      "remote-not-found",
		State:           state.Apply.Running,
	}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.pollForCompletion(ctx, apply, false, wholeApplyTaskScope(), false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "remote-not-found")

	assert.Equal(t, state.Apply.Failed, apply.State)
	assert.Contains(t, apply.ErrorMessage, "remote-not-found")
	assert.NotNil(t, apply.CompletedAt)
	assert.Equal(t, state.Task.Failed, task.State)
	assert.Contains(t, task.ErrorMessage, "remote-not-found")
	assert.NotNil(t, task.CompletedAt)
}

func TestGRPCClient_PollFailsWhenExactRemoteApplyHasNoActiveProgress(t *testing.T) {
	// An exact apply-id progress request returning no active work is inconsistent
	// cross-plane state and should fail the stored apply.
	client, cleanup := testCapturingGRPCClient(t, &capturingTernServer{
		progressState:    ternv1.State_STATE_NO_ACTIVE_CHANGE,
		progressStateSet: true,
	})
	defer cleanup()

	task := &storage.Task{
		ID:             12,
		TaskIdentifier: "task-no-active",
		State:          state.Task.Running,
	}
	apply := &storage.Apply{
		ID:              2,
		ApplyIdentifier: "apply-no-active",
		Database:        "testdb",
		Environment:     "development",
		ExternalID:      "remote-no-active",
		State:           state.Apply.Running,
	}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.pollForCompletion(ctx, apply, false, wholeApplyTaskScope(), false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no active schema change")

	assert.Equal(t, state.Apply.Failed, apply.State)
	assert.Contains(t, apply.ErrorMessage, "no active schema change")
	assert.NotNil(t, apply.CompletedAt)
	assert.Equal(t, state.Task.Failed, task.State)
	assert.Contains(t, task.ErrorMessage, "no active schema change")
	assert.NotNil(t, task.CompletedAt)
}

func TestGRPCClient_PollFailsWhenRemoteApplyStateIsUnmapped(t *testing.T) {
	// Unknown remote apply states cannot be reconciled safely. Keep the stored
	// state unchanged and surface the unmapped state instead of falling back to
	// the previous stored state.
	client, cleanup := testCapturingGRPCClient(t, &capturingTernServer{
		progressState:    ternv1.State(999),
		progressStateSet: true,
	})
	defer cleanup()

	apply := &storage.Apply{
		ID:              2,
		ApplyIdentifier: "apply-unmapped-remote-state",
		Database:        "testdb",
		Environment:     "development",
		ExternalID:      "remote-unmapped-state",
		State:           state.Apply.Running,
	}
	logs := &mockApplyLogStore{}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		logs:    logs,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.pollForCompletion(ctx, apply, false, wholeApplyTaskScope(), false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmapped remote state")
	assert.Equal(t, state.Apply.Running, apply.State)
	assert.True(t, hasLogMessageContaining(logs.logs, "Remote progress returned unmapped apply state"))
}

func TestGRPCClient_RemoteProgressLossDoesNotOverwriteTerminalApply(t *testing.T) {
	// Remote progress loss can fail the stored apply only while the stored
	// control-plane row is still non-terminal. If storage already has a terminal
	// state, preserve it instead of overwriting it with a stale remote lookup
	// failure.
	client, cleanup := testCapturingGRPCClient(t, &capturingTernServer{
		progressErr: status.Error(codes.NotFound, "apply not found"),
	})
	defer cleanup()

	storedApply := &storage.Apply{
		ID:              3,
		ApplyIdentifier: "apply-already-complete",
		Database:        "testdb",
		Environment:     "development",
		ExternalID:      "remote-already-complete",
		State:           state.Apply.Completed,
	}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: storedApply},
		tasks: &mockTaskStore{tasks: []*storage.Task{{
			ID:             13,
			TaskIdentifier: "task-already-complete",
			State:          state.Task.Completed,
		}}},
	}
	apply := &storage.Apply{
		ID:              3,
		ApplyIdentifier: "apply-already-complete",
		Database:        "testdb",
		Environment:     "development",
		ExternalID:      "remote-already-complete",
		State:           state.Apply.Running,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.pollForCompletion(ctx, apply, false, wholeApplyTaskScope(), false)
	require.Error(t, err)

	assert.Equal(t, state.Apply.Completed, apply.State)
	assert.Empty(t, apply.ErrorMessage)
}

func TestNewGRPCClient(t *testing.T) {
	t.Run("valid address", func(t *testing.T) {
		// Start a temporary server
		lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "localhost:0")
		require.NoError(t, err, "failed to listen")
		defer utils.CloseAndLog(lis)

		grpcServer := grpc.NewServer()
		ternv1.RegisterTernServer(grpcServer, &mockTernServer{})
		go func() { _ = grpcServer.Serve(lis) }()
		defer grpcServer.Stop()

		client, err := NewGRPCClient(Config{Address: lis.Addr().String()})
		require.NoError(t, err)
		defer utils.CloseAndLog(client)

		// Verify it works
		err = client.Health(t.Context())
		require.NoError(t, err)
	})
}

func TestRemoteApplyIdempotencyKey(t *testing.T) {
	apply := &storage.Apply{ApplyIdentifier: "apply-abc123", Attempt: 0}

	whole := remoteApplyIdempotencyKey(apply, wholeApplyTaskScope())
	require.NotEmpty(t, whole)
	assert.True(t, strings.HasPrefix(whole, "schemabot:v1:"), "key carries a version prefix")
	assert.LessOrEqual(t, len(whole), 255, "key fits the storage column")

	// Stable across a re-dispatch of the same generation.
	assert.Equal(t, whole, remoteApplyIdempotencyKey(apply, wholeApplyTaskScope()),
		"same apply + attempt + scope must produce the same key")

	// Rotates when the apply is deliberately retried (attempt advances).
	retried := &storage.Apply{ApplyIdentifier: "apply-abc123", Attempt: 1}
	assert.NotEqual(t, whole, remoteApplyIdempotencyKey(retried, wholeApplyTaskScope()),
		"an incremented attempt must rotate the key so a retry dispatches fresh")

	// A single-operation drive (multiOperation == false) stores its remote id on
	// the parent external_id, so it shares the whole-apply key even when an
	// operation row is present.
	singleOp := applyTaskScope{
		applyOperationID: 7,
		operation:        &storage.ApplyOperation{Deployment: "region-a", OperationKey: "commerce/users"},
		multiOperation:   false,
	}
	assert.Equal(t, whole, remoteApplyIdempotencyKey(apply, singleOp),
		"single-operation drives share the whole-apply key")

	// Sibling operations of one deployment share the key: the data plane lands
	// them on one remote apply and tells the dispatches apart by the operation
	// key derived from each request's shape, not by distinct idempotency keys.
	opA := applyTaskScope{
		applyOperationID: 1,
		operation:        &storage.ApplyOperation{Deployment: "region-a", OperationKey: "commerce/-80/users"},
		multiOperation:   true,
	}
	opB := applyTaskScope{
		applyOperationID: 2,
		operation:        &storage.ApplyOperation{Deployment: "region-a", OperationKey: "commerce/80-/users"},
		multiOperation:   true,
	}
	keyA := remoteApplyIdempotencyKey(apply, opA)
	keyB := remoteApplyIdempotencyKey(apply, opB)
	assert.Equal(t, keyA, keyB, "sibling operations of one deployment share the deployment key")
	assert.NotEqual(t, whole, keyA, "a deployment-scoped key differs from the whole-apply key")
	assert.Equal(t, keyA, remoteApplyIdempotencyKey(apply, opA), "the deployment key is stable across re-dispatch")

	// Different deployments of one apply dispatch to different data planes, so
	// their keys must differ.
	opOtherDeployment := applyTaskScope{
		applyOperationID: 3,
		operation:        &storage.ApplyOperation{Deployment: "region-b", OperationKey: "commerce/-80/users"},
		multiOperation:   true,
	}
	assert.NotEqual(t, keyA, remoteApplyIdempotencyKey(apply, opOtherDeployment),
		"distinct deployments must produce distinct keys")
}

// TestRemoteApplyIdempotencyKey_GenerationRotation verifies that a
// deployment-scoped key rotates only on the operation's own dispatch
// generation. The shared apply.Attempt advances when any sibling operation of
// any deployment is redispatched, so a key derived from it would rotate under
// an unrelated deployment's orphaned dispatch and duplicate its remote apply;
// the deployment key must ignore it. The operation's own attempt advances only
// on that operation's deliberate retry, which is exactly when the retried work
// must dispatch fresh.
func TestRemoteApplyIdempotencyKey_GenerationRotation(t *testing.T) {
	opScope := func(parentAttempt, opAttempt int) (*storage.Apply, applyTaskScope) {
		return &storage.Apply{ApplyIdentifier: "apply-abc123", Attempt: parentAttempt},
			applyTaskScope{
				applyOperationID: 1,
				operation:        &storage.ApplyOperation{Deployment: "region-a", OperationKey: "commerce/users", Attempt: opAttempt},
				multiOperation:   true,
			}
	}

	apply0, scope0 := opScope(0, 0)
	base := remoteApplyIdempotencyKey(apply0, scope0)

	// A sibling deployment's retry advances the shared parent attempt while
	// this deployment's dispatch is still orphaned mid-flight. Its key must not
	// move, so the re-drive reuses the original dispatch instead of starting a
	// duplicate remote apply.
	applySiblingRetried, scopeOrphaned := opScope(3, 0)
	assert.Equal(t, base, remoteApplyIdempotencyKey(applySiblingRetried, scopeOrphaned),
		"the shared apply attempt must not rotate an orphaned deployment's key")

	// The operation's own deliberate retry rotates the key so the retried work
	// dispatches a fresh remote apply.
	applyRetried, scopeRetried := opScope(1, 1)
	assert.NotEqual(t, base, remoteApplyIdempotencyKey(applyRetried, scopeRetried),
		"an operation's own advanced attempt must rotate the deployment key")

	// A retry key is stable across a re-dispatch of the same attempt, so an
	// orphaned retry dispatch is reused rather than duplicated.
	assert.Equal(t, remoteApplyIdempotencyKey(applyRetried, scopeRetried),
		remoteApplyIdempotencyKey(applyRetried, scopeRetried),
		"the retry key must be stable within one attempt")

	// A retry redispatches only its own operation — siblings that succeeded
	// never dispatch again — so sibling retries must not share a remote apply:
	// each retried operation gets its own operation-scoped key.
	_, scopeSiblingRetried := opScope(1, 1)
	scopeSiblingRetried.operation.OperationKey = "commerce/80-/users"
	assert.NotEqual(t, remoteApplyIdempotencyKey(applyRetried, scopeRetried),
		remoteApplyIdempotencyKey(applyRetried, scopeSiblingRetried),
		"retried siblings must dispatch to distinct operation-scoped remote applies")
}

// A deployment-keyed dispatch carries the generation manifest — the full
// operation-key set its deployment will send under the shared idempotency key —
// so the data plane's shared apply completes only when every declared sibling
// has attached and finished. The manifest must cover exactly the claimed
// operation's deployment (sorted, own key included), and whole-apply dispatches
// must carry none: their single dispatch is the whole generation.
func TestGenerationOperationKeys(t *testing.T) {
	assert.Nil(t, wholeApplyTaskScope().generationOperationKeys(),
		"a whole-apply dispatch carries no manifest")

	// A single-operation drive shares the whole-apply key, so it must not carry
	// a manifest either, even when the loader populated the deployment key set.
	singleOp := applyTaskScope{
		applyOperationID:        7,
		operation:               &storage.ApplyOperation{Deployment: "region-a", OperationKey: "commerce/users"},
		multiOperation:          false,
		deploymentOperationKeys: []string{"commerce/users"},
	}
	assert.Nil(t, singleOp.generationOperationKeys(),
		"a whole-apply-keyed drive must not declare a manifest the data plane would gate on")

	ops := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
		1: {ID: 1, ApplyID: 100, Deployment: "region-a", OperationKey: "commerce/80-/users"},
		2: {ID: 2, ApplyID: 100, Deployment: "region-a", OperationKey: "commerce/-80/users"},
		3: {ID: 3, ApplyID: 100, Deployment: "region-a", OperationKey: "commerce/group_finalizer"},
		4: {ID: 4, ApplyID: 100, Deployment: "region-b", OperationKey: "commerce/-80/users"},
	}}
	client := &GRPCClient{storage: &mockStorage{operations: ops}}
	apply := &storage.Apply{ID: 100, ApplyIdentifier: "apply-abc123"}

	scope, err := client.loadOperationApplyTaskScope(t.Context(), apply, 1)
	require.NoError(t, err)
	assert.Equal(t,
		[]string{"commerce/-80/users", "commerce/80-/users", "commerce/group_finalizer"},
		scope.generationOperationKeys(),
		"the manifest is the claimed deployment's sorted key set, finalizer and own key included, siblings of other deployments excluded")

	// Every sibling of the deployment declares the same manifest, so whichever
	// dispatch lands first stores the full expected set.
	scopeSibling, err := client.loadOperationApplyTaskScope(t.Context(), apply, 3)
	require.NoError(t, err)
	assert.Equal(t, scope.generationOperationKeys(), scopeSibling.generationOperationKeys(),
		"sibling dispatches of one deployment must declare identical manifests")

	// The other deployment's dispatch declares only its own key set.
	scopeOther, err := client.loadOperationApplyTaskScope(t.Context(), apply, 4)
	require.NoError(t, err)
	assert.Equal(t, []string{"commerce/-80/users"}, scopeOther.generationOperationKeys(),
		"a deployment's manifest never includes another deployment's operations")

	// A deliberate retry dispatches under an operation-scoped key, so its
	// manifest declares only its own operation: the retry's remote apply never
	// receives siblings, and declaring them would hold it open forever.
	scope.operation.Attempt = 1
	assert.Equal(t, []string{"commerce/80-/users"}, scope.generationOperationKeys(),
		"a retry's manifest is its own operation key alone")
}

// The idempotency key and the generation manifest are derived independently
// from the same scope, so they must rotate together on a deliberate retry: a
// rotated key paired with a still-full manifest (or an unrotated key paired
// with a narrowed one) creates a remote apply whose declared operation set can
// never attach, holding it Running under the completion gate forever. This
// exercises both derivations on one scope object across the attempt boundary
// so the pairing cannot drift apart unnoticed.
func TestRetryRotatesKeyAndManifestTogether(t *testing.T) {
	apply := &storage.Apply{ApplyIdentifier: "apply-abc123"}
	scope := applyTaskScope{
		applyOperationID: 1,
		operation: &storage.ApplyOperation{
			Deployment:   "region-a",
			OperationKey: "commerce/80-/users",
		},
		multiOperation:          true,
		deploymentOperationKeys: []string{"commerce/-80/users", "commerce/80-/users", "commerce/group_finalizer"},
	}

	// First dispatch: a deployment-scoped key paired with the full deployment
	// manifest, so the shared remote apply knows every sibling it must await.
	firstKey := remoteApplyIdempotencyKey(apply, scope)
	assert.Equal(t, scope.deploymentOperationKeys, scope.generationOperationKeys(),
		"a first dispatch pairs its deployment key with the full deployment manifest")

	// The operation's deliberate retry: the same scope must rotate the key to
	// operation scope AND narrow the manifest to the retried operation alone —
	// the retry's remote apply receives exactly this one operation.
	scope.operation.Attempt = 1
	assert.NotEqual(t, firstKey, remoteApplyIdempotencyKey(apply, scope),
		"a deliberate retry rotates to an operation-scoped key")
	assert.Equal(t, []string{"commerce/80-/users"}, scope.generationOperationKeys(),
		"the retry narrows the manifest to its own operation in the same step as the key rotation")
}

// A deployment-keyed dispatch shares one remote apply across sibling
// operations, so an accepted response only addresses this dispatch if it
// echoes the operation key the request's shape derives to. The verifier must
// accept a correct echo, reject a missing one (a data plane that answers
// without attaching sibling operations), and keep accepting the empty echo for
// a whole-deployment dispatch whose derived key is itself empty.
func TestVerifyDispatchOperationKeyEcho(t *testing.T) {
	plan := &storage.Plan{PlanIdentifier: "plan-echo"}
	shardReq := &ternv1.ApplyRequest{
		PlanId:       "plan-echo",
		TargetShards: []string{"-80"},
		DdlChanges: []*ternv1.TableChange{{
			Namespace:  "commerce",
			TableName:  "users",
			Ddl:        "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
			ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
		}},
	}

	matching := &ternv1.ApplyResponse{Accepted: true, ApplyId: "remote-1", OperationKey: "commerce/-80/users"}
	require.NoError(t, verifyDispatchOperationKeyEcho(plan, shardReq, matching))

	missing := &ternv1.ApplyResponse{Accepted: true, ApplyId: "remote-1"}
	err := verifyDispatchOperationKeyEcho(plan, shardReq, missing)
	require.Error(t, err, "a shard dispatch answered without its operation key must be refused")
	assert.Contains(t, err.Error(), `expected "commerce/-80/users"`)

	other := &ternv1.ApplyResponse{Accepted: true, ApplyId: "remote-1", OperationKey: "commerce/80-/users"}
	err = verifyDispatchOperationKeyEcho(plan, shardReq, other)
	require.Error(t, err, "another operation's key must be refused")

	// A whole-deployment dispatch derives an empty key, so the empty echo is the
	// correct answer for it.
	wholeReq := &ternv1.ApplyRequest{
		PlanId: "plan-echo",
		DdlChanges: []*ternv1.TableChange{{
			Namespace:  "commerce",
			TableName:  "users",
			Ddl:        "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
			ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
		}},
	}
	require.NoError(t, verifyDispatchOperationKeyEcho(plan, wholeReq, missing))
}

// A shard-scoped dispatch answered by a data plane that does not echo the
// derived operation key — one that aliases every sibling dispatch to the
// apply's first operation instead of attaching them — must fail closed: the
// response's remote ids are refused (never persisted) and the dispatch is
// marked failed for the operator to act on, instead of the control plane
// polling another operation's remote apply as if it were this one's.
func TestGRPCClient_ResumeApplyOperationFailsClosedOnMissingOperationKeyEcho(t *testing.T) {
	server := &capturingTernServer{
		remoteApplyID:    "remote-no-echo",
		omitOperationKey: true,
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-no-echo",
		PlanID:          99,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "testdb-target"})
	operationID := int64(42)
	task := &storage.Task{
		ID:               11,
		TaskIdentifier:   "task-users",
		ApplyID:          apply.ID,
		ApplyOperationID: &operationID,
		TableName:        "users",
		Shard:            "-80",
		DDL:              "ALTER TABLE users ADD COLUMN email varchar(255)",
		DDLAction:        "alter",
		Namespace:        "default",
		State:            state.Task.Pending,
	}
	operation := &storage.ApplyOperation{
		ID: operationID, ApplyID: apply.ID, Deployment: "testdb-deployment",
		OperationKey: "default/-80/users", State: state.ApplyOperation.Pending,
	}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
		plans:   &mockPlanStore{plan: &storage.Plan{ID: apply.PlanID, PlanIdentifier: "plan-no-echo"}},
		operations: &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
			operationID: operation,
		}},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.ResumeApplyOperation(ctx, apply, operationID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "echoed operation key")

	require.NotNil(t, server.getApplyRequest(), "the dispatch itself must have been sent")
	assert.Empty(t, apply.ExternalID, "the unverified response's remote apply id must not be persisted on the parent")
	assert.Empty(t, operation.ExternalID, "the unverified response's remote apply id must not be persisted on the operation")

	// The mismatch is non-retryable: a data plane that cannot address the
	// operation would answer every redispatch the same way, so the failure must
	// land terminally for an operator instead of retrying forever.
	assert.Equal(t, state.Apply.Failed, apply.State, "the echo mismatch must terminalize the apply, not leave it retryable")
	assert.Equal(t, state.Task.Failed, task.State, "the echo mismatch must terminalize the task, not leave it retryable")
	require.NotNil(t, task.CompletedAt, "a terminally failed task records its completion time")
	assert.Contains(t, task.ErrorMessage, "echoed operation key")
}

// A VSchema-only dispatch derives a finalizer operation key from its namespace,
// so it carries the same fail-closed echo contract as shard work: a data plane
// that answers without echoing that key has not addressed this operation, and
// the dispatch must refuse the response's remote ids and land terminally failed
// for an operator instead of polling another operation's remote apply.
func TestGRPCClient_VSchemaOnlyDispatchFailsClosedOnMissingOperationKeyEcho(t *testing.T) {
	server := &capturingTernServer{
		remoteApplyID:    "remote-finalizer-no-echo",
		omitOperationKey: true,
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              8,
		ApplyIdentifier: "apply-finalizer-no-echo",
		PlanID:          99,
		Database:        "commerce",
		DatabaseType:    storage.DatabaseTypeStrata,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "commerce-target"})
	operationID := int64(51)
	operation := &storage.ApplyOperation{
		ID:            operationID,
		ApplyID:       apply.ID,
		Deployment:    "commerce-deployment",
		OperationKey:  "commerce/group_finalizer",
		OperationKind: storage.ApplyOperationKindGroupFinalizer,
		State:         state.ApplyOperation.Pending,
	}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   &mockTaskStore{getByApplyIDErr: errors.New("finalizer drive must not load tasks")},
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-finalizer-no-echo",
			SchemaFiles: schema.SchemaFiles{
				"commerce": {Files: map[string]string{storage.VSchemaArtifactName: `{"sharded":true}`}},
			},
			Namespaces: map[string]*storage.NamespacePlanData{
				"commerce": {Artifacts: map[string]string{storage.VSchemaArtifactName: `{"sharded":true}`}},
			},
		}},
		operations: &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
			operationID: operation,
		}},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.ResumeApplyOperation(ctx, apply, operationID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "echoed operation key")

	require.NotNil(t, server.getApplyRequest(), "the VSchema dispatch itself must have been sent")
	assert.Empty(t, apply.ExternalID, "the unverified response's remote apply id must not be persisted on the parent")
	assert.Empty(t, operation.ExternalID, "the unverified response's remote apply id must not be persisted on the operation")
	assert.Equal(t, state.Apply.Failed, apply.State, "the echo mismatch must terminalize the apply, not leave it retryable")
	assert.Contains(t, apply.ErrorMessage, "echoed operation key")
}

// A task-less operation has no task rows, so the operator's task-derived
// projection can never move it: whatever terminal state the data plane reports
// has to be written onto the operation row by the drive itself. Leaving any
// terminal state unwritten holds the operation and its parent apply running
// forever with the target blocked, so every terminal state is covered here —
// including the resumable/non-resumable split, which decides whether a later
// start finds a row it can claim.
func TestGRPCClient_TasklessOperationTerminalWriterCoversEveryTerminalState(t *testing.T) {
	const operationID = int64(91)

	tests := []struct {
		name            string
		remoteState     string
		errMsg          string
		wantState       string
		wantCompletedAt bool
		wantErrMsg      string
	}{
		{
			name:            "completed",
			remoteState:     state.Apply.Completed,
			wantState:       state.ApplyOperation.Completed,
			wantCompletedAt: true,
		},
		{
			name:            "failed carries the reason an operator triages from",
			remoteState:     state.Apply.Failed,
			errMsg:          "vschema apply rejected by vtctld",
			wantState:       state.ApplyOperation.Failed,
			wantCompletedAt: true,
			wantErrMsg:      "vschema apply rejected by vtctld",
		},
		{
			name:        "stopped stays resumable, so completed_at is left unset",
			remoteState: state.Apply.Stopped,
			wantState:   state.ApplyOperation.Stopped,
		},
		{
			name:            "cancelled is terminal and not resumable",
			remoteState:     state.Apply.Cancelled,
			wantState:       state.ApplyOperation.Cancelled,
			wantCompletedAt: true,
		},
		{
			name:            "reverted is terminal and not resumable",
			remoteState:     state.Apply.Reverted,
			wantState:       state.ApplyOperation.Reverted,
			wantCompletedAt: true,
		},
		{
			name: "a non-terminal state leaves the operation claimable rather than settling it",
			// failed_retryable is the one that matters: settling it would
			// swallow a failure the operator is meant to be able to retry.
			remoteState: state.Apply.FailedRetryable,
			wantState:   state.ApplyOperation.Running,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := &capturingTernServer{}
			client, cleanup := testCapturingGRPCClient(t, server)
			defer cleanup()

			operations := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
				operationID: {
					ID:            operationID,
					ApplyID:       12,
					OperationKind: storage.ApplyOperationKindWork,
					State:         state.ApplyOperation.Running,
				},
			}}
			client.storage = &mockStorage{operations: operations}

			require.NoError(t, client.persistTasklessOperationTerminalState(t.Context(), operationID, tt.remoteState, tt.errMsg))

			op := operations.ops[operationID]
			assert.True(t, state.IsState(op.State, tt.wantState),
				"expected operation state %q, got %q", tt.wantState, op.State)
			assert.Equal(t, tt.wantErrMsg, op.ErrorMessage)
			if tt.wantCompletedAt {
				assert.NotNil(t, op.CompletedAt, "a non-resumable terminal state stamps completed_at")
			} else {
				assert.Nil(t, op.CompletedAt, "a row that must stay claimable leaves completed_at unset")
			}
		})
	}
}

// A task-less operation whose plan row is missing can never be dispatched: the
// namespaces to apply are read from the plan. The claim has to terminalize
// rather than be re-driven, which is what wrapping ErrNoTasksForApplyOperation
// tells the operator to do — otherwise the operator re-claims and re-drives the
// same operation on every poll forever.
func TestErrPlanMissingForApplyOperationTerminalizesTheClaim(t *testing.T) {
	require.ErrorIs(t, ErrPlanMissingForApplyOperation, ErrNoTasksForApplyOperation,
		"the operator decides terminalize-vs-retry on this wrap")

	wrapped := fmt.Errorf("plan 7 for apply_operation 9 (apply apply-x): %w", ErrPlanMissingForApplyOperation)
	require.ErrorIs(t, wrapped, ErrNoTasksForApplyOperation,
		"the contract has to survive the context each return site adds")
}

func TestGRPCClient_ResumeApplyOperationTerminalizesWhenThePlanRowIsMissing(t *testing.T) {
	server := &capturingTernServer{remoteApplyID: "remote-must-not-dispatch"}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              13,
		ApplyIdentifier: "apply-missing-plan",
		PlanID:          80,
		Database:        "commerce",
		DatabaseType:    storage.DatabaseTypeVitess,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	operationID := int64(64)
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   &mockTaskStore{},
		// GetByID returns (nil, nil) for a plan row that is gone.
		plans: &mockPlanStore{},
		operations: &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
			operationID: {
				ID:            operationID,
				ApplyID:       apply.ID,
				Deployment:    "commerce-deployment",
				OperationKind: storage.ApplyOperationKindWork,
				State:         state.ApplyOperation.Pending,
			},
		}},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := client.ResumeApplyOperation(ctx, apply, operationID)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNoTasksForApplyOperation,
		"a missing plan must terminalize the claim, not leave it to be re-driven forever")
	assert.Nil(t, server.getApplyRequest(), "nothing may be dispatched without a plan to dispatch")
}

// An apply whose work was dispatched before per-operation remote ids existed
// carries its remote id on the parent. Its sole operation must resume that
// remote apply rather than dispatch again, or the first claim after an upgrade
// runs the same schema change on the target a second time.
func TestGRPCClient_SoleOperationResumesTheRemoteApplyRecordedOnTheParent(t *testing.T) {
	server := &capturingTernServer{remoteApplyID: "remote-should-not-be-used"} // default Progress = COMPLETED
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              14,
		ApplyIdentifier: "apply-parent-external-id",
		PlanID:          81,
		Database:        "commerce",
		DatabaseType:    storage.DatabaseTypeVitess,
		Environment:     "staging",
		State:           state.Apply.Running,
		ExternalID:      "remote-dispatched-before-upgrade",
	}
	operationID := int64(65)
	vschema := `{"sharded":true}`
	operations := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
		operationID: {
			ID:            operationID,
			ApplyID:       apply.ID,
			Deployment:    "commerce-deployment",
			OperationKind: storage.ApplyOperationKindWork,
			State:         state.ApplyOperation.Running,
			// No ExternalID: this operation predates per-operation remote ids.
		},
	}}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   &mockTaskStore{},
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-parent-external-id",
			SchemaFiles: schema.SchemaFiles{
				"commerce": {Files: map[string]string{storage.VSchemaArtifactName: vschema}},
			},
			Namespaces: map[string]*storage.NamespacePlanData{
				"commerce": {Artifacts: map[string]string{storage.VSchemaArtifactName: vschema}},
			},
		}},
		operations: operations,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	ctx = storage.WithOperationLease(ctx, storage.OperationLease{
		ApplyID:     apply.ID,
		OperationID: operationID,
		Owner:       "host/1/driver-0",
		Token:       "operation-token",
	})
	require.NoError(t, client.ResumeApplyOperation(ctx, apply, operationID))

	assert.Nil(t, server.getApplyRequest(),
		"the parent's remote apply id must be resumed, not dispatched over")
	assert.Equal(t, "remote-dispatched-before-upgrade", server.getProgressRequest().GetApplyId(),
		"the drive must poll the remote apply the parent recorded")
}

// Stopping a task-less operation is only half of the operator's control: the
// start that follows has to reach the data plane. Nothing else re-enters the
// drive for an operation with no task rows, so without an explicit start the
// operation is re-claimed on every poll, reports the same stopped state, and
// only a cancel frees the target.
func TestGRPCClient_TasklessOperationStartsAgainAfterAnOperatorStop(t *testing.T) {
	server := &capturingTernServer{
		remoteApplyID: "remote-vschema-stopped",
		// Stopped until Start lands; the server moves itself to COMPLETED after.
		progressState:    ternv1.State_STATE_STOPPED,
		progressStateSet: true,
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              15,
		ApplyIdentifier: "apply-vschema-stopped",
		PlanID:          82,
		Database:        "commerce",
		DatabaseType:    storage.DatabaseTypeVitess,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	operationID := int64(66)
	vschema := `{"sharded":true}`
	operations := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
		operationID: {
			ID:            operationID,
			ApplyID:       apply.ID,
			Deployment:    "commerce-deployment",
			OperationKind: storage.ApplyOperationKindWork,
			State:         state.ApplyOperation.Stopped,
			ExternalID:    "remote-vschema-stopped",
		},
	}}
	controlRequests := &testControlRequestStore{requests: []*storage.ApplyControlRequest{{
		ID:          1,
		ApplyID:     apply.ID,
		Operation:   storage.ControlOperationStart,
		Status:      storage.ControlRequestPending,
		RequestedBy: "cli:alice",
	}}}
	// The stored row is a separate value from the one the drive carries, so a
	// reload during the drive returns what storage holds rather than the drive's
	// own in-memory progress.
	storedApply := *apply
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: &storedApply},
		tasks:   &mockTaskStore{},
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-vschema-stopped",
			SchemaFiles: schema.SchemaFiles{
				"commerce": {Files: map[string]string{storage.VSchemaArtifactName: vschema}},
			},
			Namespaces: map[string]*storage.NamespacePlanData{
				"commerce": {Artifacts: map[string]string{storage.VSchemaArtifactName: vschema}},
			},
		}},
		operations:      operations,
		controlRequests: controlRequests,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	ctx = storage.WithOperationLease(ctx, storage.OperationLease{
		ApplyID:     apply.ID,
		OperationID: operationID,
		Owner:       "host/1/driver-0",
		Token:       "operation-token",
	})
	require.NoError(t, client.ResumeApplyOperation(ctx, apply, operationID))

	assert.True(t, server.startWasCalled(), "the operator's start must reach the data plane")
	assert.Equal(t, "remote-vschema-stopped", server.getStartApplyID())
	assert.Nil(t, server.getApplyRequest(),
		"a stopped operation is resumed, never dispatched a second time")
	assert.True(t, state.IsState(operations.ops[operationID].State, state.ApplyOperation.Completed),
		"the resumed operation settles on its own row, but is %q", operations.ops[operationID].State)
}

// A task-less operation the operator has not asked to resume stays stopped: the
// drive must not restart work on the target on its own.
func TestGRPCClient_StoppedTasklessOperationStaysStoppedWithoutAStartRequest(t *testing.T) {
	server := &capturingTernServer{
		remoteApplyID:    "remote-vschema-left-stopped",
		progressState:    ternv1.State_STATE_STOPPED,
		progressStateSet: true,
	}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()

	apply := &storage.Apply{
		ID:              16,
		ApplyIdentifier: "apply-vschema-left-stopped",
		PlanID:          83,
		Database:        "commerce",
		DatabaseType:    storage.DatabaseTypeVitess,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
	operationID := int64(67)
	vschema := `{"sharded":true}`
	operations := &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{
		operationID: {
			ID:            operationID,
			ApplyID:       apply.ID,
			Deployment:    "commerce-deployment",
			OperationKind: storage.ApplyOperationKindWork,
			State:         state.ApplyOperation.Stopped,
			ExternalID:    "remote-vschema-left-stopped",
		},
	}}
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   &mockTaskStore{},
		plans: &mockPlanStore{plan: &storage.Plan{
			ID:             apply.PlanID,
			PlanIdentifier: "plan-vschema-left-stopped",
			SchemaFiles: schema.SchemaFiles{
				"commerce": {Files: map[string]string{storage.VSchemaArtifactName: vschema}},
			},
			Namespaces: map[string]*storage.NamespacePlanData{
				"commerce": {Artifacts: map[string]string{storage.VSchemaArtifactName: vschema}},
			},
		}},
		operations:      operations,
		controlRequests: &testControlRequestStore{},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	ctx = storage.WithOperationLease(ctx, storage.OperationLease{
		ApplyID:     apply.ID,
		OperationID: operationID,
		Owner:       "host/1/driver-0",
		Token:       "operation-token",
	})
	require.NoError(t, client.ResumeApplyOperation(ctx, apply, operationID))

	assert.False(t, server.startWasCalled(), "no operator start means no restart of the target")
	assert.True(t, state.IsState(operations.ops[operationID].State, state.ApplyOperation.Stopped),
		"the operation stays stopped, but is %q", operations.ops[operationID].State)
}
