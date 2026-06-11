package tern

import (
	"context"
	"errors"
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

func (s *mockTernServer) Start(ctx context.Context, req *ternv1.StartRequest) (*ternv1.StartResponse, error) {
	if req.ApplyId == "" {
		return nil, status.Error(codes.InvalidArgument, "apply_id is required")
	}
	return &ternv1.StartResponse{Accepted: true}, nil
}

func (s *mockTernServer) Volume(ctx context.Context, req *ternv1.VolumeRequest) (*ternv1.VolumeResponse, error) {
	if req.ApplyId == "" {
		return nil, status.Error(codes.InvalidArgument, "apply_id is required")
	}
	if req.Volume < 1 || req.Volume > 11 {
		return nil, status.Error(codes.InvalidArgument, "volume must be between 1 and 11")
	}
	return &ternv1.VolumeResponse{
		Accepted:       true,
		PreviousVolume: 3,
		NewVolume:      req.Volume,
	}, nil
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
	failPlans   int
	failApplies int
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

func (s *flakyTernServer) calls() (plans, applies int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.planCalls, s.applyCalls
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

// Retries are bounded: a deployment that stays unavailable surfaces
// UNAVAILABLE to the caller after the retry budget instead of retrying
// forever.
func TestGRPCClientPlanRetriesAreBounded(t *testing.T) {
	server := &flakyTernServer{failPlans: 10}
	client := newRetryTestClient(t, server)

	_, err := client.Plan(t.Context(), &ternv1.PlanRequest{Database: "testdb"})

	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
	plans, _ := server.calls()
	assert.Equal(t, 3, plans, "client should stop after the configured attempt budget")
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

func TestGRPCClient_Volume(t *testing.T) {
	client, cleanup := testClient(t, &mockTernServer{})
	defer cleanup()

	t.Run("valid request", func(t *testing.T) {
		resp, err := client.Volume(t.Context(), &ternv1.VolumeRequest{
			ApplyId:     "apply-vol123",
			Environment: "staging",
			Volume:      7,
		})
		require.NoError(t, err)
		assert.True(t, resp.Accepted)
		assert.Equal(t, int32(7), resp.NewVolume)
	})

	t.Run("invalid volume", func(t *testing.T) {
		_, err := client.Volume(t.Context(), &ternv1.VolumeRequest{
			ApplyId:     "apply-vol123",
			Environment: "staging",
			Volume:      15,
		})
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
	mu               sync.Mutex
	applyReq         *ternv1.ApplyRequest
	applyErr         error
	remoteApplyID    string
	stopApplyID      string
	startApplyID     string
	cutoverApplyID   string
	progressApplyID  string
	progressReq      *ternv1.ProgressRequest
	progressState    ternv1.State // state returned by Progress; 0 = STATE_COMPLETED
	progressStateSet bool
	progressStates   []ternv1.State
	progressTables   []*ternv1.TableProgress
	progressError    string
	progressErr      error
	startErr         error
	cutoverErr       error
	cutoverAccepted  bool
	cutoverMessage   string
	startCalled      bool // tracks whether Start was actually invoked
}

func (s *capturingTernServer) Apply(_ context.Context, req *ternv1.ApplyRequest) (*ternv1.ApplyResponse, error) {
	s.mu.Lock()
	s.applyReq = req
	applyID := s.remoteApplyID
	if applyID == "" {
		applyID = "remote-apply-123"
	}
	err := s.applyErr
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &ternv1.ApplyResponse{Accepted: true, ApplyId: applyID}, nil
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
	s.mu.Unlock()
	return &ternv1.StopResponse{Accepted: true}, nil
}

func (s *capturingTernServer) Cutover(_ context.Context, req *ternv1.CutoverRequest) (*ternv1.CutoverResponse, error) {
	s.mu.Lock()
	s.cutoverApplyID = req.ApplyId
	err := s.cutoverErr
	accepted := s.cutoverAccepted
	message := s.cutoverMessage
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &ternv1.CutoverResponse{Accepted: accepted, ErrorMessage: message}, nil
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
	errorMessage := s.progressError
	err := s.progressErr
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if !psSet {
		ps = ternv1.State_STATE_COMPLETED
	}
	return &ternv1.ProgressResponse{State: ps, Tables: tables, ErrorMessage: errorMessage}, nil
}

func (s *capturingTernServer) getStartApplyID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.startApplyID
}

func (s *capturingTernServer) getStopApplyID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stopApplyID
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
	apply     *storage.Apply
	updateErr error
	updates   []*storage.Apply
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
	getByApplyIDErr     error
	getByOperationIDErr error
	lastOperationID     int64
}

func (m *mockTaskStore) GetByApplyID(context.Context, int64) ([]*storage.Task, error) {
	if m.getByApplyIDErr != nil {
		return nil, m.getByApplyIDErr
	}
	return m.tasks, nil
}

func (m *mockTaskStore) GetByApplyOperationID(_ context.Context, applyOperationID int64) ([]*storage.Task, error) {
	m.lastOperationID = applyOperationID
	if m.getByOperationIDErr != nil {
		return nil, m.getByOperationIDErr
	}
	return m.tasks, nil
}
func (m *mockTaskStore) Update(context.Context, *storage.Task) error { return nil }

type mockApplyLogStore struct {
	storage.ApplyLogStore
	logs []*storage.ApplyLog
}

func (m *mockApplyLogStore) Append(_ context.Context, log *storage.ApplyLog) error {
	stored := *log
	m.logs = append(m.logs, &stored)
	return nil
}

func (m *mockApplyLogStore) GetByApply(context.Context, int64) ([]*storage.ApplyLog, error) {
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

func TestGRPCClient_ResumeApplyOperationDispatchesScopedTasks(t *testing.T) {
	// An operator worker resumes a single apply_operation over the remote path.
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
	err := client.pollForCompletion(ctx, apply, false, wholeApplyTaskScope())
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
	err := client.pollForCompletion(ctx, apply, false, wholeApplyTaskScope())
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
	err := client.pollForCompletion(ctx, apply, true, wholeApplyTaskScope())
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

func TestGRPCClient_ResumeApplyDoesNotRegressRunningApplyToPendingProgress(t *testing.T) {
	// A freshly dispatched remote apply can report pending before the remote
	// engine starts copying rows. SchemaBot has already claimed the queued apply
	// locally, so progress polling must not write pending back to the stored
	// apply row and make it claimable by another operator worker.
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, applyStateFromRemoteProgress(tc.storedState, tc.remoteState, false))
		})
	}

	assert.Equal(t, state.Apply.Running,
		applyStateFromRemoteProgress(state.Apply.Stopped, state.Apply.Running, true),
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
	err := client.pollForCompletion(ctx, apply, false, wholeApplyTaskScope())
	require.NoError(t, err)

	assert.Equal(t, state.Task.Completed, task.State)
	assert.Equal(t, 100, task.ProgressPercent)
	require.NotNil(t, task.CompletedAt)
}

func TestGRPCClient_PollReturnsErrorWhenTerminalRemoteApplyLeavesStoredTaskActive(t *testing.T) {
	// A terminal remote apply with no terminal remote task state is inconsistent.
	// The control plane should retry instead of inventing a stored task result.
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
	err := client.pollForCompletion(ctx, apply, false, wholeApplyTaskScope())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "stored task task-terminal-missing-state is still running")
	assert.Equal(t, state.Apply.Running, applyStore.apply.State)
	assert.Nil(t, applyStore.apply.CompletedAt)
}

func hasLogMessageContaining(logs []*storage.ApplyLog, want string) bool {
	for _, log := range logs {
		if strings.Contains(log.Message, want) {
			return true
		}
	}
	return false
}

func TestGRPCClient_PollReturnsTerminalStorageUpdateError(t *testing.T) {
	// A terminal remote state is not enough by itself; the control plane must
	// persist that terminal state to storage before the operator worker exits.
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
	err := client.pollForCompletion(ctx, apply, false, wholeApplyTaskScope())
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
	err := client.pollForCompletion(ctx, apply, false, wholeApplyTaskScope())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "load tasks to sync terminal gRPC progress")
	assert.Contains(t, err.Error(), "task storage unavailable")
	assert.Equal(t, state.Apply.Running, applyStore.apply.State)
	assert.Nil(t, applyStore.apply.CompletedAt)
}

func TestGRPCClient_PollSkipsTaskFinalizationWhenStoredApplyAlreadyTerminal(t *testing.T) {
	// A stale worker can receive a terminal remote state after another worker
	// has already terminalized the stored apply row. In that case the worker must
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
	err := client.pollForCompletion(ctx, apply, false, wholeApplyTaskScope())
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
	// worker may have sent the remote Apply RPC and crashed before persisting the
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
			assert.Equal(t, tt.want, shouldDispatchQueuedRemoteApply(apply))
		})
	}
}

func TestGRPCClient_ResumeApply_ThreadsExternalID(t *testing.T) {
	// Progress returns STOPPED initially so ResumeApply checks remote state,
	// confirms the apply is stopped, and calls Start. After Start, the mock
	// transitions to COMPLETED so the poller exits.
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

	client := &GRPCClient{
		conn:    conn,
		client:  ternv1.NewTernClient(conn),
		storage: &mockStorage{},
	}
	defer utils.CloseAndLog(client)

	apply := &storage.Apply{
		ID:          1,
		Database:    "testdb",
		Environment: "staging",
		ExternalID:  "remote_tern_xyz",
		State:       state.Apply.Stopped,
	}

	err = client.ResumeApply(t.Context(), apply)
	require.NoError(t, err)

	// Verify Start received the external_id as apply_id
	assert.Equal(t, "remote_tern_xyz", server.getStartApplyID())

	// Progress returns STATE_COMPLETED after Start, so ResumeApply exits after
	// syncing one terminal progress response.
	assert.Equal(t, "remote_tern_xyz", server.getProgressApplyID())
}

func TestGRPCClient_ResumeApplyStartsQueuedStartAfterClaim(t *testing.T) {
	// An operator claim can move the apply row before the worker calls remote
	// Start. The durable control request lets a later worker recover that
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
	// A pending durable stop is processed by the operator-owned worker before
	// resume/start work. The worker mirrors remote stopped progress to storage
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
	// A pending durable cutover is processed by the operator-owned worker using
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
	assert.True(t, hasLogMessageContaining(logs.logs, "Remote cutover accepted for apply remote-cutover-claimed (caller: cli:alice)"))
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

	err := client.processPendingCutoverControlRequest(t.Context(), apply)
	require.NoError(t, err)
	assert.Empty(t, server.getCutoverApplyID())
	pendingCutover, err := controlRequests.GetPending(t.Context(), apply.ID, storage.ControlOperationCutover)
	require.NoError(t, err)
	require.NotNil(t, pendingCutover)
	assert.Equal(t, storage.ControlRequestPending, pendingCutover.Status)
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

	err := client.processPendingCutoverControlRequest(t.Context(), apply)
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
	assert.True(t, hasLogMessageContaining(logs.logs, "Remote cutover failed for apply remote-cutover-error (caller: cli:alice)"))
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
	// A Start request can be accepted while an older worker is still recording
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
	assert.True(t, hasLogMessageContaining(logs.logs, "Remote stopped-state check failed before scheduler start"))
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
	// stored apply fails so workers do not keep polling a stale remote ID.
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
	err := client.pollForCompletion(ctx, apply, false, wholeApplyTaskScope())
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
	err := client.pollForCompletion(ctx, apply, false, wholeApplyTaskScope())
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
	err := client.pollForCompletion(ctx, apply, false, wholeApplyTaskScope())
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
	err := client.pollForCompletion(ctx, apply, false, wholeApplyTaskScope())
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
