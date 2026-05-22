package tern

import (
	"context"
	"log/slog"
	"net"
	"os"
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
	if req.Database == "" {
		return nil, status.Error(codes.InvalidArgument, "database is required")
	}
	return &ternv1.ProgressResponse{
		State:  ternv1.State_STATE_RUNNING,
		Engine: ternv1.Engine_ENGINE_SPIRIT,
	}, nil
}

func (s *mockTernServer) Cutover(ctx context.Context, req *ternv1.CutoverRequest) (*ternv1.CutoverResponse, error) {
	if req.Database == "" {
		return nil, status.Error(codes.InvalidArgument, "database is required")
	}
	return &ternv1.CutoverResponse{Accepted: true}, nil
}

func (s *mockTernServer) Stop(ctx context.Context, req *ternv1.StopRequest) (*ternv1.StopResponse, error) {
	if req.Database == "" {
		return nil, status.Error(codes.InvalidArgument, "database is required")
	}
	return &ternv1.StopResponse{Accepted: true}, nil
}

func (s *mockTernServer) Start(ctx context.Context, req *ternv1.StartRequest) (*ternv1.StartResponse, error) {
	if req.Database == "" {
		return nil, status.Error(codes.InvalidArgument, "database is required")
	}
	return &ternv1.StartResponse{Accepted: true}, nil
}

func (s *mockTernServer) Volume(ctx context.Context, req *ternv1.VolumeRequest) (*ternv1.VolumeResponse, error) {
	if req.Database == "" {
		return nil, status.Error(codes.InvalidArgument, "database is required")
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
	if req.Database == "" {
		return nil, status.Error(codes.InvalidArgument, "database is required")
	}
	return &ternv1.RevertResponse{Accepted: true}, nil
}

func (s *mockTernServer) SkipRevert(ctx context.Context, req *ternv1.SkipRevertRequest) (*ternv1.SkipRevertResponse, error) {
	if req.Database == "" {
		return nil, status.Error(codes.InvalidArgument, "database is required")
	}
	return &ternv1.SkipRevertResponse{Accepted: true}, nil
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
			Database: "testdb",
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
			Database: "testdb",
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
			Database: "testdb",
		})
		require.NoError(t, err)
		assert.True(t, resp.Accepted)
	})

	t.Run("missing database", func(t *testing.T) {
		_, err := client.Stop(t.Context(), &ternv1.StopRequest{})
		require.Error(t, err)
	})
}

func TestGRPCClient_Start(t *testing.T) {
	client, cleanup := testClient(t, &mockTernServer{})
	defer cleanup()

	t.Run("valid request", func(t *testing.T) {
		resp, err := client.Start(t.Context(), &ternv1.StartRequest{
			Database: "testdb",
		})
		require.NoError(t, err)
		assert.True(t, resp.Accepted)
	})

	t.Run("missing database", func(t *testing.T) {
		_, err := client.Start(t.Context(), &ternv1.StartRequest{})
		require.Error(t, err)
	})
}

func TestGRPCClient_Volume(t *testing.T) {
	client, cleanup := testClient(t, &mockTernServer{})
	defer cleanup()

	t.Run("valid request", func(t *testing.T) {
		resp, err := client.Volume(t.Context(), &ternv1.VolumeRequest{
			Database: "testdb",
			Volume:   7,
		})
		require.NoError(t, err)
		assert.True(t, resp.Accepted)
		assert.Equal(t, int32(7), resp.NewVolume)
	})

	t.Run("invalid volume", func(t *testing.T) {
		_, err := client.Volume(t.Context(), &ternv1.VolumeRequest{
			Database: "testdb",
			Volume:   15,
		})
		require.Error(t, err)
	})
}

func TestGRPCClient_Revert(t *testing.T) {
	client, cleanup := testClient(t, &mockTernServer{})
	defer cleanup()

	t.Run("valid request", func(t *testing.T) {
		resp, err := client.Revert(t.Context(), &ternv1.RevertRequest{
			Database: "testdb",
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
			Database: "testdb",
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
	startApplyID     string
	progressApplyID  string
	progressState    ternv1.State // state returned by Progress; 0 = STATE_COMPLETED
	progressStateSet bool
	progressErr      error
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
	// After Start succeeds, transition to COMPLETED so the poller exits.
	s.progressState = ternv1.State_STATE_COMPLETED
	s.mu.Unlock()
	return &ternv1.StartResponse{Accepted: true}, nil
}

func (s *capturingTernServer) Progress(_ context.Context, req *ternv1.ProgressRequest) (*ternv1.ProgressResponse, error) {
	s.mu.Lock()
	s.progressApplyID = req.ApplyId
	ps := s.progressState
	psSet := s.progressStateSet
	err := s.progressErr
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if !psSet {
		ps = ternv1.State_STATE_COMPLETED
	}
	return &ternv1.ProgressResponse{State: ps}, nil
}

func (s *capturingTernServer) getStartApplyID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.startApplyID
}

func (s *capturingTernServer) getProgressApplyID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.progressApplyID
}

func (s *capturingTernServer) getApplyRequest() *ternv1.ApplyRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applyReq
}

// mockApplyStore is a minimal ApplyStore for testing ResumeApply.
type mockApplyStore struct {
	storage.ApplyStore
	apply *storage.Apply
}

func (m *mockApplyStore) Get(context.Context, int64) (*storage.Apply, error) {
	if m.apply == nil {
		return nil, nil
	}
	apply := *m.apply
	return &apply, nil
}
func (m *mockApplyStore) Update(_ context.Context, apply *storage.Apply) error {
	stored := *apply
	m.apply = &stored
	return nil
}
func (m *mockApplyStore) Heartbeat(context.Context, int64) error { return nil }

// mockTaskStore is a minimal TaskStore for testing pollForCompletion.
type mockTaskStore struct {
	storage.TaskStore
	tasks []*storage.Task
}

func (m *mockTaskStore) GetByApplyID(context.Context, int64) ([]*storage.Task, error) {
	return m.tasks, nil
}
func (m *mockTaskStore) Update(context.Context, *storage.Task) error { return nil }

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
	applies *mockApplyStore
	tasks   *mockTaskStore
	plans   *mockPlanStore
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
	// Scheduler claims start with a durable control-plane apply and pending
	// tasks but no external_id. ResumeApply dispatches the queued work to
	// remote Tern, stores the returned data-plane ID, then polls it to terminal.
	server := &capturingTernServer{remoteApplyID: "remote-dispatched-123"}
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
	// the data plane accepted the schema change. Leave durable state unchanged
	// so the scheduler does not record a false terminal failure.
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
	// status codes stay claimable for the scheduler; known-permanent status
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

// TestGRPCClient_ResumeApply_SkipsStartWhenNotStopped verifies that ResumeApply
// checks Tern's real state before calling Start. If Tern says the apply is already
// completed (local state diverged), Start is skipped and the poller still runs.
func TestGRPCClient_ResumeApply_SkipsStartWhenNotStopped(t *testing.T) {
	// Progress returns COMPLETED — Tern already finished the apply even though
	// local state says "stopped". Start should NOT be called.
	server := &capturingTernServer{
		progressState:    ternv1.State_STATE_COMPLETED,
		progressStateSet: true,
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
		ID:          1,
		Database:    "testdb",
		Environment: "staging",
		ExternalID:  "remote_tern_xyz",
		State:       state.Apply.Stopped, // local says stopped
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
}

func TestGRPCClient_PollFailsWhenRemoteApplyIsNotFound(t *testing.T) {
	// A known remote apply ID returning NotFound means the data plane can no
	// longer report progress for work the control plane believes exists. The
	// local apply fails so workers do not keep polling a stale remote ID.
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
	err := client.pollForCompletion(ctx, apply)
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
	// STATE_NO_ACTIVE_CHANGE is only valid for database-scoped discovery. An
	// exact apply-id progress request returning no active work is inconsistent
	// cross-plane state and should fail the local apply.
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
	err := client.pollForCompletion(ctx, apply)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no active schema change")

	assert.Equal(t, state.Apply.Failed, apply.State)
	assert.Contains(t, apply.ErrorMessage, "no active schema change")
	assert.NotNil(t, apply.CompletedAt)
	assert.Equal(t, state.Task.Failed, task.State)
	assert.Contains(t, task.ErrorMessage, "no active schema change")
	assert.NotNil(t, task.CompletedAt)
}

func TestGRPCClient_RemoteProgressLossDoesNotOverwriteTerminalApply(t *testing.T) {
	// Remote progress loss can fail the local apply only while the durable
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
	err := client.pollForCompletion(ctx, apply)
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
