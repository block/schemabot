package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// mockStorage implements storage.Storage for testing.
type mockStorage struct {
	pingErr error
}

func (m *mockStorage) Locks() storage.LockStore                      { return nil }
func (m *mockStorage) Plans() storage.PlanStore                      { return nil }
func (m *mockStorage) Applies() storage.ApplyStore                   { return nil }
func (m *mockStorage) Tasks() storage.TaskStore                      { return nil }
func (m *mockStorage) ApplyLogs() storage.ApplyLogStore              { return nil }
func (m *mockStorage) ControlRequests() storage.ControlRequestStore  { return nil }
func (m *mockStorage) ApplyComments() storage.ApplyCommentStore      { return nil }
func (m *mockStorage) ApplyOperations() storage.ApplyOperationStore  { return nil }
func (m *mockStorage) VitessApplyData() storage.VitessApplyDataStore { return nil }
func (m *mockStorage) Checks() storage.CheckStore                    { return nil }
func (m *mockStorage) Settings() storage.SettingsStore               { return nil }
func (m *mockStorage) Ping(ctx context.Context) error                { return m.pingErr }
func (m *mockStorage) Close() error                                  { return nil }

type mockPlanLookupStore struct {
	plan *storage.Plan
	err  error
}

func (m *mockPlanLookupStore) Create(context.Context, *storage.Plan) (int64, error) { return 0, nil }
func (m *mockPlanLookupStore) Get(context.Context, string) (*storage.Plan, error) {
	return m.plan, m.err
}
func (m *mockPlanLookupStore) GetByID(context.Context, int64) (*storage.Plan, error) { return nil, nil }
func (m *mockPlanLookupStore) GetByLock(context.Context, int64) ([]*storage.Plan, error) {
	return nil, nil
}
func (m *mockPlanLookupStore) GetByPR(context.Context, string, int) ([]*storage.Plan, error) {
	return nil, nil
}
func (m *mockPlanLookupStore) Delete(context.Context, int64) error           { return nil }
func (m *mockPlanLookupStore) DeleteByPR(context.Context, string, int) error { return nil }

type capturingPlanStore struct {
	mockPlanLookupStore
	created   *storage.Plan
	createErr error
}

func (s *capturingPlanStore) Create(_ context.Context, plan *storage.Plan) (int64, error) {
	s.created = plan
	if s.createErr != nil {
		return 0, s.createErr
	}
	return 1, nil
}

type mockStorageWithPlanLookup struct {
	mockStorage
	plans storage.PlanStore
}

func (m *mockStorageWithPlanLookup) Plans() storage.PlanStore { return m.plans }

type mockStorageWithApplyStores struct {
	mockStorage
	plans     storage.PlanStore
	applies   storage.ApplyStore
	tasks     storage.TaskStore
	locks     storage.LockStore
	applyLogs storage.ApplyLogStore
	controls  storage.ControlRequestStore
}

func (m *mockStorageWithApplyStores) Plans() storage.PlanStore         { return m.plans }
func (m *mockStorageWithApplyStores) Applies() storage.ApplyStore      { return m.applies }
func (m *mockStorageWithApplyStores) Tasks() storage.TaskStore         { return m.tasks }
func (m *mockStorageWithApplyStores) Locks() storage.LockStore         { return m.locks }
func (m *mockStorageWithApplyStores) ApplyLogs() storage.ApplyLogStore { return m.applyLogs }
func (m *mockStorageWithApplyStores) ControlRequests() storage.ControlRequestStore {
	return m.controls
}

type staticPlanStore struct {
	storage.PlanStore
	plan *storage.Plan
	err  error
}

func (s *staticPlanStore) Get(context.Context, string) (*storage.Plan, error) {
	return s.plan, s.err
}

type staticApplyStore struct {
	storage.ApplyStore
	apply *storage.Apply
	err   error
}

func (s *staticApplyStore) GetByApplyIdentifier(context.Context, string) (*storage.Apply, error) {
	return s.apply, s.err
}
func (s *staticApplyStore) Get(context.Context, int64) (*storage.Apply, error) {
	return s.apply, s.err
}
func (s *staticApplyStore) GetByDatabase(context.Context, string, string, string) ([]*storage.Apply, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.apply == nil {
		return nil, nil
	}
	return []*storage.Apply{s.apply}, nil
}
func (s *staticApplyStore) Update(_ context.Context, apply *storage.Apply) error {
	s.apply = apply
	return s.err
}

type recentApplyStore struct {
	storage.ApplyStore
	filters []storage.RecentAppliesFilter
	applies []*storage.Apply
	err     error
}

func (s *recentApplyStore) GetRecent(_ context.Context, filter storage.RecentAppliesFilter) ([]*storage.Apply, error) {
	s.filters = append(s.filters, filter)
	if s.err != nil {
		return nil, s.err
	}
	applies := make([]*storage.Apply, 0, len(s.applies))
	for _, apply := range s.applies {
		if filter.Environment != "" && apply.Environment != filter.Environment {
			continue
		}
		if len(filter.States) > 0 && !state.IsState(apply.State, filter.States...) {
			continue
		}
		applies = append(applies, apply)
	}
	if filter.Limit > 0 && len(applies) > filter.Limit {
		return applies[:filter.Limit], nil
	}
	return applies, nil
}

type memoryControlRequestStore struct {
	storage.ControlRequestStore
	mu       sync.Mutex
	nextID   int64
	requests []*storage.ApplyControlRequest
}

func (s *memoryControlRequestStore) RequestPending(_ context.Context, req *storage.ApplyControlRequest) (*storage.ApplyControlRequest, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.requests {
		if existing.ApplyID == req.ApplyID && existing.Operation == req.Operation {
			if existing.Status == storage.ControlRequestPending {
				return cloneControlRequest(existing), true, nil
			}
			existing.Status = storage.ControlRequestPending
			existing.RequestedBy = req.RequestedBy
			existing.ErrorMessage = ""
			existing.Metadata = append(existing.Metadata[:0], req.Metadata...)
			existing.CompletedAt = nil
			return cloneControlRequest(existing), false, nil
		}
	}
	s.nextID++
	stored := cloneControlRequest(req)
	stored.ID = s.nextID
	s.requests = append(s.requests, stored)
	return cloneControlRequest(stored), false, nil
}

func (s *memoryControlRequestStore) GetPending(_ context.Context, applyID int64, operation storage.ControlOperation) (*storage.ApplyControlRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range slices.Backward(s.requests) {
		req := v
		if req.ApplyID == applyID && req.Operation == operation && req.Status == storage.ControlRequestPending {
			return cloneControlRequest(req), nil
		}
	}
	return nil, nil
}

func (s *memoryControlRequestStore) CompletePending(_ context.Context, applyID int64, operation storage.ControlOperation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, req := range s.requests {
		if req.ApplyID == applyID && req.Operation == operation && req.Status == storage.ControlRequestPending {
			req.Status = storage.ControlRequestCompleted
		}
	}
	return nil
}

func (s *memoryControlRequestStore) FailPending(_ context.Context, applyID int64, operation storage.ControlOperation, errorMessage string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, req := range s.requests {
		if req.ApplyID == applyID && req.Operation == operation && req.Status == storage.ControlRequestPending {
			req.Status = storage.ControlRequestFailed
			req.ErrorMessage = errorMessage
		}
	}
	return nil
}

func cloneControlRequest(req *storage.ApplyControlRequest) *storage.ApplyControlRequest {
	if req == nil {
		return nil
	}
	clone := *req
	if req.Metadata != nil {
		clone.Metadata = append([]byte(nil), req.Metadata...)
	}
	return &clone
}

type capturingApplyStore struct {
	storage.ApplyStore
	mu         sync.Mutex
	apply      *storage.Apply
	operations []*storage.ApplyOperation
	taskStore  *capturingTaskStore
	claimed    bool
	findCh     chan struct{}
	err        error
}

func (s *capturingApplyStore) Create(_ context.Context, apply *storage.Apply) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.apply = apply
	if s.err != nil {
		return 0, s.err
	}
	return 123, nil
}

func (s *capturingApplyStore) CreateWithTasks(ctx context.Context, apply *storage.Apply, tasks []*storage.Task) (int64, error) {
	return s.CreateWithTasksAndOperations(ctx, apply, tasks, nil)
}

func (s *capturingApplyStore) CreateWithTasksAndOperations(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, operations []*storage.ApplyOperation) (int64, error) {
	s.mu.Lock()
	if s.err != nil {
		err := s.err
		s.mu.Unlock()
		return 0, err
	}
	s.mu.Unlock()

	applyID := int64(123)
	previousTaskCount := 0
	if s.taskStore != nil {
		s.taskStore.mu.Lock()
		previousTaskCount = len(s.taskStore.tasks)
		s.taskStore.mu.Unlock()
	}
	for _, task := range tasks {
		task.ApplyID = applyID
		if s.taskStore != nil {
			if _, err := s.taskStore.Create(ctx, task); err != nil {
				s.taskStore.mu.Lock()
				s.taskStore.tasks = s.taskStore.tasks[:previousTaskCount]
				s.taskStore.mu.Unlock()
				return 0, err
			}
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.apply = apply
	s.operations = append(s.operations, operations...)
	return applyID, nil
}

func (s *capturingApplyStore) Update(_ context.Context, apply *storage.Apply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.apply = apply
	return nil
}

func (s *capturingApplyStore) FindNextApply(_ context.Context, owner string) (*storage.Apply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.findCh != nil {
		select {
		case s.findCh <- struct{}{}:
		default:
		}
	}
	if s.apply == nil || s.claimed {
		return nil, nil
	}
	s.claimed = true
	apply := *s.apply
	apply.ID = 123
	apply.LeaseOwner = owner
	apply.LeaseToken = "test-lease-token"
	return &apply, nil
}

func (s *capturingApplyStore) CheckLease(context.Context, storage.ApplyLease) error {
	return nil
}

func (s *capturingApplyStore) ExpireRetryable(context.Context) ([]*storage.RetryableApplyExpiration, error) {
	return nil, nil
}

type capturingTaskStore struct {
	storage.TaskStore
	mu           sync.Mutex
	tasks        []*storage.Task
	createCalls  int
	failOnCreate int
	err          error
}

func (s *capturingTaskStore) Create(_ context.Context, task *storage.Task) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.createCalls++
	if s.failOnCreate == s.createCalls {
		if s.err != nil {
			return 0, s.err
		}
		return 0, errors.New("create task failed")
	}
	task.ID = int64(len(s.tasks) + 1)
	s.tasks = append(s.tasks, task)
	return int64(len(s.tasks)), nil
}

func (s *capturingTaskStore) Update(_ context.Context, task *storage.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, storedTask := range s.tasks {
		if storedTask.ID == task.ID || storedTask.TaskIdentifier == task.TaskIdentifier {
			s.tasks[i] = task
			return nil
		}
	}
	return storage.ErrTaskNotFound
}
func (s *capturingTaskStore) GetByApplyID(_ context.Context, applyID int64) ([]*storage.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var tasks []*storage.Task
	for _, task := range s.tasks {
		if task.ApplyID == applyID {
			tasks = append(tasks, task)
		}
	}
	return tasks, s.err
}

type emptyLockStore struct {
	storage.LockStore
}

func (s *emptyLockStore) Get(context.Context, string, string) (*storage.Lock, error) {
	return nil, nil
}

type noopApplyLogStore struct {
	storage.ApplyLogStore
}

func (s *noopApplyLogStore) Append(context.Context, *storage.ApplyLog) error {
	return nil
}

type capturingApplyLogStore struct {
	storage.ApplyLogStore
	logs []*storage.ApplyLog
}

func (s *capturingApplyLogStore) Append(_ context.Context, log *storage.ApplyLog) error {
	stored := *log
	s.logs = append(s.logs, &stored)
	return nil
}

func hasApplyLogMessageContaining(logs []*storage.ApplyLog, want string) bool {
	for _, log := range logs {
		if strings.Contains(log.Message, want) {
			return true
		}
	}
	return false
}

// mockTernClient implements tern.Client for testing.
type mockTernClient struct {
	healthErr      error
	planResp       *ternv1.PlanResponse
	planErr        error
	planReq        *ternv1.PlanRequest
	applyResp      *ternv1.ApplyResponse
	applyErr       error
	applyReq       *ternv1.ApplyRequest
	progressResp   *ternv1.ProgressResponse
	progressErr    error
	progressReq    *ternv1.ProgressRequest
	volumeResp     *ternv1.VolumeResponse
	volumeErr      error
	volumeReq      *ternv1.VolumeRequest // captured request
	stopResp       *ternv1.StopResponse
	stopErr        error
	stopReq        *ternv1.StopRequest // captured request
	stopHook       func()
	startResp      *ternv1.StartResponse
	startErr       error
	startReq       *ternv1.StartRequest // captured request
	cutoverResp    *ternv1.CutoverResponse
	cutoverErr     error
	cutoverReq     *ternv1.CutoverRequest // captured request
	revertResp     *ternv1.RevertResponse
	revertErr      error
	revertReq      *ternv1.RevertRequest // captured request
	skipRevertResp *ternv1.SkipRevertResponse
	skipRevertErr  error
	skipRevertReq  *ternv1.SkipRevertRequest // captured request
	resumeMu       sync.Mutex
	resumeErr      error
	resumeApply    *storage.Apply
	resumeCh       chan *storage.Apply
	isRemote       bool
}

func (m *mockTernClient) Health(ctx context.Context) error { return m.healthErr }
func (m *mockTernClient) Plan(ctx context.Context, req *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	m.planReq = req
	if m.planResp != nil {
		return m.planResp, m.planErr
	}
	return nil, m.planErr
}
func (m *mockTernClient) Apply(ctx context.Context, req *ternv1.ApplyRequest) (*ternv1.ApplyResponse, error) {
	m.applyReq = req
	if m.applyResp != nil {
		return m.applyResp, m.applyErr
	}
	return nil, m.applyErr
}
func (m *mockTernClient) Progress(ctx context.Context, req *ternv1.ProgressRequest) (*ternv1.ProgressResponse, error) {
	m.progressReq = req
	if m.progressResp != nil {
		return m.progressResp, m.progressErr
	}
	return nil, m.progressErr
}
func (m *mockTernClient) Cutover(ctx context.Context, req *ternv1.CutoverRequest) (*ternv1.CutoverResponse, error) {
	m.cutoverReq = req
	if m.cutoverResp != nil {
		return m.cutoverResp, m.cutoverErr
	}
	return nil, m.cutoverErr
}
func (m *mockTernClient) Stop(ctx context.Context, req *ternv1.StopRequest) (*ternv1.StopResponse, error) {
	m.stopReq = req
	if m.stopHook != nil {
		m.stopHook()
	}
	if m.stopResp != nil {
		return m.stopResp, m.stopErr
	}
	return nil, m.stopErr
}
func (m *mockTernClient) Start(ctx context.Context, req *ternv1.StartRequest) (*ternv1.StartResponse, error) {
	m.startReq = req
	if m.startResp != nil {
		return m.startResp, m.startErr
	}
	return nil, m.startErr
}
func (m *mockTernClient) Volume(ctx context.Context, req *ternv1.VolumeRequest) (*ternv1.VolumeResponse, error) {
	m.volumeReq = req
	if m.volumeResp != nil {
		return m.volumeResp, m.volumeErr
	}
	return nil, m.volumeErr
}
func (m *mockTernClient) Revert(ctx context.Context, req *ternv1.RevertRequest) (*ternv1.RevertResponse, error) {
	m.revertReq = req
	if m.revertResp != nil {
		return m.revertResp, m.revertErr
	}
	return nil, m.revertErr
}
func (m *mockTernClient) SkipRevert(ctx context.Context, req *ternv1.SkipRevertRequest) (*ternv1.SkipRevertResponse, error) {
	m.skipRevertReq = req
	if m.skipRevertResp != nil {
		return m.skipRevertResp, m.skipRevertErr
	}
	return nil, m.skipRevertErr
}
func (m *mockTernClient) RollbackPlan(ctx context.Context, database string) (*ternv1.PlanResponse, error) {
	return nil, nil
}
func (m *mockTernClient) ResumeApply(ctx context.Context, apply *storage.Apply) error {
	m.resumeMu.Lock()
	m.resumeApply = apply
	resumeCh := m.resumeCh
	resumeErr := m.resumeErr
	m.resumeMu.Unlock()

	if resumeCh != nil {
		select {
		case resumeCh <- apply:
		default:
		}
	}
	return resumeErr
}
func (m *mockTernClient) Endpoint() string                                  { return "mock" }
func (m *mockTernClient) IsRemote() bool                                    { return m.isRemote }
func (m *mockTernClient) SetPendingObserver(observer tern.ProgressObserver) {}
func (m *mockTernClient) SetObserver(applyID int64, observer tern.ProgressObserver) {
}
func (m *mockTernClient) Close() error { return nil }

// testServerConfig returns a minimal valid ServerConfig for testing.
// Only includes "staging" environment - tests that need "production"
// should create their own config or add it to the mock ternClients.
func testServerConfig() *ServerConfig {
	return &ServerConfig{
		TernDeployments: TernConfig{
			"default": TernEndpoints{
				"staging": "localhost:9090",
			},
		},
	}
}

func executeApplyTestPlan() *storage.Plan {
	return &storage.Plan{
		ID:             1,
		PlanIdentifier: "plan-1",
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     DefaultDeployment,
		Target:         "testdb",
		Environment:    "staging",
		Namespaces: map[string]*storage.NamespacePlanData{
			"testdb": {
				Tables: []storage.TableChange{
					{
						Namespace: "testdb",
						Table:     "users",
						DDL:       "ALTER TABLE users ADD COLUMN email varchar(255)",
						Operation: "alter",
					},
				},
			},
		},
	}
}

func activeTestApply(applyID string) *storage.Apply {
	return &storage.Apply{
		ID:              1,
		ApplyIdentifier: applyID,
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Deployment:      DefaultDeployment,
		Environment:     "staging",
		State:           state.Apply.Running,
	}
}

func stoppedTestApply(applyID string) *storage.Apply {
	apply := activeTestApply(applyID)
	apply.State = state.Apply.Stopped
	startedAt := time.Now().Add(-time.Minute)
	apply.StartedAt = &startedAt
	return apply
}

func newExecuteApplyTestService(client tern.Client, applies storage.ApplyStore) (*Service, *capturingTaskStore) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	tasks := &capturingTaskStore{}
	if capturingApplies, ok := applies.(*capturingApplyStore); ok {
		capturingApplies.taskStore = tasks
	}
	return New(&mockStorageWithApplyStores{
		plans:     &staticPlanStore{plan: executeApplyTestPlan()},
		applies:   applies,
		tasks:     tasks,
		locks:     &emptyLockStore{},
		applyLogs: &noopApplyLogStore{},
		controls:  &memoryControlRequestStore{},
	}, testServerConfig(), map[string]tern.Client{
		"default/staging": client,
	}, logger), tasks
}

func newControlTestService(client tern.Client, apply *storage.Apply) *Service {
	return newControlTestServiceWithTasks(client, apply, nil)
}

func newControlTestServiceWithTasks(client tern.Client, apply *storage.Apply, tasks []*storage.Task) *Service {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(&mockStorageWithApplyStores{
		applies:  &staticApplyStore{apply: apply},
		tasks:    &capturingTaskStore{tasks: tasks},
		controls: &memoryControlRequestStore{},
	}, testServerConfig(), map[string]tern.Client{
		"default/staging": client,
	}, logger)
}

func newTestService() *Service {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(&mockStorage{}, testServerConfig(), nil, logger)
}

func TestExecutePlanSourcePolicy(t *testing.T) {
	newPolicyService := func() (*Service, *mockTernClient, *capturingPlanStore) {
		t.Helper()
		plans := &capturingPlanStore{}
		mockClient := &mockTernClient{
			planResp: &ternv1.PlanResponse{PlanId: "plan-source-policy"},
		}
		cfg := &ServerConfig{
			Databases: map[string]DatabaseConfig{
				"payments": {
					Type: storage.DatabaseTypeMySQL,
					Environments: map[string]EnvironmentConfig{
						"staging": {Target: "payments-staging-target", Deployment: DefaultDeployment},
					},
					AllowedRepos: []string{"octocat/hello-world"},
					AllowedDirs:  []string{"schema/payments"},
				},
			},
			TernDeployments: TernConfig{
				DefaultDeployment: {"staging": "localhost:9090"},
			},
		}
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		svc := New(&mockStorageWithPlanLookup{plans: plans}, cfg, map[string]tern.Client{
			DefaultDeployment + "/staging": mockClient,
		}, logger)
		return svc, mockClient, plans
	}

	schemaFiles := map[string]*ternv1.SchemaFiles{
		"payments": {Files: map[string]string{"users.sql": "CREATE TABLE users (id bigint primary key)"}},
	}

	t.Run("trusted GitHub source is authorized and persisted", func(t *testing.T) {
		svc, mockClient, plans := newPolicyService()
		pr := int32(1)

		resp, err := svc.ExecutePlan(t.Context(), PlanRequest{
			Database:      "payments",
			Environment:   "staging",
			Type:          storage.DatabaseTypeMySQL,
			SchemaFiles:   schemaFiles,
			Repository:    "octocat/hello-world",
			PullRequest:   &pr,
			SchemaPath:    "schema/payments",
			SourceTrusted: true,
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, "plan-source-policy", resp.PlanID)
		require.NotNil(t, mockClient.planReq, "expected source-authorized plan to call Tern")
		assert.Equal(t, "payments-staging-target", mockClient.planReq.Target)
		assert.Equal(t, "schema/payments", mockClient.planReq.SchemaPath)
		require.NotNil(t, plans.created, "expected source-authorized plan to be stored")
		assert.Equal(t, "schema/payments", plans.created.SchemaPath)
		assert.Equal(t, "octocat/hello-world", plans.created.Repository)
	})

	t.Run("direct API source keeps operator path working", func(t *testing.T) {
		svc, mockClient, plans := newPolicyService()
		pr := int32(1)

		resp, err := svc.ExecutePlan(t.Context(), PlanRequest{
			Database:    "payments",
			Environment: "staging",
			Type:        storage.DatabaseTypeMySQL,
			SchemaFiles: schemaFiles,
			Repository:  "octocat/hello-world",
			PullRequest: &pr,
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, "plan-source-policy", resp.PlanID)
		require.NotNil(t, mockClient.planReq, "direct API planning should still call Tern")
		assert.Empty(t, mockClient.planReq.SchemaPath)
		require.NotNil(t, plans.created, "direct API planning should still store the plan")
		assert.Empty(t, plans.created.SchemaPath)
	})

	t.Run("duplicate plan identifier is tolerated", func(t *testing.T) {
		svc, _, plans := newPolicyService()
		plans.createErr = storage.ErrPlanIDExists
		pr := int32(1)

		resp, err := svc.ExecutePlan(t.Context(), PlanRequest{
			Database:      "payments",
			Environment:   "staging",
			Type:          storage.DatabaseTypeMySQL,
			SchemaFiles:   schemaFiles,
			Repository:    "octocat/hello-world",
			PullRequest:   &pr,
			SchemaPath:    "schema/payments",
			SourceTrusted: true,
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, plans.created)
		assert.Equal(t, "schema/payments", plans.created.SchemaPath)
	})
}

func TestExecutePlanUnavailableRemoteErrorIncludesDeployment(t *testing.T) {
	plans := &capturingPlanStore{}
	mockClient := &mockTernClient{
		planErr:  status.Error(codes.Unavailable, "no healthy upstream"),
		isRemote: true,
	}
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"orders": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"staging": {Target: "orders-staging", Deployment: "pie"},
				},
			},
		},
		TernDeployments: TernConfig{
			"pie": {"staging": "tern.example.com:80"},
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithPlanLookup{plans: plans}, cfg, map[string]tern.Client{
		"pie/staging": mockClient,
	}, logger)

	_, err := svc.ExecutePlan(t.Context(), PlanRequest{
		Database:    "orders",
		Environment: "staging",
		Type:        storage.DatabaseTypeMySQL,
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"orders": {Files: map[string]string{"users.sql": "CREATE TABLE users (id bigint primary key)"}},
		},
		Repository: "example/app",
	})

	var remoteErr *RemoteDeploymentUnavailableError
	require.ErrorAs(t, err, &remoteErr)
	assert.Equal(t, "pie", remoteErr.Deployment)
	assert.Equal(t, "orders-staging", remoteErr.Target)
	assert.Equal(t, codes.Unavailable, status.Code(err))
}

func TestExecuteApplySourcePolicyAllowsDirectPlan(t *testing.T) {
	plan := &storage.Plan{
		ID:             42,
		PlanIdentifier: "plan-old-direct",
		Database:       "payments",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     DefaultDeployment,
		Target:         "payments-staging-target",
		Repository:     "octocat/hello-world",
		PullRequest:    1,
		Environment:    "staging",
		Namespaces: map[string]*storage.NamespacePlanData{
			"payments": {
				Tables: []storage.TableChange{
					{
						Namespace: "payments",
						Table:     "users",
						DDL:       "ALTER TABLE users ADD COLUMN email varchar(255)",
						Operation: "alter",
					},
				},
			},
		},
	}
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"payments": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"staging": {Target: "payments-staging-target", Deployment: DefaultDeployment},
				},
				AllowedRepos: []string{"octocat/hello-world"},
			},
		},
		TernDeployments: TernConfig{
			DefaultDeployment: {"staging": "localhost:9090"},
		},
	}
	applies := &capturingApplyStore{}
	tasks := &capturingTaskStore{}
	applies.taskStore = tasks
	stor := &mockStorageWithApplyStores{
		plans:     &mockPlanLookupStore{plan: plan},
		applies:   applies,
		tasks:     tasks,
		locks:     &emptyLockStore{},
		applyLogs: &noopApplyLogStore{},
	}
	mockClient := &mockTernClient{isRemote: true}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(stor, cfg, map[string]tern.Client{
		DefaultDeployment + "/staging": mockClient,
	}, logger)

	resp, applyID, err := svc.ExecuteApply(t.Context(), ApplyRequest{
		PlanID:      "plan-old-direct",
		Environment: "staging",
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Accepted)
	assert.Equal(t, int64(123), applyID)
	assert.Nil(t, mockClient.applyReq, "direct API apply should queue work without dispatching remote Tern")
	require.NotNil(t, applies.apply)
	assert.Equal(t, state.Apply.Pending, applies.apply.State)
	assert.Equal(t, "payments-staging-target", applies.apply.GetOptions().Target)
	require.Len(t, tasks.tasks, 1)
	assert.Equal(t, state.Task.Pending, tasks.tasks[0].State)
}

func TestExecuteApplySourcePolicyBlocksStoredTrustedPlan(t *testing.T) {
	plan := &storage.Plan{
		ID:             42,
		PlanIdentifier: "plan-untrusted-repo",
		Database:       "payments",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     DefaultDeployment,
		Target:         "payments-staging-target",
		Repository:     "octocat/orders",
		PullRequest:    1,
		SchemaPath:     "schema/payments",
		Environment:    "staging",
	}
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"payments": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"staging": {Target: "payments-staging-target", Deployment: DefaultDeployment},
				},
				AllowedRepos: []string{"octocat/hello-world"},
			},
		},
		TernDeployments: TernConfig{
			DefaultDeployment: {"staging": "localhost:9090"},
		},
	}
	mockClient := &mockTernClient{
		applyResp: &ternv1.ApplyResponse{Accepted: false, ErrorMessage: "engine rejected"},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithPlanLookup{
		plans: &mockPlanLookupStore{plan: plan},
	}, cfg, map[string]tern.Client{
		DefaultDeployment + "/staging": mockClient,
	}, logger)

	resp, applyID, err := svc.ExecuteApply(t.Context(), ApplyRequest{
		PlanID:      "plan-untrusted-repo",
		Environment: "staging",
	})

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Zero(t, applyID)
	assert.Nil(t, mockClient.applyReq, "stored trusted plan with unauthorized source should not call Tern")
	var policyErr *SourcePolicyError
	require.True(t, errors.As(err, &policyErr), "expected SourcePolicyError")
	assert.Equal(t, SourcePolicyReasonUnauthorizedRepo, policyErr.Reason)
}

func TestExecuteApplySourcePolicyBlocksMissingDatabaseConfig(t *testing.T) {
	plan := &storage.Plan{
		ID:             42,
		PlanIdentifier: "plan-missing-database-config",
		Database:       "payments",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     DefaultDeployment,
		Target:         "payments-staging-target",
		Repository:     "octocat/hello-world",
		PullRequest:    1,
		SchemaPath:     "schema/payments",
		Environment:    "staging",
	}
	cfg := &ServerConfig{
		TernDeployments: TernConfig{
			DefaultDeployment: {"staging": "localhost:9090"},
		},
	}
	mockClient := &mockTernClient{
		applyResp: &ternv1.ApplyResponse{Accepted: false, ErrorMessage: "engine rejected"},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithPlanLookup{
		plans: &mockPlanLookupStore{plan: plan},
	}, cfg, map[string]tern.Client{
		DefaultDeployment + "/staging": mockClient,
	}, logger)

	resp, applyID, err := svc.ExecuteApply(t.Context(), ApplyRequest{
		PlanID:      "plan-missing-database-config",
		Environment: "staging",
	})

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Zero(t, applyID)
	assert.Nil(t, mockClient.applyReq, "stored trusted plan without database config should not call Tern")
	var policyErr *SourcePolicyError
	require.True(t, errors.As(err, &policyErr), "expected SourcePolicyError")
	assert.Equal(t, SourcePolicyReasonMissingDatabaseConfig, policyErr.Reason)
}

func TestPlanHandlerRejectsClientSuppliedSchemaPath(t *testing.T) {
	svc := newTestService()
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	body := `{
		"database": "payments",
		"environment": "staging",
		"type": "mysql",
		"schema_path": "schema/payments",
		"schema_files": {"payments": {"files": {"users.sql": "CREATE TABLE users (id bigint primary key)"}}}
	}`
	req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/plan", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "unknown field")
	assert.Contains(t, w.Body.String(), "schema_path")
}

func TestPlanHandlerSourcePolicyAllowsDirectSource(t *testing.T) {
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"payments": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"staging": {Target: "payments-staging-target", Deployment: DefaultDeployment},
				},
				AllowedRepos: []string{"octocat/hello-world"},
				AllowedDirs:  []string{"schema/payments"},
			},
		},
		TernDeployments: TernConfig{
			DefaultDeployment: {"staging": "localhost:9090"},
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	mockClient := &mockTernClient{planResp: &ternv1.PlanResponse{PlanId: "plan-source-policy"}}
	svc := New(&mockStorageWithPlanLookup{plans: &capturingPlanStore{}}, cfg, map[string]tern.Client{
		DefaultDeployment + "/staging": mockClient,
	}, logger)
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	body := `{
		"database": "payments",
		"environment": "staging",
		"type": "mysql",
		"repository": "octocat/hello-world",
		"pull_request": 1,
		"schema_files": {"payments": {"files": {"users.sql": "CREATE TABLE users (id bigint primary key)"}}}
	}`
	req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/plan", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NotNil(t, mockClient.planReq, "direct HTTP planning should still call Tern")
	assert.Empty(t, mockClient.planReq.SchemaPath)
	var resp apitypes.PlanResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "plan-source-policy", resp.PlanID)
}

func TestExecuteApplyQueuesRemoteApplyForOperator(t *testing.T) {
	// Remote applies follow the same durable queue path as local applies. The
	// request returns the control-plane apply ID before the operator dispatches
	// work to remote Tern and stores external_id.
	applies := &capturingApplyStore{}
	mock := &mockTernClient{isRemote: true}
	svc, tasks := newExecuteApplyTestService(mock, applies)

	resp, applyID, err := svc.ExecuteApply(t.Context(), ApplyRequest{
		PlanID:      "plan-1",
		Environment: "staging",
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Accepted)
	assert.Equal(t, int64(123), applyID)
	assert.NotEmpty(t, resp.ApplyID)
	require.NotNil(t, applies.apply)
	assert.Equal(t, state.Apply.Pending, applies.apply.State)
	assert.Empty(t, applies.apply.ExternalID)
	assert.Equal(t, storage.EngineSpirit, applies.apply.Engine)
	assert.Equal(t, "testdb", applies.apply.GetOptions().Target)
	assert.Nil(t, mock.applyReq, "request path should not call remote Tern before operator claim")
	require.Len(t, tasks.tasks, 1)
	assert.Equal(t, state.Task.Pending, tasks.tasks[0].State)
	// Apply create dual-writes one apply_operations row mirroring the apply's
	// (deployment, target). Today's hard-block keeps this at one row; the
	// operator claim-loop PR is what consumes them.
	require.Len(t, applies.operations, 1)
	assert.Equal(t, DefaultDeployment, applies.operations[0].Deployment)
	assert.Equal(t, "testdb", applies.operations[0].Target)
	assert.Equal(t, state.ApplyOperation.Pending, applies.operations[0].State)
}

func TestProgressByApplyIDServesQueuedRemoteApplyFromStorage(t *testing.T) {
	// The operator marks the control-plane row running before gRPC dispatch
	// stores external_id. During that handoff, apply-id progress should be
	// served locally as pending instead of asking the data plane about an ID it
	// does not know yet.
	mock := &mockTernClient{
		isRemote:    true,
		progressErr: errors.New("remote progress should not be called before external_id is set"),
	}
	apply := activeTestApply("apply-queued-remote")
	apply.ExternalID = ""
	apply.State = state.Apply.Running
	task := &storage.Task{
		ID:             1,
		ApplyID:        apply.ID,
		TaskIdentifier: "task-queued-remote",
		Database:       apply.Database,
		DatabaseType:   apply.DatabaseType,
		Environment:    apply.Environment,
		TableName:      "users",
		State:          state.Task.Pending,
		Engine:         storage.EngineSpirit,
	}
	svc := newControlTestServiceWithTasks(mock, apply, []*storage.Task{task})
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/progress/apply/apply-queued-remote", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Nil(t, mock.progressReq, "remote progress should wait until operator stores external_id")

	var resp apitypes.ProgressResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "apply-queued-remote", resp.ApplyID)
	assert.Equal(t, state.Apply.Pending, resp.State)
	require.Len(t, resp.Tables, 1)
	assert.Equal(t, "users", resp.Tables[0].TableName)
}

func TestDatabaseEnvironmentsUsesServerPromotionOrder(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorage{}, &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"testdb": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"sandbox":    {},
					"staging":    {},
					"production": {},
				},
			},
		},
		EnvironmentOrder: []string{"production", "staging"},
	}, nil, logger)
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/databases/testdb/environments", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var resp struct {
		Database     string   `json:"database"`
		Environments []string `json:"environments"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "testdb", resp.Database)
	assert.Equal(t, []string{"production", "staging", "sandbox"}, resp.Environments)
}

func TestProgressByApplyIDResolvesExternalIDForRemoteApply(t *testing.T) {
	mock := &mockTernClient{
		isRemote: true,
		progressResp: &ternv1.ProgressResponse{
			ApplyId: "remote-apply-123",
			State:   ternv1.State_STATE_RUNNING,
		},
	}
	apply := activeTestApply("apply-control-123")
	apply.ExternalID = "remote-apply-123"
	svc := newControlTestService(mock, apply)
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/progress/apply/apply-control-123", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NotNil(t, mock.progressReq)
	assert.Equal(t, "remote-apply-123", mock.progressReq.ApplyId)
	assert.Equal(t, "staging", mock.progressReq.Environment)

	var resp apitypes.ProgressResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "apply-control-123", resp.ApplyID)
	assert.Equal(t, state.Apply.Running, resp.State)
}

func TestProgressByApplyIDOnlySendsApplyIDAndEnvironment(t *testing.T) {
	// Remote progress lookups use the apply ID as the stable routing key. The
	// data plane should not need database routing hints to interpret that ID.
	mock := &mockTernClient{
		isRemote: true,
		progressResp: &ternv1.ProgressResponse{
			State: ternv1.State_STATE_RUNNING,
		},
	}
	apply := activeTestApply("apply-active-remote")
	apply.DatabaseType = storage.DatabaseTypeVitess
	apply.ExternalID = "remote-active-remote"
	svc := newControlTestService(mock, apply)
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/progress/apply/apply-active-remote", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NotNil(t, mock.progressReq)
	assert.Equal(t, "remote-active-remote", mock.progressReq.ApplyId)
	assert.Equal(t, "staging", mock.progressReq.Environment)
}

func TestExecuteApplyQueuesLocalApplyForOperator(t *testing.T) {
	applies := &capturingApplyStore{}
	mock := &mockTernClient{}
	svc, tasks := newExecuteApplyTestService(mock, applies)

	resp, applyID, err := svc.ExecuteApply(t.Context(), ApplyRequest{
		PlanID:      "plan-1",
		Environment: "staging",
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Accepted)
	assert.Equal(t, int64(123), applyID)
	assert.NotEmpty(t, resp.ApplyID)
	require.NotNil(t, applies.apply)
	assert.Equal(t, state.Apply.Pending, applies.apply.State)
	assert.Equal(t, storage.EngineSpirit, applies.apply.Engine)
	assert.Equal(t, "testdb", applies.apply.GetOptions().Target)
	assert.Nil(t, mock.applyReq, "request path should enqueue work without dispatching the engine")
	require.Len(t, tasks.tasks, 1)
	assert.Equal(t, state.Task.Pending, tasks.tasks[0].State)
}

func TestExecuteApplyDoesNotStorePartialQueueWhenTaskCreateFails(t *testing.T) {
	plan := executeApplyTestPlan()
	plan.Namespaces["testdb"].Tables = append(plan.Namespaces["testdb"].Tables, storage.TableChange{
		Namespace: "testdb",
		Table:     "orders",
		DDL:       "ALTER TABLE orders ADD COLUMN status varchar(255)",
		Operation: "alter",
	})

	applies := &capturingApplyStore{}
	tasks := &capturingTaskStore{
		failOnCreate: 2,
		err:          errors.New("task insert failed"),
	}
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

	resp, applyID, err := svc.ExecuteApply(t.Context(), ApplyRequest{
		PlanID:      "plan-1",
		Environment: "staging",
	})

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Zero(t, applyID)
	assert.Contains(t, err.Error(), "task insert failed")
	assert.Nil(t, applies.apply)
	assert.Empty(t, tasks.tasks)
}

func TestExecuteApplyWakesOperatorForQueuedLocalApply(t *testing.T) {
	applies := &capturingApplyStore{findCh: make(chan struct{}, 1)}
	mock := &mockTernClient{resumeCh: make(chan *storage.Apply, 1)}
	svc, _ := newExecuteApplyTestService(mock, applies)
	svc.config.OperatorWorkers = 1
	require.NoError(t, svc.SetOperatorPollInterval(time.Hour))
	svc.StartOperator(t.Context())
	t.Cleanup(svc.StopOperator)

	select {
	case <-applies.findCh:
	case <-time.After(2 * time.Second):
		require.Fail(t, "operator did not perform startup claim")
	}

	resp, applyID, err := svc.ExecuteApply(t.Context(), ApplyRequest{
		PlanID:      "plan-1",
		Environment: "staging",
	})

	require.NoError(t, err)
	require.True(t, resp.Accepted)
	assert.Equal(t, int64(123), applyID)

	select {
	case resumedApply := <-mock.resumeCh:
		assert.Equal(t, int64(123), resumedApply.ID)
		assert.Equal(t, state.Apply.Pending, resumedApply.State)
		assert.Equal(t, "testdb", resumedApply.Database)
	case <-time.After(2 * time.Second):
		require.Fail(t, "operator did not resume queued apply after wake")
	}
}

func TestProgressResponseFromProtoPreservesVSchemaChangeType(t *testing.T) {
	resp := progressResponseFromProto(&ternv1.ProgressResponse{
		State: ternv1.State_STATE_RUNNING,
		Tables: []*ternv1.TableProgress{
			{
				TableName:  "VSchema: testapp",
				Namespace:  "testapp",
				ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA,
				Status:     state.Task.Running,
			},
		},
	})

	require.Len(t, resp.Tables, 1)
	assert.Equal(t, "vschema_update", resp.Tables[0].ChangeType)
}

func TestHandleStatusLimitAndEnvironment(t *testing.T) {
	now := time.Now().UTC()
	applies := &recentApplyStore{
		applies: []*storage.Apply{
			{
				ApplyIdentifier: "apply-one",
				ExternalID:      "external-one",
				Database:        "orders",
				Environment:     "staging",
				Engine:          storage.EngineSpirit,
				State:           state.Apply.Completed,
				Caller:          "cli",
				CreatedAt:       now,
				UpdatedAt:       now,
			},
			{
				ApplyIdentifier: "apply-two",
				Database:        "payments",
				Environment:     "staging",
				Engine:          storage.EngineSpirit,
				State:           state.Apply.Running,
				Caller:          "cli",
				CreatedAt:       now.Add(-time.Minute),
				UpdatedAt:       now.Add(-time.Minute),
			},
			{
				ApplyIdentifier: "apply-failed",
				Database:        "orders",
				Environment:     "staging",
				Engine:          storage.EngineSpirit,
				State:           state.Apply.Failed,
				Caller:          "github:alice",
				ErrorMessage:    "duplicate column name 'status'",
				CreatedAt:       now.Add(-90 * time.Second),
				UpdatedAt:       now.Add(-90 * time.Second),
			},
			{
				ApplyIdentifier: "apply-three",
				Database:        "inventory",
				Environment:     "staging",
				Engine:          storage.EngineSpirit,
				State:           state.Apply.Completed,
				Caller:          "cli",
				CreatedAt:       now.Add(-2 * time.Minute),
				UpdatedAt:       now.Add(-2 * time.Minute),
			},
		},
	}
	stor := &mockStorageWithApplyStores{applies: applies}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(stor, testServerConfig(), nil, logger)
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status?limit=2&environment=staging", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var resp apitypes.StatusResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

	require.Len(t, applies.filters, 1)
	assert.Equal(t, 3, applies.filters[0].Limit, "server should request one extra row to detect truncation")
	assert.Equal(t, "staging", applies.filters[0].Environment)
	assert.Empty(t, applies.filters[0].States)
	assert.Equal(t, 2, resp.Limit)
	assert.Equal(t, maxStatusLimit, resp.MaxLimit)
	assert.True(t, resp.HasMore)
	assert.False(t, resp.FailuresOnly)
	assert.Equal(t, 1, resp.ActiveCount)
	require.Len(t, resp.Applies, 2)
	assert.Equal(t, "apply-one", resp.Applies[0].ApplyID)
	assert.Equal(t, "external-one", resp.Applies[0].ExternalID)
	assert.Equal(t, "apply-two", resp.Applies[1].ApplyID)
}

func TestHandleStatusFailedFilter(t *testing.T) {
	now := time.Now().UTC()
	applies := &recentApplyStore{
		applies: []*storage.Apply{
			{
				ApplyIdentifier: "apply-completed",
				Database:        "orders",
				Environment:     "staging",
				Engine:          storage.EngineSpirit,
				State:           state.Apply.Completed,
				Caller:          "cli",
				CreatedAt:       now,
				UpdatedAt:       now,
			},
			{
				ApplyIdentifier: "apply-failed",
				ExternalID:      "external-failed",
				Database:        "payments",
				Environment:     "staging",
				Engine:          storage.EngineSpirit,
				State:           state.Apply.Failed,
				Caller:          "github:alice",
				ErrorMessage:    "duplicate column name 'status'",
				CreatedAt:       now.Add(-time.Minute),
				UpdatedAt:       now.Add(-time.Minute),
			},
		},
	}
	stor := &mockStorageWithApplyStores{applies: applies}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(stor, testServerConfig(), nil, logger)
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status?limit=2&environment=staging&failed=true", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var resp apitypes.StatusResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

	require.Len(t, applies.filters, 1)
	assert.Equal(t, 3, applies.filters[0].Limit)
	assert.Equal(t, "staging", applies.filters[0].Environment)
	assert.Equal(t, []string{state.Apply.Failed, state.Apply.FailedRetryable}, applies.filters[0].States)
	assert.True(t, resp.FailuresOnly)
	assert.Equal(t, 0, resp.ActiveCount)
	require.Len(t, resp.Applies, 1)
	assert.Equal(t, "apply-failed", resp.Applies[0].ApplyID)
	assert.Equal(t, "external-failed", resp.Applies[0].ExternalID)
	assert.Equal(t, "duplicate column name 'status'", resp.Applies[0].ErrorMessage)
}

func TestParseStatusLimit(t *testing.T) {
	for _, tt := range []struct {
		name      string
		target    string
		want      int
		wantError bool
	}{
		{name: "default", target: "/api/status", want: defaultStatusLimit},
		{name: "custom", target: "/api/status?limit=50", want: 50},
		{name: "clamped", target: "/api/status?limit=5000", want: maxStatusLimit},
		{name: "zero", target: "/api/status?limit=0", wantError: true},
		{name: "not a number", target: "/api/status?limit=lots", wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, tt.target, nil)
			got, err := parseStatusLimit(req)
			if tt.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseStatusFailuresOnly(t *testing.T) {
	for _, tt := range []struct {
		name      string
		target    string
		want      bool
		wantError bool
	}{
		{name: "default", target: "/api/status"},
		{name: "true", target: "/api/status?failed=true", want: true},
		{name: "false", target: "/api/status?failed=false"},
		{name: "invalid", target: "/api/status?failed=maybe", wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, tt.target, nil)
			got, err := parseStatusFailuresOnly(req)
			if tt.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHealth(t *testing.T) {
	t.Run("healthy", func(t *testing.T) {
		svc := newTestService()
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		req := httptest.NewRequestWithContext(t.Context(), "GET", "/health", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var resp map[string]string
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err, "failed to decode response")
		assert.Equal(t, "ok", resp["status"])
	})

	t.Run("unhealthy", func(t *testing.T) {
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		svc := New(&mockStorage{pingErr: errors.New("connection refused")}, testServerConfig(), nil, logger)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		req := httptest.NewRequestWithContext(t.Context(), "GET", "/health", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusServiceUnavailable, w.Code)

		var resp map[string]string
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err, "failed to decode response")
		assert.NotEmpty(t, resp["error"], "expected error message")
	})
}

func TestServiceClose(t *testing.T) {
	svc := newTestService()
	assert.NoError(t, svc.Close())
}

func TestApplyHandler(t *testing.T) {
	t.Run("returns conflict when an active apply already exists", func(t *testing.T) {
		plan := &storage.Plan{
			ID:             42,
			PlanIdentifier: "plan-active",
			Database:       "testdb",
			DatabaseType:   storage.DatabaseTypeMySQL,
			Deployment:     DefaultDeployment,
			Target:         "testdb",
			Environment:    "staging",
		}
		stor := &mockStorageWithApplyStores{
			plans:     &mockPlanLookupStore{plan: plan},
			applies:   &capturingApplyStore{err: fmt.Errorf("create apply: %w", storage.ErrActiveApplyExists)},
			tasks:     &capturingTaskStore{},
			locks:     &emptyLockStore{},
			applyLogs: &noopApplyLogStore{},
		}
		mock := &mockTernClient{}
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		ternClients := map[string]tern.Client{"default/staging": mock}
		svc := New(stor, testServerConfig(), ternClients, logger)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"plan_id": "plan-active", "environment": "staging"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/apply", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusConflict, w.Code, w.Body.String())
		assert.Nil(t, mock.applyReq, "request path should not dispatch local apply work")

		var resp apitypes.ErrorResponse
		require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
		assert.Equal(t, apitypes.ErrCodeActiveApplyExists, resp.ErrorCode)
		assert.Contains(t, resp.Error, storage.ErrActiveApplyExists.Error())
	})

	t.Run("allows direct stored plans without source metadata", func(t *testing.T) {
		plan := &storage.Plan{
			ID:             42,
			PlanIdentifier: "plan-old-direct",
			Database:       "payments",
			DatabaseType:   storage.DatabaseTypeMySQL,
			Deployment:     DefaultDeployment,
			Target:         "payments-staging-target",
			Repository:     "octocat/hello-world",
			PullRequest:    1,
			Environment:    "staging",
			Namespaces: map[string]*storage.NamespacePlanData{
				"payments": {
					Tables: []storage.TableChange{
						{
							Namespace: "payments",
							Table:     "users",
							DDL:       "ALTER TABLE users ADD COLUMN email varchar(255)",
							Operation: "alter",
						},
					},
				},
			},
		}
		cfg := &ServerConfig{
			Databases: map[string]DatabaseConfig{
				"payments": {
					Type: storage.DatabaseTypeMySQL,
					Environments: map[string]EnvironmentConfig{
						"staging": {Target: "payments-staging-target", Deployment: DefaultDeployment},
					},
					AllowedRepos: []string{"octocat/hello-world"},
				},
			},
			TernDeployments: TernConfig{
				DefaultDeployment: {"staging": "localhost:9090"},
			},
		}
		applies := &capturingApplyStore{}
		tasks := &capturingTaskStore{}
		applies.taskStore = tasks
		stor := &mockStorageWithApplyStores{
			plans:     &mockPlanLookupStore{plan: plan},
			applies:   applies,
			tasks:     tasks,
			locks:     &emptyLockStore{},
			applyLogs: &noopApplyLogStore{},
		}
		mock := &mockTernClient{isRemote: true}
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		ternClients := map[string]tern.Client{DefaultDeployment + "/staging": mock}
		svc := New(stor, cfg, ternClients, logger)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"plan_id": "plan-old-direct", "environment": "staging"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/apply", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
		assert.Nil(t, mock.applyReq, "direct HTTP apply should queue work without dispatching remote Tern")

		var resp apitypes.ApplyResponse
		require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
		assert.True(t, resp.Accepted)
		assert.NotEmpty(t, resp.ApplyID)
		require.NotNil(t, applies.apply)
		assert.Equal(t, state.Apply.Pending, applies.apply.State)
		assert.Equal(t, "payments-staging-target", applies.apply.GetOptions().Target)
		require.Len(t, tasks.tasks, 1)
		assert.Equal(t, state.Task.Pending, tasks.tasks[0].State)
	})
}

func TestTernHealth(t *testing.T) {
	t.Run("healthy", func(t *testing.T) {
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		ternClients := map[string]tern.Client{
			"default/staging": &mockTernClient{},
		}
		svc := New(&mockStorage{}, testServerConfig(), ternClients, logger)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		req := httptest.NewRequestWithContext(t.Context(), "GET", "/tern-health/default/staging", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var resp map[string]string
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err, "failed to decode response")
		assert.Equal(t, "ok", resp["status"])
		assert.Equal(t, "default", resp["deployment"])
		assert.Equal(t, "staging", resp["environment"])
	})

	t.Run("unhealthy", func(t *testing.T) {
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		ternClients := map[string]tern.Client{
			"default/staging": &mockTernClient{healthErr: errors.New("connection refused")},
		}
		svc := New(&mockStorage{}, testServerConfig(), ternClients, logger)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		req := httptest.NewRequestWithContext(t.Context(), "GET", "/tern-health/default/staging", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusServiceUnavailable, w.Code)

		var resp map[string]string
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err, "failed to decode response")
		assert.NotEmpty(t, resp["error"], "expected error message")
	})

	t.Run("unknown deployment", func(t *testing.T) {
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		ternClients := map[string]tern.Client{
			"default/staging": &mockTernClient{},
		}
		svc := New(&mockStorage{}, testServerConfig(), ternClients, logger)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		req := httptest.NewRequestWithContext(t.Context(), "GET", "/tern-health/unknown/staging", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)

		var resp map[string]string
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err, "failed to decode response")
		assert.NotEmpty(t, resp["error"], "expected error message")
	})

	t.Run("unknown environment", func(t *testing.T) {
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		ternClients := map[string]tern.Client{
			"default/staging": &mockTernClient{},
		}
		svc := New(&mockStorage{}, testServerConfig(), ternClients, logger)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		req := httptest.NewRequestWithContext(t.Context(), "GET", "/tern-health/default/production", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)
	})
}

func TestVolumeHandler(t *testing.T) {
	t.Run("success returns volume values", func(t *testing.T) {
		mock := &mockTernClient{
			volumeResp: &ternv1.VolumeResponse{
				Accepted:       true,
				PreviousVolume: 3,
				NewVolume:      11,
			},
		}
		svc := newControlTestService(mock, activeTestApply("apply-vol123"))
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "apply_id": "apply-vol123", "volume": 11}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/volume", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())

		var resp map[string]any
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err, "failed to decode response")

		// Verify the response includes volume values (not zeros)
		assert.Equal(t, true, resp["accepted"])
		prevVol, _ := resp["previous_volume"].(float64) // JSON numbers are float64
		newVol, _ := resp["new_volume"].(float64)
		assert.Equal(t, float64(3), prevVol)
		assert.Equal(t, float64(11), newVol)

		require.NotNil(t, mock.volumeReq, "expected volume request to be captured")
		assert.Equal(t, "apply-vol123", mock.volumeReq.ApplyId)
		assert.Equal(t, "staging", mock.volumeReq.Environment)
	})

	t.Run("invalid volume range", func(t *testing.T) {
		svc := newControlTestService(&mockTernClient{}, activeTestApply("apply-vol123"))
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "apply_id": "apply-vol123", "volume": 0}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/volume", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)

		var resp map[string]string
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err, "failed to decode response")
		assert.NotEmpty(t, resp["error"], "expected error message for invalid volume")
	})

	t.Run("missing apply_id", func(t *testing.T) {
		svc := newTestService()
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "volume": 5}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/volume", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "apply_id is required")
	})
}

func TestControlHandlersRejectClientDeployment(t *testing.T) {
	tests := []struct {
		name string
		path string
		body string
	}{
		{
			name: "stop",
			path: "/api/stop",
			body: `{"environment": "staging", "apply_id": "apply-123", "deployment": "default"}`,
		},
		{
			name: "start",
			path: "/api/start",
			body: `{"environment": "staging", "apply_id": "apply-123", "deployment": "default"}`,
		},
		{
			name: "cutover",
			path: "/api/cutover",
			body: `{"environment": "staging", "apply_id": "apply-123", "deployment": "default"}`,
		},
		{
			name: "volume",
			path: "/api/volume",
			body: `{"environment": "staging", "apply_id": "apply-123", "deployment": "default", "volume": 5}`,
		},
		{
			name: "revert",
			path: "/api/revert",
			body: `{"environment": "staging", "apply_id": "apply-123", "deployment": "default"}`,
		},
		{
			name: "skip-revert",
			path: "/api/skip-revert",
			body: `{"environment": "staging", "apply_id": "apply-123", "deployment": "default"}`,
		},
		{
			name: "rollback plan",
			path: "/api/rollback/plan",
			body: `{"apply_id": "apply-123", "deployment": "default"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := newTestService()
			mux := http.NewServeMux()
			svc.ConfigureRoutes(mux)

			req := httptest.NewRequestWithContext(t.Context(), "POST", tt.path, strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)

			assert.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			assert.Contains(t, w.Body.String(), "unknown field")
			assert.Contains(t, w.Body.String(), "deployment")
		})
	}
}

func TestControlHandlersRejectClientDatabase(t *testing.T) {
	tests := []struct {
		name string
		path string
		body string
	}{
		{
			name: "stop",
			path: "/api/stop",
			body: `{"database": "testdb", "environment": "staging", "apply_id": "apply-123"}`,
		},
		{
			name: "start",
			path: "/api/start",
			body: `{"database": "testdb", "environment": "staging", "apply_id": "apply-123"}`,
		},
		{
			name: "cutover",
			path: "/api/cutover",
			body: `{"database": "testdb", "environment": "staging", "apply_id": "apply-123"}`,
		},
		{
			name: "volume",
			path: "/api/volume",
			body: `{"database": "testdb", "environment": "staging", "apply_id": "apply-123", "volume": 5}`,
		},
		{
			name: "revert",
			path: "/api/revert",
			body: `{"database": "testdb", "environment": "staging", "apply_id": "apply-123"}`,
		},
		{
			name: "skip-revert",
			path: "/api/skip-revert",
			body: `{"database": "testdb", "environment": "staging", "apply_id": "apply-123"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := newTestService()
			mux := http.NewServeMux()
			svc.ConfigureRoutes(mux)

			req := httptest.NewRequestWithContext(t.Context(), "POST", tt.path, strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)

			assert.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			assert.Contains(t, w.Body.String(), "unknown field")
			assert.Contains(t, w.Body.String(), "database")
		})
	}
}

func TestControlHandlerRejectsApplyEnvironmentMismatch(t *testing.T) {
	svc := newControlTestService(&mockTernClient{}, activeTestApply("apply-abc123"))
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	body := `{"environment": "production", "apply_id": "apply-abc123"}`
	req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/stop", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	var resp apitypes.ErrorResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err, "failed to decode response")
	assert.Contains(t, resp.Error, `belongs to environment "staging"`)
	assert.Contains(t, resp.Error, `not "production"`)
}

func TestStopHandler(t *testing.T) {
	t.Run("queues stop request for apply owner", func(t *testing.T) {
		mock := &mockTernClient{stopResp: &ternv1.StopResponse{Accepted: true, StoppedCount: 1, SkippedCount: 1}}
		apply := activeTestApply("apply-abc123")
		tasks := []*storage.Task{
			{
				ID:             20,
				TaskIdentifier: "task-stop-abc123",
				ApplyID:        apply.ID,
				State:          state.Task.Running,
			},
			{
				ID:             21,
				TaskIdentifier: "task-stop-completed",
				ApplyID:        apply.ID,
				State:          state.Task.Completed,
			},
		}
		svc := newControlTestServiceWithTasks(mock, apply, tasks)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "apply_id": "apply-abc123"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/stop", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())

		require.NotNil(t, mock.stopReq, "handler should persist durable intent and issue immediate stop")
		assert.Equal(t, "apply-abc123", mock.stopReq.ApplyId)
		assert.Equal(t, "staging", mock.stopReq.Environment)
		controlReq, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
		require.NoError(t, err)
		require.NotNil(t, controlReq)

		var resp apitypes.StopResponse
		err = json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err, "failed to decode response")
		assert.True(t, resp.Accepted)
		assert.Equal(t, int64(1), resp.StoppedCount)
		assert.Equal(t, int64(1), resp.SkippedCount)
		assert.Empty(t, resp.Status)
	})

	t.Run("completes durable stop request when immediate local stop stores stopped state", func(t *testing.T) {
		apply := activeTestApply("apply-local-stop-completes")
		mock := &mockTernClient{
			stopResp: &ternv1.StopResponse{Accepted: true, StoppedCount: 1},
			stopHook: func() {
				apply.State = state.Apply.Stopped
			},
		}
		task := &storage.Task{
			ID:             23,
			TaskIdentifier: "task-local-stop-completes",
			ApplyID:        apply.ID,
			State:          state.Task.Running,
		}
		svc := newControlTestServiceWithTasks(mock, apply, []*storage.Task{task})
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "apply_id": "apply-local-stop-completes", "caller": "cli:local"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/stop", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
		require.NotNil(t, mock.stopReq)
		controlReq, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
		require.NoError(t, err)
		assert.Nil(t, controlReq)

		var resp apitypes.StopResponse
		err = json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err)
		assert.True(t, resp.Accepted)
		assert.Equal(t, int64(1), resp.StoppedCount)
	})

	t.Run("remote stop queues durable request without immediate remote stop", func(t *testing.T) {
		mock := &mockTernClient{isRemote: true, stopResp: &ternv1.StopResponse{Accepted: true}}
		apply := activeTestApply("apply-remote-stop")
		apply.ExternalID = "remote-apply-stop"
		task := &storage.Task{
			ID:             23,
			TaskIdentifier: "task-remote-stop",
			ApplyID:        apply.ID,
			State:          state.Task.Running,
		}
		svc := newControlTestServiceWithTasks(mock, apply, []*storage.Task{task})
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "apply_id": "apply-remote-stop", "caller": "cli:remote"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/stop", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
		assert.Nil(t, mock.stopReq, "remote stop must be reconciled by the operator owner")
		controlReq, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
		require.NoError(t, err)
		require.NotNil(t, controlReq)

		var resp apitypes.StopResponse
		err = json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err)
		assert.True(t, resp.Accepted)
		assert.Equal(t, int64(1), resp.StoppedCount)
	})

	t.Run("returns already requested for duplicate queued stop", func(t *testing.T) {
		mock := &mockTernClient{stopResp: &ternv1.StopResponse{Accepted: true, StoppedCount: 1}}
		apply := activeTestApply("apply-stop-already-requested")
		task := &storage.Task{
			ID:             22,
			TaskIdentifier: "task-stop-already-requested",
			ApplyID:        apply.ID,
			State:          state.Task.Running,
		}
		svc := newControlTestServiceWithTasks(mock, apply, []*storage.Task{task})
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		logs := &capturingApplyLogStore{}
		require.IsType(t, &mockStorageWithApplyStores{}, svc.storage)
		svc.storage.(*mockStorageWithApplyStores).applyLogs = logs

		body := `{"environment": "staging", "apply_id": "apply-stop-already-requested", "caller": "cli:first"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/stop", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())

		retryBody := `{"environment": "staging", "apply_id": "apply-stop-already-requested", "caller": "cli:second"}`
		retryReq := httptest.NewRequestWithContext(t.Context(), "POST", "/api/stop", strings.NewReader(retryBody))
		retryReq.Header.Set("Content-Type", "application/json")
		retryW := httptest.NewRecorder()
		mux.ServeHTTP(retryW, retryReq)

		assert.Equal(t, http.StatusAccepted, retryW.Code, retryW.Body.String())
		require.NotNil(t, mock.stopReq)
		assert.Equal(t, "apply-stop-already-requested", mock.stopReq.ApplyId)
		var resp apitypes.StopResponse
		err := json.NewDecoder(retryW.Body).Decode(&resp)
		require.NoError(t, err)
		assert.True(t, resp.Accepted)
		assert.Equal(t, int64(1), resp.StoppedCount)
		assert.Equal(t, "already_requested", resp.Status)
		assert.True(t, hasApplyLogMessageContaining(logs.logs, "Stop requested by user (caller: cli:first)"))
		assert.True(t, hasApplyLogMessageContaining(logs.logs, "Stop requested by user while stop request already pending (caller: cli:second)"))
	})

	t.Run("requires apply_id", func(t *testing.T) {
		mock := &mockTernClient{}
		svc := newControlTestService(mock, activeTestApply("apply-active-stop"))
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/stop", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
		assert.Contains(t, w.Body.String(), "apply_id is required")
		assert.Nil(t, mock.stopReq)
	})

	t.Run("missing environment", func(t *testing.T) {
		svc := newTestService()
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"apply_id": "apply-abc123"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/stop", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "environment is required")
	})
}

func TestStartHandler(t *testing.T) {
	t.Run("passes apply_id to tern client for deferred deploy", func(t *testing.T) {
		mock := &mockTernClient{
			startResp: &ternv1.StartResponse{
				Accepted:     true,
				StartedCount: 3,
			},
		}
		apply := activeTestApply("apply-xyz789")
		apply.State = state.Apply.WaitingForDeploy
		svc := newControlTestService(mock, apply)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "apply_id": "apply-xyz789"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/start", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())

		require.NotNil(t, mock.startReq, "expected start request to be captured")
		assert.Equal(t, "apply-xyz789", mock.startReq.ApplyId)
		assert.Equal(t, "staging", mock.startReq.Environment)
	})

	t.Run("rejects start while stop request is pending", func(t *testing.T) {
		mock := &mockTernClient{startResp: &ternv1.StartResponse{Accepted: true}}
		apply := activeTestApply("apply-start-stop-pending")
		apply.State = state.Apply.WaitingForDeploy
		svc := newControlTestService(mock, apply)
		_, alreadyPending, err := svc.storage.ControlRequests().RequestPending(t.Context(), &storage.ApplyControlRequest{
			ApplyID:     apply.ID,
			Operation:   storage.ControlOperationStop,
			Status:      storage.ControlRequestPending,
			RequestedBy: "cli:stopper",
		})
		require.NoError(t, err)
		require.False(t, alreadyPending)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "apply_id": "apply-start-stop-pending", "caller": "cli:starter"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/start", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusConflict, w.Code, w.Body.String())
		assert.Nil(t, mock.startReq)
		assert.Contains(t, w.Body.String(), "pending stop request")
	})

	t.Run("queues stopped apply for operator by apply_id", func(t *testing.T) {
		mock := &mockTernClient{}
		apply := stoppedTestApply("apply-xyz789")
		task := &storage.Task{
			ID:             10,
			TaskIdentifier: "task-start-xyz789",
			ApplyID:        apply.ID,
			State:          state.Task.Stopped,
		}
		svc := newControlTestServiceWithTasks(mock, apply, []*storage.Task{task})
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "apply_id": "apply-xyz789"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/start", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())

		assert.Nil(t, mock.startReq, "request path should queue operator work without calling Tern start")
		assert.Equal(t, state.Apply.Stopped, apply.State)
		controlReq, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationStart)
		require.NoError(t, err)
		require.NotNil(t, controlReq)
		assert.Equal(t, state.Task.Stopped, task.State)

		var resp apitypes.StartResponse
		err = json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err, "failed to decode response")
		assert.True(t, resp.Accepted, "expected accepted=true")
		assert.Equal(t, int64(1), resp.StartedCount)
	})

	t.Run("returns already requested for duplicate queued start", func(t *testing.T) {
		mock := &mockTernClient{}
		apply := stoppedTestApply("apply-start-already-requested")
		task := &storage.Task{
			ID:             12,
			TaskIdentifier: "task-start-already-requested",
			ApplyID:        apply.ID,
			State:          state.Task.Stopped,
		}
		completedTask := &storage.Task{
			ID:             13,
			TaskIdentifier: "task-start-already-complete",
			ApplyID:        apply.ID,
			State:          state.Task.Completed,
		}
		svc := newControlTestServiceWithTasks(mock, apply, []*storage.Task{task, completedTask})
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "apply_id": "apply-start-already-requested"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/start", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		var firstResp apitypes.StartResponse
		err := json.NewDecoder(w.Body).Decode(&firstResp)
		require.NoError(t, err)
		assert.True(t, firstResp.Accepted)
		assert.Equal(t, int64(1), firstResp.StartedCount)
		assert.Equal(t, int64(1), firstResp.SkippedCount)

		retryReq := httptest.NewRequestWithContext(t.Context(), "POST", "/api/start", strings.NewReader(body))
		retryReq.Header.Set("Content-Type", "application/json")
		retryW := httptest.NewRecorder()
		mux.ServeHTTP(retryW, retryReq)

		assert.Equal(t, http.StatusAccepted, retryW.Code, retryW.Body.String())
		var resp apitypes.StartResponse
		err = json.NewDecoder(retryW.Body).Decode(&resp)
		require.NoError(t, err)
		assert.True(t, resp.Accepted)
		assert.Equal(t, "already_requested", resp.Status)
		assert.Equal(t, int64(1), resp.StartedCount)
		assert.Equal(t, int64(1), resp.SkippedCount)

		apply.State = state.Apply.Running
		task.State = state.Task.Pending
		claimedRetryReq := httptest.NewRequestWithContext(t.Context(), "POST", "/api/start", strings.NewReader(body))
		claimedRetryReq.Header.Set("Content-Type", "application/json")
		claimedRetryW := httptest.NewRecorder()
		mux.ServeHTTP(claimedRetryW, claimedRetryReq)

		assert.Equal(t, http.StatusAccepted, claimedRetryW.Code, claimedRetryW.Body.String())
		err = json.NewDecoder(claimedRetryW.Body).Decode(&resp)
		require.NoError(t, err)
		assert.True(t, resp.Accepted)
		assert.Equal(t, "already_requested", resp.Status)
		assert.Equal(t, state.Apply.Running, apply.State, "duplicate start must not rewind an operator-claimed apply")
	})

	t.Run("queues stopped tasks when stored apply row is still running", func(t *testing.T) {
		mock := &mockTernClient{}
		apply := activeTestApply("apply-running-stoplag")
		task := &storage.Task{
			ID:             11,
			TaskIdentifier: "task-running-stoplag",
			ApplyID:        apply.ID,
			State:          state.Task.Stopped,
		}
		svc := newControlTestServiceWithTasks(mock, apply, []*storage.Task{task})
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "apply_id": "apply-running-stoplag"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/start", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())

		assert.Nil(t, mock.startReq, "request path should queue operator work without calling Tern start")
		assert.Equal(t, state.Apply.Stopped, apply.State)
		controlReq, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationStart)
		require.NoError(t, err)
		require.NotNil(t, controlReq)
		assert.Equal(t, state.Task.Stopped, task.State)

		var resp apitypes.StartResponse
		err = json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err, "failed to decode response")
		assert.True(t, resp.Accepted, "expected accepted=true")
		assert.Equal(t, int64(1), resp.StartedCount)
	})

	t.Run("queues remote stopped apply when stored apply row is still running", func(t *testing.T) {
		mock := &mockTernClient{
			isRemote: true,
			progressResp: &ternv1.ProgressResponse{
				State: ternv1.State_STATE_STOPPED,
				Tables: []*ternv1.TableProgress{{
					TableName: "users",
					Status:    state.Task.Stopped,
				}},
			},
		}
		apply := activeTestApply("apply-remote-stoplag")
		apply.ExternalID = "remote-apply-stoplag"
		svc := newControlTestServiceWithTasks(mock, apply, nil)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "apply_id": "apply-remote-stoplag"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/start", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())

		assert.Nil(t, mock.startReq, "request path should queue operator work without calling Tern start")
		require.NotNil(t, mock.progressReq)
		assert.Equal(t, "remote-apply-stoplag", mock.progressReq.ApplyId)
		assert.Equal(t, state.Apply.Stopped, apply.State)
		controlReq, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationStart)
		require.NoError(t, err)
		require.NotNil(t, controlReq)

		var resp apitypes.StartResponse
		err = json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err, "failed to decode response")
		assert.True(t, resp.Accepted, "expected accepted=true")
		assert.Equal(t, int64(1), resp.StartedCount)
	})

	t.Run("queues remote stopped apply after completing resolved pending stop", func(t *testing.T) {
		mock := &mockTernClient{
			isRemote: true,
			progressResp: &ternv1.ProgressResponse{
				State: ternv1.State_STATE_STOPPED,
				Tables: []*ternv1.TableProgress{{
					TableName: "users",
					Status:    state.Task.Stopped,
				}},
			},
		}
		apply := activeTestApply("apply-remote-stop-pending-resolved")
		apply.ExternalID = "remote-apply-stop-pending-resolved"
		svc := newControlTestServiceWithTasks(mock, apply, nil)
		_, alreadyPending, err := svc.storage.ControlRequests().RequestPending(t.Context(), &storage.ApplyControlRequest{
			ApplyID:     apply.ID,
			Operation:   storage.ControlOperationStop,
			Status:      storage.ControlRequestPending,
			RequestedBy: "cli:stopper",
		})
		require.NoError(t, err)
		require.False(t, alreadyPending)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "apply_id": "apply-remote-stop-pending-resolved", "caller": "cli:starter"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/start", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())

		require.NotNil(t, mock.progressReq)
		assert.Equal(t, "remote-apply-stop-pending-resolved", mock.progressReq.ApplyId)
		assert.Equal(t, state.Apply.Stopped, apply.State)
		pendingStop, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
		require.NoError(t, err)
		assert.Nil(t, pendingStop)
		pendingStart, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationStart)
		require.NoError(t, err)
		require.NotNil(t, pendingStart)

		var resp apitypes.StartResponse
		err = json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err, "failed to decode response")
		assert.True(t, resp.Accepted, "expected accepted=true")
		assert.Equal(t, int64(1), resp.StartedCount)
	})

	t.Run("returns already requested for remote duplicate after operator claim", func(t *testing.T) {
		mock := &mockTernClient{
			isRemote: true,
			progressResp: &ternv1.ProgressResponse{
				State: ternv1.State_STATE_STOPPED,
				Tables: []*ternv1.TableProgress{{
					TableName: "users",
					Status:    state.Task.Stopped,
				}},
			},
		}
		apply := activeTestApply("apply-remote-already-requested")
		apply.ExternalID = "remote-apply-already-requested"
		svc := newControlTestServiceWithTasks(mock, apply, nil)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "apply_id": "apply-remote-already-requested"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/start", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())

		apply.State = state.Apply.Running
		mock.progressReq = nil
		mock.progressResp = &ternv1.ProgressResponse{State: ternv1.State_STATE_RUNNING}
		retryReq := httptest.NewRequestWithContext(t.Context(), "POST", "/api/start", strings.NewReader(body))
		retryReq.Header.Set("Content-Type", "application/json")
		retryW := httptest.NewRecorder()
		mux.ServeHTTP(retryW, retryReq)

		assert.Equal(t, http.StatusAccepted, retryW.Code, retryW.Body.String())
		var resp apitypes.StartResponse
		err := json.NewDecoder(retryW.Body).Decode(&resp)
		require.NoError(t, err)
		assert.True(t, resp.Accepted)
		assert.Equal(t, "already_requested", resp.Status)
		assert.Nil(t, mock.progressReq, "duplicate start should not re-check remote state while durable request is pending")
		assert.Nil(t, mock.startReq)
		assert.Equal(t, state.Apply.Running, apply.State)
	})

	t.Run("requires apply_id", func(t *testing.T) {
		mock := &mockTernClient{}
		svc := newControlTestService(mock, activeTestApply("apply-active-start"))
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/start", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
		assert.Contains(t, w.Body.String(), "apply_id is required")
		assert.Nil(t, mock.startReq)
	})

	t.Run("deferred deploy rejects start before waiting_for_deploy", func(t *testing.T) {
		mock := &mockTernClient{}
		apply := activeTestApply("apply-defer-pending")
		apply.State = state.Apply.Pending
		apply.SetOptions(storage.ApplyOptions{DeferDeploy: true})
		svc := newControlTestServiceWithTasks(mock, apply, nil)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "apply_id": "apply-defer-pending"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/start", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusConflict, w.Code)
		assert.Contains(t, w.Body.String(), "not ready for deploy")
		assert.Nil(t, mock.startReq, "start should only reach Tern after the apply is waiting for deploy")
		assert.Equal(t, state.Apply.Pending, apply.State)
	})

	t.Run("rejects start for states without a start action", func(t *testing.T) {
		tests := []struct {
			name        string
			applyState  string
			wantMessage string
		}{
			{
				name:        "running with no stopped tasks",
				applyState:  state.Apply.Running,
				wantMessage: "still running",
			},
			{
				name:        "pending with no pending start request",
				applyState:  state.Apply.Pending,
				wantMessage: "no start request is queued",
			},
			{
				name:        "failed retryable",
				applyState:  state.Apply.FailedRetryable,
				wantMessage: "operator retry",
			},
			{
				name:        "failed",
				applyState:  state.Apply.Failed,
				wantMessage: "failed and cannot be started",
			},
			{
				name:        "cancelled",
				applyState:  state.Apply.Cancelled,
				wantMessage: "cancelled and cannot be started",
			},
			{
				name:        "completed",
				applyState:  state.Apply.Completed,
				wantMessage: "already completed",
			},
			{
				name:        "reverted",
				applyState:  state.Apply.Reverted,
				wantMessage: "reverted and cannot be started",
			},
			{
				name:        "waiting for cutover",
				applyState:  state.Apply.WaitingForCutover,
				wantMessage: "use cutover",
			},
			{
				name:        "cutting over",
				applyState:  state.Apply.CuttingOver,
				wantMessage: "cutting over",
			},
			{
				name:        "revert window",
				applyState:  state.Apply.RevertWindow,
				wantMessage: "use revert or skip-revert",
			},
			{
				name:        "preparing branch",
				applyState:  state.Apply.PreparingBranch,
				wantMessage: "setup state preparing_branch",
			},
			{
				name:        "applying branch changes",
				applyState:  state.Apply.ApplyingBranchChanges,
				wantMessage: "setup state applying_branch_changes",
			},
			{
				name:        "validating branch",
				applyState:  state.Apply.ValidatingBranch,
				wantMessage: "setup state validating_branch",
			},
			{
				name:        "creating deploy request",
				applyState:  state.Apply.CreatingDeployRequest,
				wantMessage: "setup state creating_deploy_request",
			},
			{
				name:        "validating deploy request",
				applyState:  state.Apply.ValidatingDeployRequest,
				wantMessage: "setup state validating_deploy_request",
			},
			{
				name:        "unknown future state",
				applyState:  "new_future_state",
				wantMessage: "not stopped",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				mock := &mockTernClient{}
				apply := activeTestApply("apply-start-" + strings.ReplaceAll(tt.applyState, "_", "-"))
				apply.State = tt.applyState
				svc := newControlTestServiceWithTasks(mock, apply, nil)
				mux := http.NewServeMux()
				svc.ConfigureRoutes(mux)

				body := fmt.Sprintf(`{"environment": "staging", "apply_id": %q}`, apply.ApplyIdentifier)
				req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/start", strings.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				w := httptest.NewRecorder()
				mux.ServeHTTP(w, req)

				assert.Equal(t, http.StatusConflict, w.Code, w.Body.String())
				assert.Contains(t, w.Body.String(), tt.wantMessage)
				assert.Nil(t, mock.startReq)
				assert.Nil(t, mock.progressReq)
			})
		}
	})

	t.Run("missing environment", func(t *testing.T) {
		svc := newTestService()
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"apply_id": "apply-xyz789"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/start", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "environment is required")
	})
}

func TestCutoverHandler(t *testing.T) {
	t.Run("queues cutover for operator by apply_id", func(t *testing.T) {
		mock := &mockTernClient{}
		apply := activeTestApply("apply-cut123")
		apply.State = state.Apply.WaitingForCutover
		logs := &capturingApplyLogStore{}
		svc := newControlTestService(mock, apply)
		require.IsType(t, &mockStorageWithApplyStores{}, svc.storage)
		store := svc.storage.(*mockStorageWithApplyStores)
		store.applyLogs = logs
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "apply_id": "apply-cut123", "caller": "cli:cutter"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/cutover", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
		assert.Nil(t, mock.cutoverReq, "request path should queue operator work without calling Tern cutover")
		controlReq, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationCutover)
		require.NoError(t, err)
		require.NotNil(t, controlReq)
		assert.Equal(t, "cli:cutter", controlReq.RequestedBy)
		assert.True(t, hasApplyLogMessageContaining(logs.logs, "Cutover requested by user (caller: cli:cutter)"))

		var resp apitypes.ControlResponse
		err = json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err)
		assert.True(t, resp.Accepted)
	})

	t.Run("returns accepted for duplicate queued cutover", func(t *testing.T) {
		mock := &mockTernClient{}
		apply := activeTestApply("apply-cutover-already-requested")
		apply.State = state.Apply.WaitingForCutover
		svc := newControlTestService(mock, apply)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "apply_id": "apply-cutover-already-requested", "caller": "cli:cutter"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/cutover", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())

		retryReq := httptest.NewRequestWithContext(t.Context(), "POST", "/api/cutover", strings.NewReader(body))
		retryReq.Header.Set("Content-Type", "application/json")
		retryW := httptest.NewRecorder()
		mux.ServeHTTP(retryW, retryReq)

		assert.Equal(t, http.StatusAccepted, retryW.Code, retryW.Body.String())
		assert.Nil(t, mock.cutoverReq)
		var resp apitypes.ControlResponse
		err := json.NewDecoder(retryW.Body).Decode(&resp)
		require.NoError(t, err)
		assert.True(t, resp.Accepted)
	})

	t.Run("returns accepted without queuing when cutover already in progress", func(t *testing.T) {
		mock := &mockTernClient{}
		apply := activeTestApply("apply-cutover-in-progress")
		apply.State = state.Apply.CuttingOver
		logs := &capturingApplyLogStore{}
		svc := newControlTestService(mock, apply)
		require.IsType(t, &mockStorageWithApplyStores{}, svc.storage)
		store := svc.storage.(*mockStorageWithApplyStores)
		store.applyLogs = logs
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "apply_id": "apply-cutover-in-progress", "caller": "cli:cutter"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/cutover", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
		assert.Nil(t, mock.cutoverReq)
		assert.Nil(t, mock.progressReq)
		controlReq, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationCutover)
		require.NoError(t, err)
		assert.Nil(t, controlReq)
		assert.True(t, hasApplyLogMessageContaining(logs.logs, "Cutover requested by user while cutover already in progress (caller: cli:cutter)"))

		var resp apitypes.ControlResponse
		err = json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err)
		assert.True(t, resp.Accepted)
		assert.Equal(t, apitypes.ControlStatusAlreadyInProgress, resp.Status)
	})

	t.Run("rejects cutover while apply is recovering", func(t *testing.T) {
		mock := &mockTernClient{}
		apply := activeTestApply("apply-recovering-cutover")
		apply.State = state.Apply.Recovering
		svc := newControlTestService(mock, apply)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "apply_id": "apply-recovering-cutover", "caller": "cli:cutter"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/cutover", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusConflict, w.Code, w.Body.String())
		assert.Nil(t, mock.cutoverReq)
		assert.Nil(t, mock.progressReq)
		controlReq, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationCutover)
		require.NoError(t, err)
		assert.Nil(t, controlReq)

		var resp apitypes.ErrorResponse
		err = json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err)
		assert.Contains(t, resp.Error, "recovering after restart")
	})

	t.Run("rejects cutover while remote apply is recovering", func(t *testing.T) {
		mock := &mockTernClient{
			isRemote: true,
			progressResp: &ternv1.ProgressResponse{
				State: ternv1.State_STATE_RECOVERING,
			},
		}
		apply := activeTestApply("apply-remote-recovering-cutover")
		apply.ExternalID = "remote-recovering-cutover"
		svc := newControlTestService(mock, apply)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "apply_id": "apply-remote-recovering-cutover", "caller": "cli:cutter"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/cutover", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusConflict, w.Code, w.Body.String())
		require.NotNil(t, mock.progressReq)
		assert.Equal(t, "remote-recovering-cutover", mock.progressReq.ApplyId)
		assert.Nil(t, mock.cutoverReq)
		controlReq, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationCutover)
		require.NoError(t, err)
		assert.Nil(t, controlReq)

		var resp apitypes.ErrorResponse
		err = json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err)
		assert.Contains(t, resp.Error, "recovering after restart")
	})

	t.Run("queues cutover when remote progress reaches cutting over before stored state", func(t *testing.T) {
		mock := &mockTernClient{
			isRemote: true,
			progressResp: &ternv1.ProgressResponse{
				State: ternv1.State_STATE_CUTTING_OVER,
			},
		}
		apply := activeTestApply("apply-remote-cutover-in-progress")
		apply.ExternalID = "remote-cutover-in-progress"
		logs := &capturingApplyLogStore{}
		svc := newControlTestService(mock, apply)
		require.IsType(t, &mockStorageWithApplyStores{}, svc.storage)
		store := svc.storage.(*mockStorageWithApplyStores)
		store.applyLogs = logs
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "apply_id": "apply-remote-cutover-in-progress", "caller": "cli:cutter"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/cutover", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
		require.NotNil(t, mock.progressReq)
		assert.Equal(t, "remote-cutover-in-progress", mock.progressReq.ApplyId)
		assert.Nil(t, mock.cutoverReq)
		controlReq, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationCutover)
		require.NoError(t, err)
		require.NotNil(t, controlReq)
		assert.Equal(t, "cli:cutter", controlReq.RequestedBy)
		assert.True(t, hasApplyLogMessageContaining(logs.logs, "Cutover requested by user (caller: cli:cutter)"))

		var resp apitypes.ControlResponse
		err = json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err)
		assert.True(t, resp.Accepted)
	})

	t.Run("rejects cutover while stop request is pending", func(t *testing.T) {
		mock := &mockTernClient{
			cutoverResp: &ternv1.CutoverResponse{Accepted: true},
		}
		apply := activeTestApply("apply-cutover-stop-pending")
		logs := &capturingApplyLogStore{}
		controlRequests := &memoryControlRequestStore{requests: []*storage.ApplyControlRequest{{
			ApplyID:     apply.ID,
			Operation:   storage.ControlOperationStop,
			Status:      storage.ControlRequestPending,
			RequestedBy: "cli:stopper",
		}}}
		svc := newControlTestService(mock, apply)
		require.IsType(t, &mockStorageWithApplyStores{}, svc.storage)
		store := svc.storage.(*mockStorageWithApplyStores)
		store.controls = controlRequests
		store.applyLogs = logs
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "apply_id": "apply-cutover-stop-pending", "caller": "cli:cutter"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/cutover", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusConflict, w.Code, w.Body.String())
		assert.Nil(t, mock.cutoverReq)
		assert.Contains(t, w.Body.String(), "pending stop request")
		assert.True(t, hasApplyLogMessageContaining(logs.logs, "Pending stop request blocked cutover (caller: cli:stopper)"))
	})

	t.Run("ExecuteCutover rejects while stop request is pending", func(t *testing.T) {
		mock := &mockTernClient{
			cutoverResp: &ternv1.CutoverResponse{Accepted: true},
		}
		apply := activeTestApply("apply-execute-cutover-stop-pending")
		logs := &capturingApplyLogStore{}
		controlRequests := &memoryControlRequestStore{requests: []*storage.ApplyControlRequest{{
			ApplyID:     apply.ID,
			Operation:   storage.ControlOperationStop,
			Status:      storage.ControlRequestPending,
			RequestedBy: "cli:stopper",
		}}}
		svc := newControlTestService(mock, apply)
		require.IsType(t, &mockStorageWithApplyStores{}, svc.storage)
		store := svc.storage.(*mockStorageWithApplyStores)
		store.controls = controlRequests
		store.applyLogs = logs

		resp, err := svc.ExecuteCutover(t.Context(), apitypes.ControlRequest{
			ApplyID:     "apply-execute-cutover-stop-pending",
			Environment: "staging",
			Caller:      "github:cutter@octocat/hello-world#1",
		})

		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Nil(t, mock.cutoverReq)
		assert.Contains(t, err.Error(), "pending stop request")
		assert.True(t, hasApplyLogMessageContaining(logs.logs, "Pending stop request blocked cutover (caller: cli:stopper)"))
	})

	t.Run("requires apply_id", func(t *testing.T) {
		mock := &mockTernClient{}
		svc := newControlTestService(mock, activeTestApply("apply-active-cutover"))
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/cutover", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
		assert.Contains(t, w.Body.String(), "apply_id is required")
		assert.Nil(t, mock.cutoverReq)
	})

	t.Run("missing environment", func(t *testing.T) {
		svc := newTestService()
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"apply_id": "apply-cut123"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/cutover", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "environment is required")
	})
}

func TestRevertHandler(t *testing.T) {
	t.Run("passes apply_id to tern client", func(t *testing.T) {
		mock := &mockTernClient{
			revertResp: &ternv1.RevertResponse{Accepted: true},
		}
		svc := newControlTestService(mock, activeTestApply("apply-rev123"))
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "apply_id": "apply-rev123"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/revert", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
		require.NotNil(t, mock.revertReq)
		assert.Equal(t, "apply-rev123", mock.revertReq.ApplyId)
		assert.Equal(t, "staging", mock.revertReq.Environment)
	})

	t.Run("requires apply_id", func(t *testing.T) {
		mock := &mockTernClient{}
		svc := newControlTestService(mock, activeTestApply("apply-active-revert"))
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/revert", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
		assert.Contains(t, w.Body.String(), "apply_id is required")
		assert.Nil(t, mock.revertReq)
	})
}

func TestSkipRevertHandler(t *testing.T) {
	t.Run("passes apply_id to tern client", func(t *testing.T) {
		mock := &mockTernClient{
			skipRevertResp: &ternv1.SkipRevertResponse{Accepted: true},
		}
		svc := newControlTestService(mock, activeTestApply("apply-skip456"))
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "apply_id": "apply-skip456"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/skip-revert", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
		require.NotNil(t, mock.skipRevertReq)
		assert.Equal(t, "apply-skip456", mock.skipRevertReq.ApplyId)
		assert.Equal(t, "staging", mock.skipRevertReq.Environment)
	})

	t.Run("requires apply_id", func(t *testing.T) {
		mock := &mockTernClient{}
		svc := newControlTestService(mock, activeTestApply("apply-active-skip"))
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/skip-revert", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
		assert.Contains(t, w.Body.String(), "apply_id is required")
		assert.Nil(t, mock.skipRevertReq)
	})
}

func TestDeriveErrorCode(t *testing.T) {
	tests := []struct {
		name     string
		state    string
		errMsg   string
		expected string
	}{
		{"failed with error", "failed", "Spirit: table copy failed", apitypes.ErrCodeEngineError},
		{"failed without error", "failed", "", ""},
		{"running with error", "running", "something", ""},
		{"completed", "completed", "", ""},
		{"stopped", "stopped", "", ""},
		{"proto state format", "STATE_FAILED", "engine error", apitypes.ErrCodeEngineError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, deriveErrorCode(tt.state, tt.errMsg))
		})
	}
}
