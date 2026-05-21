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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
func (m *mockStorage) ApplyComments() storage.ApplyCommentStore      { return nil }
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
}

func (m *mockStorageWithApplyStores) Plans() storage.PlanStore         { return m.plans }
func (m *mockStorageWithApplyStores) Applies() storage.ApplyStore      { return m.applies }
func (m *mockStorageWithApplyStores) Tasks() storage.TaskStore         { return m.tasks }
func (m *mockStorageWithApplyStores) Locks() storage.LockStore         { return m.locks }
func (m *mockStorageWithApplyStores) ApplyLogs() storage.ApplyLogStore { return m.applyLogs }

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
func (s *staticApplyStore) GetByDatabase(context.Context, string, string, string) ([]*storage.Apply, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.apply == nil {
		return nil, nil
	}
	return []*storage.Apply{s.apply}, nil
}

type capturingApplyStore struct {
	storage.ApplyStore
	mu        sync.Mutex
	apply     *storage.Apply
	taskStore *capturingTaskStore
	claimed   bool
	findCh    chan struct{}
	err       error
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
	return applyID, nil
}

func (s *capturingApplyStore) Update(_ context.Context, apply *storage.Apply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.apply = apply
	return nil
}

func (s *capturingApplyStore) FindNextApply(context.Context) (*storage.Apply, error) {
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
	return &apply, nil
}

func (s *capturingApplyStore) ExpireRetryable(context.Context) ([]*storage.Apply, error) {
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

// mockTernClient implements tern.Client for testing.
type mockTernClient struct {
	healthErr      error
	planResp       *ternv1.PlanResponse
	planErr        error
	planReq        *ternv1.PlanRequest
	applyResp      *ternv1.ApplyResponse
	applyErr       error
	applyReq       *ternv1.ApplyRequest
	volumeResp     *ternv1.VolumeResponse
	volumeErr      error
	volumeReq      *ternv1.VolumeRequest // captured request
	stopResp       *ternv1.StopResponse
	stopErr        error
	stopReq        *ternv1.StopRequest // captured request
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
	return nil, nil
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
	}, testServerConfig(), map[string]tern.Client{
		"default/staging": client,
	}, logger), tasks
}

func newControlTestService(client tern.Client, apply *storage.Apply) *Service {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(&mockStorageWithApplyStores{
		applies: &staticApplyStore{apply: apply},
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
		isRemote:  true,
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithPlanLookup{
		plans: &mockPlanLookupStore{plan: plan},
	}, cfg, map[string]tern.Client{
		DefaultDeployment + "/staging": mockClient,
	}, logger)

	resp, applyID, err := svc.ExecuteApply(t.Context(), ApplyRequest{
		PlanID:      "plan-old-direct",
		Environment: "staging",
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Zero(t, applyID)
	assert.False(t, resp.Accepted)
	require.NotNil(t, mockClient.applyReq, "direct API apply should still call Tern")
	assert.Equal(t, "payments-staging-target", mockClient.applyReq.Target)
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

func TestExecuteApplyRequiresRemoteApplyID(t *testing.T) {
	svc, _ := newExecuteApplyTestService(&mockTernClient{
		applyResp: &ternv1.ApplyResponse{Accepted: true},
		isRemote:  true,
	}, &capturingApplyStore{})

	resp, applyID, err := svc.ExecuteApply(t.Context(), ApplyRequest{
		PlanID:      "plan-1",
		Environment: "staging",
	})

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Zero(t, applyID)
	assert.Contains(t, err.Error(), "accepted apply missing apply_id")
}

func TestExecuteApplyQueuesLocalApplyForScheduler(t *testing.T) {
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

func TestExecuteApplyWakesSchedulerForQueuedLocalApply(t *testing.T) {
	applies := &capturingApplyStore{findCh: make(chan struct{}, 1)}
	mock := &mockTernClient{resumeCh: make(chan *storage.Apply, 1)}
	svc, _ := newExecuteApplyTestService(mock, applies)
	svc.config.SchedulerWorkers = 1
	require.NoError(t, svc.SetSchedulerPollInterval(time.Hour))
	svc.StartScheduler(t.Context())
	t.Cleanup(svc.StopScheduler)

	select {
	case <-applies.findCh:
	case <-time.After(2 * time.Second):
		require.Fail(t, "scheduler did not perform startup claim")
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
		require.Fail(t, "scheduler did not resume queued apply after wake")
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
		stor := &mockStorageWithPlanLookup{
			plans: &mockPlanLookupStore{plan: plan},
		}
		mock := &mockTernClient{
			applyResp: &ternv1.ApplyResponse{Accepted: false, ErrorMessage: "engine rejected"},
			isRemote:  true,
		}
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
		require.NotNil(t, mock.applyReq, "direct HTTP apply should still call Tern")

		var resp apitypes.ApplyResponse
		require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
		assert.False(t, resp.Accepted)
		assert.Equal(t, apitypes.ErrCodeEngineError, resp.ErrorCode)
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
		assert.Equal(t, "testdb", mock.volumeReq.Database)
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
	t.Run("passes apply_id to tern client", func(t *testing.T) {
		mock := &mockTernClient{
			stopResp: &ternv1.StopResponse{
				Accepted:     true,
				StoppedCount: 2,
			},
		}
		svc := newControlTestService(mock, activeTestApply("apply-abc123"))
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "apply_id": "apply-abc123"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/stop", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())

		require.NotNil(t, mock.stopReq, "expected stop request to be captured")
		assert.Equal(t, "apply-abc123", mock.stopReq.ApplyId)
		assert.Equal(t, "testdb", mock.stopReq.Database)
		assert.Equal(t, "staging", mock.stopReq.Environment)
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
	t.Run("passes apply_id to tern client", func(t *testing.T) {
		mock := &mockTernClient{
			startResp: &ternv1.StartResponse{
				Accepted:     true,
				StartedCount: 3,
			},
		}
		svc := newControlTestService(mock, activeTestApply("apply-xyz789"))
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
		assert.Equal(t, "testdb", mock.startReq.Database)
		assert.Equal(t, "staging", mock.startReq.Environment)
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
	t.Run("passes apply_id to tern client", func(t *testing.T) {
		mock := &mockTernClient{
			cutoverResp: &ternv1.CutoverResponse{
				Accepted: true,
			},
		}
		svc := newControlTestService(mock, activeTestApply("apply-cut123"))
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "apply_id": "apply-cut123"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/cutover", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())

		require.NotNil(t, mock.cutoverReq, "expected cutover request to be captured")
		assert.Equal(t, "apply-cut123", mock.cutoverReq.ApplyId)
		assert.Equal(t, "testdb", mock.cutoverReq.Database)
		assert.Equal(t, "staging", mock.cutoverReq.Environment)
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
		assert.Equal(t, "testdb", mock.revertReq.Database)
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
		assert.Equal(t, "testdb", mock.skipRevertReq.Database)
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
