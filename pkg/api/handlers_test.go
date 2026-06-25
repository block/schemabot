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

func (m *mockStorage) Locks() storage.LockStore                     { return nil }
func (m *mockStorage) Plans() storage.PlanStore                     { return nil }
func (m *mockStorage) Applies() storage.ApplyStore                  { return nil }
func (m *mockStorage) Tasks() storage.TaskStore                     { return nil }
func (m *mockStorage) ApplyLogs() storage.ApplyLogStore             { return nil }
func (m *mockStorage) ControlRequests() storage.ControlRequestStore { return nil }
func (m *mockStorage) ApplyComments() storage.ApplyCommentStore     { return nil }
func (m *mockStorage) ApplyOperations() storage.ApplyOperationStore { return nil }
func (m *mockStorage) Checks() storage.CheckStore                   { return nil }
func (m *mockStorage) Settings() storage.SettingsStore              { return nil }
func (m *mockStorage) Ping(ctx context.Context) error               { return m.pingErr }
func (m *mockStorage) Close() error                                 { return nil }

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
	plans      storage.PlanStore
	applies    storage.ApplyStore
	tasks      storage.TaskStore
	locks      storage.LockStore
	applyLogs  storage.ApplyLogStore
	controls   storage.ControlRequestStore
	operations storage.ApplyOperationStore
}

func (m *mockStorageWithApplyStores) Plans() storage.PlanStore         { return m.plans }
func (m *mockStorageWithApplyStores) Applies() storage.ApplyStore      { return m.applies }
func (m *mockStorageWithApplyStores) Tasks() storage.TaskStore         { return m.tasks }
func (m *mockStorageWithApplyStores) Locks() storage.LockStore         { return m.locks }
func (m *mockStorageWithApplyStores) ApplyLogs() storage.ApplyLogStore { return m.applyLogs }
func (m *mockStorageWithApplyStores) ControlRequests() storage.ControlRequestStore {
	return m.controls
}
func (m *mockStorageWithApplyStores) ApplyOperations() storage.ApplyOperationStore {
	if m.operations == nil {
		return &staticApplyOperationStore{}
	}
	return m.operations
}

type staticApplyOperationStore struct {
	storage.ApplyOperationStore
	operations      []*storage.ApplyOperation
	err             error
	resumeStateByOp map[int64]*storage.EngineResumeState
	resumeStateErr  error
}

func (s *staticApplyOperationStore) ListByApply(_ context.Context, applyID int64) ([]*storage.ApplyOperation, error) {
	if s.err != nil {
		return nil, s.err
	}
	operations := make([]*storage.ApplyOperation, 0, len(s.operations))
	for _, op := range s.operations {
		if op.ApplyID == applyID {
			operations = append(operations, op)
		}
	}
	return operations, nil
}

func (s *staticApplyOperationStore) GetEngineResumeState(_ context.Context, operationID int64) (*storage.EngineResumeState, error) {
	if s.resumeStateErr != nil {
		return nil, s.resumeStateErr
	}
	if rs, ok := s.resumeStateByOp[operationID]; ok {
		return rs, nil
	}
	return nil, storage.ErrEngineResumeStateNotFound
}

type staticPlanStore struct {
	storage.PlanStore
	plan      *storage.Plan
	plansByID map[int64]*storage.Plan
	err       error
}

func (s *staticPlanStore) Get(context.Context, string) (*storage.Plan, error) {
	return s.plan, s.err
}

func (s *staticPlanStore) GetByID(_ context.Context, id int64) (*storage.Plan, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.plansByID != nil {
		return s.plansByID[id], nil
	}
	return s.plan, nil
}

type staticApplyStore struct {
	storage.ApplyStore
	apply   *storage.Apply
	applies []*storage.Apply
	err     error
}

func (s *staticApplyStore) GetByApplyIdentifier(_ context.Context, applyIdentifier string) (*storage.Apply, error) {
	if s.err != nil {
		return nil, s.err
	}
	if len(s.applies) == 0 {
		return s.apply, nil
	}
	for _, apply := range s.applies {
		if apply.ApplyIdentifier == applyIdentifier {
			return apply, nil
		}
	}
	return nil, nil
}
func (s *staticApplyStore) Get(context.Context, int64) (*storage.Apply, error) {
	return s.apply, s.err
}
func (s *staticApplyStore) GetByDatabase(_ context.Context, database, dbType, environment string) ([]*storage.Apply, error) {
	if s.err != nil {
		return nil, s.err
	}
	if len(s.applies) > 0 {
		var applies []*storage.Apply
		for _, apply := range s.applies {
			if apply.Database != database {
				continue
			}
			if dbType != "" {
				if apply.DatabaseType != dbType {
					continue
				}
			}
			if environment != "" {
				if apply.Environment != environment {
					continue
				}
			}
			applies = append(applies, apply)
		}
		return applies, nil
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

func (s *capturingApplyStore) CreateWithGroupedOperations(ctx context.Context, apply *storage.Apply, groups []*storage.ApplyOperationWithTasks) (int64, error) {
	operations := make([]*storage.ApplyOperation, 0, len(groups))
	var tasks []*storage.Task
	for i, group := range groups {
		group.Operation.ID = int64(i + 1)
		operations = append(operations, group.Operation)
		for _, task := range group.Tasks {
			task.ApplyOperationID = &group.Operation.ID
			tasks = append(tasks, task)
		}
	}
	return s.CreateWithTasksAndOperations(ctx, apply, tasks, operations)
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

func (s *capturingTaskStore) GetByDatabase(_ context.Context, database string) ([]*storage.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var tasks []*storage.Task
	for _, task := range s.tasks {
		if task.Database == database {
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
	healthErr                error
	planResp                 *ternv1.PlanResponse
	planErr                  error
	planReq                  *ternv1.PlanRequest
	pullSchemaResp           *ternv1.PullSchemaResponse
	pullSchemaErr            error
	pullSchemaReq            *ternv1.PullSchemaRequest
	pullSchemaReqs           []*ternv1.PullSchemaRequest
	pullSchemaHook           func(*ternv1.PullSchemaRequest) (*ternv1.PullSchemaResponse, error)
	applyResp                *ternv1.ApplyResponse
	applyErr                 error
	applyReq                 *ternv1.ApplyRequest
	progressResp             *ternv1.ProgressResponse
	progressErr              error
	progressReq              *ternv1.ProgressRequest
	volumeResp               *ternv1.VolumeResponse
	volumeErr                error
	volumeReq                *ternv1.VolumeRequest // captured request
	stopResp                 *ternv1.StopResponse
	stopErr                  error
	stopReq                  *ternv1.StopRequest // captured request
	stopHook                 func()
	startResp                *ternv1.StartResponse
	startErr                 error
	startReq                 *ternv1.StartRequest // captured request
	cutoverResp              *ternv1.CutoverResponse
	cutoverErr               error
	cutoverReq               *ternv1.CutoverRequest // captured request
	revertResp               *ternv1.RevertResponse
	revertErr                error
	revertReq                *ternv1.RevertRequest // captured request
	skipRevertResp           *ternv1.SkipRevertResponse
	skipRevertErr            error
	skipRevertReq            *ternv1.SkipRevertRequest // captured request
	resumeMu                 sync.Mutex
	resumeErr                error
	resumeApply              *storage.Apply
	resumeOperationID        int64
	resumeCutoverOperationID int64
	resumeCh                 chan *storage.Apply
	observerApplyID          int64
	observer                 tern.ProgressObserver
	isRemote                 bool
}

func (m *mockTernClient) Health(ctx context.Context) error { return m.healthErr }
func (m *mockTernClient) PullSchema(ctx context.Context, req *ternv1.PullSchemaRequest) (*ternv1.PullSchemaResponse, error) {
	m.pullSchemaReq = req
	m.pullSchemaReqs = append(m.pullSchemaReqs, req)
	if m.pullSchemaHook != nil {
		return m.pullSchemaHook(req)
	}
	if m.pullSchemaResp != nil {
		return m.pullSchemaResp, m.pullSchemaErr
	}
	return nil, m.pullSchemaErr
}
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
func (m *mockTernClient) ResumeApplyOperation(ctx context.Context, apply *storage.Apply, applyOperationID int64) error {
	m.resumeMu.Lock()
	m.resumeApply = apply
	m.resumeOperationID = applyOperationID
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
func (m *mockTernClient) ResumeApplyOperationCutover(ctx context.Context, apply *storage.Apply, applyOperationID int64) error {
	m.resumeMu.Lock()
	m.resumeApply = apply
	m.resumeCutoverOperationID = applyOperationID
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
	m.observerApplyID = applyID
	m.observer = observer
}
func (m *mockTernClient) Close() error { return nil }

// testServerConfig returns a minimal valid ServerConfig for testing.
// Only includes "staging" environment - tests that need "production"
// should create their own config or add it to the mock ternClients.
func testServerConfig() *ServerConfig {
	return &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"testdb": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"staging": {Target: "testdb", Deployment: DefaultDeployment},
				},
			},
		},
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
	cfg := testServerConfig()
	cfg.Databases = map[string]DatabaseConfig{
		"testdb": {
			Type: storage.DatabaseTypeMySQL,
			Environments: map[string]EnvironmentConfig{
				"staging": {Target: "testdb", Deployment: DefaultDeployment},
			},
		},
	}
	return New(&mockStorageWithApplyStores{
		plans:     &staticPlanStore{plan: executeApplyTestPlan()},
		applies:   applies,
		tasks:     tasks,
		locks:     &emptyLockStore{},
		applyLogs: &noopApplyLogStore{},
		controls:  &memoryControlRequestStore{},
	}, cfg, map[string]tern.Client{
		"default/staging": client,
	}, logger), tasks
}

func newQueueApplyTestService(plan *storage.Plan, client tern.Client, applies storage.ApplyStore) (*Service, *capturingTaskStore) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	tasks := &capturingTaskStore{}
	if capturingApplies, ok := applies.(*capturingApplyStore); ok {
		capturingApplies.taskStore = tasks
	}
	cfg := testServerConfig()
	cfg.Databases = map[string]DatabaseConfig{
		"testdb": {
			Type: storage.DatabaseTypeMySQL,
			Environments: map[string]EnvironmentConfig{
				"staging": {Target: "testdb", Deployment: DefaultDeployment},
			},
		},
	}
	return New(&mockStorageWithApplyStores{
		plans:     &staticPlanStore{plan: plan},
		applies:   applies,
		tasks:     tasks,
		locks:     &emptyLockStore{},
		applyLogs: &noopApplyLogStore{},
		controls:  &memoryControlRequestStore{},
	}, cfg, map[string]tern.Client{
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

func newControlTestServiceWithOperations(client tern.Client, apply *storage.Apply, tasks []*storage.Task, operations []*storage.ApplyOperation) *Service {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(&mockStorageWithApplyStores{
		applies:    &staticApplyStore{apply: apply},
		tasks:      &capturingTaskStore{tasks: tasks},
		controls:   &memoryControlRequestStore{},
		operations: &staticApplyOperationStore{operations: operations},
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

func TestExecutePlanPersistsShardPlans(t *testing.T) {
	plans := &capturingPlanStore{}
	mockClient := &mockTernClient{
		planResp: &ternv1.PlanResponse{
			PlanId: "plan-shards",
			Changes: []*ternv1.SchemaChange{{
				Namespace: "commerce",
				TableChanges: []*ternv1.TableChange{{
					TableName:  "users",
					Ddl:        "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
					ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
				}},
			}},
			Shards: []*ternv1.ShardPlan{
				{Namespace: "commerce", Shard: "-80", NeedsChange: true},
				{Namespace: "commerce", Shard: "80-", NeedsChange: false},
			},
		},
	}
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"commerce": {
				Type: storage.DatabaseTypeVitess,
				Environments: map[string]EnvironmentConfig{
					"staging": {Target: "commerce-target", Deployment: "primary"},
				},
			},
		},
		TernDeployments: TernConfig{
			"primary": {"staging": "tern.example.com:80"},
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithPlanLookup{plans: plans}, cfg, map[string]tern.Client{
		"primary/staging": mockClient,
	}, logger)

	resp, err := svc.ExecutePlan(t.Context(), PlanRequest{
		Database:    "commerce",
		Environment: "staging",
		Type:        storage.DatabaseTypeVitess,
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"commerce": {Files: map[string]string{"users.sql": "CREATE TABLE `users` (`id` bigint unsigned NOT NULL)"}},
		},
		Repository: "example/app",
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, plans.created)
	assert.Equal(t, []storage.ShardPlan{
		{Namespace: "commerce", Shard: "-80", NeedsChange: true},
		{Namespace: "commerce", Shard: "80-", NeedsChange: false},
	}, plans.created.Shards)
}

func TestExecutePullSchemaRoutesConfiguredMySQLTarget(t *testing.T) {
	mockClient := &mockTernClient{
		pullSchemaResp: &ternv1.PullSchemaResponse{
			Database:    "orders",
			Type:        storage.DatabaseTypeMySQL,
			Environment: "production",
			Namespaces: map[string]*ternv1.PulledNamespace{
				"orders": {Tables: map[string]string{"users": "CREATE TABLE `users` (`id` bigint NOT NULL);\n"}},
			},
			TableCount: 1,
		},
	}
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"orders": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"production": {Target: "orders-production", Deployment: "primary"},
				},
			},
		},
		TernDeployments: TernConfig{
			"primary": {"production": "tern.example.com:80"},
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithApplyStores{
		plans:   &staticPlanStore{},
		applies: &staticApplyStore{},
	}, cfg, map[string]tern.Client{
		"primary/production": mockClient,
	}, logger)

	resp, err := svc.ExecutePullSchema(t.Context(), apitypes.PullSchemaRequest{
		Database:    "orders",
		Environment: "production",
		Type:        storage.DatabaseTypeMySQL,
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "orders", resp.Database)
	assert.Equal(t, storage.DatabaseTypeMySQL, resp.Type)
	assert.Equal(t, int32(1), resp.TableCount)
	require.NotNil(t, mockClient.pullSchemaReq)
	assert.Equal(t, "orders-production", mockClient.pullSchemaReq.Target)
	assert.Empty(t, mockClient.pullSchemaReq.Namespace)
	assert.Equal(t, "production", mockClient.pullSchemaReq.Environment)
	assert.Equal(t, storage.DatabaseTypeMySQL, mockClient.pullSchemaReq.Type)
	assert.Equal(t, "CREATE TABLE `users` (`id` bigint NOT NULL);\n", resp.Namespaces["orders"].Tables["users"])
}

func TestExecutePullSchemaRoutesConfiguredVitessTarget(t *testing.T) {
	mockClient := &mockTernClient{
		pullSchemaResp: &ternv1.PullSchemaResponse{
			Database:    "commerce",
			Type:        storage.DatabaseTypeVitess,
			Environment: "production",
			Namespaces: map[string]*ternv1.PulledNamespace{
				"commerce_sharded": {
					Tables:    map[string]string{"users": "CREATE TABLE `users` (`id` bigint NOT NULL);\n"},
					Artifacts: map[string]string{"vschema.json": "{\"sharded\":true}"},
				},
			},
			TableCount: 1,
		},
	}
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"commerce": {
				Type: storage.DatabaseTypeVitess,
				Environments: map[string]EnvironmentConfig{
					"production": {Target: "commerce-production", Deployment: "primary"},
				},
			},
		},
		TernDeployments: TernConfig{
			"primary": {"production": "tern.example.com:80"},
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithApplyStores{
		plans:   &staticPlanStore{},
		applies: &staticApplyStore{},
	}, cfg, map[string]tern.Client{
		"primary/production": mockClient,
	}, logger)

	resp, err := svc.ExecutePullSchema(t.Context(), apitypes.PullSchemaRequest{
		Database:    "commerce",
		Environment: "production",
		Type:        storage.DatabaseTypeVitess,
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "commerce", resp.Database)
	assert.Equal(t, storage.DatabaseTypeVitess, resp.Type)
	assert.Equal(t, int32(1), resp.TableCount)
	require.NotNil(t, mockClient.pullSchemaReq)
	assert.Equal(t, "commerce-production", mockClient.pullSchemaReq.Target)
	assert.Empty(t, mockClient.pullSchemaReq.Namespace)
	assert.Equal(t, "production", mockClient.pullSchemaReq.Environment)
	assert.Equal(t, storage.DatabaseTypeVitess, mockClient.pullSchemaReq.Type)
	assert.Equal(t, "CREATE TABLE `users` (`id` bigint NOT NULL);\n", resp.Namespaces["commerce_sharded"].Tables["users"])
	assert.JSONEq(t, "{\"sharded\":true}", resp.Namespaces["commerce_sharded"].Artifacts["vschema.json"])
}

func TestExecutePullSchemaPullsRequestedNamespaces(t *testing.T) {
	mockClient := &mockTernClient{
		pullSchemaHook: func(req *ternv1.PullSchemaRequest) (*ternv1.PullSchemaResponse, error) {
			return &ternv1.PullSchemaResponse{
				Database:    req.Database,
				Type:        req.Type,
				Environment: req.Environment,
				Namespaces: map[string]*ternv1.PulledNamespace{
					req.Namespace: {Tables: map[string]string{"users": "CREATE TABLE `users` (`id` bigint NOT NULL);\n"}},
				},
				TableCount: 1,
			}, nil
		},
	}
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"orders-logical": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"production": {Target: "orders-production", Deployment: "primary"},
				},
			},
		},
		TernDeployments: TernConfig{
			"primary": {"production": "tern.example.com:80"},
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithApplyStores{
		plans:   &staticPlanStore{},
		applies: &staticApplyStore{},
	}, cfg, map[string]tern.Client{
		"primary/production": mockClient,
	}, logger)

	resp, err := svc.ExecutePullSchema(t.Context(), apitypes.PullSchemaRequest{
		Database:      "orders-logical",
		Environment:   "production",
		Type:          storage.DatabaseTypeMySQL,
		Namespaces:    []string{"orders_production", "orders_audit_production"},
		CatalogDetail: "detailed",
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "orders-logical", resp.Database)
	assert.Equal(t, int32(2), resp.TableCount)
	assert.Contains(t, resp.Namespaces, "orders_production")
	assert.Contains(t, resp.Namespaces, "orders_audit_production")
	require.Len(t, mockClient.pullSchemaReqs, 2)
	assert.Equal(t, "orders-logical", mockClient.pullSchemaReqs[0].Database)
	assert.Equal(t, "orders_production", mockClient.pullSchemaReqs[0].Namespace)
	assert.Equal(t, ternv1.PullCatalogDetail_PULL_CATALOG_DETAIL_DETAILED, mockClient.pullSchemaReqs[0].CatalogDetail)
	assert.Equal(t, "orders_audit_production", mockClient.pullSchemaReqs[1].Namespace)
	assert.Equal(t, ternv1.PullCatalogDetail_PULL_CATALOG_DETAIL_DETAILED, mockClient.pullSchemaReqs[1].CatalogDetail)
}

func TestExecutePullSchemaRejectsUnsupportedType(t *testing.T) {
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"orders": {
				Type: storage.DatabaseTypeStrata,
				Environments: map[string]EnvironmentConfig{
					"production": {Target: "orders-production", Deployment: "primary"},
				},
			},
		},
		TernDeployments: TernConfig{
			"primary": {"production": "tern.example.com:80"},
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorage{}, cfg, map[string]tern.Client{
		"primary/production": &mockTernClient{},
	}, logger)

	_, err := svc.ExecutePullSchema(t.Context(), apitypes.PullSchemaRequest{
		Database:    "orders",
		Environment: "production",
		Type:        storage.DatabaseTypeStrata,
	})

	var unsupportedErr *unsupportedPullSchemaError
	require.ErrorAs(t, err, &unsupportedErr)
	assert.Equal(t, storage.DatabaseTypeStrata, unsupportedErr.DatabaseType)
}

// A multi-deployment environment pulls its live schema from the primary
// deployment (first in deployment_order); the apply itself fans out across
// every deployment.
func TestExecutePullSchemaRoutesPrimaryDeployment(t *testing.T) {
	euClient := &mockTernClient{
		pullSchemaResp: &ternv1.PullSchemaResponse{
			Database:    "orders",
			Type:        storage.DatabaseTypeMySQL,
			Environment: "production",
			Namespaces: map[string]*ternv1.PulledNamespace{
				"orders": {Tables: map[string]string{"users": "CREATE TABLE `users` (`id` bigint NOT NULL);\n"}},
			},
			TableCount: 1,
		},
	}
	usClient := &mockTernClient{}
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"orders": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"production": {
						DeploymentOrder: []string{"eu", "us"},
						Deployments: map[string]DeploymentTarget{
							"eu": {Target: "orders-eu"},
							"us": {Target: "orders-us"},
						},
					},
				},
			},
		},
		TernDeployments: TernConfig{
			"eu": {"production": "eu.example.com:80"},
			"us": {"production": "us.example.com:80"},
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorage{}, cfg, map[string]tern.Client{
		"eu/production": euClient,
		"us/production": usClient,
	}, logger)

	resp, err := svc.ExecutePullSchema(t.Context(), apitypes.PullSchemaRequest{
		Database:    "orders",
		Environment: "production",
		Type:        storage.DatabaseTypeMySQL,
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, int32(1), resp.TableCount)
	require.NotNil(t, euClient.pullSchemaReq, "primary deployment should be pulled")
	assert.Equal(t, "orders-eu", euClient.pullSchemaReq.Target)
	assert.Equal(t, storage.DatabaseTypeMySQL, euClient.pullSchemaReq.Type)
	assert.Nil(t, usClient.pullSchemaReq, "non-primary deployment must not be pulled")
}

// The /api/pull handler succeeds for a multi-deployment environment, returning
// the live schema pulled from the primary deployment.
func TestPullSchemaHandlerRoutesPrimaryDeployment(t *testing.T) {
	euClient := &mockTernClient{
		pullSchemaResp: &ternv1.PullSchemaResponse{
			Database:    "orders",
			Type:        storage.DatabaseTypeMySQL,
			Environment: "production",
			Namespaces: map[string]*ternv1.PulledNamespace{
				"orders": {Tables: map[string]string{"users": "CREATE TABLE `users` (`id` bigint NOT NULL);\n"}},
			},
			TableCount: 1,
		},
	}
	usClient := &mockTernClient{}
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"orders": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"production": {
						DeploymentOrder: []string{"eu", "us"},
						Deployments: map[string]DeploymentTarget{
							"eu": {Target: "orders-eu"},
							"us": {Target: "orders-us"},
						},
					},
				},
			},
		},
		TernDeployments: TernConfig{
			"eu": {"production": "eu.example.com:80"},
			"us": {"production": "us.example.com:80"},
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorage{}, cfg, map[string]tern.Client{
		"eu/production": euClient,
		"us/production": usClient,
	}, logger)
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/pull", strings.NewReader(`{"database":"orders","environment":"production","type":"mysql"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NotNil(t, euClient.pullSchemaReq, "primary deployment should be pulled")
	assert.Equal(t, "orders-eu", euClient.pullSchemaReq.Target)
	assert.Nil(t, usClient.pullSchemaReq, "non-primary deployment must not be pulled")
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

// trustedQueueApplyTestPlan returns a stored plan created from the trusted PR
// discovery path (repository, pull request, and schema path recorded) so
// queue-path tests can exercise source-policy behavior.
func trustedQueueApplyTestPlan() *storage.Plan {
	plan := executeApplyTestPlan()
	plan.Repository = "octocat/hello-world"
	plan.PullRequest = 1
	plan.SchemaPath = "schema/testdb"
	return plan
}

// A data-plane deployment executes applies dispatched by a control plane that
// already evaluated source policy. EnqueueAuthorizedApply queues the trusted
// plan without re-evaluating source policy, while ExecuteApply on the same
// service keeps failing closed until database routing is configured.
func TestEnqueueAuthorizedApplyQueuesTrustedPlanWithoutDatabaseConfig(t *testing.T) {
	applies := &capturingApplyStore{}
	mockClient := &mockTernClient{isRemote: true}
	svc, tasks := newQueueApplyTestService(trustedQueueApplyTestPlan(), mockClient, applies)
	svc.config.Databases = nil

	_, _, err := svc.ExecuteApply(t.Context(), ApplyRequest{
		PlanID:      "plan-1",
		Environment: "staging",
	})
	var policyErr *SourcePolicyError
	require.True(t, errors.As(err, &policyErr), "ExecuteApply must fail closed without database config")
	assert.Equal(t, SourcePolicyReasonMissingDatabaseConfig, policyErr.Reason)
	svc.config.Databases = map[string]DatabaseConfig{
		"testdb": {
			Type: storage.DatabaseTypeMySQL,
			Environments: map[string]EnvironmentConfig{
				"staging": {Target: "testdb", Deployment: DefaultDeployment},
			},
		},
	}

	resp, applyID, err := svc.EnqueueAuthorizedApply(t.Context(), ApplyRequest{
		PlanID:      "plan-1",
		Environment: "staging",
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Accepted)
	assert.Equal(t, int64(123), applyID)
	assert.Nil(t, mockClient.applyReq, "queued apply must wait for operator dispatch")
	require.NotNil(t, applies.apply)
	assert.Equal(t, state.Apply.Pending, applies.apply.State)
	assert.Equal(t, "testdb", applies.apply.GetOptions().Target)
	require.Len(t, tasks.tasks, 1)
	assert.Equal(t, state.Task.Pending, tasks.tasks[0].State)
	assert.Equal(t, "users", tasks.tasks[0].TableName)
}

// EnqueueAuthorizedApply skips only source policy. Execution invariants still reject a
// dispatch whose stored plan was created for a different environment.
func TestEnqueueAuthorizedApplyRejectsEnvironmentMismatch(t *testing.T) {
	applies := &capturingApplyStore{}
	mockClient := &mockTernClient{isRemote: true}
	svc, tasks := newQueueApplyTestService(trustedQueueApplyTestPlan(), mockClient, applies)

	resp, applyID, err := svc.EnqueueAuthorizedApply(t.Context(), ApplyRequest{
		PlanID:      "plan-1",
		Environment: "production",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), `created for environment "staging"`)
	assert.Nil(t, resp)
	assert.Zero(t, applyID)
	assert.Nil(t, applies.apply, "mismatched environment must not store an apply")
	assert.Empty(t, tasks.tasks)
}

// EnqueueAuthorizedApply enforces the same stored-plan execution invariants as gated
// applies: the plan must exist and carry server-side routing metadata.
func TestEnqueueAuthorizedApplyRejectsInvalidStoredPlan(t *testing.T) {
	missingDeployment := trustedQueueApplyTestPlan()
	missingDeployment.Deployment = ""
	missingTarget := trustedQueueApplyTestPlan()
	missingTarget.Target = ""

	tests := []struct {
		name    string
		plan    *storage.Plan
		wantErr string
	}{
		{name: "plan not found", plan: nil, wantErr: "plan not found"},
		{name: "missing deployment", plan: missingDeployment, wantErr: `missing server-side routing metadata field "deployment"`},
		{name: "missing target", plan: missingTarget, wantErr: `missing server-side routing metadata field "target"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			applies := &capturingApplyStore{}
			svc, tasks := newQueueApplyTestService(tc.plan, &mockTernClient{}, applies)

			resp, applyID, err := svc.EnqueueAuthorizedApply(t.Context(), ApplyRequest{
				PlanID:      "plan-1",
				Environment: "staging",
			})

			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
			assert.Nil(t, resp)
			assert.Zero(t, applyID)
			assert.Nil(t, applies.apply, "invalid stored plan must not store an apply")
			assert.Empty(t, tasks.tasks)
		})
	}
}

// A queued trusted dispatch follows the same durable operator path as gated
// applies: EnqueueAuthorizedApply stores the pending apply, wakes a driver, and the
// driver claims and resumes it.
func TestEnqueueAuthorizedApplyWakesOperatorForQueuedApply(t *testing.T) {
	applies := &capturingApplyStore{findCh: make(chan struct{}, 1)}
	mock := &mockTernClient{resumeCh: make(chan *storage.Apply, 1)}
	svc, _ := newQueueApplyTestService(trustedQueueApplyTestPlan(), mock, applies)
	svc.config.Drivers = 1
	// This double models the apply-level FindNextApply claim path; the
	// default operation-level claim path is covered by the operator
	// integration tests.
	svc.config.OperatorClaimOperations = new(false)
	require.NoError(t, svc.SetOperatorPollInterval(time.Hour))
	svc.StartOperator(t.Context())
	t.Cleanup(svc.StopOperator)

	select {
	case <-applies.findCh:
	case <-time.After(2 * time.Second):
		require.Fail(t, "operator did not perform startup claim")
	}

	resp, applyID, err := svc.EnqueueAuthorizedApply(t.Context(), ApplyRequest{
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

// planBodyOfSize builds a /api/plan JSON payload of exactly totalSize bytes
// by padding the schema file content, so tests can probe either side of the
// API request body limit.
func planBodyOfSize(t *testing.T, totalSize int) string {
	const envelope = `{"database":"testdb","environment":"staging","type":"mysql","schema_files":{"testdb":{"files":{"big.sql":"%s"}}}}`
	overhead := len(envelope) - len("%s")
	require.Greater(t, totalSize, overhead)
	return fmt.Sprintf(envelope, strings.Repeat("a", totalSize-overhead))
}

// A request body over the API limit is rejected with 413 and an error that
// tells the caller the limit, instead of being buffered into server memory.
func TestAPIRoutesRejectOversizedRequestBody(t *testing.T) {
	svc := newTestService()
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	body := planBodyOfSize(t, maxAPIRequestBodyBytes+1)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/plan", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
	var resp apitypes.ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Contains(t, resp.Error, fmt.Sprintf("request body exceeds the %d MiB limit", maxAPIRequestBodyBytes>>20))
	assert.Contains(t, resp.Error, "reduce the payload size")
}

// A request body just under the API limit passes the size check and reaches
// normal request handling. The request can still fail validation downstream,
// but never with the body-size error.
func TestAPIRoutesAcceptBodyUnderSizeLimit(t *testing.T) {
	svc := newTestService()
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	body := planBodyOfSize(t, maxAPIRequestBodyBytes-1)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/plan", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.NotEqual(t, http.StatusRequestEntityTooLarge, w.Code)
	assert.NotContains(t, w.Body.String(), "request body exceeds")
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

// A null namespace value (JSON `{"default": null}`) is rejected with a clear
// 400 instead of panicking the request goroutine in schema-files conversion.
func TestPlanHandlerRejectsNullSchemaFilesNamespace(t *testing.T) {
	svc := newTestService()
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	body := `{
		"database": "payments",
		"environment": "staging",
		"type": "mysql",
		"schema_files": {"default": null}
	}`
	req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/plan", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	var resp apitypes.ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Contains(t, resp.Error, `schema_files["default"] is null`)
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
	// Apply create writes one apply_operations row mirroring the apply's
	// (deployment, target) and links the queued tasks to it.
	require.Len(t, applies.operations, 1)
	assert.Equal(t, DefaultDeployment, applies.operations[0].Deployment)
	assert.Equal(t, "testdb", applies.operations[0].Target)
	assert.Equal(t, state.ApplyOperation.Pending, applies.operations[0].State)
	require.NotNil(t, tasks.tasks[0].ApplyOperationID)
	assert.Equal(t, applies.operations[0].ID, *tasks.tasks[0].ApplyOperationID)
}

func TestCreateStoredApplyFansOutOperationsForResolvedTargets(t *testing.T) {
	// Multi-target apply creation creates an independent operation and task set
	// for each resolved deployment while preserving the first deployment on the parent apply.
	applies := &capturingApplyStore{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	tasks := &capturingTaskStore{}
	applies.taskStore = tasks
	cfg := testServerConfig()
	cfg.Databases = map[string]DatabaseConfig{}
	cfg.Databases["testdb"] = DatabaseConfig{
		Type: storage.DatabaseTypeMySQL,
		Environments: map[string]EnvironmentConfig{
			"staging": {
				Deployments: map[string]DeploymentTarget{
					"default-a": {Target: "testdb-a"},
					"default-b": {Target: "testdb-b"},
				},
				DeploymentOrder: []string{"default-a", "default-b"},
				CutoverPolicy:   storage.CutoverPolicyBarrier,
				OnFailure:       storage.OnFailureContinue,
			},
		},
	}
	svc := New(&mockStorageWithApplyStores{
		plans:     &staticPlanStore{plan: executeApplyTestPlan()},
		applies:   applies,
		tasks:     tasks,
		locks:     &emptyLockStore{},
		applyLogs: &noopApplyLogStore{},
		controls:  &memoryControlRequestStore{},
	}, cfg, map[string]tern.Client{}, logger)

	apply, storedApplyID, err := svc.createStoredApply(t.Context(), executeApplyTestPlan(), ApplyRequest{Environment: "staging"}, nil, "apply-fanout", false)

	require.NoError(t, err)
	assert.Equal(t, int64(123), storedApplyID)
	require.NotNil(t, apply)
	assert.Equal(t, "default-a", apply.Deployment)
	require.Len(t, applies.operations, 2)
	assert.Equal(t, "default-a", applies.operations[0].Deployment)
	assert.Equal(t, "testdb-a", applies.operations[0].Target)
	assert.Equal(t, storage.CutoverPolicyBarrier, applies.operations[0].CutoverPolicy)
	assert.Equal(t, storage.OnFailureContinue, applies.operations[0].OnFailure)
	assert.Equal(t, "default-b", applies.operations[1].Deployment)
	assert.Equal(t, "testdb-b", applies.operations[1].Target)
	assert.Equal(t, storage.CutoverPolicyBarrier, applies.operations[1].CutoverPolicy)
	assert.Equal(t, storage.OnFailureContinue, applies.operations[1].OnFailure)
	require.Len(t, tasks.tasks, 2)
	assert.NotEqual(t, tasks.tasks[0].TaskIdentifier, tasks.tasks[1].TaskIdentifier)
	assert.Equal(t, "users", tasks.tasks[0].TableName)
	assert.Equal(t, "users", tasks.tasks[1].TableName)
	require.NotNil(t, tasks.tasks[0].ApplyOperationID)
	require.NotNil(t, tasks.tasks[1].ApplyOperationID)
	assert.Equal(t, applies.operations[0].ID, *tasks.tasks[0].ApplyOperationID)
	assert.Equal(t, applies.operations[1].ID, *tasks.tasks[1].ApplyOperationID)
}

func TestCreateStoredApplyFansOutShardedPlanOperations(t *testing.T) {
	// A sharded plan queues one operation per changed shard/table so each shard
	// can be claimed and driven independently while unchanged shards stay out of
	// the apply operation set.
	applies := &capturingApplyStore{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	tasks := &capturingTaskStore{}
	applies.taskStore = tasks
	plan := executeApplyTestPlan()
	plan.DatabaseType = storage.DatabaseTypeStrata
	plan.Target = "commerce-target"
	plan.Namespaces = map[string]*storage.NamespacePlanData{
		"commerce": {
			Tables: []storage.TableChange{
				{Namespace: "commerce", Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", Operation: "alter"},
			},
		},
	}
	plan.Shards = []storage.ShardPlan{
		{Namespace: "commerce", Shard: "80-", NeedsChange: true},
		{Namespace: "commerce", Shard: "-80", NeedsChange: true},
		{Namespace: "commerce", Shard: "-", NeedsChange: false},
	}
	cfg := testServerConfig()
	cfg.Databases = map[string]DatabaseConfig{}
	cfg.Databases["testdb"] = DatabaseConfig{
		Type: storage.DatabaseTypeStrata,
		Environments: map[string]EnvironmentConfig{
			"staging": {Target: "commerce-target", Deployment: DefaultDeployment},
		},
	}
	svc := New(&mockStorageWithApplyStores{
		plans:     &staticPlanStore{plan: plan},
		applies:   applies,
		tasks:     tasks,
		locks:     &emptyLockStore{},
		applyLogs: &noopApplyLogStore{},
		controls:  &memoryControlRequestStore{},
	}, cfg, map[string]tern.Client{}, logger)

	apply, storedApplyID, err := svc.createStoredApply(t.Context(), plan, ApplyRequest{Environment: "staging"}, nil, "apply-sharded-fanout", true)

	require.NoError(t, err)
	assert.Equal(t, int64(123), storedApplyID)
	require.NotNil(t, apply)
	assert.Equal(t, storage.EngineStrata, apply.Engine)
	require.Len(t, applies.operations, 2)
	assert.Equal(t, DefaultDeployment, applies.operations[0].Deployment)
	assert.Equal(t, "commerce-target", applies.operations[0].Target)
	assert.Equal(t, "commerce/-80/users", applies.operations[0].OperationKey)
	assert.Equal(t, storage.ApplyOperationKindWork, applies.operations[0].OperationKind)
	assert.Equal(t, DefaultDeployment, applies.operations[1].Deployment)
	assert.Equal(t, "commerce-target", applies.operations[1].Target)
	assert.Equal(t, "commerce/80-/users", applies.operations[1].OperationKey)
	assert.Equal(t, storage.ApplyOperationKindWork, applies.operations[1].OperationKind)
	require.Len(t, tasks.tasks, 2)
	assert.Equal(t, "-80", tasks.tasks[0].Shard)
	assert.Equal(t, "users", tasks.tasks[0].TableName)
	require.NotNil(t, tasks.tasks[0].ApplyOperationID)
	assert.Equal(t, applies.operations[0].ID, *tasks.tasks[0].ApplyOperationID)
	assert.Equal(t, "80-", tasks.tasks[1].Shard)
	assert.Equal(t, "users", tasks.tasks[1].TableName)
	require.NotNil(t, tasks.tasks[1].ApplyOperationID)
	assert.Equal(t, applies.operations[1].ID, *tasks.tasks[1].ApplyOperationID)
}

func TestCreateStoredApplyFansOutShardedPlanWithFinalizerOperation(t *testing.T) {
	// A sharded plan with a namespace-level VSchema change keeps the work
	// operations shard-scoped and queues the VSchema change as a task-less
	// group_finalizer operation, driven through the same operation-scoped path.
	// The finalizer reconstructs the VSchema from the plan, so it carries no task.
	applies := &capturingApplyStore{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	tasks := &capturingTaskStore{}
	applies.taskStore = tasks
	plan := executeApplyTestPlan()
	plan.DatabaseType = storage.DatabaseTypeStrata
	plan.Target = "commerce-target"
	plan.Namespaces = map[string]*storage.NamespacePlanData{
		"commerce": {
			Artifacts: map[string]string{vSchemaArtifactName: "{\"sharded\":true}"},
			Tables: []storage.TableChange{
				{Namespace: "commerce", Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", Operation: "alter"},
			},
		},
	}
	plan.Shards = []storage.ShardPlan{
		{Namespace: "commerce", Shard: "80-", NeedsChange: true},
		{Namespace: "commerce", Shard: "-80", NeedsChange: true},
	}
	cfg := testServerConfig()
	cfg.Databases = map[string]DatabaseConfig{}
	cfg.Databases["testdb"] = DatabaseConfig{
		Type: storage.DatabaseTypeStrata,
		Environments: map[string]EnvironmentConfig{
			"staging": {Target: "commerce-target", Deployment: DefaultDeployment},
		},
	}
	svc := New(&mockStorageWithApplyStores{
		plans:     &staticPlanStore{plan: plan},
		applies:   applies,
		tasks:     tasks,
		locks:     &emptyLockStore{},
		applyLogs: &noopApplyLogStore{},
		controls:  &memoryControlRequestStore{},
	}, cfg, map[string]tern.Client{}, logger)

	_, _, err := svc.createStoredApply(t.Context(), plan, ApplyRequest{Environment: "staging"}, nil, "apply-sharded-finalizer", true)

	require.NoError(t, err)
	require.Len(t, applies.operations, 3)
	assert.Equal(t, "commerce/-80/users", applies.operations[0].OperationKey)
	assert.Equal(t, storage.ApplyOperationKindWork, applies.operations[0].OperationKind)
	assert.Equal(t, "commerce/80-/users", applies.operations[1].OperationKey)
	assert.Equal(t, storage.ApplyOperationKindWork, applies.operations[1].OperationKind)
	assert.Equal(t, "commerce/group_finalizer", applies.operations[2].OperationKey)
	assert.Equal(t, storage.ApplyOperationKindGroupFinalizer, applies.operations[2].OperationKind)

	// Only the two shard-work tasks exist; the finalizer is task-less.
	require.Len(t, tasks.tasks, 2)
	assert.Equal(t, "-80", tasks.tasks[0].Shard)
	assert.Equal(t, "users", tasks.tasks[0].TableName)
	assert.Equal(t, "80-", tasks.tasks[1].Shard)
	assert.Equal(t, "users", tasks.tasks[1].TableName)
}

func TestCreateStoredApplyDoesNotDropFinalizerOnlyNamespace(t *testing.T) {
	// When a sharded apply also carries a VSchema-only namespace (a VSchema change
	// with no shard work of its own), that namespace still gets a task-less
	// group_finalizer so its VSchema change is preserved rather than dropped.
	applies := &capturingApplyStore{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	tasks := &capturingTaskStore{}
	applies.taskStore = tasks
	plan := executeApplyTestPlan()
	plan.DatabaseType = storage.DatabaseTypeStrata
	plan.Namespaces = map[string]*storage.NamespacePlanData{
		"commerce": {
			Tables: []storage.TableChange{
				{Namespace: "commerce", Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", Operation: "alter"},
			},
		},
		"routing": {
			Artifacts: map[string]string{vSchemaArtifactName: "{\"routing\":true}"},
		},
	}
	plan.Shards = []storage.ShardPlan{{Namespace: "commerce", Shard: "-", NeedsChange: true}}
	cfg := testServerConfig()
	cfg.Databases = map[string]DatabaseConfig{}
	cfg.Databases["testdb"] = DatabaseConfig{
		Type: storage.DatabaseTypeStrata,
		Environments: map[string]EnvironmentConfig{
			"staging": {Target: "commerce-target", Deployment: DefaultDeployment},
		},
	}
	svc := New(&mockStorageWithApplyStores{
		plans:     &staticPlanStore{plan: plan},
		applies:   applies,
		tasks:     tasks,
		locks:     &emptyLockStore{},
		applyLogs: &noopApplyLogStore{},
		controls:  &memoryControlRequestStore{},
	}, cfg, map[string]tern.Client{}, logger)

	_, _, err := svc.createStoredApply(t.Context(), plan, ApplyRequest{Environment: "staging"}, nil, "apply-finalizer-only-namespace", true)

	require.NoError(t, err)
	require.Len(t, applies.operations, 2)
	assert.Equal(t, "commerce/-/users", applies.operations[0].OperationKey)
	assert.Equal(t, storage.ApplyOperationKindWork, applies.operations[0].OperationKind)
	assert.Equal(t, "routing/group_finalizer", applies.operations[1].OperationKey)
	assert.Equal(t, storage.ApplyOperationKindGroupFinalizer, applies.operations[1].OperationKind)
	// The routing VSchema change is preserved as a task-less finalizer; only the
	// commerce shard work produces a task.
	require.Len(t, tasks.tasks, 1)
	assert.Equal(t, "users", tasks.tasks[0].TableName)
}

func TestCreateStoredApplyDoesNotShardWithoutClientOptIn(t *testing.T) {
	applies := &capturingApplyStore{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	tasks := &capturingTaskStore{}
	applies.taskStore = tasks
	plan := executeApplyTestPlan()
	plan.Namespaces = map[string]*storage.NamespacePlanData{
		"commerce": {
			Tables: []storage.TableChange{
				{Namespace: "commerce", Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", Operation: "alter"},
			},
		},
	}
	plan.Shards = []storage.ShardPlan{
		{Namespace: "commerce", Shard: "-80", NeedsChange: true},
		{Namespace: "commerce", Shard: "80-", NeedsChange: true},
	}
	svc := New(&mockStorageWithApplyStores{
		plans:     &staticPlanStore{plan: plan},
		applies:   applies,
		tasks:     tasks,
		locks:     &emptyLockStore{},
		applyLogs: &noopApplyLogStore{},
		controls:  &memoryControlRequestStore{},
	}, testServerConfig(), map[string]tern.Client{}, logger)

	_, _, err := svc.createStoredApply(t.Context(), plan, ApplyRequest{Environment: "staging"}, nil, "apply-no-sharded-fanout", false)

	require.NoError(t, err)
	require.Len(t, applies.operations, 1)
	assert.Empty(t, applies.operations[0].OperationKey)
	require.Len(t, tasks.tasks, 1)
	assert.Empty(t, tasks.tasks[0].Shard)
}

func TestValidateShardOperationKeyPartsRejectsDelimiter(t *testing.T) {
	err := validateShardOperationKeyParts("commerce", "-80/80-", "users")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "reserved delimiter")
}

func TestValidateOperationKeyPartRejectsFinalizerNamespaceDelimiter(t *testing.T) {
	err := validateOperationKeyPart("namespace", "commerce/eu")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "reserved delimiter")
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

func TestDatabaseListSanitizesConfigAndReportsTopology(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorage{}, &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"accounts": {
				Type: storage.DatabaseTypeVitess,
				Environments: map[string]EnvironmentConfig{
					"production": {
						Target:     "accounts-prod-target",
						Deployment: "sled",
					},
				},
			},
			"orders": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"staging": {
						DSN: "orders_user:orders_password@tcp(localhost:3306)/orders_staging",
					},
					"production": {
						Target:     "orders-prod-target",
						Deployment: "pie",
					},
				},
				AllowedRepos: []string{"octocat/orders"},
				AllowedDirs:  []string{"schema/orders"},
			},
		},
		TernDeployments: TernConfig{
			"pie":  {"production": "pie.example:9090"},
			"sled": {"production": "sled.example:9090"},
		},
		AllowedEnvironments: []string{"staging"},
		EnvironmentOrder:    []string{"production", "staging"},
	}, nil, logger)
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/databases", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	body := w.Body.String()
	assert.NotContains(t, body, "orders_password")
	assert.NotContains(t, body, "orders-prod-target")
	assert.NotContains(t, body, "pie.example")
	assert.NotContains(t, body, "execution_mode")
	assert.NotContains(t, body, "execution_target_count")
	assert.NotContains(t, body, "server_handles_environment")

	var resp apitypes.DatabaseListResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Len(t, resp.Databases, 2)

	accounts := resp.Databases[0]
	assert.Equal(t, "accounts", accounts.Database)
	assert.Equal(t, storage.DatabaseTypeVitess, accounts.Type)
	require.Len(t, accounts.Environments, 1)
	assert.Equal(t, "production", accounts.Environments[0].Environment)
	assert.Equal(t, []string{"sled"}, accounts.Environments[0].Deployments)

	orders := resp.Databases[1]
	assert.Equal(t, "orders", orders.Database)
	assert.Equal(t, storage.DatabaseTypeMySQL, orders.Type)
	require.Len(t, orders.Environments, 2)
	assert.Equal(t, "production", orders.Environments[0].Environment)
	assert.Equal(t, []string{"pie"}, orders.Environments[0].Deployments)
	assert.Equal(t, "staging", orders.Environments[1].Environment)
	assert.Empty(t, orders.Environments[1].Deployments)

	req = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/databases?type=mysql", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Len(t, resp.Databases, 1)
	assert.Equal(t, "orders", resp.Databases[0].Database)
	assert.Equal(t, storage.DatabaseTypeMySQL, resp.Databases[0].Type)

	req = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/databases?type=vitess", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Len(t, resp.Databases, 1)
	assert.Equal(t, "accounts", resp.Databases[0].Database)
	assert.Equal(t, storage.DatabaseTypeVitess, resp.Databases[0].Type)

	req = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/databases?type=strata", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Empty(t, resp.Databases)

	req = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/databases?type=postgres", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	assert.Contains(t, w.Body.String(), "type must be")
}

func TestDatabaseListRejectsInvalidDeploymentTopology(t *testing.T) {
	_, err := databaseListResponse(&ServerConfig{
		Databases: map[string]DatabaseConfig{
			"orders": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{
					"production": {Deployments: map[string]DeploymentTarget{}},
				},
			},
		},
	}, "")

	require.Error(t, err)
	assert.Contains(t, err.Error(), `database "orders" environment "production" deployments map is empty`)
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

// The progress endpoint's headline state is always the stored apply state —
// the single source of truth shared with the PR comment observer. Even when the
// live engine reports a different phase, the response reflects stored state so
// the CLI status and the PR comment never disagree. Live engine progress still
// drives per-table detail, just not the headline state.
func TestProgressByApplyIDDisplaysStoredStateNotLiveProto(t *testing.T) {
	mock := &mockTernClient{
		isRemote: true,
		progressResp: &ternv1.ProgressResponse{
			ApplyId: "remote-apply-ps",
			State:   ternv1.State_STATE_RUNNING,
		},
	}
	apply := activeTestApply("apply-stored-state")
	apply.ExternalID = "remote-apply-ps"
	apply.State = state.Apply.WaitingForCutover
	svc := newControlTestService(mock, apply)
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/progress/apply/apply-stored-state", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var resp apitypes.ProgressResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, state.Apply.WaitingForCutover, resp.State,
		"displayed state must come from the stored apply state, not the live engine proto")
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
	svc.config.Drivers = 1
	// This double models the apply-level FindNextApply claim path; the
	// default operation-level claim path is covered by the operator
	// integration tests.
	svc.config.OperatorClaimOperations = new(false)
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

// The engine's display metadata on the progress response (branch, deploy-request
// URL, instant) is carried through to the API response, so the renderer reads it
// straight from the progress projection.
func TestProgressResponseFromProtoCopiesMetadata(t *testing.T) {
	resp := progressResponseFromProto(&ternv1.ProgressResponse{
		State: ternv1.State_STATE_RUNNING,
		Metadata: map[string]string{
			"branch_name":        "branch-x",
			"deploy_request_url": "https://app.example/deploy/7",
			"is_instant":         "true",
		},
	})

	assert.Equal(t, "branch-x", resp.Metadata["branch_name"])
	assert.Equal(t, "https://app.example/deploy/7", resp.Metadata["deploy_request_url"])
	assert.Equal(t, "true", resp.Metadata["is_instant"])
}

func TestProgressFromLocalStorageIncludesOperationProgressAndTableDeployment(t *testing.T) {
	startedAt := time.Date(2026, 6, 16, 10, 0, 0, 0, time.UTC)
	completedAt := startedAt.Add(5 * time.Minute)
	opAID := int64(101)
	opBID := int64(102)
	apply := &storage.Apply{
		ID:              10,
		ApplyIdentifier: "apply_multi_deploy",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Running,
	}
	svc := New(&mockStorageWithApplyStores{
		tasks: &capturingTaskStore{tasks: []*storage.Task{
			{
				ApplyID:          apply.ID,
				ApplyOperationID: &opAID,
				TaskIdentifier:   "task_users",
				TableName:        "users",
				Namespace:        "testdb",
				DDLAction:        "alter",
				DDL:              "ALTER TABLE users ADD COLUMN email varchar(255)",
				State:            state.Task.Running,
				ProgressPercent:  42,
				RowsCopied:       420,
				RowsTotal:        1000,
				Database:         "testdb",
				DatabaseType:     storage.DatabaseTypeMySQL,
				Engine:           storage.EngineSpirit,
				Environment:      "staging",
			},
			{
				ApplyID:          apply.ID,
				ApplyOperationID: &opBID,
				TaskIdentifier:   "task_orders",
				TableName:        "orders",
				Namespace:        "testdb",
				DDLAction:        "alter",
				DDL:              "ALTER TABLE orders ADD COLUMN shipped_at timestamp NULL",
				State:            state.Task.Completed,
				ProgressPercent:  100,
				RowsCopied:       50,
				RowsTotal:        50,
				Database:         "testdb",
				DatabaseType:     storage.DatabaseTypeMySQL,
				Engine:           storage.EngineSpirit,
				Environment:      "staging",
			},
		}},
		operations: &staticApplyOperationStore{operations: []*storage.ApplyOperation{
			{ID: opAID, ApplyID: apply.ID, Deployment: "deploy-a", OperationKind: storage.ApplyOperationKindWork, Target: "target-a", State: state.ApplyOperation.Running, StartedAt: &startedAt},
			{ID: opBID, ApplyID: apply.ID, Deployment: "deploy-b", OperationKind: storage.ApplyOperationKindGroupFinalizer, Target: "target-b", State: state.ApplyOperation.Failed, ErrorMessage: "engine failed", StartedAt: &startedAt, CompletedAt: &completedAt},
		}},
	}, testServerConfig(), nil, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))

	resp, err := svc.progressFromLocalStorage(t.Context(), apply)

	require.NoError(t, err)
	require.Len(t, resp.Operations, 2)
	assert.Equal(t, "deploy-a", resp.Operations[0].Deployment)
	assert.Equal(t, storage.ApplyOperationKindWork, resp.Operations[0].OperationKind)
	assert.Equal(t, "target-a", resp.Operations[0].Target)
	assert.Equal(t, state.ApplyOperation.Running, resp.Operations[0].State)
	assert.Equal(t, startedAt.Format(time.RFC3339), resp.Operations[0].StartedAt)
	assert.Equal(t, "deploy-b", resp.Operations[1].Deployment)
	assert.Equal(t, storage.ApplyOperationKindGroupFinalizer, resp.Operations[1].OperationKind)
	assert.Equal(t, apitypes.ErrCodeEngineError, resp.Operations[1].ErrorCode)
	assert.Equal(t, "engine failed", resp.Operations[1].ErrorMessage)
	assert.Equal(t, completedAt.Format(time.RFC3339), resp.Operations[1].CompletedAt)
	require.Len(t, resp.Tables, 2)
	assert.Equal(t, "deploy-a", resp.Tables[0].Deployment)
	assert.Equal(t, "deploy-b", resp.Tables[1].Deployment)
}

func newActiveProgressServiceWithOperations(client tern.Client, apply *storage.Apply, operations storage.ApplyOperationStore) *Service {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(&mockStorageWithApplyStores{
		applies:    &staticApplyStore{apply: apply},
		tasks:      &capturingTaskStore{},
		controls:   &memoryControlRequestStore{},
		operations: operations,
	}, testServerConfig(), map[string]tern.Client{
		"default/staging": client,
	}, logger)
}

func TestProgressByApplyIDActivePathIncludesOperations(t *testing.T) {
	// A single-operation active apply reaches the proto Progress RPC path and
	// enriches the response with its operation row from control-plane storage.
	mock := &mockTernClient{
		isRemote:     true,
		progressResp: &ternv1.ProgressResponse{State: ternv1.State_STATE_RUNNING},
	}
	apply := activeTestApply("apply-active-ops")
	apply.ExternalID = "remote-active-ops"
	operations := &staticApplyOperationStore{operations: []*storage.ApplyOperation{
		{ID: 1, ApplyID: apply.ID, Deployment: "deploy-a", Target: "target-a", State: state.ApplyOperation.Running},
	}}
	svc := newActiveProgressServiceWithOperations(mock, apply, operations)
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/progress/apply/apply-active-ops", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NotNil(t, mock.progressReq, "single-operation active apply must reach the proto Progress RPC path")

	var resp apitypes.ProgressResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Len(t, resp.Operations, 1)
	assert.Equal(t, "deploy-a", resp.Operations[0].Deployment)
	assert.Equal(t, state.ApplyOperation.Running, resp.Operations[0].State)
}

func TestProgressByApplyIDMultiOperationServedFromStorage(t *testing.T) {
	// A multi-operation apply has no single data-plane apply id, so its progress
	// is served from storage: the proto Progress RPC is not called, the headline
	// state is the stored aggregate, and every operation row is included.
	mock := &mockTernClient{
		isRemote:     true,
		progressResp: &ternv1.ProgressResponse{State: ternv1.State_STATE_RUNNING},
	}
	apply := activeTestApply("apply-multi-ops")
	apply.ExternalID = "remote-multi-ops"
	operations := &staticApplyOperationStore{operations: []*storage.ApplyOperation{
		{ID: 1, ApplyID: apply.ID, Deployment: "deploy-a", Target: "target-a", State: state.ApplyOperation.Running},
		{ID: 2, ApplyID: apply.ID, Deployment: "deploy-b", Target: "target-b", State: state.ApplyOperation.Failed, ErrorMessage: "engine failed"},
	}}
	svc := newActiveProgressServiceWithOperations(mock, apply, operations)
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/progress/apply/apply-multi-ops", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Nil(t, mock.progressReq, "multi-operation apply must not call the single-deployment proto Progress RPC")

	var resp apitypes.ProgressResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, state.Apply.Running, resp.State)
	require.Len(t, resp.Operations, 2)
	assert.Equal(t, "deploy-a", resp.Operations[0].Deployment)
	assert.Equal(t, state.ApplyOperation.Running, resp.Operations[0].State)
	assert.Equal(t, "deploy-b", resp.Operations[1].Deployment)
	assert.Equal(t, "engine failed", resp.Operations[1].ErrorMessage)
}

func TestProgressByApplyIDMultiOperationFallsBackToSingleDeploymentOnStorageError(t *testing.T) {
	// A multi-operation apply is normally served from storage, but if the
	// storage read fails the handler must fall back to the single-deployment
	// path rather than fail the request: every apply created today has one
	// operation, so this only degrades the dormant multi-op case.
	mock := &mockTernClient{
		isRemote:     true,
		progressResp: &ternv1.ProgressResponse{State: ternv1.State_STATE_RUNNING},
	}
	apply := activeTestApply("apply-multi-ops-fallback")
	apply.ExternalID = "remote-multi-ops-fallback"
	operations := &staticApplyOperationStore{operations: []*storage.ApplyOperation{
		{ID: 1, ApplyID: apply.ID, Deployment: "deploy-a", Target: "target-a", State: state.ApplyOperation.Running},
		{ID: 2, ApplyID: apply.ID, Deployment: "deploy-b", Target: "target-b", State: state.ApplyOperation.Running},
	}}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithApplyStores{
		applies:    &staticApplyStore{apply: apply},
		tasks:      &capturingTaskStore{err: errors.New("tasks store unavailable")},
		controls:   &memoryControlRequestStore{},
		operations: operations,
	}, testServerConfig(), map[string]tern.Client{
		"default/staging": mock,
	}, logger)
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/progress/apply/apply-multi-ops-fallback", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NotNil(t, mock.progressReq, "storage-error fallback must reach the single-deployment proto Progress RPC path")

	var resp apitypes.ProgressResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, state.Apply.Running, resp.State)
	// Operation rows were listed successfully, so the per-deployment enrichment
	// is still present even though the storage progress read fell back.
	require.Len(t, resp.Operations, 2)
	assert.Equal(t, "deploy-a", resp.Operations[0].Deployment)
	assert.Equal(t, "deploy-b", resp.Operations[1].Deployment)
}

func TestProgressByApplyIDActivePathToleratesOperationStorageError(t *testing.T) {
	// Operation enrichment is observability, not a safety gate: a storage error
	// from ListByApply must omit operations rather than fail the request.
	mock := &mockTernClient{
		isRemote:     true,
		progressResp: &ternv1.ProgressResponse{State: ternv1.State_STATE_RUNNING},
	}
	apply := activeTestApply("apply-active-ops-err")
	apply.ExternalID = "remote-active-ops-err"
	operations := &staticApplyOperationStore{err: errors.New("operations store unavailable")}
	svc := newActiveProgressServiceWithOperations(mock, apply, operations)
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/progress/apply/apply-active-ops-err", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NotNil(t, mock.progressReq, "active apply must reach the proto Progress RPC path")

	var resp apitypes.ProgressResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Empty(t, resp.Operations)
	assert.Equal(t, state.Apply.Running, resp.State)
}

func TestProgressFromLocalStorageSingleDeploymentOmitsOperationFields(t *testing.T) {
	apply := &storage.Apply{ID: 20, ApplyIdentifier: "apply_single", Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging", Engine: storage.EngineSpirit, State: state.Apply.Completed}
	svc := New(&mockStorageWithApplyStores{
		tasks: &capturingTaskStore{tasks: []*storage.Task{
			{ApplyID: apply.ID, TaskIdentifier: "task_users", TableName: "users", Namespace: "testdb", DDLAction: "alter", DDL: "ALTER TABLE users ADD COLUMN email varchar(255)", State: state.Task.Completed, Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL, Engine: storage.EngineSpirit, Environment: "staging"},
		}},
		operations: &staticApplyOperationStore{},
	}, testServerConfig(), nil, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))

	resp, err := svc.progressFromLocalStorage(t.Context(), apply)

	require.NoError(t, err)
	assert.Empty(t, resp.Operations)
	require.Len(t, resp.Tables, 1)
	assert.Empty(t, resp.Tables[0].Deployment)
}

// A completed PlanetScale apply served from storage still surfaces the deploy
// display fields (branch, deploy-request URL, instant/deferred flags). The
// engine is not polled on the storage path, so these are read from the durable
// engine resume state persisted on the apply's operation — the "let me look at
// what happened" case must not lose the deploy-request link.
func TestProgressFromLocalStorageOverlaysDisplayMetadataFromResumeState(t *testing.T) {
	opID := int64(77)
	apply := &storage.Apply{
		ID:              30,
		ApplyIdentifier: "apply_ps_completed",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeVitess,
		Environment:     "staging",
		Engine:          storage.EnginePlanetScale,
		State:           state.Apply.Completed,
	}
	svc := New(&mockStorageWithApplyStores{
		tasks: &capturingTaskStore{tasks: []*storage.Task{
			{ApplyID: apply.ID, ApplyOperationID: &opID, TaskIdentifier: "task_users", TableName: "users", Namespace: "testdb", DDLAction: "alter", DDL: "ALTER TABLE users ADD COLUMN email varchar(255)", State: state.Task.Completed, Database: "testdb", DatabaseType: storage.DatabaseTypeVitess, Engine: storage.EnginePlanetScale, Environment: "staging"},
		}},
		operations: &staticApplyOperationStore{
			operations: []*storage.ApplyOperation{
				{ID: opID, ApplyID: apply.ID, Deployment: "testdb", Target: "testdb", State: state.ApplyOperation.Completed},
			},
			resumeStateByOp: map[int64]*storage.EngineResumeState{
				opID: {ApplyOperationID: opID, Metadata: `{"branch_name":"schemabot-testdb-123","deploy_request_url":"https://app.planetscale.com/org/testdb/deploy-requests/42","is_instant":true,"deferred_deploy":true}`},
			},
		},
	}, testServerConfig(), nil, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))

	resp, err := svc.progressFromLocalStorage(t.Context(), apply)

	require.NoError(t, err)
	require.NotNil(t, resp.Metadata)
	assert.Equal(t, "schemabot-testdb-123", resp.Metadata["branch_name"])
	assert.Equal(t, "https://app.planetscale.com/org/testdb/deploy-requests/42", resp.Metadata["deploy_request_url"])
	assert.Equal(t, "true", resp.Metadata["is_instant"])
	assert.Equal(t, "true", resp.Metadata["deferred_deploy"])
}

// An apply with no engine resume state (e.g. one that predates resume-state
// persistence) is served from storage without the deploy display fields and
// without error — the overlay is best-effort enrichment, never a gate.
func TestProgressFromLocalStorageWithoutResumeStateOmitsDisplayMetadata(t *testing.T) {
	opID := int64(88)
	apply := &storage.Apply{
		ID:              31,
		ApplyIdentifier: "apply_ps_no_resume",
		Database:        "testdb",
		DatabaseType:    storage.DatabaseTypeVitess,
		Environment:     "staging",
		Engine:          storage.EnginePlanetScale,
		State:           state.Apply.Completed,
	}
	svc := New(&mockStorageWithApplyStores{
		tasks: &capturingTaskStore{tasks: []*storage.Task{
			{ApplyID: apply.ID, ApplyOperationID: &opID, TaskIdentifier: "task_users", TableName: "users", Namespace: "testdb", DDLAction: "alter", DDL: "ALTER TABLE users ADD COLUMN email varchar(255)", State: state.Task.Completed, Database: "testdb", DatabaseType: storage.DatabaseTypeVitess, Engine: storage.EnginePlanetScale, Environment: "staging"},
		}},
		operations: &staticApplyOperationStore{
			operations: []*storage.ApplyOperation{
				{ID: opID, ApplyID: apply.ID, Deployment: "testdb", Target: "testdb", State: state.ApplyOperation.Completed},
			},
		},
	}, testServerConfig(), nil, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))

	resp, err := svc.progressFromLocalStorage(t.Context(), apply)

	require.NoError(t, err)
	assert.Empty(t, resp.Metadata["branch_name"])
	assert.Empty(t, resp.Metadata["deploy_request_url"])
}

func TestValidateRollbackSourceApplyAcceptsLatestCompletedApplyWithOriginalFiles(t *testing.T) {
	now := time.Now().UTC()
	apply := rollbackGuardrailApply("apply_latest", 1, 10, now)
	svc := newRollbackGuardrailService(apply, rollbackGuardrailPlan(10, true), []*storage.Task{
		rollbackGuardrailTask(1, 10, now),
	})

	gotApply, gotPlan, err := svc.ValidateRollbackSourceApply(t.Context(), RollbackSourceRequest{
		ApplyIdentifier: "apply_latest",
		Environment:     "staging",
		Repository:      "org/repo",
		PullRequest:     1,
	})

	require.NoError(t, err)
	assert.Equal(t, apply, gotApply)
	require.NotNil(t, gotPlan)
	assert.Equal(t, int64(10), gotPlan.ID)
}

func TestValidateRollbackSourceApplyRequiresEnvironment(t *testing.T) {
	now := time.Now().UTC()
	apply := rollbackGuardrailApply("apply_latest", 1, 10, now)
	svc := newRollbackGuardrailService(apply, rollbackGuardrailPlan(10, true), []*storage.Task{
		rollbackGuardrailTask(1, 10, now),
	})

	_, _, err := svc.ValidateRollbackSourceApply(t.Context(), RollbackSourceRequest{
		ApplyIdentifier: "apply_latest",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "environment is required")
}

func TestValidateRollbackSourceApplyRequiresPullRequestScopeWhenRequested(t *testing.T) {
	now := time.Now().UTC()
	apply := rollbackGuardrailApply("apply_latest", 1, 10, now)
	svc := newRollbackGuardrailService(apply, rollbackGuardrailPlan(10, true), []*storage.Task{
		rollbackGuardrailTask(1, 10, now),
	})

	_, _, err := svc.ValidateRollbackSourceApply(t.Context(), RollbackSourceRequest{
		ApplyIdentifier:         "apply_latest",
		Environment:             "staging",
		RequirePullRequestScope: true,
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "repository is required")
}

func TestValidateRollbackSourceApplyRejectsPlanWithoutOriginalFiles(t *testing.T) {
	now := time.Now().UTC()
	apply := rollbackGuardrailApply("apply_no_schema", 1, 10, now)
	svc := newRollbackGuardrailService(apply, rollbackGuardrailPlan(10, false), []*storage.Task{
		rollbackGuardrailTask(1, 10, now),
	})

	_, _, err := svc.ValidateRollbackSourceApply(t.Context(), RollbackSourceRequest{
		ApplyIdentifier: "apply_no_schema",
		Environment:     "staging",
		Repository:      "org/repo",
		PullRequest:     1,
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no stored original schema files")
}

func TestValidateRollbackSourceApplyRejectsPlannerSourceMismatchForOlderApply(t *testing.T) {
	now := time.Now().UTC()
	older := rollbackGuardrailApply("apply_older", 1, 10, now.Add(-time.Minute))
	newer := rollbackGuardrailApply("apply_newer", 2, 20, now)
	svc := newRollbackGuardrailServiceWithApplies([]*storage.Apply{older, newer}, map[int64]*storage.Plan{
		10: rollbackGuardrailPlan(10, true),
		20: rollbackGuardrailPlan(20, true),
	}, []*storage.Task{
		rollbackGuardrailTask(1, 10, now.Add(-time.Minute)),
		rollbackGuardrailTask(2, 20, now),
	})

	_, _, err := svc.ValidateRollbackSourceApply(t.Context(), RollbackSourceRequest{
		ApplyIdentifier: "apply_older",
		Environment:     "staging",
		Repository:      "org/repo",
		PullRequest:     1,
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "current rollback planner would select")
}

func TestValidateRollbackSourceApplyScopesLatestCompletedTaskByEnvironment(t *testing.T) {
	now := time.Now().UTC()
	apply := rollbackGuardrailApply("apply_staging", 1, 10, now.Add(-time.Minute))
	prodTask := rollbackGuardrailTask(2, 20, now)
	prodTask.Environment = "production"
	svc := newRollbackGuardrailService(apply, rollbackGuardrailPlan(10, true), []*storage.Task{
		rollbackGuardrailTask(1, 10, now.Add(-time.Minute)),
		prodTask,
	})

	gotApply, gotPlan, err := svc.ValidateRollbackSourceApply(t.Context(), RollbackSourceRequest{
		ApplyIdentifier: "apply_staging",
		Environment:     "staging",
		Repository:      "org/repo",
		PullRequest:     1,
	})

	require.NoError(t, err)
	assert.Equal(t, apply, gotApply)
	require.NotNil(t, gotPlan)
	assert.Equal(t, int64(10), gotPlan.ID)
}

func newRollbackGuardrailService(apply *storage.Apply, plan *storage.Plan, tasks []*storage.Task) *Service {
	return newRollbackGuardrailServiceWithApplies([]*storage.Apply{apply}, map[int64]*storage.Plan{plan.ID: plan}, tasks)
}

func newRollbackGuardrailServiceWithApplies(applies []*storage.Apply, plans map[int64]*storage.Plan, tasks []*storage.Task) *Service {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(&mockStorageWithApplyStores{
		plans:   &staticPlanStore{plansByID: plans},
		applies: &staticApplyStore{applies: applies},
		tasks:   &capturingTaskStore{tasks: tasks},
	}, testServerConfig(), nil, logger)
}

func rollbackGuardrailApply(identifier string, id, planID int64, completedAt time.Time) *storage.Apply {
	return &storage.Apply{
		ID:              id,
		ApplyIdentifier: identifier,
		PlanID:          planID,
		Database:        "orders",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Repository:      "org/repo",
		PullRequest:     1,
		Environment:     "staging",
		State:           state.Apply.Completed,
		CompletedAt:     &completedAt,
		CreatedAt:       completedAt,
		UpdatedAt:       completedAt,
	}
}

func rollbackGuardrailPlan(id int64, includeOriginalFiles bool) *storage.Plan {
	plan := &storage.Plan{
		ID:           id,
		Database:     "orders",
		DatabaseType: storage.DatabaseTypeMySQL,
		Environment:  "staging",
		Namespaces: map[string]*storage.NamespacePlanData{
			"orders": {},
		},
	}
	if includeOriginalFiles {
		plan.Namespaces["orders"].OriginalFiles = map[string]string{
			"users.sql": "CREATE TABLE `users` (`id` bigint unsigned NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		}
		plan.Namespaces["orders"].OriginalFilesCaptured = true
	}
	return plan
}

func rollbackGuardrailTask(applyID, planID int64, completedAt time.Time) *storage.Task {
	return &storage.Task{
		ApplyID:      applyID,
		PlanID:       planID,
		Database:     "orders",
		DatabaseType: storage.DatabaseTypeMySQL,
		Repository:   "org/repo",
		PullRequest:  1,
		Environment:  "staging",
		State:        state.Task.Completed,
		CompletedAt:  &completedAt,
		CreatedAt:    completedAt,
		UpdatedAt:    completedAt,
	}
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
			body: `{"apply_id": "apply-123", "environment": "staging", "deployment": "default"}`,
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

func TestRollbackPlanRequiresEnvironment(t *testing.T) {
	svc := newTestService()
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/rollback/plan", strings.NewReader(`{"apply_id": "apply-123"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	assert.Contains(t, w.Body.String(), "environment is required")
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
		{
			name: "rollback plan",
			path: "/api/rollback/plan",
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

	t.Run("remote stop queues durable request and propagates to remote durable queue", func(t *testing.T) {
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
		require.NotNil(t, mock.stopReq, "remote stop propagation should queue data-plane durable intent")
		assert.Equal(t, "remote-apply-stop", mock.stopReq.ApplyId)
		assert.Equal(t, "staging", mock.stopReq.Environment)
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

	t.Run("multi-operation apply queues durable stop without an immediate single-deployment stop", func(t *testing.T) {
		// A multi-operation apply has no single data-plane apply id, so the
		// immediate stop is skipped: the durable request is queued for the
		// operator to fan out per operation, and the single-deployment remote
		// Stop RPC is never called.
		mock := &mockTernClient{isRemote: true, stopResp: &ternv1.StopResponse{Accepted: true}}
		apply := activeTestApply("apply-multi-stop")
		tasks := []*storage.Task{
			{ID: 30, TaskIdentifier: "task-multi-stop", ApplyID: apply.ID, State: state.Task.Running},
		}
		svc := newControlTestServiceWithOperations(mock, apply, tasks, []*storage.ApplyOperation{
			{ID: 1, ApplyID: apply.ID, Deployment: "deploy-a", Target: "target-a", State: state.ApplyOperation.Running},
			{ID: 2, ApplyID: apply.ID, Deployment: "deploy-b", Target: "target-b", State: state.ApplyOperation.Running},
		})
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "apply_id": "apply-multi-stop"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/stop", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
		assert.Nil(t, mock.stopReq, "multi-operation apply must not issue a single-deployment immediate stop")
		controlReq, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
		require.NoError(t, err)
		require.NotNil(t, controlReq, "durable stop request must be queued for the operator")
	})
}

func TestStartHandler(t *testing.T) {
	t.Run("queues deferred deploy start for loop processing", func(t *testing.T) {
		mock := &mockTernClient{}
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

		assert.Nil(t, mock.startReq, "request path should queue start without calling Tern start")
		controlReq, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationStart)
		require.NoError(t, err)
		require.NotNil(t, controlReq)
		assert.Equal(t, storage.ControlRequestPending, controlReq.Status)

		var resp apitypes.StartResponse
		err = json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err, "failed to decode response")
		assert.True(t, resp.Accepted)
		assert.Equal(t, int64(1), resp.StartedCount)
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

	t.Run("multi-operation apply derives cutover readiness from storage, not a single remote probe", func(t *testing.T) {
		// A multi-operation apply has no single remote data-plane apply id, so
		// readiness is derived from the stored task rows that span every
		// operation — the single-deployment remote progress probe is not called —
		// and the durable cutover request is queued for the operator.
		mock := &mockTernClient{isRemote: true}
		apply := activeTestApply("apply-multi-cutover")
		tasks := []*storage.Task{
			{ID: 40, TaskIdentifier: "task-multi-cutover", ApplyID: apply.ID, State: state.Task.WaitingForCutover},
		}
		svc := newControlTestServiceWithOperations(mock, apply, tasks, []*storage.ApplyOperation{
			{ID: 1, ApplyID: apply.ID, Deployment: "deploy-a", Target: "target-a", State: state.ApplyOperation.WaitingForCutover},
			{ID: 2, ApplyID: apply.ID, Deployment: "deploy-b", Target: "target-b", State: state.ApplyOperation.WaitingForCutover},
		})
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "apply_id": "apply-multi-cutover", "caller": "cli:cutter"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/cutover", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
		assert.Nil(t, mock.progressReq, "multi-operation apply must not probe a single-deployment remote for cutover readiness")
		assert.Nil(t, mock.cutoverReq, "request path should queue operator work without calling Tern cutover")
		controlReq, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationCutover)
		require.NoError(t, err)
		require.NotNil(t, controlReq, "durable cutover request must be queued for the operator")
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

func TestRollbackSchemaFilesAllowsCapturedEmptyOriginalFiles(t *testing.T) {
	plan := &storage.Plan{
		Namespaces: map[string]*storage.NamespacePlanData{
			"shop": {
				OriginalFiles:         map[string]string{},
				OriginalFilesCaptured: true,
			},
		},
	}

	schemaFiles, err := rollbackSchemaFiles(plan)
	require.NoError(t, err)
	require.Contains(t, schemaFiles, "shop")
	assert.Empty(t, schemaFiles["shop"].Files)
}

func TestRollbackSchemaFilesRejectsPlanWithoutCapturedOriginalFiles(t *testing.T) {
	plan := &storage.Plan{
		Namespaces: map[string]*storage.NamespacePlanData{
			"shop": {
				OriginalFiles: map[string]string{},
			},
		},
	}

	schemaFiles, err := rollbackSchemaFiles(plan)
	require.Error(t, err)
	assert.Nil(t, schemaFiles)
	assert.Contains(t, err.Error(), `no original schema files available for rollback namespace "shop"`)
}

// setRevertSkippedMetadata surfaces the skip-revert flag from the apply's stored
// revert_skipped_at, so progress consumers show that revert was skipped — read
// from apply state, not an engine-specific side table.
func TestSetRevertSkippedMetadata(t *testing.T) {
	resp := &apitypes.ProgressResponse{}
	setRevertSkippedMetadata(resp, &storage.Apply{})
	assert.NotContains(t, resp.Metadata, "revert_skipped", "no flag before skip-revert is dispatched")

	now := time.Now()
	setRevertSkippedMetadata(resp, &storage.Apply{RevertSkippedAt: &now})
	assert.Equal(t, "true", resp.Metadata["revert_skipped"], "flag set once revert_skipped_at is present")
}
