package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	pingErr       error
	webhookEvents storage.WebhookEventStore
}

func (m *mockStorage) Locks() storage.LockStore                     { return nil }
func (m *mockStorage) Plans() storage.PlanStore                     { return nil }
func (m *mockStorage) Applies() storage.ApplyStore                  { return nil }
func (m *mockStorage) Tasks() storage.TaskStore                     { return nil }
func (m *mockStorage) ApplyLogs() storage.ApplyLogStore             { return nil }
func (m *mockStorage) ControlRequests() storage.ControlRequestStore { return nil }
func (m *mockStorage) ApplyComments() storage.ApplyCommentStore     { return nil }
func (m *mockStorage) PlanComments() storage.PlanCommentStore       { return nil }
func (m *mockStorage) ApplyOperations() storage.ApplyOperationStore { return nil }
func (m *mockStorage) Checks() storage.CheckStore                   { return nil }
func (m *mockStorage) Settings() storage.SettingsStore              { return nil }
func (m *mockStorage) WebhookEvents() storage.WebhookEventStore     { return m.webhookEvents }
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
func (m *mockPlanLookupStore) List(context.Context, storage.ListPlansOptions) ([]*storage.Plan, error) {
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
	reaped          []*storage.ReapedOperation
	reapErr         error
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

func (s *staticApplyOperationStore) ListByApplies(_ context.Context, applyIDs []int64) ([]*storage.ApplyOperation, error) {
	if s.err != nil {
		return nil, s.err
	}
	applyIDSet := make(map[int64]bool, len(applyIDs))
	for _, applyID := range applyIDs {
		applyIDSet[applyID] = true
	}
	operations := make([]*storage.ApplyOperation, 0, len(s.operations))
	for _, op := range s.operations {
		if applyIDSet[op.ApplyID] {
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

// ReapStranded is called by the stranded-operation reaper, which starts with the
// operator, so every double reachable from the operator lifecycle answers it. It
// can return settlements and an error together, the way a pass that fails partway
// reports the rows it already committed.
func (s *staticApplyOperationStore) ReapStranded(context.Context, int) ([]*storage.ReapedOperation, error) {
	return s.reaped, s.reapErr
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
func (s *staticApplyStore) SetRevertSkipped(context.Context, int64, time.Time) error {
	return nil
}

// GetByDatabase compares its arguments byte-for-byte against the fixture rows,
// modelling a store with no collation forgiveness of its own. A handler that
// forwards a caller's spelling unfolded therefore misses the fixture, so a
// mixed-case request only matches when the handler folds it first.
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
	applies := s.matching(filter)
	if filter.Limit > 0 && len(applies) > filter.Limit {
		return applies[:filter.Limit], nil
	}
	return applies, nil
}

func (s *recentApplyStore) CountRecentByState(_ context.Context, filter storage.RecentAppliesFilter) (map[string]int, error) {
	if s.err != nil {
		return nil, s.err
	}
	counts := map[string]int{}
	for _, apply := range s.matching(filter) {
		counts[apply.State]++
	}
	return counts, nil
}

func (s *recentApplyStore) matching(filter storage.RecentAppliesFilter) []*storage.Apply {
	applies := make([]*storage.Apply, 0, len(s.applies))
	for _, apply := range s.applies {
		if filter.Environment != "" && apply.Environment != filter.Environment {
			continue
		}
		if filter.Deployment != "" && apply.Deployment != filter.Deployment {
			continue
		}
		if len(filter.States) > 0 && !state.IsState(apply.State, filter.States...) {
			continue
		}
		applies = append(applies, apply)
	}
	return applies
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

func (s *memoryControlRequestStore) GetByOperation(_ context.Context, applyID int64, operation storage.ControlOperation) (*storage.ApplyControlRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, v := range slices.Backward(s.requests) {
		req := v
		if req.ApplyID == applyID && req.Operation == operation {
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
	mu             sync.Mutex
	apply          *storage.Apply
	operations     []*storage.ApplyOperation
	taskStore      *capturingTaskStore
	claimed        bool
	releasedClaims []storage.ApplyLease
	findCh         chan struct{}
	err            error
}

// capture stores a snapshot of the apply, as real storage would materialize a
// row. The caller keeps mutating its own struct after the create returns (for
// example assigning the returned ID), and a concurrently running operator
// reads the captured row — sharing the caller's pointer would race.
func (s *capturingApplyStore) capture(apply *storage.Apply) {
	snapshot := *apply
	s.apply = &snapshot
}

func (s *capturingApplyStore) Create(_ context.Context, apply *storage.Apply) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.capture(apply)
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
	s.capture(apply)
	// Real storage materializes the row with its auto-increment ID; readers
	// resolving the apply by the returned ID must find it.
	s.apply.ID = applyID
	for _, op := range operations {
		snapshot := *op
		s.operations = append(s.operations, &snapshot)
	}
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
	s.capture(apply)
	return nil
}

func (s *capturingApplyStore) ClaimApplyByID(_ context.Context, _ int64, owner string) (*storage.Apply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
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

func (s *capturingApplyStore) Get(_ context.Context, applyID int64) (*storage.Apply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.apply == nil || s.apply.ID != applyID {
		return nil, nil
	}
	apply := *s.apply
	return &apply, nil
}

func (s *capturingApplyStore) ReleaseClaim(_ context.Context, lease storage.ApplyLease) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.apply == nil || s.apply.LeaseToken != lease.Token {
		return false, nil
	}
	s.apply.LeaseOwner = ""
	s.apply.LeaseToken = ""
	s.releasedClaims = append(s.releasedClaims, lease)
	return true, nil
}

// releasedClaimLeases returns the claims handed back through ReleaseClaim.
func (s *capturingApplyStore) releasedClaimLeases() []storage.ApplyLease {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.releasedClaims)
}

func (s *capturingApplyStore) FindNextApplyForStopReconciliation(context.Context, string) (*storage.Apply, error) {
	return nil, nil
}

func (s *capturingApplyStore) FindNextApplyForOperationProjection(context.Context, string) (*storage.Apply, error) {
	return nil, nil
}

func (s *capturingApplyStore) CheckLease(context.Context, storage.ApplyLease) error {
	return nil
}

func (s *capturingApplyStore) ExpireRetryable(context.Context) ([]*storage.RetryableApplyExpiration, error) {
	return nil, nil
}

// queuedOperationLeaseToken is the token this double rotates onto the operation
// row it leases out, standing in for the real store's generated token.
const queuedOperationLeaseToken = "op-lease-token"

// queuedOperationClaimStore serves the operation-level claim ladder over the
// operation rows a dual-write captured in a capturingApplyStore. The cutover
// probe always finds nothing, so every operator tick falls through to the
// operation claim, which signals the apply store's findCh — one observable
// signal per tick — and leases the first captured row exactly once.
type queuedOperationClaimStore struct {
	storage.ApplyOperationStore
	applies    *capturingApplyStore
	mu         sync.Mutex
	claimed    bool
	leaseOwner string
}

func (s *queuedOperationClaimStore) capturedOperation() *storage.ApplyOperation {
	s.applies.mu.Lock()
	defer s.applies.mu.Unlock()
	if len(s.applies.operations) == 0 {
		return nil
	}
	op := *s.applies.operations[0]
	op.ApplyID = 123
	return &op
}

func (s *queuedOperationClaimStore) FindNextApplyOperationCutover(context.Context, string) (*storage.ApplyOperation, error) {
	return nil, nil
}

func (s *queuedOperationClaimStore) FindNextApplyOperation(_ context.Context, owner string) (*storage.ApplyOperation, error) {
	s.applies.mu.Lock()
	findCh := s.applies.findCh
	s.applies.mu.Unlock()
	if findCh != nil {
		select {
		case findCh <- struct{}{}:
		default:
		}
	}

	op := s.capturedOperation()
	s.mu.Lock()
	defer s.mu.Unlock()
	if op == nil || s.claimed {
		return nil, nil
	}
	s.claimed = true
	s.leaseOwner = owner
	op.LeaseOwner = owner
	op.LeaseToken = queuedOperationLeaseToken
	return op, nil
}

// ReleaseClaim hands the operation back the way the real store does: the row
// becomes claimable again, so a later find leases it out afresh.
func (s *queuedOperationClaimStore) ReleaseClaim(_ context.Context, lease storage.OperationLease) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.claimed || lease.Token != queuedOperationLeaseToken {
		return false, nil
	}
	s.claimed = false
	s.leaseOwner = ""
	return true, nil
}

// Get returns the row with whatever lease the claim rotated onto it, the way
// the real store persists it: a driver re-reads the row to confirm its
// operation lease survived the separate transaction that claims the parent
// apply, and a double that dropped the lease on read would look to that driver
// like a peer had rotated it away.
func (s *queuedOperationClaimStore) Get(context.Context, int64) (*storage.ApplyOperation, error) {
	op := s.capturedOperation()
	if op == nil {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claimed {
		op.LeaseOwner = s.leaseOwner
		op.LeaseToken = queuedOperationLeaseToken
	}
	return op, nil
}

func (s *queuedOperationClaimStore) ListByApply(context.Context, int64) ([]*storage.ApplyOperation, error) {
	op := s.capturedOperation()
	if op == nil {
		return nil, nil
	}
	return []*storage.ApplyOperation{op}, nil
}

func (s *queuedOperationClaimStore) Heartbeat(context.Context, int64) error { return nil }

func (s *queuedOperationClaimStore) ReapStranded(context.Context, int) ([]*storage.ReapedOperation, error) {
	return nil, nil
}

type capturingTaskStore struct {
	storage.TaskStore
	mu           sync.Mutex
	tasks        []*storage.Task
	createCalls  int
	updateCalls  int
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

func (s *capturingTaskStore) ReapStrandedActive(context.Context, int) ([]*storage.ReapedTask, error) {
	return nil, nil
}

func (s *capturingTaskStore) ReapStrandedRetryable(context.Context, int) ([]*storage.ReapedTask, error) {
	return nil, nil
}

func (s *capturingTaskStore) Update(_ context.Context, task *storage.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updateCalls++
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

func (s *capturingTaskStore) GetByApplyOperationID(_ context.Context, applyOperationID int64) ([]*storage.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var tasks []*storage.Task
	for _, task := range s.tasks {
		if task.ApplyOperationID != nil && *task.ApplyOperationID == applyOperationID {
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

// windowedApplyLogStore stands in for the real log store's windowing: it holds
// available entries and returns the newest min(available, limit) of them in
// chronological order, recording the limit each read asked for.
type windowedApplyLogStore struct {
	storage.ApplyLogStore
	available int
	lastLimit int
}

func (s *windowedApplyLogStore) List(_ context.Context, filter storage.ApplyLogFilter) ([]*storage.ApplyLog, error) {
	s.lastLimit = filter.Limit
	count := min(s.available, filter.Limit)
	logs := make([]*storage.ApplyLog, 0, count)
	for id := s.available - count + 1; id <= s.available; id++ {
		logs = append(logs, &storage.ApplyLog{ID: int64(id), Level: "info", EventType: "state_transition", Message: fmt.Sprintf("entry %d", id)})
	}
	return logs, nil
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
	planDiffResp             *ternv1.PlanDiffResponse
	planDiffErr              error
	planDiffReq              *ternv1.PlanRequest
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
	logsReqs                 []*ternv1.LogsRequest
	logsHook                 func(*ternv1.LogsRequest) (*ternv1.LogsResponse, error)
	stopResp                 *ternv1.StopResponse
	stopErr                  error
	stopReq                  *ternv1.StopRequest // captured request
	stopHook                 func()
	cancelResp               *ternv1.CancelResponse
	cancelErr                error
	cancelReq                *ternv1.CancelRequest // captured request
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
func (m *mockTernClient) PlanDiff(ctx context.Context, req *ternv1.PlanRequest) (*ternv1.PlanDiffResponse, error) {
	m.planDiffReq = req
	if m.planDiffResp != nil {
		return m.planDiffResp, m.planDiffErr
	}
	return nil, m.planDiffErr
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
func (m *mockTernClient) Logs(_ context.Context, req *ternv1.LogsRequest) (*ternv1.LogsResponse, error) {
	m.logsReqs = append(m.logsReqs, req)
	if m.logsHook != nil {
		return m.logsHook(req)
	}
	return &ternv1.LogsResponse{}, nil
}

// An apply with more history than the requested window returns exactly the
// newest entries asked for and reports that older ones exist. Triage depends on
// that signal: a full window with no marker reads as the whole lifecycle, when
// the part that explains the apply may have scrolled past the limit.
func TestHandleLogsReportsTruncatedWindow(t *testing.T) {
	apply := &storage.Apply{ID: 11, ApplyIdentifier: "apply-control", Database: "orders", DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging"}
	logStore := &windowedApplyLogStore{available: 12}
	service := New(&mockStorageWithApplyStores{
		applies:   &staticApplyStore{apply: apply},
		applyLogs: logStore,
	}, testServerConfig(), nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/logs?apply_id=apply-control&limit=3", nil)
	w := httptest.NewRecorder()
	service.handleLogsWithoutDatabase(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 4, logStore.lastLimit, "the read takes one entry past the window to detect older history")
	var response apitypes.LogsResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	assert.True(t, response.Truncated)
	require.Len(t, response.Logs, 3, "the extra entry is a probe, not part of the window")
	assert.Equal(t, "entry 10", response.Logs[0].Message)
	assert.Equal(t, "entry 12", response.Logs[2].Message)
}

// An apply whose whole history fits the window is reported complete, so the
// truncation marker means something when it does appear.
func TestHandleLogsReportsCompleteWindow(t *testing.T) {
	apply := &storage.Apply{ID: 12, ApplyIdentifier: "apply-control", Database: "orders", DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging"}
	logStore := &windowedApplyLogStore{available: 3}
	service := New(&mockStorageWithApplyStores{
		applies:   &staticApplyStore{apply: apply},
		applyLogs: logStore,
	}, testServerConfig(), nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/logs?apply_id=apply-control&limit=3", nil)
	w := httptest.NewRecorder()
	service.handleLogsWithoutDatabase(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var response apitypes.LogsResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	assert.False(t, response.Truncated)
	require.Len(t, response.Logs, 3)
	assert.Equal(t, "entry 1", response.Logs[0].Message)
}

// Each data-plane source is windowed on its own, so a target with more history
// than the window holds is marked partial without casting doubt on a sibling
// whose logs are complete.
func TestHandleDeploymentLogsReportsPerSourceTruncation(t *testing.T) {
	apply := &storage.Apply{ID: 13, ApplyIdentifier: "apply-control", Database: "commerce", DatabaseType: storage.DatabaseTypeStrata, Environment: "staging"}
	operations := []*storage.ApplyOperation{
		{ApplyID: apply.ID, Deployment: "region-a", OperationKey: "commerce/-80/orders", OperationKind: storage.ApplyOperationKindWork, Target: "cluster-a", ExternalID: "remote-a"},
		{ApplyID: apply.ID, Deployment: "region-a", OperationKey: "commerce/80-/orders", OperationKind: storage.ApplyOperationKindWork, Target: "cluster-b", ExternalID: "remote-b"},
	}
	client := &mockTernClient{isRemote: true}
	client.logsHook = func(req *ternv1.LogsRequest) (*ternv1.LogsResponse, error) {
		count := 1
		if req.ApplyId == "remote-a" {
			// More history than the window holds, including the probe entry.
			count = int(req.Limit)
		}
		resp := &ternv1.LogsResponse{ApplyId: req.ApplyId}
		for id := 1; id <= count; id++ {
			resp.Logs = append(resp.Logs, &ternv1.ApplyLog{Id: int64(id), Level: "info", Message: fmt.Sprintf("entry %d", id), CreatedAt: "2026-07-18T18:33:10Z"})
		}
		return resp, nil
	}
	service := New(&mockStorageWithApplyStores{
		applies:    &staticApplyStore{apply: apply},
		operations: &staticApplyOperationStore{operations: operations},
	}, testServerConfig(), map[string]tern.Client{"region-a/staging": client}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/logs?apply_id=apply-control&deployment=region-a&limit=2", nil)
	w := httptest.NewRecorder()
	service.handleLogsWithoutDatabase(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Len(t, client.logsReqs, 2)
	assert.Equal(t, int32(3), client.logsReqs[0].Limit, "each remote read takes one entry past the window")
	var response apitypes.DeploymentLogsResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	require.Len(t, response.Sources, 2)
	assert.Equal(t, "remote-a", response.Sources[0].ExternalID)
	assert.True(t, response.Sources[0].Truncated)
	require.Len(t, response.Sources[0].Logs, 2)
	assert.Equal(t, "entry 2", response.Sources[0].Logs[0].Message)
	assert.Equal(t, "remote-b", response.Sources[1].ExternalID)
	assert.False(t, response.Sources[1].Truncated)
	require.Len(t, response.Sources[1].Logs, 1)
}

// A data-plane read is capped remotely: a request past the cap is served
// exactly the cap's worth of newest entries, so a window at the cap plus the
// probe would come back indistinguishable from a complete history. The
// handler narrows the window below the cap — an operator viewing a long
// remote apply gets one entry fewer at the cap and an honest partial marker,
// never a partial window presented as the whole lifecycle.
func TestHandleDeploymentLogsKeepsTruncationDetectableAtTheRemoteCap(t *testing.T) {
	apply := &storage.Apply{ID: 14, ApplyIdentifier: "apply-control", Database: "commerce", DatabaseType: storage.DatabaseTypeStrata, Environment: "staging"}
	operations := []*storage.ApplyOperation{
		{ApplyID: apply.ID, Deployment: "region-a", OperationKey: "commerce/-80/orders", OperationKind: storage.ApplyOperationKindWork, Target: "cluster-a", ExternalID: "remote-a"},
	}
	client := &mockTernClient{isRemote: true}
	client.logsHook = func(req *ternv1.LogsRequest) (*ternv1.LogsResponse, error) {
		// The remote serves at most its cap, from a history deeper than any
		// window the handler can request.
		count := min(int(req.Limit), tern.MaxLogsLimit)
		resp := &ternv1.LogsResponse{ApplyId: req.ApplyId}
		for id := 1; id <= count; id++ {
			resp.Logs = append(resp.Logs, &ternv1.ApplyLog{Id: int64(id), Level: "info", Message: fmt.Sprintf("entry %d", id), CreatedAt: "2026-07-18T18:33:10Z"})
		}
		return resp, nil
	}
	service := New(&mockStorageWithApplyStores{
		applies:    &staticApplyStore{apply: apply},
		operations: &staticApplyOperationStore{operations: operations},
	}, testServerConfig(), map[string]tern.Client{"region-a/staging": client}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, fmt.Sprintf("/api/logs?apply_id=apply-control&deployment=region-a&limit=%d", tern.MaxLogsLimit), nil)
	w := httptest.NewRecorder()
	service.handleLogsWithoutDatabase(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Len(t, client.logsReqs, 1)
	assert.Equal(t, int32(tern.MaxLogsLimit), client.logsReqs[0].Limit, "the probe stays within what the remote will serve")
	var response apitypes.DeploymentLogsResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	require.Len(t, response.Sources, 1)
	assert.True(t, response.Sources[0].Truncated, "a window at the remote cap still reports its older entries")
	require.Len(t, response.Sources[0].Logs, tern.MaxLogsLimit-1)
	assert.Equal(t, "entry 2", response.Sources[0].Logs[0].Message)
}

func TestDeploymentLogEntryRejectsMalformedRecords(t *testing.T) {
	_, err := deploymentLogEntry("remote-a", &ternv1.ApplyLog{CreatedAt: "not-a-time", MetadataJson: []byte(`{}`)})
	require.ErrorContains(t, err, "created_at")

	_, err = deploymentLogEntry("remote-a", &ternv1.ApplyLog{CreatedAt: "2026-07-17T01:02:03Z", MetadataJson: []byte(`{`)})
	require.EqualError(t, err, "remote log metadata is not valid JSON")

	taskID := int64(4)
	entry, err := deploymentLogEntry("remote-a", &ternv1.ApplyLog{Id: 3, TaskId: &taskID, Level: "info", EventType: "state", Source: "driver", Message: "ready", OldState: "pending", NewState: "running", MetadataJson: []byte(`{"target":"-80"}`), CreatedAt: "2026-07-17T01:02:03.000000004Z"})
	require.NoError(t, err)
	assert.Equal(t, int64(3), entry.ID)
	assert.Equal(t, &taskID, entry.TaskID)
	assert.JSONEq(t, `{"target":"-80"}`, string(entry.Metadata))
	assert.Equal(t, time.Date(2026, 7, 17, 1, 2, 3, 4, time.UTC), entry.CreatedAt)
}

func TestDeploymentLogErrorIsSanitizedAndIncludesProvenance(t *testing.T) {
	fetch := &deploymentLogFetch{target: "cluster-a", externalID: "remote-a", operations: []*apitypes.LogOperationProvenance{{OperationKey: "commerce/-80/orders", Target: "cluster-a", OperationKind: "work"}}}
	got := deploymentLogError(fetch, status.Error(codes.Unavailable, "dial secret.example:443"))
	assert.Equal(t, "RemoteLogReadFailed", got.Code)
	assert.NotContains(t, got.Message, "secret.example")
	assert.Equal(t, fetch.operations, got.Operations)

	got = deploymentLogError(fetch, status.Error(codes.Unimplemented, "method Logs not implemented"))
	assert.Equal(t, "UnsupportedCapability", got.Code)
	assert.Equal(t, "upgrade_required", got.Reason)

	// The gRPC client wraps Unimplemented with upgrade guidance before it
	// reaches this mapping; detection must see through the wrapping.
	got = deploymentLogError(fetch, fmt.Errorf("selected data plane does not support log reads; upgrade that data plane: %w", status.Error(codes.Unimplemented, "method Logs not implemented")))
	assert.Equal(t, "UnsupportedCapability", got.Code)
	assert.Equal(t, "upgrade_required", got.Reason)

	got = deploymentLogRecordError(fetch)
	assert.Equal(t, "MalformedRemoteLog", got.Code)
	assert.Equal(t, "malformed_remote_log", got.Reason)
	assert.Equal(t, fetch.operations, got.Operations)
}

func TestHandleDeploymentLogsFansOutDeduplicatesAndPreservesPartialResults(t *testing.T) {
	apply := &storage.Apply{ID: 7, ApplyIdentifier: "apply-control", Database: "commerce", DatabaseType: storage.DatabaseTypeStrata, Environment: "staging"}
	operations := []*storage.ApplyOperation{
		{ApplyID: apply.ID, Deployment: "region-a", OperationKey: "commerce/-80/orders", OperationKind: storage.ApplyOperationKindWork, Target: "cluster-a", ExternalID: "remote-a", ExternalOperationID: "101"},
		{ApplyID: apply.ID, Deployment: "region-a", OperationKey: "commerce/-80/customers", OperationKind: storage.ApplyOperationKindWork, Target: "cluster-a", ExternalID: "remote-a", ExternalOperationID: "102"},
		{ApplyID: apply.ID, Deployment: "region-a", OperationKey: "commerce/80-/orders", OperationKind: storage.ApplyOperationKindWork, Target: "cluster-b", ExternalID: "remote-b", ExternalOperationID: "103"},
		{ApplyID: apply.ID, Deployment: "region-a", OperationKey: "commerce/80-/customers", OperationKind: storage.ApplyOperationKindWork, Target: "cluster-c", ExternalID: "remote-c", ExternalOperationID: "104"},
		{ApplyID: apply.ID, Deployment: "region-a", OperationKey: "commerce/finalize", OperationKind: storage.ApplyOperationKindGroupFinalizer, Target: "cluster-a"},
	}
	client := &mockTernClient{isRemote: true}
	client.logsHook = func(req *ternv1.LogsRequest) (*ternv1.LogsResponse, error) {
		switch req.ApplyId {
		case "remote-b":
			return nil, status.Error(codes.Unavailable, "dial private.example:443")
		case "remote-c":
			return &ternv1.LogsResponse{ApplyId: req.ApplyId, Logs: []*ternv1.ApplyLog{{Id: 12, Level: "info", Message: "bad clock", CreatedAt: "not-a-time"}}}, nil
		}
		return &ternv1.LogsResponse{ApplyId: req.ApplyId, Logs: []*ternv1.ApplyLog{{Id: 11, Level: "error", EventType: "engine", Source: "driver", Message: "checksum failed", MetadataJson: []byte(`{"chunk":8}`), CreatedAt: "2026-07-17T01:02:03Z"}}}, nil
	}
	service := New(&mockStorageWithApplyStores{
		applies:    &staticApplyStore{apply: apply},
		operations: &staticApplyOperationStore{operations: operations},
	}, testServerConfig(), map[string]tern.Client{"region-a/staging": client}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/logs?apply_id=apply-control&deployment=region-a&limit=25", nil)
	w := httptest.NewRecorder()
	service.handleLogsWithoutDatabase(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var response apitypes.DeploymentLogsResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	require.Len(t, client.logsReqs, 3)
	assert.Equal(t, "remote-a", client.logsReqs[0].ApplyId)
	assert.Equal(t, "cluster-a", client.logsReqs[0].Target)
	assert.Equal(t, int32(26), client.logsReqs[0].Limit, "the remote read takes one entry past the window")
	assert.Equal(t, "remote-b", client.logsReqs[1].ApplyId)
	assert.Equal(t, "remote-c", client.logsReqs[2].ApplyId)
	require.Len(t, response.Sources, 1)
	assert.Equal(t, "remote-a", response.Sources[0].ExternalID)
	require.Len(t, response.Sources[0].Operations, 2)
	assert.Equal(t, "commerce/-80/orders", response.Sources[0].Operations[0].OperationKey)
	assert.NotContains(t, w.Body.String(), "101")
	assert.NotContains(t, w.Body.String(), "102")
	require.Len(t, response.Sources[0].Logs, 1)
	assert.JSONEq(t, `{"chunk":8}`, string(response.Sources[0].Logs[0].Metadata))
	require.Len(t, response.Errors, 2)
	assert.Equal(t, "remote-b", response.Errors[0].ExternalID)
	assert.Equal(t, "RemoteLogReadFailed", response.Errors[0].Code)
	assert.Equal(t, "remote-c", response.Errors[1].ExternalID)
	assert.Equal(t, "MalformedRemoteLog", response.Errors[1].Code)
	assert.NotContains(t, w.Body.String(), "private.example")
	assert.NotContains(t, w.Body.String(), "not-a-time")
}

func TestHandleDeploymentLogsUsesParentExternalIDForSingleOperation(t *testing.T) {
	apply := &storage.Apply{ID: 8, ApplyIdentifier: "apply-control", ExternalID: "remote-parent", Database: "orders", DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging", Deployment: "region-a"}
	operation := &storage.ApplyOperation{ApplyID: apply.ID, Deployment: "region-a", OperationKey: "schema", OperationKind: storage.ApplyOperationKindWork, Target: "cluster-a"}
	client := &mockTernClient{isRemote: true}
	client.logsHook = func(req *ternv1.LogsRequest) (*ternv1.LogsResponse, error) {
		return &ternv1.LogsResponse{ApplyId: req.ApplyId, Logs: []*ternv1.ApplyLog{{Id: 13, Level: "info", Message: "apply complete", CreatedAt: "2026-07-18T18:33:10Z"}}}, nil
	}
	service := New(&mockStorageWithApplyStores{
		applies:    &staticApplyStore{apply: apply},
		operations: &staticApplyOperationStore{operations: []*storage.ApplyOperation{operation}},
	}, testServerConfig(), map[string]tern.Client{"region-a/staging": client}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/logs?apply_id=apply-control&deployment=region-a", nil)
	w := httptest.NewRecorder()
	service.handleLogsWithoutDatabase(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Len(t, client.logsReqs, 1)
	assert.Equal(t, "remote-parent", client.logsReqs[0].ApplyId)
	var response apitypes.DeploymentLogsResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	require.Len(t, response.Sources, 1)
	assert.Equal(t, "remote-parent", response.Sources[0].ExternalID)
	require.Len(t, response.Sources[0].Logs, 1)
	assert.Equal(t, "apply complete", response.Sources[0].Logs[0].Message)
}

// A remotely dispatched operation whose remote apply id lives only in the
// legacy engine resume context carrier still contributes a data-plane log
// source: the deployment routes to a remote client, so the carrier holds a
// remote apply id, not engine resume state, and its logs must not be silently
// missing from the fan-out.
func TestHandleDeploymentLogsHonorsLegacyResumeContextCarrier(t *testing.T) {
	apply := &storage.Apply{ID: 9, ApplyIdentifier: "apply-control", Database: "commerce", DatabaseType: storage.DatabaseTypeStrata, Environment: "staging"}
	operations := []*storage.ApplyOperation{
		{ApplyID: apply.ID, Deployment: "region-a", OperationKey: "commerce/-80/orders", OperationKind: storage.ApplyOperationKindWork, Target: "cluster-a", ExternalID: "remote-a"},
		{ApplyID: apply.ID, Deployment: "region-a", OperationKey: "commerce/80-/orders", OperationKind: storage.ApplyOperationKindWork, Target: "cluster-a", EngineResumeContext: "remote-legacy"},
	}
	client := &mockTernClient{isRemote: true}
	client.logsHook = func(req *ternv1.LogsRequest) (*ternv1.LogsResponse, error) {
		return &ternv1.LogsResponse{ApplyId: req.ApplyId, Logs: []*ternv1.ApplyLog{{Id: 21, Level: "info", Message: "copying", CreatedAt: "2026-07-18T18:33:10Z"}}}, nil
	}
	service := New(&mockStorageWithApplyStores{
		applies:    &staticApplyStore{apply: apply},
		operations: &staticApplyOperationStore{operations: operations},
	}, testServerConfig(), map[string]tern.Client{"region-a/staging": client}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/logs?apply_id=apply-control&deployment=region-a", nil)
	w := httptest.NewRecorder()
	service.handleLogsWithoutDatabase(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Len(t, client.logsReqs, 2)
	assert.Equal(t, "remote-a", client.logsReqs[0].ApplyId)
	assert.Equal(t, "remote-legacy", client.logsReqs[1].ApplyId)
	var response apitypes.DeploymentLogsResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	require.Len(t, response.Sources, 2)
	assert.Equal(t, "remote-a", response.Sources[0].ExternalID)
	assert.Equal(t, "remote-legacy", response.Sources[1].ExternalID)
	require.Len(t, response.Sources[1].Operations, 1)
	assert.Equal(t, "commerce/80-/orders", response.Sources[1].Operations[0].OperationKey)
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
func (m *mockTernClient) Cancel(ctx context.Context, req *ternv1.CancelRequest) (*ternv1.CancelResponse, error) {
	m.cancelReq = req
	if m.cancelResp != nil {
		return m.cancelResp, m.cancelErr
	}
	return nil, m.cancelErr
}
func (m *mockTernClient) Start(ctx context.Context, req *ternv1.StartRequest) (*ternv1.StartResponse, error) {
	m.startReq = req
	if m.startResp != nil {
		return m.startResp, m.startErr
	}
	return nil, m.startErr
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
	return newQueueApplyTestService(executeApplyTestPlan(), client, applies)
}

func newQueueApplyTestService(plan *storage.Plan, client tern.Client, applies storage.ApplyStore) (*Service, *capturingTaskStore) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	tasks := &capturingTaskStore{}
	var operations storage.ApplyOperationStore
	if capturingApplies, ok := applies.(*capturingApplyStore); ok {
		capturingApplies.taskStore = tasks
		// The operation-claim ladder reads the dual-written operation rows back
		// through the operation store, so expose them from the same capture.
		operations = &queuedOperationClaimStore{applies: capturingApplies}
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
		plans:      &staticPlanStore{plan: plan},
		applies:    applies,
		tasks:      tasks,
		locks:      &emptyLockStore{},
		applyLogs:  &noopApplyLogStore{},
		controls:   &memoryControlRequestStore{},
		operations: operations,
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

// controlTestStores exposes the durable surfaces a control-relay test asserts
// on: the control request a rejection is recorded against, and the apply log an
// operator reads.
type controlTestStores struct {
	controls  *memoryControlRequestStore
	applyLogs *capturingApplyLogStore
}

func newControlTestServiceWithStores(client tern.Client, apply *storage.Apply, tasks []*storage.Task) (*Service, *controlTestStores) {
	stores := &controlTestStores{
		controls:  &memoryControlRequestStore{},
		applyLogs: &capturingApplyLogStore{},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(&mockStorageWithApplyStores{
		applies:   &staticApplyStore{apply: apply},
		tasks:     &capturingTaskStore{tasks: tasks},
		controls:  stores.controls,
		applyLogs: stores.applyLogs,
	}, testServerConfig(), map[string]tern.Client{
		"default/staging": client,
	}, logger)
	return svc, stores
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
				{Namespace: "commerce", Shard: "-80"},
				{Namespace: "commerce", Shard: "80-"},
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
		{Namespace: "commerce", Shard: "-80"},
		{Namespace: "commerce", Shard: "80-"},
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

// A pull with lint requested audits the live schema without planning a
// change: every pulled table runs through the same linters a plan would use,
// and the violations attach per namespace, sorted for a stable response body.
func TestExecutePullSchemaLintsPulledTables(t *testing.T) {
	mockClient := &mockTernClient{
		pullSchemaResp: &ternv1.PullSchemaResponse{
			Database:    "orders",
			Type:        storage.DatabaseTypeMySQL,
			Environment: "production",
			Namespaces: map[string]*ternv1.PulledNamespace{
				"orders": {Tables: map[string]string{
					"users": "CREATE TABLE `users` (`id` int NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=latin1;\n",
				}},
				"audit": {Tables: map[string]string{
					"events": "CREATE TABLE `events` (`id` bigint unsigned NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n",
				}},
			},
			TableCount: 2,
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
		Lint:        true,
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	violations := resp.Namespaces["orders"].Lint
	require.Len(t, violations, 2)
	assert.Equal(t, "allow_charset", violations[0].Linter)
	assert.Equal(t, "users", violations[0].Table)
	assert.Contains(t, violations[0].Message, "latin1")
	assert.Equal(t, "primary_key", violations[1].Linter)
	assert.Equal(t, "users", violations[1].Table)
	assert.Contains(t, violations[1].Message, "int")

	// A linted namespace with no violations reports an explicit empty list,
	// so a clean audit is distinguishable from lint not being requested.
	require.NotNil(t, resp.Namespaces["audit"].Lint)
	assert.Empty(t, resp.Namespaces["audit"].Lint)
	cleanJSON, err := json.Marshal(resp.Namespaces["audit"])
	require.NoError(t, err)
	assert.Contains(t, string(cleanJSON), `"lint":[]`)
}

// Every pulled table entry must be a CREATE TABLE statement: the linters
// silently pass over other statement kinds, so a namespace containing one
// would produce a partial audit. The whole request fails instead — even when
// other namespaces lint cleanly, no partial response is returned.
func TestExecutePullSchemaRejectsNonCreateTableLintEntry(t *testing.T) {
	mockClient := &mockTernClient{
		pullSchemaResp: &ternv1.PullSchemaResponse{
			Database:    "orders",
			Type:        storage.DatabaseTypeMySQL,
			Environment: "production",
			Namespaces: map[string]*ternv1.PulledNamespace{
				"orders": {Tables: map[string]string{
					"users": "DROP TABLE `users`;\n",
				}},
				"audit": {Tables: map[string]string{
					"events": "CREATE TABLE `events` (`id` bigint unsigned NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n",
				}},
			},
			TableCount: 2,
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
		Lint:        true,
	})

	var unlintableErr *unlintablePulledTableError
	require.ErrorAs(t, err, &unlintableErr)
	assert.Equal(t, "orders", unlintableErr.Namespace)
	assert.Equal(t, "users", unlintableErr.Table)
	assert.Contains(t, err.Error(), "expected CREATE TABLE")
	assert.Nil(t, resp, "a failed lint must not return a partial response")
}

// The /api/pull handler maps lint rejections to 400: both an unlintable pulled
// table entry and a lint request against a dialect the linters cannot parse
// are audit-scope defects, not server faults, and must not pollute 5xx rates.
func TestPullSchemaHandlerRejectsLintFailuresAsBadRequest(t *testing.T) {
	tests := []struct {
		name         string
		databaseType string
		client       *mockTernClient
		wantBody     string
	}{
		{
			name:         "non-CREATE table entry",
			databaseType: storage.DatabaseTypeMySQL,
			client: &mockTernClient{
				pullSchemaResp: &ternv1.PullSchemaResponse{
					Database:    "orders",
					Type:        storage.DatabaseTypeMySQL,
					Environment: "production",
					Namespaces: map[string]*ternv1.PulledNamespace{
						"orders": {Tables: map[string]string{"users": "DROP TABLE `users`;\n"}},
					},
					TableCount: 1,
				},
			},
			wantBody: "expected CREATE TABLE",
		},
		{
			name:         "unsupported dialect",
			databaseType: storage.DatabaseTypePostgres,
			client:       &mockTernClient{},
			wantBody:     "schema linting on pull is not supported for postgres databases",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := newTypeVocabularyService(t, &mockStorageWithApplyStores{
				plans:   &staticPlanStore{},
				applies: &staticApplyStore{},
			}, tt.client, "orders", tt.databaseType, "primary")
			mux := http.NewServeMux()
			svc.ConfigureRoutes(mux)

			body := fmt.Sprintf(`{"database":"orders","environment":"production","type":%q,"lint":true}`, tt.databaseType)
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/pull", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			mux.ServeHTTP(w, req)

			assert.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			assert.Contains(t, w.Body.String(), tt.wantBody)
		})
	}
}

// Linting is opt-in: a pull that does not ask for it returns no lint field,
// so existing callers see an unchanged response body.
func TestExecutePullSchemaLintOffByDefault(t *testing.T) {
	mockClient := &mockTernClient{
		pullSchemaResp: &ternv1.PullSchemaResponse{
			Database:    "orders",
			Type:        storage.DatabaseTypeMySQL,
			Environment: "production",
			Namespaces: map[string]*ternv1.PulledNamespace{
				"orders": {Tables: map[string]string{
					"users": "CREATE TABLE `users` (`id` int NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=latin1;\n",
				}},
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
	assert.Nil(t, resp.Namespaces["orders"].Lint)
}

// The schema linters parse MySQL-family DDL only, so a lint request against
// any other dialect fails closed before the pull is dispatched — silently
// returning zero violations would read as a clean audit.
func TestExecutePullSchemaRejectsLintForUnsupportedDialect(t *testing.T) {
	mockClient := &mockTernClient{}
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"ledger": {
				Type: storage.DatabaseTypePostgres,
				Environments: map[string]EnvironmentConfig{
					"production": {Target: "ledger-production", Deployment: "primary"},
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

	_, err := svc.ExecutePullSchema(t.Context(), apitypes.PullSchemaRequest{
		Database:    "ledger",
		Environment: "production",
		Lint:        true,
	})

	var lintDialectErr *unsupportedLintDialectError
	require.ErrorAs(t, err, &lintDialectErr)
	assert.Equal(t, storage.DatabaseTypePostgres, lintDialectErr.DatabaseType)
	assert.Nil(t, mockClient.pullSchemaReq)
}

// Whether pull is supported for a database type is the data plane's answer:
// the control plane dispatches the pull and converts the data plane's
// "unsupported" reply — the local client's typed sentinel, or its gRPC
// mapping (codes.Unimplemented) from a remote deployment — into the typed
// error the HTTP layer renders as 501. Other pull failures pass through
// untouched.
func TestExecutePullSchemaConvertsDataPlaneUnsupported(t *testing.T) {
	tests := []struct {
		name        string
		client      *mockTernClient
		unsupported bool
	}{
		{
			name:        "local client sentinel",
			client:      &mockTernClient{pullSchemaErr: fmt.Errorf("engine does not support schema pull: %w", tern.ErrPullSchemaUnsupportedType)},
			unsupported: true,
		},
		{
			// The gRPC client re-derives the sentinel from the remote data
			// plane's own unsupported verdict, so the remote route surfaces
			// here as a wrapped sentinel just like the local one.
			name:        "remote sentinel re-derived by the gRPC client",
			client:      &mockTernClient{isRemote: true, pullSchemaErr: fmt.Errorf("remote data plane does not support pull schema for database orders: %w", tern.ErrPullSchemaUnsupportedType)},
			unsupported: true,
		},
		{
			// A bare Unimplemented carries no data-plane verdict about the
			// database type — it can come from a proxy mapping an HTTP 404 or
			// a data plane too old to serve the RPC — so it must fail as an
			// ordinary error, never as a 501 capability answer.
			name:        "remote unimplemented without sentinel is not converted",
			client:      &mockTernClient{isRemote: true, pullSchemaErr: status.Error(codes.Unimplemented, "unexpected HTTP status code received from server: 404")},
			unsupported: false,
		},
		{
			name:        "local unimplemented code without sentinel is not converted",
			client:      &mockTernClient{pullSchemaErr: status.Error(codes.Unimplemented, "method PullSchema not implemented")},
			unsupported: false,
		},
		{
			name:        "unrelated pull failure passes through",
			client:      &mockTernClient{pullSchemaErr: errors.New("dial tcp: connection refused")},
			unsupported: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
				"primary/production": tt.client,
			}, logger)

			_, err := svc.ExecutePullSchema(t.Context(), apitypes.PullSchemaRequest{
				Database:    "orders",
				Environment: "production",
				Type:        storage.DatabaseTypeStrata,
			})

			require.Error(t, err)
			var unsupportedErr *unsupportedPullSchemaError
			if tt.unsupported {
				require.ErrorAs(t, err, &unsupportedErr)
				assert.Equal(t, storage.DatabaseTypeStrata, unsupportedErr.DatabaseType)
			} else {
				assert.False(t, errors.As(err, &unsupportedErr), "pull failure must not be misreported as unsupported: %v", err)
			}
		})
	}
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

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/pull", strings.NewReader(`{"database":"OrDeRs","environment":"PrOdUcTiOn","type":"MySQL"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NotNil(t, euClient.pullSchemaReq, "primary deployment should be pulled")
	assert.Equal(t, "orders-eu", euClient.pullSchemaReq.Target)
	assert.Nil(t, usClient.pullSchemaReq, "non-primary deployment must not be pulled")
}

// newTypeVocabularyService configures one database on a single production
// deployment, backed by the given storage and tern client. Server config is
// the one source of truth for the type vocabulary, so the database type can be
// a built-in constant or an embedder-supplied custom type.
func newTypeVocabularyService(t *testing.T, st storage.Storage, client *mockTernClient, database, databaseType, deployment string) *Service {
	t.Helper()
	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			database: {
				Type: databaseType,
				Environments: map[string]EnvironmentConfig{
					"production": {Target: database + "-production", Deployment: deployment},
				},
			},
		},
		TernDeployments: TernConfig{
			deployment: {"production": "tern.example.com:80"},
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	return New(st, cfg, map[string]tern.Client{deployment + "/production": client}, logger)
}

// The declared type is validated against server config — the one source of
// truth for the type vocabulary — so a request for a database configured with
// a custom engine type reaches the data plane, whose verdict then decides the
// outcome.
func TestPullSchemaHandlerAcceptsConfiguredCustomType(t *testing.T) {
	client := &mockTernClient{pullSchemaErr: fmt.Errorf("engine does not support schema pull: %w", tern.ErrPullSchemaUnsupportedType)}
	svc := newTypeVocabularyService(t, &mockStorage{}, client, "orders", "cockroach", "primary")
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/pull", strings.NewReader(`{"database":"orders","environment":"production","type":"cockroach"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotImplemented, w.Code, w.Body.String())
	assert.Contains(t, w.Body.String(), "pull schema is not supported for cockroach databases on this deployment")
	require.NotNil(t, client.pullSchemaReq, "the pull must be dispatched to the data plane")
}

// A declared type that disagrees with the server's configured type for the
// database is a caller defect, rejected as a 400 with both types named.
func TestPullSchemaHandlerRejectsMismatchedTypeAsBadRequest(t *testing.T) {
	client := &mockTernClient{}
	svc := newTypeVocabularyService(t, &mockStorage{}, client, "orders", "cockroach", "primary")
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/pull", strings.NewReader(`{"database":"orders","environment":"production","type":"mysql"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	var resp apitypes.ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Contains(t, resp.Error, `type "mysql" does not match server config type "cockroach"`)
	assert.Nil(t, client.pullSchemaReq, "a mismatched request must not reach the data plane")
}

// A pull request naming a database or environment absent from server config is
// a caller defect: it is rejected as a 400 naming the missing route, never a
// server failure, and never reaches the data plane.
func TestPullSchemaHandlerRejectsUnknownRouteAsBadRequest(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		wantBody string
	}{
		{
			name:     "unknown database",
			body:     `{"database":"nonexistent","environment":"production","type":"cockroach"}`,
			wantBody: `database "nonexistent" is not configured on this server`,
		},
		{
			name:     "unknown environment",
			body:     `{"database":"orders","environment":"sandbox","type":"cockroach"}`,
			wantBody: `database "orders" environment "sandbox" is not configured on this server`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockTernClient{}
			svc := newTypeVocabularyService(t, &mockStorage{}, client, "orders", "cockroach", "primary")
			mux := http.NewServeMux()
			svc.ConfigureRoutes(mux)

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/pull", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			mux.ServeHTTP(w, req)

			assert.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			var resp apitypes.ErrorResponse
			require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
			assert.Contains(t, resp.Error, tt.wantBody)
			assert.Nil(t, client.pullSchemaReq, "an unroutable request must not reach the data plane")
		})
	}
}

// A plan request for a database configured with a custom engine type is
// dispatched to the data plane carrying the config-resolved type: server
// config is the one source of truth for the type vocabulary, and the data
// plane decides whether it can plan the change.
func TestPlanHandlerAcceptsConfiguredCustomType(t *testing.T) {
	client := &mockTernClient{planResp: &ternv1.PlanResponse{PlanId: "plan-custom-type"}}
	plans := &capturingPlanStore{}
	st := &mockStorageWithPlanLookup{plans: plans}
	svc := newTypeVocabularyService(t, st, client, "orders", "cockroach", "primary")
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	body := `{"database":"OrDeRs","environment":"PrOdUcTiOn","type":"Cockroach","repository":"MixedCase/Sample-Repo","schema_files":{"orders":{"files":{"users.sql":"CREATE TABLE users (id bigint primary key)"}}}}`
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/plan", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NotNil(t, client.planReq, "the plan must be dispatched to the data plane")
	assert.Equal(t, "cockroach", client.planReq.Type)
	assert.Equal(t, "orders-production", client.planReq.Target)
	require.NotNil(t, plans.created)
	assert.Equal(t, "mixedcase/sample-repo", plans.created.Repository)
	var resp apitypes.PlanResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "plan-custom-type", resp.PlanID)
}

// A plan request must declare the database type; a declared type that
// disagrees with server config, or a database/environment absent from server
// config, is a caller defect rejected as a 400.
func TestPlanHandlerValidatesTypeAgainstServerConfig(t *testing.T) {
	svc := newTypeVocabularyService(t, &mockStorage{}, &mockTernClient{}, "payments", storage.DatabaseTypeMySQL, DefaultDeployment)
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	tests := []struct {
		name     string
		body     string
		wantCode int
		wantBody string
	}{
		{
			name:     "missing type",
			body:     `{"database":"payments","environment":"production","schema_files":{"payments":{"files":{"users.sql":"CREATE TABLE users (id bigint primary key)"}}}}`,
			wantCode: http.StatusBadRequest,
			wantBody: "type is required",
		},
		{
			name:     "mismatched type",
			body:     `{"database":"payments","environment":"production","type":"vitess","schema_files":{"payments":{"files":{"users.sql":"CREATE TABLE users (id bigint primary key)"}}}}`,
			wantCode: http.StatusBadRequest,
			wantBody: `type "vitess" does not match server config type "mysql"`,
		},
		{
			name:     "unknown database",
			body:     `{"database":"nonexistent","environment":"production","type":"garbage","schema_files":{"payments":{"files":{"users.sql":"CREATE TABLE users (id bigint primary key)"}}}}`,
			wantCode: http.StatusBadRequest,
			wantBody: `database "nonexistent" is not configured on this server`,
		},
		{
			name:     "unknown environment",
			body:     `{"database":"payments","environment":"sandbox","type":"mysql","schema_files":{"payments":{"files":{"users.sql":"CREATE TABLE users (id bigint primary key)"}}}}`,
			wantCode: http.StatusBadRequest,
			wantBody: `database "payments" environment "sandbox" is not configured on this server`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/plan", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			mux.ServeHTTP(w, req)

			assert.Equal(t, tt.wantCode, w.Code, w.Body.String())
			var resp apitypes.ErrorResponse
			require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
			assert.Contains(t, resp.Error, tt.wantBody)
		})
	}
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

	apply, storedApplyID, err := svc.createStoredApply(t.Context(), executeApplyTestPlan(), ApplyRequest{Environment: "staging"}, nil, "apply-fanout")

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
	usersChange := storage.TableChange{Namespace: "commerce", Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", Operation: "alter"}
	plan.Shards = []storage.ShardPlan{
		{Namespace: "commerce", Shard: "80-", Changes: []storage.TableChange{usersChange}},
		{Namespace: "commerce", Shard: "-80", Changes: []storage.TableChange{usersChange}},
		{Namespace: "commerce", Shard: "-"}, // unchanged: no changes, stays out of the apply
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

	apply, storedApplyID, err := svc.createStoredApply(t.Context(), plan, ApplyRequest{Environment: "staging"}, nil, "apply-sharded-fanout")

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
			Artifacts: map[string]string{storage.VSchemaArtifactName: "{\"sharded\":true}"},
			Metadata:  map[string]string{storage.PlanMetadataVSchemaChanged: "true"},
			Tables: []storage.TableChange{
				{Namespace: "commerce", Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", Operation: "alter"},
			},
		},
	}
	usersChange := storage.TableChange{Namespace: "commerce", Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", Operation: "alter"}
	plan.Shards = []storage.ShardPlan{
		{Namespace: "commerce", Shard: "80-", Changes: []storage.TableChange{usersChange}},
		{Namespace: "commerce", Shard: "-80", Changes: []storage.TableChange{usersChange}},
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

	_, _, err := svc.createStoredApply(t.Context(), plan, ApplyRequest{Environment: "staging"}, nil, "apply-sharded-finalizer")

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
			Artifacts: map[string]string{storage.VSchemaArtifactName: "{\"routing\":true}"},
			Metadata:  map[string]string{storage.PlanMetadataVSchemaChanged: "true"},
		},
	}
	plan.Shards = []storage.ShardPlan{{Namespace: "commerce", Shard: "-", Changes: []storage.TableChange{{Namespace: "commerce", Table: "users", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)", Operation: "alter"}}}}
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

	_, _, err := svc.createStoredApply(t.Context(), plan, ApplyRequest{Environment: "staging"}, nil, "apply-finalizer-only-namespace")

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

	// The type filter validates against the types present in server config, so
	// a type with no configured databases — built-in or not — is rejected with
	// the configured vocabulary named, instead of silently returning an empty
	// list for a filter that can never match.
	req = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/databases?type=strata", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	assert.Contains(t, w.Body.String(), `type \"strata\" matches no configured database type`)
	assert.Contains(t, w.Body.String(), "mysql, vitess")

	// A name filter is a case-insensitive substring match, so a family
	// prefix selects every shard-style database that contains it.
	req = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/databases?name=ORD", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Len(t, resp.Databases, 1)
	assert.Equal(t, "orders", resp.Databases[0].Database)

	req = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/databases?name=count", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Len(t, resp.Databases, 1)
	assert.Equal(t, "accounts", resp.Databases[0].Database)

	req = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/databases?type=mysql&name=accounts", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Empty(t, resp.Databases)

	req = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/databases?type=mysql&name=orders", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Len(t, resp.Databases, 1)
	assert.Equal(t, "orders", resp.Databases[0].Database)

	req = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/databases?name=nomatch", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Empty(t, resp.Databases)

	req = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/databases?type=cockroach", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	assert.Contains(t, w.Body.String(), `type \"cockroach\" matches no configured database type`)
	assert.Contains(t, w.Body.String(), "mysql, vitess")
}

// A server configured with a custom engine type accepts that type as a list
// filter: the filter vocabulary comes from server config, not a fixed list.
func TestDatabaseListFiltersByConfiguredCustomType(t *testing.T) {
	svc := newTypeVocabularyService(t, &mockStorage{}, &mockTernClient{}, "orders", "cockroach", "primary")
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/databases?type=cockroach", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var resp apitypes.DatabaseListResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Len(t, resp.Databases, 1)
	assert.Equal(t, "orders", resp.Databases[0].Database)
	assert.Equal(t, "cockroach", resp.Databases[0].Type)
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
	}, "", "")

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

func TestExecuteApplyGatesDeferredCutoverByDatabaseType(t *testing.T) {
	tests := []struct {
		name         string
		databaseType string
		options      map[string]string
		wantErr      string
	}{
		{name: "postgres without deferred cutover", databaseType: storage.DatabaseTypePostgres},
		{name: "postgres with deferred cutover", databaseType: storage.DatabaseTypePostgres, options: map[string]string{"defer_cutover": "true"}, wantErr: `database "testdb": deferred cutover is not supported for database_type: postgres`},
		{name: "mysql with deferred cutover", databaseType: storage.DatabaseTypeMySQL, options: map[string]string{"defer_cutover": "true"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := executeApplyTestPlan()
			plan.DatabaseType = tt.databaseType
			applies := &capturingApplyStore{}
			svc, _ := newQueueApplyTestService(plan, &mockTernClient{}, applies)

			resp, applyID, err := svc.ExecuteApply(t.Context(), ApplyRequest{
				PlanID:      plan.PlanIdentifier,
				Environment: plan.Environment,
				Options:     tt.options,
			})

			if tt.wantErr != "" {
				assert.EqualError(t, err, tt.wantErr)
				assert.Nil(t, resp)
				assert.Zero(t, applyID)
				assert.Nil(t, applies.apply)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.True(t, resp.Accepted)
			assert.Equal(t, int64(123), applyID)
			require.NotNil(t, applies.apply)
			assert.Equal(t, tt.databaseType, applies.apply.DatabaseType)
			assert.Equal(t, tt.options["defer_cutover"] == "true", applies.apply.GetOptions().DeferCutover)
		})
	}
}

func TestExecuteApplyRejectsUnsafeStoredPlanWithoutOptIn(t *testing.T) {
	plan := executeApplyTestPlan()
	plan.Namespaces["testdb"].Tables[0].IsUnsafe = true
	plan.Namespaces["testdb"].Tables[0].UnsafeReason = "DROP COLUMN removes data"

	applies := &capturingApplyStore{}
	tasks := &capturingTaskStore{}
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
	assert.Contains(t, err.Error(), "DROP COLUMN removes data")
	assert.Nil(t, applies.apply)
	assert.Empty(t, tasks.tasks)
}

func TestExecuteApplyRejectsStoredTableDropWithoutOptIn(t *testing.T) {
	plan := executeApplyTestPlan()
	plan.Namespaces["testdb"].Tables[0].Table = "users"
	plan.Namespaces["testdb"].Tables[0].DDL = "DROP TABLE `users`"
	plan.Namespaces["testdb"].Tables[0].Operation = "drop"

	applies := &capturingApplyStore{}
	tasks := &capturingTaskStore{}
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
	assert.Contains(t, err.Error(), "DROP TABLE removes all data")
	assert.Nil(t, applies.apply)
	assert.Empty(t, tasks.tasks)
}

// TestExecuteApplyRejectsBlockedStoredPlan proves a stored plan carrying an
// engine-blocked change never queues an apply: the drive layer rebuilds
// engine requests without the verdict, so this gate is what keeps a
// planner-refused statement from executing.
func TestExecuteApplyRejectsBlockedStoredPlan(t *testing.T) {
	plan := executeApplyTestPlan()
	plan.Namespaces["testdb"].Tables[0].ExecutionMode = "blocked"
	plan.Namespaces["testdb"].Tables[0].ModeReason = `statement for table "users" is refused: it cannot be executed safely as written`

	applies := &capturingApplyStore{}
	tasks := &capturingTaskStore{}
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
	assert.Contains(t, err.Error(), "blocked change")
	assert.Contains(t, err.Error(), "cannot be executed safely as written")
	assert.Nil(t, applies.apply)
	assert.Empty(t, tasks.tasks)
}

func TestExecuteApplyQueuesUnsafeStoredPlanWithOptIn(t *testing.T) {
	plan := executeApplyTestPlan()
	plan.Namespaces["testdb"].Tables[0].IsUnsafe = true
	plan.Namespaces["testdb"].Tables[0].UnsafeReason = "DROP COLUMN removes data"

	applies := &capturingApplyStore{}
	tasks := &capturingTaskStore{}
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
		Options:     map[string]string{"allow_unsafe": "true"},
	})

	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.True(t, resp.Accepted)
	assert.Equal(t, int64(123), applyID)
	require.NotNil(t, applies.apply)
	require.Len(t, tasks.tasks, 1)
	assert.True(t, applies.apply.GetOptions().AllowUnsafe)
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
			{ID: opAID, ApplyID: apply.ID, Deployment: "deploy-a", ExternalID: "remote-apply-a", ExternalOperationID: "remote-operation-a", OperationKind: storage.ApplyOperationKindWork, Target: "target-a", State: state.ApplyOperation.Running, StartedAt: &startedAt},
			{ID: opBID, ApplyID: apply.ID, Deployment: "deploy-b", OperationKind: storage.ApplyOperationKindGroupFinalizer, Target: "target-b", State: state.ApplyOperation.Failed, ErrorMessage: "engine failed", StartedAt: &startedAt, CompletedAt: &completedAt},
		}},
	}, testServerConfig(), nil, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))

	resp, err := svc.progressFromLocalStorage(t.Context(), apply)

	require.NoError(t, err)
	require.Len(t, resp.Operations, 2)
	assert.Equal(t, "deploy-a", resp.Operations[0].Deployment)
	assert.Equal(t, "remote-apply-a", resp.Operations[0].ExternalID)
	assert.Equal(t, "remote-operation-a", resp.Operations[0].ExternalOperationID)
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

// A task row can sit in an active state under an apply that has already reached
// a verdict: a driver settled the apply and exited without closing the row, or
// one failed task settled the apply while its siblings kept copying. The reader
// cannot tell those apart, so it reports every task exactly as stored and never
// writes. Repairing a genuinely stranded row belongs to a writer that can hold
// a lease and wait out the parent's quiescence window, and a GET does neither.
func TestProgressFromLocalStorageReportsActiveTaskUnderTerminalApplyVerbatim(t *testing.T) {
	terminalStates := []string{state.Apply.Completed, state.Apply.Failed, state.Apply.Cancelled, state.Apply.Reverted, state.Apply.Stopped}
	for _, applyState := range terminalStates {
		t.Run(applyState, func(t *testing.T) {
			apply := &storage.Apply{ID: 40, ApplyIdentifier: "apply_stranded", Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging", Engine: storage.EngineSpirit, State: applyState, ExternalID: "remote-apply"}
			tasks := &capturingTaskStore{tasks: []*storage.Task{
				{ApplyID: apply.ID, TaskIdentifier: "task_users", TableName: "users", Namespace: "testdb", DDLAction: "alter", State: state.Task.Running, ProgressPercent: 42, Database: "testdb", DatabaseType: storage.DatabaseTypeMySQL, Engine: storage.EngineSpirit, Environment: "staging"},
			}}
			svc := New(&mockStorageWithApplyStores{tasks: tasks, operations: &staticApplyOperationStore{}},
				testServerConfig(), nil, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))

			resp, err := svc.progressFromLocalStorage(t.Context(), apply)

			require.NoError(t, err)
			require.Len(t, resp.Tables, 1)
			assert.Equal(t, state.Task.Running, resp.Tables[0].Status, "the task's stored state is reported as stored, never rewritten to the apply's verdict")
			assert.Equal(t, int32(42), resp.Tables[0].PercentComplete)
			assert.Equal(t, "users", resp.Tables[0].TableName)
			assert.Equal(t, state.Task.Running, tasks.tasks[0].State, "stored task row must be untouched by a read")
			assert.Zero(t, tasks.updateCalls, "reading progress must not write task rows")
		})
	}
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
	assert.Equal(t, map[string]int{
		state.Apply.Completed: 2,
		state.Apply.Running:   1,
		state.Apply.Failed:    1,
	}, resp.StateCounts, "state counts cover every matching apply, not just the truncated page")
	require.Len(t, resp.Applies, 2)
	assert.Equal(t, "apply-one", resp.Applies[0].ApplyID)
	assert.Equal(t, "external-one", resp.Applies[0].ExternalID)
	assert.Equal(t, "apply-two", resp.Applies[1].ApplyID)
}

func TestHandleStatusDeploymentFilterProjectsMatchingOperation(t *testing.T) {
	now := time.Now().UTC()
	startedAt := now.Add(-time.Minute)
	applies := &recentApplyStore{
		applies: []*storage.Apply{
			{
				ID:              101,
				ApplyIdentifier: "apply-deployment",
				ExternalID:      "parent-external",
				Database:        "orders",
				Environment:     "staging",
				Deployment:      "deploy-a",
				Engine:          storage.EngineSpirit,
				State:           state.Apply.Running,
				Caller:          "cli",
				CreatedAt:       now,
				UpdatedAt:       now,
			},
		},
	}
	operations := &staticApplyOperationStore{
		operations: []*storage.ApplyOperation{
			{
				ID:                  202,
				ApplyID:             101,
				Deployment:          "deploy-a",
				ExternalID:          "remote-apply-202",
				ExternalOperationID: "remote-operation-202",
				State:               state.Apply.Completed,
				StartedAt:           &startedAt,
				CreatedAt:           now,
				UpdatedAt:           now,
			},
		},
	}
	stor := &mockStorageWithApplyStores{applies: applies, operations: operations}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(stor, testServerConfig(), nil, logger)
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status?environment=staging&deployment=deploy-a", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var resp apitypes.StatusResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

	require.Len(t, applies.filters, 1)
	assert.Equal(t, "staging", applies.filters[0].Environment)
	assert.Equal(t, "deploy-a", applies.filters[0].Deployment)
	assert.Equal(t, 0, resp.ActiveCount)
	require.Len(t, resp.Applies, 1)
	assert.Equal(t, "apply-deployment", resp.Applies[0].ApplyID)
	assert.Equal(t, "remote-apply-202", resp.Applies[0].ExternalID)
	assert.Equal(t, "remote-operation-202", resp.Applies[0].ExternalOperationID)
	assert.Equal(t, "deploy-a", resp.Applies[0].Deployment)
	assert.Equal(t, state.Apply.Completed, resp.Applies[0].State)
}

// A deployment applied per shard has exactly one data-plane apply, so the
// deployment-filtered status list surfaces exactly one remote apply handle per
// row: the shared id the operations recorded, or the parent apply row's when
// the drive recorded it there instead (a drive that is not operation-scoped
// writes the remote id to the parent). Per-operation remote ids surface only
// when the filter matches a single operation; a fold keeps them in the detail
// views.
func TestHandleStatusDeploymentFilterRemoteHandles(t *testing.T) {
	now := time.Now().UTC()
	startedAt := now.Add(-2 * time.Minute)
	completedAt := now.Add(-time.Minute)
	completedOp := func(externalID, externalOperationID string) *storage.ApplyOperation {
		return &storage.ApplyOperation{
			ID:                  202,
			ApplyID:             101,
			Deployment:          "deploy-a",
			ExternalID:          externalID,
			ExternalOperationID: externalOperationID,
			State:               state.Apply.Completed,
			StartedAt:           &startedAt,
			CompletedAt:         &completedAt,
			CreatedAt:           now.Add(-2 * time.Minute),
			UpdatedAt:           completedAt,
		}
	}
	runningOp := func(externalID, externalOperationID string) *storage.ApplyOperation {
		return &storage.ApplyOperation{
			ID:                  203,
			ApplyID:             101,
			Deployment:          "deploy-a",
			ExternalID:          externalID,
			ExternalOperationID: externalOperationID,
			State:               state.Apply.Running,
			StartedAt:           &startedAt,
			CreatedAt:           now.Add(-90 * time.Second),
			UpdatedAt:           now,
		}
	}

	cases := []struct {
		name             string
		parentExternalID string
		applyState       string
		operations       []*storage.ApplyOperation
		wantActiveCount  int
		wantState        string
		wantExternalID   string
		wantExternalOpID string
	}{
		{
			name:             "a fold surfaces the shared data-plane apply id and no per-operation id",
			applyState:       state.Apply.Running,
			operations:       []*storage.ApplyOperation{completedOp("remote-apply-shared", "remote-operation-202"), runningOp("remote-apply-shared", "remote-operation-203")},
			wantActiveCount:  1,
			wantState:        state.Apply.Running,
			wantExternalID:   "remote-apply-shared",
			wantExternalOpID: "",
		},
		{
			name:             "a fold whose operations recorded no remote id keeps the parent apply row's",
			parentExternalID: "parent-external",
			applyState:       state.Apply.Completed,
			operations:       []*storage.ApplyOperation{completedOp("", "remote-operation-202"), runningOp("", "remote-operation-203")},
			wantActiveCount:  1,
			wantState:        state.Apply.Running,
			wantExternalID:   "parent-external",
			wantExternalOpID: "",
		},
		{
			name:             "a single matching operation without a remote id keeps the parent apply row's",
			parentExternalID: "parent-external",
			applyState:       state.Apply.Running,
			operations:       []*storage.ApplyOperation{runningOp("", "")},
			wantActiveCount:  1,
			wantState:        state.Apply.Running,
			wantExternalID:   "parent-external",
			wantExternalOpID: "",
		},
		{
			name:             "a single matching operation's own remote ids win over the parent's",
			parentExternalID: "parent-external",
			applyState:       state.Apply.Completed,
			operations:       []*storage.ApplyOperation{completedOp("remote-apply-own", "remote-operation-202")},
			wantActiveCount:  0,
			wantState:        state.Apply.Completed,
			wantExternalID:   "remote-apply-own",
			wantExternalOpID: "remote-operation-202",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			applies := &recentApplyStore{
				applies: []*storage.Apply{
					{
						ID:              101,
						ApplyIdentifier: "apply-deployment",
						ExternalID:      tc.parentExternalID,
						Database:        "orders",
						Environment:     "staging",
						Deployment:      "deploy-a",
						Engine:          storage.EngineSpirit,
						State:           tc.applyState,
						Caller:          "cli",
						CreatedAt:       now,
						UpdatedAt:       now,
					},
				},
			}
			operations := &staticApplyOperationStore{operations: tc.operations}
			stor := &mockStorageWithApplyStores{applies: applies, operations: operations}
			logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
			svc := New(stor, testServerConfig(), nil, logger)
			mux := http.NewServeMux()
			svc.ConfigureRoutes(mux)

			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status?environment=staging&deployment=deploy-a", nil)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)

			require.Equal(t, http.StatusOK, w.Code)
			var resp apitypes.StatusResponse
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

			assert.Equal(t, tc.wantActiveCount, resp.ActiveCount)
			require.Len(t, resp.Applies, 1)
			assert.Equal(t, "apply-deployment", resp.Applies[0].ApplyID)
			assert.Equal(t, tc.wantExternalID, resp.Applies[0].ExternalID)
			assert.Equal(t, tc.wantExternalOpID, resp.Applies[0].ExternalOperationID)
			assert.Equal(t, "deploy-a", resp.Applies[0].Deployment)
			assert.Equal(t, tc.wantState, resp.Applies[0].State)
		})
	}
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

func TestParseStatusLast(t *testing.T) {
	for _, tt := range []struct {
		name      string
		target    string
		want      time.Duration
		wantError bool
	}{
		{name: "default", target: "/api/status"},
		{name: "hours", target: "/api/status?last=24h", want: 24 * time.Hour},
		{name: "minutes", target: "/api/status?last=30m", want: 30 * time.Minute},
		{name: "zero", target: "/api/status?last=0s", wantError: true},
		{name: "negative", target: "/api/status?last=-1h", wantError: true},
		{name: "not a duration", target: "/api/status?last=yesterday", wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, tt.target, nil)
			got, err := parseStatusLast(req)
			if tt.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestHandleStatusLastWindowBoundsFilter verifies that the `last` query
// parameter becomes an UpdatedSince bound on the storage filter and is echoed
// back in the response, and that an invalid window is rejected rather than
// silently ignored.
func TestHandleStatusLastWindowBoundsFilter(t *testing.T) {
	applies := &recentApplyStore{}
	stor := &mockStorageWithApplyStores{applies: applies}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(stor, testServerConfig(), nil, logger)
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status?last=24h", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var resp apitypes.StatusResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "24h0m0s", resp.Last)

	require.Len(t, applies.filters, 1)
	assert.WithinDuration(t, time.Now().Add(-24*time.Hour), applies.filters[0].UpdatedSince, time.Minute)

	req = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status?last=yesterday", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	require.Len(t, applies.filters, 1, "an invalid window must not reach storage")
}

// TestHandleStatusActiveFilter verifies that the `active` query parameter reaches
// storage as the not-terminal restriction, so a caller asking whether a target is
// busy is answered from in-flight work instead of paging settled history, and
// that a non-boolean value is rejected rather than silently ignored.
func TestHandleStatusActiveFilter(t *testing.T) {
	applies := &recentApplyStore{}
	stor := &mockStorageWithApplyStores{applies: applies}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(stor, testServerConfig(), nil, logger)
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status?active=true", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Len(t, applies.filters, 1)
	assert.True(t, applies.filters[0].ActiveOnly)
	assert.Empty(t, applies.filters[0].States, "active is expressed as not-terminal, not as a list of live states")

	req = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.Len(t, applies.filters, 2)
	assert.False(t, applies.filters[1].ActiveOnly)

	req = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status?active=sometimes", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	require.Len(t, applies.filters, 2, "an invalid active value must not reach storage")
}

// TestHandleStatusStateFilter verifies that the `state` query parameter
// normalizes to the canonical apply state and restricts the storage filter,
// that a typo'd state is rejected rather than returning a silently empty
// list, and that `state` cannot be combined with `failed`.
func TestHandleStatusStateFilter(t *testing.T) {
	now := time.Now().UTC()
	applies := &recentApplyStore{
		applies: []*storage.Apply{
			{
				ApplyIdentifier: "apply-running",
				Database:        "orders",
				Environment:     "staging",
				Engine:          storage.EngineSpirit,
				State:           state.Apply.Running,
				Caller:          "cli",
				CreatedAt:       now,
				UpdatedAt:       now,
			},
			{
				ApplyIdentifier: "apply-completed",
				Database:        "payments",
				Environment:     "staging",
				Engine:          storage.EngineSpirit,
				State:           state.Apply.Completed,
				Caller:          "cli",
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

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status?state=RUNNING", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var resp apitypes.StatusResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, state.Apply.Running, resp.State, "the state filter echoes in canonical form")
	require.Len(t, resp.Applies, 1)
	assert.Equal(t, "apply-running", resp.Applies[0].ApplyID)
	assert.Equal(t, map[string]int{state.Apply.Running: 1}, resp.StateCounts)
	require.Len(t, applies.filters, 1)
	assert.Equal(t, []string{state.Apply.Running}, applies.filters[0].States)

	req = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status?state=comleted", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "unknown state")
	require.Len(t, applies.filters, 1, "an unknown state must not reach storage")

	req = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/status?state=running&failed=true", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "state cannot be combined with failed")
	require.Len(t, applies.filters, 1, "conflicting filters must not reach storage")
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

// TestLivez verifies the liveness endpoint reflects process health only: it
// reports alive even when the storage database is unreachable, so a storage
// outage pulls instances from the Service (readiness) instead of restarting
// them and aborting in-flight schema changes.
func TestLivez(t *testing.T) {
	t.Run("alive", func(t *testing.T) {
		svc := newTestService()
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		req := httptest.NewRequestWithContext(t.Context(), "GET", "/livez", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var resp map[string]string
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err, "failed to decode response")
		assert.Equal(t, "alive", resp["status"])
	})

	t.Run("alive while storage is unreachable", func(t *testing.T) {
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		svc := New(&mockStorage{pingErr: errors.New("connection refused")}, testServerConfig(), nil, logger)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		req := httptest.NewRequestWithContext(t.Context(), "GET", "/livez", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var resp map[string]string
		err := json.NewDecoder(w.Body).Decode(&resp)
		require.NoError(t, err, "failed to decode response")
		assert.Equal(t, "alive", resp["status"])
	})
}

func TestServiceClose(t *testing.T) {
	svc := newTestService()
	assert.NoError(t, svc.Close())
}

func TestApplyHandler(t *testing.T) {
	t.Run("returns bad request for unsupported apply feature", func(t *testing.T) {
		plan := executeApplyTestPlan()
		plan.DatabaseType = storage.DatabaseTypePostgres
		applies := &capturingApplyStore{}
		svc, _ := newQueueApplyTestService(plan, &mockTernClient{}, applies)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"plan_id":"plan-1","environment":"staging","options":{"defer_cutover":"true"}}`
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/apply", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
		var resp apitypes.ErrorResponse
		require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
		assert.Equal(t, apitypes.ErrCodeInvalidRequest, resp.ErrorCode)
		assert.Equal(t, `apply rejected: database "testdb": deferred cutover is not supported for database_type: postgres`, resp.Error)
		assert.Nil(t, applies.apply)
	})

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

		body := `{"plan_id": "plan-old-direct", "environment": "StAgInG"}`
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
		assert.Equal(t, "staging", applies.apply.Environment)
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

func TestRollbackPlanCanonicalizesEnvironment(t *testing.T) {
	now := time.Now().UTC()
	apply := rollbackGuardrailApply("apply_latest", 1, 10, now)
	svc := newRollbackGuardrailService(apply, rollbackGuardrailPlan(10, true), []*storage.Task{
		rollbackGuardrailTask(1, 10, now),
	})
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/rollback/plan",
		strings.NewReader(`{"apply_id":"apply_latest","environment":"StAgInG"}`))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.NotEqual(t, http.StatusBadRequest, w.Code, w.Body.String())
	assert.NotContains(t, w.Body.String(), "belongs to environment")
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

		body := `{"environment": "StAgInG", "apply_id": "apply-abc123"}`
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

	t.Run("rejects planetscale stop before queuing durable intent", func(t *testing.T) {
		mock := &mockTernClient{stopResp: &ternv1.StopResponse{Accepted: true, StoppedCount: 1}}
		apply := activeTestApply("apply-planetscale-stop")
		apply.DatabaseType = storage.DatabaseTypeVitess
		apply.Engine = storage.EnginePlanetScale
		task := &storage.Task{ID: 31, TaskIdentifier: "task-planetscale-stop", ApplyID: apply.ID, State: state.Task.Running}
		svc := newControlTestServiceWithTasks(mock, apply, []*storage.Task{task})
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := `{"environment": "staging", "apply_id": "apply-planetscale-stop"}`
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/stop", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
		assert.Contains(t, w.Body.String(), "use cancel")
		assert.Nil(t, mock.stopReq)
		controlReq, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationStop)
		require.NoError(t, err)
		assert.Nil(t, controlReq)
	})
}

func TestCancelHandler(t *testing.T) {
	mock := &mockTernClient{cancelResp: &ternv1.CancelResponse{Accepted: true, CancelledCount: 1}}
	apply := activeTestApply("apply-cancel-abc123")
	task := &storage.Task{
		ID:             24,
		TaskIdentifier: "task-cancel-abc123",
		ApplyID:        apply.ID,
		State:          state.Task.Running,
	}
	svc := newControlTestServiceWithTasks(mock, apply, []*storage.Task{task})
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	body := `{"environment": "staging", "apply_id": "apply-cancel-abc123"}`
	req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/cancel", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NotNil(t, mock.cancelReq, "handler should persist durable intent and issue immediate cancel")
	assert.Equal(t, "apply-cancel-abc123", mock.cancelReq.ApplyId)
	assert.Equal(t, "staging", mock.cancelReq.Environment)
	controlReq, err := svc.storage.ControlRequests().GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
	require.NoError(t, err)
	require.NotNil(t, controlReq)

	var resp apitypes.CancelResponse
	err = json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	assert.Equal(t, int64(1), resp.CancelledCount)
	assert.Empty(t, resp.Status)
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
				name:        "paused",
				applyState:  state.Apply.Paused,
				wantMessage: "paused after a failed deployment; use release",
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

	// A stopped apply whose unfinished work another apply took over is refused at
	// the request, not after a queued start reaches a driver: starting it would
	// replay its statements against a target where that work already happened.
	// The refusal names the successor so the operator knows where the work went.
	t.Run("rejects start for an apply whose work was taken over", func(t *testing.T) {
		mock := &mockTernClient{}
		apply := activeTestApply("apply-superseded-holder")
		apply.State = state.Apply.Stopped
		apply.SupersededBy = "apply-superseded-successor"
		svc := newControlTestServiceWithTasks(mock, apply, nil)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		body := fmt.Sprintf(`{"environment": "staging", "apply_id": %q}`, apply.ApplyIdentifier)
		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/start", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusConflict, w.Code, w.Body.String())
		assert.Contains(t, w.Body.String(), "superseded by apply-superseded-successor")
		assert.Nil(t, mock.startReq, "a superseded apply must not reach the engine")
		assert.Nil(t, mock.progressReq)
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
		// Revert is only accepted while the apply is in its revert window.
		apply := activeTestApply("apply-rev123")
		apply.State = state.Apply.RevertWindow
		apply.Engine = storage.EnginePlanetScale
		apply.DatabaseType = storage.DatabaseTypeVitess
		svc := newControlTestService(mock, apply)
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
		// Skip-revert is only accepted while the apply is in its revert window.
		apply := activeTestApply("apply-skip456")
		apply.State = state.Apply.RevertWindow
		apply.Engine = storage.EnginePlanetScale
		apply.DatabaseType = storage.DatabaseTypeVitess
		svc := newControlTestService(mock, apply)
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

	// Skip-revert is PlanetScale-only; a non-PlanetScale apply is rejected before
	// any durable request is recorded, so it can't accumulate a stuck request.
	t.Run("rejects a non-PlanetScale apply", func(t *testing.T) {
		mock := &mockTernClient{skipRevertResp: &ternv1.SkipRevertResponse{Accepted: true}}
		apply := activeTestApply("apply-skip-mysql")
		apply.State = state.Apply.RevertWindow
		apply.Engine = storage.EngineSpirit
		svc := newControlTestService(mock, apply)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		req := httptest.NewRequestWithContext(t.Context(), "POST", "/api/skip-revert", strings.NewReader(`{"environment":"staging","apply_id":"apply-skip-mysql"}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusConflict, w.Code, w.Body.String())
		assert.Contains(t, w.Body.String(), "only supported for PlanetScale")
		assert.Nil(t, mock.skipRevertReq, "no engine call for a rejected non-PlanetScale apply")
	})

	// When a skip-revert request is already pending (e.g. the first immediate
	// attempt failed and left it queued for the apply owner), a second request is
	// accepted as already_requested and does not re-drive the engine.
	t.Run("already pending does not re-drive the engine", func(t *testing.T) {
		// The immediate attempt fails, so the durable request stays pending.
		mock := &mockTernClient{skipRevertErr: fmt.Errorf("transient data-plane error")}
		apply := activeTestApply("apply-skip-dup")
		apply.State = state.Apply.RevertWindow
		apply.Engine = storage.EnginePlanetScale
		apply.DatabaseType = storage.DatabaseTypeVitess
		svc := newControlTestService(mock, apply)
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)
		body := `{"environment":"staging","apply_id":"apply-skip-dup"}`

		first := httptest.NewRecorder()
		req1 := httptest.NewRequestWithContext(t.Context(), "POST", "/api/skip-revert", strings.NewReader(body))
		req1.Header.Set("Content-Type", "application/json")
		mux.ServeHTTP(first, req1)
		require.Equal(t, http.StatusAccepted, first.Code, first.Body.String())
		require.NotNil(t, mock.skipRevertReq, "first request attempts the engine and leaves the request pending")

		mock.skipRevertReq = nil
		second := httptest.NewRecorder()
		req2 := httptest.NewRequestWithContext(t.Context(), "POST", "/api/skip-revert", strings.NewReader(body))
		req2.Header.Set("Content-Type", "application/json")
		mux.ServeHTTP(second, req2)

		assert.Equal(t, http.StatusAccepted, second.Code, second.Body.String())
		assert.Contains(t, second.Body.String(), apitypes.ControlStatusAlreadyRequested)
		assert.Nil(t, mock.skipRevertReq, "a duplicate request must not re-drive the engine")
	})
}

// A remote deployment names the schema change by its own data-plane identifier,
// which resolves to nothing on the control plane. Wherever a rejection is
// durably recorded or shown to an operator — a failed control request, an apply
// log line — it must name the identifier the operator addressed, so the record
// reads as an answer about their schema change rather than an internal id.
func TestControlRejectionsNameTheOperatorApplyID(t *testing.T) {
	const (
		operatorID = "apply-op7788"
		remoteID   = "apply-remote999"
	)

	remoteRevertWindowApply := func(applyID string) *storage.Apply {
		apply := activeTestApply(applyID)
		apply.State = state.Apply.RevertWindow
		apply.Engine = storage.EnginePlanetScale
		apply.DatabaseType = storage.DatabaseTypeVitess
		apply.ExternalID = remoteID
		return apply
	}

	assertOperatorFacing := func(t *testing.T, message, context string) {
		t.Helper()
		assert.Contains(t, message, operatorID, context)
		assert.NotContains(t, message, remoteID, context)
	}

	postControl := func(t *testing.T, svc *Service, path, applyID string) *httptest.ResponseRecorder {
		t.Helper()
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)
		body := fmt.Sprintf(`{"environment":"staging","apply_id":%q}`, applyID)
		req := httptest.NewRequestWithContext(t.Context(), "POST", path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		return w
	}

	t.Run("a rejected revert is recorded against the operator apply id", func(t *testing.T) {
		mock := &mockTernClient{
			revertResp: &ternv1.RevertResponse{
				Accepted:     false,
				ErrorMessage: "Schema change " + remoteID + " has already been finalized",
			},
		}
		apply := remoteRevertWindowApply(operatorID)
		svc, stores := newControlTestServiceWithStores(mock, apply, nil)

		w := postControl(t, svc, "/api/revert", operatorID)
		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
		assertOperatorFacing(t, w.Body.String(), "the response an operator reads")

		stored, err := stores.controls.GetByOperation(t.Context(), apply.ID, storage.ControlOperationRevert)
		require.NoError(t, err)
		require.NotNil(t, stored, "a rejected revert must leave a terminal control request")
		assert.Equal(t, storage.ControlRequestFailed, stored.Status)
		assertOperatorFacing(t, stored.ErrorMessage, "the durable control request")

		require.NotNil(t, mock.revertReq)
		assert.Equal(t, remoteID, mock.revertReq.ApplyId, "the data plane is still addressed by its own id")
	})

	t.Run("a rejected skip-revert is recorded against the operator apply id", func(t *testing.T) {
		mock := &mockTernClient{
			skipRevertResp: &ternv1.SkipRevertResponse{
				Accepted:     false,
				ErrorMessage: "Schema change " + remoteID + " is no longer in its revert window",
			},
		}
		apply := remoteRevertWindowApply(operatorID)
		svc, stores := newControlTestServiceWithStores(mock, apply, nil)

		w := postControl(t, svc, "/api/skip-revert", operatorID)
		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
		assertOperatorFacing(t, w.Body.String(), "the response an operator reads")

		stored, err := stores.controls.GetByOperation(t.Context(), apply.ID, storage.ControlOperationSkipRevert)
		require.NoError(t, err)
		require.NotNil(t, stored, "a rejected skip-revert must leave a terminal control request")
		assert.Equal(t, storage.ControlRequestFailed, stored.Status)
		assertOperatorFacing(t, stored.ErrorMessage, "the durable control request")

		require.NotNil(t, mock.skipRevertReq)
		assert.Equal(t, remoteID, mock.skipRevertReq.ApplyId, "the data plane is still addressed by its own id")
	})

	// A stop is queued durably first, then attempted immediately. When the data
	// plane accepts the queued request but rejects the immediate attempt, the
	// apply log records why the request is still pending — and does so in the
	// operator's terms.
	t.Run("a rejected immediate stop is logged against the operator apply id", func(t *testing.T) {
		mock := &mockTernClient{
			stopResp: &ternv1.StopResponse{
				Accepted:     false,
				ErrorMessage: "Schema change " + remoteID + " is already cutting over",
			},
		}
		apply := activeTestApply(operatorID)
		apply.ExternalID = remoteID
		tasks := []*storage.Task{{
			ID:             30,
			TaskIdentifier: "task-stop-relay",
			ApplyID:        apply.ID,
			State:          state.Task.Running,
		}}
		svc, stores := newControlTestServiceWithStores(mock, apply, tasks)

		w := postControl(t, svc, "/api/stop", operatorID)
		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
		require.NotNil(t, mock.stopReq, "the queued stop is followed by an immediate attempt")
		assert.Equal(t, remoteID, mock.stopReq.ApplyId, "the data plane is still addressed by its own id")

		require.True(t, hasApplyLogMessageContaining(stores.applyLogs.logs, "Immediate stop attempt was not accepted"),
			"the apply log must record why the stop request is still pending")
		for _, log := range stores.applyLogs.logs {
			assert.NotContains(t, log.Message, remoteID, "no apply log line names the remote id")
		}
	})

	// Cancel is queued durably and then attempted immediately, exactly as stop
	// is. A rejected immediate attempt leaves the durable request pending for
	// the owning drive, so the apply log is the only place an operator learns
	// why nothing happened yet — and it reads in their terms.
	t.Run("a rejected immediate cancel is logged against the operator apply id", func(t *testing.T) {
		mock := &mockTernClient{
			isRemote: true,
			cancelResp: &ternv1.CancelResponse{
				Accepted:     false,
				ErrorMessage: "Schema change " + remoteID + " is already cutting over",
			},
		}
		apply := activeTestApply(operatorID)
		apply.ExternalID = remoteID
		tasks := []*storage.Task{{
			ID:             31,
			TaskIdentifier: "task-cancel-relay",
			ApplyID:        apply.ID,
			State:          state.Task.Running,
		}}
		svc, stores := newControlTestServiceWithStores(mock, apply, tasks)

		w := postControl(t, svc, "/api/cancel", operatorID)
		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
		require.NotNil(t, mock.cancelReq, "the queued cancel is followed by an immediate attempt")
		assert.Equal(t, remoteID, mock.cancelReq.ApplyId, "the data plane is still addressed by its own id")

		require.True(t, hasApplyLogMessageContaining(stores.applyLogs.logs, "Immediate cancel attempt was not accepted"),
			"the apply log must record why the cancel request is still pending")
		require.True(t, hasApplyLogMessageContaining(stores.applyLogs.logs, "is already cutting over"),
			"the apply log carries the reason the attempt was refused")
		for _, log := range stores.applyLogs.logs {
			assert.NotContains(t, log.Message, remoteID, "no apply log line names the remote id")
		}

		pending, err := stores.controls.GetPending(t.Context(), apply.ID, storage.ControlOperationCancel)
		require.NoError(t, err)
		require.NotNil(t, pending, "a refused immediate attempt leaves the durable request for the owning drive")
	})

	// A refusal is not obliged to carry a reason. The log line still has to read
	// as a sentence rather than trailing a colon into nothing.
	t.Run("a rejected immediate cancel with no reason logs a complete sentence", func(t *testing.T) {
		mock := &mockTernClient{cancelResp: &ternv1.CancelResponse{Accepted: false}}
		apply := activeTestApply(operatorID)
		apply.ExternalID = remoteID
		tasks := []*storage.Task{{
			ID:             32,
			TaskIdentifier: "task-cancel-relay-silent",
			ApplyID:        apply.ID,
			State:          state.Task.Running,
		}}
		svc, stores := newControlTestServiceWithStores(mock, apply, tasks)

		w := postControl(t, svc, "/api/cancel", operatorID)
		assert.Equal(t, http.StatusOK, w.Code, w.Body.String())

		var logged string
		for _, log := range stores.applyLogs.logs {
			if strings.Contains(log.Message, "Immediate cancel attempt was not accepted") {
				logged = log.Message
			}
		}
		require.NotEmpty(t, logged, "the apply log must record the refused attempt even with no reason")
		assert.NotContains(t, logged, "pending:", "with no reason there is nothing to introduce with a colon")
	})
}

// Revert and skip-revert are contradictory intents for the same revert window —
// one undoes the change, the other makes it permanent. Once either has a
// pending durable request, the opposite command is rejected with a conflict
// that names the intent already in flight, instead of recording a second,
// contradictory request for the drive to arbitrate.
func TestRevertWindowCommandsRejectContradiction(t *testing.T) {
	revertWindowApply := func(id string) *storage.Apply {
		apply := activeTestApply(id)
		apply.State = state.Apply.RevertWindow
		apply.Engine = storage.EnginePlanetScale
		apply.DatabaseType = storage.DatabaseTypeVitess
		return apply
	}
	post := func(t *testing.T, mux *http.ServeMux, path, applyID string) *httptest.ResponseRecorder {
		t.Helper()
		body := fmt.Sprintf(`{"environment": "staging", "apply_id": %q}`, applyID)
		req := httptest.NewRequestWithContext(t.Context(), "POST", path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		return w
	}

	t.Run("revert rejected while skip-revert is pending", func(t *testing.T) {
		// The immediate skip attempt fails, so the durable skip-revert request
		// stays pending for the apply owner.
		mock := &mockTernClient{skipRevertErr: errors.New("data plane unavailable")}
		svc := newControlTestService(mock, revertWindowApply("apply-conflict-rv"))
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		first := post(t, mux, "/api/skip-revert", "apply-conflict-rv")
		require.Equal(t, http.StatusAccepted, first.Code, first.Body.String())

		second := post(t, mux, "/api/revert", "apply-conflict-rv")
		assert.Equal(t, http.StatusConflict, second.Code, second.Body.String())
		assert.Contains(t, second.Body.String(), "skip-revert is already pending")
		assert.Nil(t, mock.revertReq, "the contradictory revert must not reach the data plane")
	})

	t.Run("skip-revert rejected while revert is pending", func(t *testing.T) {
		// The immediate revert attempt fails, so the durable revert request
		// stays pending for the apply owner.
		mock := &mockTernClient{revertErr: errors.New("data plane unavailable")}
		svc := newControlTestService(mock, revertWindowApply("apply-conflict-sr"))
		mux := http.NewServeMux()
		svc.ConfigureRoutes(mux)

		first := post(t, mux, "/api/revert", "apply-conflict-sr")
		require.Equal(t, http.StatusAccepted, first.Code, first.Body.String())

		second := post(t, mux, "/api/skip-revert", "apply-conflict-sr")
		assert.Equal(t, http.StatusConflict, second.Code, second.Body.String())
		assert.Contains(t, second.Body.String(), "revert is already pending")
		assert.Nil(t, mock.skipRevertReq, "the contradictory skip-revert must not reach the data plane")
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

// A source plan without captured original schema files can never produce a
// rollback plan, no matter how often the same request is retried, so
// ExecuteRollbackPlanForApply must reject it with the terminal marker that
// durable command processing keys retry decisions on.
func TestExecuteRollbackPlanForApplyUncapturedFilesIsTerminal(t *testing.T) {
	now := time.Now().UTC()
	apply := rollbackGuardrailApply("apply_no_schema", 1, 10, now)
	svc := newRollbackGuardrailService(apply, rollbackGuardrailPlan(10, false), []*storage.Task{
		rollbackGuardrailTask(1, 10, now),
	})

	resp, err := svc.ExecuteRollbackPlanForApply(t.Context(), apply)

	require.Error(t, err)
	assert.Nil(t, resp)
	assert.True(t, IsTerminalControlError(err), "an uncapturable source plan re-reads the same on every retry; the failure must never be re-driven")
	assert.Contains(t, err.Error(), "rollback source plan is invalid")
	assert.Contains(t, err.Error(), "no original schema files available")
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
