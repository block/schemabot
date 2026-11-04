package tern

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/block/schemabot/pkg/inventory"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// countingApplyStore counts the apply reads a progress answer makes.
type countingApplyStore struct {
	storage.ApplyStore
	byIdentifier, byID int
}

func (s *countingApplyStore) GetByApplyIdentifier(ctx context.Context, applyIdentifier string) (*storage.Apply, error) {
	s.byIdentifier++
	return s.ApplyStore.GetByApplyIdentifier(ctx, applyIdentifier)
}

func (s *countingApplyStore) Get(ctx context.Context, id int64) (*storage.Apply, error) {
	s.byID++
	return s.ApplyStore.Get(ctx, id)
}

// countingTaskStore counts the task reads a progress answer makes.
type countingTaskStore struct {
	storage.TaskStore
	byApply int
}

func (s *countingTaskStore) GetByApplyID(ctx context.Context, applyID int64) ([]*storage.Task, error) {
	s.byApply++
	return s.TaskStore.GetByApplyID(ctx, applyID)
}

// countingOperationStore counts the operation reads a progress answer makes.
type countingOperationStore struct {
	storage.ApplyOperationStore
	lists, gets int
}

func (s *countingOperationStore) ListByApply(ctx context.Context, applyID int64) ([]*storage.ApplyOperation, error) {
	s.lists++
	return s.ApplyOperationStore.ListByApply(ctx, applyID)
}

func (s *countingOperationStore) Get(ctx context.Context, id int64) (*storage.ApplyOperation, error) {
	s.gets++
	return s.ApplyOperationStore.Get(ctx, id)
}

// countingSettledRequestStore counts the settled control request reads a
// progress answer makes.
type countingSettledRequestStore struct {
	storage.ControlRequestStore
	settled int
}

func (s *countingSettledRequestStore) ListSettled(ctx context.Context, applyID int64) ([]*storage.ApplyControlRequest, error) {
	s.settled++
	return s.ControlRequestStore.ListSettled(ctx, applyID)
}

type batchProgressReadCounts struct {
	applies    *countingApplyStore
	tasks      *countingTaskStore
	operations *countingOperationStore
	control    *countingSettledRequestStore
}

// countProgressReads wraps the fixture's stores so a test can count the rows
// an answer reads.
func countProgressReads(fx *sharedApplyFixture) *batchProgressReadCounts {
	st := fx.client.storage.(*exactProgressStorage)
	counts := &batchProgressReadCounts{
		applies:    &countingApplyStore{ApplyStore: st.applies},
		tasks:      &countingTaskStore{TaskStore: st.tasks},
		operations: &countingOperationStore{ApplyOperationStore: st.applyOperations},
		control:    &countingSettledRequestStore{ControlRequestStore: st.controlRequests},
	}
	st.applies, st.tasks, st.applyOperations, st.controlRequests = counts.applies, counts.tasks, counts.operations, counts.control
	return counts
}

func batchProgressRequest(fx *sharedApplyFixture, operationIDs ...string) *ternv1.BatchProgressRequest {
	return &ternv1.BatchProgressRequest{ApplyId: fx.apply.ApplyIdentifier, Environment: "staging", ApplyOperationIds: operationIDs}
}

// Two member targets of one deployment share its data-plane apply: payments-001
// has failed and payments-002 is still copying. A control plane driving both
// asks for them in one BatchProgress call, and each entry is exactly the
// answer a Progress request scoped to that member returns, in request order,
// so the batched answer reports payments-002 as running and payments-001 as
// failed just as two separate polls would.
func TestLocalClient_BatchProgressAnswersEachMemberAsItsOwnProgressWould(t *testing.T) {
	fx := newSharedApplyFixture(state.ApplyOperation.Failed, state.ApplyOperation.Running, state.Task.Failed, state.Task.Running)
	fx.apply.State = state.Apply.Failed
	fx.firstTask.TableName = "orders"

	first, err := fx.client.Progress(t.Context(), &ternv1.ProgressRequest{ApplyId: fx.apply.ApplyIdentifier, Environment: "staging", ApplyOperationId: "11"})
	require.NoError(t, err)
	second, err := fx.client.Progress(t.Context(), &ternv1.ProgressRequest{ApplyId: fx.apply.ApplyIdentifier, Environment: "staging", ApplyOperationId: "12"})
	require.NoError(t, err)

	batch, err := fx.client.BatchProgress(t.Context(), batchProgressRequest(fx, "12", "11"))
	require.NoError(t, err)
	assert.Equal(t, fx.apply.ApplyIdentifier, batch.ApplyId)
	require.Len(t, batch.Operations, 2)

	assert.Equal(t, "12", batch.Operations[0].ApplyOperationId)
	assert.Equal(t, int32(codes.OK), batch.Operations[0].ErrorCode)
	assert.True(t, proto.Equal(second, batch.Operations[0].Progress), "payments-002's entry is its own scoped answer:\nwant %v\ngot  %v", second, batch.Operations[0].Progress)
	assert.Equal(t, ternv1.State_STATE_RUNNING, batch.Operations[0].Progress.State)

	assert.Equal(t, "11", batch.Operations[1].ApplyOperationId)
	assert.True(t, proto.Equal(first, batch.Operations[1].Progress), "payments-001's entry is its own scoped answer:\nwant %v\ngot  %v", first, batch.Operations[1].Progress)
	assert.Equal(t, ternv1.State_STATE_FAILED, batch.Operations[1].Progress.State)
	require.Len(t, batch.Operations[1].Progress.Tables, 1)
	assert.Equal(t, "orders", batch.Operations[1].Progress.Tables[0].TableName)
}

// One BatchProgress call reads the apply, its tasks, its operations, and its
// settled control requests once for every member it answers, where a Progress
// call per member reads each of them once per member.
func TestLocalClient_BatchProgressReadsTheApplyOnceForAllMembers(t *testing.T) {
	fx := newSharedApplyFixture(state.ApplyOperation.Running, state.ApplyOperation.Running, state.Task.Running, state.Task.Running)
	counts := countProgressReads(fx)

	batch, err := fx.client.BatchProgress(t.Context(), batchProgressRequest(fx, "11", "12"))
	require.NoError(t, err)
	require.Len(t, batch.Operations, 2)
	for _, entry := range batch.Operations {
		require.NotNil(t, entry.Progress, "operation %s", entry.ApplyOperationId)
		assert.Equal(t, ternv1.State_STATE_RUNNING, entry.Progress.State, "operation %s", entry.ApplyOperationId)
	}

	assert.Equal(t, 1, counts.applies.byIdentifier, "the apply is looked up once")
	assert.Equal(t, 1, counts.applies.byID, "the apply record is re-read once")
	assert.Equal(t, 1, counts.tasks.byApply, "the apply's tasks are read once")
	assert.Equal(t, 1, counts.operations.lists, "the apply's operations are listed once")
	assert.Equal(t, 0, counts.operations.gets, "no operation is read on its own")
	assert.Equal(t, 1, counts.control.settled, "the settled control requests are read once")
}

// A Progress request scoped to one member reads its operation row once, though
// both the scope check and the display metadata need it.
func TestLocalClient_ScopedProgressReadsItsOperationOnce(t *testing.T) {
	fx := newSharedApplyFixture(state.ApplyOperation.Running, state.ApplyOperation.Running, state.Task.Running, state.Task.Running)
	counts := countProgressReads(fx)

	resp, err := fx.client.Progress(t.Context(), &ternv1.ProgressRequest{ApplyId: fx.apply.ApplyIdentifier, Environment: "staging", ApplyOperationId: "12"})
	require.NoError(t, err)
	assert.Equal(t, "12", resp.ApplyOperationId)

	assert.Equal(t, 1, counts.operations.gets, "the scoped operation is read once")
	assert.Equal(t, 0, counts.operations.lists, "a single scoped answer does not list every operation")
	assert.Equal(t, 1, counts.applies.byID, "the apply record is re-read once")
	assert.Equal(t, 1, counts.control.settled, "the settled control requests are read once")
}

// A BatchProgress call that names an id that is not one of the apply's
// operations answers that entry with the error a scoped Progress request for
// it fails with, and still answers the members it does own.
func TestLocalClient_BatchProgressAnswersAnUnknownOperationWithItsOwnError(t *testing.T) {
	fx := newSharedApplyFixture(state.ApplyOperation.Running, state.ApplyOperation.Running, state.Task.Running, state.Task.Running)

	batch, err := fx.client.BatchProgress(t.Context(), batchProgressRequest(fx, "12", "99"))
	require.NoError(t, err)
	require.Len(t, batch.Operations, 2)

	require.NotNil(t, batch.Operations[0].Progress)
	assert.Equal(t, "12", batch.Operations[0].Progress.ApplyOperationId)

	_, scopedErr := fx.client.Progress(t.Context(), &ternv1.ProgressRequest{ApplyId: fx.apply.ApplyIdentifier, Environment: "staging", ApplyOperationId: "99"})
	require.Error(t, scopedErr)
	unknown := batch.Operations[1]
	assert.Equal(t, "99", unknown.ApplyOperationId)
	assert.Nil(t, unknown.Progress)
	assert.Equal(t, int32(codes.Internal), unknown.ErrorCode)
	assert.Equal(t, scopedErr.Error(), unknown.ErrorMessage)
	assert.Contains(t, unknown.ErrorMessage, "apply_operation 99 does not exist")
}

// A batch entry with no operation id is refused with InvalidArgument rather
// than answered for the whole apply, which would report every member's work as
// one member's. The members the call does name are still answered.
func TestLocalClient_BatchProgressRefusesAnEntryWithNoOperation(t *testing.T) {
	fx := newSharedApplyFixture(state.ApplyOperation.Running, state.ApplyOperation.Running, state.Task.Running, state.Task.Running)

	batch, err := fx.client.BatchProgress(t.Context(), batchProgressRequest(fx, "12", ""))
	require.NoError(t, err)
	require.Len(t, batch.Operations, 2)

	require.NotNil(t, batch.Operations[0].Progress)
	assert.Equal(t, "12", batch.Operations[0].Progress.ApplyOperationId)

	empty := batch.Operations[1]
	assert.Empty(t, empty.ApplyOperationId)
	assert.Nil(t, empty.Progress, "no answer carries the whole apply's tables")
	assert.Equal(t, int32(codes.InvalidArgument), empty.ErrorCode)
	assert.Contains(t, empty.ErrorMessage, "apply_operation_id is required for each entry")
	assert.Contains(t, empty.ErrorMessage, fx.apply.ApplyIdentifier)
}

// An apply the data plane does not know fails the whole call NotFound, the
// same code a Progress request for it gets, so every member's drive takes the
// missing remote apply path it takes today.
func TestServer_BatchProgressReportsAnUnknownApplyNotFound(t *testing.T) {
	fx := newSharedApplyFixture(state.ApplyOperation.Running, state.ApplyOperation.Running, state.Task.Running, state.Task.Running)
	fx.client.storage.(*exactProgressStorage).applies = &exactProgressApplyStore{}
	server := NewServer(fx.client, slog.Default())

	_, err := server.BatchProgress(t.Context(), batchProgressRequest(fx, "11", "12"))
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
	assert.Contains(t, err.Error(), storage.ErrApplyNotFound.Error())
}

// A data plane that routes each apply to its target client reports an apply
// its storage does not hold as NotFound too, for BatchProgress and for
// Progress, so a drive polling it takes the missing remote apply path rather
// than counting the answer as a failed poll.
func TestServer_RoutedProgressReportsAnUnknownApplyNotFound(t *testing.T) {
	fx := newSharedApplyFixture(state.ApplyOperation.Running, state.ApplyOperation.Running, state.Task.Running, state.Task.Running)
	st := fx.client.storage.(*exactProgressStorage)
	st.applies = &exactProgressApplyStore{}
	router, err := NewTargetRouter(TargetRouterConfig{
		Resolver: targetRouterResolverFunc(func(_ context.Context, req inventory.Request) (*inventory.Target, error) {
			return nil, fmt.Errorf("look up target %q: an unknown apply is never routed", req.Target)
		}),
		Storage: st,
		Logger:  slog.New(slog.DiscardHandler),
	})
	require.NoError(t, err)
	server := NewServer(router, slog.Default())

	_, err = server.BatchProgress(t.Context(), batchProgressRequest(fx, "11", "12"))
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
	assert.Contains(t, err.Error(), `apply "apply-shared-deployment" for batch progress routing`)

	_, err = server.Progress(t.Context(), &ternv1.ProgressRequest{ApplyId: fx.apply.ApplyIdentifier, Environment: "staging", ApplyOperationId: "11"})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

// batchUnsupportedClient routes BatchProgress to a target client that cannot
// batch, the way TargetRouter answers for one.
type batchUnsupportedClient struct {
	Client
}

func (batchUnsupportedClient) BatchProgress(context.Context, *ternv1.BatchProgressRequest) (*ternv1.BatchProgressResponse, error) {
	return nil, fmt.Errorf("route batch progress: %w", ErrBatchProgressUnsupported)
}

// An endpoint that cannot batch answers Unimplemented, which tells the
// control plane to ask Progress once per operation instead.
func TestServer_BatchProgressIsUnimplementedWhenTheClientCannotBatch(t *testing.T) {
	testCases := []struct {
		name   string
		client Client
	}{
		{name: "client without batch progress", client: noopControlClient{}},
		{name: "routed to a target client without batch progress", client: batchUnsupportedClient{}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server := NewServer(tc.client, slog.Default())
			_, err := server.BatchProgress(t.Context(), &ternv1.BatchProgressRequest{ApplyId: "apply-shared-deployment", ApplyOperationIds: []string{"11"}})
			require.Error(t, err)
			assert.Equal(t, codes.Unimplemented, status.Code(err))
		})
	}
}
