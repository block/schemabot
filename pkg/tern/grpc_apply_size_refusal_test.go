package tern

import (
	"context"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"
	"google.golang.org/protobuf/proto"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// ResourceExhausted covers both throttling, which a later send can get past,
// and a message over the size limit, which every resend meets again. Only the
// first is retryable, whichever side of the call enforced the limit.
func TestIsRetryableRemoteApplyErrorSeparatesSizeRefusalFromThrottling(t *testing.T) {
	testCases := []struct {
		name          string
		err           error
		wantRetryable bool
		wantSizeError string
	}{
		{
			name:          "server refused to receive the request",
			err:           status.Error(codes.ResourceExhausted, "grpc: received message larger than max (17000000 vs. 16777216)"),
			wantRetryable: false,
			wantSizeError: "apply request is 17000000 bytes, over the 16777216-byte gRPC message size limit",
		},
		{
			name:          "client refused to send the request",
			err:           status.Error(codes.ResourceExhausted, "trying to send message larger than max (5000 vs. 4096)"),
			wantRetryable: false,
			wantSizeError: "apply request is 5000 bytes, over the 4096-byte gRPC message size limit",
		},
		{
			// Without the size pair nothing ties the refusal to the request,
			// so the dispatch treats it as ambiguous and never explains it.
			name:          "size refusal without the size pair",
			err:           status.Error(codes.ResourceExhausted, "grpc: received message larger than max"),
			wantRetryable: false,
		},
		{
			name:          "throttling",
			err:           status.Error(codes.ResourceExhausted, "too many concurrent applies; try again later"),
			wantRetryable: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantRetryable, isRetryableRemoteApplyError(tc.err))
			assert.Equal(t, tc.wantRetryable, classifyRemoteApplyError(tc.err) == dispatchRetryable)

			explained := explainApplySizeRefusal(tc.err)
			assert.ErrorIs(t, explained, tc.err, "the gRPC error stays wrapped")
			assert.Equal(t, codes.ResourceExhausted, status.Code(explained))
			if tc.wantSizeError == "" {
				assert.Equal(t, tc.err, explained, "an error that is not a size refusal is returned unchanged")
				return
			}
			assert.Contains(t, explained.Error(), tc.wantSizeError)
			assert.Contains(t, explained.Error(), status.Convert(tc.err).Message(), "the raw gRPC message stays in the error")
		})
	}
	assert.False(t, isMessageSizeRefusal(errors.New("received message larger than max (5 vs. 4)")),
		"only a gRPC ResourceExhausted status is a size refusal")
	assert.False(t, isMessageSizeRefusal(status.Error(codes.Internal, "grpc: received message larger than max (5 vs. 4)")),
		"only a gRPC ResourceExhausted status is a size refusal")
}

// gRPC words a refused response the same way as a refused request, and a
// response is only sent once the data plane handled the request. Only a
// refusal that reports the request's own size proves the request was never
// handled; any other size refusal is ambiguous.
func TestIsUnprovenRequestSizeRefusalNeedsTheRequestsOwnSize(t *testing.T) {
	req := &ternv1.ApplyRequest{PlanId: "plan-1", Database: "testdb", DdlChanges: []*ternv1.TableChange{{TableName: "users"}}}
	size := proto.Size(req)
	refusal := func(format string, args ...any) error {
		return status.Error(codes.ResourceExhausted, fmt.Sprintf(format, args...))
	}

	assert.False(t, isUnprovenRequestSizeRefusal(refusal("grpc: received message larger than max (%d vs. %d)", size, size-1), req),
		"the server refusing a request of exactly this size is proof")
	assert.False(t, isUnprovenRequestSizeRefusal(refusal("trying to send message larger than max (%d vs. %d)", size, size-1), req),
		"the client refusing to send a message of exactly this size is proof")
	assert.True(t, isUnprovenRequestSizeRefusal(refusal("grpc: received message larger than max (%d vs. %d)", size+5000, 1024), req),
		"a refused message of another size is the response")
	assert.True(t, isUnprovenRequestSizeRefusal(refusal("grpc: trying to send message larger than max (%d vs. %d)", size, size-1), req),
		"the server refusing to send its response is not proof, even at the request's size")
	assert.True(t, isUnprovenRequestSizeRefusal(refusal("grpc: received message larger than max"), req),
		"a refusal without sizes proves nothing")
	assert.False(t, isUnprovenRequestSizeRefusal(refusal("too many concurrent applies; try again later"), req),
		"throttling is not a size refusal")
}

// An apply whose request is larger than the Tern server will read is refused
// before any handler runs. The drive sends it once, fails the apply and its
// task permanently rather than pausing them for a retry that would be refused
// the same way, and tells the operator how large the request was and what the
// limit is. The same holds when the client's own send limit refuses it first.
func TestGRPCClient_ApplyOverMessageSizeLimitFailsWithoutRetry(t *testing.T) {
	const sizeLimit = 1024
	testCases := []struct {
		name       string
		serverOpts []grpc.ServerOption
		callOpts   []grpc.CallOption
		// serverRefuses is set when the server, not the client, enforces the limit.
		serverRefuses bool
	}{
		{
			name:          "server receive limit",
			serverOpts:    []grpc.ServerOption{grpc.MaxRecvMsgSize(sizeLimit)},
			serverRefuses: true,
		},
		{
			name:     "client send limit",
			callOpts: []grpc.CallOption{grpc.MaxCallSendMsgSize(sizeLimit)},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var applySends atomic.Int32
			countApplies := grpc.InTapHandle(func(ctx context.Context, info *tap.Info) (context.Context, error) {
				if info.FullMethodName == ternv1.Tern_Apply_FullMethodName {
					applySends.Add(1)
				}
				return ctx, nil
			})
			server := &capturingTernServer{remoteApplyID: "remote-never-created"}
			client := newSizeLimitedGRPCClient(t, server, append(tc.serverOpts, countApplies), tc.callOpts)

			apply := &storage.Apply{
				ID:              31,
				ApplyIdentifier: "apply-oversized",
				PlanID:          310,
				Database:        "testdb",
				DatabaseType:    storage.DatabaseTypeMySQL,
				Environment:     "staging",
				State:           state.Apply.Pending,
			}
			// The statement alone is larger than the limit, so the request is too.
			ddl := "ALTER TABLE users ADD COLUMN notes varchar(255) COMMENT '" + strings.Repeat("x", 4*sizeLimit) + "'"
			task := &storage.Task{
				ID:             32,
				TaskIdentifier: "task-oversized",
				ApplyID:        apply.ID,
				TableName:      "users",
				DDL:            ddl,
				DDLAction:      "alter",
				Namespace:      "default",
				State:          state.Task.Pending,
			}
			client.storage = &mockStorage{
				applies: &mockApplyStore{apply: apply},
				tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
				logs:    &mockApplyLogStore{},
				plans: &mockPlanStore{plan: &storage.Plan{
					ID:             apply.PlanID,
					PlanIdentifier: "plan-oversized",
				}},
			}

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			err := client.ResumeApply(ctx, apply)
			require.Error(t, err, "an apply over the size limit must not dispatch")
			assert.Equal(t, codes.ResourceExhausted, status.Code(err))

			// A server refusal answers the one call it saw. A client refusal
			// can open the call before refusing the message, and whether the
			// server sees that opening before the client resets it is timing,
			// so it is bounded rather than pinned.
			if tc.serverRefuses {
				assert.Equal(t, int32(1), applySends.Load(), "the apply is dispatched once and never resent")
			} else {
				assert.LessOrEqual(t, applySends.Load(), int32(1), "the apply is never resent")
			}
			assert.Nil(t, server.getApplyRequest(), "the Apply handler never runs")
			assert.Equal(t, state.Apply.Failed, apply.State, "a size refusal fails the apply rather than pausing it for retry")
			assert.Equal(t, state.Task.Failed, task.State)
			assert.Empty(t, apply.ExternalID)

			size, limit := requireSizeAndLimit(t, apply.ErrorMessage)
			assert.Equal(t, sizeLimit, limit, "the error names the limit the request crossed")
			assert.Greater(t, size, len(ddl), "the error names the size of the whole request")
			assert.Contains(t, err.Error(), apply.ErrorMessage, "the drive returns the same explanation it stored")
		})
	}
}

// newSizeLimitedGRPCClient starts an in-process Tern server with serverOpts and
// connects a client that applies callOpts to every call.
func newSizeLimitedGRPCClient(t *testing.T, server ternv1.TernServer, serverOpts []grpc.ServerOption, callOpts []grpc.CallOption) *GRPCClient {
	t.Helper()

	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "localhost:0")
	require.NoError(t, err, "failed to listen")

	grpcServer := grpc.NewServer(serverOpts...)
	ternv1.RegisterTernServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()

	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if len(callOpts) > 0 {
		dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(callOpts...))
	}
	conn, err := grpc.NewClient(lis.Addr().String(), dialOpts...)
	require.NoError(t, err, "failed to dial")

	client := &GRPCClient{
		conn:    conn,
		client:  ternv1.NewTernClient(conn),
		storage: &mockStorage{},
	}
	t.Cleanup(func() {
		utils.CloseAndLog(client)
		grpcServer.Stop()
	})
	return client
}

var sizeAndLimitPattern = regexp.MustCompile(`apply request is (\d+) bytes, over the (\d+)-byte gRPC message size limit`)

// requireSizeAndLimit reads the request size and the limit back out of a size
// refusal explanation.
func requireSizeAndLimit(t *testing.T, message string) (int, int) {
	t.Helper()
	match := sizeAndLimitPattern.FindStringSubmatch(message)
	require.NotNilf(t, match, "the error must name the request size and the limit: %q", message)
	size, err := strconv.Atoi(match[1])
	require.NoError(t, err)
	limit, err := strconv.Atoi(match[2])
	require.NoError(t, err)
	return size, limit
}

// The data plane accepts a dispatch and starts a remote apply, but its answer
// is larger than the client will read. The client sees the same refusal text
// an oversized request gets, yet the remote apply may be running, so the drive
// records nothing: no failure and no remote apply id. The next claim re-sends
// the same idempotency key and learns what the data plane created.
func TestGRPCClient_OversizedApplyResponseLeavesTheDispatchUnresolved(t *testing.T) {
	const recvLimit = 1024
	oversized := &ternv1.ApplyResponse{Accepted: true, ApplyId: "remote-running", ErrorMessage: strings.Repeat("x", 8*recvLimit)}
	callOpts := []grpc.CallOption{grpc.MaxCallRecvMsgSize(recvLimit)}

	t.Run("first dispatch", func(t *testing.T) {
		server := &capturingTernServer{applyResponse: oversized}
		client := newSizeLimitedGRPCClient(t, server, nil, callOpts)
		apply := &storage.Apply{
			ID: 41, ApplyIdentifier: "apply-oversized-response", PlanID: 410, Database: "testdb",
			DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging", State: state.Apply.Pending,
		}
		task := &storage.Task{
			ID: 42, TaskIdentifier: "task-oversized-response", ApplyID: apply.ID, TableName: "users",
			DDL: "ALTER TABLE users ADD COLUMN a int", DDLAction: "alter", Namespace: "default", State: state.Task.Pending,
		}
		client.storage = &mockStorage{
			applies: &mockApplyStore{apply: apply},
			tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
			logs:    &mockApplyLogStore{},
			plans:   &mockPlanStore{plan: &storage.Plan{ID: apply.PlanID, PlanIdentifier: "plan-oversized-response"}},
		}

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		err := client.ResumeApply(ctx, apply)

		require.Error(t, err)
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))
		assert.Contains(t, err.Error(), "ambiguous remote dispatch outcome")
		require.NotNil(t, server.getApplyRequest(), "the Apply handler ran and accepted the work")
		assert.NotContains(t, []string{state.Apply.Failed, state.Apply.FailedRetryable}, apply.State)
		assert.NotContains(t, []string{state.Task.Failed, state.Task.FailedRetryable}, task.State)
		assert.NotContains(t, apply.ErrorMessage, "apply request is", "a response refusal is never described as the request's size")
	})

	t.Run("replay", func(t *testing.T) {
		server := &capturingTernServer{applyResponse: oversized}
		client := newSizeLimitedGRPCClient(t, server, nil, callOpts)
		fixture := newLostDispatchFixture(state.Apply.Running, storage.ControlOperationCancel)
		client.storage = fixture.storage

		err := resumeLostDispatch(t, client, fixture.apply)

		require.ErrorIs(t, err, ErrRemoteDispatchUnresolved)
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))
		assert.Equal(t, state.Apply.Running, fixture.apply.State, "a replay that got no verdict leaves the work untouched")
		assert.Equal(t, state.Task.Running, fixture.task.State)
		assert.Empty(t, fixture.apply.ErrorMessage)
		cancelReq, err := fixture.controlRequests.GetPending(t.Context(), fixture.apply.ID, storage.ControlOperationCancel)
		require.NoError(t, err)
		assert.NotNil(t, cancelReq, "the cancel stays pending for the drive that recovers the remote apply")
	})
}

// A lost dispatch is replayed, and the replay's request is larger than the
// data plane will read. The refusal names the request's own size, so it
// proves the data plane never handled this send; the work fails closed, and
// the operator's pending stop and cancel fail with it, as for any other
// refused replay.
func TestGRPCClient_LostDispatchReplayOverSizeLimitFailsClosed(t *testing.T) {
	const sizeLimit = 1024
	server := &capturingTernServer{remoteApplyID: "remote-never-created"}
	client := newSizeLimitedGRPCClient(t, server, []grpc.ServerOption{grpc.MaxRecvMsgSize(sizeLimit)}, nil)
	fixture := newLostDispatchFixture(state.Apply.Running, storage.ControlOperationCancel, storage.ControlOperationStop)
	fixture.task.DDL = "ALTER TABLE users ADD COLUMN notes varchar(255) COMMENT '" + strings.Repeat("x", 4*sizeLimit) + "'"
	client.storage = fixture.storage

	err := resumeLostDispatch(t, client, fixture.apply)

	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrRemoteDispatchUnresolved, "a refusal of the request is a verdict")
	assert.Nil(t, server.getApplyRequest(), "the Apply handler never runs")
	assert.Equal(t, state.Apply.Failed, fixture.apply.State)
	assert.Equal(t, state.Task.Failed, fixture.task.State)
	_, limit := requireSizeAndLimit(t, fixture.apply.ErrorMessage)
	assert.Equal(t, sizeLimit, limit)
	for _, op := range []storage.ControlOperation{storage.ControlOperationCancel, storage.ControlOperationStop} {
		req, err := fixture.controlRequests.GetByOperation(t.Context(), fixture.apply.ID, op)
		require.NoError(t, err)
		require.NotNilf(t, req, "the %s request must exist", op)
		assert.Equalf(t, storage.ControlRequestFailed, req.Status, "the refused replay must fail the pending %s", op)
	}
}

// A group_finalizer dispatches its namespace's VSchema with the plan's schema
// files, so an oversized schema makes that request too large as well. The
// data plane refuses it before any handler runs, and the task-less operation
// fails permanently with the same explanation as a work dispatch. A response
// over the client's limit instead arrives after the data plane accepted the
// dispatch, so it fails nothing and stores no remote apply id.
func TestGRPCClient_VSchemaOnlyDispatchOverSizeLimit(t *testing.T) {
	const sizeLimit = 1024
	t.Run("request refused", func(t *testing.T) {
		server := &capturingTernServer{remoteApplyID: "remote-never-created"}
		client := newSizeLimitedGRPCClient(t, server, []grpc.ServerOption{grpc.MaxRecvMsgSize(sizeLimit)}, nil)
		apply, operation := setUpFinalizerDispatch(t, client, strings.Repeat("x", 4*sizeLimit))

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		err := client.ResumeApplyOperation(ctx, apply, operation.ID)

		require.Error(t, err)
		assert.Equal(t, codes.ResourceExhausted, status.Code(err))
		assert.Nil(t, server.getApplyRequest(), "the Apply handler never runs")
		assert.Equal(t, state.Apply.Failed, apply.State, "a size refusal fails the apply rather than pausing it for retry")
		_, limit := requireSizeAndLimit(t, apply.ErrorMessage)
		assert.Equal(t, sizeLimit, limit)
		assert.Empty(t, operation.ExternalID)
	})
	t.Run("response refused", func(t *testing.T) {
		server := &capturingTernServer{applyResponse: &ternv1.ApplyResponse{
			Accepted: true, ApplyId: "remote-running", ErrorMessage: strings.Repeat("x", 8*sizeLimit),
		}}
		client := newSizeLimitedGRPCClient(t, server, nil, []grpc.CallOption{grpc.MaxCallRecvMsgSize(sizeLimit)})
		apply, operation := setUpFinalizerDispatch(t, client, "")

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		err := client.ResumeApplyOperation(ctx, apply, operation.ID)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "ambiguous remote dispatch outcome")
		require.NotNil(t, server.getApplyRequest(), "the Apply handler ran and accepted the work")
		assert.NotContains(t, []string{state.Apply.Failed, state.Apply.FailedRetryable}, apply.State)
		assert.NotContains(t, apply.ErrorMessage, "apply request is")
		assert.Empty(t, operation.ExternalID)
	})
}

// setUpFinalizerDispatch stores a pending group_finalizer operation whose plan
// carries one namespace's VSchema, padded with padding.
func setUpFinalizerDispatch(t *testing.T, client *GRPCClient, padding string) (*storage.Apply, *storage.ApplyOperation) {
	t.Helper()
	apply := &storage.Apply{
		ID: 51, ApplyIdentifier: "apply-finalizer-oversized", PlanID: 510, Database: "commerce",
		DatabaseType: storage.DatabaseTypeStrata, Environment: "staging", State: state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{Target: "commerce-target"})
	operationID := int64(52)
	operation := &storage.ApplyOperation{
		ID: operationID, ApplyID: apply.ID, Deployment: "commerce-deployment",
		OperationKey: "commerce/group_finalizer", OperationKind: storage.ApplyOperationKindGroupFinalizer,
		State: state.ApplyOperation.Pending,
	}
	vschema := `{"sharded":true,"comment":"` + padding + `"}`
	client.storage = &mockStorage{
		applies: &mockApplyStore{apply: apply},
		tasks:   &mockTaskStore{getByApplyIDErr: errors.New("finalizer drive must not load tasks")},
		logs:    &mockApplyLogStore{},
		plans: &mockPlanStore{plan: &storage.Plan{
			ID: apply.PlanID, PlanIdentifier: "plan-finalizer-oversized",
			SchemaFiles: schema.SchemaFiles{"commerce": {Files: map[string]string{storage.VSchemaArtifactName: vschema}}},
			Namespaces:  map[string]*storage.NamespacePlanData{"commerce": {Artifacts: map[string]string{storage.VSchemaArtifactName: vschema}}},
		}},
		operations: &mockApplyOperationStore{ops: map[int64]*storage.ApplyOperation{operationID: operation}},
	}
	return apply, operation
}
