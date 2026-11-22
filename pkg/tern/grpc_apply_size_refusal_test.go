package tern

import (
	"context"
	"errors"
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

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
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
			name:          "size refusal without the size pair",
			err:           status.Error(codes.ResourceExhausted, "grpc: received message larger than max"),
			wantRetryable: false,
			wantSizeError: "apply request is over the gRPC message size limit",
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
