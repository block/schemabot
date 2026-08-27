package serve

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/block/schemabot/pkg/panicsafe"
)

// A handler panic must fail only that RPC: the client sees a fixed Internal
// error with no panic text (panic values can carry internal detail), and the
// panic value and stack land in the server log for triage.
func TestRecoveryUnaryInterceptorContainsHandlerPanic(t *testing.T) {
	var logs bytes.Buffer
	interceptor := RecoveryUnaryInterceptor(slog.New(slog.NewTextHandler(&logs, nil)))

	resp, err := interceptor(t.Context(), "req", &grpc.UnaryServerInfo{FullMethod: "/tern.v1.Tern/Cutover"},
		func(ctx context.Context, req any) (any, error) {
			panic("dial tcp 10.0.0.5:5432: connect: connection refused")
		})

	require.Error(t, err)
	assert.Nil(t, resp)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.Internal, st.Code())
	assert.Equal(t, "internal error; see server logs", st.Message())

	logged := logs.String()
	assert.Contains(t, logged, "/tern.v1.Tern/Cutover")
	assert.Contains(t, logged, "connection refused")
	assert.Contains(t, logged, "stack")
}

// A handler that completes normally passes its response through untouched.
func TestRecoveryUnaryInterceptorPassesThroughResponse(t *testing.T) {
	interceptor := RecoveryUnaryInterceptor(slog.New(slog.DiscardHandler))

	resp, err := interceptor(t.Context(), "req", &grpc.UnaryServerInfo{FullMethod: "/tern.v1.Tern/Plan"},
		func(ctx context.Context, req any) (any, error) {
			return "plan-response", nil
		})

	require.NoError(t, err)
	assert.Equal(t, "plan-response", resp)
}

// A handler's own error is not a panic: it must reach the client unchanged so
// typed status codes and wrapped causes survive.
func TestRecoveryUnaryInterceptorPassesThroughHandlerError(t *testing.T) {
	interceptor := RecoveryUnaryInterceptor(slog.New(slog.DiscardHandler))
	handlerErr := status.Error(codes.FailedPrecondition, "apply already terminal")

	resp, err := interceptor(t.Context(), "req", &grpc.UnaryServerInfo{FullMethod: "/tern.v1.Tern/Apply"},
		func(ctx context.Context, req any) (any, error) {
			return nil, handlerErr
		})

	assert.Nil(t, resp)
	require.Error(t, err)
	assert.True(t, errors.Is(err, handlerErr))
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
}

// A handler error that wraps a *panicsafe.Error is still the handler's own
// error, not a panic this interceptor recovered: it must reach the client
// unchanged with its cause chain intact, nothing logged, and no recovered-panic
// metric recorded. Only the identical *panicsafe.Error that panicsafe.Call
// constructed marks a contained panic.
func TestRecoveryUnaryInterceptorPassesThroughWrappedPanicsafeError(t *testing.T) {
	var logs bytes.Buffer
	interceptor := RecoveryUnaryInterceptor(slog.New(slog.NewTextHandler(&logs, nil)))
	handlerErr := fmt.Errorf("apply already terminal: %w", &panicsafe.Error{Value: "poisoned row"})

	resp, err := interceptor(t.Context(), "req", &grpc.UnaryServerInfo{FullMethod: "/tern.v1.Tern/Apply"},
		func(ctx context.Context, req any) (any, error) {
			return nil, handlerErr
		})

	assert.Nil(t, resp)
	require.Error(t, err)
	assert.Equal(t, handlerErr, err)
	assert.True(t, errors.Is(err, handlerErr))
	assert.Empty(t, logs.String())
}

// The streaming interceptor applies the same containment: a stream handler
// panic fails the stream with a fixed Internal error instead of killing the
// process.
func TestRecoveryStreamInterceptorContainsHandlerPanic(t *testing.T) {
	var logs bytes.Buffer
	interceptor := RecoveryStreamInterceptor(slog.New(slog.NewTextHandler(&logs, nil)))

	err := interceptor("srv", testServerStream{ctx: t.Context()}, &grpc.StreamServerInfo{FullMethod: "/tern.v1.Tern/Logs"},
		func(srv any, ss grpc.ServerStream) error {
			panic("poisoned row")
		})

	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.Internal, st.Code())
	assert.Equal(t, "internal error; see server logs", st.Message())
	assert.Contains(t, logs.String(), "/tern.v1.Tern/Logs")
	assert.Contains(t, logs.String(), "poisoned row")
}

// A stream handler's own error propagates unchanged.
func TestRecoveryStreamInterceptorPassesThroughHandlerError(t *testing.T) {
	interceptor := RecoveryStreamInterceptor(slog.New(slog.DiscardHandler))
	handlerErr := status.Error(codes.NotFound, "apply not found")

	err := interceptor("srv", testServerStream{ctx: t.Context()}, &grpc.StreamServerInfo{FullMethod: "/tern.v1.Tern/Logs"},
		func(srv any, ss grpc.ServerStream) error {
			return handlerErr
		})

	require.Error(t, err)
	assert.True(t, errors.Is(err, handlerErr))
}

// The server Run constructs must contain a handler panic end-to-end over a
// real gRPC connection: the client sees the fixed Internal error and the
// server keeps serving. This pins the interceptor wiring in newTernGRPCServer,
// not just the interceptor logic — building the server without the recovery
// chain would crash this test's server goroutine instead of failing the RPC.
func TestNewTernGRPCServerContainsHandlerPanicOverWire(t *testing.T) {
	var logs bytes.Buffer
	server := newTernGRPCServer(slog.New(slog.NewTextHandler(&logs, nil)))
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Panicker",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Boom",
			Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
				boom := func(ctx context.Context, req any) (any, error) {
					panic("dial tcp 10.0.0.5:5432: connect: connection refused")
				}
				if interceptor == nil {
					return boom(ctx, nil)
				}
				return interceptor(ctx, nil, &grpc.UnaryServerInfo{Server: srv, FullMethod: "/test.Panicker/Boom"}, boom)
			},
		}},
	}, struct{}{})

	listener := bufconn.Listen(1024 * 1024)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		require.NoError(t, <-serveErr)
	})

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	err = conn.Invoke(ctx, "/test.Panicker/Boom", &emptypb.Empty{}, &emptypb.Empty{})

	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.Internal, st.Code())
	assert.Equal(t, "internal error; see server logs", st.Message())
	assert.Contains(t, logs.String(), "connection refused")
	assert.Contains(t, logs.String(), "/test.Panicker/Boom")
}

type testServerStream struct {
	ctx context.Context
}

func (s testServerStream) SetHeader(metadata.MD) error  { return nil }
func (s testServerStream) SendHeader(metadata.MD) error { return nil }
func (s testServerStream) SetTrailer(metadata.MD)       {}
func (s testServerStream) Context() context.Context     { return s.ctx }
func (s testServerStream) SendMsg(any) error            { return nil }
func (s testServerStream) RecvMsg(any) error            { return nil }
