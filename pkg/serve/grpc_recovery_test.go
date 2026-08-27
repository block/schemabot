package serve

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
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
	assert.NotContains(t, st.Message(), "10.0.0.5")

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

type testServerStream struct {
	ctx context.Context
}

func (s testServerStream) SetHeader(metadata.MD) error  { return nil }
func (s testServerStream) SendHeader(metadata.MD) error { return nil }
func (s testServerStream) SetTrailer(metadata.MD)       {}
func (s testServerStream) Context() context.Context     { return s.ctx }
func (s testServerStream) SendMsg(any) error            { return nil }
func (s testServerStream) RecvMsg(any) error            { return nil }
