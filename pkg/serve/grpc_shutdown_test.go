package serve

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

// grpcStopDeadline bounds how long stopping the gRPC server may take. It is far
// above the drain the tests below give it and far below the RPC they park in
// flight, so neither a slow machine nor an early return can make the result
// ambiguous.
const grpcStopDeadline = 20 * time.Second

// parkedRPCServer returns a gRPC server with one method whose handler blocks
// until the test ends, and a channel closed once a client call has reached that
// handler — the point at which the server provably has an RPC in flight.
func parkedRPCServer(t *testing.T) (*grpc.Server, <-chan struct{}) {
	t.Helper()

	entered := make(chan struct{})
	release := make(chan struct{})

	server := grpc.NewServer()
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Parked",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Park",
			Handler: func(_ any, _ context.Context, _ func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				close(entered)
				<-release
				return &emptypb.Empty{}, nil
			},
		}},
	}, struct{}{})

	listener := bufconn.Listen(1024 * 1024)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	t.Cleanup(func() {
		// Whichever of the two paths the test took, the server is already
		// stopped by now; this collects Serve's return so the goroutine does
		// not outlive the test.
		server.Stop()
		assert.NoError(t, <-serveErr)
	})

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, conn.Close()) })

	go func() {
		// The call is parked for the whole test, so it only ever returns with
		// the server going away under it.
		_ = conn.Invoke(context.WithoutCancel(t.Context()), "/test.Parked/Park", &emptypb.Empty{}, &emptypb.Empty{})
	}()

	// Registered last so it runs first: cleanups run in reverse, and every
	// cleanup above waits on something the parked handler is holding. Releasing
	// it here is what lets the server come down at the end of the test — the
	// code under test deliberately does not wait for it to.
	t.Cleanup(func() { close(release) })

	return server, entered
}

// Stopping the gRPC server is bounded. It runs as a deferred call ahead of
// everything else shutdown does, so an RPC that never returns would otherwise
// park the whole shutdown behind it and the process would outlive its
// termination grace period on the one stage that had no bound. Once the drain
// is spent the wait ends and says what it left behind.
func TestStopGRPCServerReturnsWhenAnRPCOutlastsItsDrain(t *testing.T) {
	server, entered := parkedRPCServer(t)

	select {
	case <-entered:
	case <-time.After(grpcStopDeadline):
		t.Fatal("the test RPC never reached the server")
	}

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		stopGRPCServer(server, logger, 100*time.Millisecond)
	}()

	select {
	case <-returned:
	case <-time.After(grpcStopDeadline):
		t.Fatalf("stopping the gRPC server did not return within %s despite its drain", grpcStopDeadline)
	}
	assert.Contains(t, logs.String(), "leaving them and continuing shutdown")
}

// A server with nothing in flight stops on the graceful path and says nothing:
// the warning above is about work walked away from, and there was none.
func TestStopGRPCServerIsSilentWithNoRPCsInFlight(t *testing.T) {
	server := grpc.NewServer()

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		stopGRPCServer(server, logger, grpcStopDeadline)
	}()

	select {
	case <-returned:
	case <-time.After(grpcStopDeadline):
		t.Fatal("stopping an idle gRPC server did not return")
	}
	assert.Empty(t, logs.String())
}
