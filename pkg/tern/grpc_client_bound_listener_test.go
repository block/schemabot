package tern

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

// healthOnlyTernServer answers Health and nothing else.
type healthOnlyTernServer struct {
	ternv1.UnimplementedTernServer
}

func (healthOnlyTernServer) Health(context.Context, *ternv1.HealthRequest) (*ternv1.HealthResponse, error) {
	return &ternv1.HealthResponse{Status: "ok"}, nil
}

// boundListenerClient binds a listener, registers a Tern server on it without
// calling Serve, and connects a client through the production constructor. The
// returned serve func starts Serve; the test decides when, or whether, to call
// it.
func boundListenerClient(t *testing.T) (client *GRPCClient, serve func()) {
	t.Helper()

	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "localhost:0")
	require.NoError(t, err, "failed to listen")

	grpcServer := grpc.NewServer()
	ternv1.RegisterTernServer(grpcServer, healthOnlyTernServer{})

	client, err = NewGRPCClient(Config{Address: lis.Addr().String()})
	require.NoError(t, err, "failed to create client")

	var served atomic.Bool
	serve = func() {
		served.Store(true)
		_ = grpcServer.Serve(lis)
	}

	t.Cleanup(func() {
		utils.CloseAndLog(client)
		grpcServer.Stop()
		// Stop closes the listener only when Serve has taken it.
		if !served.Load() {
			utils.CloseAndLog(lis)
		}
	})
	return client, serve
}

// A test harness that starts a gRPC server returns its address as soon as the
// listener is bound, without polling it for readiness. That is safe because a
// first RPC sent before Serve starts waits for the server instead of failing:
// the dial completes into the listen backlog, and the RPC waits while the
// connection is still connecting.
func TestGRPCClientFirstRPCWaitsForServeOnBoundListener(t *testing.T) {
	client, serve := boundListenerClient(t)

	const serveDelay = time.Second
	sent := time.Now()
	time.AfterFunc(serveDelay, serve)

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	require.NoError(t, client.Health(ctx), "first RPC must wait for Serve rather than fail")
	assert.GreaterOrEqual(t, time.Since(sent), serveDelay,
		"the RPC returned before Serve started, so it did not exercise the wait")
}

// The wait for a server that has not started Serve ends at the caller's
// deadline: the RPC returns DeadlineExceeded on time instead of hanging until
// gRPC's own connect timeout.
func TestGRPCClientFirstRPCHonorsCallerDeadlineBeforeServe(t *testing.T) {
	client, _ := boundListenerClient(t)

	const deadline = 200 * time.Millisecond
	ctx, cancel := context.WithTimeout(t.Context(), deadline)
	defer cancel()

	sent := time.Now()
	err := client.Health(ctx)
	elapsed := time.Since(sent)

	require.Error(t, err, "Serve never started, so the RPC cannot succeed")
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err), "unexpected error: %v", err)
	assert.Less(t, elapsed, 5*time.Second, "the RPC outlived the caller's deadline")
}
