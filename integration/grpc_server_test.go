//go:build integration

package integration

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"

	schemabotapi "github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// newGRPCServer creates a new gRPC server wrapping a Client.
func newGRPCServer(client tern.Client) *tern.Server {
	return tern.NewServer(client, slog.Default())
}

// registerGRPCServer registers the tern server on the given grpc.Server.
func registerGRPCServer(srv *tern.Server, grpcSrv *grpc.Server) {
	srv.Register(grpcSrv)
}

// The harness terns share one Tern storage database, so one operator serves
// them all: it claims whichever tern's apply is ready and routes the drive to
// the client registered for that deployment. Driving a claim through the
// wrong database-bound client would run DDL against the wrong target.
var (
	remoteTernOperatorMu   sync.Mutex
	remoteTernOperatorSvc  *schemabotapi.Service
	remoteTernOperatorStop func()
)

// remoteTernEnvironment is the environment every harness dispatch targets; the
// operator routes claimed work by deployment/environment.
const remoteTernEnvironment = "staging"

// registerRemoteTern registers a simulated data-plane client under its
// deployment and starts the shared remote-tern operator on first use. A
// dispatch to a wrapped gRPC server queues the apply in Tern storage —
// production data planes drive queued applies from their own operator — so
// the harness runs the real operator (pkg/api) over the shared Tern storage,
// claiming at the operation level like an operation-claiming data plane.
// Register before the tern's gRPC server accepts dispatches, or a claimed
// apply would have no client for its deployment. Drive failures surface as
// apply state, which the tests assert on.
func registerRemoteTern(stor storage.Storage, logger *slog.Logger, deployment string, client tern.Client) error {
	remoteTernOperatorMu.Lock()
	defer remoteTernOperatorMu.Unlock()
	if remoteTernOperatorSvc == nil {
		svc := schemabotapi.New(stor, &schemabotapi.ServerConfig{}, nil, logger)
		if err := svc.SetOperatorPollInterval(100 * time.Millisecond); err != nil {
			return fmt.Errorf("set remote tern operator poll interval: %w", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		svc.StartOperator(ctx)
		remoteTernOperatorSvc = svc
		remoteTernOperatorStop = func() {
			cancel()
			svc.StopOperator()
		}
	}
	remoteTernOperatorSvc.RegisterTernClient(deployment, remoteTernEnvironment, client)
	return nil
}

// stopRemoteTernOperator stops the shared remote-tern operator. Safe to call
// when the operator never started, and safe to call more than once.
func stopRemoteTernOperator() {
	remoteTernOperatorMu.Lock()
	defer remoteTernOperatorMu.Unlock()
	if remoteTernOperatorStop == nil {
		return
	}
	remoteTernOperatorStop()
	remoteTernOperatorStop = nil
	remoteTernOperatorSvc = nil
}
