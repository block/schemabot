//go:build integration

package integration

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// newGRPCServer creates a new gRPC server wrapping a Client.
func newGRPCServer(client tern.Client) *tern.Server {
	return tern.NewServer(client)
}

// registerGRPCServer registers the tern server on the given grpc.Server.
func registerGRPCServer(srv *tern.Server, grpcSrv *grpc.Server) {
	srv.Register(grpcSrv)
}

// remoteTernClients routes a claimed apply to the simulated data-plane client
// that serves its deployment. Several harness terns share one Tern storage
// database, so a claim loop can pick up any tern's apply; driving it through
// the wrong database-bound client would run DDL against the wrong target.
var remoteTernClients sync.Map // deployment → tern.Client

// registerRemoteTern adds a simulated data-plane client to the routing
// registry under its deployment (the LocalClient's database), so claim loops
// can drive its applies. Register before the tern's gRPC server accepts
// dispatches, or a claimed apply would have no client and burn its lease.
func registerRemoteTern(deployment string, client tern.Client) {
	remoteTernClients.Store(deployment, client)
}

// startRemoteTernOperator mimics the remote data plane's operator loop for the
// in-process Tern harness. A dispatch to the wrapped gRPC server queues the
// apply in Tern storage — production data planes drive queued applies from
// their own operator — so this loop claims work the way an operator driver
// does (FindNextApply under an owner, the claim's apply lease on the drive
// context) and drives each claim via the registered client for the apply's
// deployment, the routing key. Drive failures surface as apply state, which
// the tests assert on. Returns a stop func.
func startRemoteTernOperator(stor storage.Storage, logger *slog.Logger, owner string) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				apply, err := stor.Applies().FindNextApply(ctx, owner)
				if err != nil {
					// Storage failure, not no-work: without this log a broken claim
					// only surfaces as tests hanging to their timeouts.
					if ctx.Err() == nil {
						logger.Warn("remote tern operator: claim failed", "owner", owner, "error", err)
					}
					continue
				}
				if apply == nil {
					// No claimable work — keep polling.
					continue
				}
				clientAny, ok := remoteTernClients.Load(apply.Deployment)
				if !ok {
					// The claim burned this lease; the apply is re-claimable once its
					// heartbeat goes stale, by which point the tern should be registered.
					logger.Warn("remote tern operator: no registered client for claimed apply",
						"deployment", apply.Deployment, "apply_id", apply.ApplyIdentifier)
					continue
				}
				client := clientAny.(tern.Client)
				driveCtx := storage.WithApplyLease(ctx, apply.Lease())
				go func() {
					if err := client.ResumeApply(driveCtx, apply); err != nil && ctx.Err() == nil {
						logger.Warn("remote tern operator: resume apply", "apply_id", apply.ApplyIdentifier, "error", err)
					}
				}()
			}
		}
	}()
	return cancel
}
