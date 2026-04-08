package tern

// gRPC Mode
//
// In gRPC mode, SchemaBot delegates schema change execution to a remote Tern
// service. This is useful for deployments where:
//
//   - The database is in a different network/VPC than SchemaBot
//   - You want to run Tern with different credentials or permissions
//   - You need to scale Tern services independently of SchemaBot
//
// # Architecture
//
// In gRPC mode:
//
//	┌──────────────┐         gRPC          ┌──────────────┐
//	│  SchemaBot   │ ───────────────────▶  │  Tern Server │
//	│              │                       │              │
//	│ • Routes     │                       │ • Has DB     │
//	│   requests   │                       │   configs    │
//	│ • Tracks     │                       │ • Runs       │
//	│   progress   │                       │   Spirit     │
//	└──────────────┘                       └──────────────┘
//
// SchemaBot only needs gRPC endpoint addresses in its config—database
// connection details (DSN, credentials) are configured on the Tern server.
//
// # Configuration
//
// SchemaBot config (only endpoints, no database details):
//
//	tern_deployments:
//	  default:
//	    staging: "tern-staging:9090"
//	    production: "tern-production:9090"
//
// The Tern server has the actual database configs (DSN, credentials, etc.)
// in its own configuration file.
//
// # Comparison with Local Mode
//
// Local mode (databases config):
//   - SchemaBot has full database configs (DSN, type, credentials)
//   - Uses LocalClient which connects directly to databases
//   - Single binary deployment—no separate Tern service
//
// gRPC mode (tern_deployments config):
//   - SchemaBot only knows gRPC endpoint addresses
//   - Uses GRPCClient which delegates to remote Tern servers
//   - Separate Tern services with their own database configs
//
// # Responsibilities
//
// Even in gRPC mode, SchemaBot still manages:
//   - Apply lifecycle tracking in its storage (for history, UI)
//   - Heartbeats to maintain lease on applies
//   - Progress polling from remote Tern
//
// The remote Tern server handles:
//   - Database connections and credentials
//   - Running Spirit or other schema change engines
//   - Actual schema change execution
//
// # external_id and apply_identifier
//
// These are intentionally different in gRPC mode:
//
//   - apply_identifier: SchemaBot's own UUID (e.g. "apply-abc123"), returned
//     to HTTP callers and used in all SchemaBot API endpoints.
//   - external_id: Tern's apply_id (the remote engine's apply identifier), used in all
//     gRPC calls to the remote Tern (Progress, Stop, Start, Cutover, etc.).
//
// gRPC mode flow:
//
//	CLI/caller
//	    │ apply_identifier="apply-abc123"
//	    ▼
//	SchemaBot HTTP API
//	    │ resolveApplyID("apply-abc123")
//	    │   → storage lookup → external_id="tern-42"
//	    ▼
//	GRPCClient.Progress(ApplyId: "tern-42")
//	    │
//	    ▼
//	Remote Tern (Remote Tern)
//	    │ looks up apply by id=42
//	    ▼
//	ProgressResponse
//
// The API layer (plan_handlers.go) generates apply_identifier as a SchemaBot
// UUID and stores Tern's resp.ApplyId as external_id. The resolveApplyID
// helper translates apply_identifier → external_id before calling Tern.
//
// In local mode (client.IsRemote() == false), the LocalClient runs in the
// same process and writes to the same database as the API layer. It creates
// the apply record (with no external_id) before ExecuteApply runs.
// ExecuteApply sees IsRemote() == false and keeps applyIdentifier =
// resp.ApplyId, leaving external_id empty. LocalClient uses apply_id
// when provided, falling back to database lookup.

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// GRPCClient implements Client using gRPC.
// It delegates execution to a remote Tern service but SchemaBot still manages
// the apply lifecycle (storage, heartbeats, progress tracking).
//
// See package-level documentation for details on gRPC mode architecture.
type GRPCClient struct {
	conn    *grpc.ClientConn
	client  ternv1.TernClient
	storage storage.Storage // SchemaBot's storage for apply/task management
}

// Compile-time check that GRPCClient implements Client.
var _ Client = (*GRPCClient)(nil)

// Config holds configuration for the gRPC client.
type Config struct {
	// Address is the gRPC server address (e.g., "localhost:9090").
	Address string

	// Storage is SchemaBot's storage for apply/task management.
	// Required for ResumeApply to work.
	Storage storage.Storage
}

// NewGRPCClient creates a new gRPC client connected to the given address.
func NewGRPCClient(config Config) (*GRPCClient, error) {
	conn, err := grpc.NewClient(
		config.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", config.Address, err)
	}

	return &GRPCClient{
		conn:    conn,
		client:  ternv1.NewTernClient(conn),
		storage: config.Storage,
	}, nil
}

// IsRemote returns true — GRPCClient delegates to a separate Tern service
// with its own storage. SchemaBot must create its own apply/task records
// and store Tern's apply_id as external_id.
func (c *GRPCClient) IsRemote() bool { return true }

// Close closes the gRPC connection.
func (c *GRPCClient) Close() error {
	return c.conn.Close()
}

func (c *GRPCClient) Plan(ctx context.Context, req *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	return c.client.Plan(ctx, req)
}

func (c *GRPCClient) Apply(ctx context.Context, req *ternv1.ApplyRequest) (*ternv1.ApplyResponse, error) {
	return c.client.Apply(ctx, req)
}

func (c *GRPCClient) Progress(ctx context.Context, req *ternv1.ProgressRequest) (*ternv1.ProgressResponse, error) {
	return c.client.Progress(ctx, req)
}

func (c *GRPCClient) Cutover(ctx context.Context, req *ternv1.CutoverRequest) (*ternv1.CutoverResponse, error) {
	return c.client.Cutover(ctx, req)
}

func (c *GRPCClient) Stop(ctx context.Context, req *ternv1.StopRequest) (*ternv1.StopResponse, error) {
	return c.client.Stop(ctx, req)
}

func (c *GRPCClient) Start(ctx context.Context, req *ternv1.StartRequest) (*ternv1.StartResponse, error) {
	return c.client.Start(ctx, req)
}

func (c *GRPCClient) Volume(ctx context.Context, req *ternv1.VolumeRequest) (*ternv1.VolumeResponse, error) {
	return c.client.Volume(ctx, req)
}

func (c *GRPCClient) Revert(ctx context.Context, req *ternv1.RevertRequest) (*ternv1.RevertResponse, error) {
	return c.client.Revert(ctx, req)
}

func (c *GRPCClient) SkipRevert(ctx context.Context, req *ternv1.SkipRevertRequest) (*ternv1.SkipRevertResponse, error) {
	return c.client.SkipRevert(ctx, req)
}

// RollbackPlan is not supported via gRPC client.
// Rollback functionality requires access to storage for plan lookup, which is only
// available in local mode. Use LocalClient for rollback operations.
func (c *GRPCClient) RollbackPlan(ctx context.Context, database string) (*ternv1.PlanResponse, error) {
	return nil, fmt.Errorf("rollback is not supported via gRPC client - use local mode")
}

func (c *GRPCClient) Health(ctx context.Context) error {
	_, err := c.client.Health(ctx, &ternv1.HealthRequest{})
	return err
}

// ResumeApply starts background progress tracking for an apply.
//
// Called from two places:
//   - ExecuteApply (fresh apply): the apply was just created in SchemaBot's
//     storage but the background poller hasn't started yet. State is "pending".
//   - RecoverInProgress (crash recovery): the apply's heartbeat expired,
//     indicating the previous poller died. State may be "stopped" (needs
//     Start RPC) or still "running"/"pending" (just needs a new poller).
//
// In contrast, LocalClient doesn't need this — it starts its own poller
// inside LocalClient.Apply() because it shares the same storage.
func (c *GRPCClient) ResumeApply(ctx context.Context, apply *storage.Apply) error {
	if c.storage == nil {
		return fmt.Errorf("storage not configured for GRPCClient")
	}

	// Check the real state from Tern before deciding what to do. Local state
	// may be stale (e.g. local says "stopped" but Tern already resumed).
	if apply.State == state.Apply.Stopped {
		resp, err := c.client.Progress(ctx, &ternv1.ProgressRequest{
			ApplyId:  apply.ExternalID,
			Database: apply.Database,
		})
		if err == nil {
			remoteState := ProtoStateToStorage(resp.State)
			if remoteState != "" {
				apply.State = remoteState
			}
		}

		// Only call Start if Tern confirms the apply is actually stopped.
		if apply.State == state.Apply.Stopped {
			_, err := c.client.Start(ctx, &ternv1.StartRequest{
				Database:    apply.Database,
				Environment: apply.Environment,
				ApplyId:     apply.ExternalID,
			})
			if err != nil {
				return fmt.Errorf("start via gRPC: %w", err)
			}
			now := time.Now()
			apply.State = state.Apply.Running
			if apply.StartedAt == nil {
				apply.StartedAt = &now
			}
		}

		if err := c.storage.Applies().Update(ctx, apply); err != nil {
			return fmt.Errorf("update apply state: %w", err)
		}
	}

	// Spawn background poller to track progress and maintain heartbeat
	go c.pollForCompletion(context.Background(), apply)

	return nil
}

// pollForCompletion polls the remote Tern for progress and updates SchemaBot's storage.
// Also maintains heartbeat to keep the lease on the apply.
func (c *GRPCClient) pollForCompletion(ctx context.Context, apply *storage.Apply) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	heartbeatTicker := time.NewTicker(10 * time.Second)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeatTicker.C:
			// Heartbeat: bump updated_at to maintain lease
			_ = c.storage.Applies().Heartbeat(ctx, apply.ID)
		case <-ticker.C:
			// Poll progress from remote Tern
			resp, err := c.client.Progress(ctx, &ternv1.ProgressRequest{
				Database:    apply.Database,
				Environment: apply.Environment,
				ApplyId:     apply.ExternalID,
			})
			if err != nil {
				continue
			}

			// Update apply state based on response (skip if proto state is unspecified)
			now := time.Now()
			if newState := ProtoStateToStorage(resp.State); newState != "" {
				if apply.StartedAt == nil && newState != state.Apply.Pending {
					apply.StartedAt = &now
				}
				apply.State = newState
			}
			apply.UpdatedAt = now

			// Update tasks from response. This must happen before the
			// terminal check so task records get their final state synced
			// before we return.
			if c.storage != nil {
				tasks, _ := c.storage.Tasks().GetByApplyID(ctx, apply.ID)
				for _, task := range tasks {
					for _, tp := range resp.Tables {
						if tp.TableName == task.TableName {
							task.State = state.NormalizeTaskStatus(tp.Status)
							task.RowsCopied = tp.RowsCopied
							task.RowsTotal = tp.RowsTotal
							task.ProgressPercent = int(tp.PercentComplete)
							task.UpdatedAt = now
							_ = c.storage.Tasks().Update(ctx, task)
							break
						}
					}
				}
			}

			terminal := isTerminalProtoState(resp.State)
			if terminal {
				apply.CompletedAt = &now
			}
			_ = c.storage.Applies().Update(ctx, apply)
			if terminal {
				return
			}
		}
	}
}
