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
// same process and writes to the same database as the API layer. API-created
// applies are queued in SchemaBot storage with no external_id, then scheduler
// workers dispatch them through LocalClient.ResumeApply().

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/metrics"
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
	address string          // dial address for logging/debugging
	storage storage.Storage // SchemaBot's storage for apply/task management

	// Observer support — same pattern as LocalClient.
	// For GRPCClient, the observer is notified by the local progress poller,
	// not by the remote engine.
	observerMu      sync.RWMutex
	observers       map[int64]ProgressObserver
	pendingObserver ProgressObserver
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
//
// The address may include a port (e.g. "tern.example.com:80"). The full
// address is used to dial, but the :authority pseudo-header is set to the
// hostname only (without the port) so that intermediaries route based on
// hostname rather than host:port.
func NewGRPCClient(config Config) (*GRPCClient, error) {
	host, _, err := net.SplitHostPort(config.Address)
	if err != nil {
		return nil, fmt.Errorf("split host:port from address %s: %w", config.Address, err)
	}

	conn, err := grpc.NewClient(
		config.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithAuthority(host),
	)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", config.Address, err)
	}

	return &GRPCClient{
		conn:    conn,
		client:  ternv1.NewTernClient(conn),
		address: config.Address,
		storage: config.Storage,
	}, nil
}

// IsRemote returns true — GRPCClient delegates to a separate Tern service
// with its own storage. SchemaBot must create its own apply/task records
// and store Tern's apply_id as external_id.
func (c *GRPCClient) IsRemote() bool { return true }

// Endpoint returns the gRPC dial address for this client.
func (c *GRPCClient) Endpoint() string { return c.address }

// SetPendingObserver sets an observer consumed by the next Apply() call.
func (c *GRPCClient) SetPendingObserver(observer ProgressObserver) {
	c.observerMu.Lock()
	defer c.observerMu.Unlock()
	c.pendingObserver = observer
}

// SetObserver registers a progress observer for an active apply.
func (c *GRPCClient) SetObserver(applyID int64, observer ProgressObserver) {
	c.observerMu.Lock()
	if observer == nil {
		delete(c.observers, applyID)
		c.observerMu.Unlock()
		return
	}
	if c.observers == nil {
		c.observers = make(map[int64]ProgressObserver)
	}
	_, alreadyWatching := c.observers[applyID]
	c.observers[applyID] = observer
	shouldStartPoller := c.storage != nil && !alreadyWatching
	c.observerMu.Unlock()

	if shouldStartPoller {
		go c.pollAndNotifyObserver(applyID)
	}
}

// Close closes the gRPC connection.
func (c *GRPCClient) Close() error {
	return c.conn.Close()
}

func (c *GRPCClient) Plan(ctx context.Context, req *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	return c.client.Plan(ctx, req)
}

func (c *GRPCClient) Apply(ctx context.Context, req *ternv1.ApplyRequest) (*ternv1.ApplyResponse, error) {
	resp, err := c.client.Apply(ctx, req)
	if err != nil {
		return nil, err
	}

	// Consume pending observer and start a storage-polling goroutine.
	// GRPCClient delegates execution to a remote tern server via gRPC, so
	// there's no local engine poller to call the observer. Instead, a
	// dedicated goroutine polls apply/task records from storage (which
	// are kept in sync by periodic Progress() gRPC calls) and notifies
	// the observer on each tick.
	if obs := c.consumePendingObserver(); obs != nil && c.storage != nil && resp.Accepted {
		// Look up the apply record to get the apply ID for the observer
		apply, lookupErr := c.storage.Applies().GetByApplyIdentifier(context.Background(), resp.ApplyId)
		if lookupErr == nil && apply != nil {
			if setter, ok := obs.(interface{ SetApplyID(int64) }); ok {
				setter.SetApplyID(apply.ID)
			}
			c.SetObserver(apply.ID, obs)
		}
	}

	return resp, nil
}

// consumePendingObserver returns and clears the pending observer.
func (c *GRPCClient) consumePendingObserver() ProgressObserver {
	c.observerMu.Lock()
	defer c.observerMu.Unlock()
	obs := c.pendingObserver
	c.pendingObserver = nil
	return obs
}

// getObserver returns the observer for an apply, or nil.
func (c *GRPCClient) getObserver(applyID int64) ProgressObserver {
	c.observerMu.RLock()
	defer c.observerMu.RUnlock()
	return c.observers[applyID]
}

// clearObserver removes the observer for an apply.
func (c *GRPCClient) clearObserver(applyID int64) {
	c.observerMu.Lock()
	defer c.observerMu.Unlock()
	delete(c.observers, applyID)
}

// pollAndNotifyObserver polls storage for apply state changes and notifies the
// observer. This is the GRPCClient equivalent of LocalClient's progress poller
// calling the observer — but driven by storage reads instead of engine polling.
func (c *GRPCClient) pollAndNotifyObserver(applyID int64) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		obs := c.getObserver(applyID)
		if obs == nil {
			// Observer was cleared — apply reached terminal state and
			// OnTerminal already ran. Stop polling.
			return
		}

		apply, err := c.storage.Applies().Get(context.Background(), applyID)
		if err != nil {
			slog.Error("observer poll: failed to load apply", "apply_id", applyID, "error", err)
			continue
		}
		if apply == nil {
			slog.Error("observer poll: apply not found", "apply_id", applyID)
			continue
		}

		tasks, err := c.storage.Tasks().GetByApplyID(context.Background(), applyID)
		if err != nil {
			slog.Error("observer poll: failed to load tasks", "apply_id", applyID, "error", err)
			continue
		}

		if state.IsTerminalApplyState(apply.State) {
			obs.OnTerminal(apply, tasks)
			c.clearObserver(applyID)
			return
		}

		obs.OnProgress(apply, tasks)
	}
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
// Called when a fresh remote apply needs progress tracking or when the apply's
// heartbeat expired, indicating the previous poller died. State may be stopped
// (needs a Start RPC) or already active (just needs a new poller).
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

type remoteApplyTransitionSkipReason string

const (
	remoteApplyTransitionReady        remoteApplyTransitionSkipReason = ""
	remoteApplyTransitionReloadFailed remoteApplyTransitionSkipReason = "reload_failed"
	remoteApplyTransitionMissing      remoteApplyTransitionSkipReason = "apply_missing"
	remoteApplyTransitionTerminal     remoteApplyTransitionSkipReason = "already_terminal"
)

func (c *GRPCClient) reloadRemoteApplyForTransition(ctx context.Context, apply *storage.Apply) (*storage.Apply, remoteApplyTransitionSkipReason, error) {
	currentApply, err := c.storage.Applies().Get(ctx, apply.ID)
	if err != nil {
		return nil, remoteApplyTransitionReloadFailed, fmt.Errorf("reload remote gRPC apply %s: %w", apply.ApplyIdentifier, err)
	}
	if currentApply == nil {
		return nil, remoteApplyTransitionMissing, nil
	}
	if state.IsTerminalApplyState(currentApply.State) {
		*apply = *currentApply
		return currentApply, remoteApplyTransitionTerminal, nil
	}
	return currentApply, remoteApplyTransitionReady, nil
}

func logSkippedRemoteApplyTransition(operation string, apply, currentApply *storage.Apply, reason remoteApplyTransitionSkipReason, err error) {
	fields := []any{
		"operation", operation,
		"apply_id", apply.ApplyIdentifier,
		"external_id", apply.ExternalID,
		"reason", reason,
	}
	if currentApply != nil {
		fields = append(fields, "state", currentApply.State)
	}

	switch reason {
	case remoteApplyTransitionReloadFailed:
		fields = append(fields, "error", err)
		slog.Error("skipping remote gRPC apply state transition", fields...)
	case remoteApplyTransitionMissing:
		slog.Warn("skipping remote gRPC apply state transition", fields...)
	case remoteApplyTransitionTerminal:
		slog.Debug("skipping remote gRPC apply state transition", fields...)
	default:
		slog.Warn("skipping remote gRPC apply state transition", fields...)
	}
}

func (c *GRPCClient) failApplyAfterRemoteProgressLoss(ctx context.Context, apply *storage.Apply, message string) {
	currentApply, skipReason, err := c.reloadRemoteApplyForTransition(ctx, apply)
	if skipReason != remoteApplyTransitionReady {
		logSkippedRemoteApplyTransition("fail apply after remote progress loss", apply, currentApply, skipReason, err)
		return
	}

	now := time.Now()
	tasks, taskErr := c.storage.Tasks().GetByApplyID(ctx, currentApply.ID)
	if taskErr != nil {
		slog.Error("failed to load tasks after remote gRPC apply failed",
			"apply_id", currentApply.ApplyIdentifier,
			"external_id", currentApply.ExternalID,
			"error", taskErr)
	}
	for _, task := range tasks {
		if state.IsTerminalTaskState(task.State) {
			continue
		}
		task.State = state.Task.Failed
		task.ErrorMessage = message
		task.CompletedAt = &now
		task.UpdatedAt = now
		if err := c.storage.Tasks().Update(ctx, task); err != nil {
			slog.Error("failed to update task after remote gRPC apply failure",
				"apply_id", currentApply.ApplyIdentifier,
				"task_id", task.TaskIdentifier,
				"error", err)
		}
	}

	currentApply.State = state.Apply.Failed
	currentApply.ErrorMessage = message
	currentApply.CompletedAt = &now
	currentApply.UpdatedAt = now
	if err := c.storage.Applies().Update(ctx, currentApply); err != nil {
		slog.Error("failed to update apply after remote gRPC apply failure",
			"apply_id", currentApply.ApplyIdentifier,
			"external_id", currentApply.ExternalID,
			"error", err)
		return
	}
	*apply = *currentApply
	metrics.AdjustActiveApplies(ctx, -1, currentApply.Database, currentApply.Environment)
}

func (c *GRPCClient) persistRemoteTerminalApply(ctx context.Context, apply *storage.Apply, now time.Time) bool {
	currentApply, skipReason, err := c.reloadRemoteApplyForTransition(ctx, apply)
	if skipReason != remoteApplyTransitionReady {
		logSkippedRemoteApplyTransition("persist remote terminal apply", apply, currentApply, skipReason, err)
		return skipReason == remoteApplyTransitionMissing || skipReason == remoteApplyTransitionTerminal
	}

	currentApply.State = apply.State
	currentApply.ErrorMessage = apply.ErrorMessage
	currentApply.StartedAt = apply.StartedAt
	currentApply.CompletedAt = &now
	currentApply.UpdatedAt = now
	if err := c.storage.Applies().Update(ctx, currentApply); err != nil {
		slog.Error("failed to update terminal remote gRPC apply",
			"apply_id", currentApply.ApplyIdentifier,
			"external_id", currentApply.ExternalID,
			"state", currentApply.State,
			"error", err)
		return false
	}
	*apply = *currentApply
	metrics.AdjustActiveApplies(ctx, -1, currentApply.Database, currentApply.Environment)
	return true
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
				if status.Code(err) == codes.NotFound {
					message := fmt.Sprintf("remote apply %s was not found by data plane", apply.ExternalID)
					c.failApplyAfterRemoteProgressLoss(ctx, apply, message)
					return
				}
				slog.Warn("remote gRPC progress poll failed",
					"apply_id", apply.ApplyIdentifier,
					"external_id", apply.ExternalID,
					"database", apply.Database,
					"environment", apply.Environment,
					"error", err)
				continue
			}
			if resp.State == ternv1.State_STATE_NO_ACTIVE_CHANGE {
				message := fmt.Sprintf("remote apply %s returned no active schema change for exact apply_id", apply.ExternalID)
				c.failApplyAfterRemoteProgressLoss(ctx, apply, message)
				return
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
				if c.persistRemoteTerminalApply(ctx, apply, now) {
					return
				}
				continue
			}
			if err := c.storage.Applies().Update(ctx, apply); err != nil {
				slog.Error("failed to update remote gRPC apply progress",
					"apply_id", apply.ApplyIdentifier,
					"external_id", apply.ExternalID,
					"state", apply.State,
					"error", err)
			}
		}
	}
}
