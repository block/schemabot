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
// gRPC mode progress flow after scheduler dispatch:
//
//	CLI/caller
//	    │ apply_identifier="apply-abc123"
//	    ▼
//	SchemaBot HTTP API
//	    │ storage lookup → external_id="tern-42"
//	    ▼
//	GRPCClient.Progress(ApplyId: "tern-42")
//	    │
//	    ▼
//	Remote Tern
//	    │ looks up apply by id=42
//	    ▼
//	ProgressResponse
//
// The API layer generates apply_identifier as a SchemaBot UUID when it queues
// the apply. The scheduler later dispatches the queued apply to remote Tern and
// stores Tern's ApplyId as external_id. Apply-scoped HTTP handlers load the
// stored apply row and send external_id to Tern when it is present.
//
// In local mode, LocalClient runs in the same process and writes to the same
// database as the API layer. There is no remote Tern ID, so apply-scoped HTTP
// handlers send the SchemaBot apply_identifier to LocalClient.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/metrics"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

const grpcProgressPollInterval = 500 * time.Millisecond

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

// logApplyEvent appends a control-plane apply log entry for gRPC applies. The
// remote Tern service writes its own local logs, but operators read SchemaBot's
// control-plane apply history from SchemaBot storage.
func (c *GRPCClient) logApplyEvent(ctx context.Context, applyID int64, taskID *int64, level, eventType, source, message string, oldState, newState string) {
	logStore := c.storage.ApplyLogs()
	if logStore == nil {
		slog.Error("missing apply log store for gRPC apply event",
			"apply_id", applyID,
			"event", eventType,
			"message", message)
		return
	}
	log := &storage.ApplyLog{
		ApplyID:   applyID,
		TaskID:    taskID,
		Level:     level,
		EventType: eventType,
		Source:    source,
		Message:   message,
		OldState:  oldState,
		NewState:  newState,
		CreatedAt: time.Now(),
	}
	if err := logStore.Append(ctx, log); err != nil {
		slog.Error("failed to log gRPC apply event",
			"apply_id", applyID,
			"event", eventType,
			"message", message,
			"error", err)
	}
}

func (c *GRPCClient) logApplyStateTransition(ctx context.Context, apply *storage.Apply, level, message, oldState string) {
	c.logApplyEvent(ctx, apply.ID, nil, level, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
		message, oldState, apply.State)
}

func (c *GRPCClient) logTaskStateTransition(ctx context.Context, applyID int64, task *storage.Task, message, oldState string) {
	taskID := task.ID
	c.logApplyEvent(ctx, applyID, &taskID, storage.LogLevelInfo, storage.LogEventStateTransition, storage.LogSourceSchemaBot,
		message, oldState, task.State)
}

func (c *GRPCClient) logApplyWarning(ctx context.Context, apply *storage.Apply, message string) {
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, storage.LogEventError, storage.LogSourceSchemaBot,
		message, apply.State, apply.State)
}

func remoteApplyStateDescription(remoteState ternv1.State) string {
	return fmt.Sprintf("%s(%d)", remoteState.String(), int32(remoteState))
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

// ResumeApply runs work claimed by the scheduler. Fresh queued applies have no
// external_id yet, so this method first dispatches them to remote Tern and
// stores the returned ID. The call then polls until the apply reaches a stored
// terminal state or the scheduler context is canceled.
func (c *GRPCClient) ResumeApply(ctx context.Context, apply *storage.Apply) error {
	if c.storage == nil {
		return fmt.Errorf("storage not configured for GRPCClient")
	}
	if apply == nil {
		return fmt.Errorf("apply is required")
	}

	if shouldDispatchQueuedRemoteApply(apply) {
		return c.dispatchPendingApply(ctx, apply)
	}
	if hasAmbiguousRemoteDispatchState(apply) {
		errMsg := fmt.Sprintf("gRPC apply %s is %s without external_id; remote dispatch state is ambiguous", apply.ApplyIdentifier, apply.State)
		if err := c.markRemoteApplyFailed(ctx, apply, nil, errMsg, false); err != nil {
			return fmt.Errorf("%s; persist failure state: %w", errMsg, err)
		}
		return errors.New(errMsg)
	}

	if apply.ExternalID != "" && state.IsState(apply.State, state.Apply.Pending) {
		_, err := c.client.Start(ctx, &ternv1.StartRequest{
			Environment: apply.Environment,
			ApplyId:     apply.ExternalID,
		})
		if err != nil {
			return fmt.Errorf("start queued gRPC apply %s: %w", apply.ApplyIdentifier, err)
		}
		now := time.Now()
		apply.State = state.Apply.Running
		if apply.StartedAt == nil {
			apply.StartedAt = &now
		}
		if err := c.storage.Applies().Update(ctx, apply); err != nil {
			return fmt.Errorf("update started gRPC apply %s: %w", apply.ApplyIdentifier, err)
		}
		c.logApplyStateTransition(ctx, apply, storage.LogLevelInfo, "Remote apply start requested by scheduler", state.Apply.Pending)
	}

	// Check the real state from Tern before deciding what to do. Stored state
	// may be stale (e.g. storage says "stopped" but Tern already resumed).
	if state.IsState(apply.State, state.Apply.Stopped) {
		oldState := apply.State
		startRequested := false
		resp, err := c.client.Progress(ctx, &ternv1.ProgressRequest{
			ApplyId:     apply.ExternalID,
			Environment: apply.Environment,
		})
		if err == nil {
			if resp.State == ternv1.State_STATE_NO_ACTIVE_CHANGE {
				message := fmt.Sprintf("Remote stopped-state check returned no active schema change for remote apply %s; scheduler will not request remote start", apply.ExternalID)
				slog.Warn("remote gRPC stopped-state check returned no active schema change; scheduler will not request remote start",
					"apply_id", apply.ApplyIdentifier,
					"external_id", apply.ExternalID,
					"database", apply.Database,
					"environment", apply.Environment,
					"stored_state", apply.State)
				c.logApplyWarning(ctx, apply, message)
				return fmt.Errorf("check stopped gRPC apply %s before start: no active schema change for remote apply %s", apply.ApplyIdentifier, apply.ExternalID)
			}
			remoteState := ProtoStateToStorage(resp.State)
			if remoteState == "" {
				message := fmt.Sprintf("Remote stopped-state check returned unmapped state %s; scheduler will not request remote start", remoteApplyStateDescription(resp.State))
				slog.Warn("remote gRPC stopped-state check returned unmapped state; scheduler will not request remote start",
					"apply_id", apply.ApplyIdentifier,
					"external_id", apply.ExternalID,
					"database", apply.Database,
					"environment", apply.Environment,
					"remote_state", resp.State.String(),
					"remote_state_number", int32(resp.State),
					"stored_state", apply.State)
				c.logApplyWarning(ctx, apply, message)
				return fmt.Errorf("check stopped gRPC apply %s before start: unmapped remote state %s", apply.ApplyIdentifier, remoteApplyStateDescription(resp.State))
			}
			apply.State = remoteState
			if isTerminalProtoState(resp.State) && !state.IsState(remoteState, state.Apply.Stopped) {
				now := time.Now()
				if apply.StartedAt == nil && !state.IsState(remoteState, state.Apply.Pending) {
					apply.StartedAt = &now
				}
				return c.reconcileTerminalRemoteProgress(ctx, apply, resp.Tables, now)
			}
		} else {
			message := fmt.Sprintf("Remote stopped-state check failed before scheduler start: %v", err)
			slog.Warn("remote gRPC stopped-state check failed; scheduler will not request remote start",
				"apply_id", apply.ApplyIdentifier,
				"external_id", apply.ExternalID,
				"database", apply.Database,
				"environment", apply.Environment,
				"error", err)
			c.logApplyWarning(ctx, apply, message)
			return fmt.Errorf("check stopped gRPC apply %s before start: %w", apply.ApplyIdentifier, err)
		}

		// Only call Start if Tern confirms the apply is actually stopped.
		if state.IsState(apply.State, state.Apply.Stopped) {
			_, err := c.client.Start(ctx, &ternv1.StartRequest{
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
			startRequested = true
		}

		if err := c.storage.Applies().Update(ctx, apply); err != nil {
			return fmt.Errorf("update apply state: %w", err)
		}
		if startRequested {
			c.logApplyStateTransition(ctx, apply, storage.LogLevelInfo, "Remote apply start requested by scheduler", oldState)
		} else if oldState != apply.State {
			c.logApplyStateTransition(ctx, apply, storage.LogLevelInfo, fmt.Sprintf("Remote apply state refreshed before scheduler start: %s -> %s", oldState, apply.State), oldState)
		}
	}

	return c.pollForCompletion(ctx, apply)
}

func shouldDispatchQueuedRemoteApply(apply *storage.Apply) bool {
	if apply == nil {
		return false
	}
	return apply.ExternalID == "" && state.IsState(apply.State, state.Apply.Pending, state.Apply.FailedRetryable)
}

func hasAmbiguousRemoteDispatchState(apply *storage.Apply) bool {
	if apply == nil {
		return false
	}
	return apply.ExternalID == "" &&
		!state.IsTerminalApplyState(apply.State) &&
		!shouldDispatchQueuedRemoteApply(apply)
}

func (c *GRPCClient) dispatchPendingApply(ctx context.Context, apply *storage.Apply) error {
	plan, err := c.storage.Plans().GetByID(ctx, apply.PlanID)
	if err != nil {
		if markErr := c.markRemoteApplyFailed(ctx, apply, nil, fmt.Sprintf("queued gRPC apply failed: load plan %d: %v", apply.PlanID, err), false); markErr != nil {
			return fmt.Errorf("mark queued gRPC apply %s failed after plan load error: %w", apply.ApplyIdentifier, markErr)
		}
		return fmt.Errorf("load plan %d for queued gRPC apply %s: %w", apply.PlanID, apply.ApplyIdentifier, err)
	}
	if plan == nil {
		errMsg := fmt.Sprintf("queued gRPC apply failed: plan %d not found", apply.PlanID)
		if markErr := c.markRemoteApplyFailed(ctx, apply, nil, errMsg, false); markErr != nil {
			return fmt.Errorf("mark queued gRPC apply %s failed after missing plan: %w", apply.ApplyIdentifier, markErr)
		}
		return fmt.Errorf("queued gRPC apply %s: %s", apply.ApplyIdentifier, errMsg)
	}

	tasks, err := c.storage.Tasks().GetByApplyID(ctx, apply.ID)
	if err != nil {
		if markErr := c.markRemoteApplyFailed(ctx, apply, nil, fmt.Sprintf("queued gRPC apply failed: load tasks: %v", err), false); markErr != nil {
			return fmt.Errorf("mark queued gRPC apply %s failed after task load error: %w", apply.ApplyIdentifier, markErr)
		}
		return fmt.Errorf("load tasks for queued gRPC apply %s: %w", apply.ApplyIdentifier, err)
	}
	if len(tasks) == 0 {
		errMsg := "queued gRPC apply failed: no tasks found"
		if markErr := c.markRemoteApplyFailed(ctx, apply, nil, errMsg, false); markErr != nil {
			return fmt.Errorf("mark queued gRPC apply %s failed after missing tasks: %w", apply.ApplyIdentifier, markErr)
		}
		return fmt.Errorf("queued gRPC apply %s: %s", apply.ApplyIdentifier, errMsg)
	}
	if err := c.prepareDispatchTasks(ctx, apply, tasks); err != nil {
		return err
	}

	options := apply.GetOptions().Map()
	target := options["target"]
	if target == "" {
		target = apply.Database
	}

	resp, err := c.client.Apply(ctx, &ternv1.ApplyRequest{
		PlanId:      plan.PlanIdentifier,
		Options:     options,
		Database:    apply.Database,
		Type:        apply.DatabaseType,
		DdlChanges:  tasksToProtoTableChanges(tasks),
		Environment: apply.Environment,
		Target:      target,
		Caller:      apply.Caller,
	})
	if err != nil {
		if isAmbiguousRemoteApplyDispatchError(err) {
			return fmt.Errorf("apply queued gRPC apply %s has ambiguous remote dispatch outcome: %w", apply.ApplyIdentifier, err)
		}
		if markErr := c.markRemoteApplyFailed(ctx, apply, tasks, err.Error(), isRetryableRemoteApplyError(err)); markErr != nil {
			return fmt.Errorf("mark queued gRPC apply %s failed after remote apply error: %w", apply.ApplyIdentifier, markErr)
		}
		return fmt.Errorf("apply queued gRPC apply %s: %w", apply.ApplyIdentifier, err)
	}
	if resp == nil {
		errMsg := "remote apply returned nil response"
		if markErr := c.markRemoteApplyFailed(ctx, apply, tasks, errMsg, false); markErr != nil {
			return fmt.Errorf("mark queued gRPC apply %s failed after nil response: %w", apply.ApplyIdentifier, markErr)
		}
		return fmt.Errorf("apply queued gRPC apply %s: %s", apply.ApplyIdentifier, errMsg)
	}
	if !resp.Accepted {
		errMsg := resp.ErrorMessage
		if errMsg == "" {
			errMsg = "remote apply was not accepted"
		}
		if markErr := c.markRemoteApplyFailed(ctx, apply, tasks, errMsg, false); markErr != nil {
			return fmt.Errorf("mark queued gRPC apply %s failed after rejection: %w", apply.ApplyIdentifier, markErr)
		}
		return fmt.Errorf("apply queued gRPC apply %s: %s", apply.ApplyIdentifier, errMsg)
	}
	if resp.ApplyId == "" {
		errMsg := "remote apply accepted without apply_id"
		if markErr := c.markRemoteApplyFailed(ctx, apply, tasks, errMsg, false); markErr != nil {
			return fmt.Errorf("mark queued gRPC apply %s failed after missing remote apply id: %w", apply.ApplyIdentifier, markErr)
		}
		return fmt.Errorf("apply queued gRPC apply %s: %s", apply.ApplyIdentifier, errMsg)
	}

	oldApplyState := apply.State
	now := time.Now()
	apply.ExternalID = resp.ApplyId
	apply.State = state.Apply.Running
	apply.ErrorMessage = ""
	apply.CompletedAt = nil
	if apply.StartedAt == nil {
		apply.StartedAt = &now
	}
	apply.UpdatedAt = now
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		return fmt.Errorf("store remote apply id for %s: %w", apply.ApplyIdentifier, err)
	}
	c.logApplyStateTransition(ctx, apply, storage.LogLevelInfo,
		fmt.Sprintf("Apply dispatched to remote Tern: target=%s deployment=%s remote_apply_id=%s", target, apply.Deployment, apply.ExternalID),
		oldApplyState)

	return c.pollForCompletion(ctx, apply)
}

func isAmbiguousRemoteApplyDispatchError(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		status.Code(err) == codes.Canceled ||
		status.Code(err) == codes.DeadlineExceeded
}

// isRetryableRemoteApplyError classifies a definite remote Apply rejection.
// Ambiguous cancellation/deadline errors are handled before this path because
// the control plane cannot know whether the data plane accepted the request.
func isRetryableRemoteApplyError(err error) bool {
	if err == nil {
		return false
	}
	if isAmbiguousRemoteApplyDispatchError(err) {
		return false
	}

	st, ok := status.FromError(err)
	if !ok {
		if engine.IsTransientTransportError(err) {
			return true
		}
		return engine.IsRetryable(err)
	}

	switch st.Code() {
	case codes.Internal, codes.Unknown, codes.Unavailable, codes.ResourceExhausted, codes.Aborted:
		return true
	case codes.Canceled, codes.DeadlineExceeded:
		return false
	case codes.OK, codes.InvalidArgument, codes.NotFound, codes.AlreadyExists, codes.PermissionDenied,
		codes.Unauthenticated, codes.FailedPrecondition, codes.OutOfRange, codes.Unimplemented, codes.DataLoss:
		return false
	default:
		return false
	}
}

func (c *GRPCClient) prepareDispatchTasks(ctx context.Context, apply *storage.Apply, tasks []*storage.Task) error {
	for _, task := range tasks {
		if !state.IsState(task.State, state.Task.FailedRetryable) {
			continue
		}
		task.State = state.Task.Pending
		task.ErrorMessage = ""
		task.CompletedAt = nil
		task.Attempt++
		if err := c.storage.Tasks().Update(ctx, task); err != nil {
			return fmt.Errorf("reset retryable task %s for queued gRPC apply %s: %w", task.TaskIdentifier, apply.ApplyIdentifier, err)
		}
	}
	return nil
}

func tasksToProtoTableChanges(tasks []*storage.Task) []*ternv1.TableChange {
	changes := make([]*ternv1.TableChange, 0, len(tasks))
	for _, task := range tasks {
		changes = append(changes, &ternv1.TableChange{
			TableName:  task.TableName,
			Ddl:        task.DDL,
			ChangeType: ddlActionToProtoChangeType(task.DDLAction),
			Namespace:  task.Namespace,
		})
	}
	return changes
}

// storedApplyTransitionStatus describes whether a worker may copy a remote
// failure or terminal result into the stored apply row after reloading storage.
// Only the ready status may mutate storage; every other status explains why the
// write must be skipped or retried.
type storedApplyTransitionStatus string

const (
	storedApplyTransitionReady           storedApplyTransitionStatus = "ready"
	storedApplyTransitionReloadFailed    storedApplyTransitionStatus = "reload_failed"
	storedApplyTransitionMissing         storedApplyTransitionStatus = "apply_missing"
	storedApplyTransitionAlreadyTerminal storedApplyTransitionStatus = "already_terminal"
)

func (c *GRPCClient) reloadStoredApplyForRemoteTransition(ctx context.Context, remoteApply *storage.Apply) (*storage.Apply, storedApplyTransitionStatus, error) {
	storedApply, err := c.storage.Applies().Get(ctx, remoteApply.ID)
	if err != nil {
		return nil, storedApplyTransitionReloadFailed, fmt.Errorf("reload remote gRPC apply %s: %w", remoteApply.ApplyIdentifier, err)
	}
	if storedApply == nil {
		return nil, storedApplyTransitionMissing, nil
	}
	if state.IsTerminalApplyState(storedApply.State) {
		*remoteApply = *storedApply
		return storedApply, storedApplyTransitionAlreadyTerminal, nil
	}
	return storedApply, storedApplyTransitionReady, nil
}

func logSkippedRemoteApplyTransition(operation string, remoteApply, storedApply *storage.Apply, status storedApplyTransitionStatus, err error) {
	fields := []any{
		"operation", operation,
		"apply_id", remoteApply.ApplyIdentifier,
		"external_id", remoteApply.ExternalID,
		"reason", status,
	}
	if storedApply != nil {
		fields = append(fields, "stored_state", storedApply.State)
	}

	switch status {
	case storedApplyTransitionReloadFailed:
		fields = append(fields, "error", err)
		slog.Error("skipping remote gRPC apply state transition", fields...)
	case storedApplyTransitionMissing:
		slog.Warn("skipping remote gRPC apply state transition", fields...)
	case storedApplyTransitionAlreadyTerminal:
		slog.Debug("skipping remote gRPC apply state transition", fields...)
	default:
		slog.Warn("skipping remote gRPC apply state transition", fields...)
	}
}

func (c *GRPCClient) markRemoteApplyFailed(ctx context.Context, remoteApply *storage.Apply, storedTasks []*storage.Task, message string, retryable bool) error {
	storedApply, transitionStatus, err := c.reloadStoredApplyForRemoteTransition(ctx, remoteApply)
	if transitionStatus != storedApplyTransitionReady {
		logSkippedRemoteApplyTransition("mark remote gRPC apply failed", remoteApply, storedApply, transitionStatus, err)
		if err != nil {
			return err
		}
		return nil
	}

	now := time.Now()
	if storedTasks == nil {
		var taskErr error
		storedTasks, taskErr = c.storage.Tasks().GetByApplyID(ctx, storedApply.ID)
		if taskErr != nil {
			return fmt.Errorf("load tasks after remote gRPC apply failed %s: %w", storedApply.ApplyIdentifier, taskErr)
		}
	}

	taskState := state.Task.Failed
	applyState := state.Apply.Failed
	if retryable {
		taskState = state.Task.FailedRetryable
		applyState = state.Apply.FailedRetryable
	}
	for _, storedTask := range storedTasks {
		if state.IsTerminalTaskState(storedTask.State) {
			slog.Info("leaving terminal gRPC task unchanged after remote apply failure",
				"apply_id", storedApply.ApplyIdentifier,
				"task_id", storedTask.TaskIdentifier,
				"table", storedTask.TableName,
				"task_state", storedTask.State,
				"failure_task_state", taskState)
			continue
		}
		storedTask.State = taskState
		storedTask.ErrorMessage = message
		if retryable {
			storedTask.CompletedAt = nil
		} else {
			storedTask.CompletedAt = &now
		}
		storedTask.UpdatedAt = now
		if err := c.storage.Tasks().Update(ctx, storedTask); err != nil {
			return fmt.Errorf("update task %s after remote gRPC apply failure %s: %w", storedTask.TaskIdentifier, storedApply.ApplyIdentifier, err)
		}
	}

	oldState := storedApply.State
	storedApply.State = applyState
	storedApply.ErrorMessage = message
	if retryable {
		storedApply.CompletedAt = nil
	} else {
		storedApply.CompletedAt = &now
	}
	storedApply.UpdatedAt = now
	if err := c.storage.Applies().Update(ctx, storedApply); err != nil {
		return fmt.Errorf("update remote gRPC apply failure %s: %w", storedApply.ApplyIdentifier, err)
	}
	c.logApplyStateTransition(ctx, storedApply, storage.LogLevelError, fmt.Sprintf("Remote apply failed: %s", message), oldState)
	*remoteApply = *storedApply
	metrics.AdjustActiveApplies(ctx, -1, storedApply.Database, storedApply.Environment)
	return nil
}

func (c *GRPCClient) failMissingRemoteApply(ctx context.Context, remoteApply *storage.Apply, message string, cause error) error {
	if err := c.markRemoteApplyFailed(ctx, remoteApply, nil, message, false); err != nil {
		return fmt.Errorf("mark missing remote apply %s failed: %w", remoteApply.ApplyIdentifier, err)
	}
	if cause != nil {
		return fmt.Errorf("poll remote apply %s for %s: %w", remoteApply.ExternalID, remoteApply.ApplyIdentifier, cause)
	}
	return fmt.Errorf("poll remote apply %s for %s: %s", remoteApply.ExternalID, remoteApply.ApplyIdentifier, message)
}

func (c *GRPCClient) reconcileTerminalRemoteProgress(ctx context.Context, remoteApply *storage.Apply, remoteTasks []*ternv1.TableProgress, now time.Time) error {
	// reloadStoredApplyForRemoteTransition may overwrite remoteApply with the
	// stored row when it finds an already-terminal stored apply. Keep the remote
	// Progress result available for the stopped-row exception below.
	remoteApplyFromProgress := *remoteApply
	storedApply, transitionStatus, err := c.reloadStoredApplyForRemoteTransition(ctx, remoteApply)

	// A scheduler claim can start from a stale stored "stopped" row. If the
	// exact remote apply has already advanced to another terminal state, the
	// remote result is the newer truth and should replace the stored stopped row.
	if transitionStatus == storedApplyTransitionAlreadyTerminal && storedStoppedApplyCanAdoptRemoteTerminalState(storedApply, &remoteApplyFromProgress) {
		*remoteApply = remoteApplyFromProgress
		transitionStatus = storedApplyTransitionReady
	}

	if transitionStatus != storedApplyTransitionReady {
		logSkippedRemoteApplyTransition("persist remote terminal apply", remoteApply, storedApply, transitionStatus, err)
		if err != nil {
			return err
		}
		return nil
	}

	// Keep the stored apply active until stored task rows are written. If task
	// storage is unavailable, the scheduler can retry this worker instead of
	// treating a terminal apply as fully reconciled.
	storedTasks, err := c.storage.Tasks().GetByApplyID(ctx, storedApply.ID)
	if err != nil {
		return fmt.Errorf("load tasks to sync terminal gRPC progress for %s: %w", storedApply.ApplyIdentifier, err)
	}
	if err := c.syncStoredTasksFromRemoteTasks(ctx, storedApply, storedTasks, remoteTasks, now); err != nil {
		return err
	}
	if err := ensureStoredTasksResolvedForTerminalRemoteApply(remoteApply, storedTasks); err != nil {
		return err
	}
	return c.persistTerminalStateFromRemote(ctx, storedApply, remoteApply, now)
}

func storedStoppedApplyCanAdoptRemoteTerminalState(storedApply, remoteApply *storage.Apply) bool {
	return storedApply != nil &&
		state.IsState(storedApply.State, state.Apply.Stopped) &&
		!state.IsState(remoteApply.State, state.Apply.Stopped)
}

func (c *GRPCClient) persistTerminalStateFromRemote(ctx context.Context, storedApply, remoteApply *storage.Apply, now time.Time) error {
	oldState := storedApply.State
	storedApply.State = remoteApply.State
	storedApply.ErrorMessage = remoteApply.ErrorMessage
	storedApply.StartedAt = remoteApply.StartedAt
	storedApply.CompletedAt = &now
	storedApply.UpdatedAt = now
	if err := c.storage.Applies().Update(ctx, storedApply); err != nil {
		return fmt.Errorf("update terminal remote gRPC apply %s: %w", storedApply.ApplyIdentifier, err)
	}
	c.logApplyStateTransition(ctx, storedApply, storage.LogLevelInfo, fmt.Sprintf("Remote apply reached terminal state: %s", storedApply.State), oldState)
	*remoteApply = *storedApply
	metrics.AdjustActiveApplies(ctx, -1, storedApply.Database, storedApply.Environment)
	return nil
}

// syncStoredTasksFromRemoteTasks mirrors the per-task table progress fields
// returned by remote Tern. It only copies the remote task snapshot; terminal
// remote applies are persisted only after those copied task states are resolved.
func (c *GRPCClient) syncStoredTasksFromRemoteTasks(
	ctx context.Context,
	storedApply *storage.Apply,
	storedTasks []*storage.Task,
	remoteTasks []*ternv1.TableProgress,
	now time.Time,
) error {
	remoteTaskIndex := indexProtoTableProgress(remoteTasks)
	missingProgressTasks := 0
	for _, storedTask := range storedTasks {
		remoteTask, ok := protoProgressForTask(remoteTaskIndex, storedTask)
		if !ok {
			missingProgressTasks++
			continue
		}
		oldTaskState := storedTask.State
		remoteTaskState := state.NormalizeTaskStatus(remoteTask.Status)
		if state.IsState(remoteTaskState, state.Task.Stopped) {
			storedTask.State = remoteTaskState
		} else {
			storedTask.State = taskStateWithNoBackwardProgress(storedTask.State, remoteTaskState)
		}
		if !state.IsState(storedTask.State, remoteTaskState) {
			slog.Debug("keeping stored gRPC task state because remote progress reported earlier state",
				"apply_id", storedApply.ApplyIdentifier,
				"external_id", storedApply.ExternalID,
				"task_id", storedTask.TaskIdentifier,
				"table", storedTask.TableName,
				"stored_task_state", oldTaskState,
				"remote_task_state", remoteTaskState)
		}
		if remoteTaskOmittedRowTotals(storedTask, remoteTask) {
			slog.Debug("keeping stored gRPC task row-copy progress because remote progress omitted row totals",
				"apply_id", storedApply.ApplyIdentifier,
				"external_id", storedApply.ExternalID,
				"task_id", storedTask.TaskIdentifier,
				"namespace", storedTask.Namespace,
				"table", storedTask.TableName,
				"stored_rows_copied", storedTask.RowsCopied,
				"stored_rows_total", storedTask.RowsTotal,
				"stored_progress_percent", storedTask.ProgressPercent,
				"remote_rows_copied", remoteTask.RowsCopied,
				"remote_progress_percent", remoteTask.PercentComplete)
		} else {
			storedTask.RowsCopied = remoteTask.RowsCopied
			storedTask.RowsTotal = remoteTask.RowsTotal
			storedTask.ProgressPercent = int(remoteTask.PercentComplete)
		}
		if state.IsState(storedTask.State, state.Task.Completed) && storedTask.ProgressPercent != 100 {
			storedTask.ProgressPercent = 100
		}
		if state.IsTerminalTaskState(storedTask.State) && storedTask.CompletedAt == nil {
			storedTask.CompletedAt = &now
		}
		storedTask.UpdatedAt = now
		if err := c.storage.Tasks().Update(ctx, storedTask); err != nil {
			return fmt.Errorf("sync task %s from gRPC progress for %s: %w", storedTask.TaskIdentifier, storedApply.ApplyIdentifier, err)
		}
		if oldTaskState != storedTask.State {
			c.logTaskStateTransition(ctx, storedApply.ID, storedTask, fmt.Sprintf("Remote task %s changed state: %s -> %s", storedTask.TableName, oldTaskState, storedTask.State), oldTaskState)
		}
	}
	if missingProgressTasks > 0 {
		slog.Warn("remote gRPC progress omitted stored tasks",
			"apply_id", storedApply.ApplyIdentifier,
			"external_id", storedApply.ExternalID,
			"database", storedApply.Database,
			"environment", storedApply.Environment,
			"missing_count", missingProgressTasks)
	}
	return nil
}

func remoteTaskOmittedRowTotals(storedTask *storage.Task, remoteTask *ternv1.TableProgress) bool {
	if storedTask == nil || remoteTask == nil {
		return false
	}
	return storedTask.RowsTotal > 0 && remoteTask.RowsTotal <= 0
}

func ensureStoredTasksResolvedForTerminalRemoteApply(remoteApply *storage.Apply, storedTasks []*storage.Task) error {
	for _, storedTask := range storedTasks {
		if storedTaskResolvedForTerminalRemoteApply(remoteApply.State, storedTask.State) {
			continue
		}
		slog.Warn("terminal remote gRPC apply still has unresolved stored task after syncing remote task progress",
			"apply_id", remoteApply.ApplyIdentifier,
			"external_id", remoteApply.ExternalID,
			"remote_apply_state", remoteApply.State,
			"task_id", storedTask.TaskIdentifier,
			"table", storedTask.TableName,
			"stored_task_state", storedTask.State)
		return fmt.Errorf("terminal remote gRPC apply %s is %s but stored task %s is still %s after syncing remote task progress",
			remoteApply.ApplyIdentifier, remoteApply.State, storedTask.TaskIdentifier, storedTask.State)
	}
	return nil
}

func storedTaskResolvedForTerminalRemoteApply(remoteApplyState, storedTaskState string) bool {
	if state.IsTerminalTaskState(storedTaskState) {
		return true
	}
	return state.IsState(remoteApplyState, state.Apply.Stopped) &&
		state.IsState(storedTaskState, state.Task.Stopped)
}

// applyStateFromRemoteProgress is the apply-level counterpart to
// taskStateWithNoBackwardProgress in LocalClient. Local mode translates engine
// progress into task state first, then derives apply state from stored tasks.
// gRPC mode receives an apply state directly from the remote data plane, so the
// control plane needs the same no-backward policy at the apply row boundary.
func applyStateFromRemoteProgress(storedApplyState, remoteApplyState string) string {
	if remoteApplyState == "" {
		return storedApplyState
	}
	if state.IsTerminalApplyState(remoteApplyState) {
		return remoteApplyState
	}
	if state.IsTerminalApplyState(storedApplyState) {
		return storedApplyState
	}
	if state.IsState(storedApplyState, state.Apply.FailedRetryable) {
		return storedApplyState
	}
	if applyProgressRank(remoteApplyState) < applyProgressRank(storedApplyState) {
		return storedApplyState
	}
	return remoteApplyState
}

func applyProgressRank(applyState string) int {
	switch applyState {
	case state.Apply.Pending:
		return 0
	case state.Apply.PreparingBranch:
		return 1
	case state.Apply.ApplyingBranchChanges:
		return 2
	case state.Apply.ValidatingBranch:
		return 3
	case state.Apply.CreatingDeployRequest:
		return 4
	case state.Apply.ValidatingDeployRequest:
		return 5
	case state.Apply.WaitingForDeploy:
		return 6
	case state.Apply.Running:
		return 7
	case state.Apply.WaitingForCutover:
		return 8
	case state.Apply.CuttingOver:
		return 9
	case state.Apply.RevertWindow:
		return 10
	default:
		return 0
	}
}

// pollForCompletion polls the remote Tern for progress and updates SchemaBot's storage.
// Also maintains heartbeat to keep the lease on the apply.
func (c *GRPCClient) pollForCompletion(ctx context.Context, apply *storage.Apply) error {
	ticker := time.NewTicker(grpcProgressPollInterval)
	defer ticker.Stop()

	heartbeatTicker := time.NewTicker(10 * time.Second)
	defer heartbeatTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-heartbeatTicker.C:
			// Heartbeat: bump updated_at to maintain lease
			if err := c.storage.Applies().Heartbeat(ctx, apply.ID); err != nil {
				return fmt.Errorf("heartbeat gRPC apply %s: %w", apply.ApplyIdentifier, err)
			}
		case <-ticker.C:
			// Poll progress from remote Tern
			resp, err := c.client.Progress(ctx, &ternv1.ProgressRequest{
				ApplyId:     apply.ExternalID,
				Environment: apply.Environment,
			})
			if err != nil {
				if status.Code(err) == codes.NotFound {
					message := fmt.Sprintf("remote apply %s was not found by data plane", apply.ExternalID)
					return c.failMissingRemoteApply(ctx, apply, message, err)
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
				return c.failMissingRemoteApply(ctx, apply, message, nil)
			}

			// Update apply state from the remote response. An exact apply-id poll
			// must return a concrete state; unknown states are unsafe to reconcile.
			now := time.Now()
			oldApplyState := apply.State
			newState := ProtoStateToStorage(resp.State)
			if newState == "" {
				message := fmt.Sprintf("Remote progress returned unmapped apply state %s; scheduler will retry without changing stored state", remoteApplyStateDescription(resp.State))
				slog.Warn("remote gRPC progress returned unmapped apply state; scheduler will retry without changing stored state",
					"apply_id", apply.ApplyIdentifier,
					"external_id", apply.ExternalID,
					"database", apply.Database,
					"environment", apply.Environment,
					"remote_state", resp.State.String(),
					"remote_state_number", int32(resp.State),
					"stored_state", apply.State)
				c.logApplyWarning(ctx, apply, message)
				return fmt.Errorf("poll remote gRPC apply %s: unmapped remote state %s", apply.ApplyIdentifier, remoteApplyStateDescription(resp.State))
			}
			if apply.StartedAt == nil && newState != state.Apply.Pending {
				apply.StartedAt = &now
			}
			remoteApplyState := newState
			newState = applyStateFromRemoteProgress(apply.State, remoteApplyState)
			if !state.IsState(newState, remoteApplyState) {
				slog.Debug("keeping stored gRPC apply state because remote progress reported earlier state",
					"apply_id", apply.ApplyIdentifier,
					"external_id", apply.ExternalID,
					"database", apply.Database,
					"environment", apply.Environment,
					"stored_state", apply.State,
					"remote_state", remoteApplyState)
			}
			apply.State = newState
			apply.UpdatedAt = now

			terminal := isTerminalProtoState(resp.State)
			if terminal {
				return c.reconcileTerminalRemoteProgress(ctx, apply, resp.Tables, now)
			}
			storedTasks, err := c.storage.Tasks().GetByApplyID(ctx, apply.ID)
			if err != nil {
				return fmt.Errorf("load tasks to sync gRPC progress for %s: %w", apply.ApplyIdentifier, err)
			}
			if err := c.syncStoredTasksFromRemoteTasks(ctx, apply, storedTasks, resp.Tables, now); err != nil {
				return err
			}
			if err := c.storage.Applies().Update(ctx, apply); err != nil {
				return fmt.Errorf("sync apply %s from gRPC progress: %w", apply.ApplyIdentifier, err)
			}
			if oldApplyState != apply.State {
				c.logApplyStateTransition(ctx, apply, storage.LogLevelInfo, fmt.Sprintf("Remote apply changed state: %s -> %s", oldApplyState, apply.State), oldApplyState)
			}
		}
	}
}
