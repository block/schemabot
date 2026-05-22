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
// The API layer generates apply_identifier as a SchemaBot UUID when it queues
// the apply. The scheduler later dispatches the queued apply to remote Tern and
// stores Tern's ApplyId as external_id. The resolveApplyID helper translates
// apply_identifier → external_id before calling Tern.
//
// In local mode, LocalClient runs in the same process and writes to the same
// database as the API layer. There is no remote Tern ID, so external_id stays
// empty and resolveApplyID falls through to the SchemaBot apply_identifier.

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
// stores the returned ID. The call then polls until the apply reaches a durable
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
		c.markRemoteApplyFailed(ctx, apply, nil, errMsg, false)
		return errors.New(errMsg)
	}

	if apply.ExternalID != "" && state.IsState(apply.State, state.Apply.Pending) {
		_, err := c.client.Start(ctx, &ternv1.StartRequest{
			Database:    apply.Database,
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
		c.markRemoteApplyFailed(ctx, apply, nil, fmt.Sprintf("queued gRPC apply failed: load plan %d: %v", apply.PlanID, err), false)
		return fmt.Errorf("load plan %d for queued gRPC apply %s: %w", apply.PlanID, apply.ApplyIdentifier, err)
	}
	if plan == nil {
		errMsg := fmt.Sprintf("queued gRPC apply failed: plan %d not found", apply.PlanID)
		c.markRemoteApplyFailed(ctx, apply, nil, errMsg, false)
		return fmt.Errorf("queued gRPC apply %s: %s", apply.ApplyIdentifier, errMsg)
	}

	tasks, err := c.storage.Tasks().GetByApplyID(ctx, apply.ID)
	if err != nil {
		c.markRemoteApplyFailed(ctx, apply, nil, fmt.Sprintf("queued gRPC apply failed: load tasks: %v", err), false)
		return fmt.Errorf("load tasks for queued gRPC apply %s: %w", apply.ApplyIdentifier, err)
	}
	if len(tasks) == 0 {
		errMsg := "queued gRPC apply failed: no tasks found"
		c.markRemoteApplyFailed(ctx, apply, nil, errMsg, false)
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
		c.markRemoteApplyFailed(ctx, apply, tasks, err.Error(), isRetryableRemoteApplyError(err))
		return fmt.Errorf("apply queued gRPC apply %s: %w", apply.ApplyIdentifier, err)
	}
	if resp == nil {
		errMsg := "remote apply returned nil response"
		c.markRemoteApplyFailed(ctx, apply, tasks, errMsg, false)
		return fmt.Errorf("apply queued gRPC apply %s: %s", apply.ApplyIdentifier, errMsg)
	}
	if !resp.Accepted {
		errMsg := resp.ErrorMessage
		if errMsg == "" {
			errMsg = "remote apply was not accepted"
		}
		c.markRemoteApplyFailed(ctx, apply, tasks, errMsg, false)
		return fmt.Errorf("apply queued gRPC apply %s: %s", apply.ApplyIdentifier, errMsg)
	}
	if resp.ApplyId == "" {
		errMsg := "remote apply accepted without apply_id"
		c.markRemoteApplyFailed(ctx, apply, tasks, errMsg, false)
		return fmt.Errorf("apply queued gRPC apply %s: %s", apply.ApplyIdentifier, errMsg)
	}

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
		if task.State != state.Task.FailedRetryable {
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

func (c *GRPCClient) markRemoteApplyFailed(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, message string, retryable bool) {
	currentApply, skipReason, err := c.reloadRemoteApplyForTransition(ctx, apply)
	if skipReason != remoteApplyTransitionReady {
		logSkippedRemoteApplyTransition("mark remote gRPC apply failed", apply, currentApply, skipReason, err)
		return
	}

	now := time.Now()
	if tasks == nil {
		var taskErr error
		tasks, taskErr = c.storage.Tasks().GetByApplyID(ctx, currentApply.ID)
		if taskErr != nil {
			slog.Error("failed to load tasks after remote gRPC apply failed",
				"apply_id", currentApply.ApplyIdentifier,
				"external_id", currentApply.ExternalID,
				"error", taskErr)
		}
	}

	taskState := state.Task.Failed
	applyState := state.Apply.Failed
	if retryable {
		taskState = state.Task.FailedRetryable
		applyState = state.Apply.FailedRetryable
	}
	for _, task := range tasks {
		if state.IsTerminalTaskState(task.State) {
			continue
		}
		task.State = taskState
		task.ErrorMessage = message
		if retryable {
			task.CompletedAt = nil
		} else {
			task.CompletedAt = &now
		}
		task.UpdatedAt = now
		if err := c.storage.Tasks().Update(ctx, task); err != nil {
			slog.Error("failed to update task after remote gRPC apply failure",
				"apply_id", currentApply.ApplyIdentifier,
				"task_id", task.TaskIdentifier,
				"error", err)
		}
	}

	currentApply.State = applyState
	currentApply.ErrorMessage = message
	if retryable {
		currentApply.CompletedAt = nil
	} else {
		currentApply.CompletedAt = &now
	}
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

func (c *GRPCClient) failMissingRemoteApply(ctx context.Context, apply *storage.Apply, message string, cause error) error {
	c.markRemoteApplyFailed(ctx, apply, nil, message, false)
	if cause != nil {
		return fmt.Errorf("poll remote apply %s for %s: %w", apply.ExternalID, apply.ApplyIdentifier, cause)
	}
	return fmt.Errorf("poll remote apply %s for %s: %s", apply.ExternalID, apply.ApplyIdentifier, message)
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
				Database:    apply.Database,
				Environment: apply.Environment,
				ApplyId:     apply.ExternalID,
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
			tasks, err := c.storage.Tasks().GetByApplyID(ctx, apply.ID)
			if err != nil {
				return fmt.Errorf("load tasks to sync gRPC progress for %s: %w", apply.ApplyIdentifier, err)
			}
			for _, task := range tasks {
				for _, tp := range resp.Tables {
					if tp.TableName == task.TableName {
						task.State = state.NormalizeTaskStatus(tp.Status)
						task.RowsCopied = tp.RowsCopied
						task.RowsTotal = tp.RowsTotal
						task.ProgressPercent = int(tp.PercentComplete)
						task.UpdatedAt = now
						if err := c.storage.Tasks().Update(ctx, task); err != nil {
							return fmt.Errorf("sync task %s from gRPC progress for %s: %w", task.TaskIdentifier, apply.ApplyIdentifier, err)
						}
						break
					}
				}
			}

			terminal := isTerminalProtoState(resp.State)
			if terminal {
				if c.persistRemoteTerminalApply(ctx, apply, now) {
					return nil
				}
				continue
			}
			if err := c.storage.Applies().Update(ctx, apply); err != nil {
				return fmt.Errorf("sync apply %s from gRPC progress: %w", apply.ApplyIdentifier, err)
			}
		}
	}
}
