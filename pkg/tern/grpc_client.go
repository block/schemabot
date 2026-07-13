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
// gRPC mode progress flow after operator dispatch:
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
// the apply. The operator later dispatches the queued apply to remote Tern and
// stores Tern's ApplyId as external_id. Apply-scoped HTTP handlers load the
// stored apply row and send external_id to Tern when it is present.
//
// In local mode, LocalClient runs in the same process and writes to the same
// database as the API layer. There is no remote Tern ID, so apply-scoped HTTP
// handlers send the SchemaBot apply_identifier to LocalClient.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"strconv"
	"strings"
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

const (
	grpcProgressPollInterval       = 500 * time.Millisecond
	maxGRPCProgressPollErrorStreak = 10
)

var grpcStoppedAfterStartGracePeriod = 30 * time.Second

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

// retryServiceConfig enables client-side retries for idempotent RPCs.
//
// The network path to a remote Tern deployment often crosses proxies and
// service meshes, where connection resets or TLS handshake flaps surface as
// UNAVAILABLE before the request reaches the server. Retrying rides out the
// blip instead of failing the caller's operation.
//
// Only RPCs that are safe to re-send are retried, in two budgets:
//
// Caller-facing reads (PullSchema, Plan, PlanDiff) get a long budget: a
// human or review workflow is waiting on the response, and a data-plane pod
// restart or mesh drain lasts seconds — well past a sub-second budget — so
// these ride out up to roughly fifteen seconds before surfacing UNAVAILABLE.
// (Plan is retry-safe because each attempt produces an independent plan
// record and only the returned plan ID is used.) The budget must stay under
// the API server's 30s response timeout, which bounds these calls end to end.
// gRPC clamps maxAttempts to 5, so raising it further has no effect. During a
// sustained outage a multi-environment auto-plan pays this budget per
// environment (serial Plan + concurrent PlanDiff waves), so the whole flow
// stays within its command timeout only for a handful of environments — the
// exhausted calls fail closed either way.
//
// Fast polls (Progress, Health) keep a sub-second budget: Progress is called
// on tight drive loops that own their own failure handling, and Health feeds
// the remote-deployment outage monitor, which must observe an outage promptly
// rather than ride it out.
//
// State-changing RPCs (Apply, Cutover, Stop, Cancel, Start, Volume, Revert,
// SkipRevert) are intentionally not retried here: re-sending them could
// duplicate work or advance an apply twice, and the operator's durable
// queue already owns redelivery for dispatch failures.
const retryServiceConfig = `{
	"methodConfig": [{
		"name": [
			{"service": "tern.v1.Tern", "method": "PullSchema"},
			{"service": "tern.v1.Tern", "method": "Plan"},
			{"service": "tern.v1.Tern", "method": "PlanDiff"}
		],
		"retryPolicy": {
			"maxAttempts": 5,
			"initialBackoff": "0.5s",
			"maxBackoff": "8s",
			"backoffMultiplier": 3.0,
			"retryableStatusCodes": ["UNAVAILABLE"]
		}
	}, {
		"name": [
			{"service": "tern.v1.Tern", "method": "Progress"},
			{"service": "tern.v1.Tern", "method": "Health"}
		],
		"retryPolicy": {
			"maxAttempts": 3,
			"initialBackoff": "0.2s",
			"maxBackoff": "2s",
			"backoffMultiplier": 2.0,
			"retryableStatusCodes": ["UNAVAILABLE"]
		}
	}]
}`

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
		grpc.WithDefaultServiceConfig(retryServiceConfig),
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

func (c *GRPCClient) PlanDiff(ctx context.Context, req *ternv1.PlanRequest) (*ternv1.PlanDiffResponse, error) {
	return c.client.PlanDiff(ctx, req)
}

func (c *GRPCClient) PullSchema(ctx context.Context, req *ternv1.PullSchemaRequest) (*ternv1.PullSchemaResponse, error) {
	return c.client.PullSchema(ctx, req)
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
func (c *GRPCClient) logApplyEvent(ctx context.Context, applyID int64, taskID *int64, level, eventType, message string, oldState, newState string) {
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
		Source:    storage.LogSourceSchemaBot,
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
	c.logApplyEvent(ctx, apply.ID, nil, level, storage.LogEventStateTransition,
		message, oldState, apply.State)
}

func (c *GRPCClient) logTaskStateTransition(ctx context.Context, applyID int64, task *storage.Task, message, oldState string) {
	taskID := task.ID
	c.logApplyEvent(ctx, applyID, &taskID, storage.LogLevelInfo, storage.LogEventStateTransition,
		message, oldState, task.State)
}

func (c *GRPCClient) logApplyWarning(ctx context.Context, apply *storage.Apply, message string) {
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, storage.LogEventError,
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

	// Captured from the first successful load so a later transient load failure
	// stays searchable by the user-facing apply_id operators triage with. Before
	// the first load succeeds the identifier is unknown, so it is simply omitted.
	var applyIdentifier string
	identArgs := func() []any {
		if applyIdentifier == "" {
			return nil
		}
		return []any{"apply_id", applyIdentifier}
	}

	for range ticker.C {
		obs := c.getObserver(applyID)
		if obs == nil {
			// Observer was cleared — apply reached terminal state and
			// OnTerminal already ran. Stop polling.
			return
		}

		// Load failures here are transient — the ticker retries on the next
		// tick, so log at Warn rather than Error.
		apply, err := c.storage.Applies().Get(context.Background(), applyID)
		if err != nil {
			slog.Warn("observer poll: failed to load apply; will retry on next tick",
				append(identArgs(), "error", err)...)
			continue
		}
		if apply == nil {
			// The row is gone rather than transiently unreadable, so it will
			// never reappear — stop polling instead of spinning and warning
			// every tick for an apply that no longer exists.
			slog.Warn("observer poll: apply not found; stopping poll", identArgs()...)
			c.clearObserver(applyID)
			return
		}
		applyIdentifier = apply.ApplyIdentifier

		tasks, err := c.storage.Tasks().GetByApplyID(context.Background(), applyID)
		if err != nil {
			slog.Warn("observer poll: failed to load tasks; will retry on next tick",
				"apply_id", apply.ApplyIdentifier,
				"external_id", apply.ExternalID,
				"database", apply.Database,
				"environment", apply.Environment,
				"error", err)
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

func (c *GRPCClient) processPendingCutoverControlRequest(ctx context.Context, apply *storage.Apply, scope applyTaskScope) error {
	controlReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationCutover)
	if err != nil {
		return err
	}
	if controlReq == nil {
		return nil
	}
	if cutoverRequestResolvedByApplyState(apply.State) {
		slog.Info("completing pending gRPC cutover request for resolved apply",
			"apply_id", apply.ApplyIdentifier,
			"external_id", apply.ExternalID,
			"database", apply.Database,
			"environment", apply.Environment,
			"requested_by", controlRequestCaller(controlReq),
			"state", apply.State)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventCutoverTriggered,
			fmt.Sprintf("Pending remote cutover request completed for resolved apply%s", callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
		return completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationCutover)
	}
	if cutoverRequestFailedByApplyState(apply.State) {
		message := fmt.Sprintf("cutover request was not applied because apply is %s", apply.State)
		if err := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationCutover, message); err != nil {
			return err
		}
		return fmt.Errorf("process pending gRPC cutover for apply %s: %s", apply.ApplyIdentifier, message)
	}
	if state.IsState(apply.State, state.Apply.Recovering) {
		slog.Info("pending gRPC cutover request is waiting for recovery to complete",
			"apply_id", apply.ApplyIdentifier,
			"external_id", apply.ExternalID,
			"database", apply.Database,
			"environment", apply.Environment,
			"requested_by", controlRequestCaller(controlReq),
			"state", apply.State)
		return nil
	}
	readyForCutover, err := applyReadyForCutoverRequest(ctx, c.storage, apply)
	if err != nil {
		return fmt.Errorf("check cutover readiness for apply %s: %w", apply.ApplyIdentifier, err)
	}
	if !readyForCutover {
		slog.Info("pending gRPC cutover request is waiting for cutover-ready state",
			"apply_id", apply.ApplyIdentifier,
			"external_id", apply.ExternalID,
			"database", apply.Database,
			"environment", apply.Environment,
			"requested_by", controlRequestCaller(controlReq),
			"state", apply.State)
		return nil
	}
	remoteID := scope.remoteApplyID(apply)
	if remoteID == "" {
		message := "remote apply id is not available"
		if err := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationCutover, message); err != nil {
			return err
		}
		return fmt.Errorf("process pending gRPC cutover for apply %s: %s", apply.ApplyIdentifier, message)
	}
	if stopReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationStop); err != nil {
		return fmt.Errorf("check pending stop request before pending gRPC cutover for apply %s: %w", apply.ApplyIdentifier, err)
	} else if stopReq != nil {
		message := "schema change has a pending stop request; cutover is blocked until stop is processed"
		return fmt.Errorf("process pending gRPC cutover for apply %s: %s", apply.ApplyIdentifier, message)
	}
	if err := markApplyCuttingOverForControlRequest(ctx, c.storage, apply); err != nil {
		return err
	}
	resp, err := c.client.Cutover(ctx, &ternv1.CutoverRequest{
		ApplyId:     remoteID,
		Environment: apply.Environment,
	})
	if err != nil {
		errorMessage := fmt.Sprintf("remote cutover failed: %v", err)
		if failErr := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationCutover, errorMessage); failErr != nil {
			return fmt.Errorf("request remote gRPC cutover for apply %s remote %s: %w; fail pending cutover request: %w", apply.ApplyIdentifier, remoteID, err, failErr)
		}
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelError, storage.LogEventError,
			fmt.Sprintf("Remote cutover failed for apply %s (remote %s)%s: %v", apply.ApplyIdentifier, remoteID, callerApplyLogSuffix(controlRequestCaller(controlReq)), err), "", "")
		return fmt.Errorf("request remote gRPC cutover for apply %s remote %s: %w", apply.ApplyIdentifier, remoteID, err)
	}
	if resp == nil {
		errorMessage := "not accepted"
		if err := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationCutover, errorMessage); err != nil {
			return err
		}
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelError, storage.LogEventError,
			fmt.Sprintf("Remote cutover returned no response for apply %s (remote %s)%s", apply.ApplyIdentifier, remoteID, callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
		return fmt.Errorf("request remote gRPC cutover for apply %s remote %s: %s", apply.ApplyIdentifier, remoteID, errorMessage)
	}
	if !resp.Accepted {
		errorMessage := "not accepted"
		if resp.ErrorMessage != "" {
			errorMessage = resp.ErrorMessage
		}
		if err := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationCutover, errorMessage); err != nil {
			return err
		}
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelError, storage.LogEventError,
			fmt.Sprintf("Remote cutover was not accepted for apply %s (remote %s)%s: %s", apply.ApplyIdentifier, remoteID, callerApplyLogSuffix(controlRequestCaller(controlReq)), errorMessage), "", "")
		return fmt.Errorf("request remote gRPC cutover for apply %s remote %s: %s", apply.ApplyIdentifier, remoteID, errorMessage)
	}
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventCutoverTriggered,
		fmt.Sprintf("Remote cutover accepted for apply %s (remote %s)%s", apply.ApplyIdentifier, remoteID, callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
	if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationCutover); err != nil {
		return err
	}
	slog.Info("pending gRPC cutover request accepted and completed",
		"apply_id", apply.ApplyIdentifier,
		"external_id", apply.ExternalID,
		"database", apply.Database,
		"environment", apply.Environment,
		"requested_by", controlRequestCaller(controlReq),
		"state", apply.State)
	return nil
}

func (c *GRPCClient) Stop(ctx context.Context, req *ternv1.StopRequest) (*ternv1.StopResponse, error) {
	return c.client.Stop(ctx, req)
}

func (c *GRPCClient) Cancel(ctx context.Context, req *ternv1.CancelRequest) (*ternv1.CancelResponse, error) {
	return c.client.Cancel(ctx, req)
}

// processPendingSkipRevertControlRequest drives a durable skip-revert control
// request for a remote apply: it proxies SkipRevert to the data plane and
// completes the request. This is the apply owner's retry path when the API's
// immediate skip attempt failed or its process died, leaving the request pending.
// A transient failure returns an error so the drive exits and the operator
// retries (the request stays pending); a rejected skip fails the request.
func (c *GRPCClient) processPendingSkipRevertControlRequest(ctx context.Context, apply *storage.Apply, remoteID string) error {
	controlReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationSkipRevert)
	if err != nil {
		return err
	}
	if controlReq == nil {
		return nil
	}
	// Skip-revert is only meaningful in the revert window. Once the apply has
	// left it (finalized, reverted, …) the request is moot — complete it so it
	// does not linger.
	if !state.IsState(apply.State, state.Apply.RevertWindow) {
		return completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationSkipRevert)
	}
	resp, err := c.client.SkipRevert(ctx, &ternv1.SkipRevertRequest{
		ApplyId:     remoteID,
		Environment: apply.Environment,
	})
	if err != nil {
		return fmt.Errorf("process pending skip-revert for apply %s: %w", apply.ApplyIdentifier, err)
	}
	if !resp.Accepted {
		message := "skip-revert was not accepted by the data plane"
		if resp.ErrorMessage != "" {
			message = fmt.Sprintf("skip-revert was not accepted: %s", resp.ErrorMessage)
		}
		return failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationSkipRevert, message)
	}
	if apply.Engine == storage.EnginePlanetScale {
		if err := c.storage.Applies().SetRevertSkipped(ctx, apply.ID, time.Now()); err != nil {
			slog.Warn("failed to record skip-revert on apply", "apply_id", apply.ApplyIdentifier, "error", err)
		}
	}
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventSkipRevertTriggered,
		fmt.Sprintf("Skip-revert triggered by user%s", callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
	return completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationSkipRevert)
}

// processPendingRevertControlRequest drives a durable revert control request for
// a remote apply: it proxies Revert to the data plane and completes the request.
// This is the apply owner's retry path when the API's immediate revert attempt
// failed or its process died, leaving the request pending. A transient failure
// returns an error so the drive exits and the operator retries (the request
// stays pending); a rejected revert fails the request.
func (c *GRPCClient) processPendingRevertControlRequest(ctx context.Context, apply *storage.Apply, remoteID string) error {
	controlReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationRevert)
	if err != nil {
		return err
	}
	if controlReq == nil {
		return nil
	}
	// Revert is only meaningful in the revert window. Once the apply has left it
	// (finalized, reverted, …) the request is moot — complete it so it does not
	// linger.
	if !state.IsState(apply.State, state.Apply.RevertWindow) {
		return completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationRevert)
	}
	resp, err := c.client.Revert(ctx, &ternv1.RevertRequest{
		ApplyId:     remoteID,
		Environment: apply.Environment,
	})
	if err != nil {
		return fmt.Errorf("process pending revert for apply %s: %w", apply.ApplyIdentifier, err)
	}
	if !resp.Accepted {
		message := "revert was not accepted by the data plane"
		if resp.ErrorMessage != "" {
			message = fmt.Sprintf("revert was not accepted: %s", resp.ErrorMessage)
		}
		return failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationRevert, message)
	}
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventRevertTriggered,
		fmt.Sprintf("Revert triggered by user%s", callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
	return completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationRevert)
}

// logOperationDriveLeavesParentStop records that a multi-operation
// operation-only drive observed an apply-level pending stop request and is
// leaving it pending. Such a drive owns only its operation; the parent stop
// request is completed by the operator once the projection CAS derives the
// parent terminal, so completing it from the drive would resolve the
// apply-level stop early while sibling operations are still live.
func logOperationDriveLeavesParentStop(apply *storage.Apply, scope applyTaskScope) {
	slog.Info("operation-only drive leaving apply-level stop request for operator projection",
		append(apply.LogAttrs(), "apply_operation_id", scope.applyOperationID, "remote_apply_id", scope.remoteApplyID(apply))...)
}

func logOperationDriveLeavesParentCancel(apply *storage.Apply, scope applyTaskScope) {
	slog.Info("operation-only drive leaving apply-level cancel request for operator projection",
		append(apply.LogAttrs(), "apply_operation_id", scope.applyOperationID, "remote_apply_id", scope.remoteApplyID(apply))...)
}

func (c *GRPCClient) processPendingStopControlRequest(ctx context.Context, apply *storage.Apply, scope applyTaskScope) (bool, error) {
	controlReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationStop)
	if err != nil {
		return false, err
	}
	if controlReq == nil {
		return false, nil
	}
	if scope.suppressesDirectParentApplyWrites() {
		// An operation-only drive must not complete the apply-level stop
		// request: the operator projection owns it and completes it once the
		// parent apply derives terminal. If the parent is already terminal in
		// storage there is no more remote work for this drive to do, so leave
		// the stop pending for the operator projection to resolve; otherwise
		// fall through so this drive can still stop its own operation's remote
		// work and leave the parent stop pending for the operator.
		storedApply, err := c.storage.Applies().Get(ctx, apply.ID)
		if err != nil {
			return true, fmt.Errorf("load apply %s before leaving pending stop for operator projection: %w", apply.ApplyIdentifier, err)
		}
		if storedApply == nil {
			return true, fmt.Errorf("load apply %s before leaving pending stop for operator projection: %w", apply.ApplyIdentifier, storage.ErrApplyNotFound)
		}
		if state.IsTerminalApplyState(storedApply.State) {
			*apply = *storedApply
			logOperationDriveLeavesParentStop(apply, scope)
			return true, nil
		}
	} else if completed, err := completePendingStopIfStoredApplyResolved(ctx, c.storage, apply); err != nil {
		return true, err
	} else if completed {
		slog.Info("completing pending gRPC stop request for resolved apply",
			"apply_id", apply.ApplyIdentifier,
			"external_id", apply.ExternalID,
			"database", apply.Database,
			"environment", apply.Environment,
			"requested_by", controlRequestCaller(controlReq),
			"state", apply.State)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStopRequested,
			fmt.Sprintf("Pending remote stop request completed for resolved apply%s", callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
		if hasPendingStart, startErr := hasPendingStartControlRequest(ctx, c.storage, apply); startErr != nil {
			return true, startErr
		} else if hasPendingStart {
			return false, nil
		}
		return true, nil
	}
	if state.IsTerminalApplyState(apply.State) {
		if scope.suppressesDirectParentApplyWrites() {
			logOperationDriveLeavesParentStop(apply, scope)
			return true, nil
		}
		slog.Info("completing pending gRPC stop request for terminal apply",
			"apply_id", apply.ApplyIdentifier,
			"external_id", apply.ExternalID,
			"database", apply.Database,
			"environment", apply.Environment,
			"requested_by", controlRequestCaller(controlReq),
			"state", apply.State)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStopRequested,
			fmt.Sprintf("Pending remote stop request completed for terminal apply%s", callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
		if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStop); err != nil {
			return true, err
		}
		return true, nil
	}
	remoteID := scope.remoteApplyID(apply)
	if remoteID == "" {
		if scope.usesOperationRemoteResume() {
			// A multi-operation apply has one stop request shared by every
			// deployment. Stopping this undispatched operation must not
			// terminalize the parent or complete the apply-level request:
			// sibling deployments with their own remote apply id still need to
			// observe the durable stop. Stop only this operation and leave the
			// request pending for the siblings.
			if err := c.stopUndispatchedApplyOperation(ctx, apply, controlRequestCaller(controlReq), scope); err != nil {
				return true, err
			}
			logOperationDriveLeavesParentStop(apply, scope)
			return true, nil
		}
		if err := c.stopUndispatchedApply(ctx, apply, controlRequestCaller(controlReq), scope); err != nil {
			return true, err
		}
		if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStop); err != nil {
			return true, err
		}
		return true, nil
	}

	resp, err := c.client.Stop(ctx, &ternv1.StopRequest{
		ApplyId:     remoteID,
		Environment: apply.Environment,
	})
	if err != nil {
		if completed, completeErr := c.completeRemoteStopFromTerminalProgress(ctx, apply, controlReq, scope); completeErr == nil && completed {
			return true, nil
		} else if completeErr != nil {
			return true, fmt.Errorf("request remote gRPC stop for apply %s remote %s: %w; terminal progress reconciliation also failed: %w", apply.ApplyIdentifier, remoteID, err, completeErr)
		}
		return true, fmt.Errorf("request remote gRPC stop for apply %s remote %s: %w", apply.ApplyIdentifier, remoteID, err)
	}
	if resp == nil || !resp.Accepted {
		errorMessage := "not accepted"
		if resp != nil && resp.ErrorMessage != "" {
			errorMessage = resp.ErrorMessage
		}
		return true, fmt.Errorf("request remote gRPC stop for apply %s remote %s: %s", apply.ApplyIdentifier, remoteID, errorMessage)
	}
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStopRequested,
		fmt.Sprintf("Remote stop accepted for apply %s%s", remoteID, callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")

	progress, err := c.client.Progress(ctx, &ternv1.ProgressRequest{
		ApplyId:     remoteID,
		Environment: apply.Environment,
	})
	if err != nil {
		return true, fmt.Errorf("sync remote gRPC stop for apply %s remote %s: %w", apply.ApplyIdentifier, remoteID, err)
	}
	if progress.State == ternv1.State_STATE_NO_ACTIVE_CHANGE {
		return true, fmt.Errorf("sync remote gRPC stop for apply %s remote %s: no active schema change", apply.ApplyIdentifier, remoteID)
	}
	remoteState := ProtoStateToStorage(progress.State)
	if remoteState == "" {
		return true, fmt.Errorf("sync remote gRPC stop for apply %s remote %s: unmapped remote state %s", apply.ApplyIdentifier, remoteID, remoteApplyStateDescription(progress.State))
	}
	now := time.Now()
	priorState, priorStartedAt, priorUpdatedAt := apply.State, apply.StartedAt, apply.UpdatedAt
	if apply.StartedAt == nil && !state.IsState(remoteState, state.Apply.Pending) {
		apply.StartedAt = &now
	}
	apply.State = applyStateFromRemoteProgress(apply.State, remoteState, false)
	apply.UpdatedAt = now
	if isTerminalProtoState(progress.State) {
		if err := c.reconcileTerminalRemoteProgress(ctx, apply, progress.Tables, now, scope); err != nil {
			return true, err
		}
		// An operation-only drive owns only its operation: the apply-level stop
		// request is shared across siblings and completed by the operator
		// projection once the parent derives terminal. Leave it pending here and
		// restore the in-memory parent apply fields the driver does not own, so
		// this operation's terminal remote state does not leak onto the shared
		// apply and let a later stop pass treat the parent as terminal.
		if scope.suppressesDirectParentApplyWrites() {
			apply.State, apply.StartedAt, apply.UpdatedAt = priorState, priorStartedAt, priorUpdatedAt
			logOperationDriveLeavesParentStop(apply, scope)
			return true, nil
		}
		if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStop); err != nil {
			return true, err
		}
		if hasPendingStart, startErr := hasPendingStartControlRequest(ctx, c.storage, apply); startErr != nil {
			return true, startErr
		} else if hasPendingStart {
			return false, nil
		}
		return true, nil
	}

	storedTasks, err := c.loadApplyTasks(ctx, apply, scope)
	if err != nil {
		return true, fmt.Errorf("load tasks to sync remote gRPC stop for %s: %w", apply.ApplyIdentifier, err)
	}
	if err := c.syncStoredTasksFromRemoteTasks(ctx, apply, storedTasks, progress.Tables, now); err != nil {
		return true, err
	}
	if _, err := c.persistParentApply(ctx, apply, scope, "sync nonterminal gRPC stop"); err != nil {
		return true, fmt.Errorf("sync nonterminal remote gRPC stop state for %s: %w", apply.ApplyIdentifier, err)
	}
	slog.Info("remote gRPC stop request accepted and remains pending for remote apply owner",
		"apply_id", apply.ApplyIdentifier,
		"external_id", apply.ExternalID,
		"database", apply.Database,
		"environment", apply.Environment,
		"requested_by", controlRequestCaller(controlReq),
		"remote_state", remoteState)
	return false, nil
}

func (c *GRPCClient) processPendingCancelControlRequest(ctx context.Context, apply *storage.Apply, scope applyTaskScope) (bool, error) {
	controlReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationCancel)
	if err != nil {
		return false, err
	}
	if controlReq == nil {
		return false, nil
	}
	if state.IsTerminalApplyState(apply.State) && !state.IsState(apply.State, state.Apply.Stopped) {
		if scope.suppressesDirectParentApplyWrites() {
			logOperationDriveLeavesParentCancel(apply, scope)
			return true, nil
		}
		if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationCancel); err != nil {
			return true, err
		}
		return true, nil
	}
	remoteID := scope.remoteApplyID(apply)
	if remoteID == "" {
		return true, fmt.Errorf("cancel gRPC apply %s: remote apply id is not available", apply.ApplyIdentifier)
	}
	resp, err := c.client.Cancel(ctx, &ternv1.CancelRequest{ApplyId: remoteID, Environment: apply.Environment})
	if err != nil {
		return true, fmt.Errorf("request remote gRPC cancel for apply %s remote %s: %w", apply.ApplyIdentifier, remoteID, err)
	}
	if resp == nil || !resp.Accepted {
		errorMessage := "not accepted"
		if resp != nil && resp.ErrorMessage != "" {
			errorMessage = resp.ErrorMessage
		}
		return true, fmt.Errorf("request remote gRPC cancel for apply %s remote %s: %s", apply.ApplyIdentifier, remoteID, errorMessage)
	}
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventCancelRequested,
		fmt.Sprintf("Remote cancel accepted for apply %s%s", remoteID, callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
	progress, err := c.client.Progress(ctx, &ternv1.ProgressRequest{ApplyId: remoteID, Environment: apply.Environment})
	if err != nil {
		return true, fmt.Errorf("sync remote gRPC cancel for apply %s remote %s: %w", apply.ApplyIdentifier, remoteID, err)
	}
	if progress.State == ternv1.State_STATE_NO_ACTIVE_CHANGE {
		return true, fmt.Errorf("sync remote gRPC cancel for apply %s remote %s: no active schema change", apply.ApplyIdentifier, remoteID)
	}
	remoteState := ProtoStateToStorage(progress.State)
	if remoteState == "" {
		return true, fmt.Errorf("sync remote gRPC cancel for apply %s remote %s: unmapped remote state %s", apply.ApplyIdentifier, remoteID, remoteApplyStateDescription(progress.State))
	}
	now := time.Now()
	priorState, priorStartedAt, priorUpdatedAt := apply.State, apply.StartedAt, apply.UpdatedAt
	apply.State = applyStateFromRemoteProgress(apply.State, remoteState, false)
	apply.UpdatedAt = now
	if isTerminalProtoState(progress.State) {
		if err := c.reconcileTerminalRemoteProgress(ctx, apply, progress.Tables, now, scope); err != nil {
			return true, err
		}
		if scope.suppressesDirectParentApplyWrites() {
			apply.State, apply.StartedAt, apply.UpdatedAt = priorState, priorStartedAt, priorUpdatedAt
			logOperationDriveLeavesParentCancel(apply, scope)
			return true, nil
		}
		if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationCancel); err != nil {
			return true, err
		}
		return true, nil
	}
	if _, err := c.persistParentApply(ctx, apply, scope, "sync nonterminal gRPC cancel"); err != nil {
		return true, fmt.Errorf("sync nonterminal remote gRPC cancel state for %s: %w", apply.ApplyIdentifier, err)
	}
	return false, nil
}

func (c *GRPCClient) processPendingCancelOrStopControlRequest(ctx context.Context, apply *storage.Apply, scope applyTaskScope) (bool, error) {
	if handled, err := c.processPendingCancelControlRequest(ctx, apply, scope); handled || err != nil {
		return handled, err
	}
	return c.processPendingStopControlRequest(ctx, apply, scope)
}

func (c *GRPCClient) completeRemoteStopFromTerminalProgress(ctx context.Context, apply *storage.Apply, controlReq *storage.ApplyControlRequest, scope applyTaskScope) (bool, error) {
	progress, err := c.client.Progress(ctx, &ternv1.ProgressRequest{
		ApplyId:     scope.remoteApplyID(apply),
		Environment: apply.Environment,
	})
	if err != nil {
		slog.Warn("remote gRPC stop error could not be reconciled from progress",
			"apply_id", apply.ApplyIdentifier,
			"external_id", apply.ExternalID,
			"database", apply.Database,
			"environment", apply.Environment,
			"requested_by", controlRequestCaller(controlReq),
			"error", err)
		return false, nil
	}
	if progress.State == ternv1.State_STATE_NO_ACTIVE_CHANGE || !isTerminalProtoState(progress.State) {
		slog.Warn("remote gRPC stop error found nonterminal progress; durable stop request remains pending for operator retry",
			"apply_id", apply.ApplyIdentifier,
			"external_id", apply.ExternalID,
			"database", apply.Database,
			"environment", apply.Environment,
			"requested_by", controlRequestCaller(controlReq),
			"remote_state", progress.State.String(),
			"remote_state_number", int32(progress.State))
		return false, nil
	}
	remoteState := ProtoStateToStorage(progress.State)
	if remoteState == "" {
		return false, fmt.Errorf("sync remote gRPC stop for apply %s remote %s after stop error: unmapped remote state %s", apply.ApplyIdentifier, apply.ExternalID, remoteApplyStateDescription(progress.State))
	}
	now := time.Now()
	priorState, priorStartedAt, priorUpdatedAt := apply.State, apply.StartedAt, apply.UpdatedAt
	if apply.StartedAt == nil && !state.IsState(remoteState, state.Apply.Pending) {
		apply.StartedAt = &now
	}
	apply.State = applyStateFromRemoteProgress(apply.State, remoteState, false)
	apply.UpdatedAt = now
	if err := c.reconcileTerminalRemoteProgress(ctx, apply, progress.Tables, now, scope); err != nil {
		return false, err
	}
	// An operation-only drive owns only its operation: the apply-level stop
	// request is shared across siblings and completed by the operator projection
	// once the parent derives terminal. Leave it pending here and restore the
	// in-memory parent apply fields the driver does not own, so this operation's
	// terminal remote state does not leak onto the shared apply and let a later
	// stop pass treat the parent as terminal.
	if scope.suppressesDirectParentApplyWrites() {
		apply.State, apply.StartedAt, apply.UpdatedAt = priorState, priorStartedAt, priorUpdatedAt
		logOperationDriveLeavesParentStop(apply, scope)
		return true, nil
	}
	if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStop); err != nil {
		return false, err
	}
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStopRequested,
		fmt.Sprintf("Remote stop request completed from terminal progress after stop error%s", callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
	return true, nil
}

func (c *GRPCClient) stopUndispatchedApply(ctx context.Context, apply *storage.Apply, caller string, scope applyTaskScope) error {
	now := time.Now()
	taskState := state.Task.Stopped
	applyState := state.Apply.Stopped
	if stopTerminatesChange(apply.DatabaseType) {
		taskState = state.Task.Cancelled
		applyState = state.Apply.Cancelled
	}
	tasks, err := c.loadApplyTasks(ctx, apply, scope)
	if err != nil {
		return fmt.Errorf("load tasks for undispatched stop %s: %w", apply.ApplyIdentifier, err)
	}
	for _, task := range tasks {
		if state.IsTerminalTaskState(task.State) {
			slog.Info("leaving terminal gRPC task unchanged during undispatched stop",
				"apply_id", apply.ApplyIdentifier,
				"task_id", task.TaskIdentifier,
				"table", task.TableName,
				"task_state", task.State)
			continue
		}
		task.State = taskState
		if state.IsState(taskState, state.Task.Cancelled) {
			task.CompletedAt = &now
		}
		task.UpdatedAt = now
		if err := c.storage.Tasks().Update(ctx, task); err != nil {
			return fmt.Errorf("update task %s for undispatched stop %s: %w", task.TaskIdentifier, apply.ApplyIdentifier, err)
		}
	}
	oldState := apply.State
	apply.State = applyState
	apply.CompletedAt = nil
	if state.IsState(applyState, state.Apply.Cancelled) {
		apply.CompletedAt = &now
	}
	apply.UpdatedAt = now
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		return fmt.Errorf("update undispatched stopped gRPC apply %s: %w", apply.ApplyIdentifier, err)
	}
	c.logApplyStateTransition(ctx, apply, storage.LogLevelInfo, fmt.Sprintf("Remote apply stopped before dispatch: %s%s", apply.State, callerApplyLogSuffix(caller)), oldState)
	return nil
}

// stopUndispatchedApplyOperation stops a single undispatched operation of a
// multi-operation apply. It terminalizes only this operation's tasks and the
// operation row, never the parent apply, and never completes the apply-level
// stop request: that request is shared across deployments and must remain
// pending so sibling operations with their own remote apply id still observe
// the stop.
func (c *GRPCClient) stopUndispatchedApplyOperation(ctx context.Context, apply *storage.Apply, caller string, scope applyTaskScope) error {
	if !scope.usesOperationRemoteResume() {
		return fmt.Errorf("undispatched operation stop for apply %s requires multi-operation scope", apply.ApplyIdentifier)
	}
	op := scope.operation
	now := time.Now()
	taskState := state.Task.Stopped
	operationState := state.ApplyOperation.Stopped
	if stopTerminatesChange(apply.DatabaseType) {
		taskState = state.Task.Cancelled
		operationState = state.ApplyOperation.Cancelled
	}
	tasks, err := c.loadApplyTasks(ctx, apply, scope)
	if err != nil {
		return fmt.Errorf("load tasks for undispatched operation stop %s apply_operation %d: %w", apply.ApplyIdentifier, op.ID, err)
	}
	for _, task := range tasks {
		if state.IsTerminalTaskState(task.State) {
			slog.Info("leaving terminal gRPC task unchanged during undispatched operation stop",
				"apply_id", apply.ApplyIdentifier,
				"apply_operation_id", op.ID,
				"deployment", op.Deployment,
				"task_id", task.TaskIdentifier,
				"table", task.TableName,
				"task_state", task.State)
			continue
		}
		task.State = taskState
		if state.IsState(taskState, state.Task.Cancelled) {
			task.CompletedAt = &now
		}
		task.UpdatedAt = now
		if err := c.storage.Tasks().Update(ctx, task); err != nil {
			return fmt.Errorf("update task %s for undispatched operation stop %s apply_operation %d: %w", task.TaskIdentifier, apply.ApplyIdentifier, op.ID, err)
		}
	}
	oldState := op.State
	if state.IsState(operationState, state.ApplyOperation.Cancelled) {
		if err := c.storage.ApplyOperations().MarkTerminal(ctx, op.ID, operationState); err != nil {
			return fmt.Errorf("mark undispatched gRPC apply_operation %d cancelled for apply %s: %w", op.ID, apply.ApplyIdentifier, err)
		}
		op.CompletedAt = &now
	} else {
		if err := c.storage.ApplyOperations().UpdateState(ctx, op.ID, operationState); err != nil {
			return fmt.Errorf("mark undispatched gRPC apply_operation %d stopped for apply %s: %w", op.ID, apply.ApplyIdentifier, err)
		}
		op.CompletedAt = nil
	}
	op.State = operationState
	op.UpdatedAt = now
	slog.Info("stopped undispatched multi-operation gRPC apply operation; apply-level stop request remains pending for siblings",
		"apply_id", apply.ApplyIdentifier,
		"apply_operation_id", op.ID,
		"deployment", op.Deployment,
		"database", apply.Database,
		"environment", apply.Environment,
		"requested_by", caller,
		"old_operation_state", oldState,
		"new_operation_state", operationState)
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStopRequested,
		fmt.Sprintf("Remote apply operation %d (deployment %s) stopped before dispatch: %s%s; pending apply stop request remains for sibling operations", op.ID, op.Deployment, operationState, callerApplyLogSuffix(caller)), "", "")
	return nil
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

func (c *GRPCClient) Health(ctx context.Context) error {
	_, err := c.client.Health(ctx, &ternv1.HealthRequest{})
	return err
}

// applyTaskScope selects which task rows the remote drive re-queries and where
// the remote Tern apply id for the drive is read and written. The zero value
// scopes to the whole apply (all its operations) and uses the parent
// applies.external_id, matching the single-operation behaviour. An
// operation-scoped value restricts the drive to a single apply_operation (one
// deployment) so a driver can advance one deployment independently of its
// siblings; when the parent owns more than one operation it also routes the
// remote apply id through that operation's external_id instead of the shared
// parent external_id, so one deployment never reuses or overwrites another
// deployment's remote apply id.
type applyTaskScope struct {
	applyOperationID int64

	// operation is the claimed apply_operation row, loaded and validated for
	// operation-scoped drives. nil for whole-apply drives.
	operation *storage.ApplyOperation

	// multiOperation is true only when the parent apply owns more than one
	// operation. Deployment equality is not enough to detect this: the primary
	// operation of a multi-op apply shares apply.Deployment, so the operation
	// count is the authoritative signal for routing the remote apply id per op.
	multiOperation bool
}

func wholeApplyTaskScope() applyTaskScope {
	return applyTaskScope{}
}

func (s applyTaskScope) isOperationScoped() bool {
	return s.applyOperationID > 0
}

// usesOperationRemoteResume reports whether the remote apply id for this drive
// lives on the claimed operation rather than the parent applies.external_id.
// Only multi-operation drives do; single-operation and whole-apply drives keep
// using the parent external_id.
func (s applyTaskScope) usesOperationRemoteResume() bool {
	return s.multiOperation && s.operation != nil
}

// suppressesDirectParentApplyWrites reports whether this drive must not write
// the parent applies row (state, heartbeat) or run parent-level side effects
// (parent stop-request completion, active-apply metrics). Multi-operation drives
// own only their operation: the parent applies.state is moved solely by the
// operation-authorized projection CAS in the operator. Single-operation and
// whole-apply drives keep writing the parent directly.
func (s applyTaskScope) suppressesDirectParentApplyWrites() bool {
	return s.usesOperationRemoteResume()
}

// finalizerOperationScope reports whether this drive owns a task-less
// group_finalizer operation. Such an operation has no task rows, so the
// operator's task-derived operation→parent projection can never move its
// operation row off pending: the terminal remote state (completion or failure)
// would be lost. A finalizer drive must therefore persist its own operation
// row's terminal state, mirroring LocalClient.driveGroupFinalizer.
func (s applyTaskScope) finalizerOperationScope() bool {
	return s.usesOperationRemoteResume() &&
		s.operation != nil &&
		s.operation.OperationKind == storage.ApplyOperationKindGroupFinalizer
}

// remoteApplyID resolves the remote Tern apply id sent on this drive's
// Progress/Stop/Start/Cutover calls. Multi-operation drives read the claimed
// operation's external_id (which may be empty before dispatch); everything
// else reads the parent external_id.
func (s applyTaskScope) remoteApplyID(apply *storage.Apply) string {
	if s.usesOperationRemoteResume() {
		if s.operation.ExternalID != "" {
			return s.operation.ExternalID
		}
		return s.operation.EngineResumeContext
	}
	return apply.ExternalID
}

// dispatchState returns the state that governs the dispatch / ambiguity
// decision. A multi-operation drive keys on the claimed operation's state: the
// parent apply may already be running because a sibling deployment is active
// while this operation still needs its first remote dispatch.
func (s applyTaskScope) dispatchState(apply *storage.Apply) string {
	if s.usesOperationRemoteResume() {
		return s.operation.State
	}
	return apply.State
}

// loadOperationApplyTaskScope loads and validates the claimed apply_operation
// row and determines whether the parent apply is multi-operation. It fails
// closed on any mismatch so an operation-scoped drive can never act on another
// apply's row, a sibling deployment, or a row outside the parent's operation
// set.
func (c *GRPCClient) loadOperationApplyTaskScope(ctx context.Context, apply *storage.Apply, applyOperationID int64) (applyTaskScope, error) {
	operation, err := c.storage.ApplyOperations().Get(ctx, applyOperationID)
	if err != nil {
		return applyTaskScope{}, fmt.Errorf("load apply_operation %d for apply %s: %w", applyOperationID, apply.ApplyIdentifier, err)
	}
	if operation == nil {
		return applyTaskScope{}, fmt.Errorf("apply_operation %d not found for apply %s", applyOperationID, apply.ApplyIdentifier)
	}
	if operation.ApplyID != apply.ID {
		return applyTaskScope{}, fmt.Errorf("apply_operation %d belongs to apply %d, not %s (%d)", applyOperationID, operation.ApplyID, apply.ApplyIdentifier, apply.ID)
	}
	if operation.Deployment == "" {
		return applyTaskScope{}, fmt.Errorf("apply_operation %d for apply %s has no deployment", applyOperationID, apply.ApplyIdentifier)
	}
	if apply.Deployment != "" && apply.Deployment != operation.Deployment {
		return applyTaskScope{}, fmt.Errorf("apply %s deployment %q does not match apply_operation %d deployment %q", apply.ApplyIdentifier, apply.Deployment, applyOperationID, operation.Deployment)
	}
	ops, err := c.storage.ApplyOperations().ListByApply(ctx, apply.ID)
	if err != nil {
		return applyTaskScope{}, fmt.Errorf("list operations for apply %s: %w", apply.ApplyIdentifier, err)
	}
	found := false
	for _, op := range ops {
		if op.ID == applyOperationID {
			found = true
			break
		}
	}
	if !found {
		return applyTaskScope{}, fmt.Errorf("apply_operation %d is not part of apply %s operation set", applyOperationID, apply.ApplyIdentifier)
	}
	return applyTaskScope{
		applyOperationID: applyOperationID,
		operation:        operation,
		multiOperation:   len(ops) > 1,
	}, nil
}

// remoteApplyIdempotencyKey derives the deduplication key the control plane
// stamps on every remote Apply dispatch. The key is stable across a re-dispatch
// of the same generation — so an ambiguous dispatch whose response was lost is
// reused rather than duplicated — but rotates when the work is deliberately
// retried. Multi-operation drives key on the claimed operation's identity and
// its own attempt, so a sibling operation's retry (which advances the shared
// parent apply.Attempt but not this operation's) never rotates this operation's
// key; whole-apply and single-operation drives key on the parent apply.Attempt,
// which advances only on that apply's own failed_retryable claim. The tuple is
// hashed so the stored key stays within the column width and is free of
// delimiter collisions between variable-length identifiers.
func remoteApplyIdempotencyKey(apply *storage.Apply, scope applyTaskScope) string {
	parts := []string{
		"schemabot-remote-apply-v1",
		apply.ApplyIdentifier,
	}
	if scope.usesOperationRemoteResume() {
		parts = append(parts, "operation", scope.operation.Deployment, scope.operation.OperationKey,
			strconv.Itoa(scope.operation.Attempt))
	} else {
		parts = append(parts, "whole", strconv.Itoa(apply.Attempt))
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return "schemabot:v1:" + hex.EncodeToString(sum[:])
}

// persistRemoteApplyID stores the remote Tern apply id returned by dispatch.
// Single-operation drives keep it on the parent applies.external_id (mutated in
// place so the caller's Applies().Update persists it). Multi-operation drives
// write it to the claimed operation's external_id and never touch the parent
// external_id, refusing to overwrite a different existing id so one deployment
// can't clobber another deployment's remote apply id.
func (c *GRPCClient) persistRemoteApplyID(ctx context.Context, apply *storage.Apply, scope applyTaskScope, remoteID, remoteOperationID string) error {
	if remoteID == "" {
		return fmt.Errorf("refusing to persist empty remote apply id for apply %s", apply.ApplyIdentifier)
	}
	if !scope.usesOperationRemoteResume() {
		apply.ExternalID = remoteID
		return nil
	}
	op := scope.operation
	current, err := c.storage.ApplyOperations().Get(ctx, op.ID)
	if err != nil {
		return fmt.Errorf("reload apply_operation %d before storing remote apply id: %w", op.ID, err)
	}
	if current.ApplyID != apply.ID {
		return fmt.Errorf("apply_operation %d belongs to apply %d, not %s (%d)", op.ID, current.ApplyID, apply.ApplyIdentifier, apply.ID)
	}
	currentRemoteID := current.ExternalID
	if currentRemoteID == "" {
		currentRemoteID = current.EngineResumeContext
	}
	if currentRemoteID != "" && currentRemoteID != remoteID {
		return fmt.Errorf("apply_operation %d already has remote apply id %q; refusing to overwrite with %q", op.ID, currentRemoteID, remoteID)
	}
	if remoteOperationID != "" && current.ExternalOperationID != "" && current.ExternalOperationID != remoteOperationID {
		return fmt.Errorf("apply_operation %d already has remote apply_operation id %q; refusing to overwrite with %q", op.ID, current.ExternalOperationID, remoteOperationID)
	}
	if err := c.storage.ApplyOperations().SaveExternalID(ctx, op.ID, remoteID); err != nil {
		return fmt.Errorf("store remote apply id for apply_operation %d: %w", op.ID, err)
	}
	op.ExternalID = remoteID
	if remoteOperationID != "" {
		if err := c.storage.ApplyOperations().SaveExternalOperationID(ctx, op.ID, remoteOperationID); err != nil {
			return fmt.Errorf("store remote apply_operation id for apply_operation %d: %w", op.ID, err)
		}
		op.ExternalOperationID = remoteOperationID
	}
	slog.InfoContext(ctx, "stored remote gRPC apply identifiers for operation",
		"apply_id", apply.ApplyIdentifier,
		"apply_operation_id", op.ID,
		"deployment", op.Deployment,
		"operation_key", op.OperationKey,
		"operation_kind", op.OperationKind,
		"external_id", remoteID,
		"external_operation_id", remoteOperationID)
	return nil
}

// mirrorRemoteDisplayMetadata persists the data-plane progress response's display
// fields (deploy-request URL, VSchema status) onto the control-plane operation's
// engine_resume_metadata, so the PR comment's stored-state display projection
// (resolveDisplayByOperation) can render them. For a remote (gRPC) apply the
// engine runs in the data plane, so the control plane never sees this metadata
// otherwise. It returns the blob it persisted (or lastBlob unchanged) so the
// caller skips redundant writes across polls. Best-effort: a failure is logged,
// not fatal — the next poll re-mirrors it.
func (c *GRPCClient) mirrorRemoteDisplayMetadata(ctx context.Context, apply *storage.Apply, scope applyTaskScope, md map[string]string, lastBlob string) string {
	if c.storage == nil || apply == nil || apply.Engine != storage.EnginePlanetScale {
		return lastBlob
	}
	blob, err := PSDisplayMetadataStorageBlob(md)
	if err != nil {
		slog.Warn("comment may omit engine display metadata: failed to encode remote display metadata",
			"apply_id", apply.ApplyIdentifier, "error", err)
		return lastBlob
	}
	if blob == "" || blob == lastBlob {
		return lastBlob
	}
	// Load the operation so the write can preserve its engine_resume_context (the
	// remote apply id for a multi-operation drive). SaveEngineResumeState writes
	// both columns, so if we cannot read the current context we must not write:
	// clobbering the remote apply id to empty would break resuming the remote
	// apply after a restart. The mirror is best-effort — skip and retry next poll.
	op, err := c.operationForDisplayMirror(ctx, apply, scope)
	if err != nil || op == nil {
		slog.Warn("comment may omit engine display metadata: could not load apply_operation to preserve resume context",
			"apply_id", apply.ApplyIdentifier, "error", err)
		return lastBlob
	}
	if err := c.storage.ApplyOperations().SaveEngineResumeState(ctx, op.ID, &storage.EngineResumeState{
		ApplyOperationID: op.ID,
		MigrationContext: op.EngineResumeContext,
		Metadata:         blob,
	}); err != nil {
		slog.Warn("comment may omit engine display metadata: failed to persist to control-plane operation",
			"apply_id", apply.ApplyIdentifier, "apply_operation_id", op.ID, "error", err)
		return lastBlob
	}
	return blob
}

// mirrorRemoteVolume copies the volume level the data plane reports on a
// progress response onto the in-memory apply options, so the poll's regular
// parent-apply persistence records it. Volume changes are applied by the
// data-plane driver against its own apply row, and the control plane only
// learns the resulting level from progress responses — the PR comment and the
// control-plane progress API both read the level from the control-plane apply
// options. Returns true when the stored level changed so the caller can log
// the transition.
func mirrorRemoteVolume(apply *storage.Apply, remoteVolume int32) bool {
	if remoteVolume == 0 {
		// The data plane reports 0 when no volume level was ever set on the
		// apply; there is nothing to mirror.
		return false
	}
	if remoteVolume < storage.MinVolume || remoteVolume > storage.MaxVolume {
		slog.Warn("remote progress reported an out-of-range volume level; keeping the stored level",
			append(apply.LogAttrs(), "remote_volume", remoteVolume)...)
		return false
	}
	opts := apply.GetOptions()
	if opts.Volume == int(remoteVolume) {
		return false
	}
	opts.Volume = int(remoteVolume)
	apply.SetOptions(opts)
	return true
}

// operationForDisplayMirror loads the apply_operation whose
// engine_resume_metadata should carry the display projection. An
// operation-scoped drive already knows its operation id; a whole-apply
// (single-operation) drive resolves the apply's sole operation. The loaded row
// carries the current engine_resume_context the mirror must preserve.
func (c *GRPCClient) operationForDisplayMirror(ctx context.Context, apply *storage.Apply, scope applyTaskScope) (*storage.ApplyOperation, error) {
	if scope.applyOperationID > 0 {
		return c.storage.ApplyOperations().Get(ctx, scope.applyOperationID)
	}
	ops, err := c.storage.ApplyOperations().ListByApply(ctx, apply.ID)
	if err != nil {
		return nil, fmt.Errorf("list apply operations for apply %s: %w", apply.ApplyIdentifier, err)
	}
	if len(ops) != 1 {
		return nil, fmt.Errorf("apply %s has %d operations; need an operation scope to mirror display metadata", apply.ApplyIdentifier, len(ops))
	}
	return ops[0], nil
}

// loadApplyTasks loads the task rows the remote drive operates on, scoped either
// to the whole apply or to a single apply_operation. It never widens an
// operation-scoped query back to the whole apply.
func (c *GRPCClient) loadApplyTasks(ctx context.Context, apply *storage.Apply, scope applyTaskScope) ([]*storage.Task, error) {
	if scope.isOperationScoped() {
		tasks, err := c.storage.Tasks().GetByApplyOperationID(ctx, scope.applyOperationID)
		if err != nil {
			return nil, fmt.Errorf("load tasks for apply %s apply_operation %d: %w", apply.ApplyIdentifier, scope.applyOperationID, err)
		}
		// Guard the (apply, apply_operation) trust boundary: the caller passes
		// both an apply and an operation ID, but the query keys only on the
		// operation. A mismatched pair (programming error, stale claim) would
		// otherwise let the drive dispatch and reconcile another apply's tasks
		// under this apply's state. Refuse rather than corrupt cross-apply state.
		for _, task := range tasks {
			if task.ApplyID != apply.ID {
				return nil, fmt.Errorf("apply_operation %d task %s belongs to apply %d, not %s (%d)",
					scope.applyOperationID, task.TaskIdentifier, task.ApplyID, apply.ApplyIdentifier, apply.ID)
			}
		}
		return tasks, nil
	}
	tasks, err := c.storage.Tasks().GetByApplyID(ctx, apply.ID)
	if err != nil {
		return nil, fmt.Errorf("load tasks for apply %s: %w", apply.ApplyIdentifier, err)
	}
	return tasks, nil
}

// ResumeApply starts or resumes a remote (gRPC) apply by driving the whole
// apply — all of its operations.
func (c *GRPCClient) ResumeApply(ctx context.Context, apply *storage.Apply) error {
	return c.resumeApply(ctx, apply, wholeApplyTaskScope())
}

// ResumeApplyOperation starts or resumes a single apply_operation (one
// deployment of a multi-deployment apply) over the remote (gRPC) path. The drive
// logic is identical to ResumeApply; the operation scope only narrows the task
// re-query sites (dispatch, progress poll, terminal reconcile, failure, stop) so
// a driver advances one deployment independently of its siblings.
func (c *GRPCClient) ResumeApplyOperation(ctx context.Context, apply *storage.Apply, applyOperationID int64) error {
	if applyOperationID <= 0 {
		return fmt.Errorf("apply operation id is required")
	}
	if c.storage == nil {
		return fmt.Errorf("storage not configured for GRPCClient")
	}
	if apply == nil {
		return fmt.Errorf("apply is required")
	}
	scope, err := c.loadOperationApplyTaskScope(ctx, apply, applyOperationID)
	if err != nil {
		return err
	}
	// A group_finalizer is task-less by design: it applies the namespace VSchema
	// once its sibling shard work completes. Drive it over gRPC as a VSchema-only
	// apply rather than failing closed on the empty task set, mirroring
	// LocalClient.ResumeApplyOperation's finalizer branch.
	if scope.operation != nil && scope.operation.OperationKind == storage.ApplyOperationKindGroupFinalizer {
		return c.dispatchRemoteGroupFinalizer(ctx, apply, scope)
	}
	// Fail closed before any dispatch or state mutation: a work operation that
	// resolves to no tasks is an invalid or stale claim. The shared resume path
	// would otherwise mark the whole parent apply failed (dispatchPendingApply
	// and the remote-failure sites set applies.state regardless of scope), which
	// is wrong when only this operation's lookup came back empty. Mirrors
	// LocalClient.ResumeApplyOperation.
	tasks, err := c.loadApplyTasks(ctx, apply, scope)
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		return fmt.Errorf("apply_operation %d (apply %s): %w", applyOperationID, apply.ApplyIdentifier, ErrNoTasksForApplyOperation)
	}
	return c.resumeApply(ctx, apply, scope)
}

// dispatchRemoteGroupFinalizer drives a task-less group_finalizer apply_operation
// over gRPC. It is the remote counterpart to LocalClient.driveGroupFinalizer:
// the control plane cannot run the engine, so it dispatches the namespace's
// VSchema as a VSchema-only apply (no DDL, no target shards) to the data plane,
// which applies it via its task-less VSchema-only path
// (isTasklessVSchemaOnlyPlan); records the remote apply id on the operation; and
// polls to completion. Carrying both a VSchema change and the plan's schema
// files lets the remote drive it whether it has the plan locally or materializes
// it from the dispatch.
func (c *GRPCClient) dispatchRemoteGroupFinalizer(ctx context.Context, apply *storage.Apply, scope applyTaskScope) error {
	op := scope.operation
	namespace := namespaceFromFinalizerKey(op.OperationKey)
	if namespace == "" {
		return fmt.Errorf("group_finalizer apply_operation %d (apply %s): malformed operation key %q", op.ID, apply.ApplyIdentifier, op.OperationKey)
	}
	plan, err := c.storage.Plans().GetByID(ctx, apply.PlanID)
	if err != nil {
		return fmt.Errorf("load plan %d for group_finalizer apply_operation %d (apply %s): %w", apply.PlanID, op.ID, apply.ApplyIdentifier, err)
	}
	if plan == nil {
		return fmt.Errorf("plan %d not found for group_finalizer apply_operation %d (apply %s)", apply.PlanID, op.ID, apply.ApplyIdentifier)
	}
	// Fail closed if the namespace carries no VSchema artifact, mirroring the
	// local finalizer drive.
	if _, err := finalizerVSchemaChanges(plan, namespace); err != nil {
		return fmt.Errorf("group_finalizer apply_operation %d (apply %s): %w", op.ID, apply.ApplyIdentifier, err)
	}

	// Dispatch only if this operation has not already been dispatched. On resume
	// the recorded remote apply id lets us poll the existing remote apply instead
	// of starting a duplicate.
	if scope.remoteApplyID(apply) == "" {
		options := effectiveCopyDriveOptions(apply, scope.multiOperation, scope.operation).Map()
		target := options["target"]
		if target == "" {
			target = apply.Database
		}
		if handled, err := c.processPendingCancelOrStopControlRequest(ctx, apply, scope); handled || err != nil {
			return err
		}
		resp, err := c.client.Apply(ctx, &ternv1.ApplyRequest{
			PlanId:      plan.PlanIdentifier,
			Options:     options,
			Database:    apply.Database,
			Type:        apply.DatabaseType,
			DdlChanges:  []*ternv1.TableChange{{Namespace: namespace, TableName: "VSchema: " + namespace, ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA}},
			SchemaFiles: schemaFilesToProto(plan.SchemaFiles),
			Environment: apply.Environment,
			Target:      target,
			Caller:      apply.Caller,
			// No TargetShards: the VSchema is namespace-level, not per shard.
			IdempotencyKey: remoteApplyIdempotencyKey(apply, scope),
		})
		if err != nil {
			if isAmbiguousRemoteApplyDispatchError(err) {
				return fmt.Errorf("group_finalizer apply_operation %d (apply %s) has ambiguous remote dispatch outcome: %w", op.ID, apply.ApplyIdentifier, err)
			}
			if markErr := c.markRemoteApplyFailed(ctx, apply, nil, err.Error(), isRetryableRemoteApplyError(err), scope); markErr != nil {
				return fmt.Errorf("mark group_finalizer apply_operation %d failed after remote apply error: %w", op.ID, markErr)
			}
			return fmt.Errorf("dispatch group_finalizer apply_operation %d (apply %s): %w", op.ID, apply.ApplyIdentifier, err)
		}
		if resp == nil || !resp.Accepted || resp.ApplyId == "" {
			errMsg := "remote group_finalizer apply was not accepted"
			if resp != nil && resp.ErrorMessage != "" {
				errMsg = resp.ErrorMessage
			}
			if markErr := c.markRemoteApplyFailed(ctx, apply, nil, errMsg, false, scope); markErr != nil {
				return fmt.Errorf("mark group_finalizer apply_operation %d failed: %w", op.ID, markErr)
			}
			return fmt.Errorf("dispatch group_finalizer apply_operation %d (apply %s): %s", op.ID, apply.ApplyIdentifier, errMsg)
		}
		if err := c.persistRemoteApplyID(ctx, apply, scope, resp.ApplyId, resp.ApplyOperationId); err != nil {
			return fmt.Errorf("store remote apply id for group_finalizer apply_operation %d: %w", op.ID, err)
		}
	}

	return c.pollForCompletion(ctx, apply, false, scope, false)
}

// ResumeApplyOperationCutover drives a single barrier-parked apply_operation
// through its cutover phase over the remote (gRPC) path. It is the
// deployment-ordered counterpart to ResumeApplyOperation's copy drive: the
// operator claims the parked operation whose turn it is and calls this to force
// the remote swap, while siblings stay parked. The operation's per-operation
// remote apply id is authoritative — the drive never falls back to the parent
// apply's external id and never writes the parent applies row directly, since
// the operator projection CAS owns parent state for multi-operation applies.
func (c *GRPCClient) ResumeApplyOperationCutover(ctx context.Context, apply *storage.Apply, applyOperationID int64) error {
	if applyOperationID <= 0 {
		return fmt.Errorf("apply operation id is required")
	}
	if c.storage == nil {
		return fmt.Errorf("storage not configured for GRPCClient")
	}
	if apply == nil {
		return fmt.Errorf("apply is required")
	}
	scope, err := c.loadOperationApplyTaskScope(ctx, apply, applyOperationID)
	if err != nil {
		return err
	}
	// Ordered cutover is a per-operation remote drive: the swap must target the
	// operation's own remote apply id, never the parent apply external id. Fail
	// closed if this resolved to a whole-apply scope.
	if !scope.usesOperationRemoteResume() {
		return fmt.Errorf("apply_operation %d (apply %s): remote cutover drive requires a per-operation remote resume scope", applyOperationID, apply.ApplyIdentifier)
	}
	// Fail closed before any dispatch or state mutation: an operation that
	// resolves to no tasks is an invalid or stale claim. Mirrors
	// ResumeApplyOperation so an empty lookup never fails the whole parent apply.
	tasks, err := c.loadApplyTasks(ctx, apply, scope)
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		return fmt.Errorf("apply_operation %d (apply %s): %w", applyOperationID, apply.ApplyIdentifier, ErrNoTasksForApplyOperation)
	}
	// Fail closed unless the operation is actually parked or recovering for
	// cutover. A copy-phase or terminal operation must never be forced into a
	// cutover drive.
	if !isCutoverDriveState(scope.operation.State) {
		return fmt.Errorf("apply_operation %d (apply %s) is in state %q, not parked or recovering for cutover", applyOperationID, apply.ApplyIdentifier, scope.operation.State)
	}
	remoteID := scope.remoteApplyID(apply)
	if remoteID == "" {
		return fmt.Errorf("apply_operation %d (apply %s): no remote apply id for cutover drive", applyOperationID, apply.ApplyIdentifier)
	}
	// Honor a stop that raced in after the cutover claim before forcing the swap.
	if handled, err := c.processPendingCancelOrStopControlRequest(ctx, apply, scope); handled || err != nil {
		return err
	}
	poll, err := c.triggerRemoteOperationCutover(ctx, apply, scope, remoteID)
	if err != nil {
		return err
	}
	if !poll {
		return nil
	}
	// The cutover drive must carry the swap past the barrier to terminal, so it
	// never releases at waiting_for_cutover.
	return c.pollForCompletion(ctx, apply, false, scope, false)
}

// triggerRemoteOperationCutover preflights the exact remote state for an ordered
// cutover drive and, only when the operation is still parked at the barrier,
// issues the remote Cutover RPC. The claim moves the operation row to
// cutting_over, so the stored row alone cannot distinguish a fresh claim (cutover
// not sent yet) from a stale recovery (a prior driver already sent it); the
// preflight resolves that from the data plane. It returns poll=true when the
// caller should drive the swap to terminal via pollForCompletion, and poll=false
// when the remote was already terminal (reconciled here) or a raced stop took
// ownership. It never writes the parent applies row directly.
func (c *GRPCClient) triggerRemoteOperationCutover(ctx context.Context, apply *storage.Apply, scope applyTaskScope, remoteID string) (poll bool, err error) {
	resp, err := c.client.Progress(ctx, &ternv1.ProgressRequest{
		ApplyId:     remoteID,
		Environment: apply.Environment,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			message := fmt.Sprintf("remote apply %s was not found by data plane during cutover preflight", remoteID)
			return false, c.failMissingRemoteApply(ctx, apply, message, err, scope)
		}
		return false, fmt.Errorf("preflight remote cutover for apply_operation %d (apply %s) remote %s: %w", scope.applyOperationID, apply.ApplyIdentifier, remoteID, err)
	}
	if resp.State == ternv1.State_STATE_NO_ACTIVE_CHANGE {
		message := fmt.Sprintf("remote apply %s returned no active schema change during cutover preflight", remoteID)
		return false, c.failMissingRemoteApply(ctx, apply, message, nil, scope)
	}
	remoteState := ProtoStateToStorage(resp.State)
	if remoteState == "" {
		return false, fmt.Errorf("preflight remote cutover for apply_operation %d (apply %s): unmapped remote state %s", scope.applyOperationID, apply.ApplyIdentifier, remoteApplyStateDescription(resp.State))
	}
	// Remote already terminal: reconcile from this poll and stop. Do not re-send
	// Cutover.
	if isTerminalProtoState(resp.State) {
		now := time.Now()
		apply.State = remoteState
		apply.ErrorMessage = remoteProgressErrorMessage(apply.State, resp.ErrorMessage, apply.ErrorMessage)
		if apply.StartedAt == nil {
			apply.StartedAt = &now
		}
		return false, c.reconcileTerminalRemoteProgress(ctx, apply, resp.Tables, now, scope)
	}
	switch {
	case state.IsState(remoteState, state.Apply.CuttingOver), state.IsState(remoteState, state.Apply.RevertWindow), state.IsState(remoteState, state.Apply.SkippingRevert):
		// A prior driver already started the swap, the engine is in the post-cutover
		// revert window, or skip-revert is finalizing. Do not re-send Cutover; poll
		// to terminal.
		apply.State = remoteState
		return true, nil
	case state.IsState(remoteState, state.Apply.WaitingForCutover):
		// Parked at the barrier: this drive forces the swap below.
	default:
		// running / recovering / pending / stopped: not yet ready for cutover.
		// Return a retryable error so the operator retries once the remote copy
		// reaches the barrier.
		return false, fmt.Errorf("preflight remote cutover for apply_operation %d (apply %s): remote is %s, not parked at the cutover barrier", scope.applyOperationID, apply.ApplyIdentifier, remoteState)
	}
	// Re-check a raced stop immediately before forcing the swap.
	if handled, err := c.processPendingCancelOrStopControlRequest(ctx, apply, scope); handled || err != nil {
		return false, err
	}
	cutoverResp, err := c.client.Cutover(ctx, &ternv1.CutoverRequest{
		ApplyId:     remoteID,
		Environment: apply.Environment,
	})
	if err != nil {
		return false, fmt.Errorf("request remote cutover for apply_operation %d (apply %s) remote %s: %w", scope.applyOperationID, apply.ApplyIdentifier, remoteID, err)
	}
	if cutoverResp == nil || !cutoverResp.Accepted {
		message := "not accepted"
		if cutoverResp != nil && cutoverResp.ErrorMessage != "" {
			message = cutoverResp.ErrorMessage
		}
		return false, fmt.Errorf("request remote cutover for apply_operation %d (apply %s) remote %s: %s", scope.applyOperationID, apply.ApplyIdentifier, remoteID, message)
	}
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventCutoverTriggered,
		fmt.Sprintf("Remote ordered cutover accepted for apply %s operation %d (remote %s)", apply.ApplyIdentifier, scope.applyOperationID, remoteID), "", "")
	apply.State = state.Apply.CuttingOver
	return true, nil
}

// resumeApply runs work claimed by the operator. Fresh queued applies have no
// external_id yet, so this first dispatches them to remote Tern and stores the
// returned ID. The call then polls until the apply reaches a stored terminal
// state or the operator context is canceled. The scope selects whether the
// drive re-queries tasks for the whole apply or a single operation.
func (c *GRPCClient) resumeApply(ctx context.Context, apply *storage.Apply, scope applyTaskScope) error {
	if c.storage == nil {
		return fmt.Errorf("storage not configured for GRPCClient")
	}
	if apply == nil {
		return fmt.Errorf("apply is required")
	}
	if handled, err := c.processPendingCancelOrStopControlRequest(ctx, apply, scope); handled || err != nil {
		return err
	}
	if err := c.processPendingCutoverControlRequest(ctx, apply, scope); err != nil {
		return err
	}

	if shouldDispatchQueuedRemoteApply(apply, scope) {
		return c.dispatchPendingApply(ctx, apply, scope)
	}
	if hasAmbiguousRemoteDispatchState(apply, scope) {
		errMsg := fmt.Sprintf("gRPC apply %s is %s without a remote apply id; remote dispatch state is ambiguous", apply.ApplyIdentifier, scope.dispatchState(apply))
		if err := c.markRemoteApplyFailed(ctx, apply, nil, errMsg, false, scope); err != nil {
			return fmt.Errorf("%s; persist failure state: %w", errMsg, err)
		}
		return errors.New(errMsg)
	}

	startControlReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationStart)
	if err != nil {
		return err
	}
	startRequested := startControlReq != nil
	if startRequested {
		if deferred, err := c.waitForPendingStopBeforeStart(ctx, apply, scope, startControlReq); err != nil || deferred {
			return err
		}
	}
	if startRequested && state.IsState(apply.State, state.Apply.WaitingForDeploy) {
		if handled, err := c.processPendingCancelOrStopControlRequest(ctx, apply, scope); handled || err != nil {
			return err
		}
		if err := c.processPendingStartControlRequest(ctx, apply, scope); err != nil {
			return err
		}
	}

	remoteID := scope.remoteApplyID(apply)
	if remoteID != "" && state.IsState(apply.State, state.Apply.Pending) && !startRequested {
		if handled, err := c.processPendingCancelOrStopControlRequest(ctx, apply, scope); handled || err != nil {
			return err
		}
		_, err := c.client.Start(ctx, &ternv1.StartRequest{
			ApplyId:     remoteID,
			Environment: apply.Environment,
		})
		if err != nil {
			return fmt.Errorf("start queued gRPC apply %s: %w", apply.ApplyIdentifier, err)
		}
		now := time.Now()
		apply.State = state.InitialActiveApplyState(apply.Engine)
		if apply.StartedAt == nil {
			apply.StartedAt = &now
		}
		persisted, err := c.persistParentApply(ctx, apply, scope, "start queued gRPC apply")
		if err != nil {
			return fmt.Errorf("update started gRPC apply %s: %w", apply.ApplyIdentifier, err)
		}
		if persisted {
			c.logApplyStateTransition(ctx, apply, storage.LogLevelInfo, "Remote apply start requested by operator", state.Apply.Pending)
		}
	}

	// Check the real state from Tern before deciding what to do. Stored state
	// may be stale (e.g. storage says "stopped" but Tern already resumed).
	if state.IsState(apply.State, state.Apply.Stopped) || startRequested {
		oldState := apply.State
		remoteStartRequested := false
		resp, err := c.client.Progress(ctx, &ternv1.ProgressRequest{
			ApplyId:     remoteID,
			Environment: apply.Environment,
		})
		if err == nil {
			if resp.State == ternv1.State_STATE_NO_ACTIVE_CHANGE {
				message := fmt.Sprintf("remote apply %s returned no active schema change for exact apply_id during stopped-state check", apply.ExternalID)
				slog.Warn("remote gRPC stopped-state check returned no active schema change; operator will not request remote start",
					"apply_id", apply.ApplyIdentifier,
					"external_id", apply.ExternalID,
					"database", apply.Database,
					"environment", apply.Environment,
					"stored_state", apply.State)
				return c.failMissingStoppedRemoteApply(ctx, apply, message, nil, scope)
			}
			remoteState := ProtoStateToStorage(resp.State)
			if remoteState == "" {
				message := fmt.Sprintf("Remote stopped-state check returned unmapped state %s; operator will not request remote start", remoteApplyStateDescription(resp.State))
				slog.Warn("remote gRPC stopped-state check returned unmapped state; operator will not request remote start",
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
			apply.ErrorMessage = remoteProgressErrorMessage(apply.State, resp.ErrorMessage, apply.ErrorMessage)
			if isTerminalProtoState(resp.State) && !state.IsState(remoteState, state.Apply.Stopped) {
				now := time.Now()
				if apply.StartedAt == nil && !state.IsState(remoteState, state.Apply.Pending) {
					apply.StartedAt = &now
				}
				return c.reconcileTerminalRemoteProgress(ctx, apply, resp.Tables, now, scope)
			}
		} else {
			if status.Code(err) == codes.NotFound {
				message := fmt.Sprintf("remote apply %s was not found by data plane during stopped-state check", apply.ExternalID)
				return c.failMissingStoppedRemoteApply(ctx, apply, message, err, scope)
			}
			if isTerminalRemoteProgressError(err) {
				message := fmt.Sprintf("remote stopped-state check failed for remote apply %s: %v", apply.ExternalID, err)
				if markErr := c.markStoppedRemoteApplyFailed(ctx, apply, message, false, scope); markErr != nil {
					return fmt.Errorf("mark remote apply %s failed after stopped-state check error: %w", apply.ApplyIdentifier, markErr)
				}
				return fmt.Errorf("check stopped gRPC apply %s before start: %w", apply.ApplyIdentifier, err)
			}
			message := fmt.Sprintf("Remote stopped-state check failed before operator start: %v", err)
			slog.Warn("remote gRPC stopped-state check failed; operator will not request remote start",
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
			if deferred, err := c.completePendingStopBeforeRemoteStart(ctx, apply, scope); err != nil || deferred {
				return err
			}
			_, err := c.client.Start(ctx, &ternv1.StartRequest{
				ApplyId:     remoteID,
				Environment: apply.Environment,
			})
			if err != nil {
				message := fmt.Sprintf("remote start failed for remote apply %s: %v", remoteID, err)
				slog.Warn("remote gRPC start failed; storing stopped state for operator retry",
					"apply_id", apply.ApplyIdentifier,
					"remote_apply_id", remoteID,
					"database", apply.Database,
					"environment", apply.Environment,
					"error", err)
				c.logApplyWarning(ctx, apply, message)
				apply.State = state.Apply.Stopped
				apply.ErrorMessage = message
				if reconcileErr := c.reconcileTerminalRemoteProgress(ctx, apply, resp.Tables, time.Now(), scope); reconcileErr != nil {
					return fmt.Errorf("persist stopped gRPC apply %s after start failure: %w", apply.ApplyIdentifier, reconcileErr)
				}
				if startRequested {
					if failErr := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStart, message); failErr != nil {
						return failErr
					}
				}
				return fmt.Errorf("start gRPC apply %s: %w", apply.ApplyIdentifier, err)
			}
			now := time.Now()
			// The data plane accepted the start but may still report stopped for
			// a short window. Publish resuming, not running, so /api/status and
			// /api/progress/apply/{id} stay consistent until pollForCompletion
			// observes the data plane actually leave stopped.
			apply.State = state.Apply.Resuming
			if apply.StartedAt == nil {
				apply.StartedAt = &now
			}
			remoteStartRequested = true
			if err := c.requeueStoppedTasksForRemoteStart(ctx, apply, scope); err != nil {
				return err
			}
		}

		persisted, err := c.persistParentApply(ctx, apply, scope, "refresh stopped gRPC apply before start")
		if err != nil {
			return fmt.Errorf("update apply state: %w", err)
		}
		if startRequested {
			if err := c.completeApplyStartRequestForScope(ctx, apply, scope); err != nil {
				return err
			}
		}
		if persisted {
			if remoteStartRequested {
				c.logApplyStateTransition(ctx, apply, storage.LogLevelInfo, "Remote apply start requested by operator", oldState)
			} else if oldState != apply.State {
				c.logApplyStateTransition(ctx, apply, storage.LogLevelInfo, fmt.Sprintf("Remote apply state refreshed before operator start: %s -> %s", oldState, apply.State), oldState)
			}
		}
	}

	return c.pollForCompletion(ctx, apply, startRequested, scope,
		shouldReleaseAtCutoverBarrier(apply, scope.multiOperation, scope.operation))
}

// completePendingStopBeforeRemoteStart completes an apply-level stop request
// that raced in just before a remote start. It returns deferred=true when an
// operation-only drive observes a pending stop: that drive never completes the
// parent stop (the operator projection owns it), so it must not start remote
// work and leaves both the stop and start pending for the operator.
func (c *GRPCClient) completePendingStopBeforeRemoteStart(ctx context.Context, apply *storage.Apply, scope applyTaskScope) (bool, error) {
	controlReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationStop)
	if err != nil {
		return false, fmt.Errorf("check pending stop before starting stopped gRPC apply %s: %w", apply.ApplyIdentifier, err)
	}
	if controlReq == nil {
		return false, nil
	}
	if scope.suppressesDirectParentApplyWrites() {
		logOperationDriveLeavesParentStop(apply, scope)
		slog.Info("operation-only drive deferring remote start until apply-level stop resolves",
			"apply_id", apply.ApplyIdentifier,
			"apply_operation_id", scope.applyOperationID,
			"remote_apply_id", scope.remoteApplyID(apply),
			"database", apply.Database,
			"environment", apply.Environment,
			"requested_by", controlRequestCaller(controlReq),
			"state", apply.State)
		return true, nil
	}
	if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStop); err != nil {
		return false, fmt.Errorf("complete pending stop before starting stopped gRPC apply %s: %w", apply.ApplyIdentifier, err)
	}
	slog.Info("completed pending gRPC stop request before starting stopped remote apply",
		"apply_id", apply.ApplyIdentifier,
		"external_id", apply.ExternalID,
		"database", apply.Database,
		"environment", apply.Environment,
		"requested_by", controlRequestCaller(controlReq),
		"state", apply.State)
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStopRequested,
		fmt.Sprintf("Pending remote stop request completed before start%s", callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
	return false, nil
}

func hasPendingStartControlRequest(ctx context.Context, store storage.Storage, apply *storage.Apply) (bool, error) {
	controlReq, err := pendingControlRequest(ctx, store, apply, storage.ControlOperationStart)
	if err != nil {
		return false, err
	}
	return controlReq != nil, nil
}

// waitForPendingStopBeforeStart blocks a pending start until the apply-level
// stop request resolves. It returns deferred=true when the caller must abandon
// the start for this drive: an operation-only drive never completes the parent
// stop (the operator projection owns it), so it stops its own operation's
// remote work and leaves both the stop and start pending for the operator.
func (c *GRPCClient) waitForPendingStopBeforeStart(ctx context.Context, apply *storage.Apply, scope applyTaskScope, startControlReq *storage.ApplyControlRequest) (bool, error) {
	loggedWait := false
	for {
		stopReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationStop)
		if err != nil {
			return false, fmt.Errorf("check pending stop before pending gRPC start for apply %s: %w", apply.ApplyIdentifier, err)
		}
		if stopReq == nil {
			return false, nil
		}
		if scope.suppressesDirectParentApplyWrites() {
			// Stop this operation's own remote work once, then defer: the
			// operation-only drive must not spin waiting for a parent stop it
			// will never complete.
			handled, err := c.processPendingCancelOrStopControlRequest(ctx, apply, scope)
			if err != nil {
				return false, err
			}
			stillPending, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationStop)
			if err != nil {
				return false, fmt.Errorf("recheck pending stop before deferring pending gRPC start for apply %s: %w", apply.ApplyIdentifier, err)
			}
			if stillPending == nil {
				return false, nil
			}
			if !handled {
				logOperationDriveLeavesParentStop(apply, scope)
			}
			slog.Info("operation-only drive deferring pending gRPC start until apply-level stop resolves",
				"apply_id", apply.ApplyIdentifier,
				"apply_operation_id", scope.applyOperationID,
				"remote_apply_id", scope.remoteApplyID(apply),
				"database", apply.Database,
				"environment", apply.Environment,
				"requested_by", controlRequestCaller(startControlReq),
				"stop_requested_by", controlRequestCaller(stillPending),
				"state", apply.State)
			return true, nil
		}
		if !loggedWait {
			slog.Info("pending gRPC start request is waiting for pending stop request to finish",
				"apply_id", apply.ApplyIdentifier,
				"external_id", apply.ExternalID,
				"database", apply.Database,
				"environment", apply.Environment,
				"requested_by", controlRequestCaller(startControlReq),
				"stop_requested_by", controlRequestCaller(stopReq),
				"state", apply.State)
			loggedWait = true
		}
		if _, err := c.processPendingCancelOrStopControlRequest(ctx, apply, scope); err != nil {
			return false, err
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(grpcProgressPollInterval):
		}
	}
}

func (c *GRPCClient) processPendingStartControlRequest(ctx context.Context, apply *storage.Apply, scope applyTaskScope) error {
	controlReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationStart)
	if err != nil {
		return err
	}
	if controlReq == nil {
		return nil
	}
	if stopReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationStop); err != nil {
		return fmt.Errorf("check pending stop before pending gRPC start for apply %s: %w", apply.ApplyIdentifier, err)
	} else if stopReq != nil {
		slog.Info("pending gRPC start request is waiting for pending stop request to finish",
			"apply_id", apply.ApplyIdentifier,
			"external_id", apply.ExternalID,
			"database", apply.Database,
			"environment", apply.Environment,
			"requested_by", controlRequestCaller(controlReq),
			"stop_requested_by", controlRequestCaller(stopReq),
			"state", apply.State)
		return nil
	}
	if !state.IsState(apply.State, state.Apply.WaitingForDeploy) {
		return nil
	}
	remoteID := scope.remoteApplyID(apply)
	if remoteID == "" {
		message := fmt.Sprintf("gRPC apply %s is waiting for deploy without a remote apply id; start dispatch state is ambiguous", apply.ApplyIdentifier)
		if err := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStart, message); err != nil {
			return err
		}
		return errors.New(message)
	}
	_, err = c.client.Start(ctx, &ternv1.StartRequest{
		ApplyId:     remoteID,
		Environment: apply.Environment,
	})
	if err != nil {
		message := fmt.Sprintf("remote deferred deploy start failed for remote apply %s: %v", remoteID, err)
		slog.Warn("remote gRPC deferred deploy start failed; storing start request failure",
			"apply_id", apply.ApplyIdentifier,
			"remote_apply_id", remoteID,
			"database", apply.Database,
			"environment", apply.Environment,
			"error", err)
		if failErr := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStart, message); failErr != nil {
			return failErr
		}
		return fmt.Errorf("start gRPC deferred deploy %s: %w", apply.ApplyIdentifier, err)
	}
	now := time.Now()
	oldState := apply.State
	apply.State = state.InitialActiveApplyState(apply.Engine)
	if apply.StartedAt == nil {
		apply.StartedAt = &now
	}
	persisted, err := c.persistParentApply(ctx, apply, scope, "start gRPC deferred deploy")
	if err != nil {
		return fmt.Errorf("update started gRPC deferred deploy %s: %w", apply.ApplyIdentifier, err)
	}
	if err := c.completeApplyStartRequestForScope(ctx, apply, scope); err != nil {
		return err
	}
	if persisted {
		c.logApplyStateTransition(ctx, apply, storage.LogLevelInfo, fmt.Sprintf("Remote deferred deploy start requested%s", callerApplyLogSuffix(controlRequestCaller(controlReq))), oldState)
	}
	return nil
}

func shouldDispatchQueuedRemoteApply(apply *storage.Apply, scope applyTaskScope) bool {
	if apply == nil {
		return false
	}
	if scope.remoteApplyID(apply) != "" {
		return false
	}
	dispatchState := scope.dispatchState(apply)
	if state.IsState(dispatchState, state.Apply.Pending, state.Apply.FailedRetryable) {
		return true
	}
	// An operation-scoped multi-op drive claims its operation pending→running in
	// a separate transaction before this drive runs, so a freshly claimed
	// operation reaches dispatch in running with no operation-scoped remote id
	// yet. An empty per-operation remote id means nothing was durably dispatched
	// to remote Tern, so this is the operation's first dispatch — not the
	// ambiguous "running with no remote id" case the whole-apply path guards
	// against, where a shared external_id could have been lost after a real
	// dispatch.
	return scope.usesOperationRemoteResume() && state.IsState(dispatchState, state.Apply.Running)
}

func hasAmbiguousRemoteDispatchState(apply *storage.Apply, scope applyTaskScope) bool {
	if apply == nil {
		return false
	}
	return scope.remoteApplyID(apply) == "" &&
		!state.IsTerminalApplyState(scope.dispatchState(apply)) &&
		!shouldDispatchQueuedRemoteApply(apply, scope)
}

func (c *GRPCClient) dispatchPendingApply(ctx context.Context, apply *storage.Apply, scope applyTaskScope) error {
	plan, err := c.storage.Plans().GetByID(ctx, apply.PlanID)
	if err != nil {
		if markErr := c.markRemoteApplyFailed(ctx, apply, nil, fmt.Sprintf("queued gRPC apply failed: load plan %d: %v", apply.PlanID, err), false, scope); markErr != nil {
			return fmt.Errorf("mark queued gRPC apply %s failed after plan load error: %w", apply.ApplyIdentifier, markErr)
		}
		return fmt.Errorf("load plan %d for queued gRPC apply %s: %w", apply.PlanID, apply.ApplyIdentifier, err)
	}
	if plan == nil {
		errMsg := fmt.Sprintf("queued gRPC apply failed: plan %d not found", apply.PlanID)
		if markErr := c.markRemoteApplyFailed(ctx, apply, nil, errMsg, false, scope); markErr != nil {
			return fmt.Errorf("mark queued gRPC apply %s failed after missing plan: %w", apply.ApplyIdentifier, markErr)
		}
		return fmt.Errorf("queued gRPC apply %s: %s", apply.ApplyIdentifier, errMsg)
	}

	tasks, err := c.loadApplyTasks(ctx, apply, scope)
	if err != nil {
		if markErr := c.markRemoteApplyFailed(ctx, apply, nil, fmt.Sprintf("queued gRPC apply failed: load tasks: %v", err), false, scope); markErr != nil {
			return fmt.Errorf("mark queued gRPC apply %s failed after task load error: %w", apply.ApplyIdentifier, markErr)
		}
		return fmt.Errorf("load tasks for queued gRPC apply %s: %w", apply.ApplyIdentifier, err)
	}
	if len(tasks) == 0 {
		errMsg := "queued gRPC apply failed: no tasks found"
		if markErr := c.markRemoteApplyFailed(ctx, apply, nil, errMsg, false, scope); markErr != nil {
			return fmt.Errorf("mark queued gRPC apply %s failed after missing tasks: %w", apply.ApplyIdentifier, markErr)
		}
		return fmt.Errorf("queued gRPC apply %s: %s", apply.ApplyIdentifier, errMsg)
	}
	if err := c.prepareDispatchTasks(ctx, apply, tasks); err != nil {
		return err
	}

	// Fail closed before dispatch when a shard-scoped operation resolves no target
	// shard. A shard work operation (key "namespace/shard/table") must dispatch
	// exactly one shard; if its tasks carry no shard the dispatch would send an
	// empty TargetShards and the data plane would reject it opaquely with
	// "expected exactly one target shard, got 0". Surfacing it here — as a clear
	// control-plane error — turns a version/data skew into an actionable message
	// instead of a confusing data-plane failure.
	targetShards := taskTargetShards(tasks)
	if scope.operation != nil && isShardWorkOperationKey(scope.operation.OperationKey) && len(targetShards) != 1 {
		errMsg := fmt.Sprintf("queued gRPC apply failed: shard operation %q resolved %d target shards, expected exactly 1 — its tasks carry no shard, so refusing to dispatch (the data plane would reject with \"expected exactly one target shard, got 0\"); this indicates a version or data skew", scope.operation.OperationKey, len(targetShards))
		if markErr := c.markRemoteApplyFailed(ctx, apply, nil, errMsg, false, scope); markErr != nil {
			return fmt.Errorf("mark queued gRPC apply %s failed after shard-scope guard: %w", apply.ApplyIdentifier, markErr)
		}
		return fmt.Errorf("queued gRPC apply %s: %s", apply.ApplyIdentifier, errMsg)
	}

	// Use the per-operation copy-drive options so a multi-operation barrier
	// deployment parks the remote engine at the cutover barrier instead of
	// running straight through the swap. effectiveCopyDriveOptions OR's
	// DeferCutover on only for an operation that must auto-defer; whole-apply
	// and single-operation drives get the apply's stored options unchanged, so
	// the deployment-ordered cutover claim (OC-3) can later drive each parked
	// operation through its swap in turn.
	options := effectiveCopyDriveOptions(apply, scope.multiOperation, scope.operation).Map()
	target := options["target"]
	if target == "" {
		target = apply.Database
	}
	if handled, err := c.processPendingCancelOrStopControlRequest(ctx, apply, scope); handled || err != nil {
		return err
	}

	resp, err := c.client.Apply(ctx, &ternv1.ApplyRequest{
		PlanId:         plan.PlanIdentifier,
		Options:        options,
		Database:       apply.Database,
		Type:           apply.DatabaseType,
		DdlChanges:     tasksToProtoTableChanges(tasks),
		SchemaFiles:    schemaFilesToProto(plan.SchemaFiles),
		Environment:    apply.Environment,
		Target:         target,
		Caller:         apply.Caller,
		TargetShards:   targetShards,
		IdempotencyKey: remoteApplyIdempotencyKey(apply, scope),
	})
	if err != nil {
		if isAmbiguousRemoteApplyDispatchError(err) {
			return fmt.Errorf("apply queued gRPC apply %s has ambiguous remote dispatch outcome: %w", apply.ApplyIdentifier, err)
		}
		if markErr := c.markRemoteApplyFailed(ctx, apply, tasks, err.Error(), isRetryableRemoteApplyError(err), scope); markErr != nil {
			return fmt.Errorf("mark queued gRPC apply %s failed after remote apply error: %w", apply.ApplyIdentifier, markErr)
		}
		return fmt.Errorf("apply queued gRPC apply %s: %w", apply.ApplyIdentifier, err)
	}
	if resp == nil {
		errMsg := "remote apply returned nil response"
		if markErr := c.markRemoteApplyFailed(ctx, apply, tasks, errMsg, false, scope); markErr != nil {
			return fmt.Errorf("mark queued gRPC apply %s failed after nil response: %w", apply.ApplyIdentifier, markErr)
		}
		return fmt.Errorf("apply queued gRPC apply %s: %s", apply.ApplyIdentifier, errMsg)
	}
	if !resp.Accepted {
		errMsg := resp.ErrorMessage
		if errMsg == "" {
			errMsg = "remote apply was not accepted"
		}
		if markErr := c.markRemoteApplyFailed(ctx, apply, tasks, errMsg, false, scope); markErr != nil {
			return fmt.Errorf("mark queued gRPC apply %s failed after rejection: %w", apply.ApplyIdentifier, markErr)
		}
		return fmt.Errorf("apply queued gRPC apply %s: %s", apply.ApplyIdentifier, errMsg)
	}
	if resp.ApplyId == "" {
		errMsg := "remote apply accepted without apply_id"
		if markErr := c.markRemoteApplyFailed(ctx, apply, tasks, errMsg, false, scope); markErr != nil {
			return fmt.Errorf("mark queued gRPC apply %s failed after missing remote apply id: %w", apply.ApplyIdentifier, markErr)
		}
		return fmt.Errorf("apply queued gRPC apply %s: %s", apply.ApplyIdentifier, errMsg)
	}

	oldApplyState := apply.State
	now := time.Now()
	// Persist the remote apply id before the parent state update so a failure
	// after this point can resume the exact remote operation instead of
	// dispatching a duplicate. Multi-operation drives store it on the claimed
	// operation row; single-operation drives mutate apply.ExternalID in place.
	if err := c.persistRemoteApplyID(ctx, apply, scope, resp.ApplyId, resp.ApplyOperationId); err != nil {
		return fmt.Errorf("store remote apply id for %s: %w", apply.ApplyIdentifier, err)
	}
	apply.State = state.InitialActiveApplyState(apply.Engine)
	apply.ErrorMessage = ""
	apply.CompletedAt = nil
	if apply.StartedAt == nil {
		apply.StartedAt = &now
	}
	apply.UpdatedAt = now
	persisted, err := c.persistParentApply(ctx, apply, scope, "dispatch gRPC apply")
	if err != nil {
		return fmt.Errorf("update dispatched gRPC apply %s after storing remote apply id %s: %w", apply.ApplyIdentifier, resp.ApplyId, err)
	}
	if persisted {
		c.logApplyStateTransition(ctx, apply, storage.LogLevelInfo,
			fmt.Sprintf("Apply dispatched to remote Tern: target=%s deployment=%s remote_apply_id=%s", target, apply.Deployment, resp.ApplyId),
			oldApplyState)
	}

	return c.pollForCompletion(ctx, apply, false, scope,
		shouldReleaseAtCutoverBarrier(apply, scope.multiOperation, scope.operation))
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

func isTerminalRemoteProgressError(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		return false
	}

	switch st.Code() {
	case codes.InvalidArgument, codes.AlreadyExists, codes.PermissionDenied, codes.Unauthenticated,
		codes.FailedPrecondition, codes.OutOfRange, codes.Unimplemented, codes.DataLoss:
		return true
	case codes.OK, codes.NotFound, codes.Internal, codes.Unknown, codes.Unavailable, codes.ResourceExhausted,
		codes.Aborted, codes.Canceled, codes.DeadlineExceeded:
		return false
	default:
		return false
	}
}

// requeueStoppedTasksForRemoteStart requeues an apply's stopped task rows once the
// data plane accepts a start. The gRPC drive delegates the engine to remote Tern,
// so it must mirror LocalClient.prepareStoppedTasksForResume: a task left at
// "stopped" is pinned there by taskStateWithNoBackwardProgress on every later
// progress poll (a stopped task blocks active engine progress), so the resumed row
// copy would never surface in stored task state and the PR progress comment would
// keep rendering "Stopped" while the data plane is actively copying. Requeuing to
// pending lets the next progress sync advance the task to running.
func (c *GRPCClient) requeueStoppedTasksForRemoteStart(ctx context.Context, apply *storage.Apply, scope applyTaskScope) error {
	tasks, err := c.loadApplyTasks(ctx, apply, scope)
	if err != nil {
		return fmt.Errorf("load tasks to requeue stopped gRPC apply %s for start: %w", apply.ApplyIdentifier, err)
	}
	for _, task := range tasks {
		if !state.IsState(task.State, state.Task.Stopped) {
			continue
		}
		oldState := task.State
		task.State = state.Task.Pending
		task.CompletedAt = nil
		if err := c.storage.Tasks().Update(ctx, task); err != nil {
			return fmt.Errorf("requeue stopped task %s for gRPC apply %s start: %w", task.TaskIdentifier, apply.ApplyIdentifier, err)
		}
		c.logTaskStateTransition(ctx, apply.ID, task,
			fmt.Sprintf("Task %s requeued for start", task.TableName), oldState)
	}
	return nil
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

// isShardWorkOperationKey reports whether an operation key is a sharded work
// key ("namespace/shard/table") — the per-shard fan-out's unit. A whole-apply
// key (empty) and a finalizer key ("namespace/group_finalizer") are not, so the
// shard-scope guard applies only to per-shard work.
func isShardWorkOperationKey(key string) bool {
	parts := strings.Split(key, "/")
	return len(parts) == 3 && parts[0] != "" && parts[1] != "" && parts[2] != ""
}

func taskTargetShards(tasks []*storage.Task) []string {
	seen := make(map[string]struct{})
	var shards []string
	for _, task := range tasks {
		if task.Shard == "" {
			continue
		}
		if _, ok := seen[task.Shard]; ok {
			continue
		}
		seen[task.Shard] = struct{}{}
		shards = append(shards, task.Shard)
	}
	sort.Strings(shards)
	return shards
}

// storedApplyTransitionStatus describes whether a driver may copy a remote
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

func (c *GRPCClient) reloadStoredApplyForRemoteTransition(ctx context.Context, remoteApply *storage.Apply, allowStoppedStoredApply bool) (*storage.Apply, storedApplyTransitionStatus, error) {
	storedApply, err := c.storage.Applies().Get(ctx, remoteApply.ID)
	if err != nil {
		return nil, storedApplyTransitionReloadFailed, fmt.Errorf("reload remote gRPC apply %s: %w", remoteApply.ApplyIdentifier, err)
	}
	if storedApply == nil {
		return nil, storedApplyTransitionMissing, nil
	}
	if storedTerminalApplyBlocksRemoteTransition(storedApply, allowStoppedStoredApply) {
		*remoteApply = *storedApply
		return storedApply, storedApplyTransitionAlreadyTerminal, nil
	}
	return storedApply, storedApplyTransitionReady, nil
}

// A terminal stored apply is usually authoritative: a stale driver must not
// overwrite a newer completed/failed/reverted result. Stopped is the one
// terminal state that may still be superseded when the caller is reconciling an
// exact remote apply ID that is missing or no longer active.
func storedTerminalApplyBlocksRemoteTransition(storedApply *storage.Apply, allowStoppedStoredApply bool) bool {
	if storedApply == nil || !state.IsTerminalApplyState(storedApply.State) {
		return false
	}
	if allowStoppedStoredApply && state.IsState(storedApply.State, state.Apply.Stopped) {
		return false
	}
	return true
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

// completeApplyStartRequestForScope completes the apply-level start control
// request unless this is an operation-scoped multi-op drive. In a multi-op
// drive the start request is shared across sibling operations; one operation
// starting must not complete the shared start request, or stopped siblings
// would become unclaimable before they resume. The rollout projection completes
// it once the aggregate settles.
func (c *GRPCClient) completeApplyStartRequestForScope(ctx context.Context, apply *storage.Apply, scope applyTaskScope) error {
	if scope.usesOperationRemoteResume() {
		slog.Debug("skipping apply-level start request completion during multi-operation drive; shared start request is owned by the rollout projection",
			"apply_id", apply.ApplyIdentifier,
			"apply_operation_id", scope.applyOperationID,
			"deployment", scope.operation.Deployment,
			"environment", apply.Environment)
		return nil
	}
	return completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStart)
}

// persistParentApply writes the parent applies row unless the drive is a
// multi-operation operation-only drive, in which case the parent state is owned
// by the operator's projection CAS and the direct write is skipped. It reports
// whether the write happened so callers can gate parent-level side effects
// (state-transition logs, active-apply metrics) on an actual persist.
func (c *GRPCClient) persistParentApply(ctx context.Context, apply *storage.Apply, scope applyTaskScope, action string) (bool, error) {
	if scope.suppressesDirectParentApplyWrites() {
		slog.Debug("skipping direct parent apply write during multi-operation drive; parent state is owned by the rollout projection",
			"apply_id", apply.ApplyIdentifier,
			"apply_operation_id", scope.applyOperationID,
			"deployment", scope.operation.Deployment,
			"environment", apply.Environment,
			"action", action)
		return false, nil
	}
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		return false, err
	}
	return true, nil
}

// heartbeatScopedDrive refreshes the lease keeping a long drive claimed. A
// multi-operation drive heartbeats its own operation row (it holds no parent
// apply lease); every other drive heartbeats the parent applies row.
func (c *GRPCClient) heartbeatScopedDrive(ctx context.Context, apply *storage.Apply, scope applyTaskScope) error {
	if scope.suppressesDirectParentApplyWrites() {
		return c.storage.ApplyOperations().Heartbeat(ctx, scope.applyOperationID)
	}
	return c.storage.Applies().Heartbeat(ctx, apply.ID)
}

func (c *GRPCClient) markRemoteApplyFailed(ctx context.Context, remoteApply *storage.Apply, storedTasks []*storage.Task, message string, retryable bool, scope applyTaskScope) error {
	return c.markRemoteApplyFailedWithOptions(ctx, remoteApply, storedTasks, message, retryable, false, scope)
}

func (c *GRPCClient) markStoppedRemoteApplyFailed(ctx context.Context, remoteApply *storage.Apply, message string, retryable bool, scope applyTaskScope) error {
	return c.markRemoteApplyFailedWithOptions(ctx, remoteApply, nil, message, retryable, true, scope)
}

func (c *GRPCClient) markRemoteApplyFailedWithOptions(ctx context.Context, remoteApply *storage.Apply, storedTasks []*storage.Task, message string, retryable, allowStoppedStoredApply bool, scope applyTaskScope) error {
	storedApply, transitionStatus, err := c.reloadStoredApplyForRemoteTransition(ctx, remoteApply, allowStoppedStoredApply)
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
		storedTasks, taskErr = c.loadApplyTasks(ctx, storedApply, scope)
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

	// A multi-operation drive records only its operation's task failures here.
	// The operator derives the failed operation row from those tasks and moves
	// the parent applies.state via the projection CAS; the driver must not write
	// the parent failure or run its parent-level side effects.
	if scope.suppressesDirectParentApplyWrites() {
		// A task-less group_finalizer has no task rows to carry the failure, so the
		// operator could never derive its failed operation row. Mark it failed
		// directly on a non-retryable failure; a retryable failure is left
		// non-terminal so the operator re-drives the operation (and re-polls the
		// existing remote apply).
		if scope.finalizerOperationScope() && !retryable {
			if err := c.storage.ApplyOperations().MarkFailed(ctx, scope.applyOperationID, message); err != nil {
				return fmt.Errorf("mark group_finalizer apply_operation %d failed after remote apply failure: %w", scope.applyOperationID, err)
			}
		}
		slog.Debug("recorded operation task failures during multi-operation drive; parent failure is owned by the rollout projection",
			"apply_id", storedApply.ApplyIdentifier,
			"apply_operation_id", scope.applyOperationID,
			"deployment", scope.operation.Deployment,
			"environment", storedApply.Environment,
			"failure_task_state", taskState)
		return nil
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
	// Append the terminal log line before committing the failed state: watchers
	// (the comment observer's poller) act the moment the applies row turns
	// failed, and anything they render from apply_logs — the failure summary's
	// log fold — must already contain the failure line. A failed update after
	// the append leaves the row non-terminal and re-drivable; the retry appends
	// a duplicate line to the event log, which is harmless.
	c.logApplyStateTransition(ctx, storedApply, storage.LogLevelError, fmt.Sprintf("Remote apply failed: %s", message), oldState)
	if err := c.storage.Applies().Update(ctx, storedApply); err != nil {
		return fmt.Errorf("update remote gRPC apply failure %s: %w", storedApply.ApplyIdentifier, err)
	}
	*remoteApply = *storedApply
	metrics.AdjustActiveApplies(ctx, -1, storedApply.Database, storedApply.Deployment, storedApply.Environment)
	return nil
}

func (c *GRPCClient) failMissingRemoteApply(ctx context.Context, remoteApply *storage.Apply, message string, cause error, scope applyTaskScope) error {
	if err := c.markRemoteApplyFailed(ctx, remoteApply, nil, message, false, scope); err != nil {
		return fmt.Errorf("mark missing remote apply %s failed: %w", remoteApply.ApplyIdentifier, err)
	}
	if cause != nil {
		return fmt.Errorf("poll remote apply %s for %s: %w", remoteApply.ExternalID, remoteApply.ApplyIdentifier, cause)
	}
	return fmt.Errorf("poll remote apply %s for %s: %s", remoteApply.ExternalID, remoteApply.ApplyIdentifier, message)
}

func (c *GRPCClient) failMissingStoppedRemoteApply(ctx context.Context, remoteApply *storage.Apply, message string, cause error, scope applyTaskScope) error {
	if err := c.markStoppedRemoteApplyFailed(ctx, remoteApply, message, false, scope); err != nil {
		return fmt.Errorf("mark missing stopped remote apply %s failed: %w", remoteApply.ApplyIdentifier, err)
	}
	if cause != nil {
		return fmt.Errorf("check stopped remote apply %s for %s: %w", remoteApply.ExternalID, remoteApply.ApplyIdentifier, cause)
	}
	return fmt.Errorf("check stopped remote apply %s for %s: %s", remoteApply.ExternalID, remoteApply.ApplyIdentifier, message)
}

func (c *GRPCClient) reconcileTerminalRemoteProgress(ctx context.Context, remoteApply *storage.Apply, remoteTasks []*ternv1.TableProgress, now time.Time, scope applyTaskScope) error {
	// reloadStoredApplyForRemoteTransition may overwrite remoteApply with the
	// stored row when it finds an already-terminal stored apply. Keep the remote
	// Progress result available for the stopped-row exception below.
	remoteApplyFromProgress := *remoteApply
	storedApply, transitionStatus, err := c.reloadStoredApplyForRemoteTransition(ctx, remoteApply, false)

	// An operator claim can start from a stale stored "stopped" row. If the
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
		if storedApply != nil && state.IsTerminalApplyState(storedApply.State) {
			if scope.suppressesDirectParentApplyWrites() {
				controlReq, err := pendingControlRequest(ctx, c.storage, storedApply, storage.ControlOperationStop)
				if err != nil {
					return fmt.Errorf("check pending stop after terminal remote progress for apply %s: %w", storedApply.ApplyIdentifier, err)
				}
				if controlReq != nil {
					logOperationDriveLeavesParentStop(storedApply, scope)
				}
				return nil
			}
			if err := completePendingControlRequests(ctx, c.storage, storedApply, storage.ControlOperationStop); err != nil {
				return err
			}
		}
		return nil
	}

	// Keep the stored apply active until stored task rows are written. If task
	// storage is unavailable, the operator can retry this driver instead of
	// treating a terminal apply as fully reconciled.
	storedTasks, err := c.loadApplyTasks(ctx, storedApply, scope)
	if err != nil {
		return fmt.Errorf("load tasks to sync terminal gRPC progress for %s: %w", storedApply.ApplyIdentifier, err)
	}
	if err := c.syncStoredTasksFromRemoteTasks(ctx, storedApply, storedTasks, remoteTasks, now); err != nil {
		return err
	}
	if err := c.reconcileStoredTasksForTerminalRemoteApply(ctx, storedApply, remoteApply, storedTasks, now); err != nil {
		return err
	}
	return c.persistTerminalStateFromRemote(ctx, storedApply, remoteApply, now, scope)
}

func storedStoppedApplyCanAdoptRemoteTerminalState(storedApply, remoteApply *storage.Apply) bool {
	return storedApply != nil &&
		state.IsState(storedApply.State, state.Apply.Stopped) &&
		!state.IsState(remoteApply.State, state.Apply.Stopped)
}

func (c *GRPCClient) persistTerminalStateFromRemote(ctx context.Context, storedApply, remoteApply *storage.Apply, now time.Time, scope applyTaskScope) error {
	// A multi-operation drive owns only its operation. The terminal task states
	// were already synced by the caller; the operation row is derived from those
	// tasks by the operator, which then moves the parent applies.state via the
	// projection CAS and completes any parent control requests. The driver must
	// not write the parent row or run its parent-level side effects here.
	if scope.suppressesDirectParentApplyWrites() {
		// A task-less group_finalizer has no task rows for the operator to derive
		// the operation row from, so persist its terminal state directly here.
		if scope.finalizerOperationScope() {
			if err := c.persistFinalizerOperationTerminalState(ctx, scope.applyOperationID, remoteApply.State, remoteApply.ErrorMessage); err != nil {
				return err
			}
		}
		slog.Debug("skipping parent terminal write during multi-operation drive; operation tasks are resolved and parent state is owned by the rollout projection",
			"apply_id", storedApply.ApplyIdentifier,
			"apply_operation_id", scope.applyOperationID,
			"deployment", scope.operation.Deployment,
			"environment", storedApply.Environment,
			"remote_state", remoteApply.State)
		return nil
	}
	oldState := storedApply.State
	storedApply.State = remoteApply.State
	storedApply.ErrorMessage = remoteApply.ErrorMessage
	storedApply.StartedAt = remoteApply.StartedAt
	storedApply.CompletedAt = &now
	storedApply.UpdatedAt = now
	if err := c.storage.Applies().Update(ctx, storedApply); err != nil {
		return fmt.Errorf("update terminal remote gRPC apply %s: %w", storedApply.ApplyIdentifier, err)
	}
	if err := completePendingControlRequests(ctx, c.storage, storedApply, storage.ControlOperationStop); err != nil {
		return err
	}
	// Stopped is a terminal apply state, but it is not completion of a pending
	// Start request. A start can be queued while the previous driver is still
	// recording the stop; leave that request pending so the operator can claim
	// the stopped row and perform the resume.
	if !state.IsState(storedApply.State, state.Apply.Stopped) {
		if err := completePendingControlRequests(ctx, c.storage, storedApply, storage.ControlOperationStart); err != nil {
			return err
		}
	}
	c.logApplyStateTransition(ctx, storedApply, remoteTerminalApplyLogLevel(storedApply), remoteTerminalApplyLogMessage(storedApply), oldState)
	*remoteApply = *storedApply
	metrics.AdjustActiveApplies(ctx, -1, storedApply.Database, storedApply.Deployment, storedApply.Environment)
	return nil
}

// persistFinalizerOperationTerminalState reflects a remote terminal state onto a
// task-less group_finalizer operation row. Because such an operation carries no
// task rows, the operator's task-derived projection can never move it: the drive
// owns its terminal transition, mirroring LocalClient.driveGroupFinalizer
// (MarkCompleted on success, MarkFailed on failure). Non-completed/non-failed
// terminal states (stopped/cancelled) stay owned by the operator's stop/cancel
// handling, so the drive leaves the operation row untouched for those.
func (c *GRPCClient) persistFinalizerOperationTerminalState(ctx context.Context, applyOperationID int64, terminalState, errMsg string) error {
	switch {
	case state.IsState(terminalState, state.Apply.Completed):
		if err := c.storage.ApplyOperations().MarkCompleted(ctx, applyOperationID); err != nil {
			return fmt.Errorf("mark group_finalizer apply_operation %d completed from remote terminal state: %w", applyOperationID, err)
		}
	case state.IsState(terminalState, state.Apply.Failed):
		if err := c.storage.ApplyOperations().MarkFailed(ctx, applyOperationID, errMsg); err != nil {
			return fmt.Errorf("mark group_finalizer apply_operation %d failed from remote terminal state: %w", applyOperationID, err)
		}
	}
	return nil
}

func remoteTerminalApplyLogLevel(apply *storage.Apply) string {
	if apply != nil && state.IsState(apply.State, state.Apply.Failed) {
		return storage.LogLevelError
	}
	return storage.LogLevelInfo
}

func remoteTerminalApplyLogMessage(apply *storage.Apply) string {
	message := fmt.Sprintf("Remote apply reached terminal state: %s", apply.State)
	if state.IsState(apply.State, state.Apply.Failed) && apply.ErrorMessage != "" {
		return fmt.Sprintf("%s: %s", message, apply.ErrorMessage)
	}
	return message
}

func remoteProgressErrorMessage(applyState, remoteErrorMessage, existingErrorMessage string) string {
	if state.IsState(applyState, state.Apply.Failed, state.Apply.FailedRetryable) {
		if remoteErrorMessage == "" {
			return existingErrorMessage
		}
		return remoteErrorMessage
	}
	return ""
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
			storedTask.ChecksumRowsChecked = remoteTask.ChecksumRowsChecked
			storedTask.ChecksumRowsTotal = remoteTask.ChecksumRowsTotal
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

// reconcileStoredTasksForTerminalRemoteApply force-resolves any stored task the
// remote progress left unresolved once the remote apply itself is terminal. A
// terminal remote apply is authoritative: the remote will send no further task
// progress, so a lagging task is driven to the apply's terminal state and
// persisted rather than blocking finalization (which would otherwise re-poll the
// already-terminal remote forever). A storage failure is still returned so the
// operator retries that genuinely-transient case.
func (c *GRPCClient) reconcileStoredTasksForTerminalRemoteApply(ctx context.Context, storedApply, remoteApply *storage.Apply, storedTasks []*storage.Task, now time.Time) error {
	terminalTaskState, ok := terminalTaskStateForApply(remoteApply.State)
	if !ok {
		return fmt.Errorf("reconcile stored tasks for %s: remote apply state %q is not terminal", storedApply.ApplyIdentifier, remoteApply.State)
	}
	for _, storedTask := range storedTasks {
		if storedTaskResolvedForTerminalRemoteApply(remoteApply.State, storedTask.State) {
			continue
		}
		oldTaskState := storedTask.State
		storedTask.State = terminalTaskState
		if state.IsState(terminalTaskState, state.Task.Completed) {
			storedTask.ProgressPercent = 100
		}
		if state.IsTerminalTaskState(terminalTaskState) && storedTask.CompletedAt == nil {
			storedTask.CompletedAt = &now
		}
		storedTask.UpdatedAt = now
		if err := c.storage.Tasks().Update(ctx, storedTask); err != nil {
			return fmt.Errorf("reconcile lagging task %s to %s for terminal remote gRPC apply %s: %w", storedTask.TaskIdentifier, terminalTaskState, storedApply.ApplyIdentifier, err)
		}
		slog.Warn("reconciled lagging stored task to terminal remote gRPC apply state",
			"apply_id", storedApply.ApplyIdentifier,
			"external_id", storedApply.ExternalID,
			"remote_apply_state", remoteApply.State,
			"task_id", storedTask.TaskIdentifier,
			"table", storedTask.TableName,
			"old_task_state", oldTaskState,
			"new_task_state", terminalTaskState)
		c.logTaskStateTransition(ctx, storedApply.ID, storedTask, fmt.Sprintf("Task %s reconciled to %s for terminal remote apply state %s", storedTask.TableName, terminalTaskState, remoteApply.State), oldTaskState)
	}
	return nil
}

// terminalTaskStateForApply maps a terminal apply state to the task state a
// lagging stored task must adopt when its terminal apply is authoritative.
func terminalTaskStateForApply(applyState string) (string, bool) {
	switch {
	case state.IsState(applyState, state.Apply.Completed):
		return state.Task.Completed, true
	case state.IsState(applyState, state.Apply.Stopped):
		return state.Task.Stopped, true
	case state.IsState(applyState, state.Apply.Failed):
		return state.Task.Failed, true
	case state.IsState(applyState, state.Apply.Cancelled):
		return state.Task.Cancelled, true
	case state.IsState(applyState, state.Apply.Reverted):
		return state.Task.Reverted, true
	default:
		return "", false
	}
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
func applyStateFromRemoteProgress(storedApplyState, remoteApplyState string, allowStoppedStoredApply bool) string {
	if remoteApplyState == "" {
		return storedApplyState
	}
	if state.IsTerminalApplyState(remoteApplyState) {
		return remoteApplyState
	}
	if allowStoppedStoredApply && state.IsState(storedApplyState, state.Apply.Stopped) {
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
	case state.Apply.SkippingRevert:
		return 11
	case state.Apply.Reverting:
		return 12
	default:
		return 0
	}
}

// pollForCompletion polls the remote Tern for progress and updates SchemaBot's storage.
// Also maintains heartbeat to keep the lease on the apply.
//
// releaseAtCutoverBarrier mirrors the LocalClient copy drive: when set, an
// operation-scoped barrier copy drive exits the moment the remote reaches
// waiting_for_cutover so the operator persists the parked operation row and
// frees it for the deployment-ordered cutover claim, instead of holding the
// lease and polling the parked remote indefinitely. It is false for the cutover
// drive (which must drive the swap past the barrier to terminal) and for
// single-operation / whole-apply drives (which keep waiting for a manual
// cutover unchanged).
func (c *GRPCClient) pollForCompletion(ctx context.Context, apply *storage.Apply, allowStoppedAfterStart bool, scope applyTaskScope, releaseAtCutoverBarrier bool) error {
	ticker := time.NewTicker(grpcProgressPollInterval)
	defer ticker.Stop()

	heartbeatTicker := time.NewTicker(10 * time.Second)
	defer heartbeatTicker.Stop()

	consecutiveProgressErrors := 0
	loggedStoppedAfterStart := false
	var stoppedAfterStartDeadline time.Time
	var lastDisplayBlob string
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-heartbeatTicker.C:
			// Heartbeat: bump updated_at to maintain the lease. A multi-operation
			// drive heartbeats its own operation row; every other drive
			// heartbeats the parent applies row.
			if err := c.heartbeatScopedDrive(ctx, apply, scope); err != nil {
				return fmt.Errorf("heartbeat gRPC apply %s: %w", apply.ApplyIdentifier, err)
			}
		case <-ticker.C:
			if handled, err := c.processPendingCancelOrStopControlRequest(ctx, apply, scope); err != nil {
				slog.Warn("pending gRPC stop request processing failed; current apply owner will exit for operator retry",
					"apply_id", apply.ApplyIdentifier,
					"external_id", apply.ExternalID,
					"database", apply.Database,
					"environment", apply.Environment,
					"error", err)
				return err
			} else if handled {
				return nil
			}
			if err := c.processPendingCutoverControlRequest(ctx, apply, scope); err != nil {
				slog.Warn("pending gRPC cutover request processing failed; current apply owner will exit for operator retry",
					"apply_id", apply.ApplyIdentifier,
					"external_id", apply.ExternalID,
					"database", apply.Database,
					"environment", apply.Environment,
					"error", err)
				return err
			}
			if err := c.processPendingSkipRevertControlRequest(ctx, apply, scope.remoteApplyID(apply)); err != nil {
				slog.Warn("pending gRPC skip-revert request processing failed; current apply owner will exit for operator retry",
					"apply_id", apply.ApplyIdentifier,
					"external_id", apply.ExternalID,
					"database", apply.Database,
					"environment", apply.Environment,
					"error", err)
				return err
			}
			if err := c.processPendingRevertControlRequest(ctx, apply, scope.remoteApplyID(apply)); err != nil {
				slog.Warn("pending gRPC revert request processing failed; current apply owner will exit for operator retry",
					"apply_id", apply.ApplyIdentifier,
					"external_id", apply.ExternalID,
					"database", apply.Database,
					"environment", apply.Environment,
					"error", err)
				return err
			}

			// Poll progress from remote Tern
			remoteID := scope.remoteApplyID(apply)
			resp, err := c.client.Progress(ctx, &ternv1.ProgressRequest{
				ApplyId:     remoteID,
				Environment: apply.Environment,
			})
			if err != nil {
				if status.Code(err) == codes.NotFound {
					message := fmt.Sprintf("remote apply %s was not found by data plane", remoteID)
					return c.failMissingRemoteApply(ctx, apply, message, err, scope)
				}
				if isTerminalRemoteProgressError(err) {
					message := fmt.Sprintf("remote progress failed for remote apply %s: %v", remoteID, err)
					if markErr := c.markRemoteApplyFailed(ctx, apply, nil, message, false, scope); markErr != nil {
						return fmt.Errorf("mark remote apply %s failed after terminal progress error: %w", apply.ApplyIdentifier, markErr)
					}
					return fmt.Errorf("poll remote apply %s for %s: %w", apply.ExternalID, apply.ApplyIdentifier, err)
				}
				consecutiveProgressErrors++
				slog.Warn("remote gRPC progress poll failed",
					"apply_id", apply.ApplyIdentifier,
					"external_id", apply.ExternalID,
					"database", apply.Database,
					"environment", apply.Environment,
					"consecutive_errors", consecutiveProgressErrors,
					"max_consecutive_errors", maxGRPCProgressPollErrorStreak,
					"error", err)
				if consecutiveProgressErrors >= maxGRPCProgressPollErrorStreak {
					message := fmt.Sprintf("remote progress polling failed after %d consecutive errors for remote apply %s: %v",
						consecutiveProgressErrors, apply.ExternalID, err)
					if markErr := c.markRemoteApplyFailed(ctx, apply, nil, message, true, scope); markErr != nil {
						return fmt.Errorf("mark remote apply %s retryable after progress polling errors: %w", apply.ApplyIdentifier, markErr)
					}
					return fmt.Errorf("poll remote apply %s for %s: %w", apply.ExternalID, apply.ApplyIdentifier, err)
				}
				continue
			}
			consecutiveProgressErrors = 0
			if resp.State == ternv1.State_STATE_NO_ACTIVE_CHANGE {
				message := fmt.Sprintf("remote apply %s returned no active schema change for exact apply_id", apply.ExternalID)
				return c.failMissingRemoteApply(ctx, apply, message, nil, scope)
			}

			// Mirror the data-plane display metadata (deploy-request URL, VSchema
			// status) onto the control-plane operation so the PR comment's
			// stored-state projection can render it. The engine that produces this
			// metadata runs in the data plane, so the control plane never sees it
			// otherwise.
			lastDisplayBlob = c.mirrorRemoteDisplayMetadata(ctx, apply, scope, resp.Metadata, lastDisplayBlob)

			// Update apply state from the remote response. An exact apply-id poll
			// must return a concrete state; unknown states are unsafe to reconcile.
			now := time.Now()
			oldApplyState := apply.State
			newState := ProtoStateToStorage(resp.State)
			if newState == "" {
				message := fmt.Sprintf("Remote progress returned unmapped apply state %s; operator will retry without changing stored state", remoteApplyStateDescription(resp.State))
				slog.Warn("remote gRPC progress returned unmapped apply state; operator will retry without changing stored state",
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
			if allowStoppedAfterStart && state.IsState(remoteApplyState, state.Apply.Stopped) {
				if terminalTaskState, ok := terminalApplyStateFromRemoteTaskProgress(resp.Tables); ok {
					remoteApplyState = terminalTaskState
				}
			}
			if allowStoppedAfterStart && state.IsState(remoteApplyState, state.Apply.Stopped) {
				if stoppedAfterStartDeadline.IsZero() {
					stoppedAfterStartDeadline = now.Add(grpcStoppedAfterStartGracePeriod)
				}
				if !loggedStoppedAfterStart {
					slog.Info("remote gRPC apply still stopped after start accepted; operator will keep polling",
						"apply_id", apply.ApplyIdentifier,
						"external_id", apply.ExternalID,
						"database", apply.Database,
						"environment", apply.Environment,
						"stored_state", apply.State,
						"deadline", stoppedAfterStartDeadline)
					loggedStoppedAfterStart = true
				}
				if !now.Before(stoppedAfterStartDeadline) {
					message := fmt.Sprintf("remote apply %s remained stopped after start grace period %s", apply.ExternalID, grpcStoppedAfterStartGracePeriod)
					slog.Warn("remote gRPC apply remained stopped after start grace period; storing stopped state",
						"apply_id", apply.ApplyIdentifier,
						"external_id", apply.ExternalID,
						"database", apply.Database,
						"environment", apply.Environment,
						"stored_state", apply.State,
						"grace_period", grpcStoppedAfterStartGracePeriod)
					c.logApplyWarning(ctx, apply, message)
					apply.State = state.Apply.Stopped
					apply.ErrorMessage = message
					if err := c.reconcileTerminalRemoteProgress(ctx, apply, resp.Tables, now, scope); err != nil {
						return fmt.Errorf("persist stopped gRPC apply %s after start grace period: %w", apply.ApplyIdentifier, err)
					}
					if err := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStart, message); err != nil {
						return err
					}
					return fmt.Errorf("start accepted for gRPC apply %s but %s", apply.ApplyIdentifier, message)
				}
				continue
			}
			newState = applyStateFromRemoteProgress(apply.State, remoteApplyState, allowStoppedAfterStart)
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
			apply.ErrorMessage = remoteProgressErrorMessage(apply.State, resp.ErrorMessage, apply.ErrorMessage)
			apply.UpdatedAt = now
			if mirrorRemoteVolume(apply, resp.Volume) {
				slog.Info("mirrored remote volume level onto control-plane apply options",
					append(apply.LogAttrs(), "volume", resp.Volume)...)
			}

			terminal := isTerminalProtoState(resp.State)
			if terminal {
				return c.reconcileTerminalRemoteProgress(ctx, apply, resp.Tables, now, scope)
			}
			storedTasks, err := c.loadApplyTasks(ctx, apply, scope)
			if err != nil {
				return fmt.Errorf("load tasks to sync gRPC progress for %s: %w", apply.ApplyIdentifier, err)
			}
			if err := c.syncStoredTasksFromRemoteTasks(ctx, apply, storedTasks, resp.Tables, now); err != nil {
				return err
			}
			persisted, err := c.persistParentApply(ctx, apply, scope, "sync nonterminal gRPC progress")
			if err != nil {
				return fmt.Errorf("sync apply %s from gRPC progress: %w", apply.ApplyIdentifier, err)
			}
			if persisted && oldApplyState != apply.State {
				c.logApplyStateTransition(ctx, apply, storage.LogLevelInfo, fmt.Sprintf("Remote apply changed state: %s -> %s", oldApplyState, apply.State), oldApplyState)
			}

			// Park-and-release at the cutover barrier, mirroring the LocalClient
			// copy drive. The tasks were just synced to waiting_for_cutover above,
			// so exit the drive here and release the lease: the operator persists
			// the operation row at waiting_for_cutover and frees it for the
			// deployment-ordered cutover claim to pick up.
			if releaseAtCutoverBarrier && state.IsState(apply.State, state.Apply.WaitingForCutover) {
				slog.Info("operation parked at cutover barrier; exiting remote copy drive",
					"apply_id", apply.ApplyIdentifier,
					"external_id", apply.ExternalID,
					"database", apply.Database,
					"environment", apply.Environment,
					"deployment", apply.Deployment,
					"operation_state", apply.State)
				return nil
			}
		}
	}
}

func terminalApplyStateFromRemoteTaskProgress(remoteTasks []*ternv1.TableProgress) (string, bool) {
	if len(remoteTasks) == 0 {
		return "", false
	}
	taskStates := make([]string, 0, len(remoteTasks))
	for _, remoteTask := range remoteTasks {
		if remoteTask == nil {
			return "", false
		}
		remoteTaskState := state.NormalizeTaskStatus(remoteTask.Status)
		if !state.IsTerminalTaskState(remoteTaskState) {
			return "", false
		}
		taskStates = append(taskStates, remoteTaskState)
	}
	derivedState := state.DeriveApplyState(taskStates)
	return derivedState, state.IsTerminalApplyState(derivedState)
}
