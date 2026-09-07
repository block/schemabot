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
	"slices"
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
	"github.com/block/schemabot/pkg/panicsafe"
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
	logger  *slog.Logger    // base logger; drives bind apply identity via applyLogger

	// Observer support — same pattern as LocalClient.
	// For GRPCClient, the observer is notified by the local progress poller,
	// not by the remote engine.
	observerMu      sync.RWMutex
	observers       map[int64]ProgressObserver
	pendingObserver ProgressObserver

	// controlSendGate throttles retransmission of pending stop/cancel control
	// requests to the data plane (see remoteControlResendInterval).
	controlSendGate remoteControlSendGate

	// unrecognizedStatuses reports remote-reported statuses with no task-state
	// mapping at the drive's ingest points. Zero value is ready.
	unrecognizedStatuses unrecognizedStatusReporter
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

	// Logger is the base logger for drive-path logs. Defaults to
	// slog.Default() when nil.
	Logger *slog.Logger
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
// State-changing RPCs (Apply, Cutover, Stop, Cancel, Start, Revert,
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
		logger:  config.Logger,
	}, nil
}

// applyLogger returns a drive-scoped logger with the apply's identity
// attributes bound, so every line of the drive inherits apply_id, database,
// database_type, environment, and — when set — repo/pr/caller without
// hand-listing them per call. Mutable attributes (state, deployment,
// external_id) stay per-call via Apply.MutableLogAttrs. Falls back to
// slog.Default() when no base logger was configured.
func (c *GRPCClient) applyLogger(apply *storage.Apply) *slog.Logger {
	return c.baseLogger().With(apply.IdentityLogAttrs()...)
}

// baseLogger returns the configured service logger, falling back to
// slog.Default() when none was configured. It is for sites that must log
// before an apply row is loaded, where no identity can be bound yet.
func (c *GRPCClient) baseLogger() *slog.Logger {
	if c.logger == nil {
		return slog.Default()
	}
	return c.logger
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
	resp, err := c.client.PullSchema(ctx, req)
	if err != nil && remotePullSchemaUnsupported(err) {
		return nil, fmt.Errorf("remote data plane does not support pull schema for database %s: %w: %w", req.GetDatabase(), ErrPullSchemaUnsupportedType, err)
	}
	return resp, err
}

// remotePullSchemaUnsupported reports whether a remote PullSchema failure is
// the data plane's own unsupported-type verdict, so the caller can re-derive
// ErrPullSchemaUnsupportedType and classify local and remote pulls the same
// way. The tern server maps that sentinel to codes.Unimplemented with the
// sentinel text in the status message, so both must match here: the code alone
// also arrives from infrastructure — a proxy mapping an HTTP 404, or a data
// plane too old to serve the RPC — and says nothing about the database type.
func remotePullSchemaUnsupported(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	return st.Code() == codes.Unimplemented && strings.Contains(st.Message(), ErrPullSchemaUnsupportedType.Error())
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
		c.baseLogger().ErrorContext(ctx, "missing apply log store for gRPC apply event",
			"apply_row_id", applyID,
			"event", eventType,
			"event_message", message)
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
		c.baseLogger().ErrorContext(ctx, "failed to log gRPC apply event",
			"apply_row_id", applyID,
			"event", eventType,
			"event_message", message,
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
//
// The poll runs behind a panic boundary: observer callbacks render
// GitHub-facing progress and terminal updates from stored state, so a panic on
// one poisoned apply must stop only this apply's notifications, not the
// process that drives every apply. On a contained panic the observer is
// cleared and the poller exits; the apply's drive is unaffected.
func (c *GRPCClient) pollAndNotifyObserver(applyID int64) {
	err := panicsafe.Call(func() error {
		c.notifyObserverUntilTerminal(applyID)
		return nil
	})
	if err == nil {
		return
	}
	var pollPanic *panicsafe.Error
	if !errors.As(err, &pollPanic) {
		// The poll loop returns nothing, so only a contained panic reaches here
		// today; keep the signal if that invariant changes.
		c.baseLogger().Error("observer poll failed; progress and terminal notifications for this apply are stopped", "error", err)
		c.clearObserver(applyID)
		return
	}
	attrs := []any{
		"panic", fmt.Sprint(pollPanic.Value),
		"stack", string(pollPanic.Stack),
	}
	// Best-effort identifier lookup for triage: the poller may have panicked
	// before its first successful apply load, so the identifiers may be
	// unavailable rather than the log being skipped.
	if apply, lookupErr := c.storage.Applies().Get(context.Background(), applyID); lookupErr == nil && apply != nil {
		attrs = append(apply.LogAttrs(), attrs...)
	}
	c.baseLogger().Error("observer poll panicked; progress and terminal notifications for this apply are stopped", attrs...)
	metrics.RecordRecoveredPanic(context.Background(), "observer_poll")
	c.clearObserver(applyID)
}

// notifyObserverUntilTerminal is pollAndNotifyObserver's poll loop: it ticks
// until the apply reaches a terminal state, the observer is cleared, or the
// apply row disappears.
func (c *GRPCClient) notifyObserverUntilTerminal(applyID int64) {
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
			c.baseLogger().Warn("observer poll: failed to load apply; will retry on next tick",
				append(identArgs(), "error", err)...)
			continue
		}
		if apply == nil {
			// The row is gone rather than transiently unreadable, so it will
			// never reappear — stop polling instead of spinning and warning
			// every tick for an apply that no longer exists.
			c.baseLogger().Warn("observer poll: apply not found; stopping poll", identArgs()...)
			c.clearObserver(applyID)
			return
		}
		applyIdentifier = apply.ApplyIdentifier

		tasks, err := c.storage.Tasks().GetByApplyID(context.Background(), applyID)
		if err != nil {
			c.applyLogger(apply).Warn("observer poll: failed to load tasks; will retry on next tick",
				append(apply.MutableLogAttrs(), "error", err)...)
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

func (c *GRPCClient) Logs(ctx context.Context, req *ternv1.LogsRequest) (*ternv1.LogsResponse, error) {
	resp, err := c.client.Logs(ctx, req)
	if status.Code(err) == codes.Unimplemented {
		return nil, fmt.Errorf("selected data plane does not support log reads; upgrade that data plane: %w", err)
	}
	return resp, err
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
	logger := c.applyLogger(apply)
	if cutoverRequestResolvedByApplyState(apply.State) {
		logger.InfoContext(ctx, "completing pending gRPC cutover request for resolved apply",
			append(apply.MutableLogAttrs(), "requested_by", controlRequestCaller(controlReq))...)
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
		logger.InfoContext(ctx, "pending gRPC cutover request is waiting for recovery to complete",
			append(apply.MutableLogAttrs(), "requested_by", controlRequestCaller(controlReq))...)
		return nil
	}
	readyForCutover, err := applyReadyForCutoverRequest(ctx, c.storage, apply)
	if err != nil {
		return fmt.Errorf("check cutover readiness for apply %s: %w", apply.ApplyIdentifier, err)
	}
	if !readyForCutover {
		logger.InfoContext(ctx, "pending gRPC cutover request is waiting for cutover-ready state",
			append(apply.MutableLogAttrs(), "requested_by", controlRequestCaller(controlReq))...)
		return nil
	}
	remoteID := scope.remoteApplyID(apply)
	if remoteID == "" {
		message := "remote apply id is not available"
		if err := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationCutover, message, remoteID); err != nil {
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
	if err := markApplyCuttingOverForControlRequest(ctx, c.storage, apply, logger); err != nil {
		return err
	}
	resp, err := c.client.Cutover(ctx, &ternv1.CutoverRequest{
		ApplyId:     remoteID,
		Environment: apply.Environment,
		Caller:      controlReq.RequestedBy,
	})
	if err != nil {
		errorMessage := fmt.Sprintf("remote cutover failed: %v", err)
		if failErr := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationCutover, errorMessage, remoteID); failErr != nil {
			return fmt.Errorf("request remote gRPC cutover for apply %s remote %s: %w; fail pending cutover request: %w", apply.ApplyIdentifier, remoteID, err, failErr)
		}
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelError, storage.LogEventError,
			fmt.Sprintf("Remote cutover failed for apply %s (remote %s)%s: %v", apply.ApplyIdentifier, remoteID, callerApplyLogSuffix(controlRequestCaller(controlReq)), err), "", "")
		return fmt.Errorf("request remote gRPC cutover for apply %s remote %s: %w", apply.ApplyIdentifier, remoteID, err)
	}
	if resp == nil {
		errorMessage := "the data plane returned neither a response nor an error"
		if err := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationCutover, errorMessage, remoteID); err != nil {
			return err
		}
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelError, storage.LogEventError,
			fmt.Sprintf("Remote cutover returned no response for apply %s (remote %s)%s", apply.ApplyIdentifier, remoteID, callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
		return fmt.Errorf("request remote gRPC cutover for apply %s remote %s: %s", apply.ApplyIdentifier, remoteID, errorMessage)
	}
	if !resp.Accepted {
		errorMessage := controlRefusalMessage(storage.ControlOperationCutover, resp.ErrorMessage)
		if err := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationCutover, errorMessage, remoteID); err != nil {
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
	logger.InfoContext(ctx, "pending gRPC cutover request accepted and completed",
		append(apply.MutableLogAttrs(), "requested_by", controlRequestCaller(controlReq))...)
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
		c.applyLogger(apply).WarnContext(ctx, "skip-revert was not accepted by the data plane",
			append(apply.MutableLogAttrs(), "remote_apply_id", remoteID, "error_message", resp.ErrorMessage)...)
		message := "skip-revert was not accepted by the data plane"
		if resp.ErrorMessage != "" {
			message = fmt.Sprintf("skip-revert was not accepted: %s", resp.ErrorMessage)
		}
		return failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationSkipRevert, message, remoteID)
	}
	if apply.Engine == storage.EnginePlanetScale {
		if err := c.storage.Applies().SetRevertSkipped(ctx, apply.ID, time.Now()); err != nil {
			c.applyLogger(apply).WarnContext(ctx, "failed to record skip-revert on apply",
				append(apply.MutableLogAttrs(), "error", err)...)
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
		c.applyLogger(apply).WarnContext(ctx, "revert was not accepted by the data plane",
			append(apply.MutableLogAttrs(), "remote_apply_id", remoteID, "error_message", resp.ErrorMessage)...)
		message := "revert was not accepted by the data plane"
		if resp.ErrorMessage != "" {
			message = fmt.Sprintf("revert was not accepted: %s", resp.ErrorMessage)
		}
		return failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationRevert, message, remoteID)
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
// The logger is expected to carry the apply's identity attributes already
// bound, so each line appends only the mutable snapshot.
func logOperationDriveLeavesParentStop(logger *slog.Logger, apply *storage.Apply, scope applyTaskScope) {
	logger.Info("operation-only drive leaving apply-level stop request for operator projection",
		append(apply.MutableLogAttrs(), "apply_operation_id", scope.applyOperationID, "remote_apply_id", scope.remoteApplyID(apply))...)
}

func logOperationDriveLeavesParentCancel(logger *slog.Logger, apply *storage.Apply, scope applyTaskScope) {
	logger.Info("operation-only drive leaving apply-level cancel request for operator projection",
		append(apply.MutableLogAttrs(), "apply_operation_id", scope.applyOperationID, "remote_apply_id", scope.remoteApplyID(apply))...)
}

// failRefusedControlRequest resolves a pending stop or cancel that the data
// plane answered with an explicit refusal. The data plane stores stop and cancel
// durably on first receipt, so a refusal is its decision rather than a delivery
// failure: re-sending can only collect the same refusal on every later claim,
// while the schema change keeps running and the operator's command never
// resolves. Failing it with the stated reason ends that loop.
//
// The refusal means the operation did not take effect, so this reports the
// request as not handled, and it drops the resolved request's send-gate entry
// as map hygiene: the gate is keyed on the request id, and a later request for
// the same operation is a new row with a new id.
//
// An operation-only drive owns only its operation and never the shared
// apply-level request, so it leaves the request pending for the operator
// projection to resolve, and says so in its own line rather than reusing the
// caller's — a drive that reported the same sentence twice per tick would read
// as two drives. It records the transmission on its way out: the request stays
// pending by design there, so without the record every later tick would re-send
// a command already refused. It reports the request as not handled for the same
// reason the resolving branch does, which matters more here: an operation-only
// drive returning handled would stand its whole drive step down over a refusal
// that changed nothing.
func (c *GRPCClient) failRefusedControlRequest(ctx context.Context, logger *slog.Logger, apply *storage.Apply, operation storage.ControlOperation, eventType string, controlReq *storage.ApplyControlRequest, scope applyTaskScope, remoteID, errorMessage string) (bool, error) {
	message := controlRefusalMessage(operation, errorMessage)
	if scope.suppressesDirectParentApplyWrites() {
		logger.WarnContext(ctx, "the data plane refused the pending control request; this operation-only drive leaves the shared apply-level request for the operator projection and keeps driving",
			append(apply.MutableLogAttrs(),
				"operation", string(operation),
				"requested_by", controlRequestCaller(controlReq),
				"apply_operation_id", scope.applyOperationID,
				"remote_apply_id", remoteID,
				"error_message", message)...)
		c.controlSendGate.recordSend(controlReq.ID, time.Now())
		return false, nil
	}
	logger.WarnContext(ctx, "the data plane refused the pending control request; the schema change continues and settles on its own",
		append(apply.MutableLogAttrs(),
			"operation", string(operation),
			"requested_by", controlRequestCaller(controlReq),
			"remote_apply_id", remoteID,
			"error_message", message)...)
	// The durable record is rewritten for the operator by failPendingControlRequests;
	// the apply log lands on the same PR timeline, so it is rewritten here too rather
	// than naming a remote identifier that resolves to nothing on the control plane.
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, eventType,
		fmt.Sprintf("Pending %s request rejected by the data plane: %s%s", operation, apply.OperatorFacingMessage(message, remoteID), callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
	if err := failPendingControlRequests(ctx, c.storage, apply, operation, message, remoteID); err != nil {
		return true, fmt.Errorf("request remote gRPC %s for apply %s remote %s: refused with %q; fail pending %s request: %w", operation, apply.ApplyIdentifier, remoteID, message, operation, err)
	}
	c.controlSendGate.clear(controlReq.ID)
	return false, nil
}

func (c *GRPCClient) processPendingStopControlRequest(ctx context.Context, apply *storage.Apply, scope applyTaskScope) (bool, error) {
	controlReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationStop)
	if err != nil {
		return false, err
	}
	if controlReq == nil {
		return false, nil
	}
	logger := c.applyLogger(apply)
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
			logOperationDriveLeavesParentStop(logger, apply, scope)
			return true, nil
		}
	} else if completed, err := completePendingRequestIfStoredApplyResolved(ctx, c.storage, apply, storage.ControlOperationStop); err != nil {
		return true, err
	} else if completed {
		logger.InfoContext(ctx, "completing pending gRPC stop request for resolved apply",
			append(apply.MutableLogAttrs(), "requested_by", controlRequestCaller(controlReq))...)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStopRequested,
			fmt.Sprintf("Pending remote stop request completed for resolved apply%s", callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
		if pendingStart, startErr := pendingStartControlRequest(ctx, c.storage, apply); startErr != nil {
			return true, startErr
		} else if pendingStart != nil {
			return false, nil
		}
		return true, nil
	}
	if state.IsTerminalApplyState(apply.State) {
		if scope.suppressesDirectParentApplyWrites() {
			logOperationDriveLeavesParentStop(logger, apply, scope)
			return true, nil
		}
		logger.InfoContext(ctx, "completing pending gRPC stop request for terminal apply",
			append(apply.MutableLogAttrs(), "requested_by", controlRequestCaller(controlReq))...)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStopRequested,
			fmt.Sprintf("Pending remote stop request completed for terminal apply%s", callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
		if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStop); err != nil {
			return true, err
		}
		c.controlSendGate.clear(controlReq.ID)
		return true, nil
	}
	remoteID := scope.remoteApplyID(apply)
	if remoteID == "" {
		return c.settleUndispatchedControlRequest(ctx, apply, controlReq, scope, stopUndispatchedTerminalization(apply.DatabaseType), logOperationDriveLeavesParentStop)
	}

	// The data plane stores the stop durably on first receipt and its own
	// driver consumes it, so retransmission is redelivery insurance on a bounded
	// cadence — not a per-tick send (see the cancel counterpart).
	if now := time.Now(); c.controlSendGate.shouldSend(controlReq.ID, now) {
		resp, err := c.client.Stop(ctx, &ternv1.StopRequest{
			ApplyId:     remoteID,
			Environment: apply.Environment,
			Caller:      controlReq.RequestedBy,
		})
		if err != nil {
			if completed, completeErr := c.completeRemoteStopFromTerminalProgress(ctx, apply, controlReq, scope); completeErr == nil && completed {
				return true, nil
			} else if completeErr != nil {
				return true, fmt.Errorf("request remote gRPC stop for apply %s remote %s: %w; terminal progress reconciliation also failed: %w", apply.ApplyIdentifier, remoteID, err, completeErr)
			}
			return true, fmt.Errorf("request remote gRPC stop for apply %s remote %s: %w", apply.ApplyIdentifier, remoteID, err)
		}
		if resp == nil {
			return true, fmt.Errorf("request remote gRPC stop for apply %s remote %s: the data plane returned neither a response nor an error", apply.ApplyIdentifier, remoteID)
		}
		if !resp.Accepted {
			return c.failRefusedControlRequest(ctx, logger, apply, storage.ControlOperationStop, storage.LogEventStopRequested, controlReq, scope, remoteID, resp.ErrorMessage)
		}
		if c.controlSendGate.recordSend(controlReq.ID, now) {
			c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStopRequested,
				fmt.Sprintf("Remote stop accepted for apply %s%s", remoteID, callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
		} else {
			logRemoteControlResend(ctx, logger, apply, controlReq, now)
		}
	}

	progress, err := c.controlPathProgress(ctx, apply, remoteID)
	if err != nil {
		return true, fmt.Errorf("sync remote gRPC stop for apply %s remote %s: %w", apply.ApplyIdentifier, remoteID, err)
	}
	if progress.State == ternv1.State_STATE_NO_ACTIVE_CHANGE {
		return true, fmt.Errorf("sync remote gRPC stop for apply %s remote %s: no active schema change", apply.ApplyIdentifier, remoteID)
	}
	remoteState := remoteProgressApplyState(progress.State, progress.Tables)
	if remoteState == "" {
		return true, fmt.Errorf("sync remote gRPC stop for apply %s remote %s: unmapped remote state %s", apply.ApplyIdentifier, remoteID, remoteApplyStateDescription(progress.State))
	}
	now := time.Now()
	priorState, priorStartedAt, priorUpdatedAt := apply.State, apply.StartedAt, apply.UpdatedAt
	if apply.StartedAt == nil && !state.IsState(remoteState, state.Apply.Pending) {
		apply.StartedAt = &now
	}
	apply.State = applyStateFromRemoteProgress(apply.State, remoteState, progress.Tables, false)
	apply.UpdatedAt = now
	if remoteProgressIsTerminal(progress.State, progress.Tables) {
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
			logOperationDriveLeavesParentStop(logger, apply, scope)
			return true, nil
		}
		if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStop); err != nil {
			return true, err
		}
		c.controlSendGate.clear(controlReq.ID)
		if pendingStart, startErr := pendingStartControlRequest(ctx, c.storage, apply); startErr != nil {
			return true, startErr
		} else if pendingStart != nil {
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
	logger.InfoContext(ctx, "remote gRPC stop request accepted and remains pending for remote apply owner",
		append(apply.MutableLogAttrs(),
			"requested_by", controlRequestCaller(controlReq),
			"remote_state", remoteState)...)
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
	logger := c.applyLogger(apply)
	if state.IsTerminalApplyState(apply.State) && !state.IsState(apply.State, state.Apply.Stopped) {
		if scope.suppressesDirectParentApplyWrites() {
			logOperationDriveLeavesParentCancel(logger, apply, scope)
			return true, nil
		}
		if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationCancel); err != nil {
			return true, err
		}
		c.controlSendGate.clear(controlReq.ID)
		return true, nil
	}
	remoteID := scope.remoteApplyID(apply)
	if remoteID == "" {
		return c.settleUndispatchedControlRequest(ctx, apply, controlReq, scope, cancelUndispatchedTerminalization(), logOperationDriveLeavesParentCancel)
	}
	// The data plane stores the cancel durably on first receipt and its own
	// driver consumes it, so retransmission is redelivery insurance on a bounded
	// cadence — not a per-tick send, which floods the data plane with duplicate
	// cancels and the apply log with duplicate accept events while the remote
	// works (or fails) to consume the first one.
	if now := time.Now(); c.controlSendGate.shouldSend(controlReq.ID, now) {
		resp, err := c.client.Cancel(ctx, &ternv1.CancelRequest{ApplyId: remoteID, Environment: apply.Environment, Caller: controlReq.RequestedBy})
		if err != nil {
			if completed, completeErr := c.completeRemoteCancelFromTerminalProgress(ctx, apply, controlReq, scope); completeErr == nil && completed {
				return true, nil
			} else if completeErr != nil {
				return true, fmt.Errorf("request remote gRPC cancel for apply %s remote %s: %w; terminal progress reconciliation also failed: %w", apply.ApplyIdentifier, remoteID, err, completeErr)
			}
			return true, fmt.Errorf("request remote gRPC cancel for apply %s remote %s: %w", apply.ApplyIdentifier, remoteID, err)
		}
		if resp == nil {
			return true, fmt.Errorf("request remote gRPC cancel for apply %s remote %s: the data plane returned neither a response nor an error", apply.ApplyIdentifier, remoteID)
		}
		if !resp.Accepted {
			return c.failRefusedControlRequest(ctx, logger, apply, storage.ControlOperationCancel, storage.LogEventCancelRequested, controlReq, scope, remoteID, resp.ErrorMessage)
		}
		if c.controlSendGate.recordSend(controlReq.ID, now) {
			c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventCancelRequested,
				fmt.Sprintf("Remote cancel accepted for apply %s%s", remoteID, callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
		} else {
			logRemoteControlResend(ctx, logger, apply, controlReq, now)
		}
	}
	progress, err := c.controlPathProgress(ctx, apply, remoteID)
	if err != nil {
		return true, fmt.Errorf("sync remote gRPC cancel for apply %s remote %s: %w", apply.ApplyIdentifier, remoteID, err)
	}
	if progress.State == ternv1.State_STATE_NO_ACTIVE_CHANGE {
		return true, fmt.Errorf("sync remote gRPC cancel for apply %s remote %s: no active schema change", apply.ApplyIdentifier, remoteID)
	}
	remoteState := remoteProgressApplyState(progress.State, progress.Tables)
	if remoteState == "" {
		return true, fmt.Errorf("sync remote gRPC cancel for apply %s remote %s: unmapped remote state %s", apply.ApplyIdentifier, remoteID, remoteApplyStateDescription(progress.State))
	}
	now := time.Now()
	priorState, priorStartedAt, priorUpdatedAt := apply.State, apply.StartedAt, apply.UpdatedAt
	apply.State = applyStateFromRemoteProgress(apply.State, remoteState, progress.Tables, false)
	apply.UpdatedAt = now
	// A stopped remote is not a cancel outcome: the data plane accepts Cancel
	// for stopped applies and its own driver consumes the durable request, so
	// completing here would consume a deliverable cancel while the remote is
	// merely stopped — the stored apply would freeze at stopped after the
	// remote cancels. Keep the request pending and sync the stopped snapshot
	// below; a later drive reconciles once the remote settles. (Mirrors
	// completeRemoteCancelFromTerminalProgress.)
	if remoteProgressIsTerminal(progress.State, progress.Tables) && progress.State != ternv1.State_STATE_STOPPED {
		if err := c.reconcileTerminalRemoteProgress(ctx, apply, progress.Tables, now, scope); err != nil {
			return true, err
		}
		if scope.suppressesDirectParentApplyWrites() {
			apply.State, apply.StartedAt, apply.UpdatedAt = priorState, priorStartedAt, priorUpdatedAt
			logOperationDriveLeavesParentCancel(logger, apply, scope)
			return true, nil
		}
		if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationCancel); err != nil {
			return true, err
		}
		c.controlSendGate.clear(controlReq.ID)
		return true, nil
	}
	storedTasks, err := c.loadApplyTasks(ctx, apply, scope)
	if err != nil {
		return true, fmt.Errorf("load tasks to sync remote gRPC cancel for %s: %w", apply.ApplyIdentifier, err)
	}
	if err := c.syncStoredTasksFromRemoteTasks(ctx, apply, storedTasks, progress.Tables, now); err != nil {
		return true, err
	}
	if _, err := c.persistParentApply(ctx, apply, scope, "sync nonterminal gRPC cancel"); err != nil {
		return true, fmt.Errorf("sync nonterminal remote gRPC cancel state for %s: %w", apply.ApplyIdentifier, err)
	}
	logger.InfoContext(ctx, "remote gRPC cancel request accepted and remains pending for remote apply owner",
		append(apply.MutableLogAttrs(),
			"requested_by", controlRequestCaller(controlReq),
			"remote_state", remoteState)...)
	return false, nil
}

// settleUndispatchedControlRequest settles a pending stop or cancel for an apply
// that carries no remote apply id.
//
// Nothing remote exists to address exactly when
// undispatchedControlRequestSettlesLocally holds: either the dispatch path
// would treat this drive as a first dispatch — the same predicate dispatch
// itself trusts to mean no remote apply was ever created — or the dispatch
// state is stopped, which with no recorded remote apply id proves the work was
// stopped before it was ever dispatched. In both shapes the request is
// satisfied in control-plane storage alone and the operator's command
// completes: there is nothing to orphan.
//
// Every other shape is the ambiguous one — a dispatch may have created a remote
// apply whose response was lost. Settling locally would report the change
// stopped or cancelled while it kept running on the target, and because that
// leaves the apply terminal, nothing would ever revisit it to find out. So this
// reports the request unhandled and lets the drive continue to the dispatch
// ambiguity guard, which fails the apply closed and fails the pending stop and
// cancel requests with the same ambiguity message, so the operator sees the
// rejection rather than a command no later claim would ever answer. The guard
// is where this apply was headed regardless — a pending request must not be
// what routes around it.
//
// The request must never be answered with an error here: it stays pending across
// the error, so every later claim would abort at this same point and the apply
// would occupy a driver on every poll without ever resolving.
func (c *GRPCClient) settleUndispatchedControlRequest(ctx context.Context, apply *storage.Apply, controlReq *storage.ApplyControlRequest, scope applyTaskScope, terminalization undispatchedTerminalization, leaveParentRequestPending func(*slog.Logger, *storage.Apply, applyTaskScope)) (bool, error) {
	logger := c.applyLogger(apply)
	caller := controlRequestCaller(controlReq)
	if !undispatchedControlRequestSettlesLocally(apply, scope) {
		logger.WarnContext(ctx, "leaving pending gRPC control request to the dispatch ambiguity guard; remote dispatch state is ambiguous so the request cannot be satisfied locally",
			append(apply.MutableLogAttrs(),
				"control_operation", terminalization.controlOperation,
				"requested_by", caller,
				"dispatch_state", scope.dispatchState(apply))...)
		return false, nil
	}
	if scope.usesOperationRemoteResume() {
		// A multi-operation apply has one request shared by every deployment.
		// Settling this undispatched operation must not terminalize the parent or
		// complete the apply-level request: sibling deployments with their own
		// remote apply id still need to observe the durable command.
		if err := c.terminalizeUndispatchedApplyOperation(ctx, apply, caller, scope, terminalization); err != nil {
			return true, err
		}
		leaveParentRequestPending(logger, apply, scope)
		return true, nil
	}
	if err := c.terminalizeUndispatchedApply(ctx, apply, caller, scope, terminalization); err != nil {
		return true, err
	}
	if err := completePendingControlRequests(ctx, c.storage, apply, terminalization.controlOperation); err != nil {
		return true, err
	}
	c.controlSendGate.clear(controlReq.ID)
	return true, nil
}

// controlPathProgress polls remote progress on behalf of a control-request path
// and mirrors any control rejections the response carries. A cancel or stop that
// reconciles a terminal remote ends the drive, so the regular poll loop never
// runs again: a rejection the data plane settled after the last regular poll
// reaches the operator only if it is mirrored from here.
func (c *GRPCClient) controlPathProgress(ctx context.Context, apply *storage.Apply, remoteID string) (*ternv1.ProgressResponse, error) {
	progress, err := c.client.Progress(ctx, &ternv1.ProgressRequest{
		ApplyId:     remoteID,
		Environment: apply.Environment,
	})
	if err != nil {
		return nil, err
	}
	c.mirrorRemoteControlRejections(ctx, apply, remoteID, progress.SettledControlRequests)
	return progress, nil
}

func (c *GRPCClient) processPendingCancelOrStopControlRequest(ctx context.Context, apply *storage.Apply, scope applyTaskScope) (bool, error) {
	if handled, err := c.processPendingCancelControlRequest(ctx, apply, scope); handled || err != nil {
		return handled, err
	}
	return c.processPendingStopControlRequest(ctx, apply, scope)
}

func (c *GRPCClient) completeRemoteStopFromTerminalProgress(ctx context.Context, apply *storage.Apply, controlReq *storage.ApplyControlRequest, scope applyTaskScope) (bool, error) {
	logger := c.applyLogger(apply)
	// Route the remote apply id through the scope: an operation-scoped drive
	// tracks its remote apply on the operation, not on the parent apply's
	// ExternalID.
	remoteID := scope.remoteApplyID(apply)
	progress, err := c.controlPathProgress(ctx, apply, remoteID)
	if err != nil {
		logger.WarnContext(ctx, "remote gRPC stop error could not be reconciled from progress",
			append(apply.MutableLogAttrs(),
				"remote_apply_id", remoteID,
				"requested_by", controlRequestCaller(controlReq),
				"error", err)...)
		return false, nil
	}
	if progress.State == ternv1.State_STATE_NO_ACTIVE_CHANGE || !remoteProgressIsTerminal(progress.State, progress.Tables) {
		logger.WarnContext(ctx, "remote gRPC stop error found nonterminal progress; durable stop request remains pending for operator retry",
			append(apply.MutableLogAttrs(),
				"remote_apply_id", remoteID,
				"requested_by", controlRequestCaller(controlReq),
				"remote_state", progress.State.String(),
				"remote_state_number", int32(progress.State))...)
		return false, nil
	}
	remoteState := ProtoStateToStorage(progress.State)
	if remoteState == "" {
		return false, fmt.Errorf("sync remote gRPC stop for apply %s remote %s after stop error: unmapped remote state %s", apply.ApplyIdentifier, remoteID, remoteApplyStateDescription(progress.State))
	}
	now := time.Now()
	priorState, priorStartedAt, priorUpdatedAt, priorErrorMessage := apply.State, apply.StartedAt, apply.UpdatedAt, apply.ErrorMessage
	if apply.StartedAt == nil && !state.IsState(remoteState, state.Apply.Pending) {
		apply.StartedAt = &now
	}
	apply.State = applyStateFromRemoteProgress(apply.State, remoteState, progress.Tables, false)
	apply.ErrorMessage = remoteProgressErrorMessage(apply.State, progress.ErrorMessage, apply.ErrorMessage)
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
		apply.State, apply.StartedAt, apply.UpdatedAt, apply.ErrorMessage = priorState, priorStartedAt, priorUpdatedAt, priorErrorMessage
		logOperationDriveLeavesParentStop(logger, apply, scope)
		return true, nil
	}
	if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStop); err != nil {
		return false, err
	}
	c.controlSendGate.clear(controlReq.ID)
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStopRequested,
		fmt.Sprintf("Remote stop request completed from terminal progress (remote state: %s) after stop error%s", remoteState, callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
	return true, nil
}

// completeRemoteCancelFromTerminalProgress reconciles a failed remote cancel
// against the remote's actual progress. A remote apply that is already
// terminal (for example a cancel consumed on a previous drive before the
// driver could persist it) rejects a re-sent Cancel; that rejection means the
// requested outcome already happened, so the drive must reconcile the stored
// apply to the remote's terminal state and complete the durable cancel request
// instead of failing the resume and leaving the apply claimable forever. A
// remote that is still active — or stopped, since stopped remotes remain
// cancellable — keeps the durable request pending for the next drive to retry.
func (c *GRPCClient) completeRemoteCancelFromTerminalProgress(ctx context.Context, apply *storage.Apply, controlReq *storage.ApplyControlRequest, scope applyTaskScope) (bool, error) {
	logger := c.applyLogger(apply)
	// Route the remote apply id through the scope: an operation-scoped drive
	// tracks its remote apply on the operation, not on the parent apply's
	// ExternalID.
	remoteID := scope.remoteApplyID(apply)
	progress, err := c.controlPathProgress(ctx, apply, remoteID)
	if err != nil {
		logger.WarnContext(ctx, "remote gRPC cancel error could not be reconciled from progress",
			append(apply.MutableLogAttrs(),
				"remote_apply_id", remoteID,
				"requested_by", controlRequestCaller(controlReq),
				"error", err)...)
		return false, nil
	}
	if progress.State == ternv1.State_STATE_NO_ACTIVE_CHANGE || !remoteProgressIsTerminal(progress.State, progress.Tables) {
		logger.WarnContext(ctx, "remote gRPC cancel error found nonterminal progress; durable cancel request remains pending for operator retry",
			append(apply.MutableLogAttrs(),
				"remote_apply_id", remoteID,
				"requested_by", controlRequestCaller(controlReq),
				"remote_state", progress.State.String(),
				"remote_state_number", int32(progress.State))...)
		return false, nil
	}
	// A stopped remote is not a cancel outcome: the data plane accepts Cancel
	// for stopped applies, so the failed Cancel cannot have been an
	// already-terminal rejection. Completing the request here would consume a
	// deliverable cancel and let a pending start resume a change the user
	// cancelled. Keep the durable request pending for retry.
	if progress.State == ternv1.State_STATE_STOPPED {
		logger.WarnContext(ctx, "remote gRPC cancel error found stopped progress; stopped remotes remain cancellable so the durable cancel request remains pending for retry",
			append(apply.MutableLogAttrs(),
				"remote_apply_id", remoteID,
				"requested_by", controlRequestCaller(controlReq))...)
		return false, nil
	}
	remoteState := ProtoStateToStorage(progress.State)
	if remoteState == "" {
		return false, fmt.Errorf("sync remote gRPC cancel for apply %s remote %s after cancel error: unmapped remote state %s", apply.ApplyIdentifier, remoteID, remoteApplyStateDescription(progress.State))
	}
	now := time.Now()
	priorState, priorStartedAt, priorUpdatedAt, priorErrorMessage := apply.State, apply.StartedAt, apply.UpdatedAt, apply.ErrorMessage
	if apply.StartedAt == nil && !state.IsState(remoteState, state.Apply.Pending) {
		apply.StartedAt = &now
	}
	apply.State = applyStateFromRemoteProgress(apply.State, remoteState, progress.Tables, false)
	apply.ErrorMessage = remoteProgressErrorMessage(apply.State, progress.ErrorMessage, apply.ErrorMessage)
	apply.UpdatedAt = now
	if err := c.reconcileTerminalRemoteProgress(ctx, apply, progress.Tables, now, scope); err != nil {
		return false, err
	}
	// An operation-only drive owns only its operation: the apply-level cancel
	// request is shared across siblings and completed by the operator projection
	// once the parent derives terminal. Leave it pending here and restore the
	// in-memory parent apply fields the driver does not own, so this operation's
	// terminal remote state does not leak onto the shared apply and let a later
	// cancel pass treat the parent as terminal.
	if scope.suppressesDirectParentApplyWrites() {
		apply.State, apply.StartedAt, apply.UpdatedAt, apply.ErrorMessage = priorState, priorStartedAt, priorUpdatedAt, priorErrorMessage
		logOperationDriveLeavesParentCancel(logger, apply, scope)
		return true, nil
	}
	if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationCancel); err != nil {
		return false, err
	}
	c.controlSendGate.clear(controlReq.ID)
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventCancelRequested,
		fmt.Sprintf("Remote cancel request completed from terminal progress (remote state: %s) after cancel error%s", remoteState, callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
	return true, nil
}

// undispatchedTerminalization describes how a drive settles work that was never
// dispatched to the data plane. A control operation that arrives before dispatch
// is satisfied entirely in control-plane storage — there is no remote apply to
// address — so each operation supplies the states it terminalizes to and the
// vocabulary its operator-facing log lines use.
type undispatchedTerminalization struct {
	taskState      string
	applyState     string
	operationState string
	logEvent       string
	// verb reads in "Remote apply <verb> before dispatch".
	verb string
	// controlOperation names the durable request being settled. It is the
	// noun, so operator text reads "pending apply <controlOperation> request"
	// where the verb would read as a state.
	controlOperation storage.ControlOperation
}

// stopUndispatchedTerminalization is how a stop settles undispatched work. A
// stop is resumable on database types that can pause a change and terminal on
// those that cannot, so the states follow stopTerminatesChange.
func stopUndispatchedTerminalization(databaseType string) undispatchedTerminalization {
	terminalization := undispatchedTerminalization{
		taskState:        state.Task.Stopped,
		applyState:       state.Apply.Stopped,
		operationState:   state.ApplyOperation.Stopped,
		logEvent:         storage.LogEventStopRequested,
		verb:             "stopped",
		controlOperation: storage.ControlOperationStop,
	}
	if stopTerminatesChange(databaseType) {
		terminalization.taskState = state.Task.Cancelled
		terminalization.applyState = state.Apply.Cancelled
		terminalization.operationState = state.ApplyOperation.Cancelled
	}
	return terminalization
}

// cancelUndispatchedTerminalization is how a cancel settles undispatched work.
// A cancel is terminal on every database type, so it does not vary.
func cancelUndispatchedTerminalization() undispatchedTerminalization {
	return undispatchedTerminalization{
		taskState:        state.Task.Cancelled,
		applyState:       state.Apply.Cancelled,
		operationState:   state.ApplyOperation.Cancelled,
		logEvent:         storage.LogEventCancelRequested,
		verb:             "cancelled",
		controlOperation: storage.ControlOperationCancel,
	}
}

// terminalizeUndispatchedApply settles a whole apply that never reached the data
// plane: its tasks and the apply row move to the control operation's terminal
// states in control-plane storage alone. Already-terminal tasks are left as they
// are, so a partially settled apply is not rewritten.
func (c *GRPCClient) terminalizeUndispatchedApply(ctx context.Context, apply *storage.Apply, caller string, scope applyTaskScope, terminalization undispatchedTerminalization) error {
	now := time.Now()
	tasks, err := c.loadApplyTasks(ctx, apply, scope)
	if err != nil {
		return fmt.Errorf("load tasks for undispatched %s %s: %w", terminalization.controlOperation, apply.ApplyIdentifier, err)
	}
	logger := c.applyLogger(apply)
	for _, task := range tasks {
		if state.IsTerminalTaskState(task.State) {
			logger.InfoContext(ctx, "leaving terminal gRPC task unchanged while settling an undispatched control request",
				"task_id", task.TaskIdentifier,
				"table", task.TableName,
				"task_state", task.State,
				"control_operation", terminalization.controlOperation)
			continue
		}
		task.State = terminalization.taskState
		if state.IsState(terminalization.taskState, state.Task.Cancelled) {
			task.CompletedAt = &now
		}
		task.UpdatedAt = now
		if err := c.storage.Tasks().Update(ctx, task); err != nil {
			return fmt.Errorf("update task %s for undispatched %s %s: %w", task.TaskIdentifier, terminalization.controlOperation, apply.ApplyIdentifier, err)
		}
	}
	oldState := apply.State
	apply.State = terminalization.applyState
	apply.CompletedAt = nil
	if state.IsState(terminalization.applyState, state.Apply.Cancelled) {
		apply.CompletedAt = &now
	}
	apply.UpdatedAt = now
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		return fmt.Errorf("update undispatched %s gRPC apply %s: %w", terminalization.controlOperation, apply.ApplyIdentifier, err)
	}
	c.logApplyStateTransition(ctx, apply, storage.LogLevelInfo, fmt.Sprintf("Remote apply %s before dispatch: %s%s", terminalization.verb, apply.State, callerApplyLogSuffix(caller)), oldState)
	return nil
}

// terminalizeUndispatchedApplyOperation settles a single undispatched operation
// of a multi-operation apply. It terminalizes only this operation's tasks and
// the operation row, never the parent apply, and never completes the
// apply-level control request: that request is shared across deployments and
// must remain pending so sibling operations with their own remote apply id
// still observe it.
func (c *GRPCClient) terminalizeUndispatchedApplyOperation(ctx context.Context, apply *storage.Apply, caller string, scope applyTaskScope, terminalization undispatchedTerminalization) error {
	if !scope.usesOperationRemoteResume() {
		return fmt.Errorf("undispatched operation %s for apply %s requires multi-operation scope", terminalization.controlOperation, apply.ApplyIdentifier)
	}
	op := scope.operation
	now := time.Now()
	tasks, err := c.loadApplyTasks(ctx, apply, scope)
	if err != nil {
		return fmt.Errorf("load tasks for undispatched operation %s %s apply_operation %d: %w", terminalization.controlOperation, apply.ApplyIdentifier, op.ID, err)
	}
	logger := c.applyLogger(apply)
	for _, task := range tasks {
		if state.IsTerminalTaskState(task.State) {
			logger.InfoContext(ctx, "leaving terminal gRPC task unchanged while settling an undispatched operation's control request",
				"apply_operation_id", op.ID,
				"deployment", op.Deployment,
				"task_id", task.TaskIdentifier,
				"table", task.TableName,
				"task_state", task.State,
				"control_operation", terminalization.controlOperation)
			continue
		}
		task.State = terminalization.taskState
		if state.IsState(terminalization.taskState, state.Task.Cancelled) {
			task.CompletedAt = &now
		}
		task.UpdatedAt = now
		if err := c.storage.Tasks().Update(ctx, task); err != nil {
			return fmt.Errorf("update task %s for undispatched operation %s %s apply_operation %d: %w", task.TaskIdentifier, terminalization.controlOperation, apply.ApplyIdentifier, op.ID, err)
		}
	}
	oldState := op.State
	if state.IsState(terminalization.operationState, state.ApplyOperation.Cancelled) {
		if err := c.storage.ApplyOperations().MarkTerminal(ctx, op.ID, terminalization.operationState); err != nil {
			return fmt.Errorf("mark undispatched gRPC apply_operation %d cancelled for apply %s: %w", op.ID, apply.ApplyIdentifier, err)
		}
		op.CompletedAt = &now
	} else {
		if err := c.storage.ApplyOperations().UpdateState(ctx, op.ID, terminalization.operationState); err != nil {
			return fmt.Errorf("mark undispatched gRPC apply_operation %d %s for apply %s: %w", op.ID, terminalization.controlOperation, apply.ApplyIdentifier, err)
		}
		op.CompletedAt = nil
	}
	op.State = terminalization.operationState
	op.UpdatedAt = now
	logger.InfoContext(ctx, "settled undispatched multi-operation gRPC apply operation; apply-level control request remains pending for siblings",
		"apply_operation_id", op.ID,
		"deployment", op.Deployment,
		"requested_by", caller,
		"control_operation", terminalization.controlOperation,
		"old_operation_state", oldState,
		"new_operation_state", terminalization.operationState)
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, terminalization.logEvent,
		fmt.Sprintf("Remote apply operation %d (deployment %s) %s before dispatch: %s%s; pending apply %s request remains for sibling operations", op.ID, op.Deployment, terminalization.verb, terminalization.operationState, callerApplyLogSuffix(caller), terminalization.controlOperation), "", "")
	return nil
}

func (c *GRPCClient) Start(ctx context.Context, req *ternv1.StartRequest) (*ternv1.StartResponse, error) {
	return c.client.Start(ctx, req)
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

	// operationLeaseOnly is true when the drive was claimed under an operation
	// lease with no parent apply lease, which is how the operator claims any
	// operation — including the sole operation of a single-operation apply.
	// Such a drive owns its operation row and nothing else: storage refuses a
	// direct parent write from that context, and the rollout projection moves
	// the parent state instead. Operation count cannot stand in for this, and a
	// drive that assumes it can write the parent because it counted one
	// operation loses every parent write to the storage guard.
	operationLeaseOnly bool

	// tasklessOperation is true when the claimed operation carries no task rows,
	// so nothing derives its terminal state but this drive.
	tasklessOperation bool

	// deploymentOperationKeys is the full operation-key set of the claimed
	// operation's deployment, captured from the parent's operation rows at claim
	// load. Deployment-keyed dispatches send it as the generation manifest so
	// the data plane knows the whole generation from the first dispatch. Empty
	// for whole-apply scopes.
	deploymentOperationKeys []string
}

func wholeApplyTaskScope() applyTaskScope {
	return applyTaskScope{}
}

func (s applyTaskScope) isOperationScoped() bool {
	return s.applyOperationID > 0
}

// usesOperationRemoteResume reports whether this drive owns only its claimed
// operation, so the remote apply id lives on that operation row rather than on
// the parent applies.external_id. Every operation-lease-only drive does,
// whatever the operation count: a drive that cannot write the parent cannot
// persist a remote apply id there either, and one that tries loses the id and
// dispatches the work a second time on the next claim. Whole-apply drives keep
// using the parent external_id.
func (s applyTaskScope) usesOperationRemoteResume() bool {
	return (s.multiOperation || s.operationLeaseOnly) && s.operation != nil
}

// suppressesDirectParentApplyWrites reports whether this drive must not write
// the parent applies row (state, heartbeat) or run parent-level side effects
// (parent stop-request completion, active-apply metrics). An operation-scoped
// drive owns only its operation: the parent applies.state is moved solely by the
// operation-authorized projection CAS in the operator. Whole-apply drives keep
// writing the parent directly.
func (s applyTaskScope) suppressesDirectParentApplyWrites() bool {
	return s.usesOperationRemoteResume()
}

// tasklessOperationScope reports whether this drive owns an operation with no
// task rows: a group_finalizer, or a work operation whose plan changes only
// VSchemas. The operator's task-derived operation→parent projection can never
// move such an operation row off pending, so the terminal remote state
// (completion or failure) would be lost and the parent would stay non-terminal
// with its target blocked. A drive that owns one must therefore persist its own
// operation row's terminal state, mirroring the local drive.
func (s applyTaskScope) tasklessOperationScope() bool {
	return s.isOperationScoped() && s.tasklessOperation
}

// remoteApplyID resolves the remote Tern apply id sent on this drive's
// Progress/Stop/Start/Cutover calls. Operation-owning drives read the claimed
// operation's recorded remote apply id (which may be empty before dispatch);
// whole-apply drives read the parent external_id.
func (s applyTaskScope) remoteApplyID(apply *storage.Apply) string {
	if s.usesOperationRemoteResume() {
		if id := s.operation.RemoteApplyID(); id != "" {
			return id
		}
		// A sole operation's remote apply id is unambiguously its own, so a drive
		// that finds one recorded on the parent resumes it rather than dispatching
		// the work again. A multi-operation apply gets nothing here: the parent id
		// there belongs to whichever deployment dispatched last, and polling a
		// sibling's remote apply would report another deployment's outcome as this
		// one's.
		if !s.multiOperation {
			return apply.ExternalID
		}
		return ""
	}
	return apply.ExternalID
}

// generationOperationKeys returns the generation manifest a deployment-keyed
// dispatch carries: every operation key this deployment will send to the
// shared remote apply under the dispatch's idempotency key, the dispatch's own
// key included. It is non-empty exactly when the dispatch is deployment-keyed
// (usesOperationRemoteResume, mirroring remoteApplyIdempotencyKey): those are
// the dispatches that arrive one operation at a time, so the data plane needs
// the whole expected set to know when the shared apply's generation is
// complete. Whole-apply dispatches carry none — the dispatch is the whole
// generation.
//
// A deliberate retry (the operation's own attempt above zero) declares only
// its own key, mirroring its operation-scoped idempotency key: the retry's
// remote apply receives exactly this one operation, and declaring the whole
// deployment set would hold that apply open forever for siblings that are
// never redispatched.
func (s applyTaskScope) generationOperationKeys() []string {
	if !s.usesOperationRemoteResume() {
		return nil
	}
	if s.operation.Attempt > 0 {
		return []string{s.operation.OperationKey}
	}
	return s.deploymentOperationKeys
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
	deploymentOperationKeys := make([]string, 0, len(ops))
	for _, op := range ops {
		if op.ID == applyOperationID {
			found = true
		}
		if op.Deployment == operation.Deployment {
			deploymentOperationKeys = append(deploymentOperationKeys, op.OperationKey)
		}
	}
	if !found {
		return applyTaskScope{}, fmt.Errorf("apply_operation %d is not part of apply %s operation set", applyOperationID, apply.ApplyIdentifier)
	}
	slices.Sort(deploymentOperationKeys)
	return applyTaskScope{
		applyOperationID:        applyOperationID,
		operation:               operation,
		multiOperation:          len(ops) > 1,
		operationLeaseOnly:      operationLeaseOnly(ctx),
		deploymentOperationKeys: deploymentOperationKeys,
	}, nil
}

// operationLeaseOnly reports whether the calling drive holds an operation lease
// and no parent apply lease, which is the storage rule for whether it may write
// the parent applies row. Reading it from the lease context keeps the drive's
// idea of what it owns identical to the one storage enforces, so a drive never
// attempts a write that is guaranteed to come back as a lost lease.
func operationLeaseOnly(ctx context.Context) bool {
	if _, hasOperationLease := storage.OperationLeaseFromContext(ctx); !hasOperationLease {
		return false
	}
	_, hasApplyLease := storage.ApplyLeaseFromContext(ctx)
	return !hasApplyLease
}

// remoteApplyIdempotencyKey derives the deduplication key the control plane
// stamps on every remote Apply dispatch. The key is stable across a re-dispatch
// of the same generation — so an ambiguous dispatch whose response was lost is
// reused rather than duplicated — and rotates when the dispatched work is
// deliberately retried.
//
// Operation-scoped drives key generation zero on the deployment, not the
// individual operation: every sibling operation a deployment dispatches in its
// first generation carries the same key, so the data plane lands them on one
// remote apply — the first dispatch creates it, each sibling attaches its own
// operation, and the data plane tells the dispatches apart by the operation
// key it derives from each request's shape. The generation is the operation's
// own attempt, never the shared apply.Attempt: the parent counter advances
// when any sibling operation of any deployment is redispatched (it feeds the
// apply's retry budget), so keying on it would let one deployment's genuine
// retry rotate the key under another deployment's orphaned dispatch and
// duplicate its remote apply.
//
// A deliberate retry (the operation's own attempt above zero) keys on the
// operation as well: a retry redispatches only its own operation — siblings
// that succeeded stay on the generation-zero apply and are never sent again —
// so a deployment-shared retry key would land the retry on a remote apply
// whose declared generation includes work that can never arrive, and its
// completion gate would hold it open forever. An operation-scoped key gives
// each retried operation its own remote apply that completes on its own work.
//
// Whole-apply drives key on the parent apply alone and rotate on its attempt.
// The tuple is hashed so the stored key stays within the column width and is
// free of delimiter collisions between variable-length identifiers.
func remoteApplyIdempotencyKey(apply *storage.Apply, scope applyTaskScope) string {
	parts := []string{
		"schemabot-remote-apply-v1",
		apply.ApplyIdentifier,
	}
	if scope.usesOperationRemoteResume() {
		parts = append(parts, "deployment", scope.operation.Deployment, strconv.Itoa(scope.operation.Attempt))
		if scope.operation.Attempt > 0 {
			parts = append(parts, "operation", scope.operation.OperationKey)
		}
	} else {
		parts = append(parts, "whole", strconv.Itoa(apply.Attempt))
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return "schemabot:v1:" + hex.EncodeToString(sum[:])
}

// verifyDispatchOperationKeyEcho checks an accepted ApplyResponse against the
// operation key the dispatched request's shape derives to. Deployment-keyed
// dispatches share one remote apply across sibling operations, so the apply id
// alone no longer proves the response addresses this dispatch's operation — the
// data plane echoes the operation key it resolved, and the control plane
// re-derives the same key from the request it sent (both planes share the
// derivation helpers). A mismatch means the response's ids belong to some other
// operation of the shared apply — most often a data plane too old to attach
// sibling operations, which aliases every sibling dispatch to the apply's
// first operation and echoes no key at all — so the dispatch must fail closed
// instead of persisting another operation's remote ids.
func verifyDispatchOperationKeyEcho(plan *storage.Plan, req *ternv1.ApplyRequest, resp *ternv1.ApplyResponse) error {
	scope, err := deriveDispatchScope(plan, req)
	if err != nil {
		return fmt.Errorf("derive dispatch scope for operation key echo check: %w", err)
	}
	expectedKey, _, err := operationIdentityForDispatch(scope)
	if err != nil {
		return fmt.Errorf("derive expected operation key for echo check: %w", err)
	}
	if resp.OperationKey != expectedKey {
		return fmt.Errorf("remote apply %q echoed operation key %q, expected %q: the response does not address this dispatch's operation (data plane may predate sibling-operation attach)", resp.ApplyId, resp.OperationKey, expectedKey)
	}
	return nil
}

// persistRemoteApplyID stores the remote Tern apply id returned by dispatch.
// Single-operation drives keep it on the parent applies.external_id (mutated in
// place so the caller's Applies().Update persists it). Multi-operation drives
// write it to the claimed operation's external_id and never touch the parent
// external_id, refusing to overwrite a different existing id so one deployment
// can't clobber another deployment's remote apply id. Because deployment-keyed
// dispatch attaches every sibling operation into the deployment's one
// data-plane apply, all operations of a deployment must record the same remote
// apply id — an id that disagrees with the deployment's recorded id is refused
// fail-closed rather than giving one deployment two remote applies.
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
	currentRemoteID := current.RemoteApplyID()
	if currentRemoteID != "" && currentRemoteID != remoteID {
		// The same fail-closed class the deployment guard below counts: storing
		// this id would correlate the operation — and so its deployment — to a
		// second remote apply.
		c.applyLogger(apply).ErrorContext(ctx, "dispatch returned a remote apply id that disagrees with the one this operation already recorded; refusing to overwrite it",
			append(apply.MutableLogAttrs(),
				"apply_operation_id", op.ID,
				"operation_deployment", current.Deployment,
				"operation_key", current.OperationKey,
				"recorded_remote_apply_id", currentRemoteID,
				"refused_remote_apply_id", remoteID)...)
		metrics.RecordRemoteApplyDeploymentIDConflict(ctx, apply.Database, apply.Environment, current.Deployment)
		return fmt.Errorf("apply_operation %d already has remote apply id %q; refusing to overwrite with %q", op.ID, currentRemoteID, remoteID)
	}
	if err := c.guardDeploymentRemoteApplyID(ctx, apply, current, remoteID); err != nil {
		return err
	}
	if remoteOperationID != "" && current.ExternalOperationID != "" && current.ExternalOperationID != remoteOperationID {
		return fmt.Errorf("apply_operation %d already has remote apply_operation id %q; refusing to overwrite with %q", op.ID, current.ExternalOperationID, remoteOperationID)
	}
	if err := c.storage.ApplyOperations().SaveExternalID(ctx, apply.ID, op.ID, remoteID); err != nil {
		// The store re-verifies the deployment invariant under row locks inside
		// the writing transaction, so a sibling dispatch that persisted between
		// the guard's read above and this write still cannot give the deployment
		// a second remote apply — it is refused here instead.
		if errors.Is(err, storage.ErrRemoteApplyDeploymentIDConflict) {
			c.applyLogger(apply).ErrorContext(ctx, "a concurrent sibling dispatch recorded a different remote apply id for the deployment; refusing to give one deployment two remote applies",
				append(apply.MutableLogAttrs(),
					"apply_operation_id", op.ID,
					"operation_deployment", current.Deployment,
					"operation_key", current.OperationKey,
					"refused_remote_apply_id", remoteID,
					"error", err)...)
			metrics.RecordRemoteApplyDeploymentIDConflict(ctx, apply.Database, apply.Environment, current.Deployment)
		}
		return fmt.Errorf("store remote apply id for apply_operation %d: %w", op.ID, err)
	}
	op.ExternalID = remoteID
	if remoteOperationID != "" {
		if err := c.storage.ApplyOperations().SaveExternalOperationID(ctx, op.ID, remoteOperationID); err != nil {
			return fmt.Errorf("store remote apply_operation id for apply_operation %d: %w", op.ID, err)
		}
		op.ExternalOperationID = remoteOperationID
	}
	c.applyLogger(apply).InfoContext(ctx, "stored remote gRPC apply identifiers for operation",
		"apply_operation_id", op.ID,
		"operation_deployment", op.Deployment,
		"operation_key", op.OperationKey,
		"operation_kind", op.OperationKind,
		"external_id", remoteID,
		"external_operation_id", remoteOperationID)
	return nil
}

// guardDeploymentRemoteApplyID refuses to record a remote apply id that would
// give the operation's deployment a second data-plane apply. It resolves the
// remote apply id the deployment's sibling operations already recorded and
// fails closed on any disagreement — either among the siblings themselves
// (the planes diverged before this dispatch) or between the siblings and the
// id this dispatch returned. Sibling deployments of the same apply are
// exempt: they own their own remote applies.
//
// This read is unlocked, so it exists for precise triage logging on the
// common divergence shapes; the authoritative check is the store's, which
// re-verifies the same invariant under row locks inside the writing
// transaction so concurrent sibling persists serialize.
func (c *GRPCClient) guardDeploymentRemoteApplyID(ctx context.Context, apply *storage.Apply, current *storage.ApplyOperation, remoteID string) error {
	siblings, err := c.storage.ApplyOperations().ListByApply(ctx, apply.ID)
	if err != nil {
		return fmt.Errorf("list operations of apply %s before storing remote apply id for deployment %q: %w", apply.ApplyIdentifier, current.Deployment, err)
	}
	peers := make([]*storage.ApplyOperation, 0, len(siblings))
	for _, sib := range siblings {
		if sib.ID == current.ID {
			continue
		}
		peers = append(peers, sib)
	}
	sharedID, err := storage.DeploymentRemoteApplyID(peers, current.Deployment)
	if err != nil {
		c.applyLogger(apply).ErrorContext(ctx, "deployment's operations already record more than one remote apply id; refusing to store another until the planes agree",
			append(apply.MutableLogAttrs(),
				"apply_operation_id", current.ID,
				"operation_deployment", current.Deployment,
				"operation_key", current.OperationKey,
				"refused_remote_apply_id", remoteID,
				"error", err)...)
		metrics.RecordRemoteApplyDeploymentIDConflict(ctx, apply.Database, apply.Environment, current.Deployment)
		return fmt.Errorf("deployment %q of apply %s already correlates to more than one remote apply; refusing to store %q for apply_operation %d until the planes agree: %w", current.Deployment, apply.ApplyIdentifier, remoteID, current.ID, err)
	}
	if sharedID != "" && sharedID != remoteID {
		c.applyLogger(apply).ErrorContext(ctx, "dispatch returned a remote apply id that disagrees with the deployment's recorded remote apply; refusing to give one deployment two remote applies",
			append(apply.MutableLogAttrs(),
				"apply_operation_id", current.ID,
				"operation_deployment", current.Deployment,
				"operation_key", current.OperationKey,
				"deployment_remote_apply_id", sharedID,
				"refused_remote_apply_id", remoteID)...)
		metrics.RecordRemoteApplyDeploymentIDConflict(ctx, apply.Database, apply.Environment, current.Deployment)
		return fmt.Errorf("deployment %q of apply %s already correlates to remote apply %q; refusing to record a second remote apply %q for apply_operation %d", current.Deployment, apply.ApplyIdentifier, sharedID, remoteID, current.ID)
	}
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
	logger := c.applyLogger(apply)
	blob, err := PSDisplayMetadataStorageBlob(md)
	if err != nil {
		logger.Warn("comment may omit engine display metadata: failed to encode remote display metadata",
			"error", err)
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
		logger.Warn("comment may omit engine display metadata: could not load apply_operation to preserve resume context",
			"error", err)
		return lastBlob
	}
	if err := c.storage.ApplyOperations().SaveEngineResumeState(ctx, op.ID, &storage.EngineResumeState{
		ApplyOperationID: op.ID,
		MigrationContext: op.EngineResumeContext,
		Metadata:         blob,
	}); err != nil {
		logger.Warn("comment may omit engine display metadata: failed to persist to control-plane operation",
			"apply_operation_id", op.ID, "error", err)
		return lastBlob
	}
	return blob
}

// mirrorRemoteControlRejections records, on the control plane, the control
// requests the data plane accepted and then failed. Accepting a control RPC
// only means the request was queued: the engine call happens later on the data
// plane's own driver tick, and until it is mirrored here a rejection lives
// solely in the data plane's storage and logs — the operator who issued the
// command is told it succeeded and never learns the effect did not land.
//
// The data plane reports the same settled request on every poll until the
// operator retries the operation, so a mirror that fails is retried on the next
// tick rather than aborting the drive, and a mirror that succeeds is surfaced
// exactly once (RecordRemoteFailure reports whether the stored row changed).
//
// remoteID is the remote identifier this drive addressed. The data plane's
// message names its own apply, which is meaningless to the operator reading the
// PR — and on an operation-scoped drive the parent apply's ExternalID is empty,
// so the remote identifier is only redactable when it is passed in here.
func (c *GRPCClient) mirrorRemoteControlRejections(ctx context.Context, apply *storage.Apply, remoteID string, settled []*ternv1.SettledControlRequest) {
	if c.storage == nil || apply == nil || len(settled) == 0 {
		return
	}
	logger := c.applyLogger(apply)
	controlStore := c.storage.ControlRequests()
	if controlStore == nil {
		logger.Warn("control request store is not available; remote control rejections will not reach the operator",
			apply.MutableLogAttrs()...)
		return
	}
	for _, entry := range settled {
		if entry == nil {
			continue
		}
		operation := storage.ControlOperation(entry.Operation)
		if operation.Retired() {
			// A data plane on a previous release reports its settled retired
			// requests on every poll until the apply finishes; there is nothing
			// left to mirror for an operation this release removed, and the
			// entry recurs for the life of the drive, so it logs at debug.
			logger.Debug("data plane reported a settled control request for a retired operation; nothing to mirror",
				append(apply.MutableLogAttrs(), "operation", entry.Operation, "status", entry.Status)...)
			continue
		}
		if !operation.Valid() {
			logger.Warn("data plane reported a settled control request for an unrecognized operation; it will not reach the operator",
				append(apply.MutableLogAttrs(), "operation", entry.Operation, "status", entry.Status)...)
			continue
		}
		if entry.Status == string(storage.ControlRequestCompleted) {
			c.retireMirroredControlRejection(ctx, apply, controlStore, operation)
			continue
		}
		if entry.Status != string(storage.ControlRequestFailed) {
			// Only a failure needs mirroring, and completion is handled above. A
			// newer data plane reporting some other terminal status would drop the
			// request here, so name it rather than skipping silently.
			logger.Warn("data plane reported a settled control request in an unrecognized status; it will not reach the operator",
				append(apply.MutableLogAttrs(),
					"operation", entry.Operation,
					"status", entry.Status,
					"settled_at", entry.SettledAt)...)
			continue
		}
		message := remoteControlRejectionMessage(entry)
		changed, err := controlStore.RecordRemoteFailure(ctx, &storage.ApplyControlRequest{
			ApplyID:      apply.ID,
			Operation:    operation,
			ErrorMessage: apply.OperatorFacingMessage(message, remoteID),
			RequestedBy:  entry.RequestedBy,
		})
		if err != nil {
			logger.Warn("failed to record remote control rejection; the operator will not see it until a later poll mirrors it",
				append(apply.MutableLogAttrs(),
					"operation", entry.Operation,
					"settled_at", entry.SettledAt,
					"error", err)...)
			continue
		}
		if !changed {
			continue
		}
		logger.Warn("data plane rejected an accepted control command",
			append(apply.MutableLogAttrs(),
				"operation", entry.Operation,
				"requested_by", entry.RequestedBy,
				"settled_at", entry.SettledAt,
				"error_message", message)...)
		metrics.RecordRemoteControlRequestRejected(ctx, entry.Operation, apply.Engine, apply.Database, apply.Deployment, apply.Environment)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, storage.LogEventError,
			fmt.Sprintf("%s was accepted but not applied: %s%s", remoteControlOperationLabel(operation), message,
				callerApplyLogSuffix(entry.RequestedBy)), "", "")
	}
}

// retireMirroredControlRejection clears a rejection this plane mirrored once the
// data plane reports the same operation succeeded. The mirrored row is this
// plane's only record of that failure, so nothing else resets it: without this
// the operator re-issues the command, it works, and the PR keeps warning that
// it did not.
func (c *GRPCClient) retireMirroredControlRejection(
	ctx context.Context,
	apply *storage.Apply,
	controlStore storage.ControlRequestStore,
	operation storage.ControlOperation,
) {
	changed, err := controlStore.ClearRemoteFailure(ctx, apply.ID, operation)
	if err != nil {
		c.applyLogger(apply).Warn("failed to clear a mirrored control rejection the data plane has since completed; the notice stays until a later poll clears it",
			append(apply.MutableLogAttrs(), "operation", string(operation), "error", err)...)
		return
	}
	if !changed {
		return
	}
	c.applyLogger(apply).Info("data plane completed a control command it had previously rejected; clearing the mirrored rejection",
		append(apply.MutableLogAttrs(), "operation", string(operation))...)
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStateTransition,
		fmt.Sprintf("%s succeeded on a later attempt; the earlier rejection no longer applies", remoteControlOperationLabel(operation)), "", "")
}

// remoteControlRejectionMessage renders the reason the data plane gave, falling
// back to a fixed line so a rejection is never recorded without one.
func remoteControlRejectionMessage(entry *ternv1.SettledControlRequest) string {
	if entry.ErrorMessage != "" {
		return entry.ErrorMessage
	}
	return "the data plane reported no reason; see the data plane logs"
}

// remoteControlOperationLabel renders a control operation for an operator-facing
// apply log line.
func remoteControlOperationLabel(operation storage.ControlOperation) string {
	if operation == "" {
		return "Control command"
	}
	return strings.ToUpper(string(operation)[:1]) + strings.ReplaceAll(string(operation)[1:], "_", "-")
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
		scope.tasklessOperation = true
		return c.dispatchRemoteGroupFinalizer(ctx, apply, scope)
	}
	tasks, err := c.loadApplyTasks(ctx, apply, scope)
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		// A whole-deployment work operation for a VSchema-only plan carries no
		// tasks by design — the change is the namespace VSchema, which is never
		// modelled as a task row. Dispatch it as a VSchema-only apply, which the
		// data plane applies via its own task-less VSchema-only path, mirroring
		// LocalClient.ResumeApplyOperation.
		plan, err := c.storage.Plans().GetByID(ctx, apply.PlanID)
		if err != nil {
			return fmt.Errorf("load plan %d for task-less apply_operation %d (apply %s): %w", apply.PlanID, applyOperationID, apply.ApplyIdentifier, err)
		}
		// A missing plan row is its own cause, separate from a claim that resolved
		// to the wrong operation, so name it rather than reporting a stale claim.
		if plan == nil {
			return fmt.Errorf("plan %d for task-less apply_operation %d (apply %s): %w", apply.PlanID, applyOperationID, apply.ApplyIdentifier, ErrPlanMissingForApplyOperation)
		}
		// Fail closed before any dispatch or state mutation on every other
		// task-less work shape: it is an invalid or stale claim. The shared resume
		// path would otherwise mark the whole parent apply failed
		// (dispatchPendingApply and the remote-failure sites set applies.state
		// regardless of scope), which is wrong when only this operation's lookup
		// came back empty.
		if !scope.operation.IsTasklessVSchemaOnlyWork(plan) {
			return fmt.Errorf("apply_operation %d (apply %s): %w", applyOperationID, apply.ApplyIdentifier, ErrNoTasksForApplyOperation)
		}
		scope.tasklessOperation = true
		return c.dispatchRemoteVSchemaOnly(ctx, apply, scope, plan, plan.VSchemaNamespaces(), tasklessWorkDispatchKind)
	}
	return c.resumeApply(ctx, apply, scope)
}

// The two task-less operation shapes a remote drive dispatches as a VSchema-only
// apply, named for the operator-facing error text each produces.
const (
	groupFinalizerDispatchKind = "group_finalizer"
	tasklessWorkDispatchKind   = "VSchema-only"
)

// dispatchRemoteGroupFinalizer drives a task-less group_finalizer apply_operation
// over gRPC. It is the remote counterpart to LocalClient.driveGroupFinalizer:
// the control plane cannot run the engine, so it dispatches the operation's
// VSchema scope to the data plane, which applies it via its task-less
// VSchema-only path. A namespace-scoped finalizer (from a sharded fan-out)
// dispatches its one namespace; a deployment-scoped finalizer (a VSchema-only
// apply) dispatches every VSchema-changed namespace in the plan as one apply.
func (c *GRPCClient) dispatchRemoteGroupFinalizer(ctx context.Context, apply *storage.Apply, scope applyTaskScope) error {
	op := scope.operation
	namespace := namespaceFromFinalizerKey(op.OperationKey)
	if namespace == "" && op.OperationKey != finalizerDeploymentScopedKey {
		return fmt.Errorf("group_finalizer apply_operation %d (apply %s): malformed operation key %q", op.ID, apply.ApplyIdentifier, op.OperationKey)
	}
	plan, err := c.storage.Plans().GetByID(ctx, apply.PlanID)
	if err != nil {
		return fmt.Errorf("load plan %d for group_finalizer apply_operation %d (apply %s): %w", apply.PlanID, op.ID, apply.ApplyIdentifier, err)
	}
	if plan == nil {
		return fmt.Errorf("plan %d for group_finalizer apply_operation %d (apply %s): %w", apply.PlanID, op.ID, apply.ApplyIdentifier, ErrPlanMissingForApplyOperation)
	}
	// Fail closed if the operation's scope carries no VSchema artifact,
	// mirroring the local finalizer drive.
	if _, err := finalizerVSchemaChanges(plan, namespace); err != nil {
		return fmt.Errorf("group_finalizer apply_operation %d (apply %s): %w", op.ID, apply.ApplyIdentifier, err)
	}
	namespaces := []string{namespace}
	if namespace == "" {
		namespaces = plan.VSchemaNamespaces()
	}
	return c.dispatchRemoteVSchemaOnly(ctx, apply, scope, plan, namespaces, groupFinalizerDispatchKind)
}

// dispatchRemoteVSchemaOnly dispatches the given namespaces' VSchema to the data
// plane as a VSchema-only apply (no DDL, no target shards), records the remote
// apply id on the operation, and polls to completion. Carrying both a VSchema
// change and the plan's schema files lets the remote drive it whether it has the
// plan locally or materializes it from the dispatch.
//
// It serves both task-less operation shapes: a group_finalizer, which applies
// one namespace's VSchema once its sibling shard work completes, and a
// whole-deployment work operation for a VSchema-only plan, which applies every
// VSchema-changing namespace in one dispatch because an
// externally-authoritative engine deploys the whole branch as one operation.
// The caller has already established that the operation is one of those shapes
// and that the plan carries the VSchema; kind names the shape in error text.
func (c *GRPCClient) dispatchRemoteVSchemaOnly(ctx context.Context, apply *storage.Apply, scope applyTaskScope, plan *storage.Plan, namespaces []string, kind string) error {
	op := scope.operation
	if len(namespaces) == 0 {
		return fmt.Errorf("%s apply_operation %d (apply %s): plan %d carries no VSchema namespace to dispatch", kind, op.ID, apply.ApplyIdentifier, apply.PlanID)
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
		changes := make([]*ternv1.TableChange, 0, len(namespaces))
		for _, namespace := range namespaces {
			// Carry the namespace's persisted VSchema change-metadata so a
			// deployment materializing the plan from this dispatch runs the
			// same apply-time safety gates as one reading its own stored plan.
			var meta map[string]string
			if nsData := plan.Namespaces[namespace]; nsData != nil {
				meta = storage.VSchemaPlanMetadata(nsData.Metadata)
			}
			changes = append(changes, &ternv1.TableChange{
				Namespace:  namespace,
				TableName:  "VSchema: " + namespace,
				ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA,
				Metadata:   meta,
			})
		}
		req := &ternv1.ApplyRequest{
			PlanId:      plan.PlanIdentifier,
			Options:     options,
			Database:    apply.Database,
			Type:        apply.DatabaseType,
			DdlChanges:  changes,
			SchemaFiles: schemaFilesToProto(plan.SchemaFiles),
			Environment: apply.Environment,
			Target:      target,
			Caller:      apply.Caller,
			// No TargetShards: the VSchema is namespace-level, not per shard.
			IdempotencyKey:          remoteApplyIdempotencyKey(apply, scope),
			GenerationOperationKeys: scope.generationOperationKeys(),
		}
		resp, err := c.client.Apply(ctx, req)
		if err != nil {
			if isAmbiguousRemoteApplyDispatchError(err) {
				return fmt.Errorf("%s apply_operation %d (apply %s) has ambiguous remote dispatch outcome: %w", kind, op.ID, apply.ApplyIdentifier, err)
			}
			if markErr := c.markRemoteApplyFailed(ctx, apply, nil, err.Error(), isRetryableRemoteApplyError(err), scope); markErr != nil {
				return fmt.Errorf("mark %s apply_operation %d failed after remote apply error: %w", kind, op.ID, markErr)
			}
			return fmt.Errorf("dispatch %s apply_operation %d (apply %s): %w", kind, op.ID, apply.ApplyIdentifier, err)
		}
		if resp == nil || !resp.Accepted || resp.ApplyId == "" {
			holderApplyID := c.resolveConflictHolderApplyID(ctx, apply, resp.GetConflict())
			errMsg := remoteApplyRejectionMessage(resp, holderApplyID, fmt.Sprintf("remote %s apply was not accepted", kind))
			if markErr := c.markRemoteApplyFailed(ctx, apply, nil, errMsg, false, scope); markErr != nil {
				return fmt.Errorf("mark %s apply_operation %d failed: %w", kind, op.ID, markErr)
			}
			return fmt.Errorf("dispatch %s apply_operation %d (apply %s): %s", kind, op.ID, apply.ApplyIdentifier, errMsg)
		}
		if echoErr := verifyDispatchOperationKeyEcho(plan, req, resp); echoErr != nil {
			c.applyLogger(apply).ErrorContext(ctx, "remote dispatch response does not address this operation; refusing its remote ids and failing the operation closed",
				append(apply.MutableLogAttrs(),
					"apply_operation_id", op.ID,
					"operation_deployment", op.Deployment,
					"operation_key", op.OperationKey,
					"kind", kind,
					"remote_apply_id", resp.ApplyId,
					"error", echoErr)...)
			metrics.RecordRemoteApplyKeyEchoMismatch(ctx, apply.Database, apply.Environment)
			if markErr := c.markRemoteApplyFailed(ctx, apply, nil, echoErr.Error(), false, scope); markErr != nil {
				return fmt.Errorf("mark %s apply_operation %d failed after operation key echo mismatch: %w", kind, op.ID, markErr)
			}
			return fmt.Errorf("dispatch %s apply_operation %d (apply %s): %w", kind, op.ID, apply.ApplyIdentifier, echoErr)
		}
		if err := c.persistRemoteApplyID(ctx, apply, scope, resp.ApplyId, resp.ApplyOperationId); err != nil {
			return fmt.Errorf("store remote apply id for %s apply_operation %d: %w", kind, op.ID, err)
		}
	}

	started, err := c.startStoppedTasklessRemoteApply(ctx, apply, scope, kind)
	if err != nil {
		return err
	}
	return c.pollForCompletion(ctx, apply, started, scope, false)
}

// startStoppedTasklessRemoteApply resumes a task-less operation the operator
// stopped and has since asked to start again, reporting whether the remote was
// started. A task-less operation has no task rows, so the drive owns its state
// in both directions: nothing else asks the data plane to resume it. Without
// this the poll below reads the same stopped state on every claim, the start
// request stays pending, the operation is re-claimed forever, and only a cancel
// frees the target.
func (c *GRPCClient) startStoppedTasklessRemoteApply(ctx context.Context, apply *storage.Apply, scope applyTaskScope, kind string) (bool, error) {
	op := scope.operation
	if op == nil || !state.IsState(op.State, state.Apply.Stopped) {
		return false, nil
	}
	startReq, err := pendingStartControlRequest(ctx, c.storage, apply)
	if err != nil {
		return false, fmt.Errorf("check pending start for stopped %s apply_operation %d (apply %s): %w", kind, op.ID, apply.ApplyIdentifier, err)
	}
	if startReq == nil {
		// The operator has not asked for it back. Polling reports the stored
		// stopped state without touching the target.
		c.applyLogger(apply).DebugContext(ctx, "stopped task-less operation has no pending start request; leaving it stopped",
			append(apply.MutableLogAttrs(), "apply_operation_id", op.ID, "kind", kind)...)
		return false, nil
	}
	if deferred, err := c.completePendingStopBeforeRemoteStart(ctx, apply, scope); err != nil || deferred {
		return false, err
	}

	remoteID := scope.remoteApplyID(apply)
	logger := c.applyLogger(apply)
	// Only start what the data plane still holds stopped. A start racing the
	// drive that recorded the stop would otherwise restart an apply the data
	// plane has already resumed, or one it has since failed.
	resp, err := c.client.Progress(ctx, &ternv1.ProgressRequest{ApplyId: remoteID, Environment: apply.Environment})
	if err != nil {
		return false, fmt.Errorf("check stopped %s apply_operation %d (remote apply %s) before start: %w", kind, op.ID, remoteID, err)
	}
	remoteState := ProtoStateToStorage(resp.State)
	if !state.IsState(remoteState, state.Apply.Stopped) {
		logger.InfoContext(ctx, "data plane no longer holds the task-less operation stopped; polling its current state instead of starting it",
			append(apply.MutableLogAttrs(),
				"apply_operation_id", op.ID, "kind", kind,
				"remote_apply_id", remoteID, "remote_state", resp.State.String())...)
		return false, nil
	}
	if _, err := c.client.Start(ctx, &ternv1.StartRequest{ApplyId: remoteID, Environment: apply.Environment, Caller: startReq.RequestedBy}); err != nil {
		message := fmt.Sprintf("Remote start failed for the %s operation; it stays stopped and the command can be re-issued", kind)
		logger.WarnContext(ctx, "remote start failed for a stopped task-less operation; leaving it stopped for operator retry",
			append(apply.MutableLogAttrs(),
				"apply_operation_id", op.ID, "kind", kind,
				"remote_apply_id", remoteID, "error", err)...)
		c.logApplyWarning(ctx, apply, message)
		if failErr := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStart, message, remoteID); failErr != nil {
			return false, failErr
		}
		return false, fmt.Errorf("start stopped %s apply_operation %d (remote apply %s): %w", kind, op.ID, remoteID, err)
	}
	// The operation row still reads stopped, and it is the only thing the
	// operator's projection can derive the parent from. Move it back to running
	// so a resumed operation does not keep presenting as stopped.
	if err := c.storage.ApplyOperations().UpdateState(ctx, op.ID, state.Apply.Running); err != nil {
		return false, fmt.Errorf("update resumed %s apply_operation %d to running: %w", kind, op.ID, err)
	}
	op.State = state.Apply.Running
	logger.InfoContext(ctx, "started a stopped task-less operation on the operator's request",
		append(apply.MutableLogAttrs(),
			"apply_operation_id", op.ID, "kind", kind, "remote_apply_id", remoteID)...)
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStartRequested,
		fmt.Sprintf("Remote %s operation started by operator", kind), "", "")
	return true, nil
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

// operationCutoverCaller names the operator whose cutover this ordered drive is
// carrying out. The deployment-ordered claim, not the durable request, is what
// routes the swap here, so the requester has to be read back from the
// apply-level cutover request the operator queued. Attribution must never cost
// the swap: an unreadable or already-settled request yields an empty caller,
// which the data plane records as the forwarding path.
func (c *GRPCClient) operationCutoverCaller(ctx context.Context, apply *storage.Apply) string {
	controlReq, err := pendingControlRequest(ctx, c.storage, apply, storage.ControlOperationCutover)
	if err != nil {
		c.applyLogger(apply).WarnContext(ctx, "could not read the cutover request's operator; the data plane will record the forwarding path instead",
			append(apply.MutableLogAttrs(), "error", err)...)
		return ""
	}
	if controlReq == nil {
		// Expected whenever the request settled before this operation's claim
		// reached the swap — a sibling operation's drive resolving it, or a
		// recovery claim re-driving a cutover already sent.
		c.applyLogger(apply).DebugContext(ctx, "no pending cutover request names an operator; the data plane will record the forwarding path",
			apply.MutableLogAttrs()...)
		return ""
	}
	return controlReq.RequestedBy
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
	remoteState := remoteProgressApplyState(resp.State, resp.Tables)
	if remoteState == "" {
		return false, fmt.Errorf("preflight remote cutover for apply_operation %d (apply %s): unmapped remote state %s", scope.applyOperationID, apply.ApplyIdentifier, remoteApplyStateDescription(resp.State))
	}
	// Remote already terminal: reconcile from this poll and stop. Do not re-send
	// Cutover. A retryable pause is not terminal: it falls through to the
	// not-parked branch below and the operator retries once the data plane's
	// own recovery brings the copy back to the barrier.
	if remoteProgressIsTerminal(resp.State, resp.Tables) {
		now := time.Now()
		apply.State = remoteState
		apply.ErrorMessage = remoteProgressErrorMessage(apply.State, resp.ErrorMessage, apply.ErrorMessage)
		if apply.StartedAt == nil {
			apply.StartedAt = &now
		}
		return false, c.reconcileTerminalRemoteProgress(ctx, apply, resp.Tables, now, scope)
	}
	switch {
	case state.IsState(remoteState, state.Apply.CuttingOver, state.Apply.RevertWindow, state.Apply.Reverting, state.Apply.SkippingRevert):
		// A prior driver already started the swap, or the remote is past cutover —
		// holding the revert window open, unwinding a revert, or finalizing
		// skip-revert. Cutover must never be sent to any of these: the swap
		// already happened and the post-cutover phase owns the outcome. Do not
		// re-send Cutover; poll to terminal.
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
		Caller:      c.operationCutoverCaller(ctx, apply),
	})
	if err != nil {
		return false, fmt.Errorf("request remote cutover for apply_operation %d (apply %s) remote %s: %w", scope.applyOperationID, apply.ApplyIdentifier, remoteID, err)
	}
	if cutoverResp == nil {
		return false, fmt.Errorf("request remote cutover for apply_operation %d (apply %s) remote %s: the data plane returned neither a response nor an error", scope.applyOperationID, apply.ApplyIdentifier, remoteID)
	}
	if !cutoverResp.Accepted {
		return false, fmt.Errorf("request remote cutover for apply_operation %d (apply %s) remote %s: %s", scope.applyOperationID, apply.ApplyIdentifier, remoteID, controlRefusalMessage(storage.ControlOperationCutover, cutoverResp.ErrorMessage))
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
	logger := c.applyLogger(apply)
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
		// A failed apply is never claimed again, so a stop or cancel left
		// pending here would stay unanswered forever and a re-issued command
		// would be refused as already requested. Fail the requests with the
		// same ambiguity message the apply carries. An operation-only drive
		// leaves the shared apply-level requests pending: sibling deployments
		// still need to observe them, and the operator projection settles them
		// once the parent apply derives terminal.
		if !scope.suppressesDirectParentApplyWrites() {
			for _, operation := range []storage.ControlOperation{storage.ControlOperationStop, storage.ControlOperationCancel} {
				if err := failPendingControlRequests(ctx, c.storage, apply, operation, errMsg); err != nil {
					return fmt.Errorf("%s; fail pending %s control request: %w", errMsg, operation, err)
				}
			}
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
				logger.Warn("remote gRPC stopped-state check returned no active schema change; operator will not request remote start",
					apply.MutableLogAttrs()...)
				return c.failMissingStoppedRemoteApply(ctx, apply, message, nil, scope)
			}
			remoteState := ProtoStateToStorage(resp.State)
			if remoteState == "" {
				message := fmt.Sprintf("Remote stopped-state check returned unmapped state %s; operator will not request remote start", remoteApplyStateDescription(resp.State))
				logger.Warn("remote gRPC stopped-state check returned unmapped state; operator will not request remote start",
					append(apply.MutableLogAttrs(),
						"remote_state", resp.State.String(),
						"remote_state_number", int32(resp.State))...)
				c.logApplyWarning(ctx, apply, message)
				return fmt.Errorf("check stopped gRPC apply %s before start: unmapped remote state %s", apply.ApplyIdentifier, remoteApplyStateDescription(resp.State))
			}
			if remoteApplyPausedForDataPlaneRetry(resp.State, resp.Tables) {
				// The data plane is retrying the apply on its own; it is
				// neither stopped (startable) nor settled (reconcilable).
				if startRequested {
					// Settle the start request durably instead of leaving it
					// pending: repeating this check every claim would write a
					// warning per cycle, and the answer will not change — the
					// data plane resumes without a start.
					message := "The schema change is already retrying automatically; there is nothing to start"
					logger.Info("remote gRPC stopped-state check found a data-plane retryable pause; rejecting the start request as unneeded",
						apply.MutableLogAttrs()...)
					if failErr := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStart, message, remoteID); failErr != nil {
						return failErr
					}
				}
				if state.IsState(apply.State, state.Apply.Stopped) {
					// A stored stop with a remote that is self-retrying is
					// contradictory; exit without adopting any state and let a
					// later claim re-check once the data plane settles.
					message := "Remote apply is paused for a data-plane retry while the stored apply reads stopped; operator will re-check on a later claim"
					logger.Warn("remote gRPC stopped-state check found a data-plane retryable pause on a stopped stored apply; operator will re-check on a later claim",
						apply.MutableLogAttrs()...)
					c.logApplyWarning(ctx, apply, message)
					return fmt.Errorf("check stopped gRPC apply %s before start: remote apply is paused for a data-plane retry", apply.ApplyIdentifier)
				}
				// The stored apply is active: hold the drive and poll through
				// the pause like any other in-flight snapshot.
				return c.pollForCompletion(ctx, apply, false, scope,
					shouldReleaseAtCutoverBarrier(apply, scope.multiOperation, scope.operation))
			}
			apply.State = remoteState
			apply.ErrorMessage = remoteProgressErrorMessage(apply.State, resp.ErrorMessage, apply.ErrorMessage)
			// The pause guard above already returned for a retryable pause, so
			// a terminal proto state here is a settled verdict — no table
			// refinement left to apply.
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
			logger.Warn("remote gRPC stopped-state check failed; operator will not request remote start",
				append(apply.MutableLogAttrs(), "error", err)...)
			c.logApplyWarning(ctx, apply, message)
			return fmt.Errorf("check stopped gRPC apply %s before start: %w", apply.ApplyIdentifier, err)
		}

		// Only call Start if Tern confirms the apply is actually stopped.
		if state.IsState(apply.State, state.Apply.Stopped) {
			// A stopped apply with no pending start was claimed only to
			// deliver a pending control request (see ClaimApplyByID), and the
			// remote confirming stopped means that request has not taken
			// effect there yet. Exit without starting — a remote start here
			// would resume a copy the operator asked to discard. A later
			// claim re-checks once the lease goes stale or the request is
			// re-issued.
			if !startRequested {
				logger.Info("stopped gRPC apply has no pending start request; drive exits without requesting a remote start",
					apply.MutableLogAttrs()...)
				return nil
			}
			if deferred, err := c.completePendingStopBeforeRemoteStart(ctx, apply, scope); err != nil || deferred {
				return err
			}
			_, err := c.client.Start(ctx, &ternv1.StartRequest{
				ApplyId:     remoteID,
				Environment: apply.Environment,
			})
			if err != nil {
				message := fmt.Sprintf("remote start failed for remote apply %s: %v", remoteID, err)
				logger.Warn("remote gRPC start failed; storing stopped state for operator retry",
					append(apply.MutableLogAttrs(), "remote_apply_id", remoteID, "error", err)...)
				c.logApplyWarning(ctx, apply, message)
				apply.State = state.Apply.Stopped
				apply.ErrorMessage = message
				if reconcileErr := c.reconcileTerminalRemoteProgress(ctx, apply, resp.Tables, time.Now(), scope); reconcileErr != nil {
					return fmt.Errorf("persist stopped gRPC apply %s after start failure: %w", apply.ApplyIdentifier, reconcileErr)
				}
				if startRequested {
					if failErr := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStart, message, remoteID); failErr != nil {
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
	logger := c.applyLogger(apply)
	if scope.suppressesDirectParentApplyWrites() {
		logOperationDriveLeavesParentStop(logger, apply, scope)
		logger.InfoContext(ctx, "operation-only drive deferring remote start until apply-level stop resolves",
			append(apply.MutableLogAttrs(),
				"apply_operation_id", scope.applyOperationID,
				"remote_apply_id", scope.remoteApplyID(apply),
				"requested_by", controlRequestCaller(controlReq))...)
		return true, nil
	}
	if err := completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStop); err != nil {
		return false, fmt.Errorf("complete pending stop before starting stopped gRPC apply %s: %w", apply.ApplyIdentifier, err)
	}
	logger.InfoContext(ctx, "completed pending gRPC stop request before starting stopped remote apply",
		append(apply.MutableLogAttrs(), "requested_by", controlRequestCaller(controlReq))...)
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventStopRequested,
		fmt.Sprintf("Pending remote stop request completed before start%s", callerApplyLogSuffix(controlRequestCaller(controlReq))), "", "")
	return false, nil
}

func pendingStartControlRequest(ctx context.Context, store storage.Storage, apply *storage.Apply) (*storage.ApplyControlRequest, error) {
	return pendingControlRequest(ctx, store, apply, storage.ControlOperationStart)
}

// waitForPendingStopBeforeStart blocks a pending start until the apply-level
// stop request resolves. It returns deferred=true when the caller must abandon
// the start for this drive: an operation-only drive never completes the parent
// stop (the operator projection owns it), so it stops its own operation's
// remote work and leaves both the stop and start pending for the operator.
func (c *GRPCClient) waitForPendingStopBeforeStart(ctx context.Context, apply *storage.Apply, scope applyTaskScope, startControlReq *storage.ApplyControlRequest) (bool, error) {
	logger := c.applyLogger(apply)
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
				logOperationDriveLeavesParentStop(logger, apply, scope)
			}
			logger.InfoContext(ctx, "operation-only drive deferring pending gRPC start until apply-level stop resolves",
				append(apply.MutableLogAttrs(),
					"apply_operation_id", scope.applyOperationID,
					"remote_apply_id", scope.remoteApplyID(apply),
					"requested_by", controlRequestCaller(startControlReq),
					"stop_requested_by", controlRequestCaller(stillPending))...)
			return true, nil
		}
		if !loggedWait {
			logger.InfoContext(ctx, "pending gRPC start request is waiting for pending stop request to finish",
				append(apply.MutableLogAttrs(),
					"requested_by", controlRequestCaller(startControlReq),
					"stop_requested_by", controlRequestCaller(stopReq))...)
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
		c.applyLogger(apply).InfoContext(ctx, "pending gRPC start request is waiting for pending stop request to finish",
			append(apply.MutableLogAttrs(),
				"requested_by", controlRequestCaller(controlReq),
				"stop_requested_by", controlRequestCaller(stopReq))...)
		return nil
	}
	if !state.IsState(apply.State, state.Apply.WaitingForDeploy) {
		return nil
	}
	remoteID := scope.remoteApplyID(apply)
	if remoteID == "" {
		message := fmt.Sprintf("gRPC apply %s is waiting for deploy without a remote apply id; start dispatch state is ambiguous", apply.ApplyIdentifier)
		if err := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStart, message, remoteID); err != nil {
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
		c.applyLogger(apply).WarnContext(ctx, "remote gRPC deferred deploy start failed; storing start request failure",
			append(apply.MutableLogAttrs(),
				"remote_apply_id", remoteID,
				"error", err)...)
		if failErr := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStart, message, remoteID); failErr != nil {
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

// undispatchedControlRequestSettlesLocally reports whether a pending stop or
// cancel that addresses no remote apply id can be satisfied in control-plane
// storage alone. Two shapes qualify. When the dispatch path would treat this
// drive as a first dispatch, nothing remote was ever created. When the
// dispatch state is stopped, the same fact is proven from the other side: a
// remote apply id is persisted before an apply or operation can be recorded
// stopped and is never cleared, so a stopped one without an id was stopped
// before dispatch and has no remote work to address.
func undispatchedControlRequestSettlesLocally(apply *storage.Apply, scope applyTaskScope) bool {
	if shouldDispatchQueuedRemoteApply(apply, scope) {
		return true
	}
	return scope.remoteApplyID(apply) == "" && state.IsState(scope.dispatchState(apply), state.Apply.Stopped)
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

	req := &ternv1.ApplyRequest{
		PlanId:                  plan.PlanIdentifier,
		Options:                 options,
		Database:                apply.Database,
		Type:                    apply.DatabaseType,
		DdlChanges:              tasksToProtoTableChanges(tasks),
		SchemaFiles:             schemaFilesToProto(plan.SchemaFiles),
		Environment:             apply.Environment,
		Target:                  target,
		Caller:                  apply.Caller,
		TargetShards:            targetShards,
		IdempotencyKey:          remoteApplyIdempotencyKey(apply, scope),
		GenerationOperationKeys: scope.generationOperationKeys(),
	}
	resp, err := c.client.Apply(ctx, req)
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
		holderApplyID := c.resolveConflictHolderApplyID(ctx, apply, resp.GetConflict())
		errMsg := remoteApplyRejectionMessage(resp, holderApplyID, "remote apply was not accepted")
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
	if echoErr := verifyDispatchOperationKeyEcho(plan, req, resp); echoErr != nil {
		c.applyLogger(apply).ErrorContext(ctx, "remote dispatch response does not address this operation; refusing its remote ids and failing the apply closed",
			append(apply.MutableLogAttrs(),
				"remote_apply_id", resp.ApplyId,
				"error", echoErr)...)
		metrics.RecordRemoteApplyKeyEchoMismatch(ctx, apply.Database, apply.Environment)
		if markErr := c.markRemoteApplyFailed(ctx, apply, tasks, echoErr.Error(), false, scope); markErr != nil {
			return fmt.Errorf("mark queued gRPC apply %s failed after operation key echo mismatch: %w", apply.ApplyIdentifier, markErr)
		}
		return fmt.Errorf("apply queued gRPC apply %s: %w", apply.ApplyIdentifier, echoErr)
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

// logSkippedRemoteApplyTransition expects a logger already bound to the remote
// apply's identity attributes, so it appends only the transition-specific
// fields.
func logSkippedRemoteApplyTransition(ctx context.Context, logger *slog.Logger, operation string, remoteApply, storedApply *storage.Apply, status storedApplyTransitionStatus, err error) {
	fields := []any{
		"operation", operation,
		"external_id", remoteApply.ExternalID,
		"reason", status,
	}
	if storedApply != nil {
		fields = append(fields, "stored_state", storedApply.State)
	}

	switch status {
	case storedApplyTransitionReloadFailed:
		fields = append(fields, "error", err)
		logger.ErrorContext(ctx, "skipping remote gRPC apply state transition", fields...)
	case storedApplyTransitionMissing:
		logger.WarnContext(ctx, "skipping remote gRPC apply state transition", fields...)
	case storedApplyTransitionAlreadyTerminal:
		logger.DebugContext(ctx, "skipping remote gRPC apply state transition", fields...)
	default:
		logger.WarnContext(ctx, "skipping remote gRPC apply state transition", fields...)
	}
}

// completeApplyStartRequestForScope completes the apply-level start control
// request unless the drive owns only its operation. The start request lives on
// the parent and is shared across sibling operations; one operation starting
// must not complete it, or stopped siblings would become unclaimable before they
// resume. The rollout projection completes it once the aggregate settles.
func (c *GRPCClient) completeApplyStartRequestForScope(ctx context.Context, apply *storage.Apply, scope applyTaskScope) error {
	if scope.usesOperationRemoteResume() {
		c.applyLogger(apply).DebugContext(ctx, "skipping apply-level start request completion during operation-scoped drive; parent start request is owned by the rollout projection",
			"apply_operation_id", scope.applyOperationID,
			"operation_deployment", scope.operation.Deployment)
		return nil
	}
	return completePendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStart)
}

// persistParentApply writes the parent applies row unless the drive owns only
// its operation, in which case the parent state is owned by the operator's
// projection CAS and the direct write is skipped. It reports whether the write
// happened so callers can gate parent-level side effects (state-transition logs,
// active-apply metrics) on an actual persist.
func (c *GRPCClient) persistParentApply(ctx context.Context, apply *storage.Apply, scope applyTaskScope, action string) (bool, error) {
	if scope.suppressesDirectParentApplyWrites() {
		c.applyLogger(apply).DebugContext(ctx, "skipping direct parent apply write during operation-scoped drive; parent state is owned by the rollout projection",
			"apply_operation_id", scope.applyOperationID,
			"operation_deployment", scope.operation.Deployment,
			"action", action)
		return false, nil
	}
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		return false, err
	}
	return true, nil
}

// heartbeatScopedDrive refreshes the lease keeping a long drive claimed. An
// operation-scoped drive heartbeats its own operation row (it holds no parent
// apply lease); every other drive heartbeats the parent applies row.
func (c *GRPCClient) heartbeatScopedDrive(ctx context.Context, apply *storage.Apply, scope applyTaskScope) error {
	if scope.suppressesDirectParentApplyWrites() {
		return c.storage.ApplyOperations().Heartbeat(ctx, scope.applyOperationID)
	}
	return c.storage.Applies().Heartbeat(ctx, apply.ID)
}

// driveEndingHeartbeatFailure classifies a failed drive-lease heartbeat write.
// It returns a non-nil error when the drive must end: either storage reported
// the lease definitively lost, or heartbeat failures have persisted since
// lastSuccess for the full lease staleness window, so a peer driver can
// already have reclaimed the stale row — that case wraps
// ErrApplyLeasePresumedLost so the operator records the displacement exactly
// once. A transient failure inside the window returns nil so the drive keeps
// polling and the heartbeat is retried on the next tick. The remote Tern keeps
// executing either way; the next claimant reattaches to it.
// The logger is expected to carry the apply's identity attributes already
// bound, so each line appends only the mutable snapshot.
func driveEndingHeartbeatFailure(logger *slog.Logger, apply *storage.Apply, hbErr error, lastSuccess time.Time) error {
	if errors.Is(hbErr, storage.ErrApplyLeaseLost) {
		logger.Warn("gRPC drive heartbeat lost the lease; current owner will stop driving and writing apply state",
			append(apply.MutableLogAttrs(), "error", hbErr)...)
		return fmt.Errorf("heartbeat gRPC apply %s: %w", apply.ApplyIdentifier, hbErr)
	}
	if time.Since(lastSuccess) >= storage.ApplyLeaseStaleAfter {
		logger.Warn("gRPC drive heartbeat has failed for the full lease staleness window; a peer driver can reclaim the work, so this owner will stop driving and writing apply state",
			append(apply.MutableLogAttrs(), "last_successful_heartbeat", lastSuccess, "error", hbErr)...)
		return fmt.Errorf("heartbeat gRPC apply %s: %w (heartbeats failing since %s): %w",
			apply.ApplyIdentifier, ErrApplyLeasePresumedLost, lastSuccess.UTC().Format(time.RFC3339), hbErr)
	}
	logger.Warn("gRPC drive heartbeat failed; will retry",
		append(apply.MutableLogAttrs(), "error", hbErr)...)
	return nil
}

func (c *GRPCClient) markRemoteApplyFailed(ctx context.Context, remoteApply *storage.Apply, storedTasks []*storage.Task, message string, retryable bool, scope applyTaskScope) error {
	return c.markRemoteApplyFailedWithOptions(ctx, remoteApply, storedTasks, message, retryable, false, scope)
}

func (c *GRPCClient) markStoppedRemoteApplyFailed(ctx context.Context, remoteApply *storage.Apply, message string, retryable bool, scope applyTaskScope) error {
	return c.markRemoteApplyFailedWithOptions(ctx, remoteApply, nil, message, retryable, true, scope)
}

func (c *GRPCClient) markRemoteApplyFailedWithOptions(ctx context.Context, remoteApply *storage.Apply, storedTasks []*storage.Task, message string, retryable, allowStoppedStoredApply bool, scope applyTaskScope) error {
	logger := c.applyLogger(remoteApply)
	storedApply, transitionStatus, err := c.reloadStoredApplyForRemoteTransition(ctx, remoteApply, allowStoppedStoredApply)
	if transitionStatus != storedApplyTransitionReady {
		logSkippedRemoteApplyTransition(ctx, logger, "mark remote gRPC apply failed", remoteApply, storedApply, transitionStatus, err)
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
			logger.InfoContext(ctx, "leaving terminal gRPC task unchanged after remote apply failure",
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

	// An operation-scoped drive records only its operation's task failures here.
	// The operator derives the failed operation row from those tasks and moves
	// the parent applies.state via the projection CAS; the driver must not write
	// the parent failure or run its parent-level side effects.
	if scope.suppressesDirectParentApplyWrites() {
		// A task-less operation has no task rows to carry the failure, so the
		// operator could never derive its failed operation row. Mark it failed
		// directly on a non-retryable failure; a retryable failure is left
		// non-terminal so the operator re-drives the operation (and re-polls the
		// existing remote apply).
		if scope.tasklessOperationScope() && !retryable {
			if err := c.storage.ApplyOperations().MarkFailed(ctx, scope.applyOperationID, message); err != nil {
				return fmt.Errorf("mark task-less apply_operation %d failed after remote apply failure: %w", scope.applyOperationID, err)
			}
		}
		logger.DebugContext(ctx, "recorded operation task failures during operation-scoped drive; parent failure is owned by the rollout projection",
			"apply_operation_id", scope.applyOperationID,
			"operation_deployment", scope.operation.Deployment,
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
	logger := c.applyLogger(remoteApply)
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
		logSkippedRemoteApplyTransition(ctx, logger, "persist remote terminal apply", remoteApply, storedApply, transitionStatus, err)
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
					logOperationDriveLeavesParentStop(logger, storedApply, scope)
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
	if err := c.stampRemoteApplyErrorOnFailedTasks(ctx, storedApply, remoteApply, storedTasks, now); err != nil {
		return err
	}
	return c.persistTerminalStateFromRemote(ctx, storedApply, remoteApply, now, scope)
}

// stampRemoteApplyErrorOnFailedTasks copies a failed remote apply's error
// message onto its failed stored task rows that carry no error of their own.
// The per-table reason normally arrives on the remote TableProgress snapshot
// and is adopted by syncStoredTasksFromRemoteTasks, but it can be absent — a
// data plane on an older proto omits the field, and a failure that never
// reached a specific table (for example an engine preflight rejection) has no
// per-table text to report. Such a task persists as failed with an empty
// ErrorMessage even though the remote apply carries the actionable reason. The
// stored task row is what the operator derives the operation row from, so an
// empty task error would otherwise become an empty operation — and PR-comment
// — failure reason. A task that already carries its own, more specific error
// keeps it.
func (c *GRPCClient) stampRemoteApplyErrorOnFailedTasks(ctx context.Context, storedApply, remoteApply *storage.Apply, storedTasks []*storage.Task, now time.Time) error {
	if !state.IsState(remoteApply.State, state.Apply.Failed) || remoteApply.ErrorMessage == "" {
		return nil
	}
	logger := c.applyLogger(storedApply)
	for _, storedTask := range storedTasks {
		if !state.IsState(storedTask.State, state.Task.Failed) || storedTask.ErrorMessage != "" {
			continue
		}
		storedTask.ErrorMessage = remoteApply.ErrorMessage
		storedTask.UpdatedAt = now
		if err := c.storage.Tasks().Update(ctx, storedTask); err != nil {
			return fmt.Errorf("stamp remote apply error on failed task %s for %s: %w", storedTask.TaskIdentifier, storedApply.ApplyIdentifier, err)
		}
		logger.InfoContext(ctx, "stamped remote apply failure reason on failed task that carried no error",
			append(storedApply.MutableLogAttrs(),
				"task_id", storedTask.TaskIdentifier,
				"table", storedTask.TableName,
				"error_message", remoteApply.ErrorMessage)...)
	}
	return nil
}

func storedStoppedApplyCanAdoptRemoteTerminalState(storedApply, remoteApply *storage.Apply) bool {
	return storedApply != nil &&
		state.IsState(storedApply.State, state.Apply.Stopped) &&
		!state.IsState(remoteApply.State, state.Apply.Stopped)
}

func (c *GRPCClient) persistTerminalStateFromRemote(ctx context.Context, storedApply, remoteApply *storage.Apply, now time.Time, scope applyTaskScope) error {
	// An operation-scoped drive owns only its operation. The terminal task states
	// were already synced by the caller; the operation row is derived from those
	// tasks by the operator, which then moves the parent applies.state via the
	// projection CAS and completes any parent control requests. The driver must
	// not write the parent row or run its parent-level side effects here.
	if scope.suppressesDirectParentApplyWrites() {
		// A task-less operation has no task rows for the operator to derive the
		// operation row from, so persist its terminal state directly here.
		if scope.tasklessOperationScope() {
			if err := c.persistTasklessOperationTerminalState(ctx, scope.applyOperationID, remoteApply.State, remoteApply.ErrorMessage); err != nil {
				return err
			}
		}
		c.applyLogger(storedApply).DebugContext(ctx, "skipping parent terminal write during operation-scoped drive; operation tasks are resolved and parent state is owned by the rollout projection",
			"apply_operation_id", scope.applyOperationID,
			"operation_deployment", scope.operation.Deployment,
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

// persistTasklessOperationTerminalState reflects a remote terminal state onto a
// task-less operation row. Because such an operation carries no task rows, the
// operator's task-derived projection can never move it: the drive owns its
// terminal transition, mirroring the local drive. Every terminal state is
// written, not just completion and failure — a stop or cancel that lands on a
// task-less operation has no task rows for the operator's stop handling to move
// either, so leaving one of those unwritten would hold the operation row and its
// parent apply running forever with the target blocked.
func (c *GRPCClient) persistTasklessOperationTerminalState(ctx context.Context, applyOperationID int64, terminalState, errMsg string) error {
	opStore := c.storage.ApplyOperations()
	switch {
	case state.IsState(terminalState, state.Apply.Completed):
		if err := opStore.MarkCompleted(ctx, applyOperationID); err != nil {
			return fmt.Errorf("mark task-less apply_operation %d completed from remote terminal state: %w", applyOperationID, err)
		}
	case state.IsState(terminalState, state.Apply.Failed):
		if err := opStore.MarkFailed(ctx, applyOperationID, errMsg); err != nil {
			return fmt.Errorf("mark task-less apply_operation %d failed from remote terminal state: %w", applyOperationID, err)
		}
	case state.IsState(terminalState, state.Apply.Stopped):
		// Stopped is terminal but resumable, so mirror the state and leave
		// completed_at nil — the same convention the operator's own operation
		// writer uses, so a later start finds a row it can resume.
		if err := opStore.UpdateState(ctx, applyOperationID, terminalState); err != nil {
			return fmt.Errorf("update task-less apply_operation %d to stopped from remote terminal state: %w", applyOperationID, err)
		}
	case state.IsTerminalApplyState(terminalState):
		// cancelled / reverted — non-resumable, so stamp completed_at.
		if err := opStore.MarkTerminal(ctx, applyOperationID, terminalState); err != nil {
			return fmt.Errorf("mark task-less apply_operation %d terminal state %q from remote: %w", applyOperationID, terminalState, err)
		}
	default:
		c.baseLogger().WarnContext(ctx, "task-less apply_operation reached the terminal writer with a non-terminal remote state; operation left claimable",
			"apply_operation_id", applyOperationID, "remote_state", terminalState)
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

// remoteApplyPausedForDataPlaneRetry reports whether a remote progress
// snapshot is a retryable pause rather than a final verdict: the data plane's
// own recovery will claim another attempt, so the control plane must keep
// polling instead of terminalizing — a terminal verdict here would orphan a
// live remote apply that can still cut over. A current data plane says so
// directly with STATE_FAILED_RETRYABLE. A data plane from before that wire
// state reports the pause as STATE_FAILED and reveals the retryable truth only
// on the per-table status strings, so a STATE_FAILED snapshot with a table
// still in failed_retryable is also a pause.
func remoteApplyPausedForDataPlaneRetry(protoState ternv1.State, remoteTasks []*ternv1.TableProgress) bool {
	if protoState == ternv1.State_STATE_FAILED_RETRYABLE {
		return true
	}
	if protoState != ternv1.State_STATE_FAILED {
		return false
	}
	for _, remoteTask := range remoteTasks {
		if remoteTask == nil {
			continue
		}
		if state.IsState(state.NormalizeTaskStatus(remoteTask.Status), state.Task.FailedRetryable) {
			return true
		}
	}
	return false
}

// remoteProgressIsTerminal reports whether a remote progress snapshot settles
// the apply. It refines isTerminalProtoState with the pause check: a snapshot
// that is really a retryable pause is not terminal. A STATE_FAILED snapshot
// with no retryable table (for example a dispatch-level failure that never
// created tasks, or exhausted retries) stays terminal — the guard only holds
// the drive open when the snapshot proves the data plane will retry.
func remoteProgressIsTerminal(protoState ternv1.State, remoteTasks []*ternv1.TableProgress) bool {
	return isTerminalProtoState(protoState) && !remoteApplyPausedForDataPlaneRetry(protoState, remoteTasks)
}

// remoteProgressApplyState maps a remote progress snapshot to the stored apply
// state it represents: a snapshot that is a retryable pause maps to
// failed_retryable, not failed, even when an older data plane reported the
// pause as STATE_FAILED. Like ProtoStateToStorage it returns "" for an
// unmapped state.
func remoteProgressApplyState(protoState ternv1.State, remoteTasks []*ternv1.TableProgress) string {
	mapped := ProtoStateToStorage(protoState)
	if mapped == "" {
		return ""
	}
	if remoteApplyPausedForDataPlaneRetry(protoState, remoteTasks) {
		return state.Apply.FailedRetryable
	}
	return mapped
}

// remoteTaskResumedFromRetryablePause reports whether a remote task status
// shows the data plane actively driving the task again after a retryable
// pause. A stored failed_retryable task normally pins its state so a stale
// engine poll cannot hide an operator-written pause, but the remote Progress
// response reads the data plane's durable rows: an actively driving status
// after a pause means a recovery attempt really re-claimed the task, and the
// mirror must follow it. Pending does not count — the data plane parks
// requeued tasks there before a recovery claims them — and terminal statuses
// flow through taskStateWithNoBackwardProgress, which already admits them.
func remoteTaskResumedFromRetryablePause(remoteTaskState string) bool {
	return !state.IsTerminalTaskState(remoteTaskState) &&
		!state.IsState(remoteTaskState,
			state.Task.FailedRetryable,
			state.Task.Stopped,
			state.Task.Pending)
}

// syncStoredTasksFromRemoteTasks mirrors the per-task table progress fields
// returned by remote Tern. It only copies the remote task snapshot; terminal
// remote applies are persisted only after those copied task states are resolved.
//
// Every stored task row is written on every call, even when no field moved:
// the operator reads tasks.updated_at as the drive's liveness signal
// (ApplyDriveStallAfter) and cancels a drive whose rows stop advancing, so
// the write must stay unconditional — including through parked states such as
// deferred cutovers and revert windows, where nothing changes tick to tick.
func (c *GRPCClient) syncStoredTasksFromRemoteTasks(
	ctx context.Context,
	storedApply *storage.Apply,
	storedTasks []*storage.Task,
	remoteTasks []*ternv1.TableProgress,
	now time.Time,
) error {
	logger := c.applyLogger(storedApply)
	remoteTaskIndex := IndexProtoTableProgress(remoteTasks)
	missingProgressTasks := 0
	for _, storedTask := range storedTasks {
		remoteTask, ok := remoteTaskIndex.ForTask(storedTask)
		if !ok {
			missingProgressTasks++
			continue
		}
		oldTaskState := storedTask.State
		c.unrecognizedStatuses.observeTaskStatus(ctx, logger, storedTask, remoteTask.Status)
		remoteTaskState := state.NormalizeTaskStatus(remoteTask.Status)
		switch {
		case state.IsState(remoteTaskState, state.Task.Stopped):
			storedTask.State = remoteTaskState
		case state.IsState(storedTask.State, state.Task.FailedRetryable) &&
			state.RecognizedTaskStatus(remoteTask.Status) &&
			remoteTaskResumedFromRetryablePause(remoteTaskState):
			// The data plane owns recovery of its retryable failures: the
			// stored pause must follow the remote's new attempt instead of
			// pinning "Retrying" on the operator surfaces while the data
			// plane is actively copying. Un-pinning needs positive evidence:
			// an unrecognized remote status normalizes to running as a
			// fail-open default, which is no proof the row left its pause.
			storedTask.State = remoteTaskState
		default:
			storedTask.State = taskStateWithNoBackwardProgress(storedTask.State, remoteTaskState)
		}
		if !state.IsState(storedTask.State, remoteTaskState) {
			logger.DebugContext(ctx, "keeping stored gRPC task state because remote progress reported earlier state",
				"external_id", storedApply.ExternalID,
				"task_id", storedTask.TaskIdentifier,
				"table", storedTask.TableName,
				"stored_task_state", oldTaskState,
				"remote_task_state", remoteTaskState)
		}
		if remoteTaskOmittedRowTotals(storedTask, remoteTask) {
			logger.DebugContext(ctx, "keeping stored gRPC task row-copy progress because remote progress omitted row totals",
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
			storedTask.ETASeconds = int(remoteTask.EtaSeconds)
			storedTask.ChecksumRowsChecked = remoteTask.ChecksumRowsChecked
			storedTask.ChecksumRowsTotal = remoteTask.ChecksumRowsTotal
		}
		// Throttle state is a point-in-time signal, not cumulative progress:
		// it is mirrored on every tick — including ticks whose row totals are
		// kept — so a lifted throttle clears promptly instead of lingering on
		// the PR comment. The reason travels only with an active throttle and
		// is bounded here because the remote data plane's reason is untrusted
		// input for the operator surfaces that render it.
		storedTask.Throttled = remoteTask.Throttled
		storedTask.ThrottleReason = ""
		if remoteTask.Throttled {
			storedTask.ThrottleReason = engine.SanitizeThrottleReason(remoteTask.ThrottleReason)
		}
		// Adopt the remote task's own failure reason (for example an engine
		// preflight rejection) so the operation row derived from the stored task
		// carries a per-table error. An empty remote error never clears a stored
		// one: a data plane running an older proto omits the field entirely, and
		// the stored message may have been stamped from the apply-level error.
		if remoteTask.ErrorMessage != "" {
			storedTask.ErrorMessage = remoteTask.ErrorMessage
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

		// Mirror the per-shard progress the data plane reported into control-plane
		// storage so the PR comment / CLI can render the per-shard breakdown and the
		// control plane can see shard drift. The control plane is a reader: it never
		// polls the engine, so the only per-shard source is the remote Progress
		// response, which the in-process drive's write-through never reaches.
		c.syncShardProgressFromRemote(ctx, storedApply, storedTask, remoteTask.Shards, now)
	}
	if missingProgressTasks > 0 {
		logger.WarnContext(ctx, "remote gRPC progress omitted stored tasks",
			append(storedApply.MutableLogAttrs(), "missing_count", missingProgressTasks)...)
	}
	return nil
}

func remoteTaskOmittedRowTotals(storedTask *storage.Task, remoteTask *ternv1.TableProgress) bool {
	if storedTask == nil || remoteTask == nil {
		return false
	}
	return storedTask.RowsTotal > 0 && remoteTask.RowsTotal <= 0
}

// syncShardProgressFromRemote mirrors the per-shard progress carried in a remote
// Tern Progress response into control-plane storage as per-(table, shard) task
// rows (`shard != ""`), so the PR comment / CLI render the per-shard breakdown and
// the control plane can see shard drift. It runs only inside the operator's
// lease-held reconcile (the gRPC read path renders from storage and never reaches
// here), so a missing lease or apply_operation is unexpected and warned rather
// than silently dropping per-shard progress. A failed shard write is logged rather
// than failing the table-level sync — the next reconcile re-applies it.
func (c *GRPCClient) syncShardProgressFromRemote(ctx context.Context, storedApply *storage.Apply, storedTask *storage.Task, shards []*ternv1.ShardProgress, now time.Time) {
	if len(shards) == 0 {
		return
	}
	logger := c.applyLogger(storedApply)
	_, hasOpLease := storage.OperationLeaseFromContext(ctx)
	_, hasApplyLease := storage.ApplyLeaseFromContext(ctx)
	if !hasOpLease && !hasApplyLease {
		// This path is reached only from the lease-held drive, so no lease means
		// per-shard progress will silently not persist — surface it for triage.
		logger.WarnContext(ctx, "skipping remote per-shard progress encode: no lease on reconcile context",
			append(storedApply.MutableLogAttrs(), "table", storedTask.TableName)...)
		return
	}
	// Per-shard rows hang off the table's apply_operation; a sharded task without
	// one is unexpected and leaves the per-shard view empty.
	if storedTask.ApplyOperationID == nil {
		logger.WarnContext(ctx, "skipping remote per-shard progress encode: stored task has no apply_operation_id",
			append(storedApply.MutableLogAttrs(), "table", storedTask.TableName)...)
		return
	}
	// A shard-scoped drive task (per-shard work operation) is itself the
	// per-shard row for its shard. Fanning a table-level shard breakdown out
	// under its operation would overwrite the drive task's own row and attach
	// rows for shards the operation does not own. Only table-level tasks
	// (shard == "") mirror a per-shard breakdown.
	if storedTask.Shard != "" {
		logger.WarnContext(ctx, "skipping remote per-shard progress encode: stored task is scoped to a single shard and owns no breakdown",
			append(storedApply.MutableLogAttrs(), "table", storedTask.TableName, "task_shard", storedTask.Shard)...)
		return
	}
	for _, sh := range shards {
		// An empty shard would collide with the unsharded single-shard sentinel,
		// so the entry cannot be stored as a per-shard row.
		if sh.Shard == "" {
			logger.WarnContext(ctx, "skipping remote per-shard progress entry with an empty shard name",
				append(storedApply.MutableLogAttrs(), "table", storedTask.TableName)...)
			continue
		}
		c.unrecognizedStatuses.observeShardStatus(ctx, logger, storedTask, sh.Shard, sh.Status)
		shardState := state.NormalizeShardStatus(sh.Status)
		// The proto carries row totals, not a percent; derive it the way the read
		// model does and clamp (row counts can momentarily exceed the total).
		pct := 0
		if sh.RowsTotal > 0 {
			pct = min(int(sh.RowsCopied*100/sh.RowsTotal), 100)
		}
		if state.IsState(shardState, state.Task.Completed) {
			pct = 100
		}
		shardTask := &storage.Task{
			TaskIdentifier:   engine.NewTaskID(),
			ApplyID:          storedTask.ApplyID,
			ApplyOperationID: storedTask.ApplyOperationID,
			PlanID:           storedTask.PlanID,
			Database:         storedTask.Database,
			DatabaseType:     storedTask.DatabaseType,
			Engine:           storedTask.Engine,
			Repository:       storedTask.Repository,
			PullRequest:      storedTask.PullRequest,
			Environment:      storedTask.Environment,
			Namespace:        storedTask.Namespace,
			TableName:        storedTask.TableName,
			Shard:            sh.Shard,
			DDL:              storedTask.DDL,
			DDLAction:        storedTask.DDLAction,
			State:            shardState,
			RowsCopied:       sh.RowsCopied,
			RowsTotal:        sh.RowsTotal,
			ProgressPercent:  pct,
			ETASeconds:       int(sh.EtaSeconds),
			CutoverAttempts:  int(sh.CutoverAttempts),
			CreatedAt:        now,
			UpdatedAt:        now,
		}
		if err := c.storage.Tasks().UpsertShardProgress(ctx, shardTask); err != nil {
			if errors.Is(err, storage.ErrApplyLeaseLost) {
				// A peer claimed the work; this driver is displaced. Stop — every
				// further shard would fail the same way. The lost lease may be the
				// operation lease (fan-out drive) or the apply lease (single-operation
				// drive), since UpsertShardProgress accepts either.
				logger.DebugContext(ctx, "stopping remote per-shard progress encode: drive lease lost",
					append(storedApply.MutableLogAttrs(), "table", storedTask.TableName, "shard", sh.Shard)...)
				return
			}
			logger.ErrorContext(ctx, "failed to encode remote per-shard progress into control-plane storage",
				append(storedApply.MutableLogAttrs(),
					"apply_operation_id", *storedTask.ApplyOperationID,
					"namespace", storedTask.Namespace, "table", storedTask.TableName, "shard", sh.Shard,
					"error", err)...)
		}
	}
}

// reconcileStoredTasksForTerminalRemoteApply force-resolves any stored task the
// remote progress left unresolved once the remote apply itself is terminal. A
// terminal remote apply is authoritative: the remote will send no further task
// progress, so a lagging task is driven to the apply's terminal state and
// persisted rather than blocking finalization (which would otherwise re-poll the
// already-terminal remote forever). A storage failure is still returned so the
// operator retries that genuinely-transient case.
func (c *GRPCClient) reconcileStoredTasksForTerminalRemoteApply(ctx context.Context, storedApply, remoteApply *storage.Apply, storedTasks []*storage.Task, now time.Time) error {
	logger := c.applyLogger(storedApply)
	terminalTaskState, ok := terminalTaskStateForApply(remoteApply.State)
	if !ok {
		return fmt.Errorf("reconcile stored tasks for %s: remote apply state %q is not terminal", storedApply.ApplyIdentifier, remoteApply.State)
	}
	for _, storedTask := range storedTasks {
		if storedTaskResolvedForTerminalRemoteApply(remoteApply.State, storedTask.State) {
			continue
		}
		oldTaskState := storedTask.State
		resolvedState := terminalTaskState
		if taskCancelledByRemoteApplyFailure(terminalTaskState, oldTaskState) {
			resolvedState = state.Task.Cancelled
		}
		storedTask.State = resolvedState
		if state.IsState(resolvedState, state.Task.Completed) {
			storedTask.ProgressPercent = 100
		}
		if state.IsTerminalTaskState(resolvedState) && storedTask.CompletedAt == nil {
			storedTask.CompletedAt = &now
		}
		storedTask.UpdatedAt = now
		if err := c.storage.Tasks().Update(ctx, storedTask); err != nil {
			return fmt.Errorf("reconcile lagging task %s to %s for terminal remote gRPC apply %s: %w", storedTask.TaskIdentifier, resolvedState, storedApply.ApplyIdentifier, err)
		}
		logger.WarnContext(ctx, "reconciled lagging stored task to terminal remote gRPC apply state",
			"external_id", storedApply.ExternalID,
			"remote_apply_state", remoteApply.State,
			"task_id", storedTask.TaskIdentifier,
			"table", storedTask.TableName,
			"old_task_state", oldTaskState,
			"new_task_state", resolvedState)
		c.logTaskStateTransition(ctx, storedApply.ID, storedTask, fmt.Sprintf("Task %s reconciled to %s for terminal remote apply state %s", storedTask.TableName, resolvedState, remoteApply.State), oldTaskState)
	}
	return nil
}

// taskCancelledByRemoteApplyFailure reports whether a stored task lagging
// behind a failed remote apply resolves to cancelled instead of failed. A
// pending task never started: the remote failure happened before this table's
// work began, so it is cancelled — mirroring how a sequential drive resolves
// the tables queued behind a failed one — rather than failed, which would
// misattribute the failure to a table that did no work. A task that had
// started keeps the failed resolution: it was in flight when the apply died.
func taskCancelledByRemoteApplyFailure(terminalTaskState, storedTaskState string) bool {
	return state.IsState(terminalTaskState, state.Task.Failed) &&
		state.IsState(storedTaskState, state.Task.Pending)
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
//
// remoteTasks is the same report's per-table progress. It backs the one case
// where the no-backward rank yields to the remote: a stored cutting_over that
// the report contradicts with a table still in an earlier active phase (see
// storedCutoverContradictedByEarlierActiveWork).
func applyStateFromRemoteProgress(storedApplyState, remoteApplyState string, remoteTasks []*ternv1.TableProgress, allowStoppedStoredApply bool) string {
	if remoteApplyState == "" {
		return storedApplyState
	}
	if state.IsTerminalApplyState(remoteApplyState) {
		return remoteApplyState
	}
	// A remote retryable pause parks the apply between data-plane recovery
	// attempts. The stored apply must not adopt it while a live drive holds
	// the row: failed_retryable is immediately claimable (no stale-lease
	// requirement), so persisting it mid-drive would invite a second driver
	// onto the same apply. The task rows carry the pause for operator
	// surfaces; the drive keeps polling until the data plane settles.
	if state.IsState(remoteApplyState, state.Apply.FailedRetryable) {
		// The in-memory apply can still carry its pre-claim snapshot: the
		// claim moves the row to running but hands the driver the pre-claim
		// state. Echoing a parked snapshot back would persist an immediately
		// claimable state onto a leased row, so normalize it to the running
		// state the claim already wrote.
		if state.IsState(storedApplyState, state.Apply.FailedRetryable, state.Apply.Pending) {
			return state.Apply.Running
		}
		return storedApplyState
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
	if storedCutoverContradictedByEarlierActiveWork(storedApplyState, remoteTasks) {
		return remoteApplyState
	}
	if applyProgressRank(remoteApplyState) < applyProgressRank(storedApplyState) {
		return storedApplyState
	}
	return remoteApplyState
}

// storedCutoverContradictedByEarlierActiveWork reports whether the stored
// apply state says cutting_over while the remote report still shows a table
// in an earlier active phase — queued, copying, or verifying. A cutover
// surfaces at the apply level only when it is the least advanced work left,
// so this pairing never describes a live drive: the stored value is a sample
// of one table's cutover that the drive has since moved past, and the report
// carrying the earlier-phase table is the corrected state — it wins over the
// no-backward rank instead of being discarded as a regression. A parked
// WAITING_FOR_CUTOVER table is not a contradiction: a cutover legitimately
// proceeds while a sibling waits at the barrier for its own command.
func storedCutoverContradictedByEarlierActiveWork(storedApplyState string, remoteTasks []*ternv1.TableProgress) bool {
	if !state.IsState(storedApplyState, state.Apply.CuttingOver) {
		return false
	}
	for _, remoteTask := range remoteTasks {
		if remoteTask == nil {
			continue
		}
		if state.IsState(state.NormalizeTaskStatus(remoteTask.Status),
			state.Task.Pending, state.Task.Running,
			state.Task.CatchingUp, state.Task.Checksumming, state.Task.PostChecksum) {
			return true
		}
	}
	return false
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
	case state.Apply.CatchingUp:
		return 8
	case state.Apply.Checksumming:
		return 9
	case state.Apply.PostChecksum:
		return 10
	case state.Apply.WaitingForCutover:
		return 11
	case state.Apply.CuttingOver:
		return 12
	case state.Apply.RevertWindow:
		return 13
	case state.Apply.SkippingRevert:
		return 14
	case state.Apply.Reverting:
		return 15
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
	logger := c.applyLogger(apply)
	ticker := time.NewTicker(grpcProgressPollInterval)
	defer ticker.Stop()

	heartbeatTicker := time.NewTicker(10 * time.Second)
	defer heartbeatTicker.Stop()

	consecutiveProgressErrors := 0
	loggedStoppedAfterStart := false
	loggedRetryablePause := false
	var stoppedAfterStartDeadline time.Time
	var lastDisplayBlob string
	lastHeartbeatSuccess := time.Now()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-heartbeatTicker.C:
			// Heartbeat: bump updated_at to maintain the lease. A multi-operation
			// drive heartbeats its own operation row; every other drive
			// heartbeats the parent applies row.
			err := c.heartbeatScopedDrive(ctx, apply, scope)
			if err == nil {
				lastHeartbeatSuccess = time.Now()
				continue
			}
			if ctx.Err() != nil {
				// The drive is shutting down; the failed write is cancellation
				// fallout, not lease trouble.
				return ctx.Err()
			}
			if stopErr := driveEndingHeartbeatFailure(logger, apply, err, lastHeartbeatSuccess); stopErr != nil {
				return stopErr
			}
		case <-ticker.C:
			if handled, err := c.processPendingCancelOrStopControlRequest(ctx, apply, scope); err != nil {
				logger.Warn("pending gRPC stop request processing failed; current apply owner will exit for operator retry",
					append(apply.MutableLogAttrs(), "error", err)...)
				return err
			} else if handled {
				return nil
			}
			if err := c.processPendingCutoverControlRequest(ctx, apply, scope); err != nil {
				logger.Warn("pending gRPC cutover request processing failed; current apply owner will exit for operator retry",
					append(apply.MutableLogAttrs(), "error", err)...)
				return err
			}
			if err := c.processPendingSkipRevertControlRequest(ctx, apply, scope.remoteApplyID(apply)); err != nil {
				logger.Warn("pending gRPC skip-revert request processing failed; current apply owner will exit for operator retry",
					append(apply.MutableLogAttrs(), "error", err)...)
				return err
			}
			if err := c.processPendingRevertControlRequest(ctx, apply, scope.remoteApplyID(apply)); err != nil {
				logger.Warn("pending gRPC revert request processing failed; current apply owner will exit for operator retry",
					append(apply.MutableLogAttrs(), "error", err)...)
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
				logger.Warn("remote gRPC progress poll failed",
					append(apply.MutableLogAttrs(),
						"consecutive_errors", consecutiveProgressErrors,
						"max_consecutive_errors", maxGRPCProgressPollErrorStreak,
						"error", err)...)
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
			newState := remoteProgressApplyState(resp.State, resp.Tables)
			if newState == "" {
				message := fmt.Sprintf("Remote progress returned unmapped apply state %s; operator will retry without changing stored state", remoteApplyStateDescription(resp.State))
				logger.Warn("remote gRPC progress returned unmapped apply state; operator will retry without changing stored state",
					append(apply.MutableLogAttrs(),
						"remote_state", resp.State.String(),
						"remote_state_number", int32(resp.State))...)
				c.logApplyWarning(ctx, apply, message)
				return fmt.Errorf("poll remote gRPC apply %s: unmapped remote state %s", apply.ApplyIdentifier, remoteApplyStateDescription(resp.State))
			}
			if apply.StartedAt == nil && newState != state.Apply.Pending {
				apply.StartedAt = &now
			}
			remoteApplyState := newState
			if state.IsState(remoteApplyState, state.Apply.FailedRetryable) {
				if !loggedRetryablePause {
					logger.Info("remote gRPC apply paused for data-plane retry; drive keeps polling and will not terminalize",
						append(apply.MutableLogAttrs(), "remote_error", resp.ErrorMessage)...)
					loggedRetryablePause = true
				}
			} else {
				loggedRetryablePause = false
			}
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
					logger.Info("remote gRPC apply still stopped after start accepted; operator will keep polling",
						append(apply.MutableLogAttrs(), "deadline", stoppedAfterStartDeadline)...)
					loggedStoppedAfterStart = true
				}
				if !now.Before(stoppedAfterStartDeadline) {
					message := fmt.Sprintf("remote apply %s remained stopped after start grace period %s", apply.ExternalID, grpcStoppedAfterStartGracePeriod)
					logger.Warn("remote gRPC apply remained stopped after start grace period; storing stopped state",
						append(apply.MutableLogAttrs(), "grace_period", grpcStoppedAfterStartGracePeriod)...)
					c.logApplyWarning(ctx, apply, message)
					apply.State = state.Apply.Stopped
					apply.ErrorMessage = message
					if err := c.reconcileTerminalRemoteProgress(ctx, apply, resp.Tables, now, scope); err != nil {
						return fmt.Errorf("persist stopped gRPC apply %s after start grace period: %w", apply.ApplyIdentifier, err)
					}
					if err := failPendingControlRequests(ctx, c.storage, apply, storage.ControlOperationStart, message, remoteID); err != nil {
						return err
					}
					return fmt.Errorf("start accepted for gRPC apply %s but %s", apply.ApplyIdentifier, message)
				}
				continue
			}
			newState = applyStateFromRemoteProgress(apply.State, remoteApplyState, resp.Tables, allowStoppedAfterStart)
			if !state.IsState(newState, remoteApplyState) {
				logger.Debug("keeping stored gRPC apply state because remote progress reported earlier state",
					append(apply.MutableLogAttrs(), "remote_state", remoteApplyState)...)
			}
			apply.State = newState
			apply.ErrorMessage = remoteProgressErrorMessage(apply.State, resp.ErrorMessage, apply.ErrorMessage)
			apply.UpdatedAt = now
			c.mirrorRemoteControlRejections(ctx, apply, remoteID, resp.SettledControlRequests)

			if remoteProgressIsTerminal(resp.State, resp.Tables) {
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
				logger.Info("operation parked at cutover barrier; exiting remote copy drive",
					apply.MutableLogAttrs()...)
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
