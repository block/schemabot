// Package api provides the SchemaBot HTTP API service.
package api

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	gomysql "github.com/go-sql-driver/mysql"

	"github.com/block/schemabot/pkg/clock"
	"github.com/block/schemabot/pkg/secrets"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// Config holds configuration for the SchemaBot service.
type Config struct {
	// Tern endpoints per environment.
	// Each environment (staging, production) has its own Tern instance.
	Tern TernConfig

	// GitHubAppID is the GitHub App ID for authentication.
	GitHubAppID int64

	// GitHubPrivateKey is the PEM-encoded private key for the GitHub App.
	GitHubPrivateKey []byte

	// GitHubWebhookSecret is the secret for validating GitHub webhooks.
	GitHubWebhookSecret string
}

// TernConfig maps deployment name to environment endpoints.
// Use "default" as the deployment key for single-deployment deployments.
//
// Single-deployment:
//
//	TernConfig{
//	    "default": {
//	        "staging":    "tern-staging:9090",
//	        "production": "tern-production:9090",
//	    },
//	}
//
// Multi-deployment:
//
//	TernConfig{
//	    "a": {
//	        "staging":    "tern-a-staging:9090",
//	        "production": "tern-a-prod:9090",
//	    },
//	    "b": {
//	        "staging":    "tern-b-staging:9090",
//	        "production": "tern-b-prod:9090",
//	    },
//	}
type TernConfig map[string]TernEndpoints

// TernEndpoints maps environment name to gRPC address (host:port).
type TernEndpoints map[string]string

// DefaultDeployment is the deployment key used for single-deployment deployments.
const DefaultDeployment = "default"

// errTernDeploymentNotConfigured marks an Endpoint lookup that found no entry
// for the requested deployment/environment, as opposed to an entry that exists
// but is misconfigured (e.g. an empty endpoint). Callers use it to decide
// whether a dynamic fallback (a default client) may serve the target.
var errTernDeploymentNotConfigured = errors.New("tern deployment not configured")

// Endpoint returns the Tern endpoint for the given deployment and environment.
// For single-deployment deployments, use DefaultDeployment ("default") as the deployment.
func (c TernConfig) Endpoint(deployment, environment string) (string, error) {
	if deployment == "" {
		deployment = DefaultDeployment
	}

	endpoints, ok := c[deployment]
	if !ok {
		return "", fmt.Errorf("unknown deployment %q: %w", deployment, errTernDeploymentNotConfigured)
	}

	endpoint, ok := endpoints[environment]
	if !ok {
		return "", fmt.Errorf("unknown environment %q for deployment %q: %w", environment, deployment, errTernDeploymentNotConfigured)
	}

	if endpoint == "" {
		return "", fmt.Errorf("endpoint not configured for %s/%s", deployment, environment)
	}

	return endpoint, nil
}

// Service is the SchemaBot API service.
// RecoveryCallback is called after the operator claims an apply for recovery.
// The webhook handler uses this to start watching progress and posting PR comments.
type RecoveryCallback func(apply *storage.Apply)

// ApplyTerminalSummaryCallback is called after the operator wins the aggregate
// non-terminal→terminal projection compare-and-swap for a multi-operation apply.
// A multi-operation drive owns only its operation and suppresses the per-driver
// terminal observer, so the apply-level terminal summary is published exactly
// once, here, by the CAS winner. The apply and tasks are reloaded from storage
// (the apply at its terminal state, the tasks across every operation) before
// invocation. Single-operation applies keep publishing via the per-driver
// observer and never reach this callback.
type ApplyTerminalSummaryCallback func(ctx context.Context, apply *storage.Apply, tasks []*storage.Task) error

type pendingObserverKey struct {
	database    string
	deployment  string
	environment string
}

type Service struct {
	storage           storage.Storage
	config            *ServerConfig
	ternClients       map[string]tern.Client // keyed by "deployment/environment", lazily created
	defaultTernClient tern.Client            // fallback for any deployment/environment without a static or gRPC client (e.g. a TargetRouter)
	routingTernClient *tern.RoutingClient
	ternMu            sync.Mutex // protects tern client caches and engineFactories
	logger            *slog.Logger
	// checkRunBackfiller replays the auto-plan flow for a PR to recreate
	// missing Check Runs; wired from the webhook handler at serve startup,
	// nil when no GitHub webhook runtime exists.
	checkRunBackfiller CheckRunBackfiller
	clock              clock.Clock

	// engineFactories holds engine implementations for database types this build
	// does not provide natively, registered by an embedding service via
	// RegisterEngine. Local clients the service builds receive them.
	engineFactories map[string]tern.EngineFactory

	// Operator loop management.
	operatorMu           sync.Mutex
	stopRecovery         chan struct{}
	cancelRecovery       context.CancelFunc
	operatorWake         chan struct{}
	recoveryWg           sync.WaitGroup
	operatorPollInterval time.Duration
	remoteHealthMu       sync.Mutex
	remoteHealthCancel   context.CancelFunc
	remoteHealthWg       sync.WaitGroup
	remoteHealthInterval time.Duration

	// Pending drops cleaner loop management.
	pendingDropsMu     sync.Mutex
	pendingDropsCancel context.CancelFunc
	pendingDropsWg     sync.WaitGroup

	// OnApplyRecovered is called after the operator claims an apply and before
	// ResumeApply starts the engine/poller. Set by the webhook handler to attach
	// an observer for PR comments.
	OnApplyRecovered RecoveryCallback

	// OnApplyTerminalSummary is called after a multi-operation apply is
	// terminalized by the aggregate projection CAS. Set by the webhook handler to
	// publish the apply-level terminal PR summary and refresh GitHub check state
	// exactly once, since multi-operation drives suppress the per-driver observer.
	OnApplyTerminalSummary ApplyTerminalSummaryCallback

	pendingObserverMu sync.Mutex
	pendingObservers  map[pendingObserverKey]tern.ProgressObserver
}

// SetApplyObserver sets a progress observer on the tern client for an apply.
// The observer receives progress and terminal notifications from the poller.
func (s *Service) SetApplyObserver(database, deployment, environment string, applyID int64, observer tern.ProgressObserver) {
	deployment, err := s.deploymentForDatabaseEnvironment(database, deployment, environment)
	if err != nil {
		s.logger.Error("failed to resolve tern deployment for observer",
			"database", database, "deployment", deployment, "environment", environment, "apply_id", applyID, "error", err)
		return
	}
	client, err := s.RoutingTernClient()
	if err != nil {
		s.logger.Error("failed to get routing tern client for observer",
			"database", database, "deployment", deployment, "environment", environment, "apply_id", applyID, "error", err)
	} else {
		client.SetObserver(applyID, observer)
	}

	// A known apply can already be running by the time its observer is created,
	// so also attach directly to the concrete deployment client.
	deploymentClient, err := s.TernClient(deployment, environment)
	if err != nil {
		s.logger.Error("failed to get tern client for observer",
			"database", database, "deployment", deployment, "environment", environment, "apply_id", applyID, "error", err)
		return
	}
	deploymentClient.SetObserver(applyID, observer)
}

// SetPendingObserver stores an observer for the next apply request for this
// target. ExecuteApply registers it on the durable apply before operator
// dispatch can start.
func (s *Service) SetPendingObserver(database, deployment, environment string, observer tern.ProgressObserver) {
	deployment, err := s.deploymentForDatabaseEnvironment(database, deployment, environment)
	if err != nil {
		s.logger.Error("failed to resolve tern deployment for pending observer",
			"database", database, "deployment", deployment, "environment", environment, "error", err)
		return
	}

	key := pendingObserverKey{database: database, deployment: deployment, environment: environment}
	s.pendingObserverMu.Lock()
	defer s.pendingObserverMu.Unlock()
	if s.pendingObservers == nil {
		s.pendingObservers = make(map[pendingObserverKey]tern.ProgressObserver)
	}
	if observer == nil {
		delete(s.pendingObservers, key)
	} else {
		s.pendingObservers[key] = observer
	}
}

func (s *Service) consumePendingObserver(database, deployment, environment string) tern.ProgressObserver {
	key := pendingObserverKey{database: database, deployment: deployment, environment: environment}

	s.pendingObserverMu.Lock()
	defer s.pendingObserverMu.Unlock()
	observer := s.pendingObservers[key]
	delete(s.pendingObservers, key)
	return observer
}

// New creates a new SchemaBot service.
//
// The storage parameter is the database storage implementation. For production,
// use mysql.New(db) with a connected *sql.DB. For testing, use a mock.
//
// Pre-created ternClients can be passed to inject mock clients for testing.
// Pass nil to use lazy client creation from config.TernDeployments.
func New(st storage.Storage, config *ServerConfig, ternClients map[string]tern.Client, logger *slog.Logger) *Service {
	if ternClients == nil {
		ternClients = make(map[string]tern.Client)
	}
	return &Service{
		storage:              st,
		config:               config,
		ternClients:          ternClients,
		engineFactories:      make(map[string]tern.EngineFactory),
		logger:               logger,
		clock:                clock.Real{},
		operatorPollInterval: OperatorPollInterval,
		remoteHealthInterval: RemoteDeploymentHealthCheckInterval,
		pendingObservers:     make(map[pendingObserverKey]tern.ProgressObserver),
	}
}

// RegisterEngine registers an Engine implementation for a database type this
// build does not provide natively (the built-in types are unaffected). Call it
// during setup, before serving: local clients the service builds for that type
// use the registered factory. It is the extension point for an embedding
// service to supply an engine without the core depending on its package.
//
// It validates its inputs so a misconfiguration fails fast at setup rather than
// as a confusing downstream error (or a panic on a nil factory).
func (s *Service) RegisterEngine(databaseType string, factory tern.EngineFactory) error {
	if databaseType == "" {
		return fmt.Errorf("register engine: database type must not be empty")
	}
	if factory == nil {
		return fmt.Errorf("register engine for database type %q: factory must not be nil", databaseType)
	}
	s.ternMu.Lock()
	defer s.ternMu.Unlock()
	if s.engineFactories == nil {
		s.engineFactories = make(map[string]tern.EngineFactory)
	}
	s.engineFactories[databaseType] = factory
	return nil
}

// SetClock overrides the time source used by orchestration loops (currently
// the operator claim-duration measurement). Must be called before
// StartOperator — once operator drivers are running they read s.clock
// concurrently, so swapping the field is rejected to avoid a data race.
// Production callers should leave the default clock.Real{} in place; tests
// use clock.NewFake to make timing observable. A nil or typed-nil c is
// coalesced to clock.Real{} via clock.Default.
func (s *Service) SetClock(c clock.Clock) error {
	s.operatorMu.Lock()
	defer s.operatorMu.Unlock()
	if s.stopRecovery != nil {
		return fmt.Errorf("cannot change clock while operator is running")
	}
	s.clock = clock.Default(c)
	return nil
}

// SetOperatorPollInterval sets the operator driver poll interval.
// Most deployments should use the default interval; this is a low-level
// embedding hook for callers that need to tune the operator loop directly.
// Call before StartOperator so drivers create their tickers with the intended
// interval.
func (s *Service) SetOperatorPollInterval(interval time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("operator poll interval must be positive")
	}
	s.operatorMu.Lock()
	defer s.operatorMu.Unlock()
	if s.stopRecovery != nil {
		return fmt.Errorf("operator already running")
	}
	s.operatorPollInterval = interval
	return nil
}

// TernClient returns the Tern client for the given deployment and environment.
// Clients are created lazily on first use, so Tern connection failures only
// affect requests to that specific deployment/environment rather than blocking
// SchemaBot startup.
// For single-deployment setups, use DefaultDeployment ("default") as the deployment.
//
// The method first checks for config-based database registration (local mode),
// then uses TernDeployments for gRPC mode.
func (s *Service) TernClient(deployment, environment string) (tern.Client, error) {
	if deployment == "" {
		deployment = DefaultDeployment
	}
	key := deployment + "/" + environment

	s.ternMu.Lock()
	defer s.ternMu.Unlock()

	// Return existing client if already created
	if client, ok := s.ternClients[key]; ok {
		return client, nil
	}

	// Try local mode first (config-based database registration)
	if dbConfig := s.config.Database(deployment); dbConfig != nil {
		envConfig, ok := dbConfig.Environments[environment]
		switch {
		case !ok:
			s.logger.Debug("database config does not contain this environment, using remote tern deployment",
				"database", deployment, "environment", environment)
		case !envConfig.HasLocalDSN():
			s.logger.Debug("database config does not contain a local DSN, using remote tern deployment",
				"database", deployment, "environment", environment)
		default:
			client, err := s.newLocalTernClient(key, deployment, dbConfig.Type, envConfig)
			if err != nil {
				return nil, err
			}
			s.ternClients[key] = client
			s.logger.Info("created local tern client", "key", key, "type", dbConfig.Type, "deployment", deployment)
			return client, nil
		}
	}

	// Prefer an explicitly configured gRPC endpoint (TernDeployments) for this
	// deployment/environment over the dynamic default client.
	address, err := s.config.TernDeployments.Endpoint(deployment, environment)
	if err != nil {
		// Only fall back when the target is simply absent from TernDeployments.
		// An entry that exists but is misconfigured (e.g. an empty endpoint)
		// must fail closed rather than silently route to the default client.
		if errors.Is(err, errTernDeploymentNotConfigured) {
			// A configured default client serves any deployment/environment with
			// no static local DSN and no configured endpoint — typically a
			// TargetRouter that resolves the connection from the request's target.
			// This lets the durable-apply operator resolve dynamically-routed
			// targets without a per-request RegisterTernClient. It is shared across
			// keys (the router resolves per request), so it is not cached here.
			if s.defaultTernClient != nil {
				s.logger.Debug("using default tern client for dynamic target resolution", "deployment", deployment, "environment", environment)
				return s.defaultTernClient, nil
			}
			if deployment == DefaultDeployment {
				return nil, fmt.Errorf("not found in server configuration")
			}
		}
		return nil, err
	}

	// Create gRPC client lazily
	// Pass storage so GRPCClient can manage applies (heartbeats, progress tracking)
	client, err := tern.NewGRPCClient(tern.Config{
		Address: address,
		Storage: s.storage,
	})
	if err != nil {
		return nil, fmt.Errorf("create tern client for %s: %w", key, err)
	}

	s.ternClients[key] = client
	return client, nil
}

// RoutingTernClient returns a client that routes requests from server
// configuration and stored execution metadata. It is safe for Plan, Apply, and
// PullSchema routing; handlers that branch on transport mode must continue to
// use deployment-scoped clients until transport metadata is request-scoped.
func (s *Service) RoutingTernClient() (*tern.RoutingClient, error) {
	if s.config == nil {
		return nil, fmt.Errorf("server config is required for routing tern client")
	}
	if s.storage == nil {
		return nil, fmt.Errorf("storage is required for routing tern client")
	}

	s.ternMu.Lock()
	defer s.ternMu.Unlock()
	if s.routingTernClient != nil {
		return s.routingTernClient, nil
	}

	client, err := tern.NewRoutingClient(tern.RoutingClientConfig{
		Resolver:             s.config,
		PlanLookup:           s.storage.Plans(),
		ApplyLookup:          s.storage.Applies(),
		ApplyOperationLookup: s.storage.ApplyOperations(),
		ClientForDeployment: func(_ context.Context, deployment, environment string) (tern.Client, error) {
			return s.TernClient(deployment, environment)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create routing tern client: %w", err)
	}
	s.routingTernClient = client
	s.logger.Info("created routing tern client")
	return client, nil
}

// RegisterTernClient registers a tern client for the given deployment and
// environment. This allows embedders to add clients dynamically as they
// are created (e.g., lazily per-cluster).
func (s *Service) RegisterTernClient(deployment, environment string, client tern.Client) {
	if deployment == "" {
		deployment = DefaultDeployment
	}
	key := deployment + "/" + environment
	s.ternMu.Lock()
	defer s.ternMu.Unlock()
	s.ternClients[key] = client
}

// SetDefaultTernClient sets a fallback tern client used by TernClient for any
// deployment/environment that has no static local DSN configured. Pass a
// TargetRouter so the durable-apply operator and routing client resolve the
// connection from each request's target — the dynamic-resolution counterpart to
// RegisterTernClient, without needing one registration per deployment.
// SetCheckRunBackfiller wires the webhook handler's Check Run backfill entry
// point into the API service, so the checks operator endpoints can replay the
// auto-plan flow for a PR. Set once during serve startup, before traffic.
func (s *Service) SetCheckRunBackfiller(b CheckRunBackfiller) {
	s.checkRunBackfiller = b
}

func (s *Service) SetDefaultTernClient(client tern.Client) {
	s.ternMu.Lock()
	defer s.ternMu.Unlock()
	s.defaultTernClient = client
}

// malformedTokenError builds an error for a token that did not parse into a
// name:value pair. The secrets resolver returns literal (unprefixed) values
// as-is, so when the resolved token equals the configured ref the ref *is* the
// raw credential and must not be echoed. Only a true reference indirection
// (ref resolved to a different value, e.g. via env:/file:/secretsmanager:) is
// safe to print; for a literal we redact it, since the config key alone is
// enough for triage.
func malformedTokenError(key, ref, resolved, requirement string) error {
	if resolved != ref {
		return fmt.Errorf("token for %s resolved from %q %s", key, ref, requirement)
	}
	return fmt.Errorf("token for %s (literal value redacted) %s", key, requirement)
}

func (s *Service) newLocalTernClient(key, database, dbType string, envConfig EnvironmentConfig) (tern.Client, error) {
	// Resolve target DSN (handles env:, file: prefixes and structured DSN sources)
	targetDSN, err := envConfig.ResolveDSN()
	if err != nil {
		return nil, fmt.Errorf("resolve DSN for %s: %w", key, err)
	}

	// Resolve PlanetScale token if configured. Token validation is intentionally
	// first-use here (at client creation) rather than fail-fast at config load,
	// unlike revert_window_duration: resolving a token may require a call to a
	// secrets backend, so it is deferred until the client is actually built.
	var tokenName, tokenValue string
	if envConfig.TokenSecretRef != "" {
		token, err := secrets.Resolve(envConfig.TokenSecretRef, "")
		if err != nil {
			return nil, fmt.Errorf("resolve token for %s: %w", key, err)
		}
		parts := strings.SplitN(token, ":", 2)
		if len(parts) != 2 {
			return nil, malformedTokenError(key, envConfig.TokenSecretRef, token, "must be in name:value format")
		}
		tokenName = strings.TrimSpace(parts[0])
		tokenValue = strings.TrimSpace(parts[1])
		if tokenName == "" || tokenValue == "" {
			return nil, malformedTokenError(key, envConfig.TokenSecretRef, token, "must be in name:value format with a non-empty name and value")
		}
	}

	// Register TLS config for PlanetScale MySQL connections if configured
	var tlsName string
	if envConfig.TLS != nil {
		tlsName, err = registerTLSConfig(key, envConfig.TLS)
		if err != nil {
			return nil, fmt.Errorf("register TLS for %s: %w", key, err)
		}
	}

	// LocalClient uses SchemaBot's storage directly. ServerConfig.Validate
	// rejects an unparseable or non-positive revert_window_duration at config
	// load; parsing here fails closed rather than silently falling back to the
	// default window.
	var revertWindow time.Duration
	if envConfig.RevertWindowDuration != "" {
		d, err := time.ParseDuration(envConfig.RevertWindowDuration)
		if err != nil {
			return nil, fmt.Errorf("parse revert_window_duration %q for %s: %w", envConfig.RevertWindowDuration, key, err)
		}
		if d <= 0 {
			return nil, fmt.Errorf("revert_window_duration %q for %s must be positive (omit it to use the engine default)", envConfig.RevertWindowDuration, key)
		}
		revertWindow = d
		// Revert is engine-dependent: only Vitess/PlanetScale honors a revert
		// window. A window configured on a plain MySQL database is accepted but
		// ignored, so warn to surface the likely misconfiguration.
		if dbType == storage.DatabaseTypeMySQL {
			s.logger.Warn("revert_window_duration is configured but ignored for a MySQL database; revert is engine-dependent",
				"key", key, "database", database, "revert_window_duration", revertWindow.String())
		}
	}
	metadata := map[string]string{
		"organization": envConfig.Organization,
		"token_name":   tokenName,
		"token_value":  tokenValue,
	}
	if tlsName != "" {
		metadata["tls_name"] = tlsName
	}
	if revertWindow > 0 {
		metadata["revert_window_duration"] = revertWindow.String()
	}
	if envConfig.APIURL != "" {
		metadata["api_url"] = envConfig.APIURL
	}
	if !s.config.PendingDropsEnabled() {
		metadata["pending_drops"] = "false"
	}
	client, err := tern.NewLocalClient(tern.LocalConfig{
		Database:        database,
		Type:            dbType,
		TargetDSN:       targetDSN,
		Metadata:        metadata,
		WakeOperator:    s.wakeOperator,
		EngineFactories: s.engineFactories,
	}, s.storage, s.logger)
	if err != nil {
		return nil, fmt.Errorf("create local tern client for %s: %w", key, err)
	}
	return client, nil
}

// =============================================================================
// Exported Handlers
// =============================================================================
//
// Public HTTP handler methods that delegate to the internal handlers. These
// allow embedders to register individual SchemaBot routes on their own mux
// while using the OSS handler logic, preventing behavior drift.

// HandleProgressByApplyID is the HTTP handler for GET /api/progress/apply/{apply_id}.
func (s *Service) HandleProgressByApplyID(w http.ResponseWriter, r *http.Request) {
	s.handleProgressByApplyID(w, r)
}

// HandleStatus is the HTTP handler for GET /api/status.
// Returns recent applies across all databases.
func (s *Service) HandleStatus(w http.ResponseWriter, r *http.Request) {
	s.handleStatus(w, r)
}

// HandleDatabaseHistory is the HTTP handler for GET /api/history/{database}.
// Returns apply history for a specific database.
func (s *Service) HandleDatabaseHistory(w http.ResponseWriter, r *http.Request) {
	s.handleDatabaseHistory(w, r)
}

// HandleLogs is the HTTP handler for GET /api/logs/{database}.
func (s *Service) HandleLogs(w http.ResponseWriter, r *http.Request) {
	s.handleLogs(w, r)
}

// HandleLogsWithoutDatabase is the HTTP handler for GET /api/logs.
func (s *Service) HandleLogsWithoutDatabase(w http.ResponseWriter, r *http.Request) {
	s.handleLogsWithoutDatabase(w, r)
}

// HandlePlan is the HTTP handler for POST /api/plan.
func (s *Service) HandlePlan(w http.ResponseWriter, r *http.Request) {
	s.handlePlan(w, r)
}

// HandleApply is the HTTP handler for POST /api/apply.
func (s *Service) HandleApply(w http.ResponseWriter, r *http.Request) {
	s.handleApply(w, r)
}

// HandleCutover is the HTTP handler for POST /api/cutover.
func (s *Service) HandleCutover(w http.ResponseWriter, r *http.Request) {
	s.handleCutover(w, r)
}

// HandleStop is the HTTP handler for POST /api/stop.
func (s *Service) HandleStop(w http.ResponseWriter, r *http.Request) {
	s.handleStop(w, r)
}

// HandleStart is the HTTP handler for POST /api/start.
func (s *Service) HandleStart(w http.ResponseWriter, r *http.Request) {
	s.handleStart(w, r)
}

// HandleVolume is the HTTP handler for POST /api/volume.
func (s *Service) HandleVolume(w http.ResponseWriter, r *http.Request) {
	s.handleVolume(w, r)
}

// HandleRevert is the HTTP handler for POST /api/revert.
func (s *Service) HandleRevert(w http.ResponseWriter, r *http.Request) {
	s.handleRevert(w, r)
}

// HandleSkipRevert is the HTTP handler for POST /api/skip-revert.
func (s *Service) HandleSkipRevert(w http.ResponseWriter, r *http.Request) {
	s.handleSkipRevert(w, r)
}

// HandleRollbackPlan is the HTTP handler for POST /api/rollback/plan.
func (s *Service) HandleRollbackPlan(w http.ResponseWriter, r *http.Request) {
	s.handleRollbackPlan(w, r)
}

// =============================================================================
// Route Registration
// =============================================================================

// maxAPIRequestBodyBytes caps the request body size for every route
// registered by ConfigureRoutes, including the health endpoints.
// The largest legitimate payloads are plan and pull requests carrying full
// schema files — a database with hundreds of tables can reach a few megabytes
// of DDL — so the cap leaves generous headroom for real schemas while
// preventing a single oversized request from exhausting server memory.
const maxAPIRequestBodyBytes = 32 << 20

// limitRequestBody wraps a handler so its request body cannot exceed
// maxAPIRequestBodyBytes. Reads past the limit fail with *http.MaxBytesError,
// which writeBodyDecodeError maps to an actionable 413 response.
func (s *Service) limitRequestBody(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxAPIRequestBodyBytes)
		next(w, r)
	}
}

// ConfigureRoutes registers all HTTP routes — API and health endpoints —
// on the given mux.
// Every route is wrapped with a request body size limit so oversized
// requests are rejected instead of being buffered into memory.
func (s *Service) ConfigureRoutes(mux *http.ServeMux) {
	handle := func(pattern string, handler http.HandlerFunc) {
		mux.HandleFunc(pattern, s.limitRequestBody(handler))
	}

	// Health endpoints. /livez is process liveness (no dependency checks);
	// /health is readiness (storage-dependent). See the handler comments for
	// why the two must not be conflated.
	handle("GET /livez", s.handleLivez)
	handle("GET /health", s.handleHealth)
	handle("GET /tern-health/{deployment}/{environment}", s.handleTernHealth)

	// Config API (for CLI to discover environments)
	handle("GET /api/databases", s.handleDatabaseList)
	handle("GET /api/databases/{database}/environments", s.handleDatabaseEnvironments)

	// Orchestration API
	handle("POST /api/pull", s.handlePullSchema)
	handle("POST /api/plan", s.handlePlan)
	handle("POST /api/apply", s.handleApply)
	handle("GET /api/progress/apply/{apply_id}", s.handleProgressByApplyID)
	handle("GET /api/history/{database}", s.handleDatabaseHistory)
	handle("POST /api/cutover", s.handleCutover)
	handle("POST /api/stop", s.handleStop)
	handle("POST /api/cancel", s.handleCancel)
	handle("POST /api/start", s.handleStart)
	handle("POST /api/release", s.handleRelease)
	handle("POST /api/volume", s.handleVolume)
	handle("POST /api/revert", s.handleRevert)
	handle("POST /api/skip-revert", s.handleSkipRevert)
	handle("POST /api/rollback/plan", s.handleRollbackPlan)
	handle("GET /api/status", s.handleStatus)
	handle("GET /api/logs/{database}", s.handleLogs)
	handle("GET /api/logs", s.handleLogsWithoutDatabase)
	handle("POST /api/webhooks/redrive", s.handleWebhookRedrive)
	handle("POST /api/checks/scan", s.handleChecksScan)
	handle("POST /api/checks/synthesize", s.handleChecksSynthesize)

	// Lock API (database-level locking)
	handle("POST /api/locks/acquire", s.handleLockAcquire)
	handle("DELETE /api/locks", s.handleLockRelease)
	handle("GET /api/locks/{database}/{dbtype}", s.handleLockGet)
	handle("GET /api/locks", s.handleLockList)

	// Settings API
	handle("GET /api/settings", s.handleSettingsList)
	handle("GET /api/settings/{key}", s.handleSettingsGet)
	handle("POST /api/settings", s.handleSettingsSet)

	// GitHub webhook endpoint — registered externally via RegisterWebhook
}

// Config returns the service's server configuration.
func (s *Service) Config() *ServerConfig {
	return s.config
}

// Storage returns the service's storage instance.
// This is used by the webhook handler to store check records.
func (s *Service) Storage() storage.Storage {
	return s.storage
}

// Close closes the service and releases resources.
func (s *Service) Close() error {
	// Stop background drivers first.
	s.StopOperator()
	s.StopRemoteDeploymentHealthMonitor()

	s.ternMu.Lock()
	var errs []error
	if s.routingTernClient != nil {
		if err := s.routingTernClient.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	for _, client := range s.ternClients {
		if err := client.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	s.ternMu.Unlock()
	if err := s.storage.Close(); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("close errors: %v", errs)
	}
	return nil
}

// registerTLSConfig registers a named TLS config with the Go MySQL driver.
// Returns the config name to use in DSN parameters (tls=<name>).
func registerTLSConfig(name string, cfg *TLSConfig) (string, error) {
	if cfg.CABundle == "" {
		return "", fmt.Errorf("tls.ca_bundle is required")
	}

	caPEM, err := os.ReadFile(cfg.CABundle)
	if err != nil {
		return "", fmt.Errorf("read CA bundle %s: %w", cfg.CABundle, err)
	}
	rootPool := x509.NewCertPool()
	if !rootPool.AppendCertsFromPEM(caPEM) {
		return "", fmt.Errorf("failed to parse CA bundle %s", cfg.CABundle)
	}

	tlsCfg := &tls.Config{
		RootCAs:    rootPool,
		MinVersion: tls.VersionTLS12,
	}

	// Client certificate is optional (mTLS).
	if cfg.ClientCert != "" && cfg.ClientKey != "" {
		cert, err := tls.LoadX509KeyPair(cfg.ClientCert, cfg.ClientKey)
		if err != nil {
			return "", fmt.Errorf("load client cert/key: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	tlsName := "schemabot-" + name
	if err := gomysql.RegisterTLSConfig(tlsName, tlsCfg); err != nil {
		return "", fmt.Errorf("register TLS config %s: %w", tlsName, err)
	}
	return tlsName, nil
}
