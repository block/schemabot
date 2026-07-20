// Package serve runs the SchemaBot server. It exposes Run as an embeddable
// entrypoint so the server can be started from the CLI or from another process
// that supplies its own ServerConfig — the CLI command is a thin wrapper that
// loads configuration and calls Run.
package serve

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/block/spirit/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/auth"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/schemabot/pkg/panicsafe"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
	"github.com/block/schemabot/pkg/webhook"
)

// Option configures Run.
type Option func(*options)

type options struct {
	logger  *slog.Logger
	version string
	commit  string
	date    string
	engines map[string]tern.EngineFactory
}

// WithLogger sets the logger Run uses. A nil logger is ignored so Run keeps
// slog.Default(); when unset, Run uses slog.Default().
func WithLogger(logger *slog.Logger) Option {
	return func(o *options) {
		if logger != nil {
			o.logger = logger
		}
	}
}

// WithEngine registers an Engine factory for a database type this build does not
// provide natively, so an embedder (e.g. a data plane that consumes SchemaBot as
// a module) can supply an engine without the core depending on its package. Run
// registers it on the service (so the operator's in-process clients use it) and
// threads it into the data-plane client factory (so the gRPC/router path uses it
// too). Inputs are validated when Run registers them, failing startup on a bad
// type or nil factory. Registering the same type twice keeps the last factory.
func WithEngine(databaseType string, factory tern.EngineFactory) Option {
	return func(o *options) {
		if o.engines == nil {
			o.engines = make(map[string]tern.EngineFactory)
		}
		o.engines[databaseType] = factory
	}
}

// WithBuildInfo sets the build identifiers logged on startup.
func WithBuildInfo(version, commit, date string) Option {
	return func(o *options) {
		o.version = version
		o.commit = commit
		o.date = date
	}
}

type webhookRuntime struct {
	handler                         http.Handler
	startDurableWebhookDispatch     func(context.Context)
	stopDurableWebhookDispatch      func()
	drainInProcessWebhookWork       func(context.Context)
	reconcileMissingSummaryComments func(context.Context)
}

func (r webhookRuntime) StartMissingSummaryReconciliation(ctx context.Context, logger *slog.Logger) {
	if r.reconcileMissingSummaryComments == nil {
		logger.Debug("missing summary reconciliation disabled")
		return
	}

	reconcileCtx := context.WithoutCancel(ctx)
	go func() {
		// The reconcile pass renders GitHub comments from stored apply state; a
		// panic on one poisoned row must degrade only this startup pass, not
		// kill the process that serves webhooks and drives applies.
		err := panicsafe.Call(func() error {
			r.reconcileMissingSummaryComments(reconcileCtx)
			return nil
		})
		if err == nil {
			return
		}
		var reconcilePanic *panicsafe.Error
		if !errors.As(err, &reconcilePanic) {
			// The reconcile callback returns nothing, so only a contained panic
			// reaches here today; keep the signal if that invariant changes.
			logger.Error("missing-summary reconciliation failed", "error", err)
			return
		}
		logger.Error("missing-summary reconciliation panicked; missing summary comments will not be reconciled until the next restart",
			"panic", fmt.Sprint(reconcilePanic.Value),
			"stack", string(reconcilePanic.Stack))
		metrics.RecordRecoveredPanic(reconcileCtx, "summary_reconciliation")
	}()
}

// Run starts the SchemaBot server with the given configuration and blocks until
// it receives SIGINT/SIGTERM or either HTTP server fails, then shuts down
// gracefully. The storage DSN is resolved from cfg (falling back to MYSQL_DSN);
// PORT and GRPC_PORT are read from the environment. Prometheus metrics are
// served on a dedicated listener at cfg.MetricsListenPort, not on the API port.
func Run(ctx context.Context, cfg *api.ServerConfig, opts ...Option) error {
	port := getEnv("PORT", "8080")
	grpcPort := os.Getenv("GRPC_PORT")

	srv, err := Build(ctx, cfg, opts...)
	if err != nil {
		return err
	}
	defer utils.CloseAndLog(srv)

	// A configured GRPC_PORT means this process serves the data plane, whose
	// LocalClient drives claim at the apply level — so default operator claiming
	// to the apply level (unless config set it explicitly). This is a Run
	// convenience, not a server mode: an embedder sets
	// ServerConfig.OperatorClaimOperations directly. Applied before Start
	// (which launches the operator) so the operator reads the resolved value.
	if applyDataPlaneClaimDefault(cfg, grpcPort != "") {
		srv.logger.Info("data-plane gRPC mode: defaulting operator claiming to the apply level", "grpc_port", grpcPort)
	} else if grpcPort != "" && cfg.ShouldClaimOperations() {
		srv.logger.Warn("data-plane gRPC mode has operation-level operator claiming enabled; data-plane drives hold apply-level leases, so stop/start resume will not recover", "grpc_port", grpcPort)
	}

	// Optionally start a gRPC server for the Tern proto (used by
	// docker-compose.grpc.yml). Embedders attach to their own server instead.
	if grpcPort != "" {
		grpcServer := grpc.NewServer()
		if err := srv.RegisterGRPC(ctx, grpcServer); err != nil {
			return fmt.Errorf("register grpc tern service: %w", err)
		}
		var lc net.ListenConfig
		listener, err := lc.Listen(ctx, "tcp", ":"+grpcPort)
		if err != nil {
			return fmt.Errorf("listen on port %s: %w", grpcPort, err)
		}
		go func() {
			srv.logger.Info("starting gRPC server", "port", grpcPort)
			// Serve returns ErrServerStopped on GracefulStop during normal
			// shutdown; that is expected, not an error worth alerting on.
			if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				srv.logger.Error("gRPC server error", "port", grpcPort, "error", err)
			}
		}()
		defer grpcServer.GracefulStop()
	}

	// Start background loops (operator, health monitor, pending-drops cleaner,
	// missing-summary reconciliation). Server.Close stops them.
	srv.Start(ctx)

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      srv.Handler(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Metrics get their own listener so scrapers never traverse the API port
	// (see ServerConfig.MetricsPort).
	metricsPort := strconv.Itoa(cfg.MetricsListenPort())
	metricsMux := http.NewServeMux()
	metricsMux.Handle("GET /metrics", srv.MetricsHandler())
	metricsServer := &http.Server{
		Addr:         ":" + metricsPort,
		Handler:      metricsMux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	errCh := make(chan error, 2)
	go func() {
		srv.logger.Info("starting http server", "port", port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server: %w", err)
		}
	}()
	go func() {
		srv.logger.Info("starting metrics server", "port", metricsPort)
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("metrics server: %w", err)
		}
	}()

	// Wait for a shutdown signal, context cancellation (embedded callers), or a
	// fatal server error.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case sig := <-sigCh:
		srv.logger.Info("received shutdown signal", "signal", sig)
	case <-ctx.Done():
		srv.logger.Info("context canceled, shutting down", "error", ctx.Err())
	case err := <-errCh:
		return err
	}

	// Graceful shutdown of both HTTP servers; Server.Close (deferred) releases
	// the rest.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv.logger.Info("shutting down server")
	return errors.Join(server.Shutdown(shutdownCtx), metricsServer.Shutdown(shutdownCtx))
}

// Server is a built but not-yet-listening SchemaBot server. Build constructs it
// from a ServerConfig; an embedder attaches it to its own gRPC server and HTTP
// mux (RegisterGRPC, Handler), starts its background loops (Start), and releases
// its resources (Close). Run wires the same Server to its own listeners. This is
// the embedding seam: a data plane consuming SchemaBot as a module configures it
// entirely through ServerConfig rather than reimplementing this wiring.
type Server struct {
	cfg             *api.ServerConfig
	svc             *api.Service
	storage         *mysqlstore.Storage
	logger          *slog.Logger
	dataPlaneClient tern.Client
	// grpcClient is the single-database client RegisterGRPC builds when no
	// target resolver is configured. It is owned here (not by the service) so
	// Close releases it; the resolver-backed dataPlaneClient is the service's
	// default client and is closed by svc.Close.
	grpcClient tern.Client
	webhook    webhookRuntime
	telemetry  *api.Telemetry
	authz      auth.Authorizer
	engines    map[string]tern.EngineFactory
}

// Build constructs a SchemaBot server from cfg without opening any listener. It
// resolves and migrates storage, constructs the service, registers
// embedder-supplied engines, builds the webhook runtime and (when a target
// resolver is configured) the shared data-plane client, sets up authentication
// and telemetry, and returns a Server. The caller wires it to a transport
// (RegisterGRPC / Handler), starts background work (Start), and releases
// resources (Close). Run is Build plus SchemaBot's own gRPC/HTTP listeners.
func Build(ctx context.Context, cfg *api.ServerConfig, opts ...Option) (*Server, error) {
	o := options{logger: slog.Default()}
	for _, opt := range opts {
		opt(&o)
	}
	logger := o.logger
	logger.Info("building server", "version", o.version, "commit", o.commit, "built", o.date)

	// Get storage DSN from config (with fallback to MYSQL_DSN env var)
	dsn, err := cfg.StorageDSN()
	if err != nil {
		return nil, fmt.Errorf("resolve storage DSN: %w", err)
	}
	if dsn == "" {
		return nil, fmt.Errorf("storage DSN not configured (set storage.dsn in config or MYSQL_DSN env var)")
	}

	// Storage boot is patient: failures here are expected transients — DNS may
	// not resolve yet when the container starts, and during a credential
	// rotation every new connection is rejected until the mounted secret
	// catches up with the database password. Retrying inside the startup-probe
	// budget lets the pod wait the window out instead of crash-looping
	// through it.
	logger.Info("ensuring storage schema")
	db, err := bootStorage(ctx, cfg, logger)
	if err != nil {
		return nil, err
	}

	// On any error past this point, close the resources Build has opened so a
	// failed Build leaks neither the pool nor the service.
	success := false
	defer func() {
		if !success {
			utils.CloseAndLog(db)
		}
	}()

	// Proactively discard idle connections before MySQL's wait_timeout (default 28800s)
	// to avoid "invalid connection" errors when the pool hands out stale connections.
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(3 * time.Minute)

	// Log config summary for debugging
	logger.Info("config loaded",
		"databases", len(cfg.Databases),
		"tern_deployments", len(cfg.TernDeployments),
		"repos", len(cfg.Repos),
		"allowed_environments", cfg.AllowedEnvironments,
		"respond_to_unscoped", cfg.ShouldRespondToUnscoped(),
	)
	for name, dbCfg := range cfg.Databases {
		envs := make([]string, 0, len(dbCfg.Environments))
		for env := range dbCfg.Environments {
			envs = append(envs, env)
		}
		logger.Info("registered database", "name", name, "type", dbCfg.Type, "environments", envs)
	}

	// Create service with dependencies
	storage := mysqlstore.New(db)
	svc := api.New(storage, cfg, nil, logger)
	defer func() {
		if !success {
			utils.CloseAndLog(svc)
		}
	}()

	// Register embedder-supplied engines before any client is built or the
	// operator starts, so both the operator's in-process clients (via the
	// service) and the data-plane gRPC/router clients can resolve custom
	// database types. Validation lives in RegisterEngine, so a bad type or nil
	// factory fails startup here.
	for databaseType, factory := range o.engines {
		// RegisterEngine's error already names the operation and the database
		// type, so return it as-is rather than double-prefixing.
		if err := svc.RegisterEngine(databaseType, factory); err != nil {
			return nil, err
		}
		logger.Info("registered engine", "database_type", databaseType)
	}

	// Build the webhook runtime before the operator starts so recovered applies
	// can attach PR comment observers. If GitHub is not configured, the runtime
	// serves a disabled webhook endpoint and skips comment reconciliation.
	webhookRuntime, err := buildWebhookRuntime(cfg, svc, logger)
	if err != nil {
		return nil, err
	}

	// When a dynamic target resolver is configured, build the data-plane client (a
	// TargetRouter that resolves each request's target to a connection) and set it
	// as the operator's default client, so the operator resumes durable applies by
	// resolving their target — not just statically-configured deployments. The
	// gRPC transport reuses the same instance.
	var dataPlaneClient tern.Client
	if cfg.TargetResolver.Enabled() {
		dataPlaneClient, err = buildGRPCTernClient(ctx, cfg, storage, logger, os.Getenv("TERN_ENVIRONMENT"), o.engines, svc.WakeOperator)
		if err != nil {
			return nil, fmt.Errorf("build data-plane target router: %w", err)
		}
		svc.SetDefaultTernClient(dataPlaneClient)
	}

	// Authentication middleware. With the default (none) auth this is an
	// allow-all NoneAuthorizer that lets every request through (attaching an
	// anonymous user); with "oidc" it validates Bearer JWTs and bypasses
	// non-API paths (/webhook, health) itself.
	authz, err := buildAuthorizer(ctx, cfg.Auth, cfg.PRCommandAuthorization.AdminTeams, logger)
	if err != nil {
		return nil, fmt.Errorf("setup auth: %w", err)
	}

	// Initialize telemetry (OTel metrics via Prometheus /metrics endpoint).
	telemetry, err := api.SetupTelemetry(logger)
	if err != nil {
		return nil, fmt.Errorf("setup telemetry: %w", err)
	}

	success = true
	return &Server{
		cfg:             cfg,
		svc:             svc,
		storage:         storage,
		logger:          logger,
		dataPlaneClient: dataPlaneClient,
		webhook:         webhookRuntime,
		telemetry:       telemetry,
		authz:           authz,
		engines:         o.engines,
	}, nil
}

// Storage boot retry policy. The budget is sized so that even a final attempt
// that runs to the schema-ensure timeout still finishes inside the
// deployment's startup-probe budget: the HTTP listener (and with it /livez)
// only starts after boot completes, so the startup probe is what bounds a pod
// whose storage never becomes reachable.
const (
	storageBootRetryBudget   = 8 * time.Minute
	storageBootRetryInterval = 5 * time.Second
)

// inProcessWebhookDrainTimeout bounds how long Close waits for detached
// in-process webhook goroutines to finish before giving up and letting the
// process exit. It sits within the deployment's overall shutdown grace period.
const inProcessWebhookDrainTimeout = 25 * time.Second

// bootStorage brings up the storage database for a booting server: it applies
// the storage schema, opens the pool, and verifies connectivity, retrying
// failed attempts until the boot budget is spent. The DSN is re-resolved on
// every attempt so file-backed references pick up credentials rotated while
// the server waits.
func bootStorage(ctx context.Context, cfg *api.ServerConfig, logger *slog.Logger) (*sql.DB, error) {
	deadline := time.Now().Add(storageBootRetryBudget)
	for attempt := 1; ; attempt++ {
		db, err := connectStorage(ctx, cfg, logger)
		if err == nil {
			return db, nil
		}
		if time.Until(deadline) < storageBootRetryInterval {
			return nil, fmt.Errorf("storage not ready after %d attempts over %s: %w", attempt, storageBootRetryBudget, err)
		}
		logger.Warn("storage not ready, retrying",
			"attempt", attempt,
			"retry_in", storageBootRetryInterval,
			"budget_remaining", time.Until(deadline).Round(time.Second),
			"error", err)
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("storage boot canceled after %d attempts: %w", attempt, ctx.Err())
		case <-time.After(storageBootRetryInterval):
		}
	}
}

// connectStorage runs a single storage boot attempt: resolve the DSN, apply
// the storage schema, open the pool, and verify it with a ping.
func connectStorage(ctx context.Context, cfg *api.ServerConfig, logger *slog.Logger) (*sql.DB, error) {
	const pingTimeout = 10 * time.Second
	dsn, err := cfg.StorageDSN()
	if err != nil {
		return nil, fmt.Errorf("resolve storage DSN: %w", err)
	}
	if err := api.EnsureSchema(dsn, logger, api.WithAllowDestructiveSchemaChanges(cfg.Storage.AllowDestructiveSchemaChanges)); err != nil {
		return nil, fmt.Errorf("ensure storage schema: %w", err)
	}
	db, err := mysqlconn.OpenReloadable(dsn, cfg.StorageDSN)
	if err != nil {
		return nil, fmt.Errorf("open storage database: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		utils.CloseAndLog(db)
		return nil, fmt.Errorf("ping storage database: %w", err)
	}
	return db, nil
}

// Service returns the underlying API service for embedders that need direct
// access (for example to register additional routes or inspect state).
func (s *Server) Service() *api.Service { return s.svc }

// RegisterGRPC registers the Tern gRPC service on the embedder's server. Call it
// before the server starts serving. When a target resolver is configured the
// shared data-plane client is reused; otherwise a single-database client bound
// to TERN_ENVIRONMENT is built.
func (s *Server) RegisterGRPC(ctx context.Context, gs *grpc.Server) error {
	client := s.dataPlaneClient
	if client == nil {
		built, err := buildGRPCTernClient(ctx, s.cfg, s.storage, s.logger, os.Getenv("TERN_ENVIRONMENT"), s.engines, s.svc.WakeOperator)
		if err != nil {
			return fmt.Errorf("build grpc tern client: %w", err)
		}
		// Owned by the Server (Close releases it; the service does not close its
		// default client).
		s.grpcClient = built
		// A dispatched apply is queued for this data plane's own operator, which
		// routes each claim by the apply's deployment/environment. A dispatch can
		// carry an environment the static database config does not list, so the
		// operator must fall back to the same client the gRPC transport serves —
		// without it, every claim of a queued apply would fail "tern deployment
		// not configured" and the apply would sit re-claimable forever.
		s.svc.SetDefaultTernClient(built)
		client = built
	}
	tern.NewServer(client).Register(gs)
	return nil
}

// Handler returns the SchemaBot HTTP handler: API routes, the webhook endpoint,
// the auth middleware, and OTel instrumentation. An embedder mounts it on its
// own server; Run serves it directly. Prometheus metrics are not part of this
// handler — mount MetricsHandler on a dedicated listener instead.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	s.svc.ConfigureRoutes(mux)
	mux.Handle("POST /webhook", s.webhook.handler)

	authedHandler := s.authz.Middleware(mux)

	// Wrap with OTel HTTP instrumentation for automatic request duration,
	// request body size, and response body size metrics.
	metricHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		labeler, _ := otelhttp.LabelerFromContext(r.Context())
		labeler.Add(metrics.EnvironmentAttribute(""))
		authedHandler.ServeHTTP(w, r)
	})
	return otelhttp.NewHandler(metricHandler, "schemabot")
}

// MetricsHandler returns the Prometheus /metrics handler. Run serves it on the
// dedicated metrics listener (ServerConfig.MetricsPort); an embedder that owns
// its listeners mounts it wherever its scraper expects it.
func (s *Server) MetricsHandler() http.Handler {
	return s.telemetry.MetricsHandler
}

// Start launches the server's background work: the operator driver pool
// (dispatches queued applies and recovers stale ones), the remote-deployment
// health monitor, the webhook inbox monitor (emits durable-inbox depth/backlog
// metrics), and the pending-drops cleaner — all of which run until ctx is
// canceled or Close is called. It also kicks off a one-shot missing-summary
// reconciliation that, once started, runs to completion independently of ctx (it
// repairs interrupted terminal comments and must not be cut short by a request
// context); it runs before the operator so recovered applies attach observers
// first.
func (s *Server) Start(ctx context.Context) {
	s.webhook.StartMissingSummaryReconciliation(ctx, s.logger)
	if s.webhook.startDurableWebhookDispatch != nil {
		s.webhook.startDurableWebhookDispatch(ctx)
	}
	s.svc.StartOperator(ctx)
	s.svc.StartRemoteDeploymentHealthMonitor(ctx)
	s.svc.StartWebhookInboxMonitor(ctx)
	s.svc.StartPendingDropsCleaner(ctx)
}

// Close releases the resources the Server owns and returns all cleanup errors
// encountered, joined together. It stops the pending-drops cleaner, stops the
// operator (before closing the gRPC client it built, see below), shuts down
// telemetry, closes that gRPC fallback client, and closes the service. svc.Close
// stops the health monitor and closes the service's clients and storage (the
// database pool); it repeats StopOperator, which is idempotent, so that is a
// no-op. It does not stop any gRPC server the embedder owns. Safe to call once
// after Start.
func (s *Server) Close() error {
	s.svc.StopPendingDropsCleaner()
	if s.webhook.stopDurableWebhookDispatch != nil {
		s.webhook.stopDurableWebhookDispatch()
	}
	// Drain the detached in-process webhook goroutines (non-durable event types)
	// before closing storage below, since that already-acked work can still read
	// or write the database. Run/embedders stop the HTTP server before Close, so
	// no new deliveries arrive during the drain.
	if s.webhook.drainInProcessWebhookWork != nil {
		drainCtx, cancelDrain := context.WithTimeout(context.Background(), inProcessWebhookDrainTimeout)
		s.webhook.drainInProcessWebhookWork(drainCtx)
		cancelDrain()
	}
	// Stop the operator before closing the gRPC client below: RegisterGRPC set
	// that client as the service's default, so until the drivers drain, a claim
	// of a queued apply can route to it — closing it first would hand a
	// shutdown-window drive a closed client. StopOperator waits for in-flight
	// drivers and is idempotent, so svc.Close repeating it is a no-op.
	s.svc.StopOperator()

	var errs []error
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.telemetry.Shutdown(shutdownCtx); err != nil {
		errs = append(errs, fmt.Errorf("telemetry shutdown: %w", err))
	}
	if s.grpcClient != nil {
		if err := s.grpcClient.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close grpc client: %w", err))
		}
	}
	if err := s.svc.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close service: %w", err))
	}
	if len(errs) > 0 {
		return fmt.Errorf("close server: %w", errors.Join(errs...))
	}
	return nil
}

// buildGRPCTernClient builds the tern.Client backing the data-plane gRPC server.
// When a target resolver is configured, it returns a TargetRouter that resolves
// each request's opaque target to a connection per request; the server-level
// environment is unused in this mode because each request carries its own.
// Otherwise it falls back to a single LocalClient bound to the one database
// configured for env.
func buildGRPCTernClient(ctx context.Context, config *api.ServerConfig, st *mysqlstore.Storage, logger *slog.Logger, env string, engineFactories map[string]tern.EngineFactory, wakeOperator ...func(applyIdentifier, database, environment string)) (tern.Client, error) {
	var wake func(applyIdentifier, database, environment string)
	if len(wakeOperator) > 0 {
		wake = wakeOperator[0]
	}
	if config.TargetResolver.Enabled() {
		resolver, err := config.TargetResolver.BuildResolver(ctx, logger)
		if err != nil {
			return nil, err
		}
		router, err := tern.NewTargetRouter(tern.TargetRouterConfig{
			Resolver:           resolver,
			Storage:            st,
			Logger:             logger,
			LocalClientFactory: grpcLocalClientFactory(config, wake, engineFactories),
		})
		if err != nil {
			return nil, fmt.Errorf("build target router: %w", err)
		}
		return router, nil
	}

	// Single-database fallback selects the one database in config with a local
	// DSN for env. It requires an environment to select against.
	if env == "" {
		return nil, fmt.Errorf("TERN_ENVIRONMENT is required for single-database gRPC mode; set it or configure target_resolver")
	}

	// A single LocalClient serves exactly one database, so selection must be
	// deterministic and unambiguous. More than one match is a configuration
	// error rather than a nondeterministic pick over map iteration order.
	var matches []string
	for dbName, dbConfig := range config.Databases {
		envConfig, ok := dbConfig.Environments[env]
		if !ok || !envConfig.HasLocalDSN() {
			continue
		}
		matches = append(matches, dbName)
	}
	sort.Strings(matches)
	switch len(matches) {
	case 0:
		return nil, fmt.Errorf("no database with a local DSN found for environment %q in config", env)
	case 1:
		// Exactly one database matches — serve it below.
	default:
		return nil, fmt.Errorf("environment %q matches %d databases with local DSNs (%s); single-database gRPC mode serves one database — configure target_resolver to route multiple", env, len(matches), strings.Join(matches, ", "))
	}

	dbName := matches[0]
	dbConfig := config.Databases[dbName]
	targetDSN, err := dbConfig.Environments[env].ResolveDSN()
	if err != nil {
		return nil, fmt.Errorf("resolve DSN for %s/%s: %w", dbName, env, err)
	}
	client, err := grpcLocalClientFactory(config, wake, engineFactories)(tern.LocalConfig{
		Database:  dbName,
		Type:      dbConfig.Type,
		TargetDSN: targetDSN,
	}, st, logger)
	if err != nil {
		return nil, fmt.Errorf("create local client for %s: %w", dbName, err)
	}
	logger.Info("gRPC server using database", "database", dbName, "environment", env)
	return client, nil
}

// grpcLocalClientFactory returns a LocalClientFactory that applies server-level
// policy (pending drops) and the embedder-supplied engine factories to every
// LocalClient the data plane builds, so the router and single-database paths
// share identical execution semantics and can resolve custom database types.
func grpcLocalClientFactory(config *api.ServerConfig, wakeOperator func(applyIdentifier, database, environment string), engineFactories map[string]tern.EngineFactory) tern.LocalClientFactory {
	pendingDropsDisabled := !config.PendingDropsEnabled()
	return func(cfg tern.LocalConfig, st storage.Storage, logger *slog.Logger) (tern.Client, error) {
		if pendingDropsDisabled {
			if cfg.Metadata == nil {
				cfg.Metadata = map[string]string{}
			}
			cfg.Metadata["pending_drops"] = "false"
		}
		if cfg.WakeOperator == nil {
			cfg.WakeOperator = wakeOperator
		}
		// Merge the embedder registry into this config so custom types always
		// resolve, regardless of whether the resolved config already carries
		// factories. Build a fresh map so the caller's is never mutated, and let
		// any per-config entry win over the server-level registration.
		if len(engineFactories) > 0 {
			merged := make(map[string]tern.EngineFactory, len(engineFactories)+len(cfg.EngineFactories))
			maps.Copy(merged, engineFactories)
			maps.Copy(merged, cfg.EngineFactories)
			cfg.EngineFactories = merged
		}
		return tern.NewLocalClient(cfg, st, logger)
	}
}

func buildWebhookRuntime(serverConfig *api.ServerConfig, svc *api.Service, logger *slog.Logger) (webhookRuntime, error) {
	if len(serverConfig.Apps) > 0 {
		return buildMultiAppWebhookRuntime(serverConfig, svc, logger)
	}
	return buildSingleAppWebhookRuntime(serverConfig, svc, logger)
}

func buildSingleAppWebhookRuntime(serverConfig *api.ServerConfig, svc *api.Service, logger *slog.Logger) (webhookRuntime, error) {
	if !serverConfig.GitHub.Configured() {
		if serverConfig.GitHub.PrivateKey != "" {
			logger.Warn("GitHub App config found but credentials not available yet — webhook endpoint disabled")
		}
		return webhookRuntime{handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			if _, err := w.Write([]byte(`{"error":"GitHub App credentials not available — webhook endpoint is disabled"}`)); err != nil {
				logger.Error("failed to write disabled webhook response", "method", r.Method, "path", r.URL.Path, "error", err)
			}
		})}, nil
	}

	ghPrivateKey, err := serverConfig.GitHub.ResolvePrivateKey()
	if err != nil {
		return webhookRuntime{}, fmt.Errorf("resolve GitHub private key: %w", err)
	}
	ghWebhookSecret, err := serverConfig.GitHub.ResolveWebhookSecret()
	if err != nil {
		return webhookRuntime{}, fmt.Errorf("resolve GitHub webhook secret: %w", err)
	}
	if ghWebhookSecret == "" {
		return webhookRuntime{}, fmt.Errorf("GitHub App is configured but webhook secret is empty — set github.webhook-secret to secure the /webhook endpoint")
	}

	repoWebhookSecret, err := serverConfig.GitHub.ResolveRepoWebhookSecret()
	if err != nil {
		return webhookRuntime{}, fmt.Errorf("resolve GitHub repo-webhook secret: %w", err)
	}

	appID := serverConfig.GitHub.ResolveAppID()
	ghClient := ghclient.NewClient(appID, []byte(ghPrivateKey), logger,
		ghclient.WithTrustedCheckAppSlugs(serverConfig.GitHub.TrustedCheckAppSlugs),
		ghclient.WithConfigDirHints(serverConfig))
	handler := webhook.NewHandler(svc, ghClient, []byte(ghWebhookSecret), logger,
		webhook.WithRepoWebhookSecret([]byte(repoWebhookSecret)),
		webhook.WithDurableWebhookDispatch(),
		webhook.WithWebhookReconciler())
	svc.SetCheckRunBackfiller(handler)
	logger.Info("GitHub webhook endpoint registered",
		"app_id", appID, "trusted_check_app_slugs", serverConfig.GitHub.TrustedCheckAppSlugs,
		"repo_webhook_dispatch", repoWebhookSecret != "")
	return webhookRuntime{
		startDurableWebhookDispatch:     handler.StartDurableWebhookDispatch,
		stopDurableWebhookDispatch:      handler.StopDurableWebhookDispatch,
		drainInProcessWebhookWork:       handler.DrainInProcessWebhookWork,
		handler:                         handler,
		reconcileMissingSummaryComments: handler.ReconcileMissingSummaryComments,
	}, nil
}

// buildMultiAppWebhookRuntime constructs a webhook handler that dispatches
// inbound deliveries across multiple GitHub Apps. App-ID resolution and
// duplicate detection are delegated to ServerConfig.ResolveGitHubAppsByID
// so app-id validation has a single source of truth; this function then
// resolves the remaining per-App credentials (private key, webhook secret)
// and assembles the dispatch tables and ClientSet. Any resolution error
// fails startup so a misconfigured multi-App deployment never serves the
// webhook endpoint.
func buildMultiAppWebhookRuntime(serverConfig *api.ServerConfig, svc *api.Service, logger *slog.Logger) (webhookRuntime, error) {
	appsByID, err := serverConfig.ResolveGitHubAppsByID()
	if err != nil {
		return webhookRuntime{}, fmt.Errorf("resolve GitHub Apps: %w", err)
	}

	clients := make(map[string]ghclient.GitHubClientFactory, len(appsByID))
	secretsByApp := make(map[string][]byte, len(appsByID))
	appByID := make(map[int64]string, len(appsByID))

	// Iterate App names in sorted order so startup log output is
	// deterministic across restarts.
	type appEntry struct {
		id   int64
		name string
		cfg  api.GitHubAppConfig
	}
	entries := make([]appEntry, 0, len(appsByID))
	for id, app := range appsByID {
		entries = append(entries, appEntry{id: id, name: app.Name, cfg: app.Config})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].name < entries[j].name })

	for _, e := range entries {
		privateKey, err := e.cfg.ResolvePrivateKey()
		if err != nil {
			return webhookRuntime{}, fmt.Errorf("resolve private key for app %q: %w", e.name, err)
		}
		if privateKey == "" {
			return webhookRuntime{}, fmt.Errorf("app %q private key resolved to empty value", e.name)
		}

		secret, err := e.cfg.ResolveWebhookSecret()
		if err != nil {
			return webhookRuntime{}, fmt.Errorf("resolve webhook secret for app %q: %w", e.name, err)
		}
		if secret == "" {
			return webhookRuntime{}, fmt.Errorf("app %q webhook secret resolved to empty value", e.name)
		}

		clients[e.name] = ghclient.NewClient(e.id, []byte(privateKey), logger,
			ghclient.WithTrustedCheckAppSlugs(e.cfg.TrustedCheckAppSlugs),
			ghclient.WithConfigDirHints(serverConfig))
		secretsByApp[e.name] = []byte(secret)
		appByID[e.id] = e.name

		logger.Info("registered GitHub App",
			"app_name", e.name, "app_id", e.id, "trusted_check_app_slugs", e.cfg.TrustedCheckAppSlugs)
	}

	handler := webhook.NewHandlerWithDispatch(
		svc,
		ghclient.NewClientSet(clients),
		secretsByApp,
		appByID,
		logger,
		webhook.WithDurableWebhookDispatch(),
		webhook.WithWebhookReconciler(),
	)
	svc.SetCheckRunBackfiller(handler)
	logger.Info("GitHub multi-App webhook endpoint registered", "apps", len(serverConfig.Apps))
	return webhookRuntime{
		startDurableWebhookDispatch:     handler.StartDurableWebhookDispatch,
		stopDurableWebhookDispatch:      handler.StopDurableWebhookDispatch,
		drainInProcessWebhookWork:       handler.DrainInProcessWebhookWork,
		handler:                         handler,
		reconcileMissingSummaryComments: handler.ReconcileMissingSummaryComments,
	}, nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// applyDataPlaneClaimDefault sets the operator claim mode for a data-plane tern
// process. A process serving the Tern proto over gRPC drives applies inline via
// LocalClient and does not own the apply_operations lifecycle, so its operator
// must recover work at the apply level via FindNextApply; operation-level
// claiming reads a vestigial operation row that never tracks the apply and so
// cannot resume it. When the mode is unset, default it to apply-level claiming
// for this process; an operator can still opt in explicitly. Operation-level
// claiming is a control-plane concept. Reports whether the default was applied.
func applyDataPlaneClaimDefault(cfg *api.ServerConfig, isDataPlane bool) bool {
	if !isDataPlane || cfg.OperatorClaimOperations != nil {
		return false
	}
	applyLevel := false
	cfg.OperatorClaimOperations = &applyLevel
	return true
}

// buildAuthorizer selects the API authorizer from config. The default
// allow-all NoneAuthorizer lets every request through (with an anonymous user
// in context); "oidc" validates Bearer JWTs against the issuer's JWKS. Unknown
// types are rejected so a misconfigured auth type fails closed at startup
// rather than silently disabling auth.
func buildAuthorizer(ctx context.Context, cfg api.AuthConfig, adminGroups []string, logger *slog.Logger) (auth.Authorizer, error) {
	switch cfg.Type {
	case "", "none":
		logger.Info("API authentication disabled — all requests allowed")
		return auth.NoneAuthorizer{}, nil
	case "oidc":
		logger.Info("initializing OIDC authentication", "issuer", cfg.Issuer, "admin_groups", len(adminGroups))
		authz, err := auth.NewOIDCAuthorizer(ctx, auth.OIDCConfig{
			Issuer:      cfg.Issuer,
			Audience:    cfg.Audience,
			GroupsClaim: cfg.GroupsClaim,
			AdminGroups: adminGroups,
		}, logger)
		if err != nil {
			return nil, err
		}
		if len(adminGroups) == 0 {
			logger.Warn("OIDC authentication enabled with no admin groups configured: all write operations will be denied (read and plan still work). Set pr_command_authorization.admin_teams to allow writes.")
		}
		logger.Info("OIDC authentication enabled", "issuer", cfg.Issuer)
		return authz, nil
	case "forward_auth":
		fa := cfg.ForwardAuth
		logger.Info("initializing forward-auth authentication",
			"trusted_proxy_cidrs", len(fa.TrustedProxyCIDRs),
			"trusted_proxy_spiffe", len(fa.TrustedProxySPIFFE),
			"read_groups", len(fa.ReadGroups),
			"write_groups", len(fa.WriteGroups))
		authz, err := auth.NewForwardAuthAuthorizer(auth.ForwardAuthConfig{
			UserHeader:         fa.UserHeader,
			GroupsHeader:       fa.GroupsHeader,
			GroupsDelimiter:    fa.GroupsDelimiter,
			TrustedProxySPIFFE: fa.TrustedProxySPIFFE,
			TrustedProxyCIDRs:  fa.TrustedProxyCIDRs,
			ReadGroups:         fa.ReadGroups,
			WriteGroups:        fa.WriteGroups,
		}, logger)
		if err != nil {
			return nil, err
		}
		if len(fa.WriteGroups) == 0 {
			logger.Warn("forward-auth enabled with no write groups configured: all write operations will be denied (read still works). Set auth.forward_auth.write_groups to allow writes.")
		}
		if len(fa.ReadGroups) == 0 {
			logger.Info("forward-auth enabled with no read groups configured: read operations are open to any authenticated caller from the trusted proxy. Set auth.forward_auth.read_groups to restrict reads.")
		}
		logger.Info("forward-auth authentication enabled")
		return authz, nil
	default:
		return nil, fmt.Errorf("auth type %q is not yet supported", cfg.Type)
	}
}
