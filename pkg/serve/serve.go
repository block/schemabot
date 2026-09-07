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
	"github.com/block/schemabot/pkg/engine/planetscale"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/schemabot/pkg/panicsafe"
	"github.com/block/schemabot/pkg/postgresconn"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/storage/postgresstore"
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
// gracefully. The storage DSN is resolved from cfg (falling back to the
// STORAGE_DSN env var, then MYSQL_DSN);
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

	// Optionally start a gRPC server for the Tern proto (used by
	// docker-compose.grpc.yml). Embedders attach to their own server instead.
	if grpcPort != "" {
		grpcServer := newTernGRPCServer(srv.logger)
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
	storage         storage.Storage
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

// registerPlanetScaleMTLS registers the configured planetscale.mtls
// certificates with the Go MySQL driver, making every MySQL connection the
// Vitess engine opens (branch hosts and vtgates) present the client identity.
// Unreadable or unparseable certificate material is a hard error so the
// caller fails startup rather than running without the identity the
// endpoints require.
func registerPlanetScaleMTLS(cfg *api.ServerConfig, logger *slog.Logger) error {
	mtls := cfg.PlanetScale.MTLS
	if mtls == nil {
		logger.Debug("planetscale.mtls not configured; Vitess engine MySQL connections use no TLS")
		return nil
	}
	if err := planetscale.RegisterMTLS(planetscale.MTLSConfig{
		CABundlePath:   mtls.CABundle,
		ClientCertPath: mtls.ClientCert,
		ClientKeyPath:  mtls.ClientKey,
	}); err != nil {
		return fmt.Errorf("register PlanetScale mTLS from planetscale.mtls config: %w", err)
	}
	logger.Info("registered PlanetScale mTLS for Vitess engine MySQL connections",
		"ca_bundle", mtls.CABundle,
		"client_cert", mtls.ClientCert,
		"client_key", mtls.ClientKey)
	return nil
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

	// Register PlanetScale mTLS before anything else so a worker with
	// missing or unreadable certificate material fails startup immediately
	// instead of failing every Vitess connection it later opens.
	if err := registerPlanetScaleMTLS(cfg, logger); err != nil {
		return nil, err
	}

	// The postgres ceiling only surfaces at apply time (a refusal names it;
	// an attempt below it names nothing), so state the effective value once
	// at startup where operators can correlate it across pods and config
	// revisions.
	logger.Info("PostgreSQL engine native-safe table size ceiling in effect",
		"limit_bytes", cfg.Postgres.NativeSafeTableSizeLimit(),
		"configured", cfg.Postgres.NativeSafeTableSizeLimitBytes != nil)

	// Get storage DSN from config (with fallback to the STORAGE_DSN env var,
	// then MYSQL_DSN)
	dsn, err := cfg.StorageDSN()
	if err != nil {
		return nil, fmt.Errorf("resolve storage DSN: %w", err)
	}
	if dsn == "" {
		return nil, fmt.Errorf("storage DSN not configured (set storage.dsn in config or the STORAGE_DSN env var)")
	}

	// The storage dialect routes schema bootstrapping, the connection pool,
	// and the storage implementation together, so every layer of the storage
	// stack agrees on the database family. Unknown dialects fail startup here.
	dialect, err := cfg.Storage.ResolveDialect()
	if err != nil {
		return nil, fmt.Errorf("resolve storage dialect: %w", err)
	}

	// Storage boot is patient: failures here are expected transients — DNS may
	// not resolve yet when the container starts, and during a credential
	// rotation every new connection is rejected until the mounted secret
	// catches up with the database password. Retrying inside the startup-probe
	// budget lets the pod wait the window out instead of crash-looping
	// through it.
	logger.Info("ensuring storage schema", "dialect", dialect)
	db, err := bootStorage(ctx, cfg, dialect, logger)
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

	// Apply the storage connection-pool settings. Each knob is configurable via
	// storage.pool; the *OrDefault helpers supply the defaults (and their
	// rationale) from the api package. MaxOpenConns is left unset when zero so
	// the pool stays unbounded, matching database/sql's default.
	pool := cfg.Storage.Pool
	db.SetConnMaxLifetime(pool.ConnMaxLifetimeOrDefault())
	db.SetConnMaxIdleTime(pool.ConnMaxIdleTimeOrDefault())
	db.SetMaxIdleConns(pool.MaxIdleConnsOrDefault())
	if pool.MaxOpenConns > 0 {
		db.SetMaxOpenConns(pool.MaxOpenConns)
	}

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
	store, err := newStore(dialect, db, storage.WithMaxDriversPerApply(cfg.MaxDriversPerApply))
	if err != nil {
		return nil, err
	}
	svc := api.New(store, cfg, nil, logger)
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
		dataPlaneClient, err = buildGRPCTernClient(ctx, cfg, store, logger, os.Getenv("TERN_ENVIRONMENT"), o.engines, svc.WakeOperator)
		if err != nil {
			return nil, fmt.Errorf("build data-plane target router: %w", err)
		}
		svc.SetDefaultTernClient(dataPlaneClient)
	}

	// Authentication middleware. With the default (none) auth this is an
	// allow-all NoneAuthorizer that lets every request through (attaching an
	// anonymous user); with "oidc" it validates Bearer JWTs and bypasses
	// non-API paths (/webhook, health) itself.
	authz, err := buildServerAuthorizer(ctx, cfg, logger)
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
		storage:         store,
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
func bootStorage(ctx context.Context, cfg *api.ServerConfig, dialect schema.Dialect, logger *slog.Logger) (*sql.DB, error) {
	deadline := time.Now().Add(storageBootRetryBudget)
	for attempt := 1; ; attempt++ {
		db, err := connectStorage(ctx, cfg, dialect, logger)
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
func connectStorage(ctx context.Context, cfg *api.ServerConfig, dialect schema.Dialect, logger *slog.Logger) (*sql.DB, error) {
	const pingTimeout = 10 * time.Second
	dsn, err := cfg.StorageDSN()
	if err != nil {
		return nil, fmt.Errorf("resolve storage DSN: %w", err)
	}
	if err := api.EnsureSchema(dsn, logger,
		api.WithAllowDestructiveSchemaChanges(cfg.Storage.AllowDestructiveSchemaChanges),
		api.WithPostgresStatementTimeout(cfg.Postgres.StatementTimeoutOrDefault()),
		api.WithDialect(dialect)); err != nil {
		return nil, fmt.Errorf("ensure storage schema: %w", err)
	}
	db, err := openStoragePool(dialect, dsn, cfg)
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

// openStoragePool opens the long-lived reloadable storage pool for the
// configured dialect. Both connectors re-resolve the DSN through
// cfg.StorageDSN on authentication failure so a rotated storage credential
// is picked up without a restart. The dispatch fails closed: a dialect
// without a connector returns an error instead of dialing with another
// family's driver.
func openStoragePool(dialect schema.Dialect, dsn string, cfg *api.ServerConfig) (*sql.DB, error) {
	connectTimeout := cfg.Storage.Pool.ConnectTimeoutOrZero()
	switch dialect {
	case schema.DialectMySQL:
		return mysqlconn.OpenReloadable(dsn, cfg.StorageDSN,
			mysqlconn.WithConnectTimeout(connectTimeout))
	case schema.DialectPostgres:
		// The storage pool carries a statement budget of its own so steady-state
		// storage queries run under a value SchemaBot states rather than
		// whatever the platform imposed at the role or database level. It is
		// the ordinary-query budget, not the bootstrap's DDL budget: this pool
		// never executes DDL.
		return postgresconn.OpenReloadable(dsn, cfg.StorageDSN,
			postgresconn.WithConnectTimeout(connectTimeout),
			postgresconn.WithStatementTimeout(cfg.Postgres.StatementTimeoutOrDefault()))
	default:
		return nil, fmt.Errorf("no storage connector for storage dialect %q (supported: %q, %q)", dialect, schema.DialectMySQL, schema.DialectPostgres)
	}
}

// newStore returns the storage implementation for the configured storage
// dialect. The dispatch fails closed: a dialect without a store returns an
// error instead of running the MySQL implementation against another database
// family.
func newStore(dialect schema.Dialect, db *sql.DB, opts ...storage.Option) (storage.Storage, error) {
	switch dialect {
	case schema.DialectMySQL:
		return mysqlstore.New(db, opts...), nil
	case schema.DialectPostgres:
		return postgresstore.New(db, opts...), nil
	default:
		return nil, fmt.Errorf("no storage implementation for storage dialect %q (supported: %q, %q)", dialect, schema.DialectMySQL, schema.DialectPostgres)
	}
}

// Service returns the underlying API service for embedders that need direct
// access (for example to register additional routes or inspect state).
func (s *Server) Service() *api.Service { return s.svc }

// RegisterGRPC registers the Tern gRPC service on the embedder's server. Call it
// before the server starts serving. When a target resolver is configured the
// shared data-plane client is reused; otherwise a single-database client bound
// to TERN_ENVIRONMENT is built.
//
// Registration installs no panic containment: interceptors cannot be attached
// to an already-constructed grpc.Server, so a handler panic (for example in
// Stop or Cutover) kills the embedding process — the one that drives applies —
// unless the embedder installed RecoveryUnaryInterceptor and
// RecoveryStreamInterceptor when constructing gs. Install them first in
// grpc.ChainUnaryInterceptor / grpc.ChainStreamInterceptor, and never combine
// them with the non-chained grpc.UnaryInterceptor / grpc.StreamInterceptor
// options, which grpc-go runs ahead of the entire chain. The server that Run
// serves installs both by construction.
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
	tern.NewServer(client, s.logger).Register(gs)
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
	s.svc.StartOperatorStuckPendingMonitor(ctx)
	s.svc.StartPendingDropsCleaner(ctx)
}

// Close releases the resources the Server owns and returns all cleanup errors
// encountered, joined together. It stops the pending-drops cleaner, stops the
// operator (before closing the gRPC client it built, see below), shuts down
// telemetry (best-effort: flush failures are logged, not returned), closes
// that gRPC fallback client, and closes the service. svc.Close
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
	// Telemetry shutdown is best-effort: a failure here means the final
	// metrics/traces flush was dropped (e.g. the collector is unreachable or
	// rejects the export), which is not worth failing the close over —
	// embedders treat a Close error as a fatal exit, so returning it would
	// turn a routine SIGTERM rotation into a reported crash.
	if err := s.telemetry.Shutdown(shutdownCtx); err != nil {
		s.logger.Warn("telemetry shutdown failed; final metrics and traces were not exported", "error", err)
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
func buildGRPCTernClient(ctx context.Context, config *api.ServerConfig, st storage.Storage, logger *slog.Logger, env string, engineFactories map[string]tern.EngineFactory, wakeOperator ...func(applyIdentifier, database, environment string)) (tern.Client, error) {
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
	envConfig := dbConfig.Environments[env]
	targetDSN, err := envConfig.ResolveDSN()
	if err != nil {
		return nil, fmt.Errorf("resolve DSN for %s/%s: %w", dbName, env, err)
	}
	metadata, err := envConfig.DirectExecution.EngineMetadata()
	if err != nil {
		return nil, fmt.Errorf("resolve direct_execution metadata for %s/%s: %w", dbName, env, err)
	}
	client, err := grpcLocalClientFactory(config, wake, engineFactories)(tern.LocalConfig{
		Database:  dbName,
		Type:      dbConfig.Type,
		TargetDSN: targetDSN,
		Metadata:  metadata,
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
	pendingDrops := strconv.FormatBool(config.PendingDropsEnabled())
	return func(cfg tern.LocalConfig, st storage.Storage, logger *slog.Logger) (tern.Client, error) {
		spiritMetadata, err := config.SpiritMetadata()
		if err != nil {
			return nil, fmt.Errorf("resolve spirit config for database %q: %w", cfg.Database, err)
		}
		if cfg.Metadata == nil {
			cfg.Metadata = map[string]string{}
		}
		cfg.PostgresNativeSafeTableSizeLimitBytes = config.Postgres.NativeSafeTableSizeLimit()
		// Stated either way rather than only when disabled: a data plane that
		// predates the opt-in default reads an absent key as "quarantine", so
		// leaving it out during a rolling deploy would quarantine on a
		// deployment that has turned the quarantine off.
		cfg.Metadata["pending_drops"] = pendingDrops
		// Server-level spirit overrides are defaults; a database's own
		// metadata entry for the same key wins.
		for key, value := range spiritMetadata {
			if _, ok := cfg.Metadata[key]; !ok {
				cfg.Metadata[key] = value
			}
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
	handlerOpts := append([]webhook.HandlerOption{
		webhook.WithRepoWebhookSecret([]byte(repoWebhookSecret)),
		webhook.WithDurableWebhookDispatch(),
		webhook.WithWebhookReconciler(),
	}, webhookReconcileSynthesisOptions(logger)...)
	handlerOpts = append(handlerOpts, checkSuiteRecoveryOptions(logger)...)
	handler := webhook.NewHandler(svc, ghClient, []byte(ghWebhookSecret), logger, handlerOpts...)
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

	handlerOpts := append([]webhook.HandlerOption{
		webhook.WithDurableWebhookDispatch(),
		webhook.WithWebhookReconciler(),
	}, webhookReconcileSynthesisOptions(logger)...)
	handlerOpts = append(handlerOpts, checkSuiteRecoveryOptions(logger)...)
	handler := webhook.NewHandlerWithDispatch(
		svc,
		ghclient.NewClientSet(clients),
		secretsByApp,
		appByID,
		logger,
		handlerOpts...,
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

// webhookReconcileSynthesisOptions returns the handler option enabling the
// reconciler's missing-head synthesis, unless the operator disabled it with
// WEBHOOK_RECONCILE_SYNTHESIS=false. The kill switch drops the reconciler
// back to a report-only missing-delivery scan with a restart instead of a
// code revert; the missing-delivery log and metric keep flowing either
// way, so detection is unaffected. An unparseable value also disables
// synthesis: the only reason to set the variable is to turn synthesis off,
// so a malformed value ("off", "disabled") is treated as intent to disable —
// an operator reaching for a kill switch mid-incident must get the
// switched-off behavior, not a warning in pod logs they are not watching.
func webhookReconcileSynthesisOptions(logger *slog.Logger) []webhook.HandlerOption {
	if value := os.Getenv("WEBHOOK_RECONCILE_SYNTHESIS"); value != "" {
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			logger.Error("invalid WEBHOOK_RECONCILE_SYNTHESIS value; webhook reconcile synthesis disabled (fail-safe) — missing-delivery scan will run report-only",
				"value", value, "error", err)
			return nil
		}
		if !enabled {
			logger.Info("webhook reconcile synthesis disabled by WEBHOOK_RECONCILE_SYNTHESIS; missing-delivery scan will run report-only")
			return nil
		}
	}
	return []webhook.HandlerOption{webhook.WithWebhookReconcileSynthesis()}
}

// checkSuiteRecoveryOptions returns the handler option enabling durable
// check_suite.requested recovery, unless the operator disabled it with
// WEBHOOK_CHECK_SUITE_RECOVERY=false. The kill switch makes the webhook
// endpoint acknowledge and ignore check_suite deliveries with a restart
// instead of a code revert; the reconciler's missing-head scan remains the
// recovery backstop either way. An unparseable value also disables recovery:
// the only reason to set the variable is to turn recovery off, so a malformed
// value ("off", "disabled") is treated as intent to disable — an operator
// reaching for a kill switch mid-incident must get the switched-off behavior,
// not a warning in pod logs they are not watching.
func checkSuiteRecoveryOptions(logger *slog.Logger) []webhook.HandlerOption {
	if value := os.Getenv("WEBHOOK_CHECK_SUITE_RECOVERY"); value != "" {
		enabled, err := strconv.ParseBool(value)
		if err != nil {
			logger.Error("invalid WEBHOOK_CHECK_SUITE_RECOVERY value; check-suite recovery disabled (fail-safe) — lost auto-plan deliveries recover only via the reconciler",
				"value", value, "error", err)
			return nil
		}
		if !enabled {
			logger.Info("check-suite recovery disabled by WEBHOOK_CHECK_SUITE_RECOVERY; lost auto-plan deliveries recover only via the reconciler")
			return nil
		}
	}
	return []webhook.HandlerOption{webhook.WithCheckSuiteRecovery()}
}

// buildServerAuthorizer constructs the API authorizer exactly as the server
// wires it: admin teams from PR command authorization, and the operator-group
// union that widens forward-auth write admission. Every server build and any
// test that claims to exercise the real authorizer wiring must go through
// this function, so the union cannot be dropped from one without the other.
func buildServerAuthorizer(ctx context.Context, cfg *api.ServerConfig, logger *slog.Logger) (auth.Authorizer, error) {
	return buildAuthorizer(ctx, cfg.Auth, cfg.PRCommandAuthorization.AdminTeams, cfg.OperatorGroupUnion(), logger)
}

// buildAuthorizer selects the API authorizer from config. The default
// allow-all NoneAuthorizer lets every request through (with an anonymous user
// in context); "oidc" validates Bearer JWTs against the issuer's JWKS. Unknown
// types are rejected so a misconfigured auth type fails closed at startup
// rather than silently disabling auth.
// operatorGroups is the union of every database's operator_groups; forward-auth
// admits its members to the write tier, with per-database enforcement in the
// handlers.
func buildAuthorizer(ctx context.Context, cfg api.AuthConfig, adminGroups, operatorGroups []string, logger *slog.Logger) (auth.Authorizer, error) {
	switch cfg.Type {
	case "", "none":
		logger.Info("API authentication disabled — all requests allowed; write operations will be logged and counted so unauthenticated mutating traffic stays visible")
		return auth.NoneAuthorizer{Logger: logger}, nil
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
			logger.Warn("OIDC authentication enabled with no admin groups configured: every write-tier operation, plan included, will be denied; read-tier operations still work. Set pr_command_authorization.admin_teams to allow writes.")
		}
		logger.Info("OIDC authentication enabled", "issuer", cfg.Issuer)
		return authz, nil
	case "forward_auth":
		fa := cfg.ForwardAuth
		logger.Info("initializing forward-auth authentication",
			"trusted_proxy_cidrs", len(fa.TrustedProxyCIDRs),
			"trusted_proxy_spiffe", len(fa.TrustedProxySPIFFE),
			"read_groups", len(fa.ReadGroups),
			"write_groups", len(fa.WriteGroups),
			"operator_groups", len(operatorGroups),
			"trusted_gateway_spiffe", len(fa.TrustedGatewaySPIFFE),
			"read_service_spiffe", len(fa.ReadServiceSPIFFE))
		authz, err := auth.NewForwardAuthAuthorizer(auth.ForwardAuthConfig{
			UserHeader:           fa.UserHeader,
			GroupsHeader:         fa.GroupsHeader,
			GroupsDelimiter:      fa.GroupsDelimiter,
			TrustedProxySPIFFE:   fa.TrustedProxySPIFFE,
			TrustedProxyCIDRs:    fa.TrustedProxyCIDRs,
			ReadGroups:           fa.ReadGroups,
			WriteGroups:          fa.WriteGroups,
			OperatorGroups:       operatorGroups,
			TrustedGatewaySPIFFE: fa.TrustedGatewaySPIFFE,
			CallerSPIFFEHeader:   fa.CallerSPIFFEHeader,
			ReadServiceSPIFFE:    fa.ReadServiceSPIFFE,
		}, logger)
		if err != nil {
			return nil, err
		}
		switch {
		case len(fa.WriteGroups) == 0 && len(operatorGroups) == 0:
			logger.Warn("forward-auth enabled with no write groups configured: all write operations will be denied (read still works). Set auth.forward_auth.write_groups to allow writes.")
		case len(fa.WriteGroups) == 0:
			logger.Warn("forward-auth enabled with no write groups configured: only per-database operator writes will be allowed; operations with no target database (settings, checks, redrive) will be denied. Set auth.forward_auth.write_groups to allow admin writes.")
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
