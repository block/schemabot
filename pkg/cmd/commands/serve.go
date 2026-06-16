package commands

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"sort"
	"strings"
	"syscall"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/block/spirit/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/auth"
	"github.com/block/schemabot/pkg/awscreds"
	"github.com/block/schemabot/pkg/etre"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/inventory"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/schemabot/pkg/secrets"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
	"github.com/block/schemabot/pkg/webhook"
)

type webhookRuntime struct {
	handler                         http.Handler
	reconcileMissingSummaryComments func(context.Context)
}

func (r webhookRuntime) StartMissingSummaryReconciliation(ctx context.Context, logger *slog.Logger) {
	if r.reconcileMissingSummaryComments == nil {
		logger.Debug("missing summary reconciliation disabled")
		return
	}

	reconcileCtx := context.WithoutCancel(ctx)
	go func() {
		r.reconcileMissingSummaryComments(reconcileCtx)
	}()
}

// ServeCmd starts the SchemaBot HTTP API server.
type ServeCmd struct{}

// Run executes the serve command.
func (cmd *ServeCmd) Run(g *Globals) error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel(),
	})).With("schemabot_version", g.Version)
	slog.SetDefault(logger)

	// Load server configuration from YAML file
	serverConfig, err := api.LoadServerConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// Get storage DSN from config (with fallback to MYSQL_DSN env var)
	dsn, err := serverConfig.StorageDSN()
	if err != nil {
		return fmt.Errorf("resolve storage DSN: %w", err)
	}
	if dsn == "" {
		return fmt.Errorf("storage DSN not configured (set storage.dsn in config or MYSQL_DSN env var)")
	}

	port := getEnv("PORT", "8080")

	// Apply storage schema with retries for transient failures (e.g., DNS
	// not yet available when the container starts in Kubernetes).
	logger.Info("ensuring storage schema")
	var db *sql.DB
	const maxRetries = 5
	const pingTimeout = 10 * time.Second
	for attempt := range maxRetries {
		if err := api.EnsureSchema(dsn, logger); err != nil {
			if attempt < maxRetries-1 {
				logger.Warn("ensure schema failed, retrying", "attempt", attempt+1, "error", err)
				time.Sleep(2 * time.Second)
				continue
			}
			return fmt.Errorf("ensure schema after %d attempts: %w", maxRetries, err)
		}

		db, err = mysqlconn.Open(dsn)
		if err != nil {
			return fmt.Errorf("open database: %w", err)
		}
		pingCtx, pingCancel := context.WithTimeout(context.Background(), pingTimeout)
		pingErr := db.PingContext(pingCtx)
		pingCancel()
		if pingErr != nil {
			utils.CloseAndLog(db)
			if attempt < maxRetries-1 {
				logger.Warn("database ping failed, retrying", "attempt", attempt+1, "error", pingErr)
				time.Sleep(2 * time.Second)
				continue
			}
			return fmt.Errorf("ping database after %d attempts: %w", maxRetries, pingErr)
		}
		break
	}

	// Proactively discard idle connections before MySQL's wait_timeout (default 28800s)
	// to avoid "invalid connection" errors when the pool hands out stale connections.
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(3 * time.Minute)

	// Log config summary for debugging
	logger.Info("config loaded",
		"databases", len(serverConfig.Databases),
		"tern_deployments", len(serverConfig.TernDeployments),
		"repos", len(serverConfig.Repos),
		"allowed_environments", serverConfig.AllowedEnvironments,
		"respond_to_unscoped", serverConfig.ShouldRespondToUnscoped(),
	)
	for name, db := range serverConfig.Databases {
		envs := make([]string, 0, len(db.Environments))
		for env := range db.Environments {
			envs = append(envs, env)
		}
		logger.Info("registered database", "name", name, "type", db.Type, "environments", envs)
	}

	// Create service with dependencies
	storage := mysqlstore.New(db)
	svc := api.New(storage, serverConfig, nil, logger)
	defer utils.CloseAndLog(svc)

	ctx := context.Background()

	// Build the webhook runtime before recovery starts so recovered applies can
	// attach PR comment observers. If GitHub is not configured, the runtime
	// serves a disabled webhook endpoint and skips comment reconciliation.
	webhookRuntime, err := buildWebhookRuntime(serverConfig, svc, logger)
	if err != nil {
		return err
	}

	// On startup, find applies that have a progress comment but no summary
	// comment. This means terminal comment handling was interrupted; reconcile
	// in the background so GitHub repair does not block server startup.
	webhookRuntime.StartMissingSummaryReconciliation(ctx, logger)

	// Start the operator worker pool after webhook callbacks are registered.
	// This polls for apply work every 10 seconds:
	// - Runs immediately on startup
	// - Dispatches queued local applies
	// - Recovers applies with stale heartbeats (> 1 minute) using FOR UPDATE SKIP LOCKED
	// - STOPPED applies are NOT auto-resumed (user must call `schemabot start`)
	svc.StartOperator(ctx)

	// Optionally start gRPC server for Tern proto (used by docker-compose.grpc.yml)
	var grpcServer *grpc.Server
	grpcPort := os.Getenv("GRPC_PORT")
	if grpcPort != "" {
		grpcServer, err = startGRPCServer(ctx, serverConfig, storage, logger, grpcPort)
		if err != nil {
			return fmt.Errorf("start grpc server: %w", err)
		}
		defer grpcServer.GracefulStop()
	}

	// Initialize telemetry (OTel metrics via Prometheus /metrics endpoint)
	telemetry, err := api.SetupTelemetry(logger)
	if err != nil {
		return fmt.Errorf("setup telemetry: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := telemetry.Shutdown(shutdownCtx); err != nil {
			logger.Error("telemetry shutdown failed", "error", err)
		}
	}()

	// Emit steady-state availability metrics for every configured remote Tern
	// deployment so dashboards can show deployment-specific health even when no
	// schema changes are running.
	svc.StartRemoteDeploymentHealthMonitor(ctx)
	defer svc.StopRemoteDeploymentHealthMonitor()

	// Permanently drop expired quarantined tables from local-mode MySQL
	// databases once their pending drops retention period has passed.
	svc.StartPendingDropsCleaner(ctx)
	defer svc.StopPendingDropsCleaner()

	// Configure routes
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)
	mux.Handle("GET /metrics", telemetry.MetricsHandler)

	mux.Handle("POST /webhook", webhookRuntime.handler)

	// Authentication middleware. With the default (none) auth this is an
	// allow-all NoneAuthorizer that lets every request through (attaching an
	// anonymous user); with "oidc" it validates Bearer JWTs and bypasses
	// non-API paths (/webhook, /metrics, health) itself.
	authz, err := buildAuthorizer(ctx, serverConfig.Auth, logger)
	if err != nil {
		return fmt.Errorf("setup auth: %w", err)
	}
	authedHandler := authz.Middleware(mux)

	// Wrap with OTel HTTP instrumentation for automatic request duration,
	// request body size, and response body size metrics.
	metricHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		labeler, _ := otelhttp.LabelerFromContext(r.Context())
		labeler.Add(metrics.EnvironmentAttribute(""))
		authedHandler.ServeHTTP(w, r)
	})
	handler := otelhttp.NewHandler(metricHandler, "schemabot")

	// Create server
	server := &http.Server{
		Addr:         ":" + port,
		Handler:      handler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in goroutine
	errCh := make(chan error, 1)
	go func() {
		logger.Info("starting server", "port", port, "version", g.Version, "commit", g.Commit, "built", g.Date)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		logger.Info("received shutdown signal", "signal", sig)
	case err := <-errCh:
		return err
	}

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger.Info("shutting down server")
	return server.Shutdown(ctx)
}

// startGRPCServer starts a gRPC server serving the Tern proto. When the data
// plane is configured with a target resolver, requests are routed by opaque
// execution target through a TargetRouter; otherwise it falls back to a single
// LocalClient bound to the one database configured for TERN_ENVIRONMENT.
func startGRPCServer(ctx context.Context, config *api.ServerConfig, st *mysqlstore.Storage, logger *slog.Logger, port string) (*grpc.Server, error) {
	client, err := buildGRPCTernClient(ctx, config, st, logger, os.Getenv("TERN_ENVIRONMENT"))
	if err != nil {
		return nil, err
	}

	grpcSrv := grpc.NewServer()
	ternServer := tern.NewServer(client)
	ternServer.Register(grpcSrv)

	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", ":"+port)
	if err != nil {
		return nil, fmt.Errorf("listen on port %s: %w", port, err)
	}

	go func() {
		logger.Info("starting gRPC server", "port", port)
		if err := grpcSrv.Serve(listener); err != nil {
			logger.Error("gRPC server error", "error", err)
		}
	}()

	return grpcSrv, nil
}

// buildGRPCTernClient builds the tern.Client backing the data-plane gRPC server.
// When a target resolver is configured, it returns a TargetRouter that resolves
// each request's opaque target to a connection per request; the server-level
// environment is unused in this mode because each request carries its own.
// Otherwise it falls back to a single LocalClient bound to the one database
// configured for env.
func buildGRPCTernClient(ctx context.Context, config *api.ServerConfig, st *mysqlstore.Storage, logger *slog.Logger, env string) (tern.Client, error) {
	etreConfigured := config.TargetResolver.Etre.Configured()
	staticConfigured := config.TargetResolver.Configured()

	if etreConfigured && staticConfigured {
		return nil, fmt.Errorf("target_resolver configures both etre and static targets; per-target overrides are not yet supported — use one")
	}

	if etreConfigured || staticConfigured {
		resolver, err := buildTargetResolver(ctx, config.TargetResolver, logger)
		if err != nil {
			return nil, err
		}
		router, err := tern.NewTargetRouter(tern.TargetRouterConfig{
			Resolver:           resolver,
			Storage:            st,
			Logger:             logger,
			LocalClientFactory: grpcLocalClientFactory(config),
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
	client, err := grpcLocalClientFactory(config)(tern.LocalConfig{
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

// buildTargetResolver builds the configured inventory.Resolver — the Etre
// dynamic backend or the static inventory. The caller guarantees exactly one is
// configured.
func buildTargetResolver(ctx context.Context, cfg api.TargetResolverConfig, logger *slog.Logger) (inventory.Resolver, error) {
	if cfg.Etre.Configured() {
		resolver, err := buildEtreResolver(ctx, cfg.Etre, logger)
		if err != nil {
			return nil, fmt.Errorf("build etre resolver: %w", err)
		}
		logger.Info("gRPC server routing by etre resolver",
			"entity_type", cfg.Etre.EntityType, "target_label", cfg.Etre.TargetLabel,
			"credentials", credentialType(cfg.Etre.Credentials))
		return resolver, nil
	}

	resolver, err := inventory.NewStaticResolver(cfg.StaticInventory())
	if err != nil {
		return nil, fmt.Errorf("build static target resolver: %w", err)
	}
	logger.Info("gRPC server routing by static target resolver", "targets", len(cfg.Targets))
	return resolver, nil
}

// buildEtreResolver assembles the Etre-backed MySQL resolver from config: the
// Etre query client, the namespace-free MySQL connection assembler, and the
// configured credential resolver. Lazily-validated fields (host, credentials)
// are checked here so a misconfiguration fails at startup, not first request.
func buildEtreResolver(ctx context.Context, cfg api.EtreConfig, logger *slog.Logger) (inventory.Resolver, error) {
	if cfg.HostField == "" {
		return nil, fmt.Errorf("target_resolver.etre.host_field is required")
	}
	creds, err := buildCredentialResolver(ctx, cfg.Credentials)
	if err != nil {
		return nil, err
	}
	addr, err := secrets.Resolve(cfg.Addr, "")
	if err != nil {
		return nil, fmt.Errorf("resolve target_resolver.etre.addr: %w", err)
	}
	// A secret ref (env:/file:/secretsmanager:) can resolve to "" without error;
	// surface that as a clear config error rather than a generic downstream one.
	if addr == "" {
		return nil, fmt.Errorf("target_resolver.etre.addr resolved to an empty value")
	}
	client, err := etre.New(etre.Config{Addr: addr, EntityType: cfg.EntityType, Logger: logger})
	if err != nil {
		return nil, fmt.Errorf("build etre client: %w", err)
	}
	return etre.NewEtreResolver(etre.EtreResolverConfig{
		Client:          client,
		TargetLabel:     cfg.TargetLabel,
		Labels:          cfg.Labels,
		EnvLabel:        cfg.EnvLabel,
		HostField:       cfg.HostField,
		AttributeFields: credentialAttributeFields(cfg),
		Credentials:     creds,
		Assembler:       inventory.MySQLConnectionAssembler{DefaultPort: cfg.DefaultPort},
	})
}

const (
	credentialTypeSecretRef = "secret_ref"
	credentialTypeAWSSM     = "awssm"
)

// credentialType returns the configured credential backend, defaulting to
// secret_ref.
func credentialType(cfg api.EtreCredentialsConfig) string {
	if cfg.Type == "" {
		return credentialTypeSecretRef
	}
	return cfg.Type
}

// buildCredentialResolver builds the configured credential backend. Each backend
// is one inventory.CredentialResolver implementation; the data plane is not
// coupled to any single secret store.
func buildCredentialResolver(ctx context.Context, cfg api.EtreCredentialsConfig) (inventory.CredentialResolver, error) {
	switch credentialType(cfg) {
	case credentialTypeSecretRef:
		if cfg.Username == "" {
			return nil, fmt.Errorf("target_resolver.etre.credentials.username is required")
		}
		if cfg.PasswordRef == "" {
			return nil, fmt.Errorf("target_resolver.etre.credentials.password_ref is required")
		}
		return inventory.SecretRefCredentialResolver{Username: cfg.Username, PasswordRef: cfg.PasswordRef}, nil

	case credentialTypeAWSSM:
		// Validate required fields with config-path context before loading AWS
		// config, so a misconfiguration fails fast and actionably instead of
		// after (potentially slow) credential-chain resolution.
		switch {
		case cfg.Region == "":
			return nil, fmt.Errorf("target_resolver.etre.credentials.region is required for the awssm backend")
		case cfg.RoleARN == "":
			return nil, fmt.Errorf("target_resolver.etre.credentials.role_arn is required for the awssm backend")
		case cfg.SecretName == "":
			return nil, fmt.Errorf("target_resolver.etre.credentials.secret_name is required for the awssm backend")
		}
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("load AWS config for target_resolver.etre.credentials: %w", err)
		}
		resolver, err := awscreds.New(awscreds.Config{
			AWSConfig:        awsCfg,
			Region:           cfg.Region,
			RoleARN:          cfg.RoleARN,
			ExternalID:       cfg.ExternalID,
			SecretName:       cfg.SecretName,
			AccountAttribute: cfg.AccountAttribute,
		})
		if err != nil {
			return nil, fmt.Errorf("build target_resolver.etre.credentials awssm resolver: %w", err)
		}
		return resolver, nil

	default:
		return nil, fmt.Errorf("unknown target_resolver.etre.credentials.type %q (want %q or %q)", cfg.Type, credentialTypeSecretRef, credentialTypeAWSSM)
	}
}

// credentialAttributeFields returns the entity attribute fields the resolver
// must surface, ensuring the assume-role backend's account attribute is included
// alongside any explicitly configured fields.
func credentialAttributeFields(cfg api.EtreConfig) []string {
	fields := cfg.AttributeFields
	if credentialType(cfg.Credentials) != credentialTypeAWSSM {
		return fields
	}
	accountAttr := cfg.Credentials.AccountAttribute
	if accountAttr == "" {
		accountAttr = "aws_account_id"
	}
	if slices.Contains(fields, accountAttr) {
		return fields
	}
	return append(append([]string(nil), fields...), accountAttr)
}

// grpcLocalClientFactory returns a LocalClientFactory that applies server-level
// policy (pending drops) to every LocalClient the data plane builds, so the
// router and single-database paths share identical execution semantics.
func grpcLocalClientFactory(config *api.ServerConfig) tern.LocalClientFactory {
	pendingDropsDisabled := !config.PendingDropsEnabled()
	return func(cfg tern.LocalConfig, st storage.Storage, logger *slog.Logger) (tern.Client, error) {
		if pendingDropsDisabled {
			if cfg.Metadata == nil {
				cfg.Metadata = map[string]string{}
			}
			cfg.Metadata["pending_drops"] = "false"
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
				logger.Error("failed to write disabled webhook response", "error", err)
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

	appID := serverConfig.GitHub.ResolveAppID()
	ghClient := ghclient.NewClient(appID, []byte(ghPrivateKey), logger,
		ghclient.WithTrustedCheckAppSlugs(serverConfig.GitHub.TrustedCheckAppSlugs))
	handler := webhook.NewHandler(svc, ghClient, []byte(ghWebhookSecret), logger)
	logger.Info("GitHub webhook endpoint registered",
		"app_id", appID, "trusted_check_app_slugs", serverConfig.GitHub.TrustedCheckAppSlugs)
	return webhookRuntime{
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
			ghclient.WithTrustedCheckAppSlugs(e.cfg.TrustedCheckAppSlugs))
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
	)
	logger.Info("GitHub multi-App webhook endpoint registered", "apps", len(serverConfig.Apps))
	return webhookRuntime{
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

// buildAuthorizer selects the API authorizer from config. The default
// allow-all NoneAuthorizer lets every request through (with an anonymous user
// in context); "oidc" validates Bearer JWTs against the issuer's JWKS. Unknown
// types are rejected so a misconfigured auth type fails closed at startup
// rather than silently disabling auth.
func buildAuthorizer(ctx context.Context, cfg api.AuthConfig, logger *slog.Logger) (auth.Authorizer, error) {
	switch cfg.Type {
	case "", "none":
		logger.Info("API authentication disabled — all requests allowed")
		return auth.NoneAuthorizer{}, nil
	case "oidc":
		logger.Info("initializing OIDC authentication", "issuer", cfg.Issuer)
		authz, err := auth.NewOIDCAuthorizer(ctx, auth.OIDCConfig{
			Issuer:      cfg.Issuer,
			Audience:    cfg.Audience,
			GroupsClaim: cfg.GroupsClaim,
		}, logger)
		if err != nil {
			return nil, err
		}
		logger.Info("OIDC authentication enabled", "issuer", cfg.Issuer)
		return authz, nil
	default:
		return nil, fmt.Errorf("auth type %q is not yet supported", cfg.Type)
	}
}

func logLevel() slog.Level {
	switch os.Getenv("LOG_LEVEL") {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
