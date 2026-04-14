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
	"syscall"
	"time"

	"github.com/block/spirit/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/secrets"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
	"github.com/block/schemabot/pkg/webhook"
)

// ServeCmd starts the SchemaBot HTTP API server.
type ServeCmd struct{}

// Run executes the serve command.
func (cmd *ServeCmd) Run(g *Globals) error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel(),
	}))

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

	// Apply storage schema using Spirit (same mechanism as LocalClient)
	// This ensures SchemaBot's storage tables exist and are up-to-date.
	logger.Info("ensuring storage schema")
	if err := api.EnsureSchema(dsn, logger); err != nil {
		return fmt.Errorf("ensure schema: %w", err)
	}

	// Connect to database
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	if err := db.PingContext(context.Background()); err != nil {
		utils.CloseAndLog(db)
		return fmt.Errorf("ping database: %w", err)
	}

	// Create service with dependencies
	storage := mysqlstore.New(db)
	svc := api.New(storage, serverConfig, nil, logger)
	defer utils.CloseAndLog(svc)

	// Start the recovery worker.
	// This unified approach polls for stale applies every 10 seconds:
	// - Runs immediately on startup
	// - Recovers applies with stale heartbeats (> 1 minute) using FOR UPDATE SKIP LOCKED
	// - STOPPED applies are NOT auto-resumed (user must call `schemabot start`)
	ctx := context.Background()
	svc.StartRecoveryWorker(ctx)

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

	// Configure routes
	mux := http.NewServeMux()
	svc.ConfigureRoutes(mux)

	// Register GitHub webhook handler if GitHub App is configured
	if serverConfig.GitHub.Configured() { //nolint:nestif
		ghPrivateKey, err := serverConfig.GitHub.ResolvePrivateKey()
		if err != nil {
			return fmt.Errorf("resolve GitHub private key: %w", err)
		}
		ghWebhookSecret, err := serverConfig.GitHub.ResolveWebhookSecret()
		if err != nil {
			return fmt.Errorf("resolve GitHub webhook secret: %w", err)
		}
		if ghWebhookSecret == "" {
			return fmt.Errorf("GitHub App is configured but webhook secret is empty — set github.webhook_secret to secure the /webhook endpoint")
		}
		appID := serverConfig.GitHub.ResolveAppID()
		ghClient := ghclient.NewClient(appID, []byte(ghPrivateKey), logger)
		webhookHandler := webhook.NewHandler(svc, ghClient, []byte(ghWebhookSecret), logger)
		mux.Handle("POST /webhook", webhookHandler)
		logger.Info("GitHub webhook endpoint registered", "app_id", appID)
	} else if serverConfig.GitHub.PrivateKey != "" {
		logger.Warn("GitHub App config found but credentials not available yet — webhook endpoint disabled")
	}

	// Create server
	server := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
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

// startGRPCServer starts a gRPC server serving the Tern proto.
// It creates a LocalClient for the first database in config using TERN_ENVIRONMENT's DSN.
func startGRPCServer(ctx context.Context, config *api.ServerConfig, st *mysqlstore.Storage, logger *slog.Logger, port string) (*grpc.Server, error) {
	env := os.Getenv("TERN_ENVIRONMENT")
	if env == "" {
		return nil, fmt.Errorf("TERN_ENVIRONMENT is required when GRPC_PORT is set")
	}

	// Find the first database in config and create a LocalClient for it
	var localClient tern.Client
	for dbName, dbConfig := range config.Databases {
		envConfig, ok := dbConfig.Environments[env]
		if !ok {
			continue
		}
		targetDSN, err := secrets.Resolve(envConfig.DSN, "")
		if err != nil {
			return nil, fmt.Errorf("resolve DSN for %s/%s: %w", dbName, env, err)
		}
		localClient, err = tern.NewLocalClient(tern.LocalConfig{
			Database:  dbName,
			Type:      dbConfig.Type,
			TargetDSN: targetDSN,
		}, st, logger)
		if err != nil {
			return nil, fmt.Errorf("create local client for %s: %w", dbName, err)
		}
		logger.Info("gRPC server using database", "database", dbName, "environment", env)
		break
	}

	if localClient == nil {
		return nil, fmt.Errorf("no database found for environment %q in config", env)
	}

	grpcSrv := grpc.NewServer()
	ternServer := tern.NewServer(localClient)
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

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
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
