// Package api provides the SchemaBot HTTP API service.
package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	"github.com/block/schemabot/pkg/secrets"
	"github.com/block/schemabot/pkg/state"
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

// Endpoint returns the Tern endpoint for the given deployment and environment.
// For single-deployment deployments, use DefaultDeployment ("default") as the deployment.
func (c TernConfig) Endpoint(deployment, environment string) (string, error) {
	if deployment == "" {
		deployment = DefaultDeployment
	}

	endpoints, ok := c[deployment]
	if !ok {
		return "", fmt.Errorf("unknown deployment: %s", deployment)
	}

	endpoint, ok := endpoints[environment]
	if !ok {
		return "", fmt.Errorf("unknown environment %q for deployment %q", environment, deployment)
	}

	if endpoint == "" {
		return "", fmt.Errorf("endpoint not configured for %s/%s", deployment, environment)
	}

	return endpoint, nil
}

// Service is the SchemaBot API service.
type Service struct {
	storage     storage.Storage
	config      *ServerConfig
	ternClients map[string]tern.Client // keyed by "deployment/environment", lazily created
	ternMu      sync.Mutex             // protects ternClients
	logger      *slog.Logger

	// Recovery loop management
	stopRecovery chan struct{}
	recoveryWg   sync.WaitGroup
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
		storage:     st,
		config:      config,
		ternClients: ternClients,
		logger:      logger,
	}
}

// TernDeployment returns the Tern deployment name for the given repository.
// In gRPC mode (TernDeployments configured), this reads the repo-specific
// mapping from server config, falling back to DefaultDeployment.
// In local mode (no TernDeployments), returns empty string so that
// resolveDeployment can derive the deployment from the database name.
func (s *Service) TernDeployment(repo string) string {
	if len(s.config.TernDeployments) == 0 {
		return ""
	}
	return s.config.TernDeployment(repo)
}

// TernClient returns the Tern client for the given deployment and environment.
// Clients are created lazily on first use, so Tern connection failures only
// affect requests to that specific deployment/environment rather than blocking
// SchemaBot startup.
// For single-deployment setups, use DefaultDeployment ("default") as the deployment.
//
// The method first checks for config-based database registration (local mode),
// then falls back to TernDeployments (gRPC mode).
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
		if envConfig, ok := dbConfig.Environments[environment]; ok {
			// Resolve target DSN (handles env:, file: prefixes)
			targetDSN, err := secrets.Resolve(envConfig.DSN, "")
			if err != nil {
				return nil, fmt.Errorf("resolve DSN for %s: %w", key, err)
			}

			// LocalClient uses SchemaBot's storage directly
			client, err := tern.NewLocalClient(tern.LocalConfig{
				Database:  deployment,
				Type:      dbConfig.Type,
				TargetDSN: targetDSN,
			}, s.storage, s.logger)
			if err != nil {
				return nil, fmt.Errorf("create local tern client for %s: %w", key, err)
			}
			s.ternClients[key] = client
			return client, nil
		}
	}

	// Fall back to gRPC mode (TernDeployments)
	address, err := s.config.TernDeployments.Endpoint(deployment, environment)
	if err != nil {
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

// ConfigureRoutes registers HTTP handlers on the given mux.
func (s *Service) ConfigureRoutes(mux *http.ServeMux) {
	// Health endpoints
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /tern-health/{deployment}/{environment}", s.handleTernHealth)

	// Config API (for CLI to discover environments)
	mux.HandleFunc("GET /api/databases/{database}/environments", s.handleDatabaseEnvironments)

	// Orchestration API
	mux.HandleFunc("POST /api/plan", s.handlePlan)
	mux.HandleFunc("POST /api/apply", s.handleApply)
	mux.HandleFunc("GET /api/progress/{database}", s.handleProgress)
	mux.HandleFunc("GET /api/progress/apply/{apply_id}", s.handleProgressByApplyID)
	mux.HandleFunc("GET /api/history/{database}", s.handleDatabaseHistory)
	mux.HandleFunc("POST /api/cutover", s.handleCutover)
	mux.HandleFunc("POST /api/stop", s.handleStop)
	mux.HandleFunc("POST /api/start", s.handleStart)
	mux.HandleFunc("POST /api/volume", s.handleVolume)
	mux.HandleFunc("POST /api/revert", s.handleRevert)
	mux.HandleFunc("POST /api/skip-revert", s.handleSkipRevert)
	mux.HandleFunc("POST /api/rollback/plan", s.handleRollbackPlan)
	mux.HandleFunc("GET /api/status", s.handleStatus)
	mux.HandleFunc("GET /api/logs/{database}", s.handleLogs)
	mux.HandleFunc("GET /api/logs", s.handleLogsWithoutDatabase)

	// Lock API (database-level locking)
	mux.HandleFunc("POST /api/locks/acquire", s.handleLockAcquire)
	mux.HandleFunc("DELETE /api/locks", s.handleLockRelease)
	mux.HandleFunc("GET /api/locks/{database}/{dbtype}", s.handleLockGet)
	mux.HandleFunc("GET /api/locks", s.handleLockList)

	// Settings API
	mux.HandleFunc("GET /api/settings", s.handleSettingsList)
	mux.HandleFunc("GET /api/settings/{key}", s.handleSettingsGet)
	mux.HandleFunc("POST /api/settings", s.handleSettingsSet)

	// GitHub webhook endpoint — registered externally via RegisterWebhook
}

// resolveApplyID translates a SchemaBot apply_identifier into the ID that the
// Tern backend expects.
//
// gRPC mode (external_id is set by ExecuteApply when Tern is remote):
//
//	caller sends "apply-abc123"
//	  → storage lookup: apply_identifier="apply-abc123", external_id="tern-42"
//	  → returns "tern-42" (Tern's ID)
//	  → GRPCClient sends ApplyId:"tern-42" to remote Tern
//
// Local mode (external_id is empty):
//
// LocalClient.Apply() creates the apply record in the same database as
// the API layer. It never sets external_id because there is no remote
// Tern — LocalClient IS the engine. ExecuteApply checks
// client.IsRemote() == false and skips generating a new identifier.
//
//	caller sends "apply-def456"
//	  → storage lookup: apply_identifier="apply-def456", external_id=""
//	  → external_id is empty, falls through to return apply_identifier
//	  → returns "apply-def456"
//	  → LocalClient receives ApplyId and scopes to that apply
func (s *Service) resolveApplyID(ctx context.Context, applyIdentifier string) (string, error) {
	if applyIdentifier == "" {
		return "", nil
	}
	applyStore := s.storage.Applies()
	if applyStore == nil {
		return applyIdentifier, nil
	}
	apply, err := applyStore.GetByApplyIdentifier(ctx, applyIdentifier)
	if err != nil {
		return "", fmt.Errorf("failed to look up apply %q: %w", applyIdentifier, err)
	}
	if apply == nil {
		return "", fmt.Errorf("apply %q not found", applyIdentifier)
	}
	if apply.ExternalID != "" {
		return apply.ExternalID, nil
	}
	return apply.ApplyIdentifier, nil
}

// findActiveApplyID finds the active (non-terminal) apply for a database/environment
// and returns the Tern-facing apply ID.
func (s *Service) findActiveApplyID(ctx context.Context, database, environment string) (string, *storage.Apply, error) {
	applyStore := s.storage.Applies()
	if applyStore == nil {
		return "", nil, nil
	}
	applies, err := applyStore.GetByDatabase(ctx, database, "", environment)
	if err != nil {
		return "", nil, fmt.Errorf("failed to look up active applies for %s/%s: %w", database, environment, err)
	}
	for _, apply := range applies {
		if !state.IsTerminalApplyState(apply.State) {
			if apply.ExternalID != "" {
				return apply.ExternalID, apply, nil
			}
			return apply.ApplyIdentifier, apply, nil
		}
	}
	return "", nil, nil
}

// Storage returns the service's storage instance.
// This is used by the webhook handler to store check records.
func (s *Service) Storage() storage.Storage {
	return s.storage
}

// Close closes the service and releases resources.
func (s *Service) Close() error {
	// Stop the background recovery worker first
	s.StopRecoveryWorker()

	s.ternMu.Lock()
	var errs []error
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
