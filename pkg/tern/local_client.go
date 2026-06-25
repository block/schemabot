package tern

// Client Architecture - Two Integration Patterns
//
// The tern package provides two Client implementations (LocalClient, GRPCClient)
// for two deployment patterns. SchemaBot always maintains its own storage layer
// (locks, plans, applies, tasks, etc.) regardless of which client is used.
//
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │                        INTEGRATION PATTERNS                                 │
// ├─────────────────────────────────────────────────────────────────────────────┤
// │  1. Local Mode   │ LocalClient  │ SchemaBot Storage + Spirit Engine direct │
// │  2. gRPC Mode    │ GRPCClient   │ External Tern service (or e2e tests)      │
// └─────────────────────────────────────────────────────────────────────────────┘
//
//
// 1. LOCAL MODE (LocalClient) - Single process, SchemaBot-owned storage:
//
//    Used for: local development, self-hosted deployments, single-binary setups
//
//	  ┌──────────────────────────────────────────────────────────────────────────┐
//	  │                         schemabot process                                │
//	  │                                                                          │
//	  │  ┌───────────┐     ┌─────────────────────────────────────────────────┐  │
//	  │  │ commands/ │────▶│              SchemaBot API                      │  │
//	  │  └───────────┘     │  ┌─────────────────────────────────────────┐   │  │
//	  │                    │  │ SchemaBot Storage                       │   │  │
//	  │                    │  │ (locks, plans, applies, tasks, etc.)    │   │  │
//	  │                    │  └─────────────────────────────────────────┘   │  │
//	  │                    │                      │                         │  │
//	  │                    │                      ▼                         │  │
//	  │                    │  ┌─────────────────────────────────────────┐   │  │
//	  │                    │  │ LocalClient (uses SchemaBot storage)    │   │  │
//	  │                    │  │  ┌───────────────────────────────────┐  │   │  │
//	  │                    │  │  │ Spirit Engine                     │──┼───┼──┼──▶ Target DB
//	  │                    │  │  └───────────────────────────────────┘  │   │  │
//	  │                    │  └─────────────────────────────────────────┘   │  │
//	  │                    └────────────────────────────────────────────────┘  │
//	  └──────────────────────────────────────────────────────────────────────────┘
//	                                       │
//	                                       ▼
//	                              ┌─────────────────┐
//	                              │      MySQL      │
//	                              └─────────────────┘
//
//
// 2. gRPC MODE (GRPCClient) - External Tern service:
//
//    Used for: distributed deployments (e2e tests simulate this)
//
//	                                              ┌─────────────────────────────┐
//	  CLI ──────────┐                             │      External Tern          │
//	                │                             │  (remote Tern, or e2e test) │
//	                ▼                             │  ┌───────────────────────┐  │
//	  ┌─────────────────────────────────┐  gRPC  │  │  Internal state:      │  │
//	  │       SchemaBot Server          │        │  │  - schema changes     │  │
//	  │  ┌───────────────────────────┐  │        │  │  - engine state       │──┼──▶ Target DB
//	  │  │      GRPCClient          ─┼──┼────────┼──▶  - tasks              │  │
//	  │  ├───────────────────────────┤  │        │  │  (opaque to us)       │  │
//	  │  │    SchemaBot Storage      │  │        │  └───────────────────────┘  │
//	  │  │  (locks, plans, applies)  │  │        └─────────────────────────────┘
//	  │  └───────────────────────────┘  │
//	  └─────────────────────────────────┘
//	                ▲           │
//	                │           ▼
//	  GitHub ───────┘  ┌─────────────────┐
//	  Webhooks         │ SchemaBot MySQL │
//	                   └─────────────────┘
//
// Storage layers (SchemaBot always has these):
//   - LockStore: Deployment locks to prevent concurrent schema changes
//   - PlanStore: Schema change plans from `schemabot plan`
//   - ApplyStore: Tracks each `schemabot apply` invocation
//   - TaskStore: Tracks individual DDL operations (1 Apply → N Tasks)
//   - CheckStore: GitHub status checks
//   - SettingsStore: Admin settings
//
// The Tern proto interface is the abstraction boundary:
//
//   A remote Tern service has its own internal state tracking.
//   But it implements the same proto interface (Plan, Apply, Progress, Cutover...).
//   SchemaBot uses proto responses to update its own ApplyStore/TaskStore,
//   without caring about the remote Tern's internal implementation details.
//
// LocalClient uses SchemaBot's storage directly - use this when you control everything.
// GRPCClient talks to external Tern - use for distributed deployments or e2e testing.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/block/spirit/pkg/statement"
	spirittable "github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	ps "github.com/planetscale/planetscale-go/planetscale"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/engine/planetscale"
	"github.com/block/schemabot/pkg/engine/spirit"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/mysqlconn"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/psclient"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

const vSchemaArtifactName = "vschema.json"

func namespaceHasVSchemaArtifact(nsData *storage.NamespacePlanData) bool {
	if nsData == nil {
		return false
	}
	return nsData.Artifacts[vSchemaArtifactName] != ""
}

// LocalConfig holds configuration for the local Tern client.
type LocalConfig struct {
	// Database is the name of this database.
	Database string

	// Type is the database type. "mysql" and "vitess" have built-in engines; any
	// other value requires a matching EngineFactories entry.
	Type string

	// TargetDSN is the connection string to the target database for schema changes.
	TargetDSN string

	// Metadata holds engine-specific configuration as key-value pairs.
	// The tern layer does not interpret these — it passes them through to the
	// engine via Credentials.Metadata and reads specific keys as needed.
	// Keys used by PlanetScale: organization, token_name, token_value,
	// tls_name, revert_window_duration, main_branch.
	// Keys used by Spirit: pending_drops ("false" disables the pending drops
	// quarantine so DROP TABLE executes directly).
	Metadata map[string]string

	// WakeOperator notifies the owner loop after an external control request is
	// recorded. The callback must not execute control actions itself; it only
	// nudges the storage-claiming operator to process durable intent promptly.
	WakeOperator func(applyIdentifier, database, environment string)

	// EngineFactories supplies engine implementations for database types this
	// build does not implement natively. An embedding service populates it (via
	// api.Service.RegisterEngine); NewLocalClient uses the matching factory for a
	// Type that has no built-in engine.
	EngineFactories map[string]EngineFactory
}

// EngineFactory builds an Engine for a database type this build does not
// implement natively. It is the extension point that lets an embedding service
// supply an engine without the core depending on its package.
type EngineFactory func(cfg LocalConfig, logger *slog.Logger) (engine.Engine, error)

// LocalClient implements Client by calling an embedded engine directly — the
// built-in Spirit (mysql) or PlanetScale (vitess) engine, or an engine supplied
// by an embedder for another database type. It uses SchemaBot's storage for
// plans and tasks.
type LocalClient struct {
	config            LocalConfig
	storage           storage.Storage
	spiritEngine      engine.Engine
	planetscaleEngine engine.Engine
	customEngine      engine.Engine
	psClientFunc      func(tokenName, tokenValue string) (psclient.PSClient, error)
	logger            *slog.Logger

	// heartbeatInterval controls how often the apply heartbeat updates updated_at.
	// Defaults to 10s. Tests may lower this to verify heartbeat behavior.
	heartbeatInterval time.Duration

	// cancelApply cancels the background goroutine running executeApplySequential
	// or executeGroupedApply. Set when an apply starts, called by Stop().
	// Protected by cancelMu since Apply and Stop run on different goroutines.
	cancelMu              sync.Mutex
	cancelApply           context.CancelFunc
	cancelApplyGeneration uint64

	// observers holds per-apply progress observers. The progress poller notifies
	// the observer on state changes and terminal state. Cleared on terminal state.
	// Protected by observerMu.
	observerMu sync.RWMutex
	observers  map[int64]ProgressObserver // keyed by apply ID

	// pendingObserver is consumed by the next direct Apply() call and registered
	// before Spirit starts.
	// Protected by observerMu.
	pendingObserver ProgressObserver
}

type applyCancelHandle struct {
	generation uint64
	cancel     context.CancelFunc
}

// Compile-time check that LocalClient implements Client.
var _ Client = (*LocalClient)(nil)

// NewLocalClient creates a new local Tern client that calls the Spirit engine directly.
// The storage parameter should be SchemaBot's storage instance for plan/task management.
func NewLocalClient(cfg LocalConfig, stor storage.Storage, logger *slog.Logger) (*LocalClient, error) {
	// For Vitess databases, create a PlanetScale engine with a client factory
	// that points at the API base URL from metadata (e.g., "http://localscale:8080").
	// TargetDSN is the vtgate MySQL DSN for SHOW VITESS_MIGRATIONS.
	var psEngine engine.Engine
	var psClientFunc func(tokenName, tokenValue string) (psclient.PSClient, error)
	if cfg.Type == storage.DatabaseTypeVitess {
		apiURL := cfg.Metadata["api_url"]
		psClientFunc = func(tokenName, tokenValue string) (psclient.PSClient, error) {
			return psclient.NewPSClientWithBaseURL(tokenName, tokenValue, apiURL)
		}
		psEngine = planetscale.NewWithClient(logger, psClientFunc)
	}

	// For a database type without a built-in engine, build it from a registered
	// factory. This is the embedder extension point for engines this build does
	// not include.
	var customEngine engine.Engine
	if cfg.Type != storage.DatabaseTypeMySQL && cfg.Type != storage.DatabaseTypeVitess {
		factory, ok := cfg.EngineFactories[cfg.Type]
		if !ok {
			return nil, fmt.Errorf("no engine registered for database type %q", cfg.Type)
		}
		if factory == nil {
			return nil, fmt.Errorf("engine factory registered for database type %q is nil", cfg.Type)
		}
		eng, err := factory(cfg, logger)
		if err != nil {
			return nil, fmt.Errorf("build engine for database type %q: %w", cfg.Type, err)
		}
		if eng == nil {
			return nil, fmt.Errorf("engine factory for database type %q returned a nil engine", cfg.Type)
		}
		customEngine = eng
	}

	return &LocalClient{
		config:  cfg,
		storage: stor,
		spiritEngine: spirit.New(spirit.Config{
			Logger: logger,
			// Pending drops quarantine is on by default; deployments opt out
			// via the pending_drops metadata key.
			DisablePendingDrops: cfg.Metadata["pending_drops"] == "false",
		}),
		planetscaleEngine: psEngine,
		customEngine:      customEngine,
		psClientFunc:      psClientFunc,
		logger:            logger,
		heartbeatInterval: 10 * time.Second,
	}, nil
}

// IsRemote returns false — LocalClient runs in the same process and creates
// apply/task records in the same database as the API layer.
func (c *LocalClient) IsRemote() bool { return false }

// Endpoint returns the database name for this local client.
func (c *LocalClient) Endpoint() string { return c.config.Database }

func (c *LocalClient) wakeOperatorForControlRequest(apply *storage.Apply) {
	if c.config.WakeOperator == nil {
		c.logger.Debug("operator wake skipped because no wake callback is configured",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment)
		return
	}
	c.config.WakeOperator(apply.ApplyIdentifier, apply.Database, apply.Environment)
}

// protoEngine returns the proto engine type based on database configuration.
func (c *LocalClient) protoEngine() ternv1.Engine {
	// Derive from the engine actually backing this client, so a registered
	// engine reports its own type rather than the Spirit default.
	if eng := c.getEngine(); eng != nil {
		if e, err := engineNameToProto(eng.Name()); err == nil {
			return e
		}
	}
	// Fall back to the type default when there is no engine or its name has no
	// proto representation.
	if c.config.Type == storage.DatabaseTypeVitess {
		return ternv1.Engine_ENGINE_PLANETSCALE
	}
	return ternv1.Engine_ENGINE_SPIRIT
}

func localPlanTarget(req *ternv1.PlanRequest, database string) string {
	if req.Target != "" {
		return req.Target
	}
	return database
}

// engineNameToProto converts a storage engine name to the proto enum.
func engineNameToProto(name string) (ternv1.Engine, error) {
	switch name {
	case storage.EnginePlanetScale:
		return ternv1.Engine_ENGINE_PLANETSCALE, nil
	case storage.EngineSpirit:
		return ternv1.Engine_ENGINE_SPIRIT, nil
	case storage.EngineStrata:
		return ternv1.Engine_ENGINE_STRATA, nil
	default:
		return 0, fmt.Errorf("unknown engine: %s", name)
	}
}

// Close closes the client and releases resources.
func (c *LocalClient) Close() error {
	// LocalClient doesn't own storage, so nothing to close
	return nil
}

// credentials returns engine credentials from the client config.
func (c *LocalClient) credentials() *engine.Credentials {
	return &engine.Credentials{
		DSN:      c.config.TargetDSN,
		Metadata: c.config.Metadata,
	}
}

func (c *LocalClient) credentialsForMySQLNamespace(namespace string) (*engine.Credentials, error) {
	if c.config.Type != storage.DatabaseTypeMySQL {
		return c.credentials(), nil
	}
	hasDatabase, err := mysqlDSNHasDatabase(c.config.TargetDSN)
	if err != nil {
		return nil, fmt.Errorf("inspect MySQL target DSN for namespace injection: %w", err)
	}
	// Transitional: a target DSN that already names a database is used as-is.
	// The data-plane model is a namespace-free DSN with the schema injected per
	// operation (below); existing static/local configs still carry the database
	// in the DSN, and those keep working until they migrate to namespace-free.
	if hasDatabase {
		return c.credentials(), nil
	}
	// A namespace-free target DSN is the inventory/data-plane shape: the concrete
	// namespace is the connection schema and must be injected per operation.
	if namespace == "" {
		return nil, fmt.Errorf("MySQL namespace is required for a namespace-free target DSN")
	}
	dsn, err := mysqlDSNWithDatabase(c.config.TargetDSN, namespace)
	if err != nil {
		return nil, err
	}
	return &engine.Credentials{
		DSN:      dsn,
		Metadata: c.config.Metadata,
	}, nil
}

func (c *LocalClient) credentialsForTask(task *storage.Task) (*engine.Credentials, error) {
	if c.config.Type != storage.DatabaseTypeMySQL {
		return c.credentials(), nil
	}
	if task == nil {
		return nil, fmt.Errorf("task is required for MySQL credentials")
	}
	return c.credentialsForMySQLNamespace(task.Namespace)
}

// credentialsForGroupedApply resolves the single-namespace credentials for a
// grouped/atomic MySQL apply. A grouped apply runs one Spirit execution against
// one schema, so the plan must carry exactly one namespace. Fail closed rather
// than pick a namespace by map iteration order (or silently use a namespace-free
// DSN) if that invariant is ever violated.
func (c *LocalClient) credentialsForGroupedApply(plan *storage.Plan) (*engine.Credentials, error) {
	if c.config.Type != storage.DatabaseTypeMySQL {
		return c.credentials(), nil
	}
	if len(plan.Namespaces) != 1 {
		return nil, fmt.Errorf("grouped MySQL apply requires exactly one namespace, plan has %d", len(plan.Namespaces))
	}
	var namespace string
	for ns := range plan.Namespaces {
		namespace = ns
	}
	return c.credentialsForMySQLNamespace(namespace)
}

func mysqlDSNWithDatabase(dsn, database string) (string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("parse MySQL DSN: %w", err)
	}
	cfg.DBName = database
	return cfg.FormatDSN(), nil
}

func mysqlDSNHasDatabase(dsn string) (bool, error) {
	database, err := mysqlDSNDatabase(dsn)
	if err != nil {
		return false, err
	}
	return database != "", nil
}

func mysqlDSNDatabase(dsn string) (string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("parse MySQL DSN: %w", err)
	}
	return cfg.DBName, nil
}

func (c *LocalClient) deferredCutoverSignalExists(ctx context.Context, apply *storage.Apply) (bool, bool, error) {
	if apply == nil {
		return false, false, fmt.Errorf("apply is required for deferred cutover signal lookup")
	}
	eng := c.getEngine()
	checker, ok := eng.(engine.DeferredCutoverSignalChecker)
	if !ok {
		return false, false, nil
	}
	exists, err := checker.DeferredCutoverSignalExists(ctx, &engine.DeferredCutoverSignalRequest{
		Database:    apply.Database,
		Credentials: c.credentials(),
	})
	if err != nil {
		return false, true, fmt.Errorf("check deferred cutover signal for apply %s database %s: %w", apply.ApplyIdentifier, apply.Database, err)
	}
	return exists, true, nil
}

func (c *LocalClient) normalizeSchemaFiles(schemaFiles schema.SchemaFiles) (schema.SchemaFiles, error) {
	if c.config.Type != storage.DatabaseTypeMySQL {
		return schemaFiles, nil
	}
	normalized := make(schema.SchemaFiles, len(schemaFiles))
	for ns, files := range schemaFiles {
		targetNamespace := c.planNamespace(ns)
		if normalized[targetNamespace] != nil {
			return nil, fmt.Errorf("schema files contain duplicate namespace %q", targetNamespace)
		}
		normalized[targetNamespace] = files
	}
	return normalized, nil
}

func (c *LocalClient) planNamespace(ns string) string {
	if ns == "" || (c.config.Type == storage.DatabaseTypeMySQL && ns == "default") {
		return c.config.Database
	}
	return ns
}

// Health checks the service health.
func (c *LocalClient) Health(ctx context.Context) error {
	return c.storage.Ping(ctx)
}

// PullSchema fetches the live schema and returns declarative schema files.
func (c *LocalClient) PullSchema(ctx context.Context, req *ternv1.PullSchemaRequest) (*ternv1.PullSchemaResponse, error) {
	if c.config.Type != storage.DatabaseTypeMySQL && c.config.Type != storage.DatabaseTypeVitess {
		return nil, fmt.Errorf("pull schema for database %s type %s: only %s and %s are supported: %w", c.config.Database, c.config.Type, storage.DatabaseTypeMySQL, storage.DatabaseTypeVitess, ErrPullSchemaUnsupportedType)
	}
	if req.Type != "" && req.Type != c.config.Type {
		return nil, fmt.Errorf("pull schema for database %s: request type %q does not match client type %q: %w", c.config.Database, req.Type, c.config.Type, ErrPullSchemaInvalidRequest)
	}
	if req.GetNamespace() == "" {
		return c.pullAllNamespaces(ctx, req)
	}
	return c.pullSchemaNamespace(ctx, req, req.GetNamespace())
}

func (c *LocalClient) pullAllNamespaces(ctx context.Context, req *ternv1.PullSchemaRequest) (*ternv1.PullSchemaResponse, error) {
	namespaces, err := c.discoverPullNamespaces(ctx)
	if err != nil {
		return nil, err
	}
	merged := &ternv1.PullSchemaResponse{
		Database:    c.pullResponseDatabase(req),
		Type:        c.config.Type,
		Environment: req.Environment,
		Namespaces:  make(map[string]*ternv1.PulledNamespace, len(namespaces)),
	}
	for _, namespace := range namespaces {
		resp, err := c.pullSchemaNamespace(ctx, req, namespace)
		if err != nil {
			return nil, err
		}
		merged.TableCount += resp.TableCount
		maps.Copy(merged.Namespaces, resp.Namespaces)
	}
	return merged, nil
}

func (c *LocalClient) discoverPullNamespaces(ctx context.Context) ([]string, error) {
	if c.config.Type == storage.DatabaseTypeVitess {
		return c.discoverVitessPullKeyspaces(ctx)
	}

	if database, err := mysqlDSNDatabase(c.config.TargetDSN); err != nil {
		return nil, fmt.Errorf("inspect MySQL target DSN for namespace discovery: %w", err)
	} else if database != "" {
		c.logger.Info("LocalClient.PullSchema: using target DSN database as live namespace", "database", c.config.Database, "namespace", database)
		return []string{database}, nil
	}

	attrs := []any{"database", c.config.Database}
	attrs = append(attrs, dsnLogAttrs(c.config.TargetDSN)...)
	c.logger.Info("LocalClient.PullSchema: discovering live namespaces", attrs...)

	db, err := mysqlconn.Open(c.config.TargetDSN)
	if err != nil {
		return nil, fmt.Errorf("open database target for namespace discovery: %w", err)
	}
	defer utils.CloseAndLog(db)
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping database target for namespace discovery: %w", err)
	}
	rows, err := db.QueryContext(ctx, `SELECT schema_name FROM information_schema.schemata ORDER BY schema_name`)
	if err != nil {
		return nil, fmt.Errorf("list namespaces for schema pull: %w", err)
	}
	defer utils.CloseAndLog(rows)

	var namespaces []string
	for rows.Next() {
		var namespace string
		if err := rows.Scan(&namespace); err != nil {
			return nil, fmt.Errorf("scan namespace for schema pull: %w", err)
		}
		if schema.IsReservedPullNamespace(namespace) {
			c.logger.Debug("LocalClient.PullSchema: skipping reserved namespace", "database", c.config.Database, "namespace", namespace)
			continue
		}
		namespaces = append(namespaces, namespace)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate namespaces for schema pull: %w", err)
	}
	c.logger.Info("LocalClient.PullSchema: discovered live namespaces", "database", c.config.Database, "namespace_count", len(namespaces))
	return namespaces, nil
}

func (c *LocalClient) pullSchemaNamespace(ctx context.Context, req *ternv1.PullSchemaRequest, namespace string) (*ternv1.PullSchemaResponse, error) {
	if c.config.Type == storage.DatabaseTypeVitess {
		return c.pullVitessSchemaNamespace(ctx, req, namespace)
	}

	targetDSN := c.config.TargetDSN
	if c.config.Type == storage.DatabaseTypeMySQL {
		creds, err := c.credentialsForMySQLPullNamespace(namespace)
		if err != nil {
			return nil, fmt.Errorf("resolve database %s namespace %s credentials for schema pull: %w", c.config.Database, namespace, err)
		}
		targetDSN = creds.DSN
	}

	attrs := []any{"database", c.config.Database, "namespace", namespace}
	attrs = append(attrs, dsnLogAttrs(targetDSN)...)
	c.logger.Info("LocalClient.PullSchema: loading live schema", attrs...)

	db, err := mysqlconn.Open(targetDSN)
	if err != nil {
		return nil, fmt.Errorf("open database %s namespace %s for schema pull: %w", c.config.Database, namespace, err)
	}
	defer utils.CloseAndLog(db)

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping database %s namespace %s for schema pull: %w", c.config.Database, namespace, err)
	}

	tables, err := spirittable.LoadSchemaFromDB(ctx, db, spirittable.WithoutUnderscoreTables, spirittable.WithoutArchiveTables, spirittable.WithStrippedAutoIncrement)
	if err != nil {
		return nil, fmt.Errorf("load live schema for database %s namespace %s: %w", c.config.Database, namespace, err)
	}
	sort.Slice(tables, func(i, j int) bool { return tables[i].Name < tables[j].Name })

	pulledTables := make(map[string]string, len(tables))
	for _, tbl := range tables {
		content, err := pulledSchemaFileContent(namespace, tbl.Name, tbl.Schema)
		if err != nil {
			return nil, err
		}
		pulledTables[tbl.Name] = content
	}
	catalog, err := c.pullNamespaceCatalog(ctx, db, namespace, pulledTables, req.GetCatalogDetail())
	if err != nil {
		return nil, err
	}

	c.logger.Info("LocalClient.PullSchema: loaded live schema",
		"database", c.config.Database,
		"namespace", namespace,
		"table_count", len(tables),
	)

	return &ternv1.PullSchemaResponse{
		Database:    c.pullResponseDatabase(req),
		Type:        c.config.Type,
		Environment: req.Environment,
		Namespaces: map[string]*ternv1.PulledNamespace{
			namespace: {
				Tables:           pulledTables,
				NamespaceCatalog: catalog.namespace,
				TableCatalog:     catalog.tables,
			},
		},
		TableCount: int32(len(tables)),
	}, nil
}

func (c *LocalClient) discoverVitessPullKeyspaces(ctx context.Context) ([]string, error) {
	client, org, branch, err := c.planetScalePullClient()
	if err != nil {
		return nil, err
	}

	c.logger.Info("LocalClient.PullSchema: discovering Vitess keyspaces", "database", c.config.Database, "branch", branch)
	keyspaces, err := client.ListKeyspaces(ctx, &ps.ListKeyspacesRequest{
		Organization: org,
		Database:     c.config.Database,
		Branch:       branch,
	})
	if err != nil {
		return nil, fmt.Errorf("list Vitess keyspaces for database %s branch %s: %w", c.config.Database, branch, err)
	}
	namespaces := make([]string, 0, len(keyspaces))
	for _, keyspace := range keyspaces {
		if keyspace == nil {
			c.logger.Warn("LocalClient.PullSchema: skipping nil Vitess keyspace", "database", c.config.Database, "branch", branch)
			continue
		}
		if keyspace.Name == "" {
			return nil, fmt.Errorf("list Vitess keyspaces for database %s branch %s returned a keyspace with no name", c.config.Database, branch)
		}
		if schema.IsReservedPullNamespace(keyspace.Name) {
			c.logger.Debug("LocalClient.PullSchema: skipping reserved Vitess keyspace", "database", c.config.Database, "branch", branch, "namespace", keyspace.Name)
			continue
		}
		namespaces = append(namespaces, keyspace.Name)
	}
	sort.Strings(namespaces)
	c.logger.Info("LocalClient.PullSchema: discovered Vitess keyspaces", "database", c.config.Database, "branch", branch, "namespace_count", len(namespaces))
	return namespaces, nil
}

func (c *LocalClient) pullVitessSchemaNamespace(ctx context.Context, req *ternv1.PullSchemaRequest, namespace string) (*ternv1.PullSchemaResponse, error) {
	client, org, branch, err := c.planetScalePullClient()
	if err != nil {
		return nil, err
	}

	c.logger.Info("LocalClient.PullSchema: loading live Vitess schema", "database", c.config.Database, "branch", branch, "namespace", namespace)
	schemaResult, err := client.GetBranchSchema(ctx, &ps.BranchSchemaRequest{
		Organization: org,
		Database:     c.config.Database,
		Branch:       branch,
		Keyspace:     namespace,
	})
	if err != nil {
		return nil, fmt.Errorf("fetch Vitess schema for database %s branch %s keyspace %s: %w", c.config.Database, branch, namespace, err)
	}

	pulledTables := make(map[string]string, len(schemaResult))
	for _, tbl := range schemaResult {
		if tbl == nil {
			c.logger.Warn("LocalClient.PullSchema: skipping nil Vitess table schema", "database", c.config.Database, "branch", branch, "namespace", namespace)
			continue
		}
		if tbl.Name == "" {
			return nil, fmt.Errorf("fetch Vitess schema for database %s branch %s keyspace %s returned a table with no name", c.config.Database, branch, namespace)
		}
		content, err := pulledSchemaFileContent(c.config.Database, tbl.Name, tbl.Raw)
		if err != nil {
			return nil, fmt.Errorf("fetch Vitess schema for database %s branch %s keyspace %s: %w", c.config.Database, branch, namespace, err)
		}
		pulledTables[tbl.Name] = content
	}

	artifacts := map[string]string{}
	vschema, err := client.GetKeyspaceVSchema(ctx, &ps.GetKeyspaceVSchemaRequest{
		Organization: org,
		Database:     c.config.Database,
		Branch:       branch,
		Keyspace:     namespace,
	})
	if err != nil {
		return nil, fmt.Errorf("fetch Vitess VSchema for database %s branch %s keyspace %s: %w", c.config.Database, branch, namespace, err)
	}
	if vschema != nil && strings.TrimSpace(vschema.Raw) != "" {
		artifacts[vSchemaArtifactName] = strings.TrimRight(vschema.Raw, "\n") + "\n"
	}

	c.logger.Info("LocalClient.PullSchema: loaded live Vitess schema",
		"database", c.config.Database,
		"branch", branch,
		"namespace", namespace,
		"table_count", len(pulledTables),
		"artifact_count", len(artifacts),
	)

	return &ternv1.PullSchemaResponse{
		Database:    c.pullResponseDatabase(req),
		Type:        c.config.Type,
		Environment: req.Environment,
		Namespaces: map[string]*ternv1.PulledNamespace{
			namespace: {
				Tables:    pulledTables,
				Artifacts: artifacts,
				NamespaceCatalog: &ternv1.NamespaceCatalog{
					Name:       namespace,
					Engine:     c.config.Type,
					TableCount: int32(len(pulledTables)),
				},
			},
		},
		TableCount: int32(len(pulledTables)),
	}, nil
}

func (c *LocalClient) planetScalePullClient() (psclient.PSClient, string, string, error) {
	if c.psClientFunc == nil {
		return nil, "", "", fmt.Errorf("PlanetScale client is not configured for database %s: %w", c.config.Database, ErrPullSchemaUnsupportedType)
	}
	org := c.config.Metadata["organization"]
	if org == "" {
		return nil, "", "", fmt.Errorf("PlanetScale organization metadata is required for database %s", c.config.Database)
	}
	branch := c.config.Metadata["main_branch"]
	if branch == "" {
		branch = "main"
	}
	client, err := c.psClientFunc(c.config.Metadata["token_name"], c.config.Metadata["token_value"])
	if err != nil {
		return nil, "", "", fmt.Errorf("create PlanetScale client for database %s: %w", c.config.Database, err)
	}
	return client, org, branch, nil
}

type pulledCatalog struct {
	namespace *ternv1.NamespaceCatalog
	tables    map[string]*ternv1.TableCatalog
}

func (c *LocalClient) pullNamespaceCatalog(ctx context.Context, db *sql.DB, namespace string, pulledTables map[string]string, catalogDetail ternv1.PullCatalogDetail) (*pulledCatalog, error) {
	catalog := &pulledCatalog{
		namespace: &ternv1.NamespaceCatalog{
			Name:       namespace,
			Engine:     c.config.Type,
			TableCount: int32(len(pulledTables)),
		},
		tables: make(map[string]*ternv1.TableCatalog, len(pulledTables)),
	}
	if len(pulledTables) == 0 {
		return catalog, nil
	}
	if err := c.loadTableCatalog(ctx, db, namespace, pulledTables, catalog.tables); err != nil {
		return nil, err
	}
	if catalogDetail != ternv1.PullCatalogDetail_PULL_CATALOG_DETAIL_DETAILED {
		return catalog, nil
	}
	if err := c.loadColumnCatalog(ctx, db, namespace, pulledTables, catalog.tables); err != nil {
		return nil, err
	}
	if err := c.loadIndexCatalog(ctx, db, namespace, pulledTables, catalog.tables); err != nil {
		return nil, err
	}
	return catalog, nil
}

func (c *LocalClient) loadTableCatalog(ctx context.Context, db *sql.DB, namespace string, pulledTables map[string]string, catalog map[string]*ternv1.TableCatalog) error {
	rows, err := db.QueryContext(ctx, `
		SELECT table_name, table_type, table_comment
		FROM information_schema.tables
		WHERE table_schema = ?
		ORDER BY table_name`, namespace)
	if err != nil {
		return fmt.Errorf("load table catalog for database %s namespace %s: %w", c.config.Database, namespace, err)
	}
	defer utils.CloseAndLog(rows)

	for rows.Next() {
		var tableName, tableType, tableComment string
		if err := rows.Scan(&tableName, &tableType, &tableComment); err != nil {
			return fmt.Errorf("scan table catalog for database %s namespace %s: %w", c.config.Database, namespace, err)
		}
		if _, ok := pulledTables[tableName]; ok {
			catalog[tableName] = &ternv1.TableCatalog{
				Name:    tableName,
				Kind:    normalizedTableKind(tableType),
				Comment: tableComment,
			}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate table catalog for database %s namespace %s: %w", c.config.Database, namespace, err)
	}
	return nil
}

func (c *LocalClient) loadColumnCatalog(ctx context.Context, db *sql.DB, namespace string, pulledTables map[string]string, catalog map[string]*ternv1.TableCatalog) error {
	rows, err := db.QueryContext(ctx, `
		SELECT table_name, column_name, column_type, is_nullable, column_default, column_comment
		FROM information_schema.columns
		WHERE table_schema = ?
		ORDER BY table_name, ordinal_position`, namespace)
	if err != nil {
		return fmt.Errorf("load column catalog for database %s namespace %s: %w", c.config.Database, namespace, err)
	}
	defer utils.CloseAndLog(rows)

	for rows.Next() {
		var tableName, columnName, columnType, nullable, comment string
		var defaultValue sql.NullString
		if err := rows.Scan(&tableName, &columnName, &columnType, &nullable, &defaultValue, &comment); err != nil {
			return fmt.Errorf("scan column catalog for database %s namespace %s: %w", c.config.Database, namespace, err)
		}
		if _, ok := pulledTables[tableName]; ok {
			tableCatalog := ensurePulledTableCatalog(catalog, tableName)
			column := &ternv1.ColumnCatalog{
				Name:     columnName,
				Type:     columnType,
				Nullable: nullable == "YES",
				Comment:  comment,
			}
			if defaultValue.Valid {
				column.DefaultValue = defaultValue.String
			}
			tableCatalog.Columns = append(tableCatalog.Columns, column)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate column catalog for database %s namespace %s: %w", c.config.Database, namespace, err)
	}
	return nil
}

func (c *LocalClient) loadIndexCatalog(ctx context.Context, db *sql.DB, namespace string, pulledTables map[string]string, catalog map[string]*ternv1.TableCatalog) error {
	rows, err := db.QueryContext(ctx, `
		SELECT table_name, index_name, non_unique, column_name, expression
		FROM information_schema.statistics
		WHERE table_schema = ?
		ORDER BY table_name, index_name, seq_in_index`, namespace)
	if err != nil {
		return fmt.Errorf("load index catalog for database %s namespace %s: %w", c.config.Database, namespace, err)
	}
	defer utils.CloseAndLog(rows)

	indexesByTable := make(map[string]map[string]*ternv1.IndexCatalog)
	for rows.Next() {
		var tableName, indexName string
		var columnName, expression sql.NullString
		var nonUnique int32
		if err := rows.Scan(&tableName, &indexName, &nonUnique, &columnName, &expression); err != nil {
			return fmt.Errorf("scan index catalog for database %s namespace %s: %w", c.config.Database, namespace, err)
		}
		if _, ok := pulledTables[tableName]; ok {
			indexedValue := ""
			switch {
			case columnName.Valid:
				indexedValue = columnName.String
			case expression.Valid:
				indexedValue = expression.String
			default:
				c.logger.Warn("LocalClient.PullSchema: skipping index part without column or expression", "database", c.config.Database, "namespace", namespace, "table", tableName, "index", indexName)
				continue
			}
			tableCatalog := ensurePulledTableCatalog(catalog, tableName)
			if indexesByTable[tableName] == nil {
				indexesByTable[tableName] = make(map[string]*ternv1.IndexCatalog)
			}
			idx := indexesByTable[tableName][indexName]
			if idx == nil {
				idx = &ternv1.IndexCatalog{
					Name:    indexName,
					Primary: indexName == "PRIMARY",
					Unique:  nonUnique == 0,
				}
				indexesByTable[tableName][indexName] = idx
				tableCatalog.Indexes = append(tableCatalog.Indexes, idx)
			}
			idx.Parts = append(idx.Parts, indexedValue)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate index catalog for database %s namespace %s: %w", c.config.Database, namespace, err)
	}
	return nil
}

func ensurePulledTableCatalog(catalog map[string]*ternv1.TableCatalog, tableName string) *ternv1.TableCatalog {
	tableCatalog := catalog[tableName]
	if tableCatalog == nil {
		tableCatalog = &ternv1.TableCatalog{Name: tableName}
		catalog[tableName] = tableCatalog
	}
	return tableCatalog
}

func normalizedTableKind(tableType string) string {
	switch tableType {
	case "BASE TABLE":
		return "table"
	case "VIEW":
		return "view"
	default:
		return strings.ToLower(strings.ReplaceAll(tableType, " ", "_"))
	}
}

func (c *LocalClient) pullResponseDatabase(req *ternv1.PullSchemaRequest) string {
	if req.GetDatabase() != "" {
		return req.GetDatabase()
	}
	return c.config.Database
}

func (c *LocalClient) credentialsForMySQLPullNamespace(namespace string) (*engine.Credentials, error) {
	if c.config.Type != storage.DatabaseTypeMySQL {
		return c.credentials(), nil
	}
	database, err := mysqlDSNDatabase(c.config.TargetDSN)
	if err != nil {
		return nil, fmt.Errorf("inspect MySQL target DSN for namespace injection: %w", err)
	}
	if database != "" {
		if database != namespace {
			return nil, fmt.Errorf("target DSN database %q does not match requested namespace %q", database, namespace)
		}
		return c.credentials(), nil
	}
	if namespace == "" {
		return nil, fmt.Errorf("MySQL namespace is required for a namespace-free target DSN")
	}
	dsn, err := mysqlDSNWithDatabase(c.config.TargetDSN, namespace)
	if err != nil {
		return nil, err
	}
	return &engine.Credentials{
		DSN:      dsn,
		Metadata: c.config.Metadata,
	}, nil
}

func pulledSchemaFileContent(database string, tableName string, tableDDL string) (string, error) {
	if tableName == "" {
		return "", fmt.Errorf("load live schema for database %s: table with empty name", database)
	}
	content := strings.TrimRight(tableDDL, "\n") + "\n"
	if _, err := statement.ParseCreateTable(content); err != nil {
		return "", fmt.Errorf("parse pulled schema for database %s table %s: %w", database, tableName, err)
	}
	return content, nil
}

// Plan generates a schema change plan from declarative schema files.
func (c *LocalClient) Plan(ctx context.Context, req *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	if c.getEngine() == nil {
		return nil, fmt.Errorf("no engine available for database type %q", c.config.Type)
	}

	// Convert schema files from proto to engine type.
	schemaFiles, err := c.normalizeSchemaFiles(protoToSchemaFiles(req.SchemaFiles))
	if err != nil {
		return nil, err
	}

	planLogAttrs := []any{"database", c.config.Database}
	planLogAttrs = append(planLogAttrs, dsnLogAttrs(c.config.TargetDSN)...)
	planLogAttrs = append(planLogAttrs, "schema_file_count", len(schemaFiles))
	c.logger.Info("LocalClient.Plan: calling engine", planLogAttrs...)

	result, err := c.planWithEngine(ctx, req, c.config.Database, schemaFiles)
	if err != nil {
		c.logger.Error("plan failed", "error", err, "database", c.config.Database)
		return nil, err // Error already has clear prefix (SQL syntax/usage error)
	}

	c.logger.Info("LocalClient.Plan: engine result",
		"plan_id", result.PlanID,
		"change_count", len(result.Changes),
		"flat_table_change_count", len(result.FlatTableChanges()),
	)
	for _, sc := range result.Changes {
		for _, tc := range sc.TableChanges {
			c.logger.Info("LocalClient.Plan: table change from engine",
				"table", tc.Table,
				"operation", tc.Operation,
				"ddl_len", len(tc.DDL),
			)
		}
	}

	// Store the plan in SchemaBot's storage
	ddlChanges := make([]storage.TableChange, len(result.FlatTableChanges()))
	for i, t := range result.FlatTableChanges() {
		ddlChanges[i] = storage.TableChange{
			Table:     t.Table,
			DDL:       t.DDL,
			Operation: ddl.StatementTypeToOp(t.Operation),
		}
	}

	// Build per-namespace plan data from the engine's changes.
	// For Vitess, each namespace is a keyspace. For Spirit, there's one namespace.
	namespaces := make(map[string]*storage.NamespacePlanData)
	seenTable := make(map[string]map[string]bool)
	var allShardPlans []storage.ShardPlan
	for _, sc := range result.Changes {
		ns := c.planNamespace(sc.Namespace)
		nsData := namespaces[ns]
		if nsData == nil {
			nsData = &storage.NamespacePlanData{}
			namespaces[ns] = nsData
			seenTable[ns] = make(map[string]bool)
		}
		// A plan is keyed by (namespace, shard), so a sharded engine emits one
		// SchemaChange per shard and the same table repeats across a keyspace's
		// shards. The stored plan keeps namespace-level tables, so dedupe by table.
		for _, tc := range sc.TableChanges {
			if seenTable[ns][tc.Table] {
				continue
			}
			seenTable[ns][tc.Table] = true
			nsData.Tables = append(nsData.Tables, storage.TableChange{
				Table:     tc.Table,
				DDL:       tc.DDL,
				Operation: ddl.StatementTypeToOp(tc.Operation),
			})
		}
		// Record per-shard membership so apply-create can rebuild per-shard
		// operation groups. A SchemaChange with an empty shard targets the whole
		// namespace (non-sharded engines) and contributes no shard rows.
		if shardName := strings.TrimSpace(sc.Shard.Name); shardName != "" {
			sp := storage.ShardPlan{Shard: shardName, Namespace: ns, NeedsChange: true}
			nsData.Shards = append(nsData.Shards, sp)
			allShardPlans = append(allShardPlans, sp)
		}
		if len(sc.OriginalFiles) > 0 {
			nsData.OriginalFiles = sc.OriginalFiles
		}
		if sc.OriginalFilesCaptured {
			nsData.OriginalFilesCaptured = true
			if nsData.OriginalFiles == nil {
				nsData.OriginalFiles = map[string]string{}
			}
		}
		// Only store VSchema artifacts when the Plan detected a change.
		if sc.Metadata["vschema_changed"] == "true" {
			if nsFiles, ok := schemaFiles[ns]; ok && nsFiles != nil {
				if vs, ok := nsFiles.Files[vSchemaArtifactName]; ok && vs != "" {
					if nsData.Artifacts == nil {
						nsData.Artifacts = map[string]string{}
					}
					nsData.Artifacts[vSchemaArtifactName] = vs
				}
			}
		}
	}
	if len(namespaces) == 0 {
		namespaces[c.config.Database] = &storage.NamespacePlanData{
			Tables: ddlChanges,
		}
	}

	// Don't store empty plans — no DDL changes, no VSchema changes.
	hasVSchemaChanges := false
	for _, ns := range namespaces {
		if namespaceHasVSchemaArtifact(ns) {
			hasVSchemaChanges = true
			break
		}
	}
	if len(ddlChanges) == 0 && !hasVSchemaChanges {
		c.logger.Info("Plan: no changes, skipping storage", "plan_id", result.PlanID, "database", c.config.Database)
		return &ternv1.PlanResponse{
			PlanId: result.PlanID,
			Engine: c.protoEngine(),
		}, nil
	}

	plan := &storage.Plan{
		PlanIdentifier: result.PlanID,
		Database:       c.config.Database,
		DatabaseType:   c.config.Type,
		Deployment:     c.config.Database,
		Target:         localPlanTarget(req, c.config.Database),
		Repository:     req.Repository,
		PullRequest:    int(req.PullRequest),
		SchemaPath:     req.SchemaPath,
		Environment:    req.Environment,
		SchemaFiles:    schemaFiles,
		Namespaces:     namespaces,
		HeadSHA:        req.HeadSha,
		CreatedAt:      time.Now(),
	}
	c.logger.Info("Plan: storing plan",
		"plan_id", result.PlanID,
		"ddl_change_count", len(ddlChanges),
		"database", c.config.Database,
	)
	for i, tc := range ddlChanges {
		c.logger.Debug("Plan: DDLChange to store",
			"index", i,
			"table", tc.Table,
			"ddl", tc.DDL,
		)
	}
	planID, err := c.storage.Plans().Create(ctx, plan)
	if err != nil {
		c.logger.Error("save plan failed", "error", err, "plan_id", result.PlanID)
		return nil, fmt.Errorf("save plan failed: %w", err)
	}
	plan.ID = planID

	// Convert engine SchemaChanges to proto SchemaChanges. A sharded engine emits
	// one SchemaChange per (namespace, shard); collapse them back to one proto
	// SchemaChange per namespace (deduping repeated tables). Per-shard membership
	// travels separately on PlanResponse.Shards below.
	var changes []*ternv1.SchemaChange
	protoByNS := make(map[string]*ternv1.SchemaChange)
	protoTableSeen := make(map[string]map[string]bool)
	for _, sc := range result.Changes {
		ns := c.planNamespace(sc.Namespace)
		protoSC := protoByNS[ns]
		if protoSC == nil {
			protoSC = &ternv1.SchemaChange{
				Namespace:             ns,
				Metadata:              sc.Metadata,
				OriginalFiles:         sc.OriginalFiles,
				OriginalFilesCaptured: sc.OriginalFilesCaptured,
			}
			protoByNS[ns] = protoSC
			protoTableSeen[ns] = make(map[string]bool)
			changes = append(changes, protoSC)
		}
		for _, t := range sc.TableChanges {
			if protoTableSeen[ns][t.Table] {
				continue
			}
			protoTableSeen[ns][t.Table] = true
			protoSC.TableChanges = append(protoSC.TableChanges, &ternv1.TableChange{
				TableName:    t.Table,
				ChangeType:   changeTypeToProto(t.Operation),
				Ddl:          t.DDL,
				IsUnsafe:     t.IsUnsafe,
				UnsafeReason: t.UnsafeReason,
				Namespace:    ns,
			})
		}
	}

	// Convert lint violations to proto
	violations := make([]*ternv1.LintViolation, len(result.LintViolations))
	for i, w := range result.LintViolations {
		violations[i] = &ternv1.LintViolation{
			Table:    w.Table,
			Column:   w.Column,
			Linter:   w.Linter,
			Message:  w.Message,
			Severity: w.Severity,
		}
	}

	// Surface per-shard plan metadata on the response too, for parity with the
	// gRPC path: callers of Plan can display per-shard drift/membership.
	var protoShards []*ternv1.ShardPlan
	for _, sp := range allShardPlans {
		protoShards = append(protoShards, &ternv1.ShardPlan{
			Shard:       sp.Shard,
			Namespace:   sp.Namespace,
			NeedsChange: sp.NeedsChange,
		})
	}

	return &ternv1.PlanResponse{
		PlanId:         result.PlanID,
		Engine:         c.protoEngine(),
		Changes:        changes,
		LintViolations: violations,
		Shards:         protoShards,
	}, nil
}

func (c *LocalClient) planWithEngine(ctx context.Context, req *ternv1.PlanRequest, database string, schemaFiles schema.SchemaFiles) (*engine.PlanResult, error) {
	eng := c.getEngine()
	if eng == nil {
		return nil, fmt.Errorf("no engine configured for type: %s", c.config.Type)
	}
	if c.config.Type != storage.DatabaseTypeMySQL {
		return c.planNamespaceWithEngine(ctx, eng, req, database, schemaFiles, c.credentials())
	}
	hasDatabase, err := mysqlDSNHasDatabase(c.config.TargetDSN)
	if err != nil {
		return nil, err
	}
	if hasDatabase {
		return c.planNamespaceWithEngine(ctx, eng, req, database, schemaFiles, c.credentials())
	}
	if len(schemaFiles) == 0 {
		return nil, fmt.Errorf("schema files are required for namespace-free MySQL target DSN")
	}
	if len(schemaFiles) == 1 {
		for namespace := range schemaFiles {
			creds, err := c.credentialsForMySQLNamespace(namespace)
			if err != nil {
				return nil, err
			}
			return c.planNamespaceWithEngine(ctx, eng, req, namespace, schemaFiles, creds)
		}
	}
	return c.planMySQLNamespacesWithEngine(ctx, eng, req, schemaFiles)
}

func (c *LocalClient) planNamespaceWithEngine(ctx context.Context, eng engine.Engine, req *ternv1.PlanRequest, database string, schemaFiles schema.SchemaFiles, creds *engine.Credentials) (*engine.PlanResult, error) {
	return eng.Plan(ctx, &engine.PlanRequest{
		Database:     database,
		DatabaseType: c.config.Type,
		SchemaFiles:  schemaFiles,
		Repository:   req.Repository,
		PullRequest:  int(req.PullRequest),
		Credentials:  creds,
	})
}

func (c *LocalClient) planMySQLNamespacesWithEngine(ctx context.Context, eng engine.Engine, req *ternv1.PlanRequest, schemaFiles schema.SchemaFiles) (*engine.PlanResult, error) {
	namespaces := make([]string, 0, len(schemaFiles))
	for namespace := range schemaFiles {
		namespaces = append(namespaces, namespace)
	}
	sort.Strings(namespaces)

	result := &engine.PlanResult{PlanID: fmt.Sprintf("plan-%d", time.Now().UnixNano()), NoChanges: true}
	for _, namespace := range namespaces {
		creds, err := c.credentialsForMySQLNamespace(namespace)
		if err != nil {
			return nil, err
		}
		nsResult, err := c.planNamespaceWithEngine(ctx, eng, req, namespace, schema.SchemaFiles{namespace: schemaFiles[namespace]}, creds)
		if err != nil {
			return nil, fmt.Errorf("plan MySQL namespace %q: %w", namespace, err)
		}
		result.Changes = append(result.Changes, nsResult.Changes...)
		result.LintViolations = append(result.LintViolations, nsResult.LintViolations...)
		if !nsResult.NoChanges || len(nsResult.Changes) > 0 {
			result.NoChanges = false
		}
	}
	return result, nil
}

// planForApplyRequest resolves the plan for an apply. It prefers a plan row in
// this deployment's own storage (the single-deployment path, and the primary
// deployment of a multi-deployment apply). When no local plan exists, a
// non-primary deployment's Tern never planned locally — the plan was created on
// the primary deployment's Tern — but the dispatch request carries the
// authoritative DDL changes and schema files, so the plan is materialized from
// them. A request with neither (a stale apply, or a local-mode apply for a plan
// that does not exist here) has nothing to materialize and resolves to no plan.
func (c *LocalClient) planForApplyRequest(ctx context.Context, req *ternv1.ApplyRequest) (*storage.Plan, error) {
	plan, err := c.storage.Plans().Get(ctx, req.PlanId)
	if err != nil {
		return nil, fmt.Errorf("get plan %s: %w", req.PlanId, err)
	}
	if plan != nil {
		return plan, nil
	}
	if len(req.DdlChanges) == 0 && len(req.SchemaFiles) == 0 {
		return nil, nil
	}
	return c.materializeApplyRequestPlan(ctx, req)
}

// materializeApplyRequestPlan persists a local plan row from a dispatch
// request's authoritative DDL changes and schema files so a non-primary
// deployment applies exactly what the primary deployment planned.
func (c *LocalClient) materializeApplyRequestPlan(ctx context.Context, req *ternv1.ApplyRequest) (*storage.Plan, error) {
	schemaFiles, err := c.normalizeSchemaFiles(protoToSchemaFiles(req.SchemaFiles))
	if err != nil {
		return nil, fmt.Errorf("materialize plan %s: normalize schema files: %w", req.PlanId, err)
	}
	namespaces, err := c.namespacesFromApplyRequest(req.DdlChanges, schemaFiles)
	if err != nil {
		return nil, fmt.Errorf("materialize plan %s: %w", req.PlanId, err)
	}
	if len(namespaces) == 0 {
		return nil, fmt.Errorf("materialize plan %s: apply request carried no DDL changes or schema files", req.PlanId)
	}

	if err := c.verifyMaterializedPlanMatchesLiveSchema(ctx, req, schemaFiles); err != nil {
		return nil, fmt.Errorf("materialize plan %s: %w", req.PlanId, err)
	}

	target := req.Target
	if target == "" {
		target = c.config.Database
	}
	plan := &storage.Plan{
		PlanIdentifier: req.PlanId,
		Database:       c.config.Database,
		DatabaseType:   c.config.Type,
		Deployment:     c.config.Database,
		Target:         target,
		Environment:    req.Environment,
		SchemaFiles:    schemaFiles,
		Namespaces:     namespaces,
		CreatedAt:      time.Now(),
	}
	c.logger.Info("Apply: materializing plan from dispatch request",
		"plan_id", req.PlanId,
		"database", c.config.Database,
		"namespace_count", len(namespaces),
	)

	planID, err := c.storage.Plans().Create(ctx, plan)
	if err != nil {
		// A concurrent drive of the same operation may have materialized the
		// plan first; reload and use the existing row rather than failing.
		existing, getErr := c.storage.Plans().Get(ctx, req.PlanId)
		if getErr == nil && existing != nil {
			return existing, nil
		}
		return nil, fmt.Errorf("create materialized plan %s: %w", req.PlanId, err)
	}
	plan.ID = planID
	return plan, nil
}

// namespacesFromApplyRequest rebuilds per-namespace plan data from a dispatch
// request so a deployment that did not plan locally applies exactly what the
// primary deployment planned. Table changes are grouped by namespace (resolved
// through planNamespace so the empty and MySQL "default" namespaces map to the
// database, consistent with plan-time), and each operation is recovered from the
// authoritative DDL. A vschema change is not a table change — it is applied from
// the namespace's vschema.json artifact — so the artifact is attached only to
// namespaces whose request carries an explicit vschema change, mirroring
// plan-time behavior (the artifact is stored only when the plan detected a
// change). Attaching it unconditionally would create spurious vschema_update
// tasks on DDL-only plans, since Vitess always ships a vschema.json schema file.
func (c *LocalClient) namespacesFromApplyRequest(changes []*ternv1.TableChange, schemaFiles schema.SchemaFiles) (map[string]*storage.NamespacePlanData, error) {
	namespaces := map[string]*storage.NamespacePlanData{}
	vschemaChangedNamespaces := map[string]bool{}
	ensure := func(ns string) *storage.NamespacePlanData {
		ns = c.planNamespace(ns)
		if namespaces[ns] == nil {
			namespaces[ns] = &storage.NamespacePlanData{}
		}
		return namespaces[ns]
	}

	for _, ch := range changes {
		if ch == nil {
			continue
		}
		if ch.ChangeType == ternv1.ChangeType_CHANGE_TYPE_VSCHEMA {
			ensure(ch.Namespace)
			vschemaChangedNamespaces[c.planNamespace(ch.Namespace)] = true
			continue
		}
		op, err := materializedTableChangeOperation(ch)
		if err != nil {
			return nil, err
		}
		nsData := ensure(ch.Namespace)
		nsData.Tables = append(nsData.Tables, storage.TableChange{
			Namespace: ch.Namespace,
			Table:     ch.TableName,
			DDL:       ch.Ddl,
			Operation: op,
		})
	}

	for ns := range vschemaChangedNamespaces {
		nsFiles := schemaFiles[ns]
		vs := ""
		if nsFiles != nil {
			vs = nsFiles.Files[vSchemaArtifactName]
		}
		if vs == "" {
			return nil, fmt.Errorf("apply request indicates a vschema change for namespace %q but carries no %s artifact", ns, vSchemaArtifactName)
		}
		nsData := namespaces[ns]
		if nsData.Artifacts == nil {
			nsData.Artifacts = map[string]string{}
		}
		nsData.Artifacts[vSchemaArtifactName] = vs
	}

	return namespaces, nil
}

// materializedTableChangeOperation recovers the storage operation for a
// materialized table change. The proto change type is authoritative when it maps
// to a known DDL action; otherwise the operation is classified from the request's
// authoritative DDL so an unmapped change type does not persist an "unknown"
// action that would resume as a no-op.
func materializedTableChangeOperation(ch *ternv1.TableChange) (string, error) {
	if op := protoChangeTypeToDDLAction(ch.ChangeType); op != "unknown" {
		return op, nil
	}
	if strings.TrimSpace(ch.Ddl) == "" {
		return "", fmt.Errorf("table change for %q has an unrecognized change type and no DDL to classify", ch.TableName)
	}
	op, _, err := ddl.ClassifyStatementOp(ch.Ddl)
	if err != nil {
		return "", fmt.Errorf("classify DDL for table %q: %w", ch.TableName, err)
	}
	return op, nil
}

// driftChangeKey identifies a single table DDL change for drift comparison. Two
// changes are the same iff they target the same namespace and table with the
// same operation and canonicalized DDL.
type driftChangeKey struct {
	namespace string
	table     string
	operation string
	ddl       string
}

// driftChangeMultiset counts table DDL changes by key so duplicate changes are
// compared exactly (set equality would silently tolerate a duplicated change).
type driftChangeMultiset map[driftChangeKey]int

// verifyMaterializedPlanMatchesLiveSchema fails closed unless the reviewed DDL
// carried by a dispatch request is exactly what this deployment would plan
// against its own live schema. A non-primary deployment never planned locally —
// the reviewed plan was computed against the primary deployment's live schema —
// so blindly materializing it would silently replay the primary's DDL on a
// deployment whose schema may have drifted. Recomputing the local diff and
// requiring an exact match keeps non-primary drift from being applied unreviewed.
func (c *LocalClient) verifyMaterializedPlanMatchesLiveSchema(ctx context.Context, req *ternv1.ApplyRequest, schemaFiles schema.SchemaFiles) error {
	if len(req.TargetShards) > 0 {
		return fmt.Errorf("drift guard does not support shard-scoped applies (target_shards=%v); a whole-deployment replan cannot be compared to a shard subset", req.TargetShards)
	}

	result, err := c.planWithEngine(ctx, &ternv1.PlanRequest{
		Database:    c.config.Database,
		Type:        c.config.Type,
		Environment: req.Environment,
		Target:      req.Target,
	}, c.config.Database, schemaFiles)
	if err != nil {
		return fmt.Errorf("recompute local plan: %w", err)
	}

	recomputed, err := c.driftMultisetFromPlanResult(result)
	if err != nil {
		return fmt.Errorf("recomputed plan: %w", err)
	}
	dispatched, err := c.driftMultisetFromApplyRequest(req.DdlChanges)
	if err != nil {
		return fmt.Errorf("dispatched plan: %w", err)
	}
	if err := compareDriftMultisets(recomputed, dispatched); err != nil {
		return fmt.Errorf("local schema has drifted from the reviewed plan (database %q, target %q): %w", c.config.Database, req.Target, err)
	}

	if err := compareVSchemaParity(vschemaNamespacesFromPlanResult(c, result), vschemaNamespacesFromApplyRequest(c, req.DdlChanges)); err != nil {
		return fmt.Errorf("local vschema has drifted from the reviewed plan (database %q, target %q): %w", c.config.Database, req.Target, err)
	}
	return nil
}

// driftMultisetFromPlanResult builds the table DDL multiset this deployment
// would plan against its own live schema. VSchema changes carry no table DDL and
// are compared separately, so they are excluded here.
func (c *LocalClient) driftMultisetFromPlanResult(result *engine.PlanResult) (driftChangeMultiset, error) {
	ms := driftChangeMultiset{}
	for _, sc := range result.Changes {
		ns := c.planNamespace(sc.Namespace)
		for _, tc := range sc.TableChanges {
			canon, err := canonicalDDLForDrift(tc.DDL)
			if err != nil {
				return nil, fmt.Errorf("table %q: %w", tc.Table, err)
			}
			ms[driftChangeKey{ns, tc.Table, ddl.StatementTypeToOp(tc.Operation), canon}]++
		}
	}
	return ms, nil
}

// driftMultisetFromApplyRequest builds the table DDL multiset the dispatch
// request carries as the reviewed, authoritative plan. VSchema changes are
// compared separately and excluded here. Nil entries are corrupt input and fail
// closed.
func (c *LocalClient) driftMultisetFromApplyRequest(changes []*ternv1.TableChange) (driftChangeMultiset, error) {
	ms := driftChangeMultiset{}
	for _, ch := range changes {
		if ch == nil {
			return nil, fmt.Errorf("dispatch request carried a nil table change")
		}
		if ch.ChangeType == ternv1.ChangeType_CHANGE_TYPE_VSCHEMA {
			continue
		}
		op, err := materializedTableChangeOperation(ch)
		if err != nil {
			return nil, err
		}
		canon, err := canonicalDDLForDrift(ch.Ddl)
		if err != nil {
			return nil, fmt.Errorf("table %q: %w", ch.TableName, err)
		}
		ms[driftChangeKey{c.planNamespace(ch.Namespace), ch.TableName, op, canon}]++
	}
	return ms, nil
}

// canonicalDDLForDrift normalizes a DDL statement for comparison and fails
// closed if it cannot be parsed — ddl.Canonicalize returns the input unchanged
// on a parse failure, so an unparseable statement would otherwise compare by raw
// text and could mask drift.
func canonicalDDLForDrift(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("empty DDL")
	}
	if _, _, err := ddl.ClassifyStatement(raw); err != nil {
		return "", fmt.Errorf("unparseable DDL: %w", err)
	}
	return ddl.Canonicalize(raw), nil
}

// compareDriftMultisets reports drift unless the recomputed and dispatched table
// DDL multisets are exactly equal.
func compareDriftMultisets(recomputed, dispatched driftChangeMultiset) error {
	var missing, unexpected []string
	for key, want := range dispatched {
		if recomputed[key] < want {
			missing = append(missing, formatDriftKey(key))
		}
	}
	for key, have := range recomputed {
		if have > dispatched[key] {
			unexpected = append(unexpected, formatDriftKey(key))
		}
	}
	if len(missing) == 0 && len(unexpected) == 0 {
		return nil
	}
	sort.Strings(missing)
	sort.Strings(unexpected)
	return fmt.Errorf("reviewed changes this deployment would not plan: %v; changes this deployment would plan that were not reviewed: %v", missing, unexpected)
}

func formatDriftKey(k driftChangeKey) string {
	// Include the canonicalized DDL: the multiset keys on it, so two changes for
	// the same namespace/table/operation that differ only in DDL must render
	// differently or the drift message would list identical-looking entries on
	// both sides and hide what actually drifted.
	return fmt.Sprintf("%s.%s/%s (%s)", k.namespace, k.table, k.operation, k.ddl)
}

// vschemaNamespacesFromPlanResult returns the namespaces the recomputed plan
// detected a vschema change for.
func vschemaNamespacesFromPlanResult(c *LocalClient, result *engine.PlanResult) map[string]bool {
	out := map[string]bool{}
	for _, sc := range result.Changes {
		if sc.Metadata["vschema_changed"] == "true" {
			out[c.planNamespace(sc.Namespace)] = true
		}
	}
	return out
}

// vschemaNamespacesFromApplyRequest returns the namespaces the dispatch request
// carries a vschema change for.
func vschemaNamespacesFromApplyRequest(c *LocalClient, changes []*ternv1.TableChange) map[string]bool {
	out := map[string]bool{}
	for _, ch := range changes {
		if ch != nil && ch.ChangeType == ternv1.ChangeType_CHANGE_TYPE_VSCHEMA {
			out[c.planNamespace(ch.Namespace)] = true
		}
	}
	return out
}

// compareVSchemaParity reports drift unless the recomputed and dispatched
// vschema-changed namespaces are exactly equal.
func compareVSchemaParity(recomputed, dispatched map[string]bool) error {
	var missing, unexpected []string
	for ns := range dispatched {
		if !recomputed[ns] {
			missing = append(missing, ns)
		}
	}
	for ns := range recomputed {
		if !dispatched[ns] {
			unexpected = append(unexpected, ns)
		}
	}
	if len(missing) == 0 && len(unexpected) == 0 {
		return nil
	}
	sort.Strings(missing)
	sort.Strings(unexpected)
	return fmt.Errorf("reviewed vschema changes this deployment would not plan: %v; vschema changes this deployment would plan that were not reviewed: %v", missing, unexpected)
}

// existingIdempotentApply returns the apply previously created for
// req.IdempotencyKey, or nil when the key is empty or unseen. The match is
// returned regardless of the existing apply's state: the key encodes the
// dispatch generation (apply id + attempt + operation), so "same key" means
// "same generation", and a deliberate retry rotates the key via apply.Attempt.
// A stored apply whose environment, database, or type disagrees with the request
// signals an accidental key reuse or a control-plane bug, which is surfaced as
// an error rather than silently aliased.
func (c *LocalClient) existingIdempotentApply(ctx context.Context, req *ternv1.ApplyRequest) (*storage.Apply, error) {
	if req.IdempotencyKey == "" {
		return nil, nil
	}
	existing, err := c.storage.Applies().GetByIdempotencyKey(ctx, req.IdempotencyKey)
	if err != nil {
		return nil, fmt.Errorf("look up apply by idempotency key %q: %w", req.IdempotencyKey, err)
	}
	if existing == nil {
		return nil, nil
	}
	if existing.Environment != req.Environment || existing.Database != req.Database || existing.DatabaseType != req.Type {
		metrics.RecordRemoteApplyDedup(ctx, req.Database, req.Environment, "key_collision_refused")
		return nil, fmt.Errorf(
			"idempotency key %q already maps to apply %s (env=%s database=%s type=%s); refusing to alias request (env=%s database=%s type=%s)",
			req.IdempotencyKey, existing.ApplyIdentifier,
			existing.Environment, existing.Database, existing.DatabaseType,
			req.Environment, req.Database, req.Type,
		)
	}
	return existing, nil
}

// Apply executes a previously generated plan.
// In local mode, Apply has additional conflict checking and polls for completion.
//
// Two modes based on --defer-cutover:
//   - Independent (default): Each DDL runs as a separate Spirit call, cuts over independently
//   - Atomic (--defer-cutover): All DDLs run in one Spirit call, atomic cutover
func (c *LocalClient) Apply(ctx context.Context, req *ternv1.ApplyRequest) (*ternv1.ApplyResponse, error) {
	if req.PlanId == "" {
		return nil, fmt.Errorf("plan_id is required")
	}
	if req.Environment == "" {
		return nil, fmt.Errorf("environment is required")
	}

	// Idempotent re-dispatch: if this request's generation already created an
	// apply, return that apply's id instead of starting a duplicate. This runs
	// before plan resolution and the active-task conflict check so a re-dispatch
	// of our own in-flight apply is recovered rather than rejected as "already
	// in progress".
	if existing, err := c.existingIdempotentApply(ctx, req); err != nil {
		return nil, err
	} else if existing != nil {
		c.logger.Info("Apply: returning existing apply for idempotency key",
			"apply_id", existing.ApplyIdentifier,
			"idempotency_key", req.IdempotencyKey,
			"state", existing.State,
		)
		metrics.RecordRemoteApplyDedup(ctx, req.Database, req.Environment, "hit")
		return &ternv1.ApplyResponse{Accepted: true, ApplyId: existing.ApplyIdentifier}, nil
	}

	// Look up the plan, materializing it from the dispatch request when this
	// deployment did not plan locally (a non-primary deployment of a
	// multi-deployment apply).
	plan, err := c.planForApplyRequest(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("resolve plan %s for apply: %w", req.PlanId, err)
	}
	if plan == nil {
		return &ternv1.ApplyResponse{
			Accepted:     false,
			ErrorMessage: "plan not found",
		}, nil
	}
	ddlChanges := plan.FlatDDLChanges()
	c.logger.Info("Apply: retrieved plan",
		"plan_id", req.PlanId,
		"plan_identifier", plan.PlanIdentifier,
		"ddl_change_count", len(ddlChanges),
		"database", plan.Database,
	)

	// Local mode: check for active tasks with engine verification
	if err := c.checkActiveTaskConflict(ctx, plan); err != nil {
		// A same-key request that committed while we were in the conflict check
		// races as "already in progress". Re-resolve by idempotency key so the
		// winning apply is returned instead of a spurious rejection.
		if existing, lookupErr := c.existingIdempotentApply(ctx, req); lookupErr != nil {
			return nil, errors.Join(err, lookupErr)
		} else if existing != nil {
			c.logger.Info("Apply: idempotency key resolved an active-conflict race",
				"apply_id", existing.ApplyIdentifier,
				"idempotency_key", req.IdempotencyKey,
				"state", existing.State,
			)
			metrics.RecordRemoteApplyDedup(ctx, req.Database, req.Environment, "conflict_race")
			return &ternv1.ApplyResponse{Accepted: true, ApplyId: existing.ApplyIdentifier}, nil
		}
		return &ternv1.ApplyResponse{
			Accepted:     false,
			ErrorMessage: err.Error(),
		}, nil
	}

	// Get the appropriate engine
	eng := c.getEngine()
	if eng == nil {
		return nil, fmt.Errorf("no engine configured for type: %s", c.config.Type)
	}

	now := time.Now()

	options := req.Options

	caller := req.Caller
	if caller == "" {
		caller = options["caller"]
	}

	deferCutover := options["defer_cutover"] == "true"
	allowUnsafe := options["allow_unsafe"] == "true"

	// Build typed ApplyOptions for storage (booleans, not strings).
	// Revert window is ON by default — only disabled when skip_revert is explicitly set.
	skipRevert := options["skip_revert"] == "true"
	deferDeploy := options["defer_deploy"] == "true"
	applyOpts := storage.ApplyOptions{
		DeferCutover: deferCutover,
		DeferDeploy:  deferDeploy,
		AllowUnsafe:  allowUnsafe,
		SkipRevert:   skipRevert,
		Target:       plan.Target,
	}
	optionsJSON := storage.MarshalApplyOptions(applyOpts)

	// VSchema application is not modeled as a synthetic task. PlanetScale
	// surfaces its VSchema status/diff from engine resume metadata, and a sharded
	// apply runs VSchema as a task-less group_finalizer derived from the plan.

	// Build the Apply record (1 Apply -> N Tasks).
	applyIdentifier := "apply-" + strings.ReplaceAll(uuid.New().String(), "-", "")[:16]
	apply := &storage.Apply{
		ApplyIdentifier: applyIdentifier,
		PlanID:          plan.ID,
		Database:        plan.Database,
		DatabaseType:    plan.DatabaseType,
		Deployment:      c.config.Database,
		Repository:      plan.Repository,
		PullRequest:     plan.PullRequest,
		Environment:     req.Environment,
		Caller:          caller,
		Engine:          eng.Name(),
		State:           state.Apply.Pending,
		Options:         optionsJSON,
		IdempotencyKey:  req.IdempotencyKey,
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	c.logger.Info("Apply: creating tasks",
		"plan_id", plan.PlanIdentifier,
		"ddl_change_count", len(ddlChanges),
	)
	for i, ddlChange := range ddlChanges {
		c.logger.Debug("Apply: DDLChange",
			"index", i,
			"table", ddlChange.Table,
			"ddl", ddlChange.DDL,
		)
	}

	tasks := make([]*storage.Task, len(ddlChanges))
	for i, ddlChange := range ddlChanges {
		taskIdentifier := "task-" + strings.ReplaceAll(uuid.New().String(), "-", "")[:16]
		tasks[i] = &storage.Task{
			TaskIdentifier: taskIdentifier,
			PlanID:         plan.ID,
			Database:       plan.Database,
			DatabaseType:   plan.DatabaseType,
			Engine:         eng.Name(),
			Repository:     plan.Repository,
			PullRequest:    plan.PullRequest,
			Environment:    req.Environment,
			State:          state.Task.Pending,
			Options:        optionsJSON,
			TableName:      ddlChange.Table,
			Namespace:      ddlChange.Namespace,
			DDL:            ddlChange.DDL,
			DDLAction:      ddlChange.Operation,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
	}

	// Dual-write one apply_operations row alongside the applies row in the
	// same transaction so every apply created via the Tern client carries a
	// claimable, resumable operation. CreateWithTasksAndOperations links each
	// task to the single operation via ApplyOperationID, which the engine
	// resume-state path requires and the operator claim loop selects on.
	//
	// CutoverPolicy and HaltOnFailure are intentionally left unset: the Tern
	// client has no environment config to resolve them from (unlike the API
	// apply path), so the store applies its safe defaults (rolling cutover,
	// halt on failure).
	operations := []*storage.ApplyOperation{{
		Deployment: apply.Deployment,
		Target:     plan.Target,
		State:      state.ApplyOperation.Pending,
		CreatedAt:  now,
		UpdatedAt:  now,
	}}

	applyID, err := c.storage.Applies().CreateWithTasksAndOperations(ctx, apply, tasks, operations)
	if err != nil {
		// Two same-key dispatches racing to create: the loser sees a duplicate
		// idempotency key (or the active-apply guard the create runs). Resolve by
		// key so the winner's apply is returned instead of a create error.
		if existing, lookupErr := c.existingIdempotentApply(ctx, req); lookupErr != nil {
			return nil, fmt.Errorf("create apply %s with tasks and operations (idempotency re-lookup also failed): %w", applyIdentifier, errors.Join(err, lookupErr))
		} else if existing != nil {
			c.logger.Info("Apply: idempotency key resolved a create race",
				"apply_id", existing.ApplyIdentifier,
				"idempotency_key", req.IdempotencyKey,
				"state", existing.State,
			)
			metrics.RecordRemoteApplyDedup(ctx, req.Database, req.Environment, "create_race")
			return &ternv1.ApplyResponse{Accepted: true, ApplyId: existing.ApplyIdentifier}, nil
		}
		return nil, fmt.Errorf("create apply %s with tasks and operations: %w", applyIdentifier, err)
	}
	apply.ID = applyID

	// Log apply started
	c.logApplyEvent(ctx, applyID, nil, storage.LogLevelInfo, storage.LogEventInfo, storage.LogSourceSchemaBot,
		fmt.Sprintf("Apply started: %s", applyIdentifier), "", state.Apply.Pending)

	// Direct client calls can still register a pending observer before starting
	// the engine. API-created applies use the service-level observer registry
	// because operator drivers dispatch them asynchronously.
	if obs := c.consumePendingObserver(); obs != nil {
		// Set the apply ID on the observer if it supports it (e.g., CommentObserver
		// needs the ID to look up tracked comments for editing).
		type applyIDSetter interface{ SetApplyID(int64) }
		if setter, ok := obs.(applyIDSetter); ok {
			setter.SetApplyID(apply.ID)
		}
		c.SetObserver(apply.ID, obs)
	}

	// Start apply in background with cancellable context (Stop() cancels this)
	applyCtx, cancelApply := context.WithCancel(context.WithoutCancel(ctx))
	cancelGeneration := c.setApplyCancel(cancelApply)
	// A fresh dispatch (including a remote gRPC drive) has no operation context,
	// so it never auto-parks at the cutover barrier; ordered-cutover parking is
	// driven by the operator's operation-scoped resume path.
	c.startApplyExecution(applyCtx, cancelGeneration, cancelApply, apply, tasks, plan, options, false)

	return &ternv1.ApplyResponse{
		Accepted: true,
		ApplyId:  apply.ApplyIdentifier,
	}, nil
}

// getEngine returns the appropriate engine based on database type.
func (c *LocalClient) getEngine() engine.Engine {
	switch c.config.Type {
	case storage.DatabaseTypeMySQL:
		return c.spiritEngine
	case storage.DatabaseTypeVitess:
		return c.planetscaleEngine
	default:
		// A registered engine for a non-built-in type (nil if none registered).
		return c.customEngine
	}
}

// engineProgressIsExternallyAuthoritative reports whether the progress read path
// may query the engine directly for live progress, or must serve progress from
// shared storage instead.
//
// One logical data-plane route can be served by multiple instances that share
// storage. A progress request can be balanced onto any instance. An engine
// whose progress comes from instance-local memory only knows the schema change
// that instance is running, so an instance that is not driving the queried
// schema change would observe unrelated or stale state — its progress must come
// from storage, which the driving instance keeps current. An engine whose
// progress comes from authoritative external state returns the same correct
// result on every instance and may be queried directly.
//
// This fails closed: an engine that does not declare its progress authoritative
// is served from storage.
func engineProgressIsExternallyAuthoritative(eng engine.Engine) bool {
	source, ok := eng.(engine.ExternallyAuthoritativeProgress)
	return ok && source.ProgressIsExternallyAuthoritative()
}

// SupportsShardedApplyFanout reports whether this local client can drive
// sharded work as independent SchemaBot apply operations. Engines that expose
// externally authoritative progress own their execution ordering outside
// SchemaBot, so apply-create must keep them as one operation.
func (c *LocalClient) SupportsShardedApplyFanout() bool {
	return !engineProgressIsExternallyAuthoritative(c.getEngine())
}

// Progress returns detailed progress for an active schema change.
// Returns ALL tasks for the current apply: completed, running, and pending.
// req.ApplyId is required so progress is always scoped to a single apply.
func (c *LocalClient) Progress(ctx context.Context, req *ternv1.ProgressRequest) (*ternv1.ProgressResponse, error) {
	var tasks []*storage.Task
	var err error

	if req.ApplyId == "" {
		return nil, fmt.Errorf("apply_id is required")
	}

	apply, lookupErr := c.storage.Applies().GetByApplyIdentifier(ctx, req.ApplyId)
	if lookupErr != nil {
		return nil, fmt.Errorf("get apply %s: %w", req.ApplyId, lookupErr)
	}
	if apply == nil {
		return nil, fmt.Errorf("get apply %s: %w", req.ApplyId, storage.ErrApplyNotFound)
	}
	tasks, err = c.storage.Tasks().GetByApplyID(ctx, apply.ID)
	if err != nil {
		return nil, fmt.Errorf("get tasks for apply %s: %w", req.ApplyId, err)
	}
	if len(tasks) == 0 {
		// A task-less apply — e.g. a VSchema-only apply driven by a
		// group_finalizer, which carries no task rows. Serve its state from the
		// apply row, which the operator maintains by deriving it from the
		// operation rows; the API progress handler overlays the engine display
		// fields (VSchema status/diff, branch) from the operations' resume
		// metadata. An apply with neither tasks nor operations is genuinely
		// taskless work and stays the fail-closed not-found case.
		ops, opErr := c.storage.ApplyOperations().ListByApply(ctx, apply.ID)
		if opErr != nil {
			return nil, fmt.Errorf("list apply_operations for apply %s: %w", req.ApplyId, opErr)
		}
		if len(ops) == 0 {
			return nil, fmt.Errorf("get tasks for apply %s: %w", req.ApplyId, storage.ErrTaskNotFound)
		}
		c.logger.Info("Progress: serving task-less apply from operations",
			"apply_id", req.ApplyId, "operation_count", len(ops), "state", apply.State)
		return &ternv1.ProgressResponse{
			State:  storageStateToProto(apply.State),
			Engine: c.protoEngine(),
		}, nil
	}

	c.logger.Debug("Progress: found tasks", "count", len(tasks), "database", c.config.Database, "apply_id", req.ApplyId)
	for _, t := range tasks {
		c.logger.Debug("Progress: task", "task_id", t.TaskIdentifier, "state", t.State, "is_terminal", state.IsTerminalTaskState(t.State))
	}

	// Find the most relevant task to determine overall apply state:
	// Priority: RUNNING > WAITING_FOR_CUTOVER > CUTTING_OVER > STOPPED > PENDING > terminal states
	// This ensures we show progress for the task that's actually executing.
	var activeTask *storage.Task
	var pendingTask *storage.Task
	var stoppedTask *storage.Task
	var latestTask *storage.Task
	for _, t := range tasks {
		switch {
		case t.State == state.Task.Running ||
			t.State == state.Task.WaitingForCutover ||
			t.State == state.Task.Recovering ||
			t.State == state.Task.CuttingOver ||
			t.State == state.Task.RevertWindow:
			// Prefer actively running/waiting tasks
			activeTask = t
		case t.State == state.Task.Stopped:
			// Stopped tasks are resumable — track them separately
			if stoppedTask == nil {
				stoppedTask = t
			}
		case t.State == state.Task.Pending:
			// Track first pending task as fallback
			if pendingTask == nil {
				pendingTask = t
			}
		case state.IsTerminalTaskState(t.State):
			// Track most recent terminal task as final fallback
			if latestTask == nil {
				latestTask = t
			}
		default:
			// Unknown/new state — still select as fallback to avoid losing engine context
			c.logger.Warn("unexpected task state in progress", "task_id", t.TaskIdentifier, "state", t.State)
			if latestTask == nil {
				latestTask = t
			}
		}
		// Stop searching once we find a running task
		if activeTask != nil {
			break
		}
	}

	// Use active task if found, otherwise stopped, pending, or latest terminal
	if activeTask == nil {
		activeTask = stoppedTask
	}
	if activeTask == nil {
		activeTask = pendingTask
	}
	if activeTask == nil {
		activeTask = latestTask
	}

	if activeTask == nil {
		return &ternv1.ProgressResponse{
			State:  ternv1.State_STATE_NO_ACTIVE_CHANGE,
			Engine: c.protoEngine(),
		}, nil
	}
	c.logger.Info("Progress: selected task", "task_id", activeTask.TaskIdentifier, "state", activeTask.State, "apply_id", activeTask.ApplyID)

	// Get ALL tasks for this apply (completed + running + pending)
	currentApplyTasks := filterTasksByApply(tasks, activeTask.ApplyID)

	// Progress renders entirely from stored state. The operator's lease-held
	// drive (pollForCompletionAtomic) is the sole engine poller: it advances task
	// and apply state, terminalizes the apply, and persists per-shard rows and
	// engine resume state every tick. Readers never poll the engine — an
	// instance-local engine has no live result to read, and for an externally-
	// authoritative engine the drive keeps stored current.
	var engineMetadata map[string]string
	var vitessApplyIsInstant bool
	if c.config.Type == storage.DatabaseTypeVitess {
		engineMetadata = c.loadStoredDisplayMetadata(ctx, activeTask)
		vitessApplyIsInstant = engineMetadata["is_instant"] == "true"
	}

	// Build tables array with ALL tasks for this apply
	tables := make([]*ternv1.TableProgress, 0, len(currentApplyTasks))

	// summary has no stored source on the read path; errorMessage falls back to
	// the failed task rows below.
	var summary string
	var errorMessage string

	// The per-shard rows the operator's drive persists, grouped by table — the
	// single read surface for sharded progress.
	storedShards := c.loadStoredShardsByTable(ctx, apply, currentApplyTasks)

	for _, t := range currentApplyTasks {
		tp := &ternv1.TableProgress{
			TableName:  t.TableName,
			Ddl:        t.DDL,
			Namespace:  t.Namespace,
			Status:     t.State,
			TaskId:     t.TaskIdentifier,
			IsInstant:  t.IsInstant || vitessApplyIsInstant,
			ChangeType: ddlActionToProtoChangeType(t.DDLAction),
		}

		// Table figures come from the stored task row the drive maintains.
		tp.PercentComplete = int32(t.ProgressPercent)
		tp.RowsCopied = t.RowsCopied
		tp.RowsTotal = t.RowsTotal
		// Clamp to 100% only for successfully completed tasks — Vitess row
		// counts can lag slightly due to concurrent inserts during copy.
		if state.IsState(t.State, state.Task.Completed) && t.RowsTotal > 0 {
			tp.PercentComplete = 100
			if tp.RowsCopied < tp.RowsTotal {
				tp.RowsCopied = tp.RowsTotal
			}
		}
		if vitessApplyIsInstant && state.IsState(t.State, state.Task.Completed) {
			tp.PercentComplete = 100
		}

		// When per-shard rows are persisted, the table headline (rows, percent,
		// ETA) is the aggregate of those rows — computed at read time so a reader
		// that does not poll the engine is correct.
		storedForTable := storedShards[progressTableKey(t.Namespace, t.TableName)]
		if len(storedForTable) > 0 {
			rowsCopied, rowsTotal, etaSeconds, percent := aggregateStoredShards(storedForTable)
			tp.RowsCopied = rowsCopied
			tp.RowsTotal = rowsTotal
			tp.EtaSeconds = etaSeconds
			tp.PercentComplete = percent
		}

		// The per-shard breakdown renders from those persisted rows — stored is
		// the single read surface, never the live engine result.
		tp.Shards = shardProgressProto(storedForTable)

		tables = append(tables, tp)
	}

	// Derive overall state from ALL tasks in this apply.
	// If tasks are all pending, check the apply record for a more specific state
	// (e.g., preparing_branch, creating_deploy_request during PlanetScale setup).
	overallState := deriveOverallState(currentApplyTasks)
	// For Vitess setup phases, the apply record has a more specific state
	// (preparing_branch, applying_branch_changes, creating_deploy_request)
	// than what task states alone can derive. Check the apply record when
	// tasks are still pending or when the overall state doesn't yet reflect
	// real progress (e.g., engine returns "running" during setup).
	if applyRec, err := c.storage.Applies().Get(ctx, activeTask.ApplyID); err == nil && applyRec != nil {
		switch {
		case state.IsSetupPhase(applyRec.State):
			c.logger.Debug("Progress: overriding task-derived state with apply record setup phase",
				"task_derived", overallState, "apply_record", applyRec.State)
			overallState = applyRec.State
		case state.IsState(applyRec.State, state.Apply.FailedRetryable):
			overallState = applyRec.State
		case state.IsTerminalApplyState(applyRec.State):
			overallState = applyRec.State
		}
	}

	// If no error from engine, check stored task errors (for restart recovery)
	if errorMessage == "" {
		for _, t := range currentApplyTasks {
			if (t.State == state.Task.Failed || t.State == state.Task.FailedRetryable) && t.ErrorMessage != "" {
				errorMessage = t.ErrorMessage
				break
			}
		}
	}

	// Clamp per-table status to match overall state. Engine per-table progress
	// can report individual table work as completed while the grouped apply is
	// still in revert window.
	if state.IsState(overallState, state.Apply.RevertWindow) {
		for _, tp := range tables {
			if state.IsState(tp.Status, state.Apply.Completed) {
				tp.Status = state.Apply.RevertWindow
			}
		}
	}

	resp := &ternv1.ProgressResponse{
		State:        storageStateToProto(overallState),
		Engine:       c.protoEngine(), // default from client config
		Tables:       tables,
		Summary:      summary,
		ErrorMessage: errorMessage,
	}

	// Surface the engine's display metadata (e.g. PlanetScale branch_name,
	// deploy_request_url, is_instant) on the response so the renderer reads it
	// from the progress projection rather than an engine-specific side table.
	for k, v := range engineMetadata {
		if resp.Metadata == nil {
			resp.Metadata = make(map[string]string, len(engineMetadata))
		}
		resp.Metadata[k] = v
	}

	// Populate apply_id, engine, and volume from the apply record.
	// The apply record's engine is the source of truth (set at apply creation time).
	if apply, err := c.storage.Applies().Get(ctx, activeTask.ApplyID); err == nil && apply != nil {
		resp.ApplyId = apply.ApplyIdentifier
		if eng, err := engineNameToProto(apply.Engine); err != nil {
			return nil, fmt.Errorf("invalid engine on apply %s: %w", apply.ApplyIdentifier, err)
		} else {
			resp.Engine = eng
		}
		opts := storage.ParseApplyOptions(apply.Options)
		resp.Volume = int32(opts.Volume)
		if opts.Branch != "" {
			resp.Metadata = ensureMetadata(resp.Metadata)
			resp.Metadata["existing_branch"] = opts.Branch
		}

		// During branch setup phases, include the latest event message so the
		// CLI can show what's happening instead of a static spinner.
		if state.IsState(overallState, state.Apply.PreparingBranch, state.Apply.ApplyingBranchChanges, state.Apply.CreatingDeployRequest) {
			if logs, err := c.storage.ApplyLogs().GetByApply(ctx, apply.ID); err == nil && len(logs) > 0 {
				latest := logs[len(logs)-1]
				resp.Metadata = ensureMetadata(resp.Metadata)
				resp.Metadata["status_detail"] = latest.Message
			}
		}
	}

	return resp, nil
}

// loadStoredShardsByTable loads the persisted per-shard rows for an apply's
// operations, grouped by (namespace, table), so the progress response renders
// the per-shard breakdown from storage. It is Vitess-only (other engines have no
// shard rows). A load error for one operation is logged and skipped, keeping the
// rows already loaded for other operations; tables whose rows could not be
// loaded render an empty breakdown until the next successful load rather than
// failing the whole response.
func (c *LocalClient) loadStoredShardsByTable(ctx context.Context, apply *storage.Apply, tasks []*storage.Task) map[string][]*storage.Task {
	if c.config.Type != storage.DatabaseTypeVitess {
		return nil
	}
	byTable := map[string][]*storage.Task{}
	seenOps := map[int64]bool{}
	for _, t := range tasks {
		if t.ApplyOperationID == nil || seenOps[*t.ApplyOperationID] {
			continue
		}
		seenOps[*t.ApplyOperationID] = true
		shardTasks, err := c.storage.Tasks().GetShardProgressByApplyOperationID(ctx, *t.ApplyOperationID)
		if err != nil {
			c.logger.Warn("operation's per-shard breakdown will be empty this poll: failed to load its persisted shard rows",
				"apply_id", apply.ApplyIdentifier, "apply_operation_id", *t.ApplyOperationID, "error", err)
			continue
		}
		for _, st := range shardTasks {
			key := progressTableKey(st.Namespace, st.TableName)
			byTable[key] = append(byTable[key], st)
		}
	}
	return byTable
}

// loadStoredDisplayMetadata reads a Vitess apply's deploy display fields
// (branch_name, deploy_request_url, is_instant, deferred_deploy) from the
// operation's persisted engine resume state — the drive's write-through is the
// source, the read path never polls the engine. Returns nil before the first
// write-through or on a decode/load error, so the response simply omits the
// display fields until the drive catches up.
func (c *LocalClient) loadStoredDisplayMetadata(ctx context.Context, task *storage.Task) map[string]string {
	operationID, err := applyOperationIDForTask(task)
	if err != nil {
		c.logger.Debug("progress: no apply operation for display metadata", "task_id", task.TaskIdentifier, "error", err)
		return nil
	}
	rs, err := c.storage.ApplyOperations().GetEngineResumeState(ctx, operationID)
	if err != nil {
		// Not-found is expected before the drive's first write-through.
		c.logger.Debug("progress: no engine resume state for display metadata", "task_id", task.TaskIdentifier, "apply_operation_id", operationID, "error", err)
		return nil
	}
	display, err := PSDisplayMetadata(rs.Metadata)
	if err != nil {
		c.logger.Warn("progress response will omit engine display fields: failed to decode engine resume state",
			"task_id", task.TaskIdentifier, "apply_operation_id", operationID, "error", err)
		return nil
	}
	return display
}

// aggregateStoredShards computes a table's headline figures from its persisted
// per-shard rows — the per-table number is computed at read time, never stored.
// Rows are summed (a completed shard's copied count is clamped up to its total,
// since row counts can lag concurrent inserts), ETA is the slowest shard, and
// percent is derived from the summed rows.
func aggregateStoredShards(shards []*storage.Task) (rowsCopied, rowsTotal, etaSeconds int64, percent int32) {
	for _, sh := range shards {
		rowsTotal += sh.RowsTotal
		copied := sh.RowsCopied
		if state.IsState(sh.State, state.Task.Completed) && sh.RowsTotal > 0 && copied < sh.RowsTotal {
			copied = sh.RowsTotal
		}
		rowsCopied += copied
		if int64(sh.ETASeconds) > etaSeconds {
			etaSeconds = int64(sh.ETASeconds)
		}
	}
	if rowsTotal > 0 {
		percent = int32(min(rowsCopied*100/rowsTotal, 100))
	}
	return rowsCopied, rowsTotal, etaSeconds, percent
}

// shardProgressProto renders a table's per-shard breakdown from the persisted
// shard rows — the durable read-model the operator's lease-held drive maintains.
// Stored state is the single read surface: the breakdown is never read from the
// live engine result. Before the drive's first write-through (or while a load
// failed) there are no rows yet and the breakdown is empty until the drive
// catches up.
func shardProgressProto(stored []*storage.Task) []*ternv1.ShardProgress {
	if len(stored) == 0 {
		return nil
	}
	out := make([]*ternv1.ShardProgress, len(stored))
	for i, s := range stored {
		out[i] = &ternv1.ShardProgress{
			Shard:           s.Shard,
			Status:          state.NormalizeShardStatus(s.State),
			RowsCopied:      s.RowsCopied,
			RowsTotal:       s.RowsTotal,
			EtaSeconds:      int64(s.ETASeconds),
			CutoverAttempts: int32(s.CutoverAttempts),
		}
	}
	return out
}

// taskStateWithNoBackwardProgress applies the engine -> task -> apply ordering:
// raw engine progress is first translated into a canonical task state, but a
// stale engine poll cannot move a stored task back to an earlier phase. This
// happens after restarts and terminal races where durable task storage is ahead
// of a lagging per-table progress snapshot.
func taskStateWithNoBackwardProgress(storedTaskState, engineTaskState string) string {
	storedTaskState = state.NormalizeTaskStatus(storedTaskState)
	engineTaskState = state.NormalizeTaskStatus(engineTaskState)

	// A terminal stored task is already the durable final answer.
	if state.IsTerminalTaskState(storedTaskState) {
		return storedTaskState
	}

	// Terminal engine results, stopped tasks, and retryable failures are real
	// outcomes from the current engine poll and can advance active storage.
	if state.IsTerminalTaskState(engineTaskState) ||
		state.IsState(engineTaskState, state.Task.Stopped, state.Task.FailedRetryable) {
		return engineTaskState
	}

	// Recovering is a temporary operator-owned wrapper while an engine reattaches
	// after restart. Recovery starts only after storage had already reached
	// waiting_for_cutover, so row-copy progress during reattach must not move
	// storage backward to running. Row counters can still be displayed from live
	// engine progress while the durable state stays cutover-blocking.
	if isRecoveryState(storedTaskState) && recoveryCompleteWithEngineState(engineTaskState) {
		return engineTaskState
	}

	// Vitess deferred deploy reports running during deploy-request setup, then
	// waiting_for_deploy once the request is ready for an operator start. That is
	// forward progress even though the generic rank order treats running as later.
	if state.IsState(storedTaskState, state.Task.Running) && state.IsState(engineTaskState, state.Task.WaitingForDeploy) {
		return engineTaskState
	}

	// Operator/control-owned states block stale active engine progress.
	if blocksActiveEngineProgress(storedTaskState) {
		return storedTaskState
	}

	engineProgressRank, engineProgressRanked := activeTaskProgressRank(engineTaskState)
	storedProgressRank, storedProgressRanked := activeTaskProgressRank(storedTaskState)

	// Unknown future canonical task states should not be ordered implicitly.
	if !engineProgressRanked || !storedProgressRanked {
		return storedTaskState
	}

	// For ordinary active phases, never let storage/display move backward.
	if engineProgressRank < storedProgressRank {
		return storedTaskState
	}
	return engineTaskState
}

// blocksActiveEngineProgress identifies durable operator/control states that
// should not be overwritten by a stale active engine poll. For example, a user
// can stop a task while the engine still reports running for a short window, or
// the operator can mark a task failed_retryable before a retry claims it.
func blocksActiveEngineProgress(taskState string) bool {
	return state.IsState(taskState, state.Task.Stopped, state.Task.FailedRetryable)
}

func isRecoveryState(taskState string) bool {
	return state.IsState(taskState, state.Task.Recovering)
}

func recoveryCompleteWithEngineState(taskState string) bool {
	return state.IsState(taskState,
		state.Task.WaitingForCutover,
	)
}

// activeTaskProgressRank orders ordinary active task phases. Terminal states
// and operator/control-owned states are handled before this helper, so new
// task states must be consciously assigned to one of those policies.
func activeTaskProgressRank(taskState string) (int, bool) {
	switch state.NormalizeTaskStatus(taskState) {
	case state.Task.Pending:
		return 0, true
	case state.Task.WaitingForDeploy:
		return 1, true
	case state.Task.Running:
		return 2, true
	case state.Task.Checksumming:
		// Row copy is done; the engine is verifying the copied data. Ranks after
		// Running and before WaitingForCutover — the phase the table moves through
		// next — so a later poll never regresses a checksumming table to Running.
		return 3, true
	case state.Task.WaitingForCutover:
		return 4, true
	case state.Task.CuttingOver:
		return 5, true
	case state.Task.RevertWindow:
		return 6, true
	default:
		return 0, false
	}
}

func ensureMetadata(m map[string]string) map[string]string {
	if m == nil {
		return make(map[string]string)
	}
	return m
}

// dsnLogAttrs returns slog key/value attributes describing a target DSN using
// only non-sensitive fields (network address and database name). The DSN
// password and raw DSN string are never included, so these attributes are safe
// to emit in logs. If the DSN cannot be parsed, the attributes record that
// parsing failed without echoing any part of the DSN, since a parse error
// message can contain fragments of the credential-bearing string.
func dsnLogAttrs(dsn string) []any {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return []any{"target_dsn_parsed", false}
	}
	return []any{
		"target_addr", cfg.Addr,
		"target_db", cfg.DBName,
	}
}
