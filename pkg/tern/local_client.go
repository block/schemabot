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
	for _, sc := range result.Changes {
		ns := c.planNamespace(sc.Namespace)
		nsData := namespaces[ns]
		if nsData == nil {
			nsData = &storage.NamespacePlanData{}
			namespaces[ns] = nsData
		}
		for _, tc := range sc.TableChanges {
			nsData.Tables = append(nsData.Tables, storage.TableChange{
				Table:     tc.Table,
				DDL:       tc.DDL,
				Operation: ddl.StatementTypeToOp(tc.Operation),
			})
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

	// Convert engine SchemaChanges to proto SchemaChanges.
	var changes []*ternv1.SchemaChange
	for _, sc := range result.Changes {
		ns := c.planNamespace(sc.Namespace)
		protoSC := &ternv1.SchemaChange{
			Namespace:             ns,
			Metadata:              sc.Metadata,
			OriginalFiles:         sc.OriginalFiles,
			OriginalFilesCaptured: sc.OriginalFilesCaptured,
		}
		for _, t := range sc.TableChanges {
			protoSC.TableChanges = append(protoSC.TableChanges, &ternv1.TableChange{
				TableName:    t.Table,
				ChangeType:   changeTypeToProto(t.Operation),
				Ddl:          t.DDL,
				IsUnsafe:     t.IsUnsafe,
				UnsafeReason: t.UnsafeReason,
				Namespace:    ns,
			})
		}
		changes = append(changes, protoSC)
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

	return &ternv1.PlanResponse{
		PlanId:         result.PlanID,
		Engine:         c.protoEngine(),
		Changes:        changes,
		LintViolations: violations,
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

	// Look up the plan
	plan, err := c.storage.Plans().Get(ctx, req.PlanId)
	if err != nil {
		return nil, fmt.Errorf("get plan: %w", err)
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

	// Create VSchema tasks for namespaces with VSchema changes so the
	// progress API and TUI can track VSchema application alongside DDL.
	// For VSchema-only deploys (0 DDL changes), this gives the progress API
	// something to track.
	for ns, nsData := range plan.Namespaces {
		if namespaceHasVSchemaArtifact(nsData) {
			ddlChanges = append(ddlChanges, storage.TableChange{
				Table:     "VSchema: " + ns,
				Namespace: ns,
				Operation: "vschema_update",
			})
		}
	}

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
		return nil, fmt.Errorf("create apply %s with tasks and operations: %w", applyIdentifier, err)
	}
	apply.ID = applyID

	// Log apply started
	c.logApplyEvent(ctx, applyID, nil, storage.LogLevelInfo, storage.LogEventInfo, storage.LogSourceSchemaBot,
		fmt.Sprintf("Apply started: %s", applyIdentifier), "", state.Apply.Pending)

	// Direct client calls can still register a pending observer before starting
	// the engine. API-created applies use the service-level observer registry
	// because operator workers dispatch them asynchronously.
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
	c.startApplyExecution(applyCtx, cancelGeneration, cancelApply, apply, tasks, plan, options)

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
		return nil, fmt.Errorf("get tasks for apply %s: %w", req.ApplyId, storage.ErrTaskNotFound)
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

	// Scope to the active task's apply operation before deriving the aggregate
	// apply state. currentApplyTasks is scoped by apply, so once an apply fans
	// out to multiple operations it is a mixed set; deriving the aggregate from
	// that mix would bypass the sibling operation rows and the on_failure policy.
	// currentApplyTasks is still used below for the response table list and the
	// all-terminal gate. With one operation per apply the two slices are equal,
	// so this is a no-op until the multi-deployment fan-out lands.
	opScopedTasks := currentApplyTasks
	if activeTask.ApplyOperationID != nil {
		opScopedTasks = tasksForOperation(currentApplyTasks, *activeTask.ApplyOperationID)
	}

	creds := c.credentials()
	eng := c.getEngine()

	// Get live progress from engine for the currently running task.
	// For Vitess, load opaque ResumeState so the engine can poll the deploy
	// request and query SHOW VITESS_MIGRATIONS.
	var engineResult *engine.ProgressResult
	var vitessApplyIsInstant bool
	// Query engine for live progress. For Vitess, also query during pending state
	// to surface PlanetScale states (preparing branch, deploy request, etc.).
	queryDuringPending := c.config.Type == storage.DatabaseTypeVitess
	// A stopped task is SchemaBot-owned state. Do not let a stale engine poll
	// report an older active state such as waiting_for_cutover and overwrite it.
	queryLiveProgress := activeTask.State != state.Task.Pending && activeTask.State != state.Task.Stopped
	if queryDuringPending && activeTask.State == state.Task.Pending {
		queryLiveProgress = true
	}
	if eng != nil && queryLiveProgress {
		progressReq := &engine.ProgressRequest{
			Database:    c.config.Database,
			Credentials: creds,
		}
		if c.config.Type == storage.DatabaseTypeVitess {
			resumeState, resumeErr := c.loadEngineResumeState(ctx, activeTask)
			if resumeErr != nil {
				c.logger.Error("failed to load Vitess engine resume state for progress", "apply_id", activeTask.ApplyID, "task_id", activeTask.TaskIdentifier, "error", resumeErr)
				return nil, fmt.Errorf("load Vitess engine resume state for progress task %s: %w", activeTask.TaskIdentifier, resumeErr)
			}
			progressReq.ResumeState = resumeState
			vad, vadErr := c.storage.VitessApplyData().GetByApplyID(ctx, activeTask.ApplyID)
			switch {
			case vadErr != nil:
				c.logger.Error("failed to load VitessApplyData for progress", "apply_id", activeTask.ApplyID, "error", vadErr)
			case vad == nil:
				c.logger.Warn("VitessApplyData not found for progress — apply may still be initializing", "apply_id", activeTask.ApplyID)
			default:
				vitessApplyIsInstant = vad.IsInstant
			}
		}
		result, err := eng.Progress(ctx, progressReq)
		if err == nil {
			engineResult = result
			if c.config.Type == storage.DatabaseTypeVitess && result.ResumeState != nil {
				operationID, operationErr := applyOperationIDForTask(activeTask)
				if operationErr != nil {
					c.logger.Error("failed to resolve apply operation for Vitess engine resume state from progress", "apply_id", apply.ApplyIdentifier, "task_id", activeTask.TaskIdentifier, "error", operationErr)
					return nil, fmt.Errorf("resolve apply operation for Vitess engine resume state from progress task %s: %w", activeTask.TaskIdentifier, operationErr)
				}
				if saveErr := c.saveEngineResumeStateForOperation(ctx, operationID, result.ResumeState); saveErr != nil {
					c.logger.Error("failed to save Vitess engine resume state from progress", "apply_id", apply.ApplyIdentifier, "error", saveErr)
					return nil, fmt.Errorf("save Vitess engine resume state from progress apply %s: %w", apply.ApplyIdentifier, saveErr)
				}
			}
			c.logger.Info("Progress: engine returned", "engine_state", result.State, "message", result.Message, "task_id", activeTask.TaskIdentifier, "storage_state", activeTask.State)
			engineTaskState := taskStateFromProgressResult(result)
			taskState := taskStateWithNoBackwardProgress(activeTask.State, engineTaskState)
			if !state.IsState(taskState, engineTaskState) {
				c.logger.Warn("keeping stored task state because engine progress reported earlier state",
					"task_id", activeTask.TaskIdentifier,
					"stored_state", activeTask.State,
					"engine_task_state", engineTaskState)
			}

			// Engine state is translated to task state first. Stored task state
			// can stay ahead of a stale engine poll; apply state is derived after
			// task rows are coherent.
			if !state.IsTerminalTaskState(activeTask.State) {
				oldTaskState := activeTask.State
				activeTask.State = taskState
				now := time.Now()
				activeTask.UpdatedAt = now
				if state.IsTerminalTaskState(taskState) && activeTask.CompletedAt == nil {
					activeTask.CompletedAt = &now
				}
				if result.State == engine.StateFailed && activeTask.ErrorMessage == "" {
					activeTask.ErrorMessage = progressFailureMessage(result)
				}
				if err := c.storage.Tasks().Update(ctx, activeTask); err != nil {
					c.logger.Warn("failed to update task state from progress poll", "task_id", activeTask.TaskIdentifier, "state", activeTask.State, "error", err)
				}
				if !state.IsState(oldTaskState, taskState) && !state.IsTerminalTaskState(taskState) {
					if apply, err := c.storage.Applies().Get(ctx, activeTask.ApplyID); err != nil {
						c.logger.Warn("failed to load apply after progress task state update", "apply_id", activeTask.ApplyID, "error", err)
					} else if apply != nil && !state.IsTerminalApplyState(apply.State) {
						if derived, ok := c.deriveAggregateApplyState(ctx, apply, opScopedTasks); ok {
							// Compare-and-swap on the just-read state so a stale
							// projection cannot clobber a newer state a sibling
							// drive already wrote.
							swapped, err := c.storage.Applies().UpdateDerivedState(ctx, apply.ID, apply.State, derived, apply.ErrorMessage, apply.StartedAt, apply.CompletedAt)
							if err != nil {
								c.logger.Warn("failed to update apply after progress task state update", "apply_id", apply.ApplyIdentifier, "state", derived, "error", err)
							} else if !swapped {
								c.logger.Debug("apply progress projection write lost a race; next poll reconciles", "apply_id", apply.ApplyIdentifier, "expected_state", apply.State, "derived_state", derived)
							}
						}
					}
				}
			}

			// Also update the apply record if the engine reports a terminal state
			// but the apply hasn't been updated yet. Only do this when ALL tasks
			// for this apply are terminal — in sequential mode, the engine reports
			// "completed" per-task, but the apply isn't done until all tasks finish.
			if result.State.IsTerminal() {
				retryableFailure := state.IsState(taskState, state.Task.FailedRetryable)
				allTerminal := !retryableFailure
				for _, t := range currentApplyTasks {
					if !state.IsTerminalTaskState(t.State) {
						allTerminal = false
						break
					}
				}
				if retryableFailure || allTerminal {
					apply, _ := c.storage.Applies().GetByApplyIdentifier(ctx, req.ApplyId)
					if apply != nil && !state.IsTerminalApplyState(apply.State) {
						// Terminalize the parent apply on the rollout projection
						// over all sibling operations, not this operation's tasks
						// alone: under on_failure=continue a failed operation must
						// hold the apply running while siblings are in flight. With
						// one operation per apply the projection equals this
						// operation's derived state, so this is a no-op until the
						// multi-deployment fan-out lands.
						expectedState := apply.State
						derived, ok := c.deriveAggregateApplyState(ctx, apply, opScopedTasks)
						quiesce, _, stampCompletedAt := applyQuiesceDecision(derived)
						if ok && quiesce {
							now := time.Now()
							apply.State = derived
							if stampCompletedAt {
								apply.CompletedAt = &now
							} else {
								apply.CompletedAt = nil
							}
							// Prefer this operation's engine failure message; fall back
							// to the failed task rows when the rollout projection resolves
							// the apply to a failure due to a sibling operation that this
							// engine result doesn't reflect.
							if result.State == engine.StateFailed {
								if msg := progressFailureMessage(result); msg != "" {
									apply.ErrorMessage = msg
								}
							}
							ensureApplyFailureMessage(apply, opScopedTasks)
							apply.UpdatedAt = now
							// Compare-and-swap on the just-read state so a stale
							// projection cannot clobber a newer state a sibling
							// drive already wrote.
							swapped, err := c.storage.Applies().UpdateDerivedState(ctx, apply.ID, expectedState, apply.State, apply.ErrorMessage, apply.StartedAt, apply.CompletedAt)
							switch {
							case err != nil:
								c.logger.Warn("failed to update apply from progress poll", "apply_id", apply.ApplyIdentifier, "state", apply.State, "apply_db_id", apply.ID, "error", err)
							case !swapped:
								c.logger.Debug("apply terminal projection write lost a race; next poll reconciles", "apply_id", apply.ApplyIdentifier, "expected_state", expectedState, "derived_state", derived)
							default:
								c.logger.Info("apply state updated from progress polling", "apply_id", apply.ApplyIdentifier, "state", apply.State)
							}
						}
					}
				}
			}
		}
	}

	// Build tables array with ALL tasks for this apply
	tables := make([]*ternv1.TableProgress, 0, len(currentApplyTasks))
	var summary string

	// Build a map of engine table progress by namespace/table for fast lookup.
	// Vitess commonly has the same table name in multiple keyspaces.
	var engineTableProgress map[string]*engine.TableProgress
	var errorMessage string
	if engineResult != nil {
		engineTableProgress = indexEngineTableProgress(engineResult.Tables)
		summary = engineResult.Message
		errorMessage = engineResult.ErrorMessage
	}

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

		// Look up engine progress for this table
		if et, ok := engineProgressForTask(engineTableProgress, t); ok {
			tp.Status = progressTableStatus(t.State, et.State)
			tp.PercentComplete = int32(et.Progress)
			tp.RowsCopied = et.RowsCopied
			tp.RowsTotal = et.RowsTotal
			tp.EtaSeconds = et.ETASeconds
			tp.IsInstant = et.IsInstant
			tp.ProgressDetail = et.ProgressDetail

			if syncStoredTaskProgressFromEngineTable(t, et, time.Now()) {
				if err := c.storage.Tasks().Update(ctx, t); err != nil {
					c.logger.Error("failed to update task progress from engine",
						"task_id", t.TaskIdentifier,
						"table", t.TableName,
						"rows_copied", t.RowsCopied,
						"rows_total", t.RowsTotal,
						"progress_percent", t.ProgressPercent,
						"error", err)
				}
			}

			// Build shards if available
			shards := make([]*ternv1.ShardProgress, len(et.Shards))
			for j, sh := range et.Shards {
				shards[j] = &ternv1.ShardProgress{
					Shard:           sh.Shard,
					Status:          state.NormalizeShardStatus(sh.State),
					RowsCopied:      sh.RowsCopied,
					RowsTotal:       sh.RowsTotal,
					EtaSeconds:      sh.ETASeconds,
					CutoverAttempts: int32(sh.CutoverAttempts),
				}
			}
			tp.Shards = shards
		} else {
			// No live engine data — use stored progress from the task.
			// This covers stopped tasks (progress saved at stop time) and
			// completed tasks that finished before the engine was shut down.
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
		}

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

func syncStoredTaskProgressFromEngineTable(task *storage.Task, progress *engine.TableProgress, now time.Time) bool {
	if task == nil || progress == nil {
		return false
	}

	changed := false
	if !engineTableProgressOmittedRowTotals(task, progress) {
		if task.RowsCopied != progress.RowsCopied {
			task.RowsCopied = progress.RowsCopied
			changed = true
		}
		if task.RowsTotal != progress.RowsTotal {
			task.RowsTotal = progress.RowsTotal
			changed = true
		}
		if task.ProgressPercent != progress.Progress {
			task.ProgressPercent = progress.Progress
			changed = true
		}
		if task.ETASeconds != int(progress.ETASeconds) {
			task.ETASeconds = int(progress.ETASeconds)
			changed = true
		}
	}
	if task.IsInstant != progress.IsInstant {
		task.IsInstant = progress.IsInstant
		changed = true
	}
	if progress.StartedAt != nil && task.StartedAt == nil {
		task.StartedAt = progress.StartedAt
		changed = true
	}
	if progress.CompletedAt != nil && task.CompletedAt == nil {
		task.CompletedAt = progress.CompletedAt
		changed = true
	}
	if changed {
		task.UpdatedAt = now
	}
	return changed
}

func engineTableProgressOmittedRowTotals(task *storage.Task, progress *engine.TableProgress) bool {
	if task == nil || progress == nil {
		return false
	}
	return task.RowsTotal > 0 && progress.RowsTotal <= 0
}

func progressTableStatus(storedTaskState, engineTableState string) string {
	return taskStateWithNoBackwardProgress(storedTaskState, state.NormalizeTaskStatus(engineTableState))
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
	case state.Task.WaitingForCutover:
		return 3, true
	case state.Task.CuttingOver:
		return 4, true
	case state.Task.RevertWindow:
		return 5, true
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
