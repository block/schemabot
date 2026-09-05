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
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/block/spirit/pkg/statement"
	spirittable "github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	ps "github.com/planetscale/planetscale-go/planetscale"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/engine/planetscale"
	"github.com/block/schemabot/pkg/engine/postgres"
	"github.com/block/schemabot/pkg/engine/spirit"
	"github.com/block/schemabot/pkg/inventory"
	"github.com/block/schemabot/pkg/metrics"
	"github.com/block/schemabot/pkg/mysqlconn"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/psclient"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// LocalConfig holds configuration for the local Tern client.
type LocalConfig struct {
	// Database is the identifier this database is registered under — a routing
	// and display key. For PlanetScale targets the API addresses the "database"
	// metadata key when set; without it this identifier doubles as the
	// PlanetScale database name.
	Database string

	// Type is the database type. "mysql", "vitess", and "postgres" have built-in
	// engines; any other value requires a matching EngineFactories entry.
	Type string

	// TargetDSN is the connection string to the target database for schema changes.
	TargetDSN string

	// PostgresNativeSafeTableSizeLimitBytes is the maximum table size in bytes
	// for PostgreSQL native-safe execution. Zero uses the engine default.
	PostgresNativeSafeTableSizeLimitBytes int64

	// Metadata holds engine-specific configuration as key-value pairs.
	// The tern layer does not interpret these — it passes them through to the
	// engine via Credentials.Metadata and reads specific keys as needed.
	// Keys used by PlanetScale: organization, database (the PlanetScale
	// database name when it differs from the registered identifier),
	// token_name, token_value, revert_window_duration, main_branch.
	// Keys used by Spirit: pending_drops ("false" disables the pending drops
	// quarantine so DROP TABLE executes directly); direct_execution ("true"
	// lets engine-refused ALTER statements run verbatim as native MySQL DDL)
	// with its required companion direct_execution_max_table_rows (positive
	// estimated-row-count bound above which direct execution is blocked) and
	// optional direct_execution_lock_acquisition_timeout_seconds (positive bound on
	// each direct statement's lock acquisition; engine default when absent);
	// plus the run-settings overrides parsed by spirit.SettingsFromMetadata
	// (enable_experimental_autoscaling, checkpoint_max_age,
	// checksum_yield_timeout).
	Metadata map[string]string

	// SchemaOverrides maps a requested (canonical) MySQL namespace to the
	// physical schema name on this target, for targets whose physical schema
	// names embed environment or region. When non-empty it is a strict
	// allowlist consulted wherever a namespace is turned into a connection
	// schema: a requested namespace without a mapping fails rather than
	// falling back to the canonical name. Empty preserves the default
	// behavior where the requested namespace is the physical schema. MySQL
	// only, and requires a namespace-free TargetDSN.
	SchemaOverrides map[string]string

	// WakeOperator notifies the owner loop after durable work is recorded — a
	// queued apply from a dispatch, or an external control request. The callback
	// must not execute the work itself; it only nudges the storage-claiming
	// operator to pick it up promptly instead of waiting out the poll interval.
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
	postgresEngine    engine.Engine
	customEngine      engine.Engine
	psClientFunc      func(tokenName, tokenValue string) (psclient.PSClient, error)
	logger            *slog.Logger

	// unrecognizedStatuses reports engine statuses with no task-state mapping
	// at the drive's ingest points. Zero value is ready.
	unrecognizedStatuses unrecognizedStatusReporter

	// heartbeatInterval controls how often the apply heartbeat updates updated_at.
	// Defaults to 10s. Tests may lower this to verify heartbeat behavior.
	heartbeatInterval time.Duration

	// taskPollIntervalOverride, when positive, replaces defaultTaskPollInterval
	// as the sequential drive's progress poll cadence. Tests may lower it to
	// drive many polls quickly.
	taskPollIntervalOverride time.Duration

	// taskStallWarnIntervalOverride, when positive, replaces
	// defaultTaskStallWarnInterval as the interval after which a polled task
	// with no state or progress movement is warned about. Tests may lower it
	// to observe the warning.
	taskStallWarnIntervalOverride time.Duration

	// lostEngineWorkPendingBudgetOverride, when positive, replaces
	// defaultLostEngineWorkPendingBudget as how long the sequential drive keeps
	// trusting an engine reporting no active schema change for an in-flight
	// task. Tests may lower it to reach the verification path quickly.
	lostEngineWorkPendingBudgetOverride time.Duration

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
	// under the created apply's ID before the operator picks the apply up; the
	// operator drives through this same client instance, so the drive finds the
	// observer in the registry.
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
	// Schema overrides select a MySQL connection schema, so they only make
	// sense for MySQL and only with a namespace-free target DSN (a DSN that
	// already names a database would silently win over the mapping). Enforce
	// the full mapping contract here as well as at inventory-config load so a
	// custom resolver cannot hand an invalid combination straight to the
	// client, and clone the map so the client's view cannot be mutated later.
	if len(cfg.SchemaOverrides) > 0 {
		if err := inventory.ValidateSchemaOverrides(cfg.Type, cfg.SchemaOverrides); err != nil {
			return nil, fmt.Errorf("local client for database %q: %w", cfg.Database, err)
		}
		hasDatabase, err := mysqlDSNHasDatabase(cfg.TargetDSN)
		if err != nil {
			return nil, fmt.Errorf("inspect MySQL target DSN for schema overrides: %w", err)
		}
		if hasDatabase {
			return nil, fmt.Errorf("schema overrides require a namespace-free target DSN; the DSN already names a database")
		}
		cfg.SchemaOverrides = maps.Clone(cfg.SchemaOverrides)
	}

	// For Vitess databases, create a PlanetScale engine with a client factory
	// that points at the API base URL from metadata (e.g., "http://localscale:8080").
	// TargetDSN is the vtgate MySQL DSN for SHOW VITESS_MIGRATIONS.
	var psEngine engine.Engine
	var psClientFunc func(tokenName, tokenValue string) (psclient.PSClient, error)
	if cfg.Type == storage.DatabaseTypeVitess {
		// api_url is optional in a database's configuration and names a private or
		// emulated endpoint when it is set. Absent, the database is a real
		// PlanetScale one, so fall back to the public endpoint the way the
		// inventory-resolved path does — the client needs a base URL to address
		// the API directly, and leaving it empty would refuse every apply.
		apiURL := cfg.Metadata["api_url"]
		if apiURL == "" {
			apiURL = inventory.DefaultPlanetScaleAPIURL
		}
		psClientFunc = func(tokenName, tokenValue string) (psclient.PSClient, error) {
			return psclient.NewPSClientWithBaseURL(tokenName, tokenValue, apiURL)
		}
		psEngine = planetscale.NewWithClient(logger, psClientFunc)
	}

	// For a database type without a built-in engine, build it from a registered
	// factory. This is the embedder extension point for engines this build does
	// not include.
	var customEngine engine.Engine
	if cfg.Type != storage.DatabaseTypeMySQL && cfg.Type != storage.DatabaseTypeVitess && cfg.Type != storage.DatabaseTypePostgres {
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

	spiritSettings, err := spirit.SettingsFromMetadata(cfg.Metadata)
	if err != nil {
		return nil, fmt.Errorf("parse Spirit settings for database %q: %w", cfg.Database, err)
	}

	return &LocalClient{
		config:  cfg,
		storage: stor,
		spiritEngine: spirit.New(spirit.Config{
			Logger:              logger,
			DisablePendingDrops: pendingDropsDisabled(cfg.Metadata),
			Settings:            spiritSettings,
		}),
		planetscaleEngine: psEngine,
		postgresEngine: postgres.NewForTarget(cfg.PostgresNativeSafeTableSizeLimitBytes, cfg.Database, &engine.Credentials{
			DSN:      cfg.TargetDSN,
			Metadata: maps.Clone(cfg.Metadata),
		}),
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

// wakeOperator nudges the storage-claiming operator to pick up a durable
// control request recorded for the apply promptly instead of waiting out the
// poll interval. A skipped control-request wake only delays processing — the
// request is serviced when the operator next claims the (already claimable)
// apply — so the skip is expected in library and test use and logged at Debug.
// Queued applies use wakeOperatorForQueuedApply, whose skip can strand work.
func (c *LocalClient) wakeOperator(apply *storage.Apply) {
	if c.config.WakeOperator == nil {
		c.logger.Debug("operator wake skipped because no wake callback is configured; the control request will be processed when an operator next claims this apply",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment)
		return
	}
	c.config.WakeOperator(apply.ApplyIdentifier, apply.Database, apply.Environment)
}

// wakeOperatorForQueuedApply nudges the operator to claim a newly queued apply.
// Unlike a control-request wake, a skipped wake here is not benign: every drive
// runs under an operator claim, so a queued apply makes no progress until an
// operator polling this storage claims it — and in an embedding that runs no
// operator at all, it will never be driven.
func (c *LocalClient) wakeOperatorForQueuedApply(apply *storage.Apply) {
	if c.config.WakeOperator == nil {
		c.logger.Warn("operator wake skipped because no wake callback is configured; the queued apply will not be driven until an operator polling this storage claims it",
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
	switch c.config.Type {
	case storage.DatabaseTypeVitess:
		return ternv1.Engine_ENGINE_PLANETSCALE
	case storage.DatabaseTypePostgres:
		return ternv1.Engine_ENGINE_POSTGRES
	default:
		return ternv1.Engine_ENGINE_SPIRIT
	}
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
	case storage.EnginePostgres:
		return ternv1.Engine_ENGINE_POSTGRES, nil
	default:
		return 0, fmt.Errorf("unknown engine: %s", name)
	}
}

// Close closes the client and releases resources.
func (c *LocalClient) Close() error {
	// LocalClient doesn't own storage, so nothing to close
	return nil
}

// HaltForShutdown brings this client's engine down when the engine runs its
// schema change work in this process, so the process can exit without leaving
// the target held by work no lease is being renewed for.
func (c *LocalClient) HaltForShutdown(ctx context.Context) error {
	eng := c.getEngine()
	if eng == nil {
		c.logger.Debug("no engine to halt for shutdown",
			"database", c.config.Database, "database_type", c.config.Type)
		return nil
	}
	supported, err := engine.HaltEngineForShutdown(ctx, eng)
	if !supported {
		c.logger.Debug("engine drives its schema changes outside this process; nothing to halt for shutdown",
			"database", c.config.Database, "database_type", c.config.Type, "engine", eng.Name())
		return nil
	}
	if err != nil {
		return fmt.Errorf("halt engine %s for database %s (%s) on shutdown: %w", eng.Name(), c.config.Database, c.config.Type, err)
	}
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
	if !usesPerNamespaceCredentials(c.config.Type) {
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
	// (A DSN-with-database target cannot carry schema overrides; NewLocalClient
	// rejects that combination.)
	if hasDatabase {
		return c.credentials(), nil
	}
	// A namespace-free target DSN is the inventory/data-plane shape: the concrete
	// namespace is the connection schema and must be injected per operation.
	if namespace == "" {
		return nil, fmt.Errorf("MySQL namespace is required for a namespace-free target DSN")
	}
	physical, err := c.physicalMySQLNamespace(namespace)
	if err != nil {
		return nil, err
	}
	dsn, err := mysqlDSNWithDatabase(c.config.TargetDSN, physical)
	if err != nil {
		return nil, err
	}
	return &engine.Credentials{
		DSN:      dsn,
		Metadata: c.config.Metadata,
	}, nil
}

// physicalMySQLNamespace resolves the physical schema name for a requested
// (canonical) namespace. Without configured overrides the requested namespace
// is the physical schema. With overrides the map is a strict allowlist: an
// unmapped namespace fails rather than falling back to the canonical name, so
// a request misrouted to this target cannot land in the wrong physical schema.
func (c *LocalClient) physicalMySQLNamespace(namespace string) (string, error) {
	if len(c.config.SchemaOverrides) == 0 {
		return namespace, nil
	}
	physical, ok := c.config.SchemaOverrides[namespace]
	if !ok {
		return "", fmt.Errorf("target does not authorize MySQL namespace %q; configured schema overrides map only %v", namespace, slices.Sorted(maps.Keys(c.config.SchemaOverrides)))
	}
	c.logger.Debug("resolved schema override for namespace",
		"database", c.config.Database,
		"namespace", namespace,
		"physical_schema", physical,
	)
	return physical, nil
}

func (c *LocalClient) credentialsForTask(task *storage.Task) (*engine.Credentials, error) {
	if !usesPerNamespaceCredentials(c.config.Type) {
		return c.credentials(), nil
	}
	if task == nil {
		return nil, fmt.Errorf("task is required for MySQL credentials")
	}
	return c.credentialsForMySQLNamespace(task.Namespace)
}

// usesPerNamespaceCredentials reports whether the database type's engine needs
// credentials resolved per namespace instead of sharing the target-level
// credentials. MySQL resolves a namespace-specific DSN so each task connects
// to its own schema (per-target overrides can remap a namespace to a different
// physical schema).
func usesPerNamespaceCredentials(databaseType string) bool {
	switch databaseType {
	case storage.DatabaseTypeMySQL:
		return true
	case storage.DatabaseTypeVitess, storage.DatabaseTypeStrata, storage.DatabaseTypePostgres:
		return false
	default:
		// LocalConfig.Type is open-world: embedder-registered engine types
		// (EngineFactories) and the zero-value type used by tests land here
		// and get the conservative disposition — shared target credentials.
		return false
	}
}

// credentialsForGroupedApply resolves the single-namespace credentials for a
// grouped/atomic MySQL apply. A grouped apply runs one Spirit execution against
// one schema, so the plan must carry exactly one namespace. Fail closed rather
// than pick a namespace by map iteration order (or silently use a namespace-free
// DSN) if that invariant is ever violated.
func (c *LocalClient) credentialsForGroupedApply(plan *storage.Plan) (*engine.Credentials, error) {
	if !usesPerNamespaceCredentials(c.config.Type) {
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

// singleTaskNamespace returns the single non-empty namespace shared by the
// tasks, or "" when no task carries one (a DSN-with-database target). Tasks
// that drive one Spirit execution must agree on the namespace — it selects the
// connection schema, so with mixed namespaces the schema addressed would
// silently depend on task order. Fail loudly instead.
func singleTaskNamespace(tasks []*storage.Task) (string, error) {
	namespace := ""
	for _, task := range tasks {
		if task == nil || task.Namespace == "" {
			continue
		}
		if namespace == "" {
			namespace = task.Namespace
			continue
		}
		if task.Namespace != namespace {
			return "", fmt.Errorf("tasks span multiple namespaces (%q, %q)", namespace, task.Namespace)
		}
	}
	return namespace, nil
}

func mysqlDSNDatabase(dsn string) (string, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("parse MySQL DSN: %w", err)
	}
	return cfg.DBName, nil
}

func (c *LocalClient) deferredCutoverSignalExists(ctx context.Context, apply *storage.Apply, tasks []*storage.Task) (bool, bool, error) {
	if apply == nil {
		return false, false, fmt.Errorf("apply is required for deferred cutover signal lookup")
	}
	eng := c.getEngine()
	checker, ok := eng.(engine.DeferredCutoverSignalChecker)
	if !ok {
		return false, false, nil
	}
	// Resolve credentials through the task namespace so the lookup addresses
	// the schema the apply actually runs against. The engine's sentinel query
	// is schema-qualified (DSN database first, request database as fallback),
	// so without injecting the mapped task namespace it would address the
	// canonical database name and miss a sentinel that lives in a different
	// physical schema.
	//
	// A deferred-cutover apply's tasks share exactly one namespace (grouped
	// applies enforce this at credential resolution). Guard that invariant
	// here: with mixed namespaces the lookup would silently depend on task
	// order, so fail loudly instead.
	namespace, err := singleTaskNamespace(tasks)
	if err != nil {
		return false, true, fmt.Errorf("deferred cutover signal lookup for apply %s database %s: %w", apply.ApplyIdentifier, apply.Database, err)
	}
	creds, err := c.credentialsForMySQLNamespace(namespace)
	if err != nil {
		return false, true, fmt.Errorf("resolve credentials for deferred cutover signal lookup for apply %s database %s: %w", apply.ApplyIdentifier, apply.Database, err)
	}
	exists, err := checker.DeferredCutoverSignalExists(ctx, &engine.DeferredCutoverSignalRequest{
		Database:    apply.Database,
		Credentials: creds,
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
// MySQL and Vitess use the built-in pull paths; any other database type is
// delegated to the configured engine's SchemaPuller capability.
func (c *LocalClient) PullSchema(ctx context.Context, req *ternv1.PullSchemaRequest) (*ternv1.PullSchemaResponse, error) {
	if req.Type != "" && req.Type != c.config.Type {
		return nil, fmt.Errorf("pull schema for database %s: request type %q does not match client type %q: %w", c.config.Database, req.Type, c.config.Type, ErrPullSchemaInvalidRequest)
	}
	if c.config.Type != storage.DatabaseTypeMySQL && c.config.Type != storage.DatabaseTypeVitess {
		return c.pullSchemaFromEngine(ctx, req)
	}
	if req.GetNamespace() == "" {
		return c.pullAllNamespaces(ctx, req)
	}
	return c.pullSchemaNamespace(ctx, req, req.GetNamespace())
}

// pullSchemaFromEngine delegates a pull for a database type without a
// built-in pull path to the configured engine's SchemaPuller capability. An
// engine that does not implement the capability fails closed with
// ErrPullSchemaUnsupportedType, which the gRPC server surfaces as
// codes.Unimplemented. A nil response from an engine that does implement the
// capability is a broken engine contract, not a missing capability: it is
// deliberately surfaced as an internal error rather than the unsupported
// sentinel, so an engine defect stays loud instead of reading as an expected
// unsupported-type condition.
func (c *LocalClient) pullSchemaFromEngine(ctx context.Context, req *ternv1.PullSchemaRequest) (*ternv1.PullSchemaResponse, error) {
	puller, ok := c.getEngine().(SchemaPuller)
	if !ok {
		return nil, fmt.Errorf("pull schema for database %s type %s: engine does not support schema pull: %w", c.config.Database, c.config.Type, ErrPullSchemaUnsupportedType)
	}
	c.logger.Info("LocalClient.PullSchema: delegating to engine schema pull",
		"database", c.config.Database,
		"type", c.config.Type,
		"namespace", req.GetNamespace(),
	)
	resp, err := puller.PullSchema(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("engine pull schema for database %s type %s: %w", c.config.Database, c.config.Type, err)
	}
	if resp == nil {
		return nil, fmt.Errorf("engine pull schema for database %s type %s returned a nil response", c.config.Database, c.config.Type)
	}
	return resp, nil
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

	// A target with schema overrides serves exactly the mapped canonical
	// namespaces. Return those keys rather than scanning the cluster, which
	// would surface physical names (and unrelated databases) the requested
	// canonical namespace model must not leak.
	if len(c.config.SchemaOverrides) > 0 {
		namespaces := slices.Sorted(maps.Keys(c.config.SchemaOverrides))
		c.logger.Info("LocalClient.PullSchema: using schema-override namespaces", "database", c.config.Database, "namespace_count", len(namespaces))
		return namespaces, nil
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

	dialect := schema.DialectForDatabaseType(c.config.Type)
	var namespaces []string
	for rows.Next() {
		var namespace string
		if err := rows.Scan(&namespace); err != nil {
			return nil, fmt.Errorf("scan namespace for schema pull: %w", err)
		}
		if schema.IsReservedPullNamespaceForDialect(dialect, namespace) {
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

	// The physical schema is what MySQL is addressed by (connection schema and
	// information_schema predicates); the requested canonical namespace stays
	// the response key so callers never see physical names.
	physical := namespace
	targetDSN := c.config.TargetDSN
	if c.config.Type == storage.DatabaseTypeMySQL {
		creds, resolvedPhysical, err := c.credentialsForMySQLPullNamespace(namespace)
		if err != nil {
			return nil, fmt.Errorf("resolve database %s namespace %s target for schema pull: %w", c.config.Database, namespace, err)
		}
		physical = resolvedPhysical
		targetDSN = creds.DSN
	}

	attrs := []any{"database", c.config.Database, "namespace", namespace, "physical_schema", physical}
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
	catalog, err := c.pullNamespaceCatalog(ctx, db, namespace, physical, pulledTables, req.GetCatalogDetail())
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
	pt, err := c.planetScalePullClient()
	if err != nil {
		return nil, err
	}

	c.logger.Info("LocalClient.PullSchema: discovering Vitess keyspaces", "database", c.config.Database, "planetscale_database", pt.database, "branch", pt.branch)
	keyspaces, err := pt.client.ListKeyspaces(ctx, &ps.ListKeyspacesRequest{
		Organization: pt.org,
		Database:     pt.database,
		Branch:       pt.branch,
	})
	if err != nil {
		return nil, fmt.Errorf("list Vitess keyspaces for database %s branch %s: %w", pt.database, pt.branch, err)
	}
	dialect := schema.DialectForDatabaseType(c.config.Type)
	namespaces := make([]string, 0, len(keyspaces))
	for _, keyspace := range keyspaces {
		if keyspace == nil {
			c.logger.Warn("LocalClient.PullSchema: skipping nil Vitess keyspace", "database", c.config.Database, "planetscale_database", pt.database, "branch", pt.branch)
			continue
		}
		if keyspace.Name == "" {
			return nil, fmt.Errorf("list Vitess keyspaces for database %s branch %s returned a keyspace with no name", pt.database, pt.branch)
		}
		if schema.IsReservedPullNamespaceForDialect(dialect, keyspace.Name) {
			c.logger.Debug("LocalClient.PullSchema: skipping reserved Vitess keyspace", "database", c.config.Database, "planetscale_database", pt.database, "branch", pt.branch, "namespace", keyspace.Name)
			continue
		}
		namespaces = append(namespaces, keyspace.Name)
	}
	sort.Strings(namespaces)
	c.logger.Info("LocalClient.PullSchema: discovered Vitess keyspaces", "database", c.config.Database, "planetscale_database", pt.database, "branch", pt.branch, "namespace_count", len(namespaces))
	return namespaces, nil
}

func (c *LocalClient) pullVitessSchemaNamespace(ctx context.Context, req *ternv1.PullSchemaRequest, namespace string) (*ternv1.PullSchemaResponse, error) {
	pt, err := c.planetScalePullClient()
	if err != nil {
		return nil, err
	}

	c.logger.Info("LocalClient.PullSchema: loading live Vitess schema", "database", c.config.Database, "planetscale_database", pt.database, "branch", pt.branch, "namespace", namespace)
	schemaResult, err := pt.client.GetBranchSchema(ctx, &ps.BranchSchemaRequest{
		Organization: pt.org,
		Database:     pt.database,
		Branch:       pt.branch,
		Keyspace:     namespace,
	})
	if err != nil {
		return nil, fmt.Errorf("fetch Vitess schema for database %s branch %s keyspace %s: %w", pt.database, pt.branch, namespace, err)
	}

	pulledTables := make(map[string]string, len(schemaResult))
	for _, tbl := range schemaResult {
		if tbl == nil {
			c.logger.Warn("LocalClient.PullSchema: skipping nil Vitess table schema", "database", c.config.Database, "planetscale_database", pt.database, "branch", pt.branch, "namespace", namespace)
			continue
		}
		if tbl.Name == "" {
			return nil, fmt.Errorf("fetch Vitess schema for database %s branch %s keyspace %s returned a table with no name", pt.database, pt.branch, namespace)
		}
		content, err := pulledSchemaFileContent(c.config.Database, tbl.Name, tbl.Raw)
		if err != nil {
			return nil, fmt.Errorf("fetch Vitess schema for database %s branch %s keyspace %s: %w", pt.database, pt.branch, namespace, err)
		}
		pulledTables[tbl.Name] = content
	}

	artifacts := map[string]string{}
	vschema, err := pt.client.GetKeyspaceVSchema(ctx, &ps.GetKeyspaceVSchemaRequest{
		Organization: pt.org,
		Database:     pt.database,
		Branch:       pt.branch,
		Keyspace:     namespace,
	})
	if err != nil {
		return nil, fmt.Errorf("fetch Vitess VSchema for database %s branch %s keyspace %s: %w", pt.database, pt.branch, namespace, err)
	}
	if vschema != nil && strings.TrimSpace(vschema.Raw) != "" {
		artifacts[storage.VSchemaArtifactName] = strings.TrimRight(vschema.Raw, "\n") + "\n"
	}

	c.logger.Info("LocalClient.PullSchema: loaded live Vitess schema",
		"database", c.config.Database,
		"planetscale_database", pt.database,
		"branch", pt.branch,
		"namespace", namespace,
		"table_count", len(pulledTables),
		"artifact_count", len(artifacts),
	)

	pulledNamespace := &ternv1.PulledNamespace{
		Tables:    pulledTables,
		Artifacts: artifacts,
	}
	// BASIC pulls only DDL and artifacts; the namespace catalog is DETAILED-only,
	// consistent with the MySQL path. Per-table catalog (table_catalog with
	// columns, indexes, and foreign keys) is not yet populated for Vitess — that
	// metadata comes from information_schema on the MySQL path and has no
	// PlanetScale equivalent wired up here; DETAILED currently returns only the
	// namespace-level catalog for Vitess.
	if req.GetCatalogDetail() == ternv1.PullCatalogDetail_PULL_CATALOG_DETAIL_DETAILED {
		pulledNamespace.NamespaceCatalog = &ternv1.NamespaceCatalog{
			Name:       namespace,
			Engine:     c.config.Type,
			TableCount: int32(len(pulledTables)),
		}
	}

	return &ternv1.PullSchemaResponse{
		Database:    c.pullResponseDatabase(req),
		Type:        c.config.Type,
		Environment: req.Environment,
		Namespaces: map[string]*ternv1.PulledNamespace{
			namespace: pulledNamespace,
		},
		TableCount: int32(len(pulledTables)),
	}, nil
}

// planetScalePullTarget carries the PlanetScale client and the identifiers a
// pull resolves once per call: the organization, the PlanetScale database
// name, and the branch to read from.
type planetScalePullTarget struct {
	client   psclient.PSClient
	org      string
	database string
	branch   string
}

func (c *LocalClient) planetScalePullClient() (*planetScalePullTarget, error) {
	if c.psClientFunc == nil {
		// A vitess database supports pull; a missing PlanetScale client is a
		// configuration defect, surfaced as an internal error rather than the
		// unsupported-type sentinel so it cannot read as "pull not supported".
		return nil, fmt.Errorf("PlanetScale client is not configured for database %s", c.config.Database)
	}
	org := c.config.Metadata["organization"]
	if org == "" {
		return nil, fmt.Errorf("PlanetScale organization metadata is required for database %s", c.config.Database)
	}
	// The SchemaBot database identifier is a routing and display key; the
	// PlanetScale database name in target metadata is what the API addresses.
	// Without the metadata the identifier doubles as the PlanetScale name.
	database := c.config.Metadata["database"]
	if database == "" {
		database = c.config.Database
	}
	branch := c.config.Metadata["main_branch"]
	if branch == "" {
		branch = "main"
	}
	client, err := c.psClientFunc(c.config.Metadata["token_name"], c.config.Metadata["token_value"])
	if err != nil {
		return nil, fmt.Errorf("create PlanetScale client for database %s: %w", c.config.Database, err)
	}
	return &planetScalePullTarget{client: client, org: org, database: database, branch: branch}, nil
}

type pulledCatalog struct {
	namespace *ternv1.NamespaceCatalog
	tables    map[string]*ternv1.TableCatalog
}

func (c *LocalClient) pullNamespaceCatalog(ctx context.Context, db *sql.DB, namespace, physical string, pulledTables map[string]string, catalogDetail ternv1.PullCatalogDetail) (*pulledCatalog, error) {
	// BASIC pulls only canonical DDL and artifacts (the pre-catalog pull shape):
	// no namespace or table catalog. The full structured catalog is built only
	// at DETAILED detail.
	if catalogDetail != ternv1.PullCatalogDetail_PULL_CATALOG_DETAIL_DETAILED {
		return &pulledCatalog{}, nil
	}
	// The catalog is named by the requested canonical namespace; the live rows
	// are read from the physical schema (they differ under schema overrides).
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
	if err := c.loadTableCatalog(ctx, db, physical, pulledTables, catalog.tables); err != nil {
		return nil, err
	}
	if err := c.loadColumnCatalog(ctx, db, physical, pulledTables, catalog.tables); err != nil {
		return nil, err
	}
	if err := c.loadIndexCatalog(ctx, db, physical, pulledTables, catalog.tables); err != nil {
		return nil, err
	}
	if err := c.loadForeignKeyCatalog(ctx, db, physical, pulledTables, catalog.tables); err != nil {
		return nil, err
	}
	return catalog, nil
}

// loadTableCatalog reads each pulled table's kind and comment together with its
// engine-maintained row-count and on-disk-size estimates. The estimates are
// approximations: NULL for views, and served from the data dictionary's cached
// table statistics, so they lag the live table until those statistics are
// refreshed.
//
// The estimate columns are what make this read more than the kind and comment
// alone would. information_schema_stats_expiry governs how long the cached
// statistics are reused; the first read after they expire makes the server
// refresh them per table. Kind and comment come from the data dictionary and
// are cheap either way, so they ride along on this query rather than paying for
// a second traversal of the view.
func (c *LocalClient) loadTableCatalog(ctx context.Context, db *sql.DB, physicalSchema string, pulledTables map[string]string, catalog map[string]*ternv1.TableCatalog) error {
	rows, err := db.QueryContext(ctx, `
		SELECT table_name, table_type, table_comment, table_rows, data_length, index_length
		FROM information_schema.tables
		WHERE table_schema = ?
		ORDER BY table_name`, physicalSchema)
	if err != nil {
		return fmt.Errorf("load table catalog for database %s physical schema %s: %w", c.config.Database, physicalSchema, err)
	}
	defer utils.CloseAndLog(rows)

	for rows.Next() {
		var tableName, tableType, tableComment string
		var tableRows, dataLength, indexLength sql.NullInt64
		if err := rows.Scan(&tableName, &tableType, &tableComment, &tableRows, &dataLength, &indexLength); err != nil {
			return fmt.Errorf("scan table catalog for database %s physical schema %s: %w", c.config.Database, physicalSchema, err)
		}
		if _, ok := pulledTables[tableName]; ok {
			catalog[tableName] = &ternv1.TableCatalog{
				Name:              tableName,
				Kind:              normalizedTableKind(tableType),
				Comment:           tableComment,
				EstimatedRowCount: tableRows.Int64,
				DataSizeBytes:     dataLength.Int64 + indexLength.Int64,
			}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate table catalog for database %s physical schema %s: %w", c.config.Database, physicalSchema, err)
	}
	return nil
}

func (c *LocalClient) loadColumnCatalog(ctx context.Context, db *sql.DB, physicalSchema string, pulledTables map[string]string, catalog map[string]*ternv1.TableCatalog) error {
	rows, err := db.QueryContext(ctx, `
		SELECT table_name, column_name, column_type, is_nullable, column_default, column_comment, extra
		FROM information_schema.columns
		WHERE table_schema = ?
		ORDER BY table_name, ordinal_position`, physicalSchema)
	if err != nil {
		return fmt.Errorf("load column catalog for database %s physical schema %s: %w", c.config.Database, physicalSchema, err)
	}
	defer utils.CloseAndLog(rows)

	for rows.Next() {
		var tableName, columnName, columnType, nullable, comment, extra string
		var defaultValue sql.NullString
		if err := rows.Scan(&tableName, &columnName, &columnType, &nullable, &defaultValue, &comment, &extra); err != nil {
			return fmt.Errorf("scan column catalog for database %s physical schema %s: %w", c.config.Database, physicalSchema, err)
		}
		if _, ok := pulledTables[tableName]; ok {
			tableCatalog := ensurePulledTableCatalog(catalog, tableName)
			// EXTRA marks generated columns as "STORED GENERATED" or "VIRTUAL
			// GENERATED". Match those specifically: a bare "GENERATED" substring
			// would also match "DEFAULT_GENERATED" (an expression default such as
			// DEFAULT CURRENT_TIMESTAMP), which is not a generated column.
			upperExtra := strings.ToUpper(extra)
			column := &ternv1.ColumnCatalog{
				Name:          columnName,
				Type:          columnType,
				Nullable:      nullable == "YES",
				Comment:       comment,
				AutoIncrement: strings.Contains(upperExtra, "AUTO_INCREMENT"),
				Generated:     strings.Contains(upperExtra, "STORED GENERATED") || strings.Contains(upperExtra, "VIRTUAL GENERATED"),
			}
			if defaultValue.Valid {
				column.DefaultValue = defaultValue.String
			}
			tableCatalog.Columns = append(tableCatalog.Columns, column)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate column catalog for database %s physical schema %s: %w", c.config.Database, physicalSchema, err)
	}
	return nil
}

func (c *LocalClient) loadIndexCatalog(ctx context.Context, db *sql.DB, physicalSchema string, pulledTables map[string]string, catalog map[string]*ternv1.TableCatalog) error {
	rows, err := db.QueryContext(ctx, `
		SELECT table_name, index_name, non_unique, column_name, expression
		FROM information_schema.statistics
		WHERE table_schema = ?
		ORDER BY table_name, index_name, seq_in_index`, physicalSchema)
	if err != nil {
		return fmt.Errorf("load index catalog for database %s physical schema %s: %w", c.config.Database, physicalSchema, err)
	}
	defer utils.CloseAndLog(rows)

	indexesByTable := make(map[string]map[string]*ternv1.IndexCatalog)
	for rows.Next() {
		var tableName, indexName string
		var columnName, expression sql.NullString
		var nonUnique int32
		if err := rows.Scan(&tableName, &indexName, &nonUnique, &columnName, &expression); err != nil {
			return fmt.Errorf("scan index catalog for database %s physical schema %s: %w", c.config.Database, physicalSchema, err)
		}
		if _, ok := pulledTables[tableName]; ok {
			indexedValue := ""
			switch {
			case columnName.Valid:
				indexedValue = columnName.String
			case expression.Valid:
				indexedValue = expression.String
			default:
				c.logger.Warn("LocalClient.PullSchema: skipping index part without column or expression", "database", c.config.Database, "physical_schema", physicalSchema, "table", tableName, "index", indexName)
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
		return fmt.Errorf("iterate index catalog for database %s physical schema %s: %w", c.config.Database, physicalSchema, err)
	}
	return nil
}

// loadForeignKeyCatalog reads foreign keys by joining key_column_usage (the
// per-column local→referenced mapping) with referential_constraints (the
// per-constraint ON UPDATE / ON DELETE rules). referential_constraints
// contains only foreign-key constraints, so the join naturally restricts to
// them; primary-key and unique constraints are already represented as indexes.
func (c *LocalClient) loadForeignKeyCatalog(ctx context.Context, db *sql.DB, physicalSchema string, pulledTables map[string]string, catalog map[string]*ternv1.TableCatalog) error {
	rows, err := db.QueryContext(ctx, `
		SELECT kcu.table_name, kcu.constraint_name, kcu.column_name,
		       kcu.referenced_table_name, kcu.referenced_column_name,
		       rc.update_rule, rc.delete_rule
		FROM information_schema.key_column_usage kcu
		JOIN information_schema.referential_constraints rc
		  ON rc.constraint_schema = kcu.constraint_schema
		 AND rc.constraint_name = kcu.constraint_name
		WHERE kcu.table_schema = ?
		ORDER BY kcu.table_name, kcu.constraint_name, kcu.ordinal_position`, physicalSchema)
	if err != nil {
		return fmt.Errorf("load foreign key catalog for database %s physical schema %s: %w", c.config.Database, physicalSchema, err)
	}
	defer utils.CloseAndLog(rows)

	foreignKeysByTable := make(map[string]map[string]*ternv1.ForeignKeyCatalog)
	for rows.Next() {
		var tableName, constraintName string
		var columnName, referencedTable, referencedColumn, updateRule, deleteRule sql.NullString
		if err := rows.Scan(&tableName, &constraintName, &columnName, &referencedTable, &referencedColumn, &updateRule, &deleteRule); err != nil {
			return fmt.Errorf("scan foreign key catalog for database %s physical schema %s: %w", c.config.Database, physicalSchema, err)
		}
		if _, ok := pulledTables[tableName]; !ok {
			continue
		}
		tableCatalog := ensurePulledTableCatalog(catalog, tableName)
		if foreignKeysByTable[tableName] == nil {
			foreignKeysByTable[tableName] = make(map[string]*ternv1.ForeignKeyCatalog)
		}
		fk := foreignKeysByTable[tableName][constraintName]
		if fk == nil {
			fk = &ternv1.ForeignKeyCatalog{
				Name:            constraintName,
				ReferencedTable: referencedTable.String,
				OnUpdate:        updateRule.String,
				OnDelete:        deleteRule.String,
			}
			foreignKeysByTable[tableName][constraintName] = fk
			tableCatalog.ForeignKeys = append(tableCatalog.ForeignKeys, fk)
		}
		fk.Columns = append(fk.Columns, columnName.String)
		fk.ReferencedColumns = append(fk.ReferencedColumns, referencedColumn.String)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate foreign key catalog for database %s physical schema %s: %w", c.config.Database, physicalSchema, err)
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

// credentialsForMySQLPullNamespace resolves the connection credentials and the
// physical schema a pull for the requested canonical namespace reads, so the
// pull path resolves the schema-override mapping exactly once.
func (c *LocalClient) credentialsForMySQLPullNamespace(namespace string) (*engine.Credentials, string, error) {
	if c.config.Type != storage.DatabaseTypeMySQL {
		return c.credentials(), namespace, nil
	}
	database, err := mysqlDSNDatabase(c.config.TargetDSN)
	if err != nil {
		return nil, "", fmt.Errorf("inspect MySQL target DSN for namespace injection: %w", err)
	}
	if database != "" {
		if database != namespace {
			return nil, "", fmt.Errorf("target DSN database %q does not match requested namespace %q", database, namespace)
		}
		return c.credentials(), namespace, nil
	}
	if namespace == "" {
		return nil, "", fmt.Errorf("MySQL namespace is required for a namespace-free target DSN")
	}
	physical, err := c.physicalMySQLNamespace(namespace)
	if err != nil {
		return nil, "", err
	}
	dsn, err := mysqlDSNWithDatabase(c.config.TargetDSN, physical)
	if err != nil {
		return nil, "", err
	}
	return &engine.Credentials{
		DSN:      dsn,
		Metadata: c.config.Metadata,
	}, physical, nil
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
		ddlChanges[i] = storageTableChangeFromEngine(t, "")
	}

	namespaces, allShardPlans := c.namespacesFromEngineChanges(result.Changes, schemaFiles)
	if len(namespaces) == 0 {
		namespaces[c.config.Database] = &storage.NamespacePlanData{
			Tables: ddlChanges,
		}
	}

	// Don't store empty plans — no DDL changes, no VSchema changes.
	hasVSchemaChanges := false
	for _, ns := range namespaces {
		if ns.ChangesVSchema() {
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
		Shards:         allShardPlans,
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

	changes, violations, protoShards := c.planResultToProtoChanges(result)
	return &ternv1.PlanResponse{
		PlanId:         result.PlanID,
		Engine:         c.protoEngine(),
		Changes:        changes,
		LintViolations: violations,
		Shards:         protoShards,
		ExistingCopies: c.protoExistingCopies(result, c.runningCopiesForPlan(ctx, result, req.Environment, localPlanTarget(req, c.config.Database))),
	}, nil
}

// PlanDiff computes this deployment's desired-vs-live diff without persisting a
// plan. It shares the engine planning and proto conversion with Plan but stops
// before storage, so its result is not applyable (no plan_id). It is the
// read-only producer the control plane runs per deployment to detect review-time
// drift.
func (c *LocalClient) PlanDiff(ctx context.Context, req *ternv1.PlanRequest) (*ternv1.PlanDiffResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("plan diff request is required")
	}
	if c.getEngine() == nil {
		return nil, fmt.Errorf("no engine available for database type %q", c.config.Type)
	}

	schemaFiles, err := c.normalizeSchemaFiles(protoToSchemaFiles(req.SchemaFiles))
	if err != nil {
		return nil, err
	}

	planLogAttrs := []any{"database", c.config.Database}
	planLogAttrs = append(planLogAttrs, dsnLogAttrs(c.config.TargetDSN)...)
	planLogAttrs = append(planLogAttrs, "schema_file_count", len(schemaFiles))
	c.logger.Info("LocalClient.PlanDiff: calling engine", planLogAttrs...)

	result, err := c.planWithEngine(ctx, req, c.config.Database, schemaFiles)
	if err != nil {
		c.logger.Error("plan diff failed", "error", err, "database", c.config.Database)
		return nil, err // Error already has clear prefix (SQL syntax/usage error)
	}

	changes, violations, protoShards := c.planResultToProtoChanges(result)
	// Log the resulting change set so a diverged deployment is triageable from
	// this side's logs alone: change count plus one line per table change, with
	// DDL length instead of the DDL body.
	changeCount := 0
	for _, ch := range changes {
		changeCount += len(ch.TableChanges)
	}
	c.logger.Info("LocalClient.PlanDiff: plan diff response",
		"database", c.config.Database,
		"change_count", changeCount,
	)
	for _, ch := range changes {
		for _, tc := range ch.TableChanges {
			c.logger.Info("LocalClient.PlanDiff: table change",
				"database", c.config.Database,
				"namespace", tc.Namespace,
				"table", tc.TableName,
				"change_type", tc.ChangeType.String(),
				"ddl_len", len(tc.Ddl),
			)
		}
	}
	return &ternv1.PlanDiffResponse{
		Engine:         c.protoEngine(),
		Changes:        changes,
		LintViolations: violations,
		Shards:         protoShards,
	}, nil
}

// planResultToProtoChanges converts an engine plan result into the proto pieces
// Plan and PlanDiff both return: namespace-collapsed schema changes, lint
// violations, and per-shard membership. It has no storage side effects, so both
// the persisting Plan path and the non-persisting PlanDiff path produce
// identical change sets for the same engine result. A sharded change's tables
// repeat across a keyspace's shards; the namespace view lists each table once
// while per-shard membership travels separately on the response's Shards. A
// non-sharded change is an ordered statement sequence and passes through
// intact, every statement in plan order (see engine.SchemaChange.Sharded).
func (c *LocalClient) planResultToProtoChanges(result *engine.PlanResult) (changes []*ternv1.SchemaChange, violations []*ternv1.LintViolation, shards []*ternv1.ShardPlan) {
	protoByNS := make(map[string]*ternv1.SchemaChange)
	protoTableSeen := make(map[string]map[string]bool)
	for _, sc := range result.Changes {
		ns := c.planNamespace(sc.Namespace)
		protoSC := protoByNS[ns]
		if protoSC == nil {
			protoSC = &ternv1.SchemaChange{
				Namespace:             ns,
				Metadata:              maps.Clone(sc.Metadata),
				OriginalFiles:         sc.OriginalFiles,
				OriginalFilesCaptured: sc.OriginalFilesCaptured,
			}
			protoByNS[ns] = protoSC
			protoTableSeen[ns] = make(map[string]bool)
			changes = append(changes, protoSC)
		} else {
			// A sharded namespace's changes collapse into one wire change, so
			// merge metadata key by key (first write wins): the namespace's
			// VSchema annotation must survive no matter which shard's change
			// carries it.
			for key, value := range sc.Metadata {
				if protoSC.Metadata == nil {
					protoSC.Metadata = map[string]string{}
				}
				if _, ok := protoSC.Metadata[key]; !ok {
					protoSC.Metadata[key] = value
				}
			}
		}
		for _, t := range sc.TableChanges {
			if sc.Sharded() {
				if protoTableSeen[ns][t.Table] {
					continue
				}
				protoTableSeen[ns][t.Table] = true
			}
			protoSC.TableChanges = append(protoSC.TableChanges, protoTableChangeFromEngine(t, ns))
		}
		// A SchemaChange with an empty shard targets the whole namespace
		// (non-sharded engines) and contributes no shard rows.
		if sc.Sharded() {
			protoSP := &ternv1.ShardPlan{Shard: sc.ShardName(), Namespace: ns}
			for _, t := range sc.TableChanges {
				protoSP.Changes = append(protoSP.Changes, protoTableChangeFromEngine(t, ns))
			}
			shards = append(shards, protoSP)
		}
	}

	violations = make([]*ternv1.LintViolation, len(result.LintViolations))
	for i, w := range result.LintViolations {
		violations[i] = &ternv1.LintViolation{
			Table:    w.Table,
			Column:   w.Column,
			Linter:   w.Linter,
			Message:  w.Message,
			Severity: w.Severity,
		}
	}

	return changes, violations, shards
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
		// A database-scoped target DSN diffs the whole database as one unit:
		// the engine loads every live table while the desired state is only
		// the files the caller sent. A namespace withheld via
		// ignore_namespaces leaves its live tables with no declaring file, so
		// the declarative diff would plan them as drops — the inverse of
		// "ignore". No reliable live-table→namespace mapping exists on this
		// shape, so refuse rather than emit a plan that proposes dropping the
		// namespace the config says to leave alone.
		if len(req.GetIgnoredNamespaces()) > 0 {
			return nil, fmt.Errorf(
				"ignore_namespaces is not supported for MySQL targets whose DSN names a database: the whole database is diffed as one unit, so ignored namespaces %v would have their live tables planned as DROP TABLE; use a namespace-free target DSN or remove ignore_namespaces",
				req.GetIgnoredNamespaces())
		}
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
	// The grouping the apply will run under decides which stored progress it
	// can continue, so a prediction made here has to use the caller's, not
	// this engine's default. A caller that leaves the field absent predates
	// the choice and cannot state one; predicting the joined batch for it errs
	// toward disclosing a discard rather than promising a resume the apply
	// will not perform.
	groupedExecution := true
	if req.GroupedExecution != nil {
		groupedExecution = req.GetGroupedExecution()
	}
	return eng.Plan(ctx, &engine.PlanRequest{
		Database:         database,
		DatabaseType:     c.config.Type,
		SchemaFiles:      schemaFiles,
		Repository:       req.Repository,
		PullRequest:      int(req.PullRequest),
		Credentials:      creds,
		GroupedExecution: groupedExecution,
	})
}

func (c *LocalClient) planMySQLNamespacesWithEngine(ctx context.Context, eng engine.Engine, req *ternv1.PlanRequest, schemaFiles schema.SchemaFiles) (*engine.PlanResult, error) {
	namespaces := make([]string, 0, len(schemaFiles))
	for namespace := range schemaFiles {
		namespaces = append(namespaces, namespace)
	}
	sort.Strings(namespaces)

	result := &engine.PlanResult{PlanID: engine.NewPlanID(), NoChanges: true}
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
		result.ExistingCopies = append(result.ExistingCopies, nsResult.ExistingCopies...)
		if !nsResult.NoChanges || len(nsResult.Changes) > 0 {
			result.NoChanges = false
		}
	}
	return result, nil
}

// dispatchTargetShard validates a shard-scoped dispatch's target shard set and
// returns the single, trimmed shard name. The per-shard fan-out emits exactly
// one shard per operation, so more than one shard — or an empty/whitespace
// shard (which would build tasks with an invalid scope the engine rejects or,
// worse, mis-scopes) — is a malformed dispatch and fails closed.
func dispatchTargetShard(targetShards []string) (string, error) {
	if len(targetShards) != 1 {
		return "", fmt.Errorf("a sharded dispatch must target exactly one shard, got %d (%v)", len(targetShards), targetShards)
	}
	shard := strings.TrimSpace(targetShards[0])
	if shard == "" {
		return "", fmt.Errorf("sharded dispatch has an empty target shard")
	}
	return shard, nil
}

// scopedDispatchDDLChanges converts a shard-scoped dispatch's DDL changes to
// storage table changes, failing closed on a missing or malformed set. A
// shard-scoped dispatch is already scoped by the control-plane operator (one
// table's change for one shard, the per-shard fan-out the control plane owns),
// so it must carry valid, non-empty changes; falling back to the whole stored
// plan would apply unrelated tables on the targeted shard. The proto change type
// round-trips to the DDL action the stored plan would carry (create/alter/drop).
func scopedDispatchDDLChanges(changes []*ternv1.TableChange) ([]storage.TableChange, error) {
	if len(changes) == 0 {
		return nil, fmt.Errorf("shard-scoped dispatch carried no ddl_changes")
	}
	out := make([]storage.TableChange, 0, len(changes))
	for i, ch := range changes {
		if ch == nil {
			return nil, fmt.Errorf("shard-scoped dispatch ddl_change %d is nil", i)
		}
		// Trim before validating and storing: these values build operation keys and
		// task rows, so preserved surrounding whitespace would yield surprising keys
		// and reconciliation/progress mismatches. A shard-scoped dispatch is the
		// per-(namespace, shard) fan-out the control plane owns, so the namespace is
		// authoritative scope and must be present.
		namespace := strings.TrimSpace(ch.Namespace)
		table := strings.TrimSpace(ch.TableName)
		ddl := strings.TrimSpace(ch.Ddl)
		if namespace == "" {
			return nil, fmt.Errorf("shard-scoped dispatch ddl_change %d has empty namespace", i)
		}
		if table == "" || ddl == "" {
			return nil, fmt.Errorf("shard-scoped dispatch ddl_change %d has empty table or DDL", i)
		}
		// A shard-scoped dispatch carries only the table DDL the plan-store shard
		// gate admits — create/alter/drop for one shard — so allow exactly those and
		// reject everything else. A VSchema update is keyspace-wide, never
		// shard-scoped — it is applied by the task-less group_finalizer path — and
		// any other change type would build a shard-tagged task the sharded path has
		// no semantics for. An explicit allow-list keeps this gate's meaning fixed
		// as the change-type vocabulary grows.
		switch ch.ChangeType {
		case ternv1.ChangeType_CHANGE_TYPE_CREATE, ternv1.ChangeType_CHANGE_TYPE_ALTER, ternv1.ChangeType_CHANGE_TYPE_DROP:
		default:
			return nil, fmt.Errorf("shard-scoped dispatch ddl_change %d (table %q) has unsupported change type %v", i, table, ch.ChangeType)
		}
		out = append(out, StorageTableChangeFromProto(ch, namespace, table, ddl, protoChangeTypeToDDLAction(ch.ChangeType)))
	}
	return out, nil
}

// vschemaOnlyDispatchNamespaces returns, deduplicated in dispatch order, the
// namespaces of a VSchema-only dispatch — one where every ddl_change carries
// CHANGE_TYPE_VSCHEMA — or nil when the dispatch carries any table DDL or no
// changes at all. This is the shape the control plane's task-less VSchema
// dispatch sends; a mixed set is a work dispatch and returns nil so the caller
// treats it as one.
func vschemaOnlyDispatchNamespaces(changes []*ternv1.TableChange) []string {
	if len(changes) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(changes))
	namespaces := make([]string, 0, len(changes))
	for _, ch := range changes {
		if ch == nil || ch.ChangeType != ternv1.ChangeType_CHANGE_TYPE_VSCHEMA {
			return nil
		}
		namespace := strings.TrimSpace(ch.Namespace)
		if namespace == "" {
			return nil
		}
		if _, ok := seen[namespace]; ok {
			continue
		}
		seen[namespace] = struct{}{}
		namespaces = append(namespaces, namespace)
	}
	return namespaces
}

// finalizerDispatchScope validates a group_finalizer dispatch's namespace set
// against the stored plan and resolves the operation scope the finalizer is
// created with. A single-namespace dispatch (a sharded plan's per-namespace
// finalizer) is namespace-scoped; a multi-namespace dispatch (a VSchema-only
// plan) is deployment-scoped (empty namespace) and applies every dispatched
// namespace's VSchema in one engine apply — the engine treats the deployment
// as the unit of change, so splitting the namespaces across operations would
// have each drive validating keyspaces whose VSchema it never applied. Every
// dispatched namespace must hold a VSchema artifact in the stored plan —
// otherwise the drive would have nothing to apply and the operation would fail
// only after it was created.
//
// A single-namespace dispatch is shape-ambiguous on its own: a sharded plan's
// per-namespace finalizer and a deployment-scoped finalizer over a plan whose
// only VSchema change is one namespace arrive identically. The dispatch's
// generation manifest names the dispatcher's actual operation keys, so when
// one is present it resolves the shape; without one the dispatch keeps the
// namespace-scoped reading.
func finalizerDispatchScope(plan *storage.Plan, namespaces []string, generationManifest []string) (string, error) {
	if len(namespaces) == 0 {
		return "", fmt.Errorf("group_finalizer dispatch names no namespaces")
	}
	for _, namespace := range namespaces {
		if _, err := finalizerVSchemaChanges(plan, namespace); err != nil {
			return "", fmt.Errorf("group_finalizer dispatch for namespace %q: %w", namespace, err)
		}
	}
	if len(namespaces) == 1 && !manifestNamesDeploymentScopedFinalizer(generationManifest, namespaces[0]) {
		return namespaces[0], nil
	}
	// A deployment-scoped finalizer's drive applies every VSchema-changed
	// namespace in the stored plan, so a deployment-scoped dispatch must cover
	// exactly that set — a partial dispatch would silently apply namespaces the
	// dispatcher never named.
	planNamespaces := plan.VSchemaNamespaces()
	dispatched := append([]string(nil), namespaces...)
	sort.Strings(dispatched)
	if !slices.Equal(dispatched, planNamespaces) {
		return "", fmt.Errorf("group_finalizer dispatch names namespaces %v but plan %s changes VSchema in %v; a deployment-scoped dispatch must cover the plan's full VSchema set",
			namespaces, plan.PlanIdentifier, planNamespaces)
	}
	return "", nil
}

// manifestNamesDeploymentScopedFinalizer reports whether a single-namespace
// finalizer dispatch resolves to the deployment-scoped shape: its generation
// manifest names the deployment-scoped finalizer key and not the namespace's
// own finalizer key. The manifest is the dispatcher's declared operation-key
// set, so the operation created here must carry a key from that set — the
// manifest is the completion authority for the apply, and a key outside it is
// refused at creation. An empty manifest (a dispatch without generation
// tracking) resolves nothing and keeps the namespace-scoped reading.
func manifestNamesDeploymentScopedFinalizer(generationManifest []string, namespace string) bool {
	if len(generationManifest) == 0 {
		return false
	}
	if slices.Contains(generationManifest, namespace+finalizerOperationKeySuffix) {
		return false
	}
	return slices.Contains(generationManifest, finalizerDeploymentScopedKey)
}

// shardScopedDispatchOperationKey builds the operation key for a shard-scoped
// dispatch's single operation. The control plane's per-shard fan-out dispatches
// one (namespace, shard, table)'s changes per operation, so every change in the
// dispatch must agree on namespace and table; a mixed set is a malformed
// dispatch and fails closed rather than stamping a key that matches only some
// of the dispatch's tasks. Components must not contain the key's "/" delimiter:
// readers split the key back into exactly three parts, so a delimiter inside a
// component would produce a key they no longer recognize as shard-scoped work.
func shardScopedDispatchOperationKey(changes []storage.TableChange, shard string) (string, error) {
	if len(changes) == 0 {
		return "", fmt.Errorf("shard-scoped dispatch has no ddl changes to key its operation from")
	}
	namespace, table := changes[0].Namespace, changes[0].Table
	for _, ch := range changes[1:] {
		if ch.Namespace != namespace || ch.Table != table {
			return "", fmt.Errorf("shard-scoped dispatch mixes tables %s.%s and %s.%s; a dispatch must carry one table's changes for one shard",
				namespace, table, ch.Namespace, ch.Table)
		}
	}
	for _, component := range []struct{ name, value string }{
		{"namespace", namespace},
		{"shard", shard},
		{"table", table},
	} {
		if strings.Contains(component.value, "/") {
			return "", fmt.Errorf("shard-scoped dispatch %s %q contains the shard operation key delimiter; refusing to stamp a key readers would misparse",
				component.name, component.value)
		}
	}
	return storage.ShardOperationKey(namespace, shard, table), nil
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
	if !applyRequestCarriesPlanPayload(req) {
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

	// A non-primary deployment never planned locally, so materializing the
	// primary's reviewed DDL could silently replay it against a deployment whose
	// schema has drifted. Recompute this deployment's own diff against its live
	// schema and refuse unless it exactly matches the dispatched (reviewed) DDL,
	// keeping unreviewed DDL from being applied. The comparison is shard-aware: a
	// shard-scoped dispatch is checked against the re-plan restricted to its shard.
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

// namespacesFromEngineChanges builds per-namespace plan data from the engine's
// plan changes. For Vitess, each namespace is a keyspace; for Spirit, there is
// one namespace.
func (c *LocalClient) namespacesFromEngineChanges(changes []engine.SchemaChange, schemaFiles schema.SchemaFiles) (map[string]*storage.NamespacePlanData, []storage.ShardPlan) {
	namespaces := make(map[string]*storage.NamespacePlanData)
	seenTable := make(map[string]map[string]bool)
	var allShardPlans []storage.ShardPlan
	for _, sc := range changes {
		ns := c.planNamespace(sc.Namespace)
		nsData := namespaces[ns]
		if nsData == nil {
			nsData = &storage.NamespacePlanData{}
			namespaces[ns] = nsData
			seenTable[ns] = make(map[string]bool)
		}
		// The stored plan keeps namespace-level tables: a sharded change's
		// tables repeat across the keyspace's shards and are listed once, while
		// a non-sharded change is an ordered statement sequence stored intact,
		// every statement in plan order (see engine.SchemaChange.Sharded).
		for _, tc := range sc.TableChanges {
			if sc.Sharded() {
				if seenTable[ns][tc.Table] {
					continue
				}
				seenTable[ns][tc.Table] = true
			}
			nsData.Tables = append(nsData.Tables, storageTableChangeFromEngine(tc, ""))
		}
		// Record each changing shard's own changes so apply-create can rebuild
		// per-shard operation groups with per-shard DDL (a keyspace whose shards
		// diverge is persisted per shard, not collapsed; a shard is changing iff
		// it has changes). A SchemaChange with an empty shard targets the whole
		// namespace (non-sharded engines) and contributes no shard rows.
		if sc.Sharded() {
			sp := storage.ShardPlan{Shard: sc.ShardName(), Namespace: ns}
			for _, tc := range sc.TableChanges {
				sp.Changes = append(sp.Changes, storageTableChangeFromEngine(tc, ns))
			}
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
		// A sharded keyspace's SchemaChanges share one nsData, so merge the
		// gate-facing VSchema metadata key by key: a sibling shard's change
		// carrying less of it (or none) must not clear a deletion or mutation
		// record another shard already persisted.
		for key, value := range storage.VSchemaPlanMetadata(sc.Metadata) {
			if nsData.Metadata == nil {
				nsData.Metadata = map[string]string{}
			}
			if _, ok := nsData.Metadata[key]; !ok {
				nsData.Metadata[key] = value
			}
		}
		if sc.Metadata[storage.PlanMetadataVSchemaChanged] == "true" {
			if nsFiles, ok := schemaFiles[ns]; ok && nsFiles != nil {
				if vs, ok := nsFiles.Files[storage.VSchemaArtifactName]; ok && vs != "" {
					if nsData.Artifacts == nil {
						nsData.Artifacts = map[string]string{}
					}
					nsData.Artifacts[storage.VSchemaArtifactName] = vs
				}
			}
		}
	}
	return namespaces, allShardPlans
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
	parser, err := c.statementParser()
	if err != nil {
		return nil, err
	}
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
			nsData := ensure(ch.Namespace)
			vschemaChangedNamespaces[c.planNamespace(ch.Namespace)] = true
			// The dispatch carries the namespace's persisted VSchema
			// change-metadata on its VSchema change; merge it key by key
			// (first write wins) so the materialized plan runs the same
			// apply-time safety gates as a locally stored one.
			for key, value := range storage.VSchemaPlanMetadata(ch.Metadata) {
				if nsData.Metadata == nil {
					nsData.Metadata = map[string]string{}
				}
				if _, ok := nsData.Metadata[key]; !ok {
					nsData.Metadata[key] = value
				}
			}
			continue
		}
		op, err := materializedTableChangeOperation(parser, ch)
		if err != nil {
			return nil, err
		}
		nsData := ensure(ch.Namespace)
		nsData.Tables = append(nsData.Tables, StorageTableChangeFromProto(ch, ch.Namespace, ch.TableName, ch.Ddl, op))
	}

	for ns := range vschemaChangedNamespaces {
		nsFiles := schemaFiles[ns]
		vs := ""
		if nsFiles != nil {
			vs = nsFiles.Files[storage.VSchemaArtifactName]
		}
		if vs == "" {
			return nil, fmt.Errorf("apply request indicates a vschema change for namespace %q but carries no %s artifact", ns, storage.VSchemaArtifactName)
		}
		nsData := namespaces[ns]
		if nsData.Artifacts == nil {
			nsData.Artifacts = map[string]string{}
		}
		nsData.Artifacts[storage.VSchemaArtifactName] = vs
	}

	return namespaces, nil
}

// materializedTableChangeOperation recovers the storage operation for a
// materialized table change. The proto change type is authoritative when it maps
// to a known DDL action; otherwise the operation is classified from the request's
// authoritative DDL with the target dialect's parser. A single statement is
// classified directly; otherwise only a valid greenfield create set is
// admitted, and its operation comes from the first statement. DDL that classifies
// outside the shared DDL vocabulary, or as DML, is rejected — never mapped to
// an "unknown" action that would resume as a no-op. Classification, create-set
// shape, and non-DDL rejection remain distinct because they call for different
// remedies.
func materializedTableChangeOperation(parser ddl.StatementParser, ch *ternv1.TableChange) (string, error) {
	if op := protoChangeTypeToDDLAction(ch.ChangeType); op != "unknown" {
		return op, nil
	}
	if strings.TrimSpace(ch.Ddl) == "" {
		return "", fmt.Errorf("table change for %q has an unrecognized change type and no DDL to classify", ch.TableName)
	}
	statementType, _, classifyErr := parser.Classify(ch.Ddl)
	if classifyErr != nil {
		createSet, err := ddl.ParseCreateSet(parser, ch.Ddl)
		if err != nil {
			return "", fmt.Errorf("parse DDL for table %q as a create set: %w", ch.TableName, err)
		}
		statementType = createSet.Type
	}
	if statementType == ddl.StatementUnknown {
		return "", fmt.Errorf("DDL for table %q classified outside the shared DDL vocabulary; cannot recover an operation", ch.TableName)
	}
	if !statementType.IsDDL() {
		return "", fmt.Errorf("DDL for table %q is not a DDL statement, got %s", ch.TableName, statementType)
	}
	return ddl.StatementTypeToOp(statementType), nil
}

func rejectUnsafeDDLChangesWithoutOptIn(planIdentifier string, changes []storage.TableChange, applyOpts storage.ApplyOptions) error {
	if applyOpts.AllowUnsafe {
		return nil
	}
	for _, change := range changes {
		if change.RequiresUnsafeOptIn() {
			return fmt.Errorf("stored plan %s contains unsafe change for table %q: %s; retry with allow_unsafe=true", planIdentifier, change.Table, change.UnsafeOptInReason())
		}
	}
	return nil
}

// rejectUnsafeVSchemaChangesWithoutOptIn is the VSchema counterpart of
// rejectUnsafeDDLChangesWithoutOptIn: it re-checks the stored plan's recorded
// VSchema deletions and mutations at apply admission, so a dispatched
// VSchema-only operation — whose scope carries no table DDL for the DDL gate
// to inspect — still requires the same explicit opt-in the queueing gate
// enforced. The gate reads the whole plan rather than the dispatch scope: the
// VSchema change is namespace-level, and admission must never accept work the
// plan discloses as unsafe.
func rejectUnsafeVSchemaChangesWithoutOptIn(plan *storage.Plan, applyOpts storage.ApplyOptions) error {
	if applyOpts.AllowUnsafe {
		return nil
	}
	changes := plan.UnsafeVSchemaChanges()
	if len(changes) == 0 {
		return nil
	}
	change := changes[0]
	return fmt.Errorf("stored plan %s contains an unsafe VSchema change in namespace %q: %s; retry with allow_unsafe=true", plan.PlanIdentifier, change.Namespace, change.Reason)
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

// dispatchScope is the execution shape derived from a dispatch request: the
// DDL changes this dispatch drives, the single target shard of a shard-scoped
// dispatch, and whether the dispatch is a task-less VSchema finalizer.
type dispatchScope struct {
	ddlChanges         []storage.TableChange
	shard              string
	finalizer          bool
	finalizerNamespace string
}

// deriveDispatchScope determines a dispatch request's scope. A sharded
// engine's work is dispatched one apply_operation per shard, so a request that
// carries target shards is scoped to that single shard: it drives the
// operation's own DDL changes (req.DdlChanges) and tags its tasks with the
// shard, so the engine receives exactly one target shard. More than one target
// shard is a malformed dispatch (the per-shard fan-out emits one shard per
// operation) and fails closed.
//
// A dispatch with no target shards whose changes are all VSchema-typed is a
// VSchema apply: it targets VSchema documents and deliberately carries no
// shard (the VSchema is namespace-level). Whether the stored plan also
// carries table DDL (a group_finalizer alongside sibling shard work) or is
// VSchema-only (the plan's entire change), falling back to the plan's flat
// DDL here would be wrong: sibling table DDL would resurrect as shard-less
// work tasks a sharded engine rejects, and a VSchema-only plan would yield
// a work operation with no tasks — a shape with nothing to drive. The scope
// is the task-less group_finalizer instead — the dispatch's one namespace, or
// the whole deployment when the dispatch names every VSchema-changed
// namespace of a VSchema-only plan — so the drive applies the VSchema
// change(s) from the plan.
//
// Every other no-target-shard dispatch (a whole-deployment or non-sharded
// apply) uses the stored plan unchanged.
func deriveDispatchScope(plan *storage.Plan, req *ternv1.ApplyRequest) (dispatchScope, error) {
	scope := dispatchScope{ddlChanges: plan.FlatDDLChanges()}
	if len(req.TargetShards) > 0 {
		shard, err := dispatchTargetShard(req.TargetShards)
		if err != nil {
			return dispatchScope{}, err
		}
		scoped, err := scopedDispatchDDLChanges(req.DdlChanges)
		if err != nil {
			return dispatchScope{}, err
		}
		scope.shard = shard
		scope.ddlChanges = scoped
		return scope, nil
	}
	if namespaces := vschemaOnlyDispatchNamespaces(req.DdlChanges); len(namespaces) > 0 {
		namespace, err := finalizerDispatchScope(plan, namespaces, req.GenerationOperationKeys)
		if err != nil {
			return dispatchScope{}, err
		}
		scope.finalizer = true
		scope.finalizerNamespace = namespace
		scope.ddlChanges = nil
	}
	return scope, nil
}

// operationIdentityForDispatch returns the operation key and kind the dispatch
// scope stores on its apply_operations row.
//
// A shard-scoped dispatch tags its tasks with the target shard, so its
// operation row must carry the matching shard operation key: the task loaders
// treat a shard-tagged row as a drive task only when its operation's key
// matches (the convention the control plane's sharded fan-out stamps).
// Without the key, the operator's claim would load no drive tasks for the
// apply and the dispatched work would never run.
//
// A group_finalizer dispatch creates a task-less finalizer operation: the
// finalizer key names the scope the drive reconstructs the VSchema change(s)
// from — one namespace, or the whole deployment for a VSchema-only plan — and
// the kind routes the claim to the finalizer drive instead of failing closed
// on the empty task set.
func operationIdentityForDispatch(scope dispatchScope) (operationKey, operationKind string, err error) {
	if scope.shard != "" {
		operationKey, err = shardScopedDispatchOperationKey(scope.ddlChanges, scope.shard)
		if err != nil {
			return "", "", err
		}
		return operationKey, "", nil
	}
	if scope.finalizer {
		operationKey = finalizerDeploymentScopedKey
		if scope.finalizerNamespace != "" {
			operationKey = scope.finalizerNamespace + finalizerOperationKeySuffix
		}
		return operationKey, storage.ApplyOperationKindGroupFinalizer, nil
	}
	return "", "", nil
}

// buildDispatchTasks constructs the task rows for a dispatch scope's DDL
// changes, each tagged with the dispatch's shard.
func buildDispatchTasks(plan *storage.Plan, scope dispatchScope, environment, engineName string, optionsJSON []byte, now time.Time) []*storage.Task {
	tasks := make([]*storage.Task, len(scope.ddlChanges))
	for i, ddlChange := range scope.ddlChanges {
		tasks[i] = &storage.Task{
			TaskIdentifier: engine.NewTaskID(),
			PlanID:         plan.ID,
			Database:       plan.Database,
			DatabaseType:   plan.DatabaseType,
			Engine:         engineName,
			Repository:     plan.Repository,
			PullRequest:    plan.PullRequest,
			Environment:    environment,
			State:          state.Task.Pending,
			Options:        optionsJSON,
			TableName:      ddlChange.Table,
			Namespace:      ddlChange.Namespace,
			Shard:          scope.shard,
			DDL:            ddlChange.DDL,
			DDLAction:      ddlChange.Operation,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
	}
	return tasks
}

// dispatchApplyResponse is the accepted-dispatch response shape: the apply's
// identifier, the operation row the dispatch resolved to, and the derived
// operation key echoed so the caller can verify the response addresses its
// own operation.
func dispatchApplyResponse(apply *storage.Apply, operationID int64, operationKey string) *ternv1.ApplyResponse {
	return &ternv1.ApplyResponse{
		Accepted:         true,
		ApplyId:          apply.ApplyIdentifier,
		ApplyOperationId: strconv.FormatInt(operationID, 10),
		OperationKey:     operationKey,
	}
}

// findApplyOperationByKey returns the apply's operation row for this
// deployment and operation key, or nil when the apply has no such operation.
func (c *LocalClient) findApplyOperationByKey(ctx context.Context, apply *storage.Apply, operationKey string) (*storage.ApplyOperation, error) {
	store := c.storage.ApplyOperations()
	if store == nil {
		return nil, fmt.Errorf("apply operation store is not configured")
	}
	op, err := store.GetByApplyDeploymentAndOperationKey(ctx, apply.ID, c.config.Database, operationKey)
	if err != nil {
		return nil, fmt.Errorf("get apply_operation (deployment=%s, operation_key=%s) for apply %s: %w", c.config.Database, operationKey, apply.ApplyIdentifier, err)
	}
	return op, nil
}

// dispatchIntoExistingApply resolves a dispatch whose idempotency key already
// maps to an apply. The dispatch is identified within the apply by the
// operation key derived from its shape: a matching operation row means this
// exact dispatch was seen before and is replayed, and a missing row means the
// dispatch is a sibling operation of the same keyed generation, which is
// attached to the apply. dedupOutcome labels the replay metric with the path
// that resolved the key (first lookup, conflict race, or create race).
func (c *LocalClient) dispatchIntoExistingApply(ctx context.Context, req *ternv1.ApplyRequest, apply *storage.Apply, plan *storage.Plan, scope dispatchScope, dedupOutcome string) (*ternv1.ApplyResponse, error) {
	operationKey, operationKind, err := operationIdentityForDispatch(scope)
	if err != nil {
		return nil, fmt.Errorf("derive operation identity for dispatch into apply %s: %w", apply.ApplyIdentifier, err)
	}
	existing, err := c.findApplyOperationByKey(ctx, apply, operationKey)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		c.logger.Info("Apply: returning existing apply for idempotency key",
			append(apply.LogAttrs(),
				"idempotency_key", req.IdempotencyKey,
				"operation_key", operationKey,
				"dedup_outcome", dedupOutcome)...)
		metrics.RecordRemoteApplyDedup(ctx, req.Database, req.Environment, dedupOutcome)
		return dispatchApplyResponse(apply, existing.ID, operationKey), nil
	}
	return c.attachDispatchOperation(ctx, req, apply, plan, scope, operationKey, operationKind)
}

// validateDispatchAgainstManifest is the fail-closed gate for a dispatch's
// operation key against a generation manifest. A key outside the manifest
// means the two planes disagree about the generation — the dispatcher declared
// one operation set and is now sending another — and accepting it would attach
// an operation the completion gate never waits for. Empty manifests (an apply
// created without one, or a dispatch that carries none) validate everything: a
// dispatcher that never declared a generation keeps the attached-rows-only
// completion semantics. A dispatch whose carried manifest disagrees with the
// stored one is logged for triage but judged against the stored manifest,
// which is immutable for the generation.
func (c *LocalClient) validateDispatchAgainstManifest(ctx context.Context, req *ternv1.ApplyRequest, apply *storage.Apply, operationKey string) *ternv1.ApplyResponse {
	if len(req.GenerationOperationKeys) > 0 && len(apply.ExpectedOperationKeys) > 0 &&
		!slices.Equal(normalizedManifest(req.GenerationOperationKeys), apply.ExpectedOperationKeys) {
		c.logger.Warn("Apply: dispatch carries a generation manifest that disagrees with the stored one; validating against the stored manifest",
			append(apply.LogAttrs(),
				"operation_key", operationKey,
				"idempotency_key", req.IdempotencyKey,
				"stored_manifest", apply.ExpectedOperationKeys,
				"dispatch_manifest", req.GenerationOperationKeys)...)
	}
	if apply.AllowsOperationKey(operationKey) {
		return nil
	}
	c.logger.Warn("Apply: refusing operation outside the apply's generation manifest; dispatch is rejected",
		append(apply.LogAttrs(),
			"operation_key", operationKey,
			"idempotency_key", req.IdempotencyKey,
			"stored_manifest", apply.ExpectedOperationKeys)...)
	metrics.RecordRemoteApplyAttach(ctx, req.Database, req.Environment, "manifest_refused")
	return &ternv1.ApplyResponse{
		Accepted:     false,
		ErrorMessage: fmt.Sprintf("operation %s is not in apply %s's generation manifest %v; refusing to attach an operation its completion gate never waits for", operationKey, apply.ApplyIdentifier, apply.ExpectedOperationKeys),
	}
}

// normalizedManifest returns the canonical stored form of a dispatched
// generation manifest: sorted with duplicates removed, so equality checks and
// the stored column are independent of dispatch ordering.
func normalizedManifest(keys []string) []string {
	if len(keys) == 0 {
		return nil
	}
	normalized := slices.Clone(keys)
	slices.Sort(normalized)
	return slices.Compact(normalized)
}

// refuseAttachToTerminalApply is the fail-closed rejection for an attach
// against an apply that is (or just became) terminal: its target reservation
// is released and no drive will pick new work up, so accepting the operation
// would strand it. The deployment's remaining operations cannot dispatch until
// an operator reconciles the apply.
func (c *LocalClient) refuseAttachToTerminalApply(ctx context.Context, req *ternv1.ApplyRequest, apply *storage.Apply, operationKey string) *ternv1.ApplyResponse {
	c.logger.Warn("Apply: refusing to attach operation to terminal keyed apply; dispatch is rejected",
		append(apply.LogAttrs(),
			"operation_key", operationKey,
			"idempotency_key", req.IdempotencyKey)...)
	metrics.RecordRemoteApplyAttach(ctx, req.Database, req.Environment, "terminal_refused")
	return &ternv1.ApplyResponse{
		Accepted:     false,
		ErrorMessage: fmt.Sprintf("apply %s for this idempotency key is terminal (%s); operation %s cannot attach", apply.ApplyIdentifier, apply.State, operationKey),
	}
}

// attachDispatchOperation adds a sibling dispatch's operation and its tasks to
// the deployment's existing keyed apply. The attach runs the same conflict and
// unsafe-DDL gates a fresh apply runs, so attaching never admits work a create
// would have refused, and it fails closed on a terminal apply.
func (c *LocalClient) attachDispatchOperation(ctx context.Context, req *ternv1.ApplyRequest, apply *storage.Apply, plan *storage.Plan, scope dispatchScope, operationKey, operationKind string) (*ternv1.ApplyResponse, error) {
	if state.IsTerminalApplyState(apply.State) {
		return c.refuseAttachToTerminalApply(ctx, req, apply, operationKey), nil
	}
	if refusal := c.validateDispatchAgainstManifest(ctx, req, apply, operationKey); refusal != nil {
		return refusal, nil
	}

	// An attach already belongs to a keyed apply, so a conflict here is another
	// apply holding the database and there is nothing for this dispatch to
	// resolve into. Adoption is a create-path outcome only.
	_, releasedHolders, err := c.checkActiveTaskConflict(ctx, plan, req.Environment, scope.shard, apply.ID)
	if err != nil {
		return &ternv1.ApplyResponse{
			Accepted:     false,
			ErrorMessage: err.Error(),
		}, nil
	}

	eng := c.getEngine()
	if eng == nil {
		return nil, fmt.Errorf("no engine configured for type: %s", c.config.Type)
	}

	applyOpts := storage.ApplyOptionsFromMap(req.Options)
	applyOpts.Target = plan.Target
	if err := rejectUnsafeDDLChangesWithoutOptIn(plan.PlanIdentifier, scope.ddlChanges, applyOpts); err != nil {
		return &ternv1.ApplyResponse{
			Accepted:     false,
			ErrorMessage: err.Error(),
		}, nil
	}
	if err := rejectUnsafeVSchemaChangesWithoutOptIn(plan, applyOpts); err != nil {
		return &ternv1.ApplyResponse{
			Accepted:     false,
			ErrorMessage: err.Error(),
		}, nil
	}
	optionsJSON := storage.MarshalApplyOptions(applyOpts)

	now := time.Now()
	tasks := buildDispatchTasks(plan, scope, req.Environment, eng.Name(), optionsJSON, now)
	operation := &storage.ApplyOperation{
		Deployment:    c.config.Database,
		OperationKey:  operationKey,
		OperationKind: operationKind,
		Target:        plan.Target,
		State:         state.ApplyOperation.Pending,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	err = c.storage.Applies().AttachOperationWithTasks(ctx, apply, operation, tasks)
	switch {
	case errors.Is(err, storage.ErrApplyOperationExists):
		// A concurrent same-operation attach won the insert; the winner's row
		// is this dispatch's replay target.
		winner, lookupErr := c.findApplyOperationByKey(ctx, apply, operationKey)
		if lookupErr != nil {
			return nil, fmt.Errorf("resolve attach race for operation %s of apply %s: %w", operationKey, apply.ApplyIdentifier, lookupErr)
		}
		if winner == nil {
			return nil, fmt.Errorf("operation %s of apply %s exists per the unique index but was not found on re-read", operationKey, apply.ApplyIdentifier)
		}
		c.logger.Info("Apply: concurrent attach won the operation insert; replaying the winner's row",
			append(apply.LogAttrs(),
				"operation_key", operationKey,
				"idempotency_key", req.IdempotencyKey)...)
		metrics.RecordRemoteApplyAttach(ctx, req.Database, req.Environment, "attach_race")
		return dispatchApplyResponse(apply, winner.ID, operationKey), nil
	case errors.Is(err, storage.ErrApplyNotActive):
		return c.refuseAttachToTerminalApply(ctx, req, apply, operationKey), nil
	case err != nil:
		return nil, fmt.Errorf("attach operation %s to apply %s: %w", operationKey, apply.ApplyIdentifier, err)
	}

	c.markSupersededHolders(ctx, apply, releasedHolders, scope.ddlChanges)

	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventInfo, storage.LogSourceSchemaBot,
		fmt.Sprintf("Operation attached: %s", operationKey), "", apply.State)
	metrics.RecordRemoteApplyAttach(ctx, req.Database, req.Environment, "attached")
	c.logger.Info("Apply: attached operation to keyed apply",
		append(apply.LogAttrs(),
			"operation_key", operationKey,
			"task_count", len(tasks),
			"plan_id", plan.PlanIdentifier)...)
	c.wakeOperatorForQueuedApply(apply)

	return dispatchApplyResponse(apply, operation.ID, operationKey), nil
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

	// Idempotent re-dispatch: if this request's idempotency key already maps to
	// an apply, resolve the dispatch against that apply instead of starting a
	// duplicate — replaying the dispatch's own operation, or attaching it as a
	// sibling operation of the same keyed generation. This runs before the
	// active-task conflict check so a re-dispatch of our own in-flight apply is
	// recovered rather than rejected as "already in progress".
	if existing, err := c.existingIdempotentApply(ctx, req); err != nil {
		return nil, err
	} else if existing != nil {
		plan, err := c.planForApplyRequest(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("resolve plan %s for idempotent re-dispatch: %w", req.PlanId, err)
		}
		if plan == nil {
			return &ternv1.ApplyResponse{
				Accepted:     false,
				ErrorMessage: "plan not found",
			}, nil
		}
		scope, err := deriveDispatchScope(plan, req)
		if err != nil {
			return nil, fmt.Errorf("apply for plan %s: %w", req.PlanId, err)
		}
		return c.dispatchIntoExistingApply(ctx, req, existing, plan, scope, "hit")
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
	scope, err := deriveDispatchScope(plan, req)
	if err != nil {
		return nil, fmt.Errorf("apply for plan %s: %w", req.PlanId, err)
	}
	c.logger.Info("Apply: retrieved plan",
		"plan_id", req.PlanId,
		"plan_identifier", plan.PlanIdentifier,
		"ddl_change_count", len(scope.ddlChanges),
		"target_shards", req.TargetShards,
		"database", plan.Database,
	)

	// Local mode: check for active tasks with engine verification
	blocking, releasedHolders, conflictErr := c.checkActiveTaskConflict(ctx, plan, req.Environment, scope.shard, 0)
	if conflictErr != nil {
		// A same-key request that committed while we were in the conflict check
		// races as "already in progress". Re-resolve by idempotency key so the
		// winning apply is returned instead of a spurious rejection.
		if existing, lookupErr := c.existingIdempotentApply(ctx, req); lookupErr != nil {
			return nil, errors.Join(conflictErr, lookupErr)
		} else if existing != nil {
			c.logger.Info("Apply: idempotency key resolved an active-conflict race",
				append(existing.LogAttrs(), "idempotency_key", req.IdempotencyKey)...)
			return c.dispatchIntoExistingApply(ctx, req, existing, plan, scope, "conflict_race")
		}
		// A dispatch whose key resolves to nothing may still be a re-apply of the
		// change the blocking apply is running — the recovery for work that
		// outlived the apply identity that started it. Resolve into that apply
		// rather than being refused by it; anything short of an exact match keeps
		// the refusal. Adoption only answers a conflict the check actually found:
		// an error without a named blocking task is a storage read failure, and
		// there is no apply to resolve into.
		if blocking.blocks() {
			if adopted, ok := c.adoptLiveApplyForDispatch(ctx, req, plan, scope, blocking); ok {
				return adopted, nil
			}
		}
		// The conflict travels as structured facts beside the error text. The
		// text is the engine's own and stays for the logs; a caller that must
		// tell an operator why the database is busy renders the conflict.
		return &ternv1.ApplyResponse{
			Accepted:     false,
			ErrorMessage: conflictErr.Error(),
			Conflict:     blocking.conflict(),
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

	// Build typed ApplyOptions for storage from the full wire option map, so
	// every engine-relevant option the dispatch carried (branch, rollback,
	// ...) survives the round trip into the stored apply — the queued
	// operator drive re-derives its options from the stored apply, not from this
	// request. Revert window is ON by default — only disabled when skip_revert
	// is explicitly set. The plan's validated target is authoritative over any
	// target string the request carried.
	applyOpts := storage.ApplyOptionsFromMap(options)
	applyOpts.Target = plan.Target
	if err := rejectUnsafeDDLChangesWithoutOptIn(plan.PlanIdentifier, scope.ddlChanges, applyOpts); err != nil {
		return &ternv1.ApplyResponse{
			Accepted:     false,
			ErrorMessage: err.Error(),
		}, nil
	}
	if err := rejectUnsafeVSchemaChangesWithoutOptIn(plan, applyOpts); err != nil {
		return &ternv1.ApplyResponse{
			Accepted:     false,
			ErrorMessage: err.Error(),
		}, nil
	}
	optionsJSON := storage.MarshalApplyOptions(applyOpts)

	// VSchema application is not modeled as a synthetic task. PlanetScale
	// surfaces its VSchema status/diff from engine resume metadata, and a sharded
	// apply runs VSchema as a task-less group_finalizer derived from the plan.

	// Build the Apply record (1 Apply -> N Tasks). The dispatch's generation
	// manifest is stored on the apply at creation: sibling dispatches of the
	// same keyed generation attach one at a time, and the manifest is what the
	// state projection holds the apply's success verdict on until every
	// declared operation has attached and finished.
	applyIdentifier := engine.NewApplyID()
	apply := &storage.Apply{
		ApplyIdentifier:       applyIdentifier,
		PlanID:                plan.ID,
		Database:              plan.Database,
		DatabaseType:          plan.DatabaseType,
		Deployment:            c.config.Database,
		Repository:            plan.Repository,
		PullRequest:           plan.PullRequest,
		Environment:           req.Environment,
		Caller:                caller,
		Engine:                eng.Name(),
		State:                 state.Apply.Pending,
		Options:               optionsJSON,
		IdempotencyKey:        req.IdempotencyKey,
		ExpectedOperationKeys: normalizedManifest(req.GenerationOperationKeys),
		CreatedAt:             now,
		UpdatedAt:             now,
	}

	c.logger.Info("Apply: creating tasks",
		"plan_id", plan.PlanIdentifier,
		"ddl_change_count", len(scope.ddlChanges),
	)
	for i, ddlChange := range scope.ddlChanges {
		c.logger.Debug("Apply: DDLChange",
			"index", i,
			"table", ddlChange.Table,
			"ddl", ddlChange.DDL,
		)
	}

	tasks := buildDispatchTasks(plan, scope, req.Environment, eng.Name(), optionsJSON, now)

	operationKey, operationKind, err := operationIdentityForDispatch(scope)
	if err != nil {
		return nil, fmt.Errorf("apply for plan %s: %w", req.PlanId, err)
	}

	// A dispatch that declares a generation manifest must name its own
	// operation in it: the manifest is the completion authority for the apply
	// this dispatch creates, and an apply whose first operation is outside its
	// own manifest could never complete. Refuse the malformed dispatch instead
	// of creating an apply that is wedged from birth.
	if !apply.AllowsOperationKey(operationKey) {
		c.logger.Error("Apply: dispatch's generation manifest does not name its own operation; refusing the malformed dispatch",
			append(apply.LogAttrs(),
				"plan_id", plan.PlanIdentifier,
				"operation_key", operationKey,
				"idempotency_key", req.IdempotencyKey,
				"manifest", apply.ExpectedOperationKeys)...)
		return &ternv1.ApplyResponse{
			Accepted:     false,
			ErrorMessage: fmt.Sprintf("dispatch generation manifest %v does not include its own operation key %q; refusing the malformed dispatch", apply.ExpectedOperationKeys, operationKey),
		}, nil
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
		Deployment:    apply.Deployment,
		OperationKey:  operationKey,
		OperationKind: operationKind,
		Target:        plan.Target,
		State:         state.ApplyOperation.Pending,
		CreatedAt:     now,
		UpdatedAt:     now,
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
				append(existing.LogAttrs(), "idempotency_key", req.IdempotencyKey)...)
			return c.dispatchIntoExistingApply(ctx, req, existing, plan, scope, "create_race")
		}
		return nil, fmt.Errorf("create apply %s with tasks and operations: %w", applyIdentifier, err)
	}
	apply.ID = applyID

	c.markSupersededHolders(ctx, apply, releasedHolders, scope.ddlChanges)

	// Record the queue event in the apply's durable log; the drive itself starts
	// when an operator claims the apply, which the timeline records separately.
	c.logApplyEvent(ctx, applyID, nil, storage.LogLevelInfo, storage.LogEventInfo, storage.LogSourceSchemaBot,
		fmt.Sprintf("Apply queued: %s", applyIdentifier), "", state.Apply.Pending)

	// Register any pending observer under the created apply's ID before queueing.
	// The observer registry is per-client-instance: in-process dispatchers — the
	// only callers that set a pending observer — resolve this client from the
	// same client cache (service client map or target router) the operator's
	// drive resolves it from, so the drive finds the observer regardless of when
	// the claim happens. Wire dispatches never set a pending observer, so a
	// data-plane operator driving through its own client has no observer to lose.
	if obs := c.consumePendingObserver(); obs != nil {
		// Set the apply ID on the observer if it supports it (e.g., CommentObserver
		// needs the ID to look up tracked comments for editing).
		type applyIDSetter interface{ SetApplyID(int64) }
		if setter, ok := obs.(applyIDSetter); ok {
			setter.SetApplyID(apply.ID)
		}
		c.SetObserver(apply.ID, obs)
		// The apply became claimable when the create transaction committed, so a
		// poll-tick claim can start — and even finish — the drive before the
		// registration above. A still-running drive finds the observer on its next
		// progress event; one that already settled will not, so deliver the missed
		// terminal notification here in that case.
		c.deliverTerminalIfSettled(ctx, apply.ID)
	}

	// The apply is queued, not driven here: every drive — the initial dispatch
	// included — runs under an operator claim, so lease-guarded writes (per-shard
	// progress write-through, operation state) always hold the capability they
	// require. The operator's pending-claim picks the apply up; the wake makes
	// that prompt instead of waiting out the poll interval.
	c.logger.Info("Apply: queued for operator drive",
		append(apply.LogAttrs(), "plan_id", plan.PlanIdentifier)...)
	c.wakeOperatorForQueuedApply(apply)

	return dispatchApplyResponse(apply, operations[0].ID, operationKey), nil
}

// getEngine returns the appropriate engine based on database type.
func (c *LocalClient) getEngine() engine.Engine {
	switch c.config.Type {
	case storage.DatabaseTypeMySQL:
		return c.spiritEngine
	case storage.DatabaseTypeVitess:
		return c.planetscaleEngine
	case storage.DatabaseTypePostgres:
		return c.postgresEngine
	default:
		// A registered engine for a non-built-in type (nil if none registered).
		return c.customEngine
	}
}

// Engine returns the engine that drives this client's database type, exposed
// so callers that assemble a LocalClient can verify the engine settings they
// configured actually reached it.
func (c *LocalClient) Engine() engine.Engine {
	return c.getEngine()
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
		settled, err := c.settledControlRequests(ctx, apply)
		if err != nil {
			c.logger.Error("progress: serving a task-less apply without its settled control requests; a rejected command stays invisible to the accepting plane until the next poll",
				append(apply.LogAttrs(), "error", err)...)
		}
		return &ternv1.ProgressResponse{
			State:                  storageStateToProto(apply.State),
			Engine:                 c.protoEngine(),
			SettledControlRequests: settled,
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
			t.State == state.Task.CatchingUp ||
			t.State == state.Task.Checksumming ||
			t.State == state.Task.PostChecksum ||
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
	c.logger.Debug("Progress: selected task", "task_id", activeTask.TaskIdentifier, "state", activeTask.State, "apply_id", activeTask.ApplyID)

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
			TableName:    t.TableName,
			Ddl:          t.DDL,
			Namespace:    t.Namespace,
			Status:       t.State,
			TaskId:       t.TaskIdentifier,
			IsInstant:    t.IsInstant || vitessApplyIsInstant,
			ChangeType:   ddlActionToProtoChangeType(t.DDLAction),
			ErrorMessage: t.ErrorMessage,
		}

		// Table figures come from the stored task row the drive maintains.
		tp.PercentComplete = int32(t.ProgressPercent)
		tp.RowsCopied = t.RowsCopied
		tp.RowsTotal = t.RowsTotal
		tp.ChecksumRowsChecked = t.ChecksumRowsChecked
		tp.ChecksumRowsTotal = t.ChecksumRowsTotal
		tp.Throttled = t.Throttled
		tp.ThrottleReason = t.ThrottleReason
		// For Spirit the stored figure is the runner-wide remaining-copy
		// estimate stamped on every still-copying table (see
		// buildSpiritTableProgress), so in a multi-table apply each table
		// reports the whole run's ETA rather than its own.
		tp.EtaSeconds = int64(t.ETASeconds)
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

	// Populate apply_id and engine from the apply record.
	// The apply record's engine is the source of truth (set at apply creation time).
	if apply, err := c.storage.Applies().Get(ctx, activeTask.ApplyID); err == nil && apply != nil {
		resp.ApplyId = apply.ApplyIdentifier
		settled, err := c.settledControlRequests(ctx, apply)
		if err != nil {
			c.logger.Error("progress: serving an apply without its settled control requests; a rejected command stays invisible to the accepting plane until the next poll",
				append(apply.LogAttrs(), "error", err)...)
		}
		resp.SettledControlRequests = settled
		if eng, err := engineNameToProto(apply.Engine); err != nil {
			return nil, fmt.Errorf("invalid engine on apply %s: %w", apply.ApplyIdentifier, err)
		} else {
			resp.Engine = eng
		}
		opts := storage.ParseApplyOptions(apply.Options)
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

// MaxLogsLimit is the most entries one Logs read returns; a larger request is
// served the cap's worth of newest entries instead of failing. Readers that
// over-fetch to detect older history must keep the extra entry within this
// cap: a request past it comes back exactly at the cap, and the probe entry
// is the one clamped away.
const MaxLogsLimit = 1000

func (c *LocalClient) Logs(ctx context.Context, req *ternv1.LogsRequest) (*ternv1.LogsResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("logs request is required")
	}
	if req.ApplyId == "" {
		return nil, fmt.Errorf("apply_id is required")
	}
	limit := int(req.Limit)
	if limit <= 0 {
		limit = 50
	}
	if limit > MaxLogsLimit {
		limit = MaxLogsLimit
	}
	apply, err := c.storage.Applies().GetByApplyIdentifier(ctx, req.ApplyId)
	if err != nil {
		return nil, fmt.Errorf("get apply %s for logs: %w", req.ApplyId, err)
	}
	if apply == nil {
		return nil, fmt.Errorf("get apply %s for logs: %w", req.ApplyId, storage.ErrApplyNotFound)
	}
	logs, err := c.storage.ApplyLogs().GetRecentByApply(ctx, apply.ID, limit)
	if err != nil {
		return nil, fmt.Errorf("get recent logs for apply %s: %w", req.ApplyId, err)
	}
	resp := &ternv1.LogsResponse{ApplyId: req.ApplyId, Logs: make([]*ternv1.ApplyLog, 0, len(logs))}
	for _, log := range logs {
		entry := &ternv1.ApplyLog{Id: log.ID, Level: log.Level, EventType: log.EventType, Source: log.Source, Message: log.Message, OldState: log.OldState, NewState: log.NewState, MetadataJson: log.Metadata, CreatedAt: log.CreatedAt.UTC().Format(time.RFC3339Nano)}
		if log.TaskID != nil {
			entry.TaskId = log.TaskID
		}
		resp.Logs = append(resp.Logs, entry)
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

// settledControlRequests projects the apply's terminal control requests onto the
// progress response. A control RPC is accepted when the request is queued, not
// when it takes effect, so this is how the plane that accepted one learns
// whether the operation actually landed — without it, a rejection recorded here
// never leaves this plane.
//
// Callers on the progress path degrade rather than propagate: the field is
// advisory, and the same settled rows are reported on every poll until the
// operator retries the operation, so a failed load costs one tick of notice and
// self-heals. Failing the RPC instead would throw away the state and task
// progress the caller drives the apply from, over a field it only displays.
func (c *LocalClient) settledControlRequests(ctx context.Context, apply *storage.Apply) ([]*ternv1.SettledControlRequest, error) {
	controlStore := c.storage.ControlRequests()
	if controlStore == nil {
		return nil, fmt.Errorf("control request store is not available for apply %s", apply.ApplyIdentifier)
	}
	requests, err := controlStore.ListSettled(ctx, apply.ID)
	if err != nil {
		return nil, fmt.Errorf("load settled control requests for apply %s: %w", apply.ApplyIdentifier, err)
	}
	settled := make([]*ternv1.SettledControlRequest, 0, len(requests))
	for _, req := range requests {
		entry := &ternv1.SettledControlRequest{
			Operation:    string(req.Operation),
			Status:       string(req.Status),
			ErrorMessage: req.ErrorMessage,
			RequestedBy:  req.RequestedBy,
		}
		if req.CompletedAt != nil {
			entry.SettledAt = req.CompletedAt.UTC().Format(time.RFC3339)
		}
		settled = append(settled, entry)
	}
	return settled, nil
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
	case state.Task.CatchingUp:
		// Row copy is done; the engine is applying the changeset accumulated
		// from the binlog during the copy. Ranks after Running and before
		// Checksumming — the verify that follows this first drain — so a
		// later poll never regresses the table to a plain copy.
		return 3, true
	case state.Task.Checksumming:
		// Row copy is done; the engine is verifying the copied data. Ranks after
		// CatchingUp and before PostChecksum — the second drain that follows
		// the verify — so a later poll never regresses a checksumming table
		// to an earlier phase.
		return 4, true
	case state.Task.PostChecksum:
		// The verify passed and the engine is applying the changes that
		// accumulated while it ran. Ranks after Checksumming so a stale
		// checksum poll never rewinds the table into a verify that already
		// finished, and before WaitingForCutover — the phase the table moves
		// through next.
		return 5, true
	case state.Task.WaitingForCutover:
		return 6, true
	case state.Task.CuttingOver:
		return 7, true
	case state.Task.RevertWindow:
		return 8, true
	case state.Task.Reverting:
		// Undoing the change after the revert window; ranks after RevertWindow so
		// a reverting table never regresses to the resumable-window phase.
		return 9, true
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

// pendingDropsDisabled reports whether this client drops tables outright rather
// than quarantining them in the pending drops database.
//
// The quarantine is opt-in: a deployment turns it on with the pending_drops
// metadata key, and anything else, including an absent key, drops the table
// outright. Quarantining is only safe for a deployment that also reaps its own
// targets, because a quarantine no cleaner reaches grows on the target server
// forever, so an embedder that never states the intent must not inherit it.
func pendingDropsDisabled(metadata map[string]string) bool {
	return metadata["pending_drops"] != "true"
}
