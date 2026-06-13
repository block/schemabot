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
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/block/spirit/pkg/statement"
	spirittable "github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/engine/planetscale"
	"github.com/block/schemabot/pkg/engine/spirit"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/psclient"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// LocalConfig holds configuration for the local Tern client.
type LocalConfig struct {
	// Database is the name of this database.
	Database string

	// Type is the database type: "mysql" or "vitess".
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
}

// LocalClient implements Client by calling the Spirit engine directly.
// This is used when SchemaBot runs as a single service with embedded engine.
// It uses SchemaBot's storage for plans and tasks.
type LocalClient struct {
	config            LocalConfig
	storage           storage.Storage
	spiritEngine      engine.Engine
	planetscaleEngine engine.Engine
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
	if cfg.Type == storage.DatabaseTypeVitess {
		apiURL := cfg.Metadata["api_url"]
		psEngine = planetscale.NewWithClient(logger, func(tokenName, tokenValue string) (psclient.PSClient, error) {
			return psclient.NewPSClientWithBaseURL(tokenName, tokenValue, apiURL)
		})
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
		logger:            logger,
		heartbeatInterval: 10 * time.Second,
	}, nil
}

// IsRemote returns false — LocalClient runs in the same process and creates
// apply/task records in the same database as the API layer.
func (c *LocalClient) IsRemote() bool { return false }

// Endpoint returns the database name for this local client.
func (c *LocalClient) Endpoint() string { return c.config.Database }

// protoEngine returns the proto engine type based on database configuration.
func (c *LocalClient) protoEngine() ternv1.Engine {
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

// Health checks the service health.
func (c *LocalClient) Health(ctx context.Context) error {
	return c.storage.Ping(ctx)
}

// PullSchema fetches the live schema and returns declarative schema files.
func (c *LocalClient) PullSchema(ctx context.Context, req *ternv1.PullSchemaRequest) (*ternv1.PullSchemaResponse, error) {
	if c.config.Type != storage.DatabaseTypeMySQL {
		return nil, fmt.Errorf("pull schema for database %s type %s: only %s is supported: %w", c.config.Database, c.config.Type, storage.DatabaseTypeMySQL, ErrPullSchemaUnsupportedType)
	}
	if req.Type != "" && req.Type != c.config.Type {
		return nil, fmt.Errorf("pull schema for database %s: request type %q does not match client type %q: %w", c.config.Database, req.Type, c.config.Type, ErrPullSchemaInvalidRequest)
	}

	attrs := []any{"database", c.config.Database}
	attrs = append(attrs, dsnLogAttrs(c.config.TargetDSN)...)
	c.logger.Info("LocalClient.PullSchema: loading live schema", attrs...)

	db, err := sql.Open("mysql", c.config.TargetDSN)
	if err != nil {
		return nil, fmt.Errorf("open database %s for schema pull: %w", c.config.Database, err)
	}
	defer utils.CloseAndLog(db)

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping database %s for schema pull: %w", c.config.Database, err)
	}

	tables, err := spirittable.LoadSchemaFromDB(ctx, db, spirittable.WithoutUnderscoreTables, spirittable.WithoutArchiveTables, spirittable.WithStrippedAutoIncrement)
	if err != nil {
		return nil, fmt.Errorf("load live schema for database %s: %w", c.config.Database, err)
	}
	sort.Slice(tables, func(i, j int) bool { return tables[i].Name < tables[j].Name })

	files := make(map[string]string, len(tables))
	for _, tbl := range tables {
		content, err := pulledSchemaFileContent(c.config.Database, tbl)
		if err != nil {
			return nil, err
		}
		files[tbl.Name+".sql"] = content
	}

	c.logger.Info("LocalClient.PullSchema: loaded live schema",
		"database", c.config.Database,
		"table_count", len(tables),
	)

	return &ternv1.PullSchemaResponse{
		Database:    c.config.Database,
		Type:        c.config.Type,
		Environment: req.Environment,
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			c.config.Database: {Files: files},
		},
		TableCount: int32(len(tables)),
	}, nil
}

func pulledSchemaFileContent(database string, tbl spirittable.TableSchema) (string, error) {
	if tbl.Name == "" {
		return "", fmt.Errorf("load live schema for database %s: table with empty name", database)
	}
	content := strings.TrimRight(tbl.Schema, "\n") + "\n"
	if _, err := statement.ParseCreateTable(content); err != nil {
		return "", fmt.Errorf("parse pulled schema for database %s table %s: %w", database, tbl.Name, err)
	}
	return content, nil
}

// Plan generates a schema change plan from declarative schema files.
func (c *LocalClient) Plan(ctx context.Context, req *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	if c.config.Type != storage.DatabaseTypeMySQL && c.config.Type != storage.DatabaseTypeVitess {
		return nil, fmt.Errorf("type must be %q or %q", storage.DatabaseTypeMySQL, storage.DatabaseTypeVitess)
	}

	eng := c.getEngine()
	if eng == nil {
		return nil, fmt.Errorf("no engine configured for type: %s", c.config.Type)
	}

	// Convert schema files from proto to engine type
	schemaFiles := protoToSchemaFiles(req.SchemaFiles)

	creds := c.credentials()

	planLogAttrs := []any{"database", c.config.Database}
	planLogAttrs = append(planLogAttrs, dsnLogAttrs(c.config.TargetDSN)...)
	planLogAttrs = append(planLogAttrs, "schema_file_count", len(schemaFiles))
	c.logger.Info("LocalClient.Plan: calling engine", planLogAttrs...)

	result, err := eng.Plan(ctx, &engine.PlanRequest{
		Database:     c.config.Database,
		DatabaseType: c.config.Type,
		SchemaFiles:  schemaFiles,
		Repository:   req.Repository,
		PullRequest:  int(req.PullRequest),
		Credentials:  creds,
	})
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
		ns := sc.Namespace
		if ns == "" {
			ns = c.config.Database
		}
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
		// Only store VSchema when the Plan detected a change.
		if sc.Metadata["vschema_changed"] == "true" {
			if nsFiles, ok := schemaFiles[ns]; ok && nsFiles != nil {
				if vs, ok := nsFiles.Files["vschema.json"]; ok {
					nsData.VSchema = json.RawMessage(vs)
				}
			}
		}
	}
	// Store original schema for rollback support.
	// For single-namespace (Spirit), attach to that namespace.
	// For multi-namespace (Vitess), original schema is per-keyspace from the engine.
	if result.OriginalSchema != nil {
		if len(namespaces) == 1 {
			for _, nsData := range namespaces {
				nsData.OriginalSchema = result.OriginalSchema
			}
		}
	}
	if len(namespaces) == 0 {
		namespaces[c.config.Database] = &storage.NamespacePlanData{
			Tables:         ddlChanges,
			OriginalSchema: result.OriginalSchema,
		}
	}

	// Don't store empty plans — no DDL changes, no VSchema changes.
	hasVSchemaChanges := false
	for _, ns := range namespaces {
		if len(ns.VSchema) > 0 {
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
		protoSC := &ternv1.SchemaChange{
			Namespace: sc.Namespace,
			Metadata:  sc.Metadata,
		}
		for _, t := range sc.TableChanges {
			protoSC.TableChanges = append(protoSC.TableChanges, &ternv1.TableChange{
				TableName:    t.Table,
				ChangeType:   changeTypeToProto(t.Operation),
				Ddl:          t.DDL,
				IsUnsafe:     t.IsUnsafe,
				UnsafeReason: t.UnsafeReason,
				Namespace:    sc.Namespace,
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
	}
	optionsJSON := storage.MarshalApplyOptions(applyOpts)

	// Create VSchema tasks for namespaces with VSchema changes so the
	// progress API and TUI can track VSchema application alongside DDL.
	// For VSchema-only deploys (0 DDL changes), this gives the progress API
	// something to track.
	for ns, nsData := range plan.Namespaces {
		if len(nsData.VSchema) > 0 {
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
		return nil
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
						if derived, ok := c.deriveAggregateApplyState(ctx, apply, currentApplyTasks); ok {
							apply.State = derived
							apply.UpdatedAt = now
							if err := c.storage.Applies().Update(ctx, apply); err != nil {
								c.logger.Warn("failed to update apply after progress task state update", "apply_id", apply.ApplyIdentifier, "state", apply.State, "error", err)
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
						now := time.Now()
						apply.State = taskStateToApplyState(taskState)
						if retryableFailure {
							apply.CompletedAt = nil
						} else {
							apply.CompletedAt = &now
						}
						if result.State == engine.StateFailed {
							apply.ErrorMessage = progressFailureMessage(result)
						}
						apply.UpdatedAt = now
						if err := c.storage.Applies().Update(ctx, apply); err != nil {
							c.logger.Warn("failed to update apply from progress poll", "apply_id", apply.ApplyIdentifier, "state", apply.State, "apply_db_id", apply.ID, "error", err)
						}
						c.logger.Info("apply state updated from progress polling", "apply_id", apply.ApplyIdentifier, "state", apply.State)
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
