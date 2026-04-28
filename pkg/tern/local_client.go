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
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

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

	// recoveryInterval controls how often the recovery loop polls for retryable applies.
	// Defaults to 30s. Tests may lower this.
	recoveryInterval time.Duration

	// cancelApply cancels the background goroutine running executeApplySequential
	// or executeApplyAtomic. Set when an apply starts, called by Stop().
	// Protected by cancelMu since Apply and Stop run on different goroutines.
	cancelMu    sync.Mutex
	cancelApply context.CancelFunc

	// cancelRecovery stops the background recovery loop. Set by StartRecoveryLoop.
	cancelRecovery context.CancelFunc
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
		config:            cfg,
		storage:           stor,
		spiritEngine:      spirit.New(spirit.Config{Logger: logger}),
		planetscaleEngine: psEngine,
		logger:            logger,
		heartbeatInterval: 10 * time.Second,
		recoveryInterval:  30 * time.Second,
	}, nil
}

// IsRemote returns false — LocalClient runs in the same process and creates
// apply/task records in the same database as the API layer.
func (c *LocalClient) IsRemote() bool { return false }

// protoEngine returns the proto engine type based on database configuration.
func (c *LocalClient) protoEngine() ternv1.Engine {
	if c.config.Type == storage.DatabaseTypeVitess {
		return ternv1.Engine_ENGINE_PLANETSCALE
	}
	return ternv1.Engine_ENGINE_SPIRIT
}

// engineNameToProto converts a storage engine name to the proto enum.
func engineNameToProto(name string) (ternv1.Engine, error) {
	switch name {
	case storage.EnginePlanetScale:
		return ternv1.Engine_ENGINE_PLANETSCALE, nil
	case storage.EngineSpirit:
		return ternv1.Engine_ENGINE_SPIRIT, nil
	default:
		return 0, fmt.Errorf("unknown engine: %s", name)
	}
}

// Close closes the client and releases resources.
func (c *LocalClient) Close() error {
	if c.cancelRecovery != nil {
		c.cancelRecovery()
	}
	return nil
}

// StartRecoveryLoop starts a background goroutine that polls for
// failed_retryable applies and re-dispatches them. Call Close() to stop.
func (c *LocalClient) StartRecoveryLoop(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	c.cancelRecovery = cancel

	go func() {
		c.logger.Info("recovery loop started", "interval", c.recoveryInterval)
		ticker := time.NewTicker(c.recoveryInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				c.logger.Info("recovery loop stopped")
				return
			case <-ticker.C:
				c.recoverOneApply(ctx)
			}
		}
	}()
}

// recoverOneApply claims one failed_retryable apply and re-dispatches it.
func (c *LocalClient) recoverOneApply(ctx context.Context) {
	apply, err := c.storage.Applies().ClaimForRecovery(ctx)
	if err != nil {
		c.logger.Error("recovery loop: claim failed", "error", err)
		return
	}
	if apply == nil {
		return // nothing to recover
	}

	c.logger.Info("recovery loop: claimed apply for retry",
		"apply_id", apply.ApplyIdentifier,
		"state", apply.State,
		"attempt", apply.Attempt,
		"database", apply.Database,
	)

	// Only re-dispatch failed_retryable applies. Stale active applies
	// (pending/running with dead heartbeat) are handled by the existing
	// conflict check in Apply().
	if apply.State != state.Apply.FailedRetryable {
		c.logger.Debug("recovery loop: skipping non-retryable apply",
			"apply_id", apply.ApplyIdentifier, "state", apply.State)
		return
	}

	// Transition back to running for the retry attempt
	apply.State = state.Apply.Running
	apply.ErrorMessage = ""
	apply.UpdatedAt = time.Now()
	if err := c.storage.Applies().Update(ctx, apply); err != nil {
		c.logger.Error("recovery loop: failed to update apply state",
			"apply_id", apply.ApplyIdentifier, "error", err)
		return
	}

	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventInfo, storage.LogSourceSchemaBot,
		fmt.Sprintf("Recovery attempt %d: retrying apply %s", apply.Attempt, apply.ApplyIdentifier), "", state.Apply.Running)

	// Load the plan and re-dispatch
	plan, err := c.storage.Plans().GetByID(ctx, apply.PlanID)
	if err != nil || plan == nil {
		c.logger.Error("recovery loop: plan not found", "apply_id", apply.ApplyIdentifier, "plan_id", apply.PlanID, "error", err)
		apply.State = state.Apply.Failed
		apply.ErrorMessage = "recovery failed: plan not found"
		now := time.Now()
		apply.CompletedAt = &now
		_ = c.storage.Applies().Update(ctx, apply)
		return
	}

	tasks, err := c.storage.Tasks().GetByApplyID(ctx, apply.ID)
	if err != nil {
		c.logger.Error("recovery loop: failed to load tasks", "apply_id", apply.ApplyIdentifier, "error", err)
		return
	}

	// Re-dispatch in background (same as initial Apply)
	applyCtx := context.WithoutCancel(ctx)
	go c.runWithRecovery(applyCtx, apply, tasks, func() {
		c.executeApplyAtomic(applyCtx, apply, tasks, plan, parseOptionsMap(apply.Options))
	})
}

// parseOptionsMap converts stored apply options JSON back to a string map
// for re-dispatching applies through executeApplyAtomic.
func parseOptionsMap(data []byte) map[string]string {
	opts := storage.ParseApplyOptions(data)
	m := make(map[string]string)
	if opts.DeferCutover {
		m["defer_cutover"] = "true"
	}
	if opts.SkipRevert {
		m["skip_revert"] = "true"
	}
	if opts.AllowUnsafe {
		m["allow_unsafe"] = "true"
	}
	if opts.Branch != "" {
		m["branch"] = opts.Branch
	}
	return m
}

// credentials returns engine credentials from the client config.
func (c *LocalClient) credentials() *engine.Credentials {
	return &engine.Credentials{
		DSN:      c.config.TargetDSN,
		Metadata: c.config.Metadata,
	}
}

// Health checks the service health.
func (c *LocalClient) Health(ctx context.Context) error {
	return c.storage.Ping(ctx)
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

	c.logger.Info("LocalClient.Plan: calling engine",
		"database", c.config.Database,
		"target_dsn_prefix", c.config.TargetDSN[:min(len(c.config.TargetDSN), 40)],
		"schema_file_count", len(schemaFiles),
	)

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
			Operation: t.Operation,
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
				Operation: tc.Operation,
			})
		}
		// Store VSchema from schema files if present for this namespace.
		if nsFiles, ok := schemaFiles[ns]; ok && nsFiles != nil {
			if vs, ok := nsFiles.Files["vschema.json"]; ok {
				nsData.VSchema = json.RawMessage(vs)
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
		Repository:     req.Repository,
		PullRequest:    int(req.PullRequest),
		Environment:    req.Environment,
		SchemaFiles:    schemaFiles,
		Namespaces:     namespaces,
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

	// Options come directly as a map from proto
	options := req.Options
	if options == nil {
		options = make(map[string]string)
	}

	// Read environment and caller from proto fields (preferred), fall back to options.
	environment := req.Environment
	if environment == "" {
		environment = options["environment"]
	}
	caller := req.Caller
	if caller == "" {
		caller = options["caller"]
	}

	// Detect atomic mode (--defer-cutover)
	deferCutover := options["defer_cutover"] == "true"
	allowUnsafe := options["allow_unsafe"] == "true"

	// Build typed ApplyOptions for storage (booleans, not strings).
	// Revert window is ON by default — only disabled when skip_revert is explicitly set.
	skipRevert := options["skip_revert"] == "true"
	applyOpts := storage.ApplyOptions{
		DeferCutover: deferCutover,
		AllowUnsafe:  allowUnsafe,
		SkipRevert:   skipRevert,
	}
	optionsJSON := storage.MarshalApplyOptions(applyOpts)

	// Create Apply record first (1 Apply -> N Tasks)
	applyIdentifier := "apply-" + strings.ReplaceAll(uuid.New().String(), "-", "")[:16]
	apply := &storage.Apply{
		ApplyIdentifier: applyIdentifier,
		PlanID:          plan.ID,
		Database:        plan.Database,
		DatabaseType:    plan.DatabaseType,
		Deployment:      c.config.Database,
		Repository:      plan.Repository,
		PullRequest:     plan.PullRequest,
		Environment:     environment,
		Caller:          caller,
		Engine:          eng.Name(),
		State:           state.Apply.Pending,
		Options:         optionsJSON,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	applyID, err := c.storage.Applies().Create(ctx, apply)
	if err != nil {
		return nil, fmt.Errorf("create apply: %w", err)
	}
	apply.ID = applyID

	// Log apply started
	c.logApplyEvent(ctx, applyID, nil, storage.LogLevelInfo, storage.LogEventInfo, storage.LogSourceSchemaBot,
		fmt.Sprintf("Apply started: %s", applyIdentifier), "", state.Apply.Pending)

	// Create one Task per DDLChange in the plan.
	// For VSchema-only deploys (0 DDL changes), create a synthetic task per
	// keyspace with VSchema changes so the progress API has something to track.
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

	// Add synthetic VSchema tasks when there are VSchema changes but no DDL
	// for a given namespace. This ensures the progress API tracks VSchema-only deploys.
	if len(ddlChanges) == 0 {
		for ns, nsData := range plan.Namespaces {
			if len(nsData.VSchema) > 0 {
				ddlChanges = append(ddlChanges, storage.TableChange{
					Table:     "vschema:" + ns,
					Namespace: ns,
					Operation: "vschema_update",
				})
			}
		}
	}

	tasks := make([]*storage.Task, len(ddlChanges))
	for i, ddlChange := range ddlChanges {
		taskIdentifier := "task-" + strings.ReplaceAll(uuid.New().String(), "-", "")[:16]
		tasks[i] = &storage.Task{
			TaskIdentifier: taskIdentifier,
			ApplyID:        applyID,
			PlanID:         plan.ID,
			Database:       plan.Database,
			DatabaseType:   plan.DatabaseType,
			Engine:         eng.Name(),
			Repository:     plan.Repository,
			PullRequest:    plan.PullRequest,
			Environment:    options["environment"],
			State:          state.Task.Pending,
			Options:        optionsJSON,
			TableName:      ddlChange.Table,
			Namespace:      ddlChange.Namespace,
			DDL:            ddlChange.DDL,
			DDLAction:      ddlChange.Operation,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		taskID, err := c.storage.Tasks().Create(ctx, tasks[i])
		if err != nil {
			return nil, fmt.Errorf("create task for table %s: %w", ddlChange.Table, err)
		}
		tasks[i].ID = taskID
	}

	// Start apply in background with cancellable context (Stop() cancels this)
	applyCtx, cancelApply := context.WithCancel(context.WithoutCancel(ctx))
	c.cancelMu.Lock()
	c.cancelApply = cancelApply
	c.cancelMu.Unlock()
	if deferCutover || c.config.Type == storage.DatabaseTypeVitess {
		// Atomic mode: all DDLs in one engine call.
		// For Spirit: atomic cutover (--defer-cutover).
		// For Vitess: always atomic — one deploy request per apply.
		go c.runWithRecovery(applyCtx, apply, tasks, func() {
			c.executeApplyAtomic(applyCtx, apply, tasks, plan, options)
		})
	} else {
		// Independent mode: each DDL runs separately, cuts over independently
		go c.runWithRecovery(applyCtx, apply, tasks, func() {
			c.executeApplySequential(applyCtx, apply, tasks, plan, options)
		})
	}

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
// If req.ApplyId is set, scopes to that specific apply. Otherwise queries by database.
func (c *LocalClient) Progress(ctx context.Context, req *ternv1.ProgressRequest) (*ternv1.ProgressResponse, error) {
	var tasks []*storage.Task
	var err error

	if req.ApplyId != "" {
		// Scope to specific apply.
		apply, lookupErr := c.storage.Applies().GetByApplyIdentifier(ctx, req.ApplyId)
		if lookupErr != nil {
			return nil, fmt.Errorf("get apply %s: %w", req.ApplyId, lookupErr)
		}
		if apply != nil {
			tasks, err = c.storage.Tasks().GetByApplyID(ctx, apply.ID)
			if err != nil {
				return nil, fmt.Errorf("get tasks for apply %s: %w", req.ApplyId, err)
			}
		}
	} else {
		// Fall back to database lookup — this means the caller didn't provide
		// an apply_id, which makes it ambiguous if multiple applies exist.
		c.logger.Warn("Progress: no apply_id provided, falling back to database lookup",
			"database", c.config.Database)
		tasks, err = c.storage.Tasks().GetByDatabase(ctx, c.config.Database)
		if err != nil {
			return nil, fmt.Errorf("get tasks failed: %w", err)
		}
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
	// For Vitess, reconstruct ResumeState from vitess_apply_data so the engine
	// can poll the deploy request and query SHOW VITESS_MIGRATIONS.
	var engineResult *engine.ProgressResult
	var vitessApplyIsInstant bool
	// Query engine for live progress. For Vitess, also query during pending state
	// to surface PlanetScale states (creating branch, deploy request, etc.).
	queryDuringPending := c.config.Type == storage.DatabaseTypeVitess
	if eng != nil && (activeTask.State != state.Task.Pending || queryDuringPending) {
		progressReq := &engine.ProgressRequest{
			Database:    c.config.Database,
			Credentials: creds,
		}
		if c.config.Type == storage.DatabaseTypeVitess {
			vad, vadErr := c.storage.VitessApplyData().GetByApplyID(ctx, activeTask.ApplyID)
			switch {
			case vadErr != nil:
				c.logger.Error("failed to load VitessApplyData for progress", "apply_id", activeTask.ApplyID, "error", vadErr)
			case vad == nil:
				c.logger.Warn("VitessApplyData not found for progress — apply may still be initializing", "apply_id", activeTask.ApplyID)
			default:
				vitessApplyIsInstant = vad.IsInstant
				meta, _ := json.Marshal(map[string]any{
					"branch_name":        vad.BranchName,
					"deploy_request_id":  vad.DeployRequestID,
					"deploy_request_url": vad.DeployRequestURL,
					"is_instant":         vad.IsInstant,
				})
				progressReq.ResumeState = &engine.ResumeState{
					MigrationContext: vad.MigrationContext,
					Metadata:         string(meta),
				}
			}
		}
		result, err := eng.Progress(ctx, progressReq)
		if err == nil {
			engineResult = result
			c.logger.Info("Progress: engine returned", "engine_state", result.State, "message", result.Message, "task_id", activeTask.TaskIdentifier, "storage_state", activeTask.State)

			// Only update task state from engine if task is not already in a terminal state.
			// Terminal states (CANCELLED, COMPLETED, FAILED, etc.) should be preserved -
			// they represent the final state set by the apply execution.
			if !state.IsTerminalTaskState(activeTask.State) {
				activeTask.State = engineStateToStorage(result.State)
				now := time.Now()
				activeTask.UpdatedAt = now
				if result.State.IsTerminal() && activeTask.CompletedAt == nil {
					activeTask.CompletedAt = &now
				}
				if err := c.storage.Tasks().Update(ctx, activeTask); err != nil {
					c.logger.Warn("failed to update task state from progress poll", "task_id", activeTask.TaskIdentifier, "state", activeTask.State, "error", err)
				}
			}

			// Also update the apply record if the engine reports a terminal state
			// but the apply hasn't been updated yet. This can happen when the
			// progress handler detects completion before the background polling
			// goroutine persists it.
			if result.State.IsTerminal() {
				apply, _ := c.storage.Applies().GetByApplyIdentifier(ctx, req.ApplyId)
				if apply != nil && !state.IsTerminalApplyState(apply.State) {
					now := time.Now()
					apply.State = engineStateToStorage(result.State)
					apply.CompletedAt = &now
					apply.UpdatedAt = now
					if err := c.storage.Applies().Update(ctx, apply); err != nil {
						c.logger.Warn("failed to update apply from progress poll", "apply_id", apply.ApplyIdentifier, "state", apply.State, "apply_db_id", apply.ID, "error", err)
					}
					c.logger.Info("apply state updated from progress polling", "apply_id", apply.ApplyIdentifier, "state", apply.State)
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
			TableName: t.TableName,
			Ddl:       t.DDL,
			Namespace: t.Namespace,
			Status:    t.State,
			TaskId:    t.TaskIdentifier,
			IsInstant: t.IsInstant || vitessApplyIsInstant,
		}

		// Look up engine progress for this table
		if et, ok := engineProgressForTask(engineTableProgress, t); ok {
			// Use live progress from engine (uppercase to match storage convention)
			tp.Status = strings.ToUpper(et.State)
			tp.PercentComplete = int32(et.Progress)
			tp.RowsCopied = et.RowsCopied
			tp.RowsTotal = et.RowsTotal
			tp.EtaSeconds = et.ETASeconds
			tp.IsInstant = et.IsInstant
			tp.ProgressDetail = et.ProgressDetail

			// Update task timestamps from engine if not already set.
			updated := false
			if et.StartedAt != nil && t.StartedAt == nil {
				t.StartedAt = et.StartedAt
				updated = true
			}
			if et.CompletedAt != nil && t.CompletedAt == nil {
				t.CompletedAt = et.CompletedAt
				updated = true
			}
			if updated {
				t.UpdatedAt = time.Now()
				if err := c.storage.Tasks().Update(ctx, t); err != nil {
					c.logger.Error("failed to update task timestamps", "task_id", t.TaskIdentifier, "error", err)
				}
			}

			// Build shards if available
			shards := make([]*ternv1.ShardProgress, len(et.Shards))
			for j, sh := range et.Shards {
				shards[j] = &ternv1.ShardProgress{
					Shard:           sh.Shard,
					Status:          sh.State,
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
	// (e.g., creating_branch, creating_deploy_request during PlanetScale setup).
	overallState := deriveOverallState(currentApplyTasks)
	// For Vitess setup phases, the apply record has a more specific state
	// (creating_branch, applying_branch_changes, creating_deploy_request)
	// than what task states alone can derive. Check the apply record when
	// tasks are still pending or when the overall state doesn't yet reflect
	// real progress (e.g., engine returns "running" during setup).
	if applyRec, err := c.storage.Applies().Get(ctx, activeTask.ApplyID); err == nil && applyRec != nil {
		switch applyRec.State {
		case state.Apply.CreatingBranch, state.Apply.ApplyingBranchChanges, state.Apply.CreatingDeployRequest:
			overallState = applyRec.State
		}
	}

	// If no error from engine, check stored task errors (for restart recovery)
	if errorMessage == "" {
		for _, t := range currentApplyTasks {
			if t.State == state.Task.Failed && t.ErrorMessage != "" {
				errorMessage = t.ErrorMessage
				break
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
		if state.IsState(overallState, state.Apply.CreatingBranch, state.Apply.ApplyingBranchChanges, state.Apply.CreatingDeployRequest) {
			if logs, err := c.storage.ApplyLogs().GetByApply(ctx, apply.ID); err == nil && len(logs) > 0 {
				latest := logs[len(logs)-1]
				resp.Metadata = ensureMetadata(resp.Metadata)
				resp.Metadata["status_detail"] = latest.Message
			}
		}
	}

	return resp, nil
}

func ensureMetadata(m map[string]string) map[string]string {
	if m == nil {
		return make(map[string]string)
	}
	return m
}
