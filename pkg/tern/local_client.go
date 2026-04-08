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
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/engine/spirit"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
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
}

// LocalClient implements Client by calling the Spirit engine directly.
// This is used when SchemaBot runs as a single service with embedded engine.
// It uses SchemaBot's storage for plans and tasks.
type LocalClient struct {
	config       LocalConfig
	storage      storage.Storage
	spiritEngine engine.Engine
	logger       *slog.Logger

	// heartbeatInterval controls how often the apply heartbeat updates updated_at.
	// Defaults to 10s. Tests may lower this to verify heartbeat behavior.
	heartbeatInterval time.Duration

	// cancelApply cancels the background goroutine running executeApplySequential
	// or executeApplyAtomic. Set when an apply starts, called by Stop().
	cancelApply context.CancelFunc
}

// Compile-time check that LocalClient implements Client.
var _ Client = (*LocalClient)(nil)

// NewLocalClient creates a new local Tern client that calls the Spirit engine directly.
// The storage parameter should be SchemaBot's storage instance for plan/task management.
func NewLocalClient(cfg LocalConfig, stor storage.Storage, logger *slog.Logger) (*LocalClient, error) {
	return &LocalClient{
		config:            cfg,
		storage:           stor,
		spiritEngine:      spirit.New(spirit.Config{Logger: logger}),
		logger:            logger,
		heartbeatInterval: 10 * time.Second,
	}, nil
}

// IsRemote returns false — LocalClient runs in the same process and creates
// apply/task records in the same database as the API layer.
func (c *LocalClient) IsRemote() bool { return false }

// Close closes the client and releases resources.
func (c *LocalClient) Close() error {
	// LocalClient doesn't own storage, so nothing to close
	return nil
}

// credentials returns engine credentials from the client config.
func (c *LocalClient) credentials() *engine.Credentials {
	return &engine.Credentials{DSN: c.config.TargetDSN}
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

	plan := &storage.Plan{
		PlanIdentifier: result.PlanID,
		Database:       c.config.Database,
		DatabaseType:   c.config.Type,
		Repository:     req.Repository,
		PullRequest:    int(req.PullRequest),
		Environment:    req.Environment,
		SchemaFiles:    schemaFiles,
		Namespaces: map[string]*storage.NamespacePlanData{
			c.config.Database: {
				Tables:         ddlChanges,
				OriginalSchema: result.OriginalSchema,
			},
		},
		CreatedAt: time.Now(),
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

	// Convert lint warnings to proto
	warnings := make([]*ternv1.LintWarning, len(result.LintWarnings))
	for i, w := range result.LintWarnings {
		warnings[i] = &ternv1.LintWarning{
			Table:    w.Table,
			Column:   w.Column,
			Linter:   w.Linter,
			Message:  w.Message,
			Severity: w.Severity,
		}
	}

	return &ternv1.PlanResponse{
		PlanId:       result.PlanID,
		Engine:       ternv1.Engine_ENGINE_SPIRIT,
		Changes:      changes,
		LintWarnings: warnings,
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

	// Detect atomic mode (--defer-cutover)
	deferCutover := options["defer_cutover"] == "true"
	allowUnsafe := options["allow_unsafe"] == "true"

	// Build typed ApplyOptions for storage (booleans, not strings)
	applyOpts := storage.ApplyOptions{
		DeferCutover: deferCutover,
		AllowUnsafe:  allowUnsafe,
	}
	optionsJSON := storage.MarshalApplyOptions(applyOpts)

	// Create Apply record first (1 Apply -> N Tasks)
	applyIdentifier := "apply-" + strings.ReplaceAll(uuid.New().String(), "-", "")[:16]
	apply := &storage.Apply{
		ApplyIdentifier: applyIdentifier,
		PlanID:          plan.ID,
		Database:        plan.Database,
		DatabaseType:    plan.DatabaseType,
		Repository:      plan.Repository,
		PullRequest:     plan.PullRequest,
		Environment:     options["environment"],
		Caller:          options["caller"],
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

	// Create one Task per DDLChange in the plan
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
	applyCtx, cancelApply := context.WithCancel(context.Background())
	c.cancelApply = cancelApply
	if deferCutover {
		// Atomic mode: all DDLs in one Spirit call, atomic cutover
		go c.executeApplyAtomic(applyCtx, apply, tasks, plan, options)
	} else {
		// Independent mode: each DDL runs separately, cuts over independently
		go c.executeApplySequential(applyCtx, apply, tasks, plan, options)
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
	default:
		// For vitess, return nil - not supported in local mode
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
			t.State == state.Task.CuttingOver:
			// Prefer actively running/waiting tasks
			activeTask = t
		case t.State == state.Task.Stopped && stoppedTask == nil:
			// Stopped tasks are resumable — track them separately
			stoppedTask = t
		case t.State == state.Task.Pending && pendingTask == nil:
			// Track first pending task as fallback
			pendingTask = t
		case state.IsTerminalTaskState(t.State) && latestTask == nil:
			// Track most recent terminal task as final fallback
			latestTask = t
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
			State: ternv1.State_STATE_NO_ACTIVE_CHANGE,
		}, nil
	}
	c.logger.Info("Progress: selected task", "task_id", activeTask.TaskIdentifier, "state", activeTask.State, "apply_id", activeTask.ApplyID)

	// Get ALL tasks for this apply (completed + running + pending)
	currentApplyTasks := filterTasksByApply(tasks, activeTask.ApplyID)

	creds := c.credentials()
	eng := c.getEngine()

	// Get live progress from engine for the currently running task
	var engineResult *engine.ProgressResult
	if eng != nil && activeTask.State != state.Task.Pending {
		result, err := eng.Progress(ctx, &engine.ProgressRequest{
			Database:    c.config.Database,
			Credentials: creds,
		})
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
				_ = c.storage.Tasks().Update(ctx, activeTask)
			}
		}
	}

	// Build tables array with ALL tasks for this apply
	tables := make([]*ternv1.TableProgress, 0, len(currentApplyTasks))
	var summary string

	// Build a map of engine table progress by table name for fast lookup
	engineTableProgress := make(map[string]*engine.TableProgress)
	var errorMessage string
	if engineResult != nil {
		for i := range engineResult.Tables {
			et := &engineResult.Tables[i]
			engineTableProgress[et.Table] = et
		}
		summary = engineResult.Message
		errorMessage = engineResult.ErrorMessage
	}

	for _, t := range currentApplyTasks {
		tp := &ternv1.TableProgress{
			TableName: t.TableName,
			Ddl:       t.DDL,
			Status:    t.State,
			TaskId:    t.TaskIdentifier,
		}

		// Look up engine progress for this table
		if et, ok := engineTableProgress[t.TableName]; ok {
			// Use live progress from engine (uppercase to match storage convention)
			tp.Status = strings.ToUpper(et.State)
			tp.PercentComplete = int32(et.Progress)
			tp.RowsCopied = et.RowsCopied
			tp.RowsTotal = et.RowsTotal
			tp.EtaSeconds = et.ETASeconds
			tp.IsInstant = et.IsInstant
			tp.ProgressDetail = et.ProgressDetail

			// Build shards if available
			shards := make([]*ternv1.ShardProgress, len(et.Shards))
			for j, sh := range et.Shards {
				shards[j] = &ternv1.ShardProgress{
					Shard:      sh.Shard,
					Status:     sh.State,
					RowsCopied: sh.RowsCopied,
					RowsTotal:  sh.RowsTotal,
					EtaSeconds: sh.ETASeconds,
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
		}

		tables = append(tables, tp)
	}

	// Derive overall state from ALL tasks in this apply
	overallState := deriveOverallState(currentApplyTasks)

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
		Engine:       ternv1.Engine_ENGINE_SPIRIT,
		Tables:       tables,
		Summary:      summary,
		ErrorMessage: errorMessage,
	}

	// Populate apply_id and volume from the apply record.
	if apply, err := c.storage.Applies().Get(ctx, activeTask.ApplyID); err == nil && apply != nil {
		resp.ApplyId = apply.ApplyIdentifier
		opts := storage.ParseApplyOptions(apply.Options)
		if opts.Volume > 0 {
			resp.Volume = int32(opts.Volume)
		}
	}

	return resp, nil
}
