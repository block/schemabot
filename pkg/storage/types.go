package storage

import (
	"encoding/json"
	"maps"
	"sort"
	"strconv"
	"time"

	"github.com/block/schemabot/pkg/schema"
)

// Lock represents a database-level deployment lock.
// Locks prevent concurrent schema changes to the same database across
// all environments and PRs. Lock key is database:type (no environment).
type Lock struct {
	// ID is the unique identifier (BIGINT AUTO_INCREMENT).
	ID int64

	// DatabaseName is the name of the database being locked.
	DatabaseName string

	// DatabaseType is the type of database: "vitess" or "mysql".
	DatabaseType string

	// Repository is the GitHub repository (owner/repo format).
	Repository string

	// PullRequest is the PR number that holds the lock (0 for CLI).
	PullRequest int

	// Owner identifies who holds the lock.
	// For PR-based: "repo#pr" (e.g., "block/myapp#123")
	// For CLI: "cli:user@hostname" or similar
	Owner string

	// CreatedAt is when the lock was acquired.
	CreatedAt time.Time

	// UpdatedAt is when the lock was last updated.
	UpdatedAt time.Time
}

// Check terminology:
//   - GitHub Check Run: the external GitHub Checks API object visible on a PR.
//   - Stored check state: a row in SchemaBot's checks table.
//
// Check represents stored check state. SchemaBot writes per-database stored
// state when planning, starting an apply, completing an apply, reconciling stale
// applies, or processing rollback completion. Those per-database rows do not
// create per-database GitHub Check Runs; they are internal inputs to aggregate
// calculation.
//
// SchemaBot stores this state because GitHub only owns the visible merge-gate
// object, not SchemaBot's per-database workflow state. Durable stored state lets
// SchemaBot recompute aggregates, survive restarts, reconcile stale applies,
// enforce ordering rules, clean up on PR close, and answer internal safety
// checks without treating the GitHub API as the source of truth.
//
// The aggregate check path reads the per-database stored state, creates or
// updates the visible GitHub Check Run, then writes an aggregate stored state
// row whose CheckRunID points at that GitHub object. Later GitHub check_run
// webhooks use CheckRunID to find the matching stored state.
type Check struct {
	// ID is the unique identifier (BIGINT AUTO_INCREMENT).
	ID int64

	// Repository is the GitHub repository (owner/repo format).
	Repository string

	// PullRequest is the PR number.
	PullRequest int

	// HeadSHA is the commit SHA the check is associated with.
	HeadSHA string

	// Environment is the target environment: "staging" or "production".
	Environment string

	// DatabaseType is the database type: "vitess" or "mysql".
	DatabaseType string

	// DatabaseName is the name of the database.
	DatabaseName string

	// CheckRunID is the GitHub Check Run ID used to update GitHub via the Checks API.
	CheckRunID int64

	// ApplyID is the storage apply ID this stored state currently represents.
	// It is set while apply state is in_progress so terminal updates cannot
	// overwrite a newer apply's stored check state.
	ApplyID int64

	// HasChanges indicates whether schema changes were detected.
	HasChanges bool

	// Status is SchemaBot's stored status: "pending_apply", "applying", "completed", etc.
	Status string

	// Conclusion is the GitHub Check Run conclusion set when the check completes.
	// Values: "success", "failure", "action_required", "neutral", "cancelled", "skipped".
	Conclusion string

	// BlockingReason is a machine-readable reason code explaining why stored
	// check state must keep the aggregate check from passing.
	// ErrorMessage remains human-readable display text.
	BlockingReason string

	// ErrorMessage contains a human-readable explanation when the check state fails.
	// Displayed in the GitHub Check Run details and PR comment.
	ErrorMessage string

	// CreatedAt is when the check was created.
	CreatedAt time.Time

	// UpdatedAt is when the check was last updated.
	UpdatedAt time.Time
}

// CheckStatus constants.
const (
	CheckStatusPending = "pending"
	CheckStatusSuccess = "success"
	CheckStatusFailure = "failure"
)

// Setting represents an admin-level SchemaBot setting.
// Examples: feature flags, default options, maintenance mode.
type Setting struct {
	// ID is the unique identifier (BIGINT AUTO_INCREMENT).
	ID int64

	// Key is the unique setting name.
	Key string

	// Value is the setting value. Use JSON for complex values.
	Value string

	// CreatedAt is when the setting was created.
	CreatedAt time.Time

	// UpdatedAt is when the setting was last updated.
	UpdatedAt time.Time
}

// DatabaseType constants.
const (
	DatabaseTypeVitess = "vitess"
	DatabaseTypeMySQL  = "mysql"
)

// Engine constants.
const (
	EngineSpirit      = "spirit"
	EnginePlanetScale = "planetscale"
)

// EngineForType returns the engine name for a database type.
func EngineForType(dbType string) string {
	if dbType == DatabaseTypeVitess {
		return EnginePlanetScale
	}
	return EngineSpirit
}

// TableChange represents a DDL change to a single table.
type TableChange struct {
	// Namespace is the schema name (MySQL) or keyspace (Vitess).
	Namespace string `json:"namespace,omitempty"`

	// Table is the table name.
	Table string `json:"table"`

	// DDL is the DDL statement to execute.
	DDL string `json:"ddl"`

	// Operation is "create", "alter", or "drop".
	Operation string `json:"operation"`
}

// NamespacePlanData contains plan data for a single namespace (database/keyspace).
type NamespacePlanData struct {
	Tables         []TableChange     `json:"tables,omitempty"`
	OriginalSchema map[string]string `json:"original_schema,omitempty"`
	VSchema        json.RawMessage   `json:"vschema,omitempty"`
}

// Plan represents a schema change plan generated by tern.Client.Plan().
// Plans are immutable after creation - they capture the schema state at plan time.
// SchemaBot stores plans so both GRPCClient and LocalClient can be stateless.
type Plan struct {
	// ID is the unique identifier (BIGINT AUTO_INCREMENT).
	ID int64

	// PlanIdentifier is the external identifier for API (e.g., "plan_abc123").
	PlanIdentifier string

	// Database is the target database name.
	Database string

	// DatabaseType is "vitess" or "mysql".
	DatabaseType string

	// Deployment is the Tern deployment selected by server config at plan time.
	Deployment string

	// Target is the Tern-facing target selected by server config at plan time.
	Target string

	// Repository is the GitHub repository (owner/repo format).
	Repository string

	// PullRequest is the PR number that generated this plan.
	PullRequest int

	// Environment is "staging" or "production".
	Environment string

	// SchemaFiles contains the input schema files organized by namespace.
	// Stored as JSON for audit trail and DDL re-generation if needed.
	SchemaFiles schema.SchemaFiles

	// Namespaces contains per-namespace plan data (DDL changes, original schema, VSchema).
	// The key is the database/schema name for MySQL, or keyspace name for Vitess.
	Namespaces map[string]*NamespacePlanData

	// CreatedAt is when the plan was generated.
	CreatedAt time.Time
}

// FlatDDLChanges returns all DDL changes across namespaces, sorted by namespace key.
func (p *Plan) FlatDDLChanges() []TableChange {
	if len(p.Namespaces) == 0 {
		return nil
	}
	keys := make([]string, 0, len(p.Namespaces))
	for k := range p.Namespaces {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var result []TableChange
	for _, k := range keys {
		for _, tc := range p.Namespaces[k].Tables {
			if tc.Namespace == "" {
				tc.Namespace = k
			}
			result = append(result, tc)
		}
	}
	return result
}

// FlatOriginalSchema returns original schema merged across all namespaces.
func (p *Plan) FlatOriginalSchema() map[string]string {
	if len(p.Namespaces) == 0 {
		return nil
	}
	result := make(map[string]string)
	for _, ns := range p.Namespaces {
		maps.Copy(result, ns.OriginalSchema)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// Apply represents a schema change execution from SchemaBot's perspective.
// Created when Apply() is called, updated during execution.
// Engine-specific state is stored in Tern (either via gRPC or LocalClient storage).
//
// Naming: "Apply" matches the CLI command (`schemabot apply`) and natural speech
// ("the apply is running"). Each Apply contains one or more Tasks (individual DDLs)
// in SchemaBot's storage.
type Apply struct {
	// ID is the unique identifier (BIGINT AUTO_INCREMENT).
	ID int64

	// ApplyIdentifier is the external identifier for API (e.g., "apply_abc123").
	// Used in API responses and CLI commands.
	ApplyIdentifier string

	// LockID points to locks.id.
	LockID int64

	// PlanID points to plans.id.
	PlanID int64

	// Database is the target database name (denormalized from lock for queries).
	Database string

	// DatabaseType is "vitess" or "mysql" (denormalized from lock for queries).
	DatabaseType string

	// Repository is the GitHub repository (denormalized from lock for GetByPR).
	Repository string

	// PullRequest is the PR number (denormalized from lock for GetByPR).
	PullRequest int

	// Environment is "staging" or "production".
	Environment string

	// Deployment is the Tern deployment name used for this apply.
	// Local mode stores the database name so recovery and controls can route
	// using the same plan-time decision as gRPC mode.
	Deployment string

	// Caller identifies who initiated this apply.
	// For CLI: "cli:user@hostname", for PR: "repo#pr"
	Caller string

	// InstallationID is the GitHub App installation ID (for webhook-triggered applies).
	InstallationID int64

	// ExternalID is the remote engine's identifier for this apply.
	// For gRPC mode: the remote Tern's apply_id (the remote engine's apply identifier).
	// Empty for local mode (SchemaBot IS the engine).
	ExternalID string

	// Engine is the schema change engine: "spirit", "planetscale", etc.
	Engine string

	// State is the current execution state.
	State string

	// ErrorMessage contains error details if state is failed.
	ErrorMessage string

	// Options contains engine-specific options as JSON.
	// Use ParseApplyOptions() to get typed access.
	Options []byte

	// Attempt tracks scheduler retry attempts for failed_retryable applies.
	// Once the retry budget is exhausted, the apply becomes failed.
	Attempt int

	// CreatedAt is when the apply was created.
	CreatedAt time.Time

	// StartedAt is when the apply started running.
	StartedAt *time.Time

	// CompletedAt is when the apply reached a terminal state.
	CompletedAt *time.Time

	// UpdatedAt is when the apply was last updated.
	UpdatedAt time.Time
}

// ApplyOptions contains engine-specific options for an apply.
// Stored as JSON in the database for flexibility across engine types.
type ApplyOptions struct {
	// AllowUnsafe permits destructive changes (DROP TABLE, DROP COLUMN).
	AllowUnsafe bool `json:"allow_unsafe,omitempty"`

	// Branch is the name of an existing PlanetScale branch to reuse.
	// When set, the engine refreshes the branch schema from main instead
	// of creating a new branch.
	Branch string `json:"branch,omitempty"`

	// DeferCutover pauses at cutover and waits for explicit trigger.
	DeferCutover bool `json:"defer_cutover,omitempty"`

	// DeferDeploy pauses before deploying the deploy request and waits for
	// explicit trigger (PlanetScale only). The user can review the deploy
	// request diff on PlanetScale before proceeding.
	DeferDeploy bool `json:"defer_deploy,omitempty"`

	// SkipRevert skips the revert window after completion (Vitess only).
	SkipRevert bool `json:"skip_revert,omitempty"`

	// Volume controls schema change aggressiveness (1-11).
	Volume int `json:"volume,omitempty"`

	// Target is the opaque endpoint-discovery target forwarded to Tern.
	// Defaults to the apply database when empty.
	Target string `json:"target,omitempty"`
}

// ApplyOptionsFromMap converts API/proto option strings into typed storage options.
func ApplyOptionsFromMap(options map[string]string) ApplyOptions {
	opts := ApplyOptions{
		AllowUnsafe:  options["allow_unsafe"] == "true",
		Branch:       options["branch"],
		DeferCutover: options["defer_cutover"] == "true",
		DeferDeploy:  options["defer_deploy"] == "true",
		SkipRevert:   options["skip_revert"] == "true",
		Target:       options["target"],
	}
	if rawVolume := options["volume"]; rawVolume != "" {
		volume, err := strconv.Atoi(rawVolume)
		if err == nil && volume >= 1 && volume <= 11 {
			opts.Volume = volume
		}
	}
	return opts
}

// Map converts typed storage options back into API/proto option strings.
func (opts ApplyOptions) Map() map[string]string {
	options := make(map[string]string)
	if opts.AllowUnsafe {
		options["allow_unsafe"] = "true"
	}
	if opts.Branch != "" {
		options["branch"] = opts.Branch
	}
	if opts.DeferCutover {
		options["defer_cutover"] = "true"
	}
	if opts.DeferDeploy {
		options["defer_deploy"] = "true"
	}
	if opts.SkipRevert {
		options["skip_revert"] = "true"
	}
	if opts.Volume > 0 {
		options["volume"] = strconv.Itoa(opts.Volume)
	}
	if opts.Target != "" {
		options["target"] = opts.Target
	}
	return options
}

// ParseApplyOptions parses the JSON options into ApplyOptions.
// Returns empty options if parsing fails or options is nil/empty.
func ParseApplyOptions(data []byte) ApplyOptions {
	if len(data) == 0 {
		return ApplyOptions{}
	}
	var opts ApplyOptions
	if err := json.Unmarshal(data, &opts); err != nil {
		return ApplyOptions{}
	}
	return opts
}

// MarshalApplyOptions serializes ApplyOptions to JSON.
func MarshalApplyOptions(opts ApplyOptions) []byte {
	data, err := json.Marshal(opts)
	if err != nil {
		return []byte("{}")
	}
	return data
}

// GetOptions returns parsed options for the Apply.
func (a *Apply) GetOptions() ApplyOptions {
	return ParseApplyOptions(a.Options)
}

// SetOptions sets the options on the Apply.
func (a *Apply) SetOptions(opts ApplyOptions) {
	a.Options = MarshalApplyOptions(opts)
}

// Task represents a schema change task (individual DDL within an apply).
// For multi-table changes, one apply contains multiple tasks.
type Task struct {
	// ID is the unique identifier (BIGINT AUTO_INCREMENT).
	ID int64

	// TaskIdentifier is the external identifier for API (e.g., "task_abc123").
	TaskIdentifier string

	// ApplyID points to applies.id.
	ApplyID int64

	// PlanID points to plans.id.
	PlanID int64

	// Database is the target database name.
	Database string

	// DatabaseType is "vitess" or "mysql".
	DatabaseType string

	// Engine is the schema change engine: "spirit", "planetscale", etc.
	Engine string

	// Repository is the GitHub repository (owner/repo format).
	// Denormalized from Apply for query convenience.
	Repository string

	// PullRequest is the PR number.
	// Denormalized from Apply for query convenience.
	PullRequest int

	// Environment is "staging" or "production".
	Environment string

	// State is the current execution state.
	State string

	// ErrorMessage contains error details if state is failed.
	ErrorMessage string

	// Options contains engine-specific options as JSON.
	Options []byte

	// Attempt tracks how many times this task has been retried by scheduler recovery.
	Attempt int

	// Namespace is the schema name (MySQL) or keyspace (Vitess) this table belongs to.
	Namespace string

	// TableName is the table being modified (empty for multi-table atomic).
	TableName string

	// DDL is the full DDL statement.
	DDL string

	// DDLAction is "alter", "create", or "drop".
	DDLAction string

	// Progress tracking (for copy-based schema changes)
	RowsCopied      int64 // Rows copied so far
	RowsTotal       int64 // Total rows to copy
	ProgressPercent int   // 0-100
	ETASeconds      int   // Estimated seconds remaining

	// Execution flags
	IsInstant         bool   // True if INSTANT DDL (no copy needed)
	ReadyToComplete   bool   // Row copy done, waiting for cutover
	EngineMigrationID string // Engine-specific migration ID

	// Timestamps
	CreatedAt   time.Time
	UpdatedAt   time.Time
	StartedAt   *time.Time
	CompletedAt *time.Time
}

// TaskFilter specifies criteria for listing tasks.
type TaskFilter struct {
	Repository       string    // Filter by repository (optional)
	PullRequest      int       // Filter by PR number, requires Repository (optional)
	IncludeCompleted bool      // Include terminal states (default: active only)
	Since            time.Time // Only tasks started after this time (optional)
}

// ApplyComment tracks a GitHub PR comment associated with an apply.
type ApplyComment struct {
	// ID is the unique identifier (BIGINT AUTO_INCREMENT).
	ID int64

	// ApplyID points to applies.id.
	ApplyID int64

	// CommentState is the comment lifecycle state: "progress", "cutover", "summary".
	CommentState string

	// GitHubCommentID is the GitHub comment ID for editing.
	GitHubCommentID int64

	// EditCount tracks how many times this comment was edited.
	EditCount int

	// LastEditedAt is when the comment was last edited via the GitHub API.
	LastEditedAt *time.Time

	// CreatedAt is when the comment was first posted.
	CreatedAt time.Time

	// UpdatedAt is when the comment was last edited.
	UpdatedAt time.Time
}

// ApplyLogLevel constants for log entry severity.
const (
	LogLevelDebug = "debug"
	LogLevelInfo  = "info"
	LogLevelWarn  = "warn"
	LogLevelError = "error"
)

// ApplyLogEventType constants for categorizing log entries.
const (
	LogEventStateTransition     = "state_transition"
	LogEventTaskUpdate          = "task_update"
	LogEventStopRequested       = "stop_requested"
	LogEventStartRequested      = "start_requested"
	LogEventDeployTriggered     = "deploy_triggered"
	LogEventCutoverTriggered    = "cutover_triggered"
	LogEventSkipRevertTriggered = "skip_revert_triggered"
	LogEventRevertTriggered     = "revert_triggered"
	LogEventError               = "error"
	LogEventInfo                = "info"
	LogEventProgress            = "progress"
)

// ApplyLogSource constants for identifying the origin of log entries.
const (
	LogSourceSchemaBot = "schemabot" // Logs from SchemaBot orchestration
	LogSourceSpirit    = "spirit"    // Logs from Spirit engine
)

// ApplyLog represents a single log entry for an apply.
// Logs capture state transitions, events, and debugging info.
type ApplyLog struct {
	// ID is the unique identifier (BIGINT AUTO_INCREMENT).
	ID int64

	// ApplyID points to applies.id.
	ApplyID int64

	// TaskID points to tasks.id (optional, for task-specific events).
	TaskID *int64

	// Level is the log level: "debug", "info", "warn", "error".
	Level string

	// EventType categorizes the log entry.
	// Examples: "state_transition", "task_update", "stop_requested".
	EventType string

	// Source identifies where the log came from: "schemabot" or "spirit".
	Source string

	// Message is the human-readable log message.
	Message string

	// OldState and NewState for state transitions (optional).
	OldState string
	NewState string

	// Metadata contains additional structured data as JSON.
	Metadata []byte

	// CreatedAt is when the log entry was created.
	CreatedAt time.Time
}

// ApplyLogFilter specifies criteria for listing apply logs.
type ApplyLogFilter struct {
	ApplyID   int64  // Required: filter by apply
	TaskID    *int64 // Optional: filter by task
	Level     string // Optional: filter by level
	EventType string // Optional: filter by event type
	Limit     int    // Optional: limit results (default 100)
}

// VitessApplyData holds Vitess-specific data for deploy request tracking.
// Stored in vitess_apply_data table, one row per apply when database_type = 'vitess'.
type VitessApplyData struct {
	ApplyID          int64
	BranchName       string
	DeployRequestID  uint64
	MigrationContext string
	DeployRequestURL string
	IsInstant        bool // True when PlanetScale reported the deploy as instant-eligible
	DeferredDeploy   bool // True when deploy was deferred (--defer-deploy flag)

	// RevertSkippedAt records when skip-revert was dispatched. Non-nil means
	// finalization is in progress — the deploy request is transitioning across
	// shards from complete_pending_revert to complete. On large keyspaces
	// this can take longer as shards are processed in batches.
	RevertSkippedAt *time.Time
}
