// Package engine provides the interface for schema change backends.
//
// SchemaBot supports multiple backends (engines) for executing schema changes:
//   - Spirit: Uses gh-ost-style online DDL for MySQL (implemented)
//   - PlanetScale: Uses PlanetScale's branch/deploy request API (stub)
//   - Postgres: Uses pg-osc for PostgreSQL (stub)
//
// Each engine implements the same interface, allowing SchemaBot to support
// different database platforms with the same API.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/block/schemabot/pkg/schema"
)

// Engine defines the interface that all schema change backends must implement.
//
// The Engine interface follows a state machine pattern:
//  1. Plan() - Compute what changes are needed
//  2. Apply() - Start executing the changes
//  3. Progress() - Check current status (poll this)
//  4. Control operations: Stop/Start/Cutover/Revert/SkipRevert/Volume
//
// Engines must support resume: if the server restarts mid-schema-change, the engine
// must be able to resume from where it left off using stored state.
type Engine interface {
	// Name returns the engine identifier (e.g., "planetscale", "spirit").
	Name() string

	// Plan computes the changes needed to reach the desired schema.
	// Returns a PlanResult with DDL statements and metadata.
	Plan(ctx context.Context, req *PlanRequest) (*PlanResult, error)

	// Apply starts executing a schema change plan.
	// This is asynchronous - call Progress() to monitor status.
	Apply(ctx context.Context, req *ApplyRequest) (*ApplyResult, error)

	// Progress returns the current status of a schema change.
	// Poll this to track progress.
	Progress(ctx context.Context, req *ProgressRequest) (*ProgressResult, error)

	// Stop pauses a running schema change.
	Stop(ctx context.Context, req *ControlRequest) (*ControlResult, error)

	// Start resumes a stopped schema change.
	Start(ctx context.Context, req *ControlRequest) (*ControlResult, error)

	// Cutover triggers the final table swap.
	Cutover(ctx context.Context, req *ControlRequest) (*ControlResult, error)

	// Revert rolls back a completed schema change during the revert window.
	Revert(ctx context.Context, req *ControlRequest) (*ControlResult, error)

	// SkipRevert ends the revert window early, making changes permanent.
	SkipRevert(ctx context.Context, req *ControlRequest) (*ControlResult, error)

	// Volume adjusts the schema change speed (1=slowest, 11=fastest).
	Volume(ctx context.Context, req *VolumeRequest) (*VolumeResult, error)
}

// Drainer is an optional interface that engines can implement to allow callers
// to wait for any in-flight background work to complete before starting new
// operations. This is used during sequential resume to ensure the previous
// schema change's goroutine has fully exited (releasing DB connections) before
// checking whether the next table still needs changes.
type Drainer interface {
	// Drain waits for any in-flight background work to complete and clears it.
	Drain()
}

// Credentials contains the resolved credentials for accessing a database.
// These are populated by the service layer from discovery before calling the engine.
type Credentials struct {
	// DSN is the primary connection endpoint (vtgate MySQL DSN, direct MySQL DSN, etc.)
	DSN string

	// Metadata holds engine-specific key-value pairs.
	// PlanetScale: "organization", "token_name", "token_value", "main_branch"
	// Spirit: (none currently)
	Metadata map[string]string
}

// PlanRequest contains the input for computing a schema change plan.
type PlanRequest struct {
	Database     string             // Target database name
	DatabaseType string             // "vitess" or "mysql"
	SchemaFiles  schema.SchemaFiles // Namespace -> files (see schema.SchemaFiles)
	Repository   string             // GitHub repo for context (optional)
	PullRequest  int                // PR number for context (optional)
	Credentials  *Credentials       // Resolved credentials (from discovery)
}

// PlanResult contains the computed schema change plan.
type PlanResult struct {
	PlanID    string         // Unique plan identifier
	Changes   []SchemaChange // Per-namespace changes (DDL + file diffs)
	NoChanges bool           // True if schema is already in desired state

	// Lint results from schema analysis. Violations with Severity "error" block
	// apply unless overridden with --allow-unsafe.
	LintViolations []LintViolation

	// OriginalSchema contains the DB schema state at plan time (before applying).
	// Maps table name -> CREATE TABLE statement.
	// Used for rollback: applying OriginalSchema as target reverses the changes.
	OriginalSchema map[string]string
}

// HasErrors returns true if any lint warning has error severity.
// Error-severity violations block apply unless overridden with --allow-unsafe.
func (r *PlanResult) HasErrors() bool {
	for _, w := range r.LintViolations {
		if w.Severity == "error" {
			return true
		}
	}
	return false
}

// Errors returns only the error-severity lint violations (blocking violations).
func (r *PlanResult) Errors() []LintViolation {
	var errors []LintViolation
	for _, w := range r.LintViolations {
		if w.Severity == "error" {
			errors = append(errors, w)
		}
	}
	return errors
}

// Warnings returns only the non-error lint violations (warning + info severity).
func (r *PlanResult) Warnings() []LintViolation {
	var warnings []LintViolation
	for _, w := range r.LintViolations {
		if w.Severity != "error" {
			warnings = append(warnings, w)
		}
	}
	return warnings
}

// FlatDDL returns all DDL statements across all namespaces.
func (r *PlanResult) FlatDDL() []string {
	var ddl []string
	for _, sc := range r.Changes {
		for _, tc := range sc.TableChanges {
			ddl = append(ddl, tc.DDL)
		}
	}
	return ddl
}

// FlatTableChanges returns all table changes across all namespaces.
func (r *PlanResult) FlatTableChanges() []TableChange {
	var tables []TableChange
	for _, sc := range r.Changes {
		tables = append(tables, sc.TableChanges...)
	}
	return tables
}

// SchemaChange groups all changes for a single namespace (keyspace/schema).
type SchemaChange struct {
	Namespace    string            // MySQL schema, Vitess keyspace, Postgres schema
	TableChanges []TableChange     // Per-table DDL changes
	Metadata     map[string]string // Engine-specific data (e.g., "vschema" → diff string for Vitess)
}

// LintViolation represents a lint finding from schema analysis.
type LintViolation struct {
	Table    string // Table name affected
	Column   string // Column name if applicable
	Linter   string // Name of the linter (e.g., "unsafe", "primary_key")
	Message  string // Human-readable description
	Severity string // "warning" or "error"
}

// TableChange describes a change to a single table within a SchemaChange namespace.
type TableChange struct {
	Table     string // Table name
	Operation string // "create", "alter", "drop"
	DDL       string // The DDL statement

	// Unsafe change tracking
	IsUnsafe     bool   // True if this is a destructive/unsafe change
	UnsafeReason string // Human-readable reason (e.g., "DROP COLUMN removes data")
}

// ApplyRequest contains the input for starting a schema change.
// On first apply, set ResumeState.MigrationContext to group related DDL.
// On resume after restart, pass the full ResumeState from storage.
type ApplyRequest struct {
	PlanID      string             // Plan being executed (for tracing and apply→plan linkage)
	Database    string             // Target database
	Changes     []SchemaChange     // Per-namespace changes to apply (DDL + files from plan)
	SchemaFiles schema.SchemaFiles // Full declarative schema files (for engines that apply whole files)
	Options     map[string]string  // Options like "defer_cutover", "skip_revert"
	ResumeState *ResumeState       // Migration context (fresh) or full resume state (restart)
	Credentials *Credentials       // Resolved credentials (from discovery)

	// OnStateChange is called by the engine to persist ResumeState at key milestones
	// during Apply (e.g., after branch creation, after deploy request creation).
	// This enables crash recovery: if the worker dies mid-Apply, the tern layer can
	// resume from the last persisted state instead of starting over.
	// Nil means no persistence (state is only returned at the end of Apply).
	OnStateChange func(state *ResumeState)

	// OnEvent is called by the engine to emit structured lifecycle events during Apply.
	// These events are recorded in apply_logs so operators can see intermediate progress
	// (e.g., branch created, DDL applied, deploy request opened). Nil means no event
	// recording — the engine still logs via slog regardless.
	OnEvent func(event ApplyEvent)
}

// ApplyEvent represents a structured lifecycle event emitted by an engine during Apply.
type ApplyEvent struct {
	Message  string            // Human-readable description (e.g., "Branch schemabot-mydb-123 created")
	Metadata map[string]string // Structured data (e.g., branch name, deploy request URL)
}

// FlatDDL returns all DDL statements across all namespaces in the apply request.
func (r *ApplyRequest) FlatDDL() []string {
	var ddl []string
	for _, sc := range r.Changes {
		for _, tc := range sc.TableChanges {
			ddl = append(ddl, tc.DDL)
		}
	}
	return ddl
}

// ResumeState contains the state needed to resume a schema change after restart.
// This is stored in the task table and passed to Apply() on resume.
// Engine-specific data (branch names, deploy request IDs, etc.) goes in Metadata.
type ResumeState struct {
	MigrationContext string // Groups related DDL operations for progress tracking
	Metadata         string // Engine-specific state (opaque JSON, interpreted only by the engine)
}

// ApplyResult contains the result of starting a schema change.
type ApplyResult struct {
	Accepted    bool         // True if schema change was accepted
	Message     string       // Human-readable status message
	ResumeState *ResumeState // State for polling progress and resuming after restart
}

// ProgressRequest contains the input for checking schema change status.
type ProgressRequest struct {
	Database    string       // Target database (engines track by database)
	ResumeState *ResumeState // State for querying progress
	Credentials *Credentials // Resolved credentials (from discovery)
}

// ProgressResult contains the current schema change status.
type ProgressResult struct {
	State        State  // Current state
	Progress     int    // 0-100 percent complete
	Message      string // Human-readable status
	ErrorMessage string // Error details when State is StateFailed
	Tables       []TableProgress
	ResumeState  *ResumeState // Updated resume state (engines may update MigrationContext/Metadata during polling)
}

// TableProgress tracks progress for a single table.
type TableProgress struct {
	Namespace      string          // Schema/keyspace name when the engine can distinguish it
	Table          string          // Table name
	State          string          // "pending", "copying", "ready", "complete", "failed"
	Progress       int             // 0-100 percent
	RowsCopied     int64           // Actual rows copied
	RowsTotal      int64           // Total rows to copy
	ETASeconds     int64           // Estimated seconds remaining
	Shards         []ShardProgress // Per-shard breakdown (for Vitess)
	IsInstant      bool            // True if using instant DDL
	ProgressDetail string          // Human-readable progress (e.g., Spirit: "12.5% copyRows ETA 1h 30m")
	DDL            string          // The DDL statement being applied
	StartedAt      *time.Time      // When execution actually began (from engine, e.g., SHOW VITESS_MIGRATIONS started_timestamp)
	CompletedAt    *time.Time      // When execution completed (from engine)
}

// ShardProgress tracks progress for a single shard.
type ShardProgress struct {
	Shard           string // Shard name (e.g., "-80", "80-")
	State           string // Migration state
	Progress        int    // 0-100 percent
	RowsCopied      int64
	RowsTotal       int64
	ETASeconds      int64
	CutoverAttempts int // Number of cutover attempts (0 if never attempted)
}

// State represents the overall schema change state.
type State string

const (
	StatePending           State = "pending"
	StateRunning           State = "running"
	StateWaitingForCutover State = "waiting_for_cutover"
	StateCuttingOver       State = "cutting_over"
	StateRevertWindow      State = "revert_window"
	StateCompleted         State = "completed"
	StateFailed            State = "failed"
	StateStopped           State = "stopped"
	StateReverted          State = "reverted"
)

// IsTerminal returns true if this is a final state.
func (s State) IsTerminal() bool {
	switch s {
	case StateCompleted, StateFailed, StateStopped, StateReverted:
		return true
	}
	return false
}

// ControlRequest is used for Stop/Start/Cutover/Revert/SkipRevert operations.
type ControlRequest struct {
	Database    string       // Target database (engines track by database)
	ResumeState *ResumeState // State for querying progress
	Credentials *Credentials // Resolved credentials (from discovery)
}

// ControlResult is the response from control operations.
type ControlResult struct {
	Accepted    bool
	Message     string
	ResumeState *ResumeState
}

// VolumeRequest adjusts the schema change speed. Volume is a 1-11 scale where
// 1 = maximum throttle (least production impact) and 11 = no throttle (fastest).
//
// The same volume number has different effects per engine:
//   - Spirit: controls thread count (1-16+) and chunk timing. Higher volume =
//     more parallel copy threads = faster but more load. State is in-process
//     and lost on worker crash (restarts with defaults).
//   - PlanetScale/Vitess: controls a server-side rejection throttle ratio
//     (0.0-0.95). Online DDL runs on a single thread per shard; the throttle
//     ratio determines what fraction of write requests are rejected to limit
//     replication lag impact. State is server-side and survives worker crashes.
//
// The scale provides a consistent user interface across engines, but the
// underlying mechanisms are fundamentally different (concurrency control
// vs rejection-based throttling).
type VolumeRequest struct {
	Database    string       // Target database (engines track by database)
	Volume      int32        // 1 (max throttle) to 11 (no throttle)
	ResumeState *ResumeState // State for querying progress
	Credentials *Credentials // Resolved credentials (from discovery)
}

// VolumeResult is the response from volume adjustment.
type VolumeResult struct {
	Accepted       bool
	PreviousVolume int32
	NewVolume      int32
	Message        string
}

// EncodeResumeState serializes a ResumeState to JSON for storage in Task.EngineMigrationID.
// Returns empty string for nil input.
func EncodeResumeState(rs *ResumeState) (string, error) {
	if rs == nil {
		return "", nil
	}
	data, err := json.Marshal(rs)
	return string(data), err
}

// DecodeResumeState deserializes a ResumeState from Task.EngineMigrationID.
// Returns nil for empty strings and for Spirit's plain-string migration UUIDs
// (which aren't valid JSON or lack PlanetScale fields).
func DecodeResumeState(encoded string) *ResumeState {
	if encoded == "" {
		return nil
	}
	var rs ResumeState
	if err := json.Unmarshal([]byte(encoded), &rs); err != nil {
		return nil
	}
	if rs.MigrationContext == "" && rs.Metadata == "" {
		return nil
	}
	return &rs
}

// Config holds common configuration for engines.
type Config struct {
	// PlanetScale-specific
	PSTokenName    string // Service token name
	PSTokenValue   string // Service token value
	PSOrganization string // PlanetScale organization

	// Vitess-specific
	VTGateDSN string // DSN for vtgate (for vitess_migrations queries)

	// Timeouts
	BranchTimeout time.Duration // Timeout for branch creation/readiness
	DeployTimeout time.Duration // Timeout for deploy operations
}

// RetryableError wraps an error to indicate it is transient and the operation
// can be retried. Engines return this for known-recoverable failures like
// network timeouts, schema snapshots in progress, or binlog streaming errors.
type RetryableError struct {
	Err error
}

func (e *RetryableError) Error() string { return e.Err.Error() }
func (e *RetryableError) Unwrap() error { return e.Err }

// NewRetryableError wraps an error as retryable.
func NewRetryableError(err error) error {
	return &RetryableError{Err: err}
}

// IsRetryable returns true if the error (or any error in its chain) is a
// RetryableError. Used by the apply orchestration to decide between
// FailedRetryable and Failed states.
func IsRetryable(err error) bool {
	var re *RetryableError
	return errors.As(err, &re)
}
