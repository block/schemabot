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
	"log/slog"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/schema"
)

// Engine defines the interface that all schema change backends must implement.
//
// The Engine interface follows a state machine pattern:
//  1. Plan() - Compute what changes are needed
//  2. Apply() - Start executing the changes
//  3. Progress() - Check current status (poll this)
//  4. Control operations: Stop/Start/Cutover/Revert/SkipRevert
//
// Engines must support resume: if the server restarts part-way through a schema
// change, the engine must be able to resume from where it left off using stored
// state.
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

	// Cancel terminates a running schema change permanently.
	Cancel(ctx context.Context, req *ControlRequest) (*ControlResult, error)

	// Start resumes a stopped schema change.
	Start(ctx context.Context, req *ControlRequest) (*ControlResult, error)

	// Cutover triggers the final table swap.
	Cutover(ctx context.Context, req *ControlRequest) (*ControlResult, error)

	// Revert rolls back a completed schema change during the revert window.
	Revert(ctx context.Context, req *ControlRequest) (*ControlResult, error)

	// SkipRevert ends the revert window early, making changes permanent.
	SkipRevert(ctx context.Context, req *ControlRequest) (*ControlResult, error)
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

// ShutdownHalter is an optional capability for engines whose schema change work
// runs inside this process. Such an engine holds resources on the target — for
// Spirit, an advisory lock on the table it is copying — for exactly as long as
// its in-process work lives, and that work outlives the drive that started it.
// Without a way to bring it down, a shutting-down process stops renewing the
// apply's lease while still holding the target, and peer drivers reclaim work
// they cannot execute.
//
// An engine whose work runs elsewhere (a remote online-DDL service) must not
// implement this: its schema change is unaffected by this process going away,
// and the lease handover alone is the correct behavior.
type ShutdownHalter interface {
	// HaltForShutdown brings this instance's in-flight schema change down now,
	// checkpointed so another driver can resume it, and returns once the engine
	// no longer holds the target's resources. It is not an operator stop: it
	// records no operator intent and leaves the apply active for reclaim.
	// It returns an error if the work has not come down by the time ctx expires,
	// so a caller can report that the target may still be held.
	HaltForShutdown(ctx context.Context) error
}

// HaltEngineForShutdown brings eng's in-process schema change work down when it
// has any, and reports whether the engine implements the capability at all. An
// engine that does not is one whose work is unaffected by this process exiting,
// so there is nothing to halt and nothing to wait for.
func HaltEngineForShutdown(ctx context.Context, eng Engine) (supported bool, err error) {
	halter, ok := eng.(ShutdownHalter)
	if !ok {
		return false, nil
	}
	return true, halter.HaltForShutdown(ctx)
}

// DeferredCutoverSignalChecker is an optional capability for engines that can
// persist deferred-cutover intent in the target data plane. SchemaBot uses this
// during restart recovery to decide whether it must reattach to a deferred
// cutover wait before accepting cutover requests.
type DeferredCutoverSignalChecker interface {
	// DeferredCutoverSignalExists reports whether the engine's durable cutover
	// signal still exists for the target database.
	DeferredCutoverSignalExists(ctx context.Context, req *DeferredCutoverSignalRequest) (bool, error)
}

// ExternallyAuthoritativeProgress is an optional interface for engines whose
// Progress result reflects authoritative external state (for example a remote
// online-DDL service) rather than instance-local in-memory state.
//
// This distinction matters when one logical data-plane route is served by
// multiple instances that share storage. A progress request can be balanced
// onto any instance. An engine that reads progress from instance-local memory
// only knows about the schema change that instance is running, so a request
// served by another instance would observe unrelated or stale state — its
// progress must instead come from shared storage, which the instance driving
// the schema change keeps current. An engine that reads progress from external
// authoritative state returns the same correct answer on every instance, so it
// may be queried directly regardless of which instance answers.
//
// Engines that do not implement this interface are treated as instance-local:
// their progress is served from shared storage. This fails closed — a new
// engine is never trusted cross-instance unless it explicitly declares its
// progress authoritative.
type ExternallyAuthoritativeProgress interface {
	// ProgressIsExternallyAuthoritative reports whether this engine's Progress
	// result is correct regardless of which instance answers.
	ProgressIsExternallyAuthoritative() bool
}

// ProgressIsExternallyAuthoritative reports whether eng declares its Progress
// result authoritative regardless of which instance answers. Engines that do
// not implement ExternallyAuthoritativeProgress are treated as instance-local —
// this fails closed: a new engine's progress is never trusted as backend truth
// unless it explicitly declares it.
func ProgressIsExternallyAuthoritative(eng Engine) bool {
	auth, ok := eng.(ExternallyAuthoritativeProgress)
	return ok && auth.ProgressIsExternallyAuthoritative()
}

// SynchronousWorkRegistration is an optional interface for engines whose Apply
// registers accepted work before it returns, so the engine can never report
// pending for work it has accepted but has not begun executing.
//
// This distinction decides how a driver reads a pending progress report for a
// task whose durable state says the work is in flight. Pending is an overloaded
// report. An engine that provisions resources after accepting the work — cutting
// a branch, opening and validating a deploy request — reports pending for real,
// healthy work for as long as that setup takes, so a driver has to give it time
// before concluding anything from the report. An engine that registers the work
// synchronously has no such phase: once Apply has returned, the work is either
// running or it is gone, and a single pending report is already conclusive.
//
// Engines that do not implement this interface are treated as having a setup
// phase. That is the safe default for a healthy schema change: an undeclared
// engine is given the driver's full trust budget, so provisioning is never
// mistaken for lost work and a change that was about to run is never bounced.
type SynchronousWorkRegistration interface {
	// RegistersWorkSynchronously reports whether Apply registers accepted work
	// before returning, which makes a pending progress report conclusive
	// evidence that accepted work is gone rather than not yet started.
	RegistersWorkSynchronously() bool
}

// RegistersWorkSynchronously reports whether eng declares that Apply registers
// accepted work before returning. Engines that do not implement
// SynchronousWorkRegistration are treated as having a post-acceptance setup
// phase, so a pending report about in-flight work is never read as conclusive
// on its own.
func RegistersWorkSynchronously(eng Engine) bool {
	reg, ok := eng.(SynchronousWorkRegistration)
	return ok && reg.RegistersWorkSynchronously()
}

// DeferredCutoverSignalRequest identifies the target database whose deferred
// cutover signal should be inspected.
type DeferredCutoverSignalRequest struct {
	Database    string
	Credentials *Credentials
}

// CancelledArtifactReleaser is an optional capability for engines that leave
// their unfinished work on the target database as tables the engine itself
// owns. Such an engine copies rows into its own tables, and cancelling the
// schema change abandons them: nothing in the engine's own lifecycle reclaims
// them, and they can outlive the process, the apply, and the pull request that
// created them.
//
// An engine whose unfinished work lives in the service it drives (a remote
// online-DDL service) must not implement this. Cancelling there releases the
// work server-side and there is nothing locally to reclaim, so implementing the
// capability would claim a responsibility the engine does not have.
//
// The release is stateless by design. It reclaims work belonging to a schema
// change this process may never have run — one cancelled days later, on an
// instance that has restarted since — so it takes the target and the tables
// from the caller rather than from anything held in memory.
type CancelledArtifactReleaser interface {
	// ReleaseCancelledArtifacts reclaims what a cancelled schema change left on
	// the target and reports what it reclaimed. Data the schema change copied
	// is kept somewhere recoverable where the deployment offers one; the
	// metadata describing where the copy had got to is always discarded.
	//
	// Callers must establish that no live schema change is running anywhere in
	// the target schema before calling — not merely none on the tables the
	// request names. The engine's table names are derived from the target's own
	// table names, so an apply running against the same tables uses the same
	// names; and an engine's artifacts can include schema-scoped ones shared by
	// every schema change in the schema, one of which can be a cutover gate.
	// Reclaiming that gate on a cancelled change's behalf releases the cutover a
	// live change is still waiting on. The engine cannot see that change and
	// will not check for it.
	//
	// Tables must not be empty. Every schema change names at least one table, so
	// an empty list is a lost one, and the schema-scoped artifacts above would
	// be reclaimed regardless of it.
	ReleaseCancelledArtifacts(ctx context.Context, req *ReleaseArtifactsRequest) (*ReleaseArtifactsResult, error)
}

// ReleaseArtifactsRequest names the target and the tables whose cancelled
// schema change artifacts should be reclaimed. Tables are the target's own
// table names, not the engine's derived ones — deriving those is the engine's
// job, because only the engine knows how it names them.
type ReleaseArtifactsRequest struct {
	Database    string
	Tables      []string
	Credentials *Credentials
}

// ReleaseArtifactsResult reports what a release reclaimed, so a caller can tell
// an operator where their copy went. Both are empty when the schema change left
// nothing behind, which is the ordinary outcome for a cancel that arrives
// before any copying started.
//
// Every table in either field is named in full, as schema.table, so an operator
// reading one release can act on any line of it without having to supply the
// schema from context — including the lines naming tables that left the schema
// the release ran against.
type ReleaseArtifactsResult struct {
	// Preserved names each table whose data was kept, and where it was kept.
	Preserved []PreservedArtifact
	// Discarded names each table that was removed outright.
	Discarded []string
}

// PreservedArtifact records where a cancelled schema change's copied data was
// put, so an operator can find it while it is still recoverable.
type PreservedArtifact struct {
	Source      string
	Destination string
}

// ReleaseCancelledArtifacts reclaims eng's leftovers for the given target when
// eng owns any, and reports whether the engine implements the capability at
// all. An engine that does not is one whose unfinished work is not this
// process's to reclaim, so there is nothing to release and nothing to report.
func ReleaseCancelledArtifacts(ctx context.Context, eng Engine, req *ReleaseArtifactsRequest) (supported bool, result *ReleaseArtifactsResult, err error) {
	releaser, ok := eng.(CancelledArtifactReleaser)
	if !ok {
		return false, nil, nil
	}
	result, err = releaser.ReleaseCancelledArtifacts(ctx, req)
	return true, result, err
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

	// GroupedExecution reports whether an apply of this plan will hand the
	// engine every ALTER at once or one table at a time.
	//
	// It exists for predictions an engine makes about work already on the
	// target. Progress an unfinished change left behind is stored per batch, so
	// what a later apply can resume depends on the grouping it runs under, not
	// only on the statements. A prediction made for the wrong grouping looks for
	// progress under a key the apply will never use, and reports work as lost
	// that the apply would in fact continue.
	//
	// A caller that does not yet know the grouping leaves this false, which is
	// the ungrouped default every engine falls back to. Grouping is opted into,
	// so a plan made before that choice predicts the shape it would get today,
	// and the re-plan an apply runs predicts the shape it is actually about to
	// use.
	GroupedExecution bool
}

// PlanResult contains the computed schema change plan.
type PlanResult struct {
	PlanID    string         // Unique plan identifier
	Changes   []SchemaChange // Per-namespace changes (DDL + file diffs)
	NoChanges bool           // True if schema is already in desired state

	// Lint results from schema analysis. Violations with Severity "error" block
	// apply unless overridden with --allow-unsafe.
	LintViolations []LintViolation

	// ExistingCopies is unfinished work earlier schema changes left on the
	// target that applying this plan will continue or destroy, one entry per
	// namespace that holds any. Empty when the target holds none, which is the
	// ordinary case.
	ExistingCopies []*ExistingCopy
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

// SchemaChange is a namespace-level bundle. Engines emit at most one
// SchemaChange per namespace; OriginalFiles is captured once for that namespace
// and applies to every table/artifact change in the bundle.
type SchemaChange struct {
	Namespace string // MySQL schema, Vitess keyspace, Postgres schema
	// Shard identifies the shard this change targets within Namespace. A plan is
	// a slice of SchemaChange keyed by (Namespace, Shard): a sharded engine emits
	// one SchemaChange per changed shard, each carrying that shard's own
	// TableChanges, so shards that have drifted — e.g. one was offline during a
	// prior apply, or a change was canaried to a subset — can carry different
	// DDL. Non-sharded engines (Spirit) leave it zero, targeting the whole
	// namespace.
	Shard        Shard
	TableChanges []TableChange // Per-table DDL changes for this (Namespace, Shard)
	// Metadata contains engine-specific plan annotations, such as display diffs
	// or apply flags. It is not rollback input; rollback uses OriginalFiles.
	Metadata              map[string]string
	OriginalFiles         map[string]string // Declarative schema files and artifacts for this namespace before applying
	OriginalFilesCaptured bool              // True when OriginalFiles was captured, including an empty namespace
}

// ShardName returns the shard this change targets, trimmed of surrounding
// whitespace. Empty when the change targets the whole namespace.
func (sc SchemaChange) ShardName() string {
	return strings.TrimSpace(sc.Shard.Name)
}

// Sharded reports whether the change targets one shard of its namespace. A
// sharded engine plans one SchemaChange per changed shard, so the same table
// repeats across a keyspace's shards; a non-sharded change lists each
// statement once, and a table may legitimately appear more than once when its
// change is a multi-statement sequence.
//
// An engine that fans a change out to its shards behind one endpoint reports
// a whole-namespace change like a single-node engine does, so any repeated
// table in it is a statement sequence, never a per-shard repeat. Per-shard
// changes come from callers that scope a change to one shard — a shard-scoped
// dispatch, or a task that recorded the shard it ran on.
func (sc SchemaChange) Sharded() bool {
	return sc.ShardName() != ""
}

// Shard identifies a shard within a namespace for a sharded schema change. It is
// the zero value for non-sharded engines, where a SchemaChange targets the whole
// namespace.
type Shard struct {
	Name string // Shard name (e.g., "-80", "80-")
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
	Operation ddl.StatementType
	DDL       string // The DDL statement

	// Unsafe change tracking
	IsUnsafe     bool   // True if this is a destructive/unsafe change
	UnsafeReason string // Human-readable reason (e.g., "DROP COLUMN removes data")

	// Execution-mode verdict: how the engine will run this statement at apply
	// time. Empty means the engine's default path. "blocked" means the engine
	// deterministically refuses the statement — the apply will fail, and the
	// plan surfaces that up front. "direct" means the database's direct
	// execution policy routes the refused statement to native DDL on the
	// target instead. Distinct from IsUnsafe, which flags a change the engine
	// *can* run but the operator must acknowledge.
	ExecutionMode string
	ModeReason    string // Engine's reason for any non-empty ExecutionMode verdict
}

// Execution-mode verdicts recorded on a planned table change. The verdict
// answers "how will this statement actually run?" so operators learn about
// engine limitations at plan time instead of at apply time.
const (
	// ExecutionModeBlocked marks a statement the engine deterministically
	// refuses. An apply containing it will fail, and retrying cannot succeed
	// until whatever the reason names changes: the statement itself for an
	// unsupported shape, or the target's provisioning for a refusal such as
	// a missing grant.
	ExecutionModeBlocked = "blocked"

	// ExecutionModeDirect marks a statement the engine refuses but that the
	// database's direct execution policy routes to native DDL on the target
	// instead: it runs synchronously, it blocks writes to the table while it
	// runs, and it is not revertible.
	ExecutionModeDirect = "direct"
)

// CopyDisposition is what applying a plan will do with work an earlier schema
// change already did on the target and left behind — for engines that copy a
// table, the partly filled copy and the checkpoint describing it.
type CopyDisposition string

const (
	// CopyNone means the target holds no unfinished work for any table in the
	// plan. This is the ordinary case and is not surfaced.
	CopyNone CopyDisposition = "none"

	// CopyAdopt means applying continues the existing work rather than
	// repeating it. Nothing is destroyed, so it is disclosed and proceeds.
	CopyAdopt CopyDisposition = "adopt"

	// CopyDiscard means applying destroys the existing work and starts the
	// affected tables over. The cost is everything the earlier schema change
	// had done, which can be days of copying.
	CopyDiscard CopyDisposition = "discard"
)

// Discard reasons, kept distinct because they call for different operator
// advice: a plan that drifted from the copy's own batch can be restored, an
// expired checkpoint cannot, and a partial copy is not the operator's doing at
// all.
const (
	// DiscardStatementDiffers means the existing work was done for a different
	// set of statements than this plan will run, so the engine will not
	// continue it.
	DiscardStatementDiffers = "statement_differs"

	// DiscardCheckpointExpired means the statements match but the engine's
	// record of the existing work is too old to resume from.
	DiscardCheckpointExpired = "checkpoint_expired"

	// DiscardCopyIncomplete means the existing work covers only some of the
	// tables this plan changes. An engine that continues work continues all of
	// it or none, so the tables that did get copied are destroyed along with the
	// ones that never started.
	DiscardCopyIncomplete = "copy_incomplete"
)

// ExistingCopy is unfinished work an earlier schema change left on the target
// that applying this plan will either continue or destroy. It never carries
// CopyNone: a plan that destroys nothing reports no ExistingCopy at all.
type ExistingCopy struct {
	// Namespace names the target the work sits on, as the engine that read it
	// addresses that target. An engine planning each namespace separately
	// reports one ExistingCopy per namespace that holds any. Where several
	// namespaces share one target — schema subdirectories dividing a single
	// connection-scoped database only logically — the engine reads that target
	// once and this names the database it read, not the subdirectory the change
	// came from. Either way it names something an operator can go and look at,
	// which is what a disclosure has to do; it is not a key to group or route
	// on.
	Namespace string
	// Disposition is what applying will do with the existing work.
	Disposition CopyDisposition
	// Reason names why a discard cannot be avoided by applying as planned.
	// Empty for an adopt.
	Reason string
	// Tables are the tables in this plan that already hold unfinished work.
	Tables []string
	// Age is how long ago the engine last recorded progress on it. Zero when
	// the engine has no record to resume from, which is itself a discard.
	Age time.Duration
	// Statement is the schema change this work was started for, verbatim as
	// the engine recorded it. Empty when the engine has no record of it, which
	// is itself a reason the work cannot be resumed.
	//
	// It is what makes a statement-drift discard answerable rather than just
	// announced: a surface can say which change the work belongs to, so an
	// operator told "the schema change differs from the one that started it"
	// can see what it differs from and decide whether to restore it. For an
	// adopt it repeats the plan and says nothing new, so a surface renders it
	// only where the two disagree.
	Statement string
}

// Engine metadata keys carrying the direct execution policy from config
// surfaces (server config, embedder assemblers) to an engine via request
// credentials. Exported so producers and consumers share one spelling and
// cannot drift on the key strings.
const (
	// MetadataDirectExecution enables direct execution ("true") for ALTER
	// statements the engine deterministically refuses. Absent or "false"
	// leaves refused statements blocked.
	MetadataDirectExecution = "direct_execution"

	// MetadataDirectExecutionMaxTableRows bounds direct execution by the
	// target table's row count. Required (a positive integer) when direct
	// execution is enabled, so a native table rebuild can never run
	// unbounded: above the bound — or when the size cannot be determined —
	// the statement stays blocked.
	MetadataDirectExecutionMaxTableRows = "direct_execution_max_table_rows"

	// MetadataDirectExecutionLockAcquisitionTimeoutSeconds bounds, in whole
	// seconds, how long each direct statement waits to acquire its locks
	// before failing with a retryable busy-table error instead of queueing
	// on the table's lock indefinitely. Optional; engines apply their
	// default when the key is absent.
	MetadataDirectExecutionLockAcquisitionTimeoutSeconds = "direct_execution_lock_acquisition_timeout_seconds"
)

// ApplyRequest contains the input for starting a schema change.
// On first apply, set the resume context to group related DDL.
// On resume after restart, pass the full ResumeState from storage.
type ApplyRequest struct {
	PlanID       string             // Plan being executed (for tracing and apply→plan linkage)
	Database     string             // Target database
	Changes      []SchemaChange     // Per-namespace changes to apply (DDL + files from plan)
	TargetShards []string           // Optional shard selector for sharded engines
	SchemaFiles  schema.SchemaFiles // Full declarative schema files (for engines that apply whole files)
	Options      map[string]string  // Options like "defer_cutover", "skip_revert"
	ResumeState  *ResumeState       // Fresh context or full resume state after restart
	Credentials  *Credentials       // Resolved credentials (from discovery)

	// Logger is an optional logger scoped to this schema change, already bound with
	// the caller's triage identity (apply id, repo, PR, environment). Engines
	// use it for every log line about this schema change so engine lines stay
	// filterable by the same identity as the drive logs. Nil falls back to
	// the engine's configured logger.
	Logger *slog.Logger

	// OnStateChange is called by the engine to persist ResumeState at key milestones
	// during Apply (e.g., after branch creation, after deploy request creation).
	// This enables crash recovery: if the driver dies mid-Apply, the tern layer can
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
// Mirrors the OldState/NewState convention from storage.ApplyLog.
type ApplyEvent struct {
	Message  string            // Human-readable description (e.g., "Branch schemabot-mydb-123 created")
	Metadata map[string]string // Structured data (e.g., branch name, deploy request URL)

	// NewState is the apply state this event transitions to (e.g., state.Apply.ApplyingBranchChanges).
	// Empty means the event is informational and does not trigger a state transition.
	// Set by the engine; the tern layer derives OldState from the current apply record.
	NewState string
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
	Retryable    bool   // True when a failed progress result can be retried
	Tables       []TableProgress
	ResumeState  *ResumeState // Updated resume state (engines may update MigrationContext/Metadata during polling)

	// ResumedFromCheckpoint reports that this run reattached to a durable
	// checkpoint left by an earlier run rather than starting the copy from
	// scratch, so preserved progress can be told apart from a fresh restart.
	// False for engines without checkpoint resume.
	ResumedFromCheckpoint bool

	// Metadata carries engine-specific display fields for the progress response
	// (e.g. PlanetScale branch_name, deploy_request_url, is_instant). It lets the
	// engine surface structured status to the renderer without core decoding the
	// opaque ResumeState.Metadata or reading an engine-specific side table.
	Metadata map[string]string

	// PerShardProgressUnavailable is set by sharded engines when a progress poll
	// cannot produce per-shard/row-copy progress, carrying a machine-readable
	// reason (one of the PerShardUnavailable* constants). Empty means per-shard
	// progress was available (or the engine has no per-shard concept). The drive
	// surfaces this once per apply so an operator sees the degraded visibility in
	// Datadog without enabling debug logging.
	PerShardProgressUnavailable string
}

// Reasons a sharded engine could not report per-shard/row-copy progress for a
// progress poll, carried in ProgressResult.PerShardProgressUnavailable.
const (
	// PerShardUnavailableNoVtgateDSN means the target resolved without a vtgate
	// DSN, so SHOW VITESS_MIGRATIONS cannot be queried. This persists for the
	// whole apply — a target-resolution gap (missing vtgate endpoint).
	PerShardUnavailableNoVtgateDSN = "no_vtgate_dsn"
	// PerShardUnavailableNoChangeContext means no schema change context
	// identifier is known for the deploy yet, so per-shard rows cannot be
	// correlated to this apply. Transient during setup/recovery.
	PerShardUnavailableNoChangeContext = "no_change_context"
	// PerShardUnavailableNoShardRows means the per-shard query ran but returned
	// no rows for this schema change while the apply was active. Can occur
	// transiently at the start of the copy phase, before shard rows register.
	PerShardUnavailableNoShardRows = "no_shard_rows"
)

// TableProgress tracks progress for a single table.
type TableProgress struct {
	Namespace  string // Schema/keyspace name when the engine can distinguish it
	Table      string // Table name
	State      string // "pending", "copying", "ready", "complete", "failed"
	Progress   int    // 0-100 percent
	RowsCopied int64  // Actual rows copied
	RowsTotal  int64  // Total rows to copy
	ETASeconds int64  // Estimated seconds remaining
	// Checksum phase progress: rows verified so far and the total to verify.
	// Populated while the table is checksumming (verifying copied data), 0 otherwise.
	ChecksumRowsChecked int64
	ChecksumRowsTotal   int64
	// Throttled reports that the engine's throttler is currently pausing the
	// phase this table's work is in (the row copy or the checksum verify), so
	// stalled row counts read as a deliberate pause rather than a hang. False
	// in phases nothing paces, and cleared when the pause lifts.
	Throttled bool
	// ThrottleReason names the signal pausing the work, for display only
	// (e.g. "replica-lag 5s >= 2s"). Empty when Throttled is false or the
	// engine cannot explain the pause.
	ThrottleReason string
	Shards         []ShardProgress // Per-shard breakdown (for Vitess)
	IsInstant      bool            // True if using instant DDL
	ProgressDetail string          // Free-text note for a human reader; never parsed, never persisted
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
	StateWaitingForDeploy  State = "waiting_for_deploy"
	StateWaitingForCutover State = "waiting_for_cutover"
	StateCuttingOver       State = "cutting_over"
	StateRevertWindow      State = "revert_window"
	StateReverting         State = "reverting"
	StateCompleted         State = "completed"
	StateFailed            State = "failed"
	StateStopped           State = "stopped"
	StateCancelled         State = "cancelled"
	StateReverted          State = "reverted"
)

// MessageApplyingVSchema is the engine progress message emitted during the
// VSchema application phase of a deploy. Used to detect VSchema task transitions.
const MessageApplyingVSchema = "Applying VSchema changes"

// IsTerminal returns true if this is a final state.
func (s State) IsTerminal() bool {
	switch s {
	case StateCompleted, StateFailed, StateStopped, StateCancelled, StateReverted:
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

// ControlOperation names the control action that will consume a ResumeState.
// Engines can use this to validate opaque metadata before a caller attempts an
// operation that would otherwise fail later against the backing service.
type ControlOperation string

const (
	ControlStop       ControlOperation = "stop"
	ControlCancel     ControlOperation = "cancel"
	ControlStart      ControlOperation = "start"
	ControlCutover    ControlOperation = "cutover"
	ControlRevert     ControlOperation = "revert"
	ControlSkipRevert ControlOperation = "skip_revert"
)

// ControlResumeValidator is an optional interface for engines whose resume
// metadata has operation-specific readiness requirements. The tern layer owns
// loading persisted data; engines own the meaning of ResumeState.Metadata.
type ControlResumeValidator interface {
	ValidateControlResumeState(operation ControlOperation, state *ResumeState) error
}

// ControlResult is the response from control operations.
type ControlResult struct {
	Accepted    bool
	Message     string
	ResumeState *ResumeState
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
