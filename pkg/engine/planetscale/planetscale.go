// Package planetscale implements the Engine interface for PlanetScale/Vitess databases
// using PlanetScale deploy requests.
//
// # How It Works
//
// Unlike Spirit (which runs schema changes inside the SchemaBot process), PlanetScale
// deploy requests run inside Vitess itself — SchemaBot only orchestrates them via API.
// This means:
//   - Schema changes survive SchemaBot crashes (they continue in Vitess)
//   - Cancel permanently cancels the deploy request (no resume/checkpoint)
//   - Stop is not supported because cancellation is not resumable
//   - Start is not supported — a cancelled deploy request cannot be restarted
//   - Progress polls the deploy request status from PlanetScale's API
//
// Apply creates a branch on demand, applies DDL and VSchema updates
// to it, then creates and starts a deploy request to merge the changes back.
//
// Deploy requests use Vitess online DDL under the hood:
//   - https://vitess.io/docs/23.0/user-guides/schema-changes/managed-online-schema-changes/
//   - https://vitess.io/docs/23.0/user-guides/schema-changes/
//
// # Engine Operation Mapping
//
// Each engine operation maps to a PlanetScale/Vitess concept:
//
//	Plan     → Diff schema files against PlanetScale main branch schema
//	Apply    → Create a deploy request and start it (tern client polls Progress to track to completion)
//	Progress → Poll deploy request status (GET /deploy-requests/{number}) and check shard progress at the vtgate
//	Stop     → Not supported (deploy request cancellation is permanent)
//	Cancel   → Cancel the deploy request (permanent, maps to vtctldclient OnlineDDL cancel)
//	Start    → Not supported (cancelled deploys cannot resume)
//	Cutover  → Complete the deploy request (maps to vtctldclient OnlineDDL complete)
//	Revert   → Revert the deploy request during the revert window
//	SkipRevert → Close the revert window, making changes permanent
//	Volume   → Throttle/unthrottle the deploy request (maps to vtctldclient OnlineDDL throttle/unthrottle)
//
// # Deploy Request States
//
// PlanetScale deploy requests have ~28 states. Key categories:
//
//	Pre-deploy:  pending, ready, no_changes
//	Active:      queued, submitting, in_progress, pending_cutover,
//	             in_progress_vschema, in_progress_cutover
//	Complete:    complete, complete_pending_revert
//	Error:       complete_error, error, failed
//	Cancelled:   in_progress_cancel, complete_cancel, cancelled
//	Revert:      in_progress_revert, in_progress_revert_vschema,
//	             complete_revert, complete_revert_error
//
// The engine maps these to engine.State values:
//
//	Deploy State              → engine.State              Message
//	─────────────────────────────────────────────────────────────────────
//	pending                   → StatePending              Validating schema changes...
//	ready                     → StatePending              Schema validation complete
//	no_changes                → StateCompleted            No changes detected
//	queued                    → StateRunning              Deploy queued
//	submitting                → StateRunning              Submitting deploy...
//	in_progress               → StateRunning              Deployment in progress
//	in_progress_vschema       → StateRunning              Applying VSchema changes
//	pending_cutover           → StateWaitingForCutover    Waiting for cutover
//	in_progress_cutover       → StateCuttingOver          Cutover in progress...
//	complete                  → StateCompleted            Deployment complete
//	complete_pending_revert   → StateRevertWindow         Deployment complete (revert available)
//	complete_error, error     → StateFailed               Deployment failed
//	failed                    → StateFailed               Deployment failed
//	in_progress_cancel        → StateCancelled            Cancelling deploy...
//	cancelled, complete_cancel→ StateCancelled            Deployment cancelled
//	in_progress_revert        → StateRunning              Revert in progress...
//	in_progress_revert_vschema→ StateRunning              Reverting VSchema changes
//	complete_revert           → StateReverted             Deployment reverted
//	complete_revert_error     → StateFailed               Revert failed
//
// Unknown states default to StateRunning to avoid blocking progress polling.
//
// See also: vitess.io/vitess/go/vt/schema.OnlineDDLStatus for the underlying
// Vitess migration statuses (queued, running, ready_to_complete, complete,
// failed, cancelled), which are distinct from PlanetScale deploy request states.
//
// # Progress Tracking
//
// Deploy request progress comes from two sources:
//
//  1. PlanetScale API: deploy request status, lint errors, instant DDL eligibility.
//     Coarser granularity — gives overall state but not per-table row counts.
//
//  2. Vitess migrations via SHOW VITESS_MIGRATIONS: per-table, per-shard row counts,
//     ETA, progress %, migration context, cutover attempts, throttle reasons.
//     Requires a direct DSN to the Vitess database (DSN in engine.Credentials).
//
// Progress is reported at two levels of granularity:
//
//   - Per-DDL (aggregated): rows_copied and table_rows summed across all shards
//     for a given migration_uuid. This is the task-level view — e.g. "orders:
//     33M/35M rows (94%)".
//   - Per-shard: individual shard progress within each DDL. This is the detail
//     view — e.g. "orders -80: 18M/18M (complete), orders 80-: 15M/17M (90%)".
//
// Both levels are surfaced in ProgressResult. The aggregated view drives task
// state and the progress bar. The per-shard view is available for debugging
// and for identifying lagging or failed shards.
//
// The migration_context groups all migrations from a single deploy request. On the
// first progress poll after Apply, the engine should discover the migration_context
// by comparing against a baseline captured before the deploy started, then filter
// subsequent SHOW VITESS_MIGRATIONS queries by that context.
//
// # Apply Workflow
//
// One apply = one deploy request. A deploy request contains one or more keyspace
// updates. Each keyspace update has one or more DDLs and an optional VSchema update.
//
// Schema files are organized by keyspace, with schemabot.yaml alongside:
//
//	schema/
//	├── schemabot.yaml
//	├── commerce/
//	│   ├── orders.sql
//	│   ├── items.sql
//	│   └── vschema.json
//	└── customers/
//	    ├── users.sql
//	    └── vschema.json
//
// Each .sql file contains a CREATE TABLE statement (declarative). The engine
// diffs these against the current branch schema to compute ALTER statements.
// Each vschema.json is a full Vitess VSchema definition (vindexes, table
// routing, sequences) applied declaratively to the branch.
//
// Apply performs these steps:
//  1. Create a branch from the main branch (on demand, no branch pool)
//  2. Get branch credentials via CreateBranchPassword
//  3. For each keyspace: apply DDLs via MySQL connection to the branch, plus
//     optional VSchema update via the PlanetScale API
//  4. Create a deploy request
//  5. Start the deploy request
//  6. Return — the tern layer polls Progress() to track to completion
//
// The deploy request runs inside Vitess. If SchemaBot crashes, the deploy continues.
// On restart, the tern layer's operator calls Progress() and finds the deploy
// still running — no special resume logic needed beyond polling.
//
// # Instant DDL
//
// PlanetScale auto-detects instant DDL eligibility. When eligible and neither
// enableRevert nor deferCutover is set, instant DDL is used automatically.
// Instant DDL completes immediately without a row copy phase.
//
// # VSchema
//
// Vitess uses VSchema to define sharding rules, vindexes, and table routing.
// VSchema updates are declarative (like DDL schema files) and are part of the apply.
// They are applied to the branch alongside DDL changes before creating the deploy
// request. The deploy request handles both DDL and VSchema updates together.
//
// # Task Architecture
//
// SchemaBot models each DDL statement as a separate task within an apply. For
// PlanetScale, one apply maps to one deploy request, and each DDL in the deploy
// request becomes one task. This is true even though Vitess executes each DDL
// independently on every shard — task granularity stays at the DDL level, not
// the shard level.
//
//	┌─────────────────────────────────────────────────────────────┐
//	│ Apply (apply_id=42)                                         │
//	│                                                             │
//	│  Deploy Request (dr_number=7, migration_context=ctx:abc123) │
//	│                                                             │
//	│  ┌────────────────────────┐  ┌────────────────────────┐     │
//	│  │ Keyspace: commerce     │  │ Keyspace: customers    │     │
//	│  │                        │  │                        │     │
//	│  │ ┌────────────────────┐ │  │ ┌────────────────────┐ │     │
//	│  │ │ Task 1             │ │  │ │ Task 3             │ │     │
//	│  │ │ ALTER TABLE orders │ │  │ │ ALTER TABLE users  │ │     │
//	│  │ │ migration_uuid: A  │ │  │ │ migration_uuid: C  │ │     │
//	│  │ │                    │ │  │ │                    │ │     │
//	│  │ │  -80: running      │ │  │ │  -80: queued       │ │     │
//	│  │ │  80-: running      │ │  │ │  80-: queued       │ │     │
//	│  │ └────────────────────┘ │  │ └────────────────────┘ │     │
//	│  │ ┌────────────────────┐ │  │                        │     │
//	│  │ │ Task 2             │ │  │ VSchema: vschema.json  │     │
//	│  │ │ ALTER TABLE items  │ │  └────────────────────────┘     │
//	│  │ │ migration_uuid: B  │ │                                 │
//	│  │ │                    │ │                                 │
//	│  │ │  -80: queued       │ │                                 │
//	│  │ │  80-: queued       │ │                                 │
//	│  │ └────────────────────┘ │                                 │
//	│  └────────────────────────┘                                 │
//	└─────────────────────────────────────────────────────────────┘
//
// Why one task per DDL (not per shard or per keyspace):
//   - Users think in terms of tables, not shards. "ALTER TABLE users" is one
//     logical operation regardless of how many shards execute it.
//   - Vitess itself orchestrates per-shard execution. Whether using PlanetScale
//     deploy requests or native vtctldclient, the control boundary for cancel,
//     throttle, and complete is the DDL (migration UUID), not individual shards.
//   - The proto already models shards as sub-detail: TableProgress contains a
//     repeated Shard field for per-shard row counts, ETA, and status.
//   - DeriveApplyState() stays simple — it aggregates task states, not shard states.
//
// Per-shard detail is surfaced for visibility (via SHOW VITESS_MIGRATIONS) but
// does not create separate tasks. A shard-level failure within a DDL is surfaced
// in the task's progress detail. Remediation of shard-level failures is deferred
// to PlanetScale support — that's the platform abstraction boundary.
//
// The migration_context groups all shard-level migrations belonging to the same
// deploy request. It is shared across all keyspaces and all shards within a
// single deploy request, and maps to a single apply_id. On the first progress
// poll after Apply, the engine discovers the migration_context by comparing
// against a baseline snapshot captured before the deploy started.
//
// Each task's engine_migration_id stores the Vitess migration UUID for that DDL.
// Progress() uses migration_context to query all shard migrations, then maps
// each migration back to its task via the migration UUID.
//
// # SHOW VITESS_MIGRATIONS
//
// Vitess exposes per-shard migration progress via SHOW VITESS_MIGRATIONS. Each
// row represents one DDL executing on one shard. A 3-shard table ALTER produces
// 3 rows, all sharing the same migration_uuid but with different shard values.
// Rows from the same deploy request also share the same migration_context.
//
// Full field reference (from SHOW VITESS_MIGRATIONS output):
//
// Identity and grouping:
//
//	migration_uuid       Unique ID for this DDL. Shared across all shards executing
//	                     the same statement. Maps to task.engine_migration_id.
//	migration_context    Groups all migrations from a single deploy request.
//	                     Format: "<system>:<uuid>" (e.g. "singularity:17694ee9-...").
//	                     Shared across all keyspaces and shards in one deploy.
//	                     Reverts use "revert:<original_context>".
//	                     Filter with: SHOW VITESS_MIGRATIONS LIKE '<context>'.
//	keyspace             The Vitess keyspace (e.g. "commerce", "customers").
//	shard                The shard this row tracks (e.g. "-80", "80-c0", "c0-").
//	mysql_table          The target table name.
//
// Statement and strategy:
//
//	migration_statement  The full DDL or revert command.
//	                     Regular: "alter table `t` add column ..."
//	                     Revert:  "revert vitess_migration '<uuid>'"
//	strategy             "vitess" for regular DDL, "online" for reverts.
//	ddl_action           "alter", "create", "drop". Reverts of a DROP show "create".
//	options              Vitess migration flags, space-separated. Key flags:
//	                       --postpone-completion    Defer cutover (maps to defer_cutover)
//	                       --prefer-instant-ddl     Try instant DDL first
//	                       --force-cut-over-after   Force cutover after delay
//	                       --in-order-completion    Complete migrations in submission order
//
// Status and progress:
//
//	migration_status     Per-shard status: queued, running, ready_to_complete,
//	                     complete, failed, cancelled.
//	progress             Vitess-computed progress percentage (0-100) for this shard.
//	rows_copied          Rows copied so far on this shard. 0 for instant/drop DDL.
//	table_rows           Estimated total rows on this shard (from information_schema).
//	eta_seconds          Estimated seconds remaining. 0 when complete, -1 when cancelled.
//	vreplication_lag_seconds  Replication lag during the copy phase.
//	stage                Current execution phase (e.g. "re-enabling writes",
//	                     "graceful wait for buffering"). Empty when queued or done.
//
// Timestamps:
//
//	added_timestamp      When the migration was submitted.
//	requested_timestamp  When execution was requested.
//	started_timestamp    When copy/execution began on this shard.
//	completed_timestamp  When this shard finished. NULL if in progress.
//	ready_to_complete_timestamp  When copy finished and migration became cuttable.
//	liveness_timestamp   Last heartbeat from the executing tablet.
//	reviewed_timestamp   When Vitess reviewed/accepted the migration.
//
// Instant DDL:
//
//	is_immediate_operation  1 if instant (no copy phase). True for DROP TABLE,
//	                        and ALTERs that MySQL can execute instantly.
//	special_plan            JSON describing the execution plan. For instant DDL:
//	                        {"operation":"instant-ddl"}. Empty for regular online DDL.
//
// Cutover and completion:
//
//	ready_to_complete    1 if copy is done and migration is awaiting cutover.
//	postpone_completion  1 when --postpone-completion is set (deferred cutover).
//	cutover_attempts     Number of cutover attempts on this shard.
//	last_cutover_attempt_timestamp  When the last cutover was attempted.
//	force_cutover        1 if cutover was force-triggered.
//	cutover_threshold_seconds  Max acceptable cutover lock time.
//
// Throttling:
//
//	user_throttle_ratio       User-set throttle ratio (0.0-1.0). Maps to Volume.
//	                          0.85 means 85% throttled.
//	last_throttled_timestamp  When last throttled on this shard.
//	component_throttled       Which component caused throttling (e.g. "vplayer").
//	reason_throttled          Human-readable throttle reason. Example:
//	                          "vplayer:<uuid>:vreplication:online-ddl is explicitly denied access"
//
// Revert:
//
//	reverted_uuid        For revert migrations, the UUID of the migration being
//	                     reverted. Empty for regular migrations.
//	cancelled_timestamp  When a cancel was issued. Reverts that are cancelled show
//	                     message "CANCEL ALL issued by user".
//
// Tablet:
//
//	tablet               The tablet running this shard's migration (e.g.
//	                     "zone1-0000000101").
//	tablet_failure       1 if the tablet failed during execution.
//
// Example: a deploy request with 2 DDLs across 2 shards. The ALTER on
// orders is a row-copy migration (18M rows), while the ALTER on items
// is instant DDL:
//
//	uuid      shard  table   status    rows_copied  table_rows  instant  special_plan
//	──────────────────────────────────────────────────────────────────────────────────────
//	528f9479  -80    orders  running   17790507     18150430    0
//	528f9479  80-    orders  running   15230102     16890221    0
//	8bbc0560  -80    items   complete  0            0           1        {"operation":"instant-ddl"}
//	8bbc0560  80-    items   complete  0            0           1        {"operation":"instant-ddl"}
//
// All 4 rows share the same migration_context. Progress() aggregates per-shard
// rows into per-task totals: task 1 (528f9479, orders) has 33020609/35040651
// rows copied (~94%), task 2 (8bbc0560, items) completed instantly with no
// row copy.
//
// # VSchema Application
//
// VSchema application is not modeled as a task. Its status and diff ride in the
// apply's engine resume metadata and are projected onto the progress response's
// display metadata (vschema_status / vschema_diff), which the CLI and PR comment
// render as their own VSchema section. The status follows the deploy request:
// it becomes "applying" when the deploy reaches in_progress_vschema and
// "applied" when it passes. A VSchema-only deploy (zero DDLs) carries no task
// rows and is driven to completion by a task-less group finalizer.
//
// # Storage
//
// Per-apply deploy metadata (branch name, deploy request number/URL, migration
// context) is opaque engine state: it rides in the apply operation's engine
// resume metadata, which the tern layer persists and replays without
// interpreting it — there is no engine-specific side table.
//
// DDL tasks use the regular tasks table. Per-task Vitess data is minimal:
// just the engine_migration_id (Vitess migration UUID) on the task record.
//
// # Native Vitess DDL
//
// If SchemaBot ever supports native Vitess DDL (via vtctldclient directly, without
// PlanetScale), the one-task-per-DDL architecture still holds. Vitess itself
// orchestrates per-shard execution for online DDL — vtctldclient OnlineDDL cancel,
// throttle, and complete all operate at the migration UUID level, not per-shard.
// The only difference is that SchemaBot would call vtctldclient directly instead
// of the PlanetScale API.
//
// # Key Resources
//
// PlanetScale API:
//   - Go client: https://github.com/planetscale/planetscale-go
//   - Deploy requests: https://planetscale.com/docs/vitess/schema-changes/deploy-requests
//   - API reference: https://planetscale.com/docs/api/reference/get_deploy_request
//
// Vitess online DDL (underlying mechanism):
//   - vtctldclient OnlineDDL: https://vitess.io/docs/23.0/reference/programs/vtctldclient/vtctldclient_onlineddl/
//   - Cancel: https://vitess.io/docs/23.0/reference/programs/vtctldclient/vtctldclient_onlineddl/vtctldclient_onlineddl_cancel/
//   - Throttle: https://vitess.io/docs/23.0/reference/programs/vtctldclient/vtctldclient_onlineddl/vtctldclient_onlineddl_throttle/
//   - Complete: https://vitess.io/docs/23.0/reference/programs/vtctldclient/vtctldclient_onlineddl/vtctldclient_onlineddl_complete/
package planetscale

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	ps "github.com/planetscale/planetscale-go/planetscale"

	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/lint"
	"github.com/block/schemabot/pkg/psclient"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
)

const (
	// maxConcurrentKeyspaces limits parallel DDL application during Apply.
	// Each keyspace gets its own MySQL connection to the branch.
	maxConcurrentKeyspaces = 3

	// maxRetries is the number of retry attempts per keyspace when applying DDL.
	maxRetries = 3

	// maxSnapshotRetries is used when a schema snapshot is in progress
	// (e.g., after RefreshSchema or VSchema updates). With exponential
	// backoff (20s, 40s, 60s, 60s) this gives ~3 minutes of total
	// wait time before failing.
	maxSnapshotRetries = 5

	// deployRequestPollInterval is how long to wait between polls while a deploy
	// request is pending PlanetScale's asynchronous schema diff computation.
	deployRequestPollInterval = 500 * time.Millisecond
)

// deployState is a shorthand alias for PlanetScale deploy request state constants.
var deployState = state.DeployRequest

// formatDeployRequestError builds a detailed error message for a failed deploy request,
// including any lint errors from PlanetScale's validation.
func formatDeployRequestError(dr *ps.DeployRequest) string {
	var b strings.Builder
	fmt.Fprintf(&b, "deploy request #%d failed during preparation (state: %s)", dr.Number, dr.DeploymentState)
	if dr.Deployment != nil && len(dr.Deployment.LintErrors) > 0 {
		for _, le := range dr.Deployment.LintErrors {
			fmt.Fprintf(&b, "\n  [%s] %s: %s (keyspace: %s, table: %s)",
				le.LintError, le.SubjectType, le.ErrorDescription, le.Keyspace, le.Table)
		}
	}
	return b.String()
}

// vschemaKeyspaceDiff is one keyspace's VSchema diff captured at deploy creation.
type vschemaKeyspaceDiff struct {
	Namespace string `json:"namespace"`
	Diff      string `json:"diff"`
}

// psMetadata holds PlanetScale-specific state stored as JSON in ResumeState.Metadata.
type psMetadata struct {
	BranchName       string     `json:"branch_name"`
	DeployRequestID  uint64     `json:"deploy_request_id"`
	DeployRequestURL string     `json:"deploy_request_url,omitempty"`
	DeployedAt       *time.Time `json:"deployed_at,omitempty"`
	IsInstant        bool       `json:"is_instant,omitempty"`
	DeferredDeploy   bool       `json:"deferred_deploy,omitempty"`

	// VSchemaStatus tracks the deploy's VSchema-application phase so it can be
	// surfaced from stored state without a synthetic task row. Empty when the
	// deploy carries no VSchema change; "applying" once the deploy reaches the
	// VSchema phase; "applied" once it completes after that phase. The renderer
	// reads it through the display-metadata projection (PSDisplayMetadata), the
	// same path branch/deploy-URL/instant use.
	VSchemaStatus string `json:"vschema_status,omitempty"`

	// VSchemaDiffs holds the per-keyspace VSchema diffs, captured at deploy
	// creation from the plan annotations and kept separate so each keyspace
	// renders and tracks independently. Surfaced through the display-metadata
	// projection alongside the deploy's VSchema status, without a synthetic task
	// row. Empty when the deploy carries no VSchema change.
	VSchemaDiffs []vschemaKeyspaceDiff `json:"vschema_diffs,omitempty"`

	// ExistingMigrationCtxs is the set of SHOW VITESS_MIGRATIONS contexts that
	// already existed just before this deploy started, keyed by context. It is
	// the durable baseline that lets a later process — a resume on another pod,
	// or an API progress poll — discover this deploy's own context by diffing
	// against it; the process that captured the baseline would otherwise be the
	// only one that knows it. Map membership drives the baseline diff; the stored
	// timestamp values are diagnostic only — the earliest-requested tie-break in
	// discovery reads requested_timestamp from the current rows, not from these.
	ExistingMigrationCtxs map[string]MigrationContextTimestamps `json:"existing_migration_contexts,omitempty"`
}

// MigrationContextTimestamps records the Vitess timestamp fields seen on a
// migration_context in SHOW VITESS_MIGRATIONS before this deploy started. The
// keys of the enclosing map provide baseline membership; these values are
// diagnostic for operators inspecting recovery decisions.
type MigrationContextTimestamps struct {
	RequestedTimestamp string `json:"requested_timestamp,omitempty"`
	StartedTimestamp   string `json:"started_timestamp,omitempty"`
	CompletedTimestamp string `json:"completed_timestamp,omitempty"`
}

// ResumeData is PlanetScale deploy metadata persisted by the tern layer and
// encoded into engine.ResumeState.Metadata for engine control and progress calls.
type ResumeData struct {
	BranchName            string
	DeployRequestID       uint64
	DeployRequestURL      string
	MigrationContext      string
	ExistingMigrationCtxs map[string]MigrationContextTimestamps
	DeployedAt            *time.Time
	IsInstant             bool
	DeferredDeploy        bool
}

// BuildResumeState encodes PlanetScale resume metadata into the opaque
// engine.ResumeState contract. Partial metadata is allowed because setup states
// can persist branch information before the deploy request exists.
func BuildResumeState(data ResumeData) (*engine.ResumeState, error) {
	metadata, err := encodePSMetadata(&psMetadata{
		BranchName:            data.BranchName,
		DeployRequestID:       data.DeployRequestID,
		DeployRequestURL:      data.DeployRequestURL,
		DeployedAt:            data.DeployedAt,
		IsInstant:             data.IsInstant,
		DeferredDeploy:        data.DeferredDeploy,
		ExistingMigrationCtxs: data.ExistingMigrationCtxs,
	})
	if err != nil {
		return nil, err
	}
	return &engine.ResumeState{
		MigrationContext: data.MigrationContext,
		Metadata:         metadata,
	}, nil
}

// BuildControlResumeState encodes PlanetScale resume metadata for control
// operations that act on an existing deploy request.
func BuildControlResumeState(data ResumeData) (*engine.ResumeState, error) {
	resumeState, err := BuildResumeState(data)
	if err != nil {
		return nil, err
	}
	if err := validateControlResumeState("", resumeState); err != nil {
		return nil, err
	}
	return resumeState, nil
}

func encodePSMetadata(m *psMetadata) (string, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("encode ps metadata: %w", err)
	}
	return string(data), nil
}

func decodePSMetadata(s string) (*psMetadata, error) {
	if s == "" {
		return nil, fmt.Errorf("empty metadata")
	}
	var m psMetadata
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil, fmt.Errorf("decode ps metadata: %w", err)
	}
	return &m, nil
}

// Engine implements engine.Engine for PlanetScale databases.
type Engine struct {
	clientFunc func(tokenName, tokenValue string) (psclient.PSClient, error)
	linter     *lint.Linter
	logger     *slog.Logger

	vtgateDBsMu sync.Mutex
	vtgateDBs   map[string]*sql.DB // dsn -> cached *sql.DB

}

// Compile-time check that Engine implements the interface.
var _ engine.Engine = (*Engine)(nil)
var _ engine.ExternallyAuthoritativeProgress = (*Engine)(nil)

// ProgressIsExternallyAuthoritative reports that PlanetScale progress is read
// from PlanetScale's deploy-request and VITESS_MIGRATIONS state rather than
// instance-local memory, so any instance returns the same correct result and
// the progress read path may query the engine directly regardless of which
// instance owns the schema change.
func (e *Engine) ProgressIsExternallyAuthoritative() bool {
	return true
}

// New creates a new PlanetScale engine with the given logger.
func New(logger *slog.Logger) *Engine {
	if logger == nil {
		logger = slog.Default()
	}
	return &Engine{
		clientFunc: func(tokenName, tokenValue string) (psclient.PSClient, error) {
			return psclient.NewPSClient(tokenName, tokenValue)
		},
		linter:    lint.New(),
		logger:    logger,
		vtgateDBs: make(map[string]*sql.DB),
	}
}

// NewWithClient creates a new PlanetScale engine with a custom client factory.
// Use this when the default PlanetScale SDK client needs to be replaced (e.g.,
// pointing at a different API base URL or using custom authentication).
func NewWithClient(logger *slog.Logger, clientFunc func(tokenName, tokenValue string) (psclient.PSClient, error)) *Engine {
	if logger == nil {
		logger = slog.Default()
	}
	return &Engine{
		clientFunc: clientFunc,
		linter:     lint.New(),
		logger:     logger,
		vtgateDBs:  make(map[string]*sql.DB),
	}
}

// Name returns the engine identifier.
func (e *Engine) Name() string {
	return "planetscale"
}

// getClient creates a PlanetScale client from the provided credentials.
func (e *Engine) getClient(creds *engine.Credentials) (psclient.PSClient, error) {
	if creds == nil {
		return nil, fmt.Errorf("credentials required")
	}
	if credTokenName(creds) == "" || credTokenValue(creds) == "" {
		return nil, fmt.Errorf("token credentials required")
	}
	return e.clientFunc(credTokenName(creds), credTokenValue(creds))
}

// getVtgateDB returns a cached *sql.DB for the given DSN, creating one if needed.
// If RegisterMTLS has been called, the mTLS config is applied automatically.
func (e *Engine) getVtgateDB(ctx context.Context, dsn string) (*sql.DB, error) {
	// Apply mTLS before cache lookup so the cache key matches the actual connection.
	if mtlsRegistered.Load() {
		mysqlCfg, err := mysql.ParseDSN(dsn)
		if err != nil {
			return nil, fmt.Errorf("parse vtgate DSN: %w", err)
		}
		mysqlCfg.TLSConfig = mtlsConfigName
		dsn = mysqlCfg.FormatDSN()
	}

	e.vtgateDBsMu.Lock()
	defer e.vtgateDBsMu.Unlock()
	if db, ok := e.vtgateDBs[dsn]; ok {
		return db, nil
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open vtgate: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		utils.CloseAndLog(db)
		return nil, fmt.Errorf("vtgate connection failed (check DSN and network access): %w", err)
	}
	e.vtgateDBs[dsn] = db
	return db, nil
}

func (e *Engine) getVtgateKeyspaceDB(ctx context.Context, dsn, keyspace string) (*sql.DB, error) {
	mysqlCfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse vtgate DSN for keyspace %s: %w", keyspace, err)
	}
	mysqlCfg.DBName = keyspace
	return e.getVtgateDB(ctx, mysqlCfg.FormatDSN())
}

// mainBranch returns the main branch name from credentials, defaulting to "main".
// Credential helpers — read PlanetScale-specific values from Metadata.

func credOrg(creds *engine.Credentials) string {
	if creds != nil {
		return creds.Metadata["organization"]
	}
	return ""
}

func credTokenName(creds *engine.Credentials) string {
	if creds != nil {
		return creds.Metadata["token_name"]
	}
	return ""
}

func credTokenValue(creds *engine.Credentials) string {
	if creds != nil {
		return creds.Metadata["token_value"]
	}
	return ""
}

func mainBranch(creds *engine.Credentials) string {
	if creds != nil && creds.Metadata["main_branch"] != "" {
		return creds.Metadata["main_branch"]
	}
	return "main"
}

// isRetryableEngineError returns true if the error is a transient condition
// that may succeed on a future attempt.
func isRetryableEngineError(err error) bool {
	return isRetryablePSError(err) || isRetryableMySQLConnectionError(err) || engine.IsTransientTransportError(err)
}

func isRetryableMySQLConnectionError(err error) bool {
	return errors.Is(err, mysql.ErrInvalidConn) || errors.Is(err, driver.ErrBadConn)
}

// isSnapshotInProgress returns true if the error indicates PlanetScale is
// running a schema snapshot (e.g., after RefreshSchema). VSchema updates
// are blocked while the snapshot completes.
func isSnapshotInProgress(err error) bool {
	return err != nil && strings.Contains(err.Error(), "schema snapshot is in progress")
}

// isRetryablePSError returns true if the error is a transient PlanetScale
// condition that may succeed on retry. Uses the SDK's typed error codes
// (e.g., ps.ErrRetry for 422, ps.ErrInternal for 500) and falls back to
// message matching for errors outside the SDK.
func isRetryablePSError(err error) bool {
	if err == nil {
		return false
	}
	var psErr *ps.Error
	if errors.As(err, &psErr) {
		switch psErr.Code {
		case ps.ErrRetry, ps.ErrInternal, ps.ErrResponseMalformed:
			return true
		}
	}
	return isSnapshotInProgress(err)
}

// retryDelay returns the backoff duration for a retry attempt using
// exponential backoff with full jitter. When a schema snapshot is in
// progress the base delay is longer since snapshots can take 30-60s.
func retryDelay(attempt int, lastErr error) time.Duration {
	if isSnapshotInProgress(lastErr) {
		// Snapshot: 10s, 20s, 40s, 60s, 60s + up to 5s jitter
		base := min(10*time.Second*(1<<min(attempt, 3)), 60*time.Second)
		return base + time.Duration(rand.IntN(5000))*time.Millisecond
	}
	// Normal: 2s, 4s, 8s capped at 10s + up to 2s jitter
	base := min(2*time.Second*(1<<min(attempt, 3)), 10*time.Second)
	return base + time.Duration(rand.IntN(2000))*time.Millisecond
}

func generateBranchName(database, planID string) string {
	sanitized := strings.ReplaceAll(database, "_", "-")
	if len(sanitized) > 20 {
		sanitized = sanitized[:20]
	}
	// Use last 8 chars of plan ID for uniqueness
	shortID := planID
	if len(shortID) > 8 {
		shortID = shortID[len(shortID)-8:]
	}
	return fmt.Sprintf("schemabot-%s-%s", sanitized, shortID)
}

func deployStateToEngineState(drState string) engine.State {
	switch drState {
	case deployState.Pending, deployState.Ready:
		return engine.StatePending
	case deployState.NoChanges, deployState.Complete:
		return engine.StateCompleted
	case deployState.CompletePendingRevert:
		return engine.StateRevertWindow
	case deployState.Queued, deployState.Submitting, deployState.InProgress, deployState.InProgressVSchema:
		return engine.StateRunning
	case deployState.PendingCutover:
		return engine.StateWaitingForCutover
	case deployState.InProgressCutover:
		return engine.StateCuttingOver
	case deployState.CompleteError, deployState.Error, deployState.Failed, deployState.CompleteRevertError:
		return engine.StateFailed
	case deployState.InProgressCancel:
		return engine.StateCancelled
	case deployState.CompleteCancel, deployState.Cancelled:
		return engine.StateCancelled
	case deployState.InProgressRevert, deployState.InProgressRevertVSchema:
		return engine.StateRunning
	case deployState.CompleteRevert:
		return engine.StateReverted
	default:
		return engine.StateRunning
	}
}

func deployStateToMessage(drState string) string {
	switch drState {
	case deployState.Pending:
		return "Validating schema changes..."
	case deployState.Ready:
		return "Schema validation complete"
	case deployState.NoChanges:
		return "No changes detected"
	case deployState.Queued:
		return "Deploy queued"
	case deployState.Submitting:
		return "Submitting deploy..."
	case deployState.InProgress:
		return "Deployment in progress"
	case deployState.InProgressVSchema:
		return engine.MessageApplyingVSchema
	case deployState.PendingCutover:
		return "Waiting for cutover"
	case deployState.InProgressCutover:
		return "Cutover in progress..."
	case deployState.Complete:
		return "Deployment complete"
	case deployState.CompletePendingRevert:
		return "Deployment complete (revert available)"
	case deployState.CompleteError, deployState.Error, deployState.Failed:
		return "Deployment failed"
	case deployState.InProgressCancel:
		return "Cancelling deploy..."
	case deployState.CompleteCancel, deployState.Cancelled:
		return "Deployment cancelled"
	case deployState.InProgressRevert:
		return "Revert in progress..."
	case deployState.InProgressRevertVSchema:
		return "Reverting VSchema changes"
	case deployState.CompleteRevert:
		return "Deployment reverted"
	case deployState.CompleteRevertError:
		return "Revert failed"
	default:
		return fmt.Sprintf("Processing (%s)", drState)
	}
}

// VSchema-application status values surfaced on the progress projection. Empty
// means the deploy carries no VSchema change (or hasn't reached the phase yet).
const (
	vschemaStatusApplying = "applying"
	vschemaStatusApplied  = "applied"
)

// nextVSchemaStatus advances the stored VSchema status from the current deploy
// state. It becomes "applying" when the deploy enters the VSchema phase, and
// "applied" once a deploy that went through that phase completes. A deploy that
// never reaches the VSchema phase (instant DDL, no VSchema change) keeps an
// empty status, so no VSchema indicator is surfaced for it.
func nextVSchemaStatus(current, drState string) string {
	switch drState {
	case deployState.InProgressVSchema:
		return vschemaStatusApplying
	case deployState.Complete, deployState.CompletePendingRevert:
		// A completed deploy has applied its VSchema. Mark it applied even when no
		// poll observed the in-progress VSchema phase (a fast deploy can pass
		// between polls), so a completed VSchema change never renders as still
		// "Pending". Harmless when the deploy carries no VSchema change — the
		// status is only surfaced for keyspaces that have a diff.
		return vschemaStatusApplied
	}
	return current
}

// sortedKeyspaces returns keyspace names from SchemaFiles in sorted order.
func sortedKeyspaces(sf schema.SchemaFiles) []string {
	keys := make([]string, 0, len(sf))
	for k := range sf {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
