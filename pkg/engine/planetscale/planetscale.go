// Package planetscale implements the Engine interface for PlanetScale/Vitess databases
// using PlanetScale deploy requests.
//
// # How It Works
//
// Unlike Spirit (which runs schema changes inside the SchemaBot process), PlanetScale
// deploy requests run inside Vitess itself — SchemaBot only orchestrates them via API.
// This means:
//   - Schema changes survive SchemaBot crashes (they continue in Vitess)
//   - Stop permanently cancels the deploy request (no resume/checkpoint)
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
//	Stop     → Cancel the deploy request (permanent, maps to vtctldclient OnlineDDL cancel)
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
//	in_progress_cancel        → StateStopped              Cancelling deploy...
//	cancelled, complete_cancel→ StateStopped              Deployment cancelled
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
//     Requires a direct DSN to the Vitess database (mainBranchDSN in engine.Credentials).
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
//  2. For each keyspace: apply DDLs and optional VSchema update to the branch
//  3. Create a deploy request
//  4. Start the deploy request
//  5. Return — the tern layer polls Progress() to track to completion
//
// The deploy request runs inside Vitess. If SchemaBot crashes, the deploy continues.
// On restart, the tern layer's recovery worker calls Progress() and finds the deploy
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
// # VSchema Tasks
//
// VSchema updates and routing rule changes are tracked as Vitess-specific tasks
// in the vitess_tasks table (one row per keyspace with changes). This is needed
// because:
//   - A deploy request can be VSchema-only (zero DDLs). Without vitess_tasks,
//     the apply would have zero tasks and DeriveApplyState() would have nothing
//     to aggregate.
//   - The progress view surfaces when VSchema updates are happening during
//     a deploy (the in_progress_vschema deploy state).
//   - VSchema tasks don't fit the DDL task schema (no table_name, no DDL string,
//     no rows_copied, no engine_migration_id).
//
// VSchema task state follows the deploy request: when the deploy hits
// in_progress_vschema, VSchema tasks transition to running; when it passes,
// they complete. They don't appear in SHOW VITESS_MIGRATIONS.
//
// DeriveApplyState() aggregates both DDL tasks (from the tasks table) and
// VSchema tasks (from vitess_tasks) to compute the overall apply state.
//
// # Storage Tables
//
// vitess_apply_data — per-apply deploy metadata. One row per apply:
//   - apply_id:           links to the apply
//   - branch_name:        the branch created for this deploy
//   - deploy_request_id:  PlanetScale deploy request number
//   - migration_context:  groups all SHOW VITESS_MIGRATIONS rows for this deploy
//   - deploy_request_url: link to the deploy request in PlanetScale console
//
// vitess_tasks — per-keyspace non-DDL tasks. One row per keyspace with changes:
//   - apply_id:   links to the apply
//   - keyspace:   which keyspace this task covers
//   - task_type:  "vschema" or "routing_rules"
//   - state:      task state (pending, running, completed, failed)
//   - payload:    JSON (e.g. the new VSchema or routing rules)
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
	"fmt"

	"github.com/block/schemabot/pkg/engine"
)

// Engine implements engine.Engine for PlanetScale databases.
type Engine struct{}

// New creates a new PlanetScale engine.
func New() *Engine {
	return &Engine{}
}

// Name returns the engine identifier.
func (e *Engine) Name() string {
	return "planetscale"
}

// Plan computes the changes needed to reach the desired schema.
// Will diff schema files against the PlanetScale branch schema.
func (e *Engine) Plan(ctx context.Context, req *engine.PlanRequest) (*engine.PlanResult, error) {
	return nil, fmt.Errorf("planetscale engine not implemented")
}

// Apply creates a PlanetScale deploy request and starts it.
// The deploy request runs inside Vitess — it survives SchemaBot crashes.
// The tern client layer polls Progress() to track the deploy to completion.
func (e *Engine) Apply(ctx context.Context, req *engine.ApplyRequest) (*engine.ApplyResult, error) {
	return nil, fmt.Errorf("planetscale engine not implemented")
}

// Progress polls deploy request status from PlanetScale's API and optionally queries
// SHOW VITESS_MIGRATIONS for per-table, per-shard row counts and ETA.
// The mainBranchDSN in Credentials enables the detailed vitess_migrations queries.
func (e *Engine) Progress(ctx context.Context, req *engine.ProgressRequest) (*engine.ProgressResult, error) {
	return nil, fmt.Errorf("planetscale engine not implemented")
}

// Stop cancels the deploy request. This is permanent — cancelled deploys cannot be resumed.
// Maps to vtctldclient OnlineDDL cancel.
func (e *Engine) Stop(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return nil, fmt.Errorf("planetscale engine not implemented")
}

// Start is not supported for PlanetScale. Cancelled deploy requests cannot be restarted.
func (e *Engine) Start(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return nil, fmt.Errorf("start not supported for planetscale engine: cancelled deploy requests cannot be restarted")
}

// Cutover completes the deploy request, triggering the final schema swap.
// Maps to vtctldclient OnlineDDL complete.
func (e *Engine) Cutover(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return nil, fmt.Errorf("planetscale engine not implemented")
}

// Revert reverts a completed deploy request during the revert window.
func (e *Engine) Revert(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return nil, fmt.Errorf("planetscale engine not implemented")
}

// SkipRevert closes the revert window, making the schema change permanent.
func (e *Engine) SkipRevert(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return nil, fmt.Errorf("planetscale engine not implemented")
}

// Volume throttles or unthrottles the deploy request.
// Maps to vtctldclient OnlineDDL throttle/unthrottle.
func (e *Engine) Volume(ctx context.Context, req *engine.VolumeRequest) (*engine.VolumeResult, error) {
	return nil, fmt.Errorf("planetscale engine not implemented")
}

// Compile-time check that Engine implements engine.Engine.
var _ engine.Engine = (*Engine)(nil)
