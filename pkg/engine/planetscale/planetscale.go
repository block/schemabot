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
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	ps "github.com/planetscale/planetscale-go/planetscale"

	"github.com/block/spirit/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
	"golang.org/x/sync/errgroup"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/lint"
	"github.com/block/schemabot/pkg/psclient"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
)

const (
	// maxConcurrentKeyspaces limits parallel DDL application during Apply.
	// Each keyspace gets its own MySQL connection to the branch.
	maxConcurrentKeyspaces = 6

	// maxRetries is the number of retry attempts per keyspace when applying DDL.
	maxRetries = 3

	// maxSnapshotRetries is used when a schema snapshot is in progress
	// (e.g., after RefreshSchema). Snapshots can take 30-60s on large databases.
	maxSnapshotRetries = 5
)

// deployState is a shorthand alias for PlanetScale deploy request state constants.
var deployState = state.DeployRequest

// psMetadata holds PlanetScale-specific state stored as JSON in ResumeState.Metadata.
type psMetadata struct {
	BranchName       string     `json:"branch_name"`
	DeployRequestID  uint64     `json:"deploy_request_id"`
	DeployRequestURL string     `json:"deploy_request_url,omitempty"`
	DeployedAt       *time.Time `json:"deployed_at,omitempty"`
	IsInstant        bool       `json:"is_instant,omitempty"`
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

// --- Plan ---

// Plan computes the schema changes needed by diffing current schema against desired.
// For each keyspace in the schema files, it fetches the current schema and uses
// Spirit's PlanChanges to diff and lint in a single pass.
func (e *Engine) Plan(ctx context.Context, req *engine.PlanRequest) (*engine.PlanResult, error) {
	e.logger.Info("computing plan",
		"database", req.Database,
		"schema_files", len(req.SchemaFiles),
	)

	client, err := e.getClient(req.Credentials)
	if err != nil {
		return nil, fmt.Errorf("get planetscale client: %w", err)
	}

	org := credOrg(req.Credentials)
	branch := mainBranch(req.Credentials)

	// Sort keyspaces for deterministic order
	keyspaces := sortedKeyspaces(req.SchemaFiles)

	// Prefer the PlanetScale schema API when safe schema changes are enabled,
	// and use vtgate only when they are not.
	currentSchema, err := e.fetchPlanSchema(ctx, client, org, req.Database, branch, req.Credentials, keyspaces)
	if err != nil {
		return nil, fmt.Errorf("fetch current schema: %w", err)
	}

	// Diff and lint per keyspace in parallel using Spirit's PlanChanges.
	type keyspaceResult struct {
		change     engine.SchemaChange
		violations []engine.LintViolation
		schemas    map[string]string // keyspace.table -> CREATE TABLE
		hasChanges bool
	}

	var mu sync.Mutex
	results := make(map[string]*keyspaceResult, len(keyspaces))
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(20)

	for _, keyspace := range keyspaces {
		ks := keyspace
		g.Go(func() error {
			ns := req.SchemaFiles[ks]

			var currentTableSchemas []table.TableSchema
			schemas := make(map[string]string)
			if tables, ok := currentSchema[ks]; ok {
				for _, t := range tables {
					currentTableSchemas = append(currentTableSchemas, t)
					schemas[ks+"."+t.Name] = t.Schema
				}
			}

			desiredTableSchemas, parseErr := parseDesiredSchemas(ks, ns)
			if parseErr != nil {
				return parseErr
			}

			sc := engine.SchemaChange{
				Namespace: ks,
				Metadata:  make(map[string]string),
			}

			// Diff VSchema
			if content, ok := ns.Files["vschema.json"]; ok && content != "" {
				currentVSchema, fetchErr := client.GetKeyspaceVSchema(gCtx, &ps.GetKeyspaceVSchemaRequest{
					Organization: org,
					Database:     req.Database,
					Branch:       branch,
					Keyspace:     ks,
				})
				if fetchErr != nil {
					e.logger.Warn("failed to fetch current VSchema, treating as empty",
						"keyspace", ks, "error", fetchErr)
				}
				currentVSchemaRaw := ""
				if currentVSchema != nil {
					currentVSchemaRaw = currentVSchema.Raw
				}
				if VSchemaChanged(currentVSchemaRaw, content) {
					sc.Metadata["vschema"] = VSchemaDiff(currentVSchemaRaw, content)
				}
			}

			plan, planErr := lint.PlanChanges(currentTableSchemas, desiredTableSchemas, nil, e.linter.SpiritConfig())
			if planErr != nil {
				return fmt.Errorf("plan changes for keyspace %s: %w", ks, planErr)
			}

			var violations []engine.LintViolation
			for _, pc := range plan.Changes {
				op, _, classifyErr := ddl.ClassifyStatementAST(pc.Statement)
				if classifyErr != nil {
					return fmt.Errorf("classify statement in keyspace %s: %w", ks, classifyErr)
				}
				change := engine.TableChange{
					Table:     pc.TableName,
					Operation: op,
					DDL:       pc.Statement,
				}
				if errViolations := pc.Errors(); len(errViolations) > 0 {
					change.IsUnsafe = true
					msgs := make([]string, len(errViolations))
					for i, v := range errViolations {
						msgs[i] = v.Message
					}
					change.UnsafeReason = strings.Join(msgs, "; ")
				}
				sc.TableChanges = append(sc.TableChanges, change)

				for _, v := range pc.Violations {
					violations = append(violations, engine.LintViolation{
						Table:    pc.TableName,
						Linter:   v.Linter.Name(),
						Message:  v.Message,
						Severity: strings.ToLower(v.Severity.String()),
					})
				}
			}

			mu.Lock()
			results[ks] = &keyspaceResult{
				change:     sc,
				violations: violations,
				schemas:    schemas,
				hasChanges: len(sc.TableChanges) > 0 || sc.Metadata["vschema"] != "",
			}
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Collect results in deterministic keyspace order
	var changes []engine.SchemaChange
	var lintViolations []engine.LintViolation
	originalSchema := make(map[string]string)
	for _, ks := range keyspaces {
		r := results[ks]
		if r == nil {
			continue
		}
		maps.Copy(originalSchema, r.schemas)
		lintViolations = append(lintViolations, r.violations...)
		if r.hasChanges {
			changes = append(changes, r.change)
		}
	}

	if len(changes) == 0 {
		return &engine.PlanResult{
			PlanID:    fmt.Sprintf("plan-%d", time.Now().UnixNano()),
			NoChanges: true,
		}, nil
	}

	return &engine.PlanResult{
		PlanID:         fmt.Sprintf("plan-%d", time.Now().UnixNano()),
		Changes:        changes,
		LintViolations: lintViolations,
		OriginalSchema: originalSchema,
	}, nil
}

// --- Apply ---

// Apply starts executing a schema change plan.
// Creates a PlanetScale branch, applies DDL via MySQL connection to the branch,
// then creates and starts a deploy request.
func (e *Engine) Apply(ctx context.Context, req *engine.ApplyRequest) (*engine.ApplyResult, error) {
	e.logger.Info("applying plan",
		"plan_id", req.PlanID,
		"database", req.Database,
	)

	client, err := e.getClient(req.Credentials)
	if err != nil {
		return nil, fmt.Errorf("get planetscale client: %w", err)
	}

	org := credOrg(req.Credentials)
	main := mainBranch(req.Credentials)

	// Check if resuming
	if req.ResumeState != nil && req.ResumeState.Metadata != "" {
		return e.resumeApply(ctx, client, org, req)
	}

	// emitEvent logs a lifecycle event and sends it to the caller for apply_logs recording.
	emitEvent := func(message string, metadata map[string]string) {
		attrs := []any{"database", req.Database}
		for k, v := range metadata {
			attrs = append(attrs, k, v)
		}
		e.logger.Info(message, attrs...)
		if req.OnEvent != nil {
			req.OnEvent(engine.ApplyEvent{
				Message:  message,
				Metadata: metadata,
			})
		}
	}

	// Track in-flight apply metadata for progress queries during setup.
	migCtx := ""
	if req.ResumeState != nil {
		migCtx = req.ResumeState.MigrationContext
	}
	// persistState persists apply metadata to storage via OnStateChange for crash recovery.
	// On first apply, migCtx is empty until the tern layer assigns one via ResumeState.
	// persistState is a no-op in this window — if the worker crashes before Apply returns,
	// there's no ResumeState to recover from. The tern layer handles this by retrying
	// the full Apply on the next heartbeat recovery cycle.
	persistState := func(meta *psMetadata) {
		if migCtx == "" || req.OnStateChange == nil {
			return
		}
		encoded, err := encodePSMetadata(meta)
		if err != nil {
			e.logger.Warn("failed to encode apply metadata for persistence", "error", err)
			return
		}
		req.OnStateChange(&engine.ResumeState{
			MigrationContext: migCtx,
			Metadata:         encoded,
		})
	}

	// Create or reuse a branch
	existingBranch := req.Options["branch"]
	var branchName string
	branchStart := time.Now()

	if existingBranch != "" {
		// Reuse existing branch: wait for ready, refresh schema from main, wait again
		branchName = existingBranch
		if branchName == main {
			return nil, fmt.Errorf("cannot reuse the %s branch — use a development branch", main)
		}
		persistState(&psMetadata{BranchName: branchName})
		emitEvent(fmt.Sprintf("Reusing branch %s", branchName), map[string]string{"branch": branchName})

		// Verify branch exists
		if _, err := client.GetBranch(ctx, &ps.GetDatabaseBranchRequest{
			Organization: org, Database: req.Database, Branch: branchName,
		}); err != nil {
			return nil, fmt.Errorf("branch %s not found: %w", branchName, err)
		}

		// Wait for branch to be ready (may be initializing from a prior create)
		if err := e.waitForBranchReady(ctx, client, org, req.Database, branchName); err != nil {
			return nil, fmt.Errorf("wait for branch %s: %w", branchName, err)
		}

		// Sync with main to pick up latest schema
		emitEvent(fmt.Sprintf("Refreshing schema for branch %s from %s", branchName, main), map[string]string{"branch": branchName})
		if err := client.RefreshSchema(ctx, org, req.Database, branchName); err != nil {
			return nil, fmt.Errorf("refresh schema for branch %s: %w", branchName, err)
		}

		// Wait for sync to complete
		if err := e.waitForBranchReady(ctx, client, org, req.Database, branchName); err != nil {
			return nil, fmt.Errorf("wait for schema refresh %s: %w", branchName, err)
		}
		elapsed := time.Since(branchStart).Round(time.Second)
		emitEvent(fmt.Sprintf("Branch %s schema refreshed (%s)", branchName, elapsed), map[string]string{"branch": branchName})
	} else {
		// Create a new branch
		branchName = generateBranchName(req.Database, req.PlanID)
		persistState(&psMetadata{BranchName: branchName})
		emitEvent(fmt.Sprintf("Creating branch %s", branchName), map[string]string{"branch": branchName})

		_, err = e.createBranch(ctx, client, org, req.Database, branchName, main)
		if err != nil {
			return nil, fmt.Errorf("create branch: %w", err)
		}

		// Wait for branch to be ready
		if err := e.waitForBranchReady(ctx, client, org, req.Database, branchName); err != nil {
			return nil, fmt.Errorf("wait for branch: %w", err)
		}
		elapsed := time.Since(branchStart).Round(time.Second)
		emitEvent(fmt.Sprintf("Branch %s ready (%s)", branchName, elapsed), map[string]string{"branch": branchName})
	}

	// Get branch credentials to apply DDL via MySQL
	pwCtx, pwCancel := context.WithTimeout(ctx, 10*time.Second)
	defer pwCancel()

	password, err := client.CreateBranchPassword(pwCtx, &ps.DatabaseBranchPasswordRequest{
		Organization: org,
		Database:     req.Database,
		Branch:       branchName,
		Role:         "admin",
		TTL:          3600,
	})
	if err != nil {
		return nil, fmt.Errorf("create branch password: %w", err)
	}

	// Apply DDL and VSchema changes to all keyspaces
	emitEvent("Applying changes to branch", map[string]string{"branch": branchName})
	if err := e.applyChangesToBranch(ctx, req.Changes, req.SchemaFiles, password, client, org, req.Database, branchName, emitEvent); err != nil {
		return nil, fmt.Errorf("apply changes to branch: %w", err)
	}
	ddlCount := 0
	for _, sc := range req.Changes {
		ddlCount += len(sc.TableChanges)
	}
	emitEvent(fmt.Sprintf("Applied %d DDL changes to branch %s", ddlCount, branchName), map[string]string{"branch": branchName})

	// Capture existing migration_contexts before deploy so we can discover the new one
	existingContexts := e.captureExistingContexts(ctx, client, req.Database, req.Credentials)

	// Check if we should defer cutover
	deferCutover := req.Options["defer_cutover"] == "true"

	// Create deploy request and wait for it to be ready.
	// The server computes the schema diff asynchronously — poll until the deploy
	// request transitions from "pending" to "ready" (or "no_changes"/"error").
	drStart := time.Now()
	autoDeleteBranch := existingBranch == "" // don't delete reused branches
	dr, err := e.createDeployRequest(ctx, client, org, req.Database, branchName, main, !deferCutover, autoDeleteBranch)
	if err != nil {
		return nil, fmt.Errorf("create deploy request: %w", err)
	}
	emitEvent(fmt.Sprintf("Deploy request #%d created", dr.Number), map[string]string{
		"deploy_request_id":  fmt.Sprintf("%d", dr.Number),
		"deploy_request_url": dr.HtmlURL,
		"branch":             branchName,
	})
	persistState(&psMetadata{
		BranchName:       branchName,
		DeployRequestID:  dr.Number,
		DeployRequestURL: dr.HtmlURL,
	})
	for dr.DeploymentState == deployState.Pending {
		time.Sleep(500 * time.Millisecond)
		dr, err = e.getDeployRequest(ctx, client, org, req.Database, dr.Number)
		if err != nil {
			return nil, fmt.Errorf("poll deploy request %d: %w", dr.Number, err)
		}
	}
	if dr.DeploymentState == deployState.Error {
		return nil, fmt.Errorf("deploy request #%d failed during preparation (state: %s)", dr.Number, dr.DeploymentState)
	}
	if dr.DeploymentState == deployState.NoChanges {
		emitEvent(fmt.Sprintf("Deploy request #%d: no changes detected", dr.Number), map[string]string{
			"deploy_request_id": fmt.Sprintf("%d", dr.Number),
		})
		return &engine.ApplyResult{Message: "no changes detected"}, nil
	}

	// Determine instant DDL eligibility. Prefer instant when PlanetScale reports
	// it as eligible — instant DDL modifies metadata only (no row copy), so it
	// completes immediately and has no revert window regardless of skip_revert.
	instantEligible := dr.Deployment != nil && dr.Deployment.InstantDDLEligible
	useInstant := instantEligible && !deferCutover

	// Log the raw deploy request fields for debugging instant DDL detection.
	if dr.Deployment != nil {
		e.logger.Info("deploy request deployment info",
			"database", req.Database,
			"deploy_request", dr.Number,
			"instant_ddl_eligible", dr.Deployment.InstantDDLEligible,
			"deployment_state", dr.Deployment.State,
		)
	} else {
		e.logger.Warn("deploy request has nil deployment",
			"database", req.Database,
			"deploy_request", dr.Number,
			"deploy_state", dr.DeploymentState,
		)
	}
	e.logger.Info("instant DDL decision",
		"database", req.Database,
		"deploy_request", dr.Number,
		"has_deployment", dr.Deployment != nil,
		"instant_eligible", instantEligible,
		"use_instant", useInstant,
		"defer_cutover", deferCutover,
		"deploy_state", dr.DeploymentState,
	)

	drElapsed := time.Since(drStart).Round(time.Second)
	readyMsg := fmt.Sprintf("Deploy request #%d ready (%s)", dr.Number, drElapsed)
	if useInstant {
		readyMsg += " — instant DDL eligible"
	}
	emitEvent(readyMsg, map[string]string{
		"deploy_request_id":  fmt.Sprintf("%d", dr.Number),
		"deploy_request_url": dr.HtmlURL,
		"instant_ddl":        fmt.Sprintf("%t", useInstant),
	})

	// Deploy (starts the schema change)
	drNumber := dr.Number
	dr, err = client.DeployDeployRequest(ctx, &ps.PerformDeployRequest{
		Organization: org,
		Database:     req.Database,
		Number:       drNumber,
		InstantDDL:   useInstant,
	})
	if err != nil {
		if strings.Contains(err.Error(), "approved") {
			return nil, fmt.Errorf("deploy request #%d could not be deployed: PlanetScale deploy request approvals are not supported — disable 'Require administrator approval for deploy requests' in the PlanetScale database settings", drNumber)
		}
		return nil, fmt.Errorf("deploy deploy request: %w", err)
	}

	emitEvent(fmt.Sprintf("Deploy request #%d deployed", dr.Number), map[string]string{
		"deploy_request_id": fmt.Sprintf("%d", dr.Number),
		"instant_ddl":       fmt.Sprintf("%t", useInstant),
	})

	// Discover migration_context by diffing current SHOW VITESS_MIGRATIONS against
	// the pre-deploy baseline. Retries because Vitess may not have created migrations
	// immediately after the deploy request is submitted.
	var migrationContext string
	for attempt := range 10 {
		migrationContext = e.discoverMigrationContext(ctx, client, req.Database, req.Credentials, existingContexts)
		if migrationContext != "" {
			break
		}
		if attempt < 9 {
			time.Sleep(500 * time.Millisecond)
		}
	}

	meta, err := encodePSMetadata(&psMetadata{
		BranchName:       branchName,
		DeployRequestID:  dr.Number,
		DeployRequestURL: dr.HtmlURL,
		IsInstant:        useInstant,
	})
	if err != nil {
		return nil, fmt.Errorf("encode metadata for deploy request #%d: %w", dr.Number, err)
	}

	return &engine.ApplyResult{
		Accepted: true,
		Message:  fmt.Sprintf("Deploy request #%d created", dr.Number),
		ResumeState: &engine.ResumeState{
			MigrationContext: migrationContext,
			Metadata:         meta,
		},
	}, nil
}

// applyChangesToBranch applies VSchema and DDL changes to all keyspaces.
// VSchema updates are applied sequentially (PlanetScale rejects concurrent
// VSchema writes during schema snapshots). DDL is applied in parallel after
// all VSchema changes are committed.
func (e *Engine) applyChangesToBranch(ctx context.Context, changes []engine.SchemaChange, schemaFiles schema.SchemaFiles, password *ps.DatabaseBranchPassword, client psclient.PSClient, org, database, branchName string, emitEvent func(string, map[string]string)) error {
	if len(changes) == 0 {
		e.logger.Debug("no changes to apply to branch", "branch", branchName)
		return nil
	}

	total := len(changes)
	var applied atomic.Int32

	emitEvent(fmt.Sprintf("Applying changes to %d keyspaces on branch %s", total, branchName), map[string]string{"branch": branchName})

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrentKeyspaces)
	for _, sc := range changes {
		g.Go(func() error {
			if err := e.applyKeyspaceChanges(gCtx, sc, schemaFiles, password, client, org, database, branchName); err != nil {
				return err
			}
			n := int(applied.Add(1))
			emitEvent(fmt.Sprintf("Applied keyspace %s (%d/%d)", sc.Namespace, n, total),
				map[string]string{"keyspace": sc.Namespace})
			return nil
		})
	}
	return g.Wait()
}

// applyKeyspaceChanges applies VSchema and DDL for a single keyspace with retries.
// Uses longer backoff when PlanetScale reports a schema snapshot is in progress.
func (e *Engine) applyKeyspaceChanges(ctx context.Context, sc engine.SchemaChange, schemaFiles schema.SchemaFiles, password *ps.DatabaseBranchPassword, client psclient.PSClient, org, database, branchName string) error {
	start := time.Now()
	e.logger.Info("applying changes to keyspace",
		"keyspace", sc.Namespace,
		"ddl_count", len(sc.TableChanges),
		"has_vschema", sc.Metadata["vschema"] != "",
		"branch", branchName,
	)

	maxAttempts := maxRetries
	var lastErr error
	for attempt := range maxAttempts {
		if attempt > 0 {
			delay := retryDelay(attempt, lastErr)
			e.logger.Warn("retrying keyspace apply", "keyspace", sc.Namespace, "attempt", attempt+1, "delay", delay.Round(time.Millisecond), "error", lastErr)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		if err := e.applyKeyspaceChangesOnce(ctx, sc, schemaFiles, password, client, org, database, branchName); err != nil {
			lastErr = err
			e.logger.Error("keyspace apply attempt failed", "keyspace", sc.Namespace, "attempt", attempt+1, "error", err)
			if isSnapshotInProgress(err) && maxAttempts == maxRetries {
				maxAttempts = maxSnapshotRetries
				e.logger.Info("schema snapshot in progress, extending retries",
					"keyspace", sc.Namespace, "max_attempts", maxAttempts)
			}
			continue
		}
		e.logger.Info("keyspace changes applied", "keyspace", sc.Namespace, "elapsed", time.Since(start).Round(time.Second))
		return nil
	}
	return fmt.Errorf("apply keyspace %s (after %d attempts): %w", sc.Namespace, maxAttempts, lastErr)
}

// isSnapshotInProgress returns true if the error indicates PlanetScale is
// running a schema snapshot (e.g., after RefreshSchema). VSchema updates
// are blocked while the snapshot completes.
func isSnapshotInProgress(err error) bool {
	return err != nil && strings.Contains(err.Error(), "schema snapshot is in progress")
}

// retryDelay returns the backoff duration for a retry attempt. Uses longer
// delays when a schema snapshot is in progress since those can take 30-60s.
func retryDelay(attempt int, lastErr error) time.Duration {
	if isSnapshotInProgress(lastErr) {
		// Snapshot retries: 5s base + jitter
		return 5*time.Second + time.Duration(rand.IntN(2000))*time.Millisecond
	}
	// Normal retries: exponential backoff 2s, 4s, 6s + jitter
	base := time.Duration(attempt) * 2 * time.Second
	jitter := time.Duration(rand.IntN(1000)) * time.Millisecond
	return base + jitter
}

// applyKeyspaceChangesOnce applies VSchema and DDL for a single keyspace in one attempt.
func (e *Engine) applyKeyspaceChangesOnce(ctx context.Context, sc engine.SchemaChange, schemaFiles schema.SchemaFiles, password *ps.DatabaseBranchPassword, client psclient.PSClient, org, database, branchName string) error {
	// Apply VSchema first — vtgate needs VSchema to route DDL correctly
	if vschemaContent := getVSchemaContent(sc, schemaFiles); vschemaContent != "" {
		if err := e.updateBranchVSchema(ctx, client, org, database, branchName, sc.Namespace, vschemaContent); err != nil {
			return fmt.Errorf("update vschema for %s: %w", sc.Namespace, err)
		}
		e.logger.Info("applied vschema to branch", "keyspace", sc.Namespace, "branch", branchName)
	}

	if len(sc.TableChanges) == 0 {
		e.logger.Debug("no DDL for keyspace, vschema-only", "keyspace", sc.Namespace, "branch", branchName)
		return nil
	}

	// Build DSN targeting this specific keyspace.
	// TLS is configured automatically when RegisterMTLS has been called.
	mysqlCfg := mysql.NewConfig()
	mysqlCfg.User = password.Username
	mysqlCfg.Passwd = password.PlainText
	mysqlCfg.Net = "tcp"
	mysqlCfg.Addr = password.Hostname
	mysqlCfg.InterpolateParams = true
	if mtlsRegistered.Load() {
		mysqlCfg.TLSConfig = mtlsConfigName
	}
	dsn := mysqlCfg.FormatDSN()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("open branch connection for %s: %w", sc.Namespace, err)
	}
	defer utils.CloseAndLog(db)

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping branch for %s: %w", sc.Namespace, err)
	}

	// USE the keyspace — vtgate branch proxy routes based on the database name.
	if _, err := db.ExecContext(ctx, "USE `"+sc.Namespace+"`"); err != nil {
		return fmt.Errorf("use keyspace %s: %w", sc.Namespace, err)
	}

	for _, tc := range sc.TableChanges {
		e.logger.Info("applying DDL to branch",
			"keyspace", sc.Namespace,
			"table", tc.Table,
			"operation", tc.Operation,
			"ddl", tc.DDL,
		)
		if _, err := db.ExecContext(ctx, tc.DDL); err != nil {
			return fmt.Errorf("execute DDL on %s.%s: %w\nstatement: %s", sc.Namespace, tc.Table, err, tc.DDL)
		}
	}
	return nil
}

// getVSchemaContent extracts the vschema.json content for a keyspace from schema files.
// Returns empty string if no VSchema change is needed.
func getVSchemaContent(sc engine.SchemaChange, schemaFiles schema.SchemaFiles) string {
	if sc.Metadata["vschema"] == "" {
		return ""
	}
	if ns, ok := schemaFiles[sc.Namespace]; ok && ns != nil {
		if content, ok := ns.Files["vschema.json"]; ok {
			return content
		}
	}
	return ""
}

// updateBranchVSchema updates the VSchema for a keyspace on a branch
// using the PlanetScale SDK's UpdateKeyspaceVSchema endpoint.
func (e *Engine) updateBranchVSchema(ctx context.Context, client psclient.PSClient, org, database, branch, keyspace, vschemaJSON string) error {
	e.logger.Info("updating VSchema on branch",
		"branch", branch,
		"keyspace", keyspace,
	)
	_, err := client.UpdateKeyspaceVSchema(ctx, &ps.UpdateKeyspaceVSchemaRequest{
		Organization: org,
		Database:     database,
		Branch:       branch,
		Keyspace:     keyspace,
		VSchema:      vschemaJSON,
	})
	if err != nil {
		return fmt.Errorf("update vschema for keyspace %s on branch %s: %w", keyspace, branch, err)
	}
	return nil
}

// diffBranchForResume fetches the working branch's current schema and diffs it
// against the desired schema to find DDL that wasn't applied before the crash.
func (e *Engine) diffBranchForResume(ctx context.Context, client psclient.PSClient, org, database, branch string, schemaFiles schema.SchemaFiles) ([]engine.SchemaChange, error) {
	currentSchema, err := e.fetchDatabaseSchema(ctx, client, org, database, branch, sortedKeyspaces(schemaFiles))
	if err != nil {
		return nil, fmt.Errorf("fetch branch schema: %w", err)
	}

	var changes []engine.SchemaChange
	for _, keyspace := range sortedKeyspaces(schemaFiles) {
		ns := schemaFiles[keyspace]

		// Build current table schemas from branch
		var currentTableSchemas []table.TableSchema
		if tables, ok := currentSchema[keyspace]; ok {
			currentTableSchemas = append(currentTableSchemas, tables...)
		}

		// Build desired table schemas from files
		desiredTableSchemas, err := parseDesiredSchemas(keyspace, ns)
		if err != nil {
			return nil, err
		}

		// Diff: what DDL is needed to bring branch from current to desired?
		plan, err := lint.PlanChanges(currentTableSchemas, desiredTableSchemas, nil, e.linter.SpiritConfig())
		if err != nil {
			return nil, fmt.Errorf("diff keyspace %s for resume: %w", keyspace, err)
		}
		if !plan.HasChanges() {
			continue
		}

		sc := engine.SchemaChange{
			Namespace: keyspace,
			Metadata:  make(map[string]string),
		}
		for _, pc := range plan.Changes {
			op, _, classifyErr := ddl.ClassifyStatementAST(pc.Statement)
			if classifyErr != nil {
				return nil, fmt.Errorf("classify statement in keyspace %s: %w", keyspace, classifyErr)
			}
			sc.TableChanges = append(sc.TableChanges, engine.TableChange{
				Table:     pc.TableName,
				Operation: op,
				DDL:       pc.Statement,
			})
		}
		changes = append(changes, sc)
	}
	return changes, nil
}

// resumeApply resumes a schema change after restart.
// Handles two crash scenarios:
//   - Branch exists, no deploy request: diff branch against desired schema, apply remaining DDL, create deploy request
//   - Branch exists, deploy request exists: just return current state for Progress polling
func (e *Engine) resumeApply(ctx context.Context, client psclient.PSClient, org string, req *engine.ApplyRequest) (*engine.ApplyResult, error) {
	meta, err := decodePSMetadata(req.ResumeState.Metadata)
	if err != nil {
		return nil, fmt.Errorf("decode resume state: %w", err)
	}

	emitEvent := func(message string, metadata map[string]string) {
		attrs := []any{"database", req.Database}
		for k, v := range metadata {
			attrs = append(attrs, k, v)
		}
		e.logger.Info(message, attrs...)
		if req.OnEvent != nil {
			req.OnEvent(engine.ApplyEvent{Message: message, Metadata: metadata})
		}
	}

	e.logger.Info("resuming apply",
		"branch", meta.BranchName,
		"deploy_request", meta.DeployRequestID,
	)

	// If we have a deploy request ID, check its current state.
	if meta.DeployRequestID != 0 {
		dr, err := e.getDeployRequest(ctx, client, org, req.Database, meta.DeployRequestID)
		if err != nil {
			// Deploy request may have been cleaned up — start fresh.
			e.logger.Warn("deploy request not found on resume, starting fresh",
				"deploy_request", meta.DeployRequestID, "error", err)
			req.ResumeState = nil
			return e.Apply(ctx, req)
		}

		// If the deploy request failed, start fresh with a new branch rather
		// than resuming a broken deploy.
		if dr.DeploymentState == deployState.Error || dr.DeploymentState == deployState.CompleteError {
			e.logger.Warn("deploy request in error state on resume, starting fresh",
				"deploy_request", meta.DeployRequestID, "state", dr.DeploymentState)
			req.ResumeState = nil
			return e.Apply(ctx, req)
		}

		meta.DeployRequestURL = dr.HtmlURL
		updatedMeta, err := encodePSMetadata(meta)
		if err != nil {
			return nil, fmt.Errorf("encode metadata for deploy request #%d: %w", meta.DeployRequestID, err)
		}
		return &engine.ApplyResult{
			Accepted: true,
			Message:  fmt.Sprintf("Resumed deploy request #%d (state: %s)", dr.Number, dr.DeploymentState),
			ResumeState: &engine.ResumeState{
				MigrationContext: req.ResumeState.MigrationContext,
				Metadata:         updatedMeta,
			},
		}, nil
	}

	// No deploy request yet — worker crashed after branch creation but before
	// the deploy request was created. Diff the branch against desired schema
	// to find DDL that wasn't applied before the crash, then apply only the
	// missing changes.
	e.logger.Info("resuming from branch (no deploy request yet)", "branch", meta.BranchName)

	// Check if the branch still exists — it may have been deleted by TTL
	// between the crash and recovery. If so, start fresh.
	if err := e.waitForBranchReady(ctx, client, org, req.Database, meta.BranchName); err != nil {
		e.logger.Warn("branch no longer available on resume, starting fresh", "branch", meta.BranchName, "error", err)
		req.ResumeState = nil
		return e.Apply(ctx, req)
	}

	// Diff branch's current state against desired to find un-applied DDL
	remainingChanges, err := e.diffBranchForResume(ctx, client, org, req.Database, meta.BranchName, req.SchemaFiles)
	if err != nil {
		return nil, fmt.Errorf("diff branch for resume: %w", err)
	}

	if len(remainingChanges) > 0 {
		e.logger.Info("applying remaining DDL on resume", "branch", meta.BranchName, "keyspaces", len(remainingChanges))
		resumePwCtx, resumePwCancel := context.WithTimeout(ctx, 10*time.Second)
		defer resumePwCancel()

		password, err := client.CreateBranchPassword(resumePwCtx, &ps.DatabaseBranchPasswordRequest{
			Organization: org, Database: req.Database, Branch: meta.BranchName, Role: "admin", TTL: 3600,
		})
		if err != nil {
			return nil, fmt.Errorf("create branch password on resume: %w", err)
		}
		if err := e.applyChangesToBranch(ctx, remainingChanges, req.SchemaFiles, password, client, org, req.Database, meta.BranchName, emitEvent); err != nil {
			return nil, fmt.Errorf("apply remaining DDL on resume: %w", err)
		}
	} else {
		e.logger.Info("all DDL already applied on branch", "branch", meta.BranchName)
	}

	// VSchema may not have been applied before the crash — re-apply
	// (VSchema updates are idempotent, they overwrite the entire VSchema)
	for _, sc := range req.Changes {
		if vschemaContent := getVSchemaContent(sc, req.SchemaFiles); vschemaContent != "" {
			if err := e.updateBranchVSchema(ctx, client, org, req.Database, meta.BranchName, sc.Namespace, vschemaContent); err != nil {
				return nil, fmt.Errorf("update vschema for %s on resume: %w", sc.Namespace, err)
			}
		}
	}

	// Create deploy request
	main := mainBranch(req.Credentials)
	deferCutover := req.Options["defer_cutover"] == "true"

	dr, err := e.createDeployRequest(ctx, client, org, req.Database, meta.BranchName, main, !deferCutover, true)
	if err != nil {
		return nil, fmt.Errorf("create deploy request on resume: %w", err)
	}
	for dr.DeploymentState == deployState.Pending {
		time.Sleep(500 * time.Millisecond)
		dr, err = e.getDeployRequest(ctx, client, org, req.Database, dr.Number)
		if err != nil {
			return nil, fmt.Errorf("poll deploy request %d on resume: %w", dr.Number, err)
		}
	}
	if dr.DeploymentState == deployState.Error {
		return nil, fmt.Errorf("deploy request #%d failed on resume (state: %s)", dr.Number, dr.DeploymentState)
	}
	if dr.DeploymentState == deployState.NoChanges {
		return &engine.ApplyResult{Message: "no changes detected on resume"}, nil
	}

	// Deploy — prefer instant when eligible (no row copy, no revert window needed).
	instantEligible := dr.Deployment != nil && dr.Deployment.InstantDDLEligible
	useInstant := instantEligible && !deferCutover

	meta.DeployRequestID = dr.Number
	meta.DeployRequestURL = dr.HtmlURL
	meta.IsInstant = useInstant
	persistMeta, err := encodePSMetadata(meta)
	if err != nil {
		return nil, fmt.Errorf("encode metadata on resume: %w", err)
	}
	if req.OnStateChange != nil {
		req.OnStateChange(&engine.ResumeState{
			MigrationContext: req.ResumeState.MigrationContext,
			Metadata:         persistMeta,
		})
	}

	dr, err = client.DeployDeployRequest(ctx, &ps.PerformDeployRequest{
		Organization: org, Database: req.Database, Number: dr.Number, InstantDDL: useInstant,
	})
	if err != nil {
		return nil, fmt.Errorf("deploy on resume: %w", err)
	}

	e.logger.Info("resumed and deployed", "number", dr.Number, "branch", meta.BranchName)
	return &engine.ApplyResult{
		Accepted: true,
		Message:  fmt.Sprintf("Resumed and deployed request #%d", dr.Number),
		ResumeState: &engine.ResumeState{
			MigrationContext: req.ResumeState.MigrationContext,
			Metadata:         persistMeta,
		},
	}, nil
}

// --- Progress ---

// Progress polls deploy request status from PlanetScale's API and optionally queries
// SHOW VITESS_MIGRATIONS for per-table, per-shard row counts and ETA.
func (e *Engine) Progress(ctx context.Context, req *engine.ProgressRequest) (*engine.ProgressResult, error) {
	if req.ResumeState == nil || req.ResumeState.Metadata == "" {
		return &engine.ProgressResult{
			State:   engine.StatePending,
			Message: "No active schema change",
		}, nil
	}

	meta, err := decodePSMetadata(req.ResumeState.Metadata)
	if err != nil {
		return nil, fmt.Errorf("decode resume state: %w", err)
	}

	if meta.DeployRequestID == 0 {
		return &engine.ProgressResult{
			State:   engine.StatePending,
			Message: fmt.Sprintf("Setting up branch %s", meta.BranchName),
		}, nil
	}

	client, err := e.getClient(req.Credentials)
	if err != nil {
		return nil, fmt.Errorf("get planetscale client: %w", err)
	}

	dr, err := e.getDeployRequest(ctx, client, credOrg(req.Credentials), req.Database, meta.DeployRequestID)
	if err != nil {
		return nil, fmt.Errorf("get deploy request: %w", err)
	}

	engineState, progress := deployStateToEngineState(dr.DeploymentState)

	// Update metadata with DeployedAt when available (used by tern layer for
	// revert window timeout calculation).
	if dr.DeployedAt != nil && meta.DeployedAt == nil {
		meta.DeployedAt = dr.DeployedAt
		if encoded, encErr := encodePSMetadata(meta); encErr == nil {
			req.ResumeState = &engine.ResumeState{
				MigrationContext: req.ResumeState.MigrationContext,
				Metadata:         encoded,
			}
		}
	}

	e.logger.Info("progress poll",
		"database", req.Database,
		"deploy_request", meta.DeployRequestID,
		"deploy_state", dr.DeploymentState,
		"engine_state", engineState,
		"is_instant", meta.IsInstant,
		"has_migration_context", req.ResumeState != nil && req.ResumeState.MigrationContext != "",
		"has_vtgate_dsn", req.Credentials.DSN != "",
	)

	result := &engine.ProgressResult{
		State:       engineState,
		Progress:    progress,
		Message:     deployStateToMessage(dr.DeploymentState),
		ResumeState: req.ResumeState,
	}

	// Enrich with per-shard progress from SHOW VITESS_MIGRATIONS.
	// Requires a vtgate DSN (Credentials.DSN) and a migration context
	// (from VitessApplyData) to query per-shard state.
	hasMigrationContext := req.Credentials.DSN != "" &&
		req.ResumeState != nil && req.ResumeState.MigrationContext != ""
	if hasMigrationContext {
		tables, overallProgress := e.queryVitessMigrations(ctx, client, req.Database, req.Credentials, req.ResumeState.MigrationContext)
		e.logger.Info("vitess migrations queried",
			"database", req.Database,
			"table_count", len(tables),
			"overall_progress", overallProgress,
		)
		if len(tables) > 0 {
			result.Tables = tables
			if overallProgress > 0 {
				result.Progress = overallProgress
			}
		}
	} else {
		e.logger.Debug("skipping per-shard progress",
			"database", req.Database,
			"has_vtgate_dsn", req.Credentials.DSN != "",
			"has_migration_context", req.ResumeState != nil && req.ResumeState.MigrationContext != "",
		)
	}

	// Propagate instant DDL flag to all tables. Instant DDL may complete
	// before migration context discovery, so we use the flag from deploy
	// metadata as the authoritative source.
	if meta.IsInstant {
		e.logger.Info("marking tables as instant DDL",
			"database", req.Database,
			"table_count", len(result.Tables),
		)
		for i := range result.Tables {
			result.Tables[i].IsInstant = true
		}
	}

	return result, nil
}

// --- Control operations ---

// Stop cancels the deploy request. This is permanent.
func (e *Engine) Stop(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	meta, err := e.controlMeta(req)
	if err != nil {
		return nil, fmt.Errorf("decode control metadata: %w", err)
	}

	client, err := e.getClient(req.Credentials)
	if err != nil {
		return nil, fmt.Errorf("get planetscale client: %w", err)
	}

	_, err = client.CancelDeployRequest(ctx, &ps.CancelDeployRequestRequest{
		Organization: credOrg(req.Credentials),
		Database:     req.Database,
		Number:       meta.DeployRequestID,
	})
	if err != nil {
		return nil, fmt.Errorf("cancel deploy request #%d (may have been deleted): %w", meta.DeployRequestID, err)
	}

	return &engine.ControlResult{
		Accepted:    true,
		Message:     "Deploy request cancelled",
		ResumeState: req.ResumeState,
	}, nil
}

// Start is not supported for PlanetScale. Cancelled deploy requests cannot be restarted.
func (e *Engine) Start(_ context.Context, _ *engine.ControlRequest) (*engine.ControlResult, error) {
	return nil, fmt.Errorf("start not supported for planetscale engine: cancelled deploy requests cannot be restarted")
}

// Cutover completes the deploy request, triggering the final schema swap.
func (e *Engine) Cutover(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	meta, err := e.controlMeta(req)
	if err != nil {
		return nil, fmt.Errorf("decode control metadata: %w", err)
	}

	client, err := e.getClient(req.Credentials)
	if err != nil {
		return nil, fmt.Errorf("get planetscale client: %w", err)
	}

	dr, err := client.ApplyDeployRequest(ctx, &ps.ApplyDeployRequestRequest{
		Organization: credOrg(req.Credentials),
		Database:     req.Database,
		Number:       meta.DeployRequestID,
	})
	if err != nil {
		return nil, fmt.Errorf("cutover deploy request #%d (may have been deleted): %w", meta.DeployRequestID, err)
	}

	return &engine.ControlResult{
		Accepted:    true,
		Message:     fmt.Sprintf("Cutover initiated for deploy request #%d", dr.Number),
		ResumeState: req.ResumeState,
	}, nil
}

// Revert rolls back a completed schema change during the revert window.
func (e *Engine) Revert(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	meta, err := e.controlMeta(req)
	if err != nil {
		return nil, fmt.Errorf("decode control metadata: %w", err)
	}

	client, err := e.getClient(req.Credentials)
	if err != nil {
		return nil, fmt.Errorf("get planetscale client: %w", err)
	}

	dr, err := client.RevertDeployRequest(ctx, &ps.RevertDeployRequestRequest{
		Organization: credOrg(req.Credentials),
		Database:     req.Database,
		Number:       meta.DeployRequestID,
	})
	if err != nil {
		return nil, fmt.Errorf("revert deploy request #%d (may have been deleted): %w", meta.DeployRequestID, err)
	}

	return &engine.ControlResult{
		Accepted:    true,
		Message:     fmt.Sprintf("Revert initiated for deploy request #%d", dr.Number),
		ResumeState: req.ResumeState,
	}, nil
}

// SkipRevert closes the revert window, making the schema change permanent.
func (e *Engine) SkipRevert(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	meta, err := e.controlMeta(req)
	if err != nil {
		return nil, fmt.Errorf("decode control metadata: %w", err)
	}

	client, err := e.getClient(req.Credentials)
	if err != nil {
		return nil, fmt.Errorf("get planetscale client: %w", err)
	}

	dr, err := client.SkipRevertDeployRequest(ctx, &ps.SkipRevertDeployRequestRequest{
		Organization: credOrg(req.Credentials),
		Database:     req.Database,
		Number:       meta.DeployRequestID,
	})
	if err != nil {
		return nil, fmt.Errorf("skip revert for deploy request #%d (may have been deleted): %w", meta.DeployRequestID, err)
	}

	return &engine.ControlResult{
		Accepted:    true,
		Message:     fmt.Sprintf("Revert window skipped for deploy request #%d", dr.Number),
		ResumeState: req.ResumeState,
	}, nil
}

// controlMeta extracts and validates psMetadata from a control request.
func (e *Engine) controlMeta(req *engine.ControlRequest) (*psMetadata, error) {
	if req.ResumeState == nil || req.ResumeState.Metadata == "" {
		return nil, fmt.Errorf("no active schema change")
	}
	meta, err := decodePSMetadata(req.ResumeState.Metadata)
	if err != nil {
		return nil, fmt.Errorf("decode resume state: %w", err)
	}
	if meta.DeployRequestID == 0 {
		return nil, fmt.Errorf("no active schema change")
	}
	return meta, nil
}

// --- Volume ---

// Volume adjusts schema change speed by setting the Vitess throttle ratio.
// Volume 1 = fully throttled (ratio 1.0), Volume 11 = full speed (ratio 0.0).
// NOTE: Volume/Throttle requires the PlanetScale client to be initialized with a
// base URL (via Credentials.DSN). This is wired in the tern layer.
func (e *Engine) Volume(ctx context.Context, req *engine.VolumeRequest) (*engine.VolumeResult, error) {
	if req.ResumeState == nil || req.ResumeState.Metadata == "" {
		return nil, fmt.Errorf("no active schema change")
	}
	meta, err := decodePSMetadata(req.ResumeState.Metadata)
	if err != nil {
		return nil, fmt.Errorf("decode resume state: %w", err)
	}
	if meta.DeployRequestID == 0 {
		return nil, fmt.Errorf("no active schema change")
	}

	client, err := e.getClient(req.Credentials)
	if err != nil {
		return nil, fmt.Errorf("get planetscale client: %w", err)
	}

	if req.Volume < 1 || req.Volume > 11 {
		e.logger.Warn("volume out of range, clamping to [1, 11]", "requested", req.Volume)
	}
	ratio := volumeToThrottleRatio(req.Volume)

	err = client.ThrottleDeployRequest(ctx, &psclient.ThrottleDeployRequestRequest{
		Organization:  credOrg(req.Credentials),
		Database:      req.Database,
		Number:        meta.DeployRequestID,
		ThrottleRatio: ratio,
	})
	if err != nil {
		return nil, fmt.Errorf("throttle deploy request: %w", err)
	}

	return &engine.VolumeResult{
		Accepted:       true,
		PreviousVolume: 0, // Unknown — PlanetScale has no query API for current ratio
		NewVolume:      req.Volume,
		Message:        fmt.Sprintf("Throttle ratio set to %.0f%%", ratio*100),
	}, nil
}

// DefaultVolume is the default throttle volume for new deploys.
// Maps to a throttle ratio of 0.85 — aggressive enough to limit impact on
// production traffic while still making progress.
const DefaultVolume int32 = 2

// volumeToThrottleRatio converts volume (1-11) to a PlanetScale throttle ratio.
// Lower volume = more throttling. DefaultVolume (2) maps to 0.85.
// See engine.VolumeRequest for how volume semantics differ between engines.
var volumeThrottleMap = [12]float64{
	0:  0.95, // unused (volume is 1-indexed)
	1:  0.95, // max throttle
	2:  0.85, // default
	3:  0.75,
	4:  0.65,
	5:  0.55,
	6:  0.45,
	7:  0.35,
	8:  0.25,
	9:  0.15,
	10: 0.05,
	11: 0.0, // no throttle
}

func volumeToThrottleRatio(volume int32) float64 {
	if volume <= 1 {
		return volumeThrottleMap[1]
	}
	if volume >= 11 {
		return volumeThrottleMap[11]
	}
	return volumeThrottleMap[volume]
}

// --- SHOW VITESS_MIGRATIONS shard progress ---

// captureExistingContexts returns the set of migration_context values currently
// in SHOW VITESS_MIGRATIONS. Used as a baseline before deploying so that new
// contexts can be identified after deploy.
func (e *Engine) captureExistingContexts(ctx context.Context, client psclient.PSClient, database string, creds *engine.Credentials) map[string]bool {
	existing := make(map[string]bool)
	if creds.DSN == "" {
		return existing
	}

	branch := mainBranch(creds)
	keyspaces, err := client.ListKeyspaces(ctx, &ps.ListKeyspacesRequest{
		Organization: credOrg(creds),
		Database:     database,
		Branch:       branch,
	})
	if err != nil {
		e.logger.Warn("captureExistingContexts: failed to list keyspaces", "error", err)
		return existing
	}

	for _, ks := range keyspaces {
		rows, err := e.showVitessMigrationsForKeyspace(ctx, creds.DSN, ks.Name, "")
		if err != nil {
			e.logger.Debug("capture existing contexts: query failed", "keyspace", ks.Name, "error", err)
			continue
		}
		for _, r := range rows {
			if r.MigrationContext != "" {
				existing[r.MigrationContext] = true
			}
		}
	}

	e.logger.Info("captured schema change context baseline", "count", len(existing))
	return existing
}

// discoverMigrationContext finds the new migration_context that appeared after
// deploying by comparing current contexts against the pre-deploy baseline.
func (e *Engine) discoverMigrationContext(ctx context.Context, client psclient.PSClient, database string, creds *engine.Credentials, existingContexts map[string]bool) string {
	if creds.DSN == "" {
		e.logger.Debug("skipping schema change context discovery, no DSN configured")
		return ""
	}

	e.logger.Info("discovering schema change context", "database", database, "baseline_count", len(existingContexts))

	branch := mainBranch(creds)
	keyspaces, err := client.ListKeyspaces(ctx, &ps.ListKeyspacesRequest{
		Organization: credOrg(creds),
		Database:     database,
		Branch:       branch,
	})
	if err != nil {
		e.logger.Warn("failed to list keyspaces for schema change context discovery", "error", err)
		return ""
	}

	for _, ks := range keyspaces {
		rows, err := e.showVitessMigrationsForKeyspace(ctx, creds.DSN, ks.Name, "")
		if err != nil {
			e.logger.Debug("failed to query schema changes for keyspace", "keyspace", ks.Name, "error", err)
			continue
		}
		for _, r := range rows {
			if r.MigrationContext != "" && !existingContexts[r.MigrationContext] {
				e.logger.Info("discovered schema change context", "context", r.MigrationContext)
				return r.MigrationContext
			}
		}
	}

	e.logger.Warn("schema change context not discovered yet")
	return ""
}

// vitessMigrationRow holds a single row from SHOW VITESS_MIGRATIONS.
type vitessMigrationRow struct {
	MigrationUUID    string
	MigrationContext string
	Keyspace         string
	Shard            string
	Table            string
	Status           string // queued, running, ready_to_complete, complete, failed, cancelled
	ReadyToComplete  bool
	DDLAction        string
	Progress         int
	ETASeconds       int64
	RowsCopied       int64
	TableRows        int64
	IsImmediate      bool
	CutoverAttempts  int
	StartedAt        *time.Time
	CompletedAt      *time.Time
}

// queryVitessMigrations queries SHOW VITESS_MIGRATIONS across all keyspaces via vtgate
// and aggregates per-shard results into per-table TableProgress entries.
func (e *Engine) queryVitessMigrations(ctx context.Context, client psclient.PSClient, database string, creds *engine.Credentials, migrationContext string) ([]engine.TableProgress, int) {
	branch := mainBranch(creds)
	keyspaces, err := client.ListKeyspaces(ctx, &ps.ListKeyspacesRequest{
		Organization: credOrg(creds),
		Database:     database,
		Branch:       branch,
	})
	if err != nil {
		e.logger.Warn("queryVitessMigrations: failed to list keyspaces", "error", err)
		return nil, 0
	}

	var allRows []vitessMigrationRow
	for _, ks := range keyspaces {
		rows, err := e.showVitessMigrationsForKeyspace(ctx, creds.DSN, ks.Name, migrationContext)
		if err != nil {
			e.logger.Error("per-shard progress query failed", "keyspace", ks.Name, "database", database, "error", err)
			continue
		}
		allRows = append(allRows, rows...)
	}

	if len(allRows) == 0 {
		return nil, 0
	}

	return aggregateShardProgress(allRows)
}

// showVitessMigrationsForKeyspace connects to vtgate and runs
// SHOW VITESS_MIGRATIONS LIKE '<context>' for a single keyspace.
// If migrationContext is empty, returns all migrations.
func (e *Engine) showVitessMigrationsForKeyspace(ctx context.Context, dsn, keyspace, migrationContext string) ([]vitessMigrationRow, error) {
	if migrationContext != "" {
		if err := validateMigrationContext(migrationContext); err != nil {
			return nil, fmt.Errorf("validate context for keyspace %s: %w", keyspace, err)
		}
	}

	db, err := e.getVtgateDB(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("get vtgate connection for keyspace %s: %w", keyspace, err)
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("get connection: %w", err)
	}
	defer utils.CloseAndLog(conn)

	if _, err := conn.ExecContext(ctx, "USE `"+keyspace+"`"); err != nil {
		return nil, fmt.Errorf("use keyspace %s: %w", keyspace, err)
	}

	query := "SHOW VITESS_MIGRATIONS"
	if migrationContext != "" {
		query += " LIKE '" + migrationContext + "'"
	}
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("show vitess_migrations: %w", err)
	}
	defer utils.CloseAndLog(rows)

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("get columns: %w", err)
	}

	var result []vitessMigrationRow
	for rows.Next() {
		colValues := make([]sql.NullString, len(columns))
		colPtrs := make([]any, len(columns))
		for i := range colValues {
			colPtrs[i] = &colValues[i]
		}
		if err := rows.Scan(colPtrs...); err != nil {
			e.logger.Debug("scan vitess_migrations row", "keyspace", keyspace, "error", err)
			continue
		}
		colMap := make(map[string]string)
		for i, col := range columns {
			if colValues[i].Valid {
				colMap[col] = colValues[i].String
			}
		}

		row := vitessMigrationRow{
			MigrationUUID:    colMap["migration_uuid"],
			MigrationContext: colMap["migration_context"],
			Keyspace:         colMap["keyspace"],
			Shard:            colMap["shard"],
			Table:            colMap["mysql_table"],
			Status:           colMap["migration_status"],
			ReadyToComplete:  colMap["ready_to_complete"] == "1",
			DDLAction:        colMap["ddl_action"],
			IsImmediate:      colMap["is_immediate_operation"] == "1",
		}
		if v, err := strconv.Atoi(colMap["progress"]); err != nil && colMap["progress"] != "" {
			e.logger.Debug("parse vitess_migrations field", "field", "progress", "value", colMap["progress"], "error", err)
		} else {
			row.Progress = v
		}
		if v, err := parseInt64(colMap["eta_seconds"]); err != nil {
			e.logger.Debug("parse vitess_migrations field", "field", "eta_seconds", "value", colMap["eta_seconds"], "error", err)
		} else {
			row.ETASeconds = v
		}
		if v, err := parseInt64(colMap["rows_copied"]); err != nil {
			e.logger.Debug("parse vitess_migrations field", "field", "rows_copied", "value", colMap["rows_copied"], "error", err)
		} else {
			row.RowsCopied = v
		}
		if v, err := parseInt64(colMap["table_rows"]); err != nil {
			e.logger.Debug("parse vitess_migrations field", "field", "table_rows", "value", colMap["table_rows"], "error", err)
		} else {
			row.TableRows = v
		}
		if v, err := parseInt64(colMap["cutover_attempts"]); err == nil {
			row.CutoverAttempts = int(v)
		}

		if ts, parseErr := time.Parse("2006-01-02 15:04:05", colMap["started_timestamp"]); parseErr == nil {
			row.StartedAt = &ts
		}
		if ts, parseErr := time.Parse("2006-01-02 15:04:05", colMap["completed_timestamp"]); parseErr == nil {
			row.CompletedAt = &ts
		}

		result = append(result, row)
	}
	return result, rows.Err()
}

// validateMigrationContext rejects migration context strings containing unsafe characters.
func validateMigrationContext(s string) error {
	if strings.ContainsAny(s, "'\"\\`") {
		return fmt.Errorf("invalid context: contains unsafe characters")
	}
	return nil
}

func parseInt64(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	return strconv.ParseInt(s, 10, 64)
}

// aggregateShardProgress groups SHOW VITESS_MIGRATIONS rows by migration_uuid
// and produces per-table progress with per-shard breakdown.
func aggregateShardProgress(rows []vitessMigrationRow) ([]engine.TableProgress, int) {
	type tableKey struct {
		keyspace string
		table    string
		uuid     string
	}
	type shardData struct {
		shard           string
		status          string
		readyToComplete bool
		progress        int
		rowsCopied      int64
		tableRows       int64
		etaSeconds      int64
		isImmediate     bool
		cutoverAttempts int
		startedAt       *time.Time
		completedAt     *time.Time
	}

	tableShards := make(map[tableKey][]shardData)
	tableOrder := make([]tableKey, 0)

	for _, r := range rows {
		key := tableKey{keyspace: r.Keyspace, table: r.Table, uuid: r.MigrationUUID}
		if _, exists := tableShards[key]; !exists {
			tableOrder = append(tableOrder, key)
		}
		tableShards[key] = append(tableShards[key], shardData{
			shard:           r.Shard,
			status:          r.Status,
			readyToComplete: r.ReadyToComplete,
			progress:        r.Progress,
			rowsCopied:      r.RowsCopied,
			tableRows:       r.TableRows,
			etaSeconds:      r.ETASeconds,
			isImmediate:     r.IsImmediate,
			cutoverAttempts: r.CutoverAttempts,
			startedAt:       r.StartedAt,
			completedAt:     r.CompletedAt,
		})
	}

	var totalRowsCopied, totalTableRows int64
	var tables []engine.TableProgress

	for _, key := range tableOrder {
		shards := tableShards[key]

		// Sort shards by Vitess key range for consistent ordering
		sort.Slice(shards, func(i, j int) bool {
			return shardLess(shards[i].shard, shards[j].shard)
		})

		var tblRowsCopied, tblTableRows, maxETA int64
		var tblProgress int
		var tblStartedAt *time.Time
		var latestCompletedAt *time.Time
		allShardsCompleted := true
		shardProgress := make([]engine.ShardProgress, len(shards))
		isInstant := true

		// Determine aggregate table state from shard states
		tableState := state.Vitess.Complete
		for i, sh := range shards {
			tblTableRows += sh.tableRows
			if sh.etaSeconds > maxETA {
				maxETA = sh.etaSeconds
			}
			if !sh.isImmediate {
				isInstant = false
			}
			// Table started_at = earliest shard started_at
			if sh.startedAt != nil && (tblStartedAt == nil || sh.startedAt.Before(*tblStartedAt)) {
				tblStartedAt = sh.startedAt
			}
			// Track latest completed_at across shards
			if sh.completedAt == nil {
				allShardsCompleted = false
			} else if latestCompletedAt == nil || sh.completedAt.After(*latestCompletedAt) {
				latestCompletedAt = sh.completedAt
			}

			// Resolve effective shard state: running + ready_to_complete = ready_to_complete
			shardState := sh.status
			if sh.status == state.Vitess.Running && sh.readyToComplete {
				shardState = state.Vitess.ReadyToComplete
			}

			shardPct := sh.progress
			shardCopied := sh.rowsCopied
			// When a shard is ready for cutover, the copy phase is complete.
			// Clamp to 100% since Vitess row counts can lag behind slightly.
			if shardState == state.Vitess.ReadyToComplete || shardState == state.Vitess.Complete {
				shardPct = 100
				if sh.tableRows > 0 && shardCopied < sh.tableRows {
					shardCopied = sh.tableRows
				}
			}

			tblRowsCopied += shardCopied

			shardProgress[i] = engine.ShardProgress{
				Shard:           sh.shard,
				State:           shardState,
				Progress:        shardPct,
				RowsCopied:      shardCopied,
				RowsTotal:       sh.tableRows,
				ETASeconds:      sh.etaSeconds,
				CutoverAttempts: sh.cutoverAttempts,
			}

			tableState = resolveTableState(tableState, shardState)
		}

		if tblTableRows > 0 {
			tblProgress = int(tblRowsCopied * 100 / tblTableRows)
		} else if tableState == state.Vitess.Complete || tableState == state.Vitess.ReadyToComplete {
			tblProgress = 100
		}

		totalRowsCopied += tblRowsCopied
		totalTableRows += tblTableRows

		// Table completed_at is only set when all shards have completed.
		var tblCompletedAt *time.Time
		if allShardsCompleted {
			tblCompletedAt = latestCompletedAt
		}

		tables = append(tables, engine.TableProgress{
			Namespace:   key.keyspace,
			Table:       key.table,
			State:       tableState,
			Progress:    tblProgress,
			RowsCopied:  tblRowsCopied,
			RowsTotal:   tblTableRows,
			ETASeconds:  maxETA,
			Shards:      shardProgress,
			IsInstant:   isInstant,
			StartedAt:   tblStartedAt,
			CompletedAt: tblCompletedAt,
		})
	}

	overallProgress := 0
	if totalTableRows > 0 {
		overallProgress = int(totalRowsCopied * 100 / totalTableRows)
	} else if len(tables) > 0 {
		allDone := true
		for _, t := range tables {
			if t.State != state.Vitess.Complete && t.State != state.Vitess.ReadyToComplete {
				allDone = false
				break
			}
		}
		if allDone {
			overallProgress = 100
		}
	}

	return tables, overallProgress
}

// resolveTableState merges a shard's state into the current table state.
// A table has one Vitess migration per shard, each in a different state.
// This picks the "worst" state so the table reflects the least-progressed shard:
//
//	failed            — any shard failed, table is failed
//	running           — at least one shard still copying rows
//	queued            — at least one shard not started, none running or failed
//	ready_to_complete — all shards done copying, waiting for cutover
//	complete          — all shards finished (initial value)
func resolveTableState(tableState, shardState string) string {
	switch shardState {
	case state.Vitess.Failed, state.Vitess.Cancelled:
		return state.Vitess.Failed
	case state.Vitess.Running:
		if tableState != state.Vitess.Failed {
			return state.Vitess.Running
		}
	case state.Vitess.Queued, state.Vitess.Ready, state.Vitess.Requested:
		if tableState != state.Vitess.Failed && tableState != state.Vitess.Running {
			return state.Vitess.Queued
		}
	case state.Vitess.ReadyToComplete:
		if tableState == state.Vitess.Complete {
			return state.Vitess.ReadyToComplete
		}
	}
	return tableState
}

// shardLess compares two Vitess shard key ranges for sorting.
func shardLess(a, b string) bool {
	aStart := ""
	bStart := ""
	if idx := strings.Index(a, "-"); idx > 0 {
		aStart = a[:idx]
	}
	if idx := strings.Index(b, "-"); idx > 0 {
		bStart = b[:idx]
	}
	if aStart == "" && bStart != "" {
		return true
	}
	if aStart != "" && bStart == "" {
		return false
	}
	return aStart < bStart
}

// --- Helper functions ---

func (e *Engine) fetchDatabaseSchema(ctx context.Context, client psclient.PSClient, org, database, branch string, keyspaces []string) (map[string][]table.TableSchema, error) {
	var mu sync.Mutex
	result := make(map[string][]table.TableSchema, len(keyspaces))
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(20)

	for _, keyspace := range keyspaces {
		ks := keyspace
		g.Go(func() error {
			schemaResult, err := client.GetBranchSchema(gCtx, &ps.BranchSchemaRequest{
				Organization: org,
				Database:     database,
				Branch:       branch,
				Keyspace:     ks,
			})
			if err != nil {
				var psErr *ps.Error
				if errors.As(err, &psErr) && psErr.Code == ps.ErrNotFound {
					// Keyspace doesn't exist yet — treat as empty so all
					// tables appear as CREATEs in the diff.
					e.logger.Info("keyspace not found on branch, treating as empty",
						"keyspace", ks, "branch", branch)
					mu.Lock()
					result[ks] = nil
					mu.Unlock()
					return nil
				}
				return fmt.Errorf("fetch schema for keyspace %s: %w", ks, err)
			}

			tables := make([]table.TableSchema, len(schemaResult))
			for i, t := range schemaResult {
				tables[i] = table.TableSchema{Name: t.Name, Schema: t.Raw}
			}
			mu.Lock()
			result[ks] = tables
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return result, nil
}

func (e *Engine) fetchPlanSchema(ctx context.Context, client psclient.PSClient, org, database, branch string, creds *engine.Credentials, keyspaces []string) (map[string][]table.TableSchema, error) {
	parent, err := client.GetBranch(ctx, &ps.GetDatabaseBranchRequest{
		Organization: org,
		Database:     database,
		Branch:       branch,
	})
	if err != nil {
		return nil, fmt.Errorf("get branch %s: %w", branch, err)
	}

	if parent.SafeMigrations {
		e.logger.Debug("using PlanetScale schema API for plan", "database", database, "branch", branch)
		return e.fetchDatabaseSchema(ctx, client, org, database, branch, keyspaces)
	}

	if creds == nil || creds.DSN == "" {
		return nil, fmt.Errorf("safe schema changes are not enabled on branch %q of database %q and vtgate DSN is not configured", branch, database)
	}

	e.logger.Info("using vtgate schema for plan because PlanetScale safe schema changes are disabled", "database", database, "branch", branch)
	return e.fetchVtgateSchema(ctx, creds.DSN, keyspaces)
}

func (e *Engine) fetchVtgateSchema(ctx context.Context, dsn string, keyspaces []string) (map[string][]table.TableSchema, error) {
	var mu sync.Mutex
	result := make(map[string][]table.TableSchema, len(keyspaces))
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(20)

	for _, keyspace := range keyspaces {
		ks := keyspace
		g.Go(func() error {
			db, err := e.getVtgateKeyspaceDB(gCtx, dsn, ks)
			if err != nil {
				return fmt.Errorf("get vtgate connection for keyspace %s: %w", ks, err)
			}
			tables, err := table.LoadSchemaFromDB(gCtx, db, table.WithoutUnderscoreTables)
			if err != nil {
				return fmt.Errorf("load schema for keyspace %s: %w", ks, err)
			}
			mu.Lock()
			result[ks] = tables
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return result, nil
}

func (e *Engine) createBranch(ctx context.Context, client psclient.PSClient, org, database, branchName, parentBranch string) (*ps.DatabaseBranch, error) {
	getCtx, getCancel := context.WithTimeout(ctx, 10*time.Second)
	defer getCancel()

	parent, err := client.GetBranch(getCtx, &ps.GetDatabaseBranchRequest{
		Organization: org,
		Database:     database,
		Branch:       parentBranch,
	})
	if err != nil {
		return nil, fmt.Errorf("get parent branch: %w", err)
	}

	if !parent.SafeMigrations {
		return nil, fmt.Errorf("safe schema changes not enabled on branch %q of database %q — enable it in the PlanetScale console before running schema changes", parentBranch, database)
	}

	createCtx, createCancel := context.WithTimeout(ctx, 30*time.Second)
	defer createCancel()

	branch, err := client.CreateBranch(createCtx, &ps.CreateDatabaseBranchRequest{
		Organization: org,
		Database:     database,
		Name:         branchName,
		ParentBranch: parentBranch,
		Region:       parent.Region.Slug,
	})
	if err != nil {
		// Idempotent: if branch exists, return it
		if strings.Contains(err.Error(), "Name has already been taken") {
			e.logger.Info("branch already exists, reusing", "branch", branchName)
			return client.GetBranch(ctx, &ps.GetDatabaseBranchRequest{
				Organization: org,
				Database:     database,
				Branch:       branchName,
			})
		}
		return nil, fmt.Errorf("create branch %s: %w", branchName, err)
	}
	return branch, nil
}

func (e *Engine) waitForBranchReady(ctx context.Context, client psclient.PSClient, org, database, branchName string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	var consecutiveErrors int
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for branch %s", branchName)
		case <-ticker.C:
			branch, err := client.GetBranch(ctx, &ps.GetDatabaseBranchRequest{
				Organization: org,
				Database:     database,
				Branch:       branchName,
			})
			if err != nil {
				consecutiveErrors++
				e.logger.Warn("error checking branch status",
					"branch", branchName, "error", err, "consecutive_errors", consecutiveErrors)
				if consecutiveErrors >= 5 {
					return fmt.Errorf("branch %s not reachable after %d attempts: %w", branchName, consecutiveErrors, err)
				}
				continue
			}
			consecutiveErrors = 0
			if branch.Ready {
				return nil
			}
		}
	}
}

func (e *Engine) createDeployRequest(ctx context.Context, client psclient.PSClient, org, database, branchName, intoBranch string, autoCutover, autoDeleteBranch bool) (*ps.DeployRequest, error) {
	return client.CreateDeployRequest(ctx, &ps.CreateDeployRequestRequest{
		Organization:     org,
		Database:         database,
		Branch:           branchName,
		IntoBranch:       intoBranch,
		AutoCutover:      autoCutover,
		AutoDeleteBranch: autoDeleteBranch,
	})
}

func (e *Engine) getDeployRequest(ctx context.Context, client psclient.PSClient, org, database string, number uint64) (*ps.DeployRequest, error) {
	return client.GetDeployRequest(ctx, &ps.GetDeployRequestRequest{
		Organization: org,
		Database:     database,
		Number:       number,
	})
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

// --- Deploy state mapping ---

func deployStateToEngineState(drState string) (engine.State, int) {
	switch drState {
	case deployState.Pending:
		return engine.StatePending, 0
	case deployState.Ready:
		return engine.StatePending, 0
	case deployState.NoChanges:
		return engine.StateCompleted, 100
	case deployState.Queued, deployState.Submitting:
		return engine.StateRunning, 5
	case deployState.InProgress:
		return engine.StateRunning, 50
	case deployState.InProgressVSchema:
		return engine.StateRunning, 50
	case deployState.PendingCutover:
		return engine.StateWaitingForCutover, 90
	case deployState.InProgressCutover:
		return engine.StateCuttingOver, 95
	case deployState.Complete:
		return engine.StateCompleted, 100
	case deployState.CompletePendingRevert:
		return engine.StateRevertWindow, 100
	case deployState.CompleteError, deployState.Error, deployState.Failed:
		return engine.StateFailed, 0
	case deployState.InProgressCancel:
		return engine.StateStopped, 0
	case deployState.CompleteCancel, deployState.Cancelled:
		return engine.StateStopped, 0
	case deployState.InProgressRevert, deployState.InProgressRevertVSchema:
		return engine.StateRunning, 50
	case deployState.CompleteRevert:
		return engine.StateReverted, 100
	case deployState.CompleteRevertError:
		return engine.StateFailed, 0
	default:
		return engine.StateRunning, 25
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
		return "Applying VSchema changes"
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

// --- SQL helpers ---

// parseDesiredSchemas parses CREATE TABLE statements from schema files in a namespace,
// returning table schemas suitable for diffing against current state. Skips vschema.json
// and non-.sql files.
func parseDesiredSchemas(keyspace string, ns *schema.Namespace) ([]table.TableSchema, error) {
	var schemas []table.TableSchema
	for filename, content := range ns.Files {
		if filename == "vschema.json" || !strings.HasSuffix(filename, ".sql") {
			continue
		}
		stmts, err := ddl.SplitStatements(content)
		if err != nil {
			return nil, fmt.Errorf("split SQL for keyspace %s: %w", keyspace, err)
		}
		for _, stmt := range stmts {
			ct, err := statement.ParseCreateTable(stmt)
			if err != nil {
				return nil, fmt.Errorf("parse desired schema in keyspace %s/%s: %w", keyspace, filename, err)
			}
			if err := ddl.ValidateCreateTable(ct); err != nil {
				return nil, fmt.Errorf("SQL usage error in keyspace %s/%s: %w", keyspace, filename, err)
			}
			schemas = append(schemas, table.TableSchema{
				Name:   ct.TableName,
				Schema: stmt,
			})
		}
	}
	return schemas, nil
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
