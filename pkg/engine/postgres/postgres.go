// Package postgres implements the Engine interface for PostgreSQL databases.
package postgres

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/block/pg-sprite/pkg/dbconn"
	"github.com/block/pg-sprite/pkg/diffplan"
	pgplan "github.com/block/pg-sprite/pkg/plan"
	"github.com/block/pg-sprite/pkg/planner"
	"github.com/block/pg-sprite/pkg/preflight"
	"github.com/block/pg-sprite/pkg/router"
	pgstatement "github.com/block/pg-sprite/pkg/statement"
	"github.com/block/spirit/pkg/utils"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/postgresconn"
	"github.com/block/schemabot/pkg/schema"
)

// Engine implements engine.Engine for PostgreSQL databases.
type Engine struct {
	mu sync.Mutex
	wg sync.WaitGroup
	// progress is the tracked schema change's latest state, keyed by
	// progressKey (the apply's ResumeState.MigrationContext). One engine is
	// shared for the lifetime of a target, so Progress must answer for the
	// apply the caller identifies — never for whichever apply wrote last.
	progress       *engine.ProgressResult
	progressKey    string
	tableSizeLimit int64
}

// DefaultNativeSafeTableSizeLimitBytes preserves the native-safe execution
// ceiling when the server does not configure one.
const DefaultNativeSafeTableSizeLimitBytes = int64(1 << 30)

// New creates a new PostgreSQL engine.
func New() *Engine {
	return NewWithTableSizeLimit(DefaultNativeSafeTableSizeLimitBytes)
}

// NewWithTableSizeLimit creates a PostgreSQL engine with the native-safe
// table size ceiling expressed in bytes.
func NewWithTableSizeLimit(tableSizeLimit int64) *Engine {
	if tableSizeLimit <= 0 {
		tableSizeLimit = DefaultNativeSafeTableSizeLimitBytes
	}
	return &Engine{tableSizeLimit: tableSizeLimit}
}

// TableSizeLimit exposes the native-safe ceiling for wiring verification and observability.
func (e *Engine) TableSizeLimit() int64 {
	return e.tableSizeLimit
}

// Name returns the engine identifier.
func (e *Engine) Name() string {
	return "postgres"
}

// Plan computes the changes needed to reach the desired schema.
func (e *Engine) Plan(ctx context.Context, req *engine.PlanRequest) (*engine.PlanResult, error) {
	if req == nil {
		return nil, fmt.Errorf("plan PostgreSQL schema: request is required")
	}
	if req.Credentials == nil || req.Credentials.DSN == "" {
		return nil, fmt.Errorf("plan PostgreSQL database %q: DSN credentials are required", req.Database)
	}

	caPath, err := caCertPath(req.Credentials)
	if err != nil {
		return nil, fmt.Errorf("plan PostgreSQL database %q: %w", req.Database, err)
	}
	validationOpts, err := validationRootCAs(caPath)
	if err != nil {
		return nil, fmt.Errorf("plan PostgreSQL database %q: %w", req.Database, err)
	}

	// Validate the SchemaBot-managed connection path, including its transport
	// policy, before adapting the same normalized DSN to pg-sprite's pool API.
	db, err := postgresconn.Open(req.Credentials.DSN, validationOpts...)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL database %q for planning: %w", req.Database, err)
	}
	defer utils.CloseAndLog(db)
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping PostgreSQL database %q for planning: %w", req.Database, err)
	}

	poolCfg, err := spritePoolConfig(req.Credentials.DSN, caPath)
	if err != nil {
		return nil, fmt.Errorf("plan PostgreSQL database %q: %w", req.Database, err)
	}
	pool, err := dbconn.NewPool(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("open pg-sprite pool for PostgreSQL database %q: %w", req.Database, err)
	}
	defer pool.Close()
	return planSchemas(ctx, pool, req)
}

func planSchemas(ctx context.Context, pool *pgxpool.Pool, req *engine.PlanRequest) (*engine.PlanResult, error) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	if err != nil {
		return nil, fmt.Errorf("select PostgreSQL statement parser: %w", err)
	}

	namespaces := sortedKeys(req.SchemaFiles)
	result := &engine.PlanResult{}
	for _, namespace := range namespaces {
		ns := req.SchemaFiles[namespace]
		if ns == nil {
			return nil, fmt.Errorf("plan PostgreSQL namespace %q: schema files are required", namespace)
		}
		files := sortedKeys(ns.Files)
		schemaChange := engine.SchemaChange{Namespace: namespace}
		for _, filename := range files {
			desired, err := pgstatement.ParseDesired(ns.Files[filename])
			if err != nil {
				return nil, fmt.Errorf("parse desired PostgreSQL schema in %q/%q: %w", namespace, filename, err)
			}
			report, err := diffplan.Plan(ctx, pool, diffplan.Request{Schema: namespace, Desired: desired})
			if err != nil {
				return nil, fmt.Errorf("diff PostgreSQL table %q in namespace %q from file %q: %w", desired.Table(), namespace, filename, err)
			}
			changes, err := tableChanges(report, parser)
			if err != nil {
				return nil, fmt.Errorf("render PostgreSQL plan for table %q in namespace %q: %w", desired.Table(), namespace, err)
			}
			schemaChange.TableChanges = append(schemaChange.TableChanges, changes...)
		}
		if len(schemaChange.TableChanges) > 0 {
			result.Changes = append(result.Changes, schemaChange)
		}
	}
	result.NoChanges = len(result.Changes) == 0
	result.PlanID = engine.NewPlanID()
	return result, nil
}

func tableChanges(report pgplan.Report, parser ddl.StatementParser) ([]engine.TableChange, error) {
	changes := make([]engine.TableChange, 0, len(report.Statements))
	for _, statement := range report.Statements {
		mode, reason := executionVerdict(report.FormatVersion, statement, report.Table)
		rendered := statement.ExecSQL
		if len(rendered) == 0 {
			rendered = []string{statement.SQL}
		}
		for _, sql := range rendered {
			operation, table, err := parser.Classify(sql)
			if err != nil {
				return nil, fmt.Errorf("classify planned statement for table %q: %w", report.Table, err)
			}
			if table == "" {
				table = report.Table
			}
			stepMode, stepReason := mode, reason
			if stepMode == "" {
				// The apply path derives a privilege tier for every statement
				// it executes, and that derivation refuses shapes outside the
				// native-safe set. Run the same authority here so the verdict
				// the operator reviews matches what the engine will do,
				// instead of emitting an executable plan that deterministically
				// fails at apply.
				if _, tierErr := preflight.RequiredTier([]string{sql}); tierErr != nil {
					stepMode = engine.ExecutionModeBlocked
					stepReason = fmt.Sprintf("statement for table %q is a shape SchemaBot's PostgreSQL support does not execute yet; rewriting the change cannot make it eligible", table)
				}
			}
			changes = append(changes, engine.TableChange{
				Table:         table,
				Operation:     operation,
				DDL:           sql,
				IsUnsafe:      statement.Destructive,
				UnsafeReason:  destructiveReason(statement.Destructive, table),
				ExecutionMode: stepMode,
				ModeReason:    stepReason,
			})
		}
	}
	return changes, nil
}

func executionVerdict(formatVersion int, statement pgplan.Statement, table string) (string, string) {
	if formatVersion != pgplan.FormatVersion {
		return engine.ExecutionModeBlocked, fmt.Sprintf("statement for table %q has an unrecognized plan contract", table)
	}
	if statement.Disposition == router.DispositionExecute && statement.Route == planner.RouteNative &&
		statement.Backend == router.BackendNative && len(statement.ExecSQL) > 0 {
		return "", ""
	}

	// Planner explanations are deliberately not copied to operator-facing
	// text, and neither is the planner's own vocabulary: each known
	// disposition maps to a sentence in SchemaBot's words, since the operator
	// reading it has never heard of the planning library.
	switch statement.Disposition {
	case router.DispositionUnavailable:
		if statement.Backend == router.BackendCopyAndSwap {
			return engine.ExecutionModeBlocked, fmt.Sprintf("statement for table %q requires copy-and-swap, which is unavailable", table)
		}
		return engine.ExecutionModeBlocked, fmt.Sprintf("statement for table %q requires an execution path SchemaBot's PostgreSQL support does not provide yet", table)
	case router.DispositionRewriteRequired:
		return engine.ExecutionModeBlocked, fmt.Sprintf("statement for table %q must be rewritten into a form the engine can execute natively, then re-planned", table)
	case router.DispositionRefuse:
		return engine.ExecutionModeBlocked, fmt.Sprintf("statement for table %q is refused: it cannot be executed safely as written", table)
	default:
		return engine.ExecutionModeBlocked, fmt.Sprintf("statement for table %q has an unrecognized planner verdict", table)
	}
}

func destructiveReason(destructive bool, table string) string {
	if !destructive {
		return ""
	}
	return fmt.Sprintf("statement removes live structure from table %q", table)
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// Drain blocks until every background apply goroutine has finished, then
// clears the tracked schema change so the next Progress reports the idle
// sentinel. Resume and recovery paths call this before re-planning so a
// statement still in flight from a lost lease cannot race the next drive's
// view of the schema, and so the next poll reads a clean engine instead of
// the previous change's terminal snapshot.
func (e *Engine) Drain() {
	e.wg.Wait()
	e.mu.Lock()
	e.progress = nil
	e.progressKey = ""
	e.mu.Unlock()
}

// Stop declines: a PostgreSQL schema change runs each statement as a single
// transactional DDL with no engine phase to pause — an in-flight statement
// either commits or fails on its own. The typed decline lets the control
// path resolve a durable stop request terminally instead of retrying it.
func (e *Engine) Stop(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return nil, engine.NewUnsupportedOperationError("stop is not supported for PostgreSQL schema changes: each statement runs as a single transaction that commits or fails on its own")
}

// Cancel declines: the engine runs each statement as one transaction and does
// not track the database backend executing it, so it cannot terminate the
// statement itself. An in-flight DDL can still be interrupted at the database
// — during a lock pileup that is exactly what an operator needs — so the
// decline reason points at the out-of-band path instead of stopping at "no".
func (e *Engine) Cancel(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return nil, engine.NewUnsupportedOperationError("cancel is not implemented for PostgreSQL schema changes: the engine cannot terminate its in-flight statement, which commits or fails as one transaction; to interrupt it at the database, find the backend running the DDL in pg_stat_activity and cancel it with pg_cancel_backend")
}

// Start declines: PostgreSQL schema changes cannot be stopped, so there is
// never a stopped engine phase to resume.
func (e *Engine) Start(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return nil, engine.NewUnsupportedOperationError("start is not supported for PostgreSQL schema changes: there is no stopped engine phase to resume")
}

// Cutover declines: PostgreSQL schema changes apply DDL directly and have no
// table-swap phase to trigger.
func (e *Engine) Cutover(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return nil, engine.NewUnsupportedOperationError("cutover is not supported for PostgreSQL schema changes: DDL is applied directly with no table swap")
}

// Revert declines: PostgreSQL schema changes commit directly and have no
// revert window.
func (e *Engine) Revert(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return nil, engine.NewUnsupportedOperationError("revert is not supported for PostgreSQL schema changes: changes commit directly with no revert window")
}

// SkipRevert declines: PostgreSQL schema changes have no revert window to end
// early — every committed change is already permanent.
func (e *Engine) SkipRevert(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return nil, engine.NewUnsupportedOperationError("skip-revert is not supported for PostgreSQL schema changes: changes commit directly with no revert window")
}

// Volume declines: PostgreSQL schema changes run statement phases with no
// tunable row copy to retune.
func (e *Engine) Volume(ctx context.Context, req *engine.VolumeRequest) (*engine.VolumeResult, error) {
	return nil, engine.NewUnsupportedOperationError("volume is not supported for PostgreSQL schema changes: there is no tunable row copy")
}

// Compile-time check that Engine implements engine.Engine.
var _ engine.Engine = (*Engine)(nil)

// Compile-time check that Engine implements engine.Drainer.
var _ engine.Drainer = (*Engine)(nil)
