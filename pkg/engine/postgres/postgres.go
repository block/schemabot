// Package postgres implements the Engine interface for PostgreSQL databases.
package postgres

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/block/pg-sprite/pkg/dbconn"
	"github.com/block/pg-sprite/pkg/diffplan"
	pgplan "github.com/block/pg-sprite/pkg/plan"
	"github.com/block/pg-sprite/pkg/planner"
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
	mu       sync.Mutex
	wg       sync.WaitGroup
	progress *engine.ProgressResult
}

// New creates a new PostgreSQL engine.
func New() *Engine {
	return &Engine{}
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

	// Validate the SchemaBot-managed connection path, including its transport
	// policy, before adapting the same normalized DSN to pg-sprite's pool API.
	db, err := postgresconn.Open(req.Credentials.DSN)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL database %q for planning: %w", req.Database, err)
	}
	defer utils.CloseAndLog(db)
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping PostgreSQL database %q for planning: %w", req.Database, err)
	}

	dsn, err := postgresconn.ConnectionDSN(req.Credentials.DSN)
	if err != nil {
		return nil, fmt.Errorf("normalize PostgreSQL database %q DSN for planning: %w", req.Database, err)
	}
	pool, err := dbconn.NewPool(ctx, dbconn.Config{URL: dsn})
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
	result.PlanID = fmt.Sprintf("postgres-plan-%d", time.Now().UnixNano())
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
			changes = append(changes, engine.TableChange{
				Table:         table,
				Operation:     operation,
				DDL:           sql,
				IsUnsafe:      statement.Destructive,
				UnsafeReason:  destructiveReason(statement.Destructive, table),
				ExecutionMode: mode,
				ModeReason:    reason,
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

	verdict := string(statement.Disposition)
	switch statement.Disposition {
	case router.DispositionUnavailable:
		if statement.Backend == router.BackendCopyAndSwap {
			return engine.ExecutionModeBlocked, fmt.Sprintf("statement for table %q requires copy-and-swap, which is unavailable", table)
		}
	case router.DispositionRewriteRequired, router.DispositionRefuse:
		// These known dispositions use the stable typed value below. Planner
		// explanations are deliberately not copied to operator-facing text.
	default:
		verdict = "unrecognized"
	}
	return engine.ExecutionModeBlocked, fmt.Sprintf("statement for table %q has blocked verdict %q", table, verdict)
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

// Stop pauses a running schema change.
func (e *Engine) Stop(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return nil, fmt.Errorf("postgres engine not implemented")
}

// Cancel terminates a running schema change.
func (e *Engine) Cancel(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return nil, fmt.Errorf("postgres engine not implemented")
}

// Start resumes a stopped schema change.
func (e *Engine) Start(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return nil, fmt.Errorf("postgres engine not implemented")
}

// Cutover triggers the final table swap.
func (e *Engine) Cutover(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return nil, fmt.Errorf("postgres engine not implemented")
}

// Revert rolls back a completed schema change during the revert window.
func (e *Engine) Revert(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return nil, fmt.Errorf("postgres engine not implemented")
}

// SkipRevert ends the revert window early, making changes permanent.
func (e *Engine) SkipRevert(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return nil, fmt.Errorf("postgres engine not implemented")
}

// Volume adjusts the schema change speed.
func (e *Engine) Volume(ctx context.Context, req *engine.VolumeRequest) (*engine.VolumeResult, error) {
	return nil, fmt.Errorf("postgres engine not implemented")
}

// Compile-time check that Engine implements engine.Engine.
var _ engine.Engine = (*Engine)(nil)
