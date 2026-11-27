// Package postgres implements the Engine interface for PostgreSQL databases.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/block/pg-sprite/pkg/dbconn"
	"github.com/block/pg-sprite/pkg/executor"
	pgplan "github.com/block/pg-sprite/pkg/plan"
	"github.com/block/pg-sprite/pkg/planner"
	"github.com/block/pg-sprite/pkg/preflight"
	"github.com/block/pg-sprite/pkg/progress"
	"github.com/block/pg-sprite/pkg/router"
	"github.com/block/pg-sprite/pkg/schemadiff"
	pgstatement "github.com/block/pg-sprite/pkg/statement"
	"github.com/block/pg-sprite/pkg/suggest"
	"github.com/block/pg-sprite/pkg/verdict"
	"github.com/block/spirit/pkg/utils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/postgresconn"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/targetauth"
)

// Engine implements engine.Engine for PostgreSQL databases.
type Engine struct {
	mu              sync.Mutex
	wg              sync.WaitGroup
	pullDatabase    string
	pullCredentials *engine.Credentials
	tableOwner      string
	// progress holds the latest state of every schema change this engine is
	// tracking, keyed by the apply's identity (its
	// ResumeState.MigrationContext). One engine is shared for the lifetime of a
	// target, so Progress must answer for the apply the caller identifies — and
	// accepting a second apply on the same target must not evict the first
	// one's state while it is still running, or the running apply's driver
	// would be told its work no longer exists.
	progress       map[string]*trackedApply
	tableSizeLimit int64

	// optimisticApplyCeiling is the fixed client-side ceiling on one ordinary
	// apply, seeded from the package constant of the same name. It is not an
	// operator option: production never varies it, and only in-package tests
	// assign it, to observe the ceiling ending an apply without waiting out
	// the production value.
	optimisticApplyCeiling     time.Duration
	concurrentIndexMaxDuration time.Duration
	validateMaxDuration        time.Duration

	// execute is a test seam standing in for executeOptimistic, so the apply
	// drive — accept, claim, execute, terminal publish — can be exercised
	// against an executor the test scripts instead of a target to dial. Nil
	// selects the real executor.
	execute func(ctx context.Context, conn targetConn, change nativeApply, tableSizeLimit int64, tracker *progress.Tracker, logger *slog.Logger) error
}

// trackedApply pairs the progress the engine has published for one apply
// with the pg-sprite tracker its executor feeds. The published result changes
// only at accept and at the terminal outcome; the tracker is what moves in
// between, so Progress reads the step position and statement from it. The
// logger is the apply's own, so a poll that cannot read the tracker logs
// under the identifiers the apply was accepted with.
//
// Every field but done is read and written under Engine.mu. done is closed
// by the drive goroutine once its terminal result is published, so a cancel
// can wait for the outcome the drive settles on instead of guessing it.
type trackedApply struct {
	result  *engine.ProgressResult
	tracker buildTracker
	logger  *slog.Logger
	// cancellableBuild records the shape the apply was admitted as: an
	// index build, concurrent or partition by partition, is the one
	// PostgreSQL change an operator can cancel, so the shape decides the
	// answer before any server is asked.
	cancellableBuild bool
	// haltable records that a shutdown or a drive handing the apply back
	// ends this apply's work rather than waiting for it: an index build or a
	// constraint validation, which can run for hours. It is wider than
	// cancellableBuild, which decides only what an operator can cancel.
	haltable bool
	// validationBound is set for a constraint validation: how long its scan
	// may run before it is cut off.
	validationBound time.Duration
	// partitionedBuild records that the build runs one concurrent build per
	// partition. A cancel signal reaches only the partition build in flight,
	// which can finish before the signal lands, so Cancel also ends the
	// drive's context to keep the next partition from starting.
	partitionedBuild bool
	// percentStep is the executor step the result's precise percent was
	// read from. A percent measures one step's completion, so the
	// never-regress floor holds only while later polls report that step.
	percentStep string
	// cancelApply ends the drive's own context. It is the cancel of last
	// resort: the build backend is signalled through the tracker first, and
	// the context is cancelled only when that signal cannot reach a running
	// statement — the build has not started, has already returned, or its
	// backend cannot be observed or signalled by this role.
	cancelApply context.CancelFunc
	// cancelRequested records that a Cancel may have reached this apply — its
	// signal may have landed on the build backend, or it cancelled the drive's
	// context — so the drive can tell the operator's cancellation from a
	// backend cancellation it did not ask for. Once set it is never reset.
	cancelRequested bool
	// cancelsInFlight counts the Cancel calls that have committed to acting
	// on this apply but have not yet learned whether their signal was sent.
	// A signal can land before the call that sent it hears back, so a drive
	// that sees a cancellation while any is in flight waits for them to
	// settle before it classifies: a backend cancellation SchemaBot did not
	// send is the operator's only if one of those calls may have acted.
	cancelsInFlight int
	// cancelsSettled is closed when cancelsInFlight returns to zero. Cancel
	// replaces it each time the count leaves zero.
	cancelsSettled chan struct{}
	done           chan struct{}
	// owner is the drive the apply was started for (engine.WithWorkOwner).
	owner string
}

type buildTracker interface {
	Progress(context.Context) (progress.Snapshot, error)
	CancelBuild(context.Context) error
}

// DefaultNativeSafeTableSizeLimitBytes preserves the native-safe execution
// ceiling when the server does not configure one.
const DefaultNativeSafeTableSizeLimitBytes = int64(1 << 30)

// DefaultConcurrentIndexMaxDuration bounds one concurrent index build.
const DefaultConcurrentIndexMaxDuration = 24 * time.Hour

// MaxConcurrentIndexMaxDuration is the largest bound the engine accepts for
// one concurrent index build: the largest statement_timeout PostgreSQL
// accepts, in milliseconds. The build runs under that server-side timer set
// to the bound, so a larger value could not be handed to the server; the
// recovery's engine-owned deadline honors the same ceiling. A bound above it
// is not a build anyone waits for; it is the absence of a bound.
const MaxConcurrentIndexMaxDuration = time.Duration(math.MaxInt32) * time.Millisecond

// DefaultValidateMaxDuration bounds one VALIDATE CONSTRAINT scan. The scan
// takes SHARE UPDATE EXCLUSIVE, so reads and writes continue through it, and
// on a large table it is long by design, like a concurrent index build.
const DefaultValidateMaxDuration = 24 * time.Hour

// MinConcurrentIndexMaxDuration is the smallest bound the engine accepts for
// one concurrent index build: statement_timeout is an integer millisecond
// count, so anything shorter rounds to zero on the server, which disables
// the timer instead of tightening it. The executor refuses such a budget as
// unbounded; the engine never hands it one.
const MinConcurrentIndexMaxDuration = time.Millisecond

// MinValidateMaxDuration is the smallest bound the engine accepts for one
// VALIDATE CONSTRAINT scan. The validation's lock wait gets half its bound
// when the bound is shorter than the lock budget, and that half has to be a
// whole millisecond for the server to honor it.
const MinValidateMaxDuration = 2 * time.Millisecond

// New creates a new PostgreSQL engine.
func New() *Engine {
	return NewWithTableSizeLimit(DefaultNativeSafeTableSizeLimitBytes)
}

// NewWithTableSizeLimit creates a PostgreSQL engine with the native-safe
// table size ceiling expressed in bytes. Zero means unset and adopts
// DefaultNativeSafeTableSizeLimitBytes. A negative value is kept as-is
// rather than silently replaced with a ceiling the caller did not choose:
// the plan-time preflight check rejects a non-positive limit before apply,
// and server config validation rejects it at startup.
func NewWithTableSizeLimit(tableSizeLimit int64) *Engine {
	return NewWithOptions(tableSizeLimit, DefaultConcurrentIndexMaxDuration)
}

// NewWithOptions creates a PostgreSQL engine with its process-wide apply
// bounds. A concurrentIndexMaxDuration that is not positive adopts
// DefaultConcurrentIndexMaxDuration: unlike the table size limit, no later
// check refuses a negative bound, and a deadline already in the past would
// end every concurrent build the instant it started. One above
// MaxConcurrentIndexMaxDuration is clamped to it, and a positive one below
// MinConcurrentIndexMaxDuration is raised to it, so the bound the engine
// serves is always one the server's timer can hold. Server config validation
// refuses all three before they reach this constructor; the normalization
// here covers embedders that build the engine directly.
func NewWithOptions(tableSizeLimit int64, concurrentIndexMaxDuration time.Duration) *Engine {
	if tableSizeLimit == 0 {
		tableSizeLimit = DefaultNativeSafeTableSizeLimitBytes
	}
	if concurrentIndexMaxDuration <= 0 {
		concurrentIndexMaxDuration = DefaultConcurrentIndexMaxDuration
	}
	if concurrentIndexMaxDuration < MinConcurrentIndexMaxDuration {
		concurrentIndexMaxDuration = MinConcurrentIndexMaxDuration
	}
	if concurrentIndexMaxDuration > MaxConcurrentIndexMaxDuration {
		concurrentIndexMaxDuration = MaxConcurrentIndexMaxDuration
	}
	return &Engine{
		tableSizeLimit:             tableSizeLimit,
		optimisticApplyCeiling:     optimisticApplyCeiling,
		concurrentIndexMaxDuration: concurrentIndexMaxDuration,
		validateMaxDuration:        DefaultValidateMaxDuration,
	}
}

// WithValidateMaxDuration sets the bound one VALIDATE CONSTRAINT scan runs
// under. It is normalized as NewWithOptions normalizes the concurrent index
// bound: a bound that is not positive adopts DefaultValidateMaxDuration, and
// one outside what the server's statement_timeout can hold is clamped into
// range. Server config validation refuses those values before they reach
// here.
func (e *Engine) WithValidateMaxDuration(d time.Duration) *Engine {
	switch {
	case d <= 0:
		d = DefaultValidateMaxDuration
	case d < MinValidateMaxDuration:
		d = MinValidateMaxDuration
	case d > MaxConcurrentIndexMaxDuration:
		d = MaxConcurrentIndexMaxDuration
	}
	e.validateMaxDuration = d
	return e
}

// NewForTarget creates a PostgreSQL engine with the target information needed
// by capabilities whose request does not carry resolved credentials.
func NewForTarget(tableSizeLimit int64, concurrentIndexMaxDuration time.Duration, database string, credentials *engine.Credentials) *Engine {
	e := NewWithOptions(tableSizeLimit, concurrentIndexMaxDuration)
	e.pullDatabase = database
	e.pullCredentials = credentials
	return e
}

// WithTableOwner configures the PostgreSQL role used for greenfield table
// creation. Empty preserves creation as the connected role.
func (e *Engine) WithTableOwner(owner string) *Engine {
	e.tableOwner = owner
	return e
}

// TableSizeLimit exposes the native-safe ceiling for wiring verification and observability.
func (e *Engine) TableSizeLimit() int64 {
	return e.tableSizeLimit
}

// ConcurrentIndexMaxDuration exposes the bound one concurrent index build
// runs under, for wiring verification and observability.
func (e *Engine) ConcurrentIndexMaxDuration() time.Duration {
	return e.concurrentIndexMaxDuration
}

// ValidateMaxDuration exposes the bound one VALIDATE CONSTRAINT scan runs
// under, for wiring verification and observability.
func (e *Engine) ValidateMaxDuration() time.Duration {
	return e.validateMaxDuration
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
	if err := engine.RefuseDropTables("PostgreSQL", req.DropTables); err != nil {
		return nil, &engine.PlanRefusedError{Err: fmt.Errorf("plan PostgreSQL database %q: %w", req.Database, err)}
	}

	pool, closePool, err := openTargetPool(ctx, req.Credentials)
	if err != nil {
		return nil, fmt.Errorf("plan PostgreSQL database %q: %w", req.Database, err)
	}
	defer closePool()
	return planSchemas(ctx, pool, req, e.tableSizeLimit, e.tableOwner)
}

// openTargetPool opens the pg-sprite pool one engine operation runs on.
// Before it, it verifies the target through the SchemaBot-managed connection
// path, including its transport policy, and adapts the same normalized DSN to
// pg-sprite's pool API. Errors do not name the database or the operation;
// the caller wraps them with both. The returned close releases the pool and
// the validation connection.
func openTargetPool(ctx context.Context, creds *engine.Credentials) (*pgxpool.Pool, func(), error) {
	caPath, err := caCertPath(creds)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve PostgreSQL CA certificate: %w", err)
	}
	validationOpts, err := validationRootCAs(caPath)
	if err != nil {
		return nil, nil, fmt.Errorf("load PostgreSQL validation root CAs: %w", err)
	}
	db, err := postgresconn.Open(creds.DSN, validationOpts...)
	if err != nil {
		return nil, nil, fmt.Errorf("open PostgreSQL connection: %w", err)
	}
	// Open only parsed the DSN; this ping is the first dial, so it is where the
	// target can refuse the session and the only error worth classifying.
	if err := db.PingContext(ctx); err != nil {
		utils.CloseAndLog(db)
		return nil, nil, fmt.Errorf("ping PostgreSQL target: %w", targetauth.Wrap(err))
	}
	poolCfg, err := spritePoolConfig(creds.DSN, caPath)
	if err != nil {
		utils.CloseAndLog(db)
		return nil, nil, fmt.Errorf("build pg-sprite pool config: %w", err)
	}
	pool, err := dbconn.NewPool(ctx, poolCfg)
	if err != nil {
		utils.CloseAndLog(db)
		return nil, nil, fmt.Errorf("open pg-sprite pool: %w", err)
	}
	return pool, func() {
		pool.Close()
		utils.CloseAndLog(db)
	}, nil
}

func planSchemas(ctx context.Context, pool *pgxpool.Pool, req *engine.PlanRequest, tableSizeLimit int64, tableOwner string) (*engine.PlanResult, error) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	if err != nil {
		return nil, fmt.Errorf("select PostgreSQL statement parser: %w", err)
	}

	namespaces := sortedKeys(req.SchemaFiles)
	ignored, err := engine.NewIgnoredTables(req.IgnoreTables)
	if err != nil {
		return nil, fmt.Errorf("plan PostgreSQL database %q: %w", req.Database, err)
	}
	result := &engine.PlanResult{}
	for _, namespace := range namespaces {
		ns := req.SchemaFiles[namespace]
		if ns == nil {
			return nil, fmt.Errorf("plan PostgreSQL namespace %q: schema files are required", namespace)
		}
		parsed, err := parseDesiredFiles(namespace, ns.Files)
		if err != nil {
			return nil, err
		}
		if err := refuseTableDeclaredTwice(namespace, parsed); err != nil {
			return nil, err
		}
		if err := refuseEnumTypeDeclaredTwice(namespace, parsed); err != nil {
			return nil, err
		}
		schemaChange := engine.SchemaChange{Namespace: namespace}
		files := sortedKeys(ns.Files)
		desiredTables := make(map[string]bool, len(files))
		// rowSecurityTables are the tables whose file declares row security,
		// so the rollback baseline declares their live row security too, even
		// when the live table has none.
		rowSecurityTables := make(map[string]bool)
		// Enum label additions lead the namespace's changes, whichever file
		// sorts first. The apply runs the changes one at a time in this
		// order, each committed before the next starts, so every label the
		// plan adds exists before any table change in the apply runs. A
		// table change that names a new label still fails its own plan,
		// since the table is planned against the live type.
		var enumChanges []engine.TableChange
		for _, filename := range files {
			file := parsed[filename]
			// An enum type is not a table: it stays out of desiredTables,
			// so it never counts against the undeclared-table drops, the
			// ignore list, or the rollback baseline.
			if enumType, ok := file.EnumType(); ok {
				changes, err := planEnumType(ctx, pool, req.Database, namespace, enumType, parser)
				if err != nil {
					return nil, plannerRefusal(fmt.Errorf("plan PostgreSQL schema in %q/%q: %w", namespace, filename, err))
				}
				enumChanges = append(enumChanges, changes...)
				continue
			}
			var report pgplan.Report
			var table string
			if rowSecurity, ok := file.RowSecurity(); ok {
				rowSecurityTables[rowSecurity.Table()] = true
				// A file whose row security matches the live table plans its
				// table definition like any other file, below. A row security
				// delta, or a table that does not exist yet, is one reviewed
				// operation of its own.
				tableReport, matched, err := planRowSecurityTableDefinition(ctx, pool, namespace, rowSecurity)
				if err != nil {
					return nil, plannerRefusal(fmt.Errorf("plan PostgreSQL schema in %q/%q: %w", namespace, filename, err))
				}
				if !matched {
					rlsChanges, err := planRowSecurityOperation(ctx, pool, namespace, rowSecurity)
					if err != nil {
						return nil, plannerRefusal(fmt.Errorf("plan PostgreSQL schema in %q/%q: %w", namespace, filename, err))
					}
					desiredTables[file.Table()] = true
					schemaChange.TableChanges = append(schemaChange.TableChanges, rlsChanges...)
					continue
				}
				report, table = tableReport, rowSecurity.Table()
			} else {
				desired, ok := file.Schema()
				if !ok {
					return nil, fmt.Errorf("plan PostgreSQL schema in %q/%q: the parse admitted no planner input", namespace, filename)
				}
				report, table, err = planPostgresDefinition(ctx, pool, namespace, desired)
				if err != nil {
					return nil, plannerRefusal(fmt.Errorf("plan PostgreSQL schema in %q/%q: %w", namespace, filename, err))
				}
			}
			desiredTables[table] = true
			crossNamespace, err := crossNamespaceForeignKeysForDrops(ctx, pool, namespace, report)
			if err != nil {
				return nil, err
			}
			changes, tiers, unrecognized, err := tableChanges(report, crossNamespace, parser)
			if err != nil {
				return nil, fmt.Errorf("render PostgreSQL plan for table %q in namespace %q: %w", table, namespace, err)
			}
			for _, vocabulary := range unrecognized {
				// The plan renders a blocked placeholder for the statement,
				// so the operator is safe but uninformed; the log is where
				// triage learns the planner's vocabulary grew, for whose
				// schema file, and which value to map next.
				slog.Warn("PostgreSQL planner returned vocabulary SchemaBot does not recognize; the plan blocks the statement with a placeholder verdict",
					"database", req.Database,
					"namespace", namespace,
					"table", table,
					"vocabulary", vocabulary.kind,
					"value", vocabulary.value)
			}
			changes, err = blockMissingPrivileges(ctx, pool, req.Database, report, changes, tiers, tableOwner)
			if err != nil {
				return nil, fmt.Errorf("verify privileges for table %q in namespace %q: %w", table, namespace, err)
			}
			changes, err = blockOversizedTable(ctx, pool, req.Database, report, changes, tableSizeLimit)
			if err != nil {
				return nil, fmt.Errorf("verify size for table %q in namespace %q: %w", table, namespace, err)
			}
			schemaChange.TableChanges = append(schemaChange.TableChanges, changes...)
		}
		schemaChange.TableChanges = append(enumChanges, schemaChange.TableChanges...)
		// A table the config withholds that a schema file also declares is a
		// contradiction: the declaring file would keep managing the table while
		// the config says to leave it alone.
		if err := ignored.RefuseDeclared(namespace, sortedKeys(desiredTables)); err != nil {
			return nil, &engine.PlanRefusedError{Err: err}
		}
		drops, exempt, withheld, err := undeclaredTableDrops(ctx, pool, req.Database, namespace, desiredTables, ignored, parser)
		if err != nil {
			return nil, fmt.Errorf("compare live tables against schema files in namespace %q: %w", namespace, err)
		}
		schemaChange.TableChanges = append(schemaChange.TableChanges, drops...)
		if len(exempt) > 0 {
			slices.Sort(exempt)
			result.ExemptTables = append(result.ExemptTables, &engine.ExemptTables{
				Namespace: namespace,
				Tables:    exempt,
				Reason:    engine.ExemptReasonArchiveNaming,
			})
		}
		if exemption := ignored.Exemption(namespace, withheld); exemption != nil {
			result.ExemptTables = append(result.ExemptTables, exemption)
		}
		if len(schemaChange.TableChanges) > 0 {
			// Only a namespace with changes needs a rollback baseline, so
			// the render is paid once per changed namespace rather than for
			// every namespace the schema files declare.
			schemaChange.OriginalFiles, schemaChange.RollbackBlockers, err = captureOriginalFiles(ctx, pool, req.Database, namespace, desiredTables, rowSecurityTables, ignored)
			schemaChange.OriginalFilesCaptured = len(schemaChange.RollbackBlockers) == 0
			if err != nil {
				return nil, err
			}
			result.Changes = append(result.Changes, schemaChange)
		}
	}
	sizeDroppedTables(ctx, pool, req.Database, result.Changes)
	result.NoChanges = len(result.Changes) == 0
	result.PlanID = engine.NewPlanID()
	return result, nil
}

// refuseTableDeclaredTwice fails the plan when two schema files in one
// namespace declare the same table. Each file is diffed against the live table
// on its own, so a table declared twice would get two contradictory diffs,
// each dropping what only the other file declares; there is no single desired
// definition to review. The error names the table and both files so the
// operator knows which one to remove. It runs before any file is planned, on
// the same admitted files the plan uses, and applies the same rule every
// planner does (ddl.TableDeclarations), so a duplicate declaration reads the
// same whichever engine refuses it. An enum types file declares no table and
// is checked by refuseEnumTypeDeclaredTwice instead.
func refuseTableDeclaredTwice(namespace string, files map[string]pgstatement.DesiredFile) error {
	var declared ddl.TableDeclarations
	for _, filename := range sortedKeys(files) {
		if _, ok := files[filename].EnumType(); ok {
			continue
		}
		if err := declared.Declare(filename, files[filename].Table()); err != nil {
			return &engine.PlanRefusedError{Err: fmt.Errorf("plan PostgreSQL namespace %q: %w", namespace, err)}
		}
	}
	return nil
}

// captureOriginalFiles renders the live namespace as the plan's rollback
// baseline, keyed by schema file name. The baseline is every managed table
// in the namespace except ignored tables and undeclared archives. Declared
// archives keep their originals. Exclusions are applied before introspection,
// so only tables the forward plan manages decide capture completeness. It is
// complete or it is nothing, and the two ways it can fall short end
// differently. A table the engine read but pg-sprite's renderer refuses —
// each shape it refuses is named by one of its ErrUnrenderable errors,
// inheritance among them — or that a foreign key on an unmanaged table or
// in another namespace references, is returned as a blocker in place of the
// files, and RV-7 then refuses rollback for this plan rather than
// reconstructing the originals. The plan itself still proceeds, because
// rollback capability is not a precondition for reviewing or applying the
// change, and the blockers let the plan say why it cannot be rolled back
// before anyone applies it. A table the engine could not read — the listing named it and
// introspection cannot find or resolve it, a catalog query fails, the
// context is cancelled — is an error that ends the plan, whether it is one
// table's read or the namespace's: a catalog that could not be read
// consistently is not one to plan against, and a plan interrupted
// mid-capture is never recorded as rollback-incapable.
//
// The render introspects every managed table in the namespace, changed or
// not, so its cost grows with the namespace rather than with the change; the
// introspections run concurrently within the pool's ceiling, so the wall
// time grows more slowly than the table count does.
func captureOriginalFiles(ctx context.Context, pool *pgxpool.Pool, database, namespace string, declared, rowSecurityDeclared map[string]bool, ignored engine.IgnoredTables) (files map[string]string, blockers []engine.RollbackBlocker, err error) {
	originalTables, renderErrors, err := renderPostgresTables(ctx, pool, database, namespace, rollbackBaseline(declared, rowSecurityDeclared, ignored))
	if err != nil {
		return nil, nil, fmt.Errorf("capture original PostgreSQL schema in namespace %q: %w", namespace, err)
	}
	if len(renderErrors) > 0 {
		refused, err := skippedTables(renderErrors)
		if err != nil {
			return nil, nil, fmt.Errorf("capture original PostgreSQL schema in namespace %q: %w", namespace, err)
		}
		blockers = make([]engine.RollbackBlocker, 0, len(refused))
		for _, table := range refused {
			blockers = append(blockers, engine.RollbackBlocker{Table: table.GetTable(), Reason: table.GetReason()})
		}
		// One representative cause is enough to start triage; the count says
		// how far the namespace is from a complete baseline without the log
		// line growing with the number of unrenderable tables.
		slog.Warn("PostgreSQL plan could not capture the whole namespace as rollback originals; the plan proceeds rollback-incapable",
			"database", database,
			"namespace", namespace,
			"unrenderable_tables", len(renderErrors),
			"first_error", renderErrors[0])
		return nil, blockers, nil
	}
	files = make(map[string]string, len(originalTables))
	for table, content := range originalTables {
		files[table+".sql"] = content
	}
	return files, nil, nil
}

// tierUnderived marks a step whose privilege tier could not be derived: its
// statement is a shape the engine does not execute, so the step is blocked
// and there is no access to probe on its behalf. It sits below preflight's
// lowest rung so it can never match a checked tier.
const tierUnderived preflight.Tier = -1

// tableChanges renders the report's statements as planned table changes and
// derives each step's privilege tier alongside them. The returned slices are
// parallel: tiers[i] is the access changes[i] needs from the engine role,
// including when an earlier verdict already blocked that step, so a later
// gate can name the grant a blocked step will also need; a blocked step whose
// shape has no tier carries tierUnderived. unrecognized carries one entry per
// statement whose verdict is a placeholder for planner vocabulary this build
// does not map, for the caller to log with the plan's identifiers.
// crossNamespaceForeignKeysForDrops reads the live table's foreign keys into
// another namespace when the plan drops a foreign key, so the drop's remedy
// can name them; a plan with no dropped foreign key never reads the catalog.
// The live table must exist, since only a live constraint can be dropped, so
// a missing row is an error rather than a table without foreign keys.
func crossNamespaceForeignKeysForDrops(ctx context.Context, pool *pgxpool.Pool, namespace string, report pgplan.Report) ([]string, error) {
	if !slices.ContainsFunc(report.Statements, func(s pgplan.Statement) bool { return s.Kind == schemadiff.ChangeDropForeignKey }) {
		return nil, nil
	}
	keys, err := foreignKeysFor(ctx, pool, namespace, []string{report.Table})
	if err != nil {
		return nil, err
	}
	if len(keys) != 1 {
		return nil, fmt.Errorf("table %q in namespace %q left the catalog while the plan was reading its foreign keys; retry the plan", report.Table, namespace)
	}
	return keys[0].crossNamespaceForeignKeys, nil
}

// tableChanges maps a table's planned statements to the changes the operator
// reviews. crossNamespaceForeignKeys names the live table's foreign keys into
// another namespace, which a schema file cannot declare, so a dropped foreign
// key's remedy names them only when the table has any.
func tableChanges(report pgplan.Report, crossNamespaceForeignKeys []string, parser ddl.StatementParser) ([]engine.TableChange, []preflight.Tier, []unrecognizedPlannerVocabulary, error) {
	// Each statement's verdict is derived once and reused for both the
	// greenfield decision and the rendering, so a placeholder verdict is
	// reported once per statement.
	verdicts := make([]stepVerdict, len(report.Statements))
	var unrecognized []unrecognizedPlannerVocabulary
	for i, statement := range report.Statements {
		verdicts[i] = executionVerdict(report.FormatVersion, statement, report.Table, crossNamespaceForeignKeys)
		if verdicts[i].unrecognized != nil {
			unrecognized = append(unrecognized, *verdicts[i].unrecognized)
		}
	}
	if isGreenfieldCreateSet(report, verdicts) {
		change, tier, err := greenfieldCreateSet(report, parser)
		if err != nil {
			return nil, nil, nil, err
		}
		return []engine.TableChange{change}, []preflight.Tier{tier}, unrecognized, nil
	}

	changes := make([]engine.TableChange, 0, len(report.Statements))
	tiers := make([]preflight.Tier, 0, len(report.Statements))
	for i, statement := range report.Statements {
		mode, reason := verdicts[i].mode, verdicts[i].reason
		rendered := statement.ExecSQL
		if len(rendered) == 0 {
			rendered = []string{statement.SQL}
		}
		for _, sql := range rendered {
			operation, table, err := parser.Classify(sql)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("classify planned statement for table %q: %w", report.Table, err)
			}
			if table == "" {
				table = report.Table
			}
			stepMode, stepReason := mode, reason
			// The apply path derives a privilege tier for every statement it
			// executes, and that derivation refuses shapes outside the
			// native-safe set. Run the same authority here, for blocked steps
			// too: an executable step whose shape it refuses becomes a blocked
			// verdict that matches what the engine will do, instead of an
			// executable plan that deterministically fails at apply, and a
			// step the planner already blocked keeps the tier the privilege
			// gate needs to name the grant the operator will also need.
			stepTier := tierUnderived
			tier, tierErr := preflight.RequiredTier([]string{sql})
			switch {
			case tierErr == nil:
				stepTier = tier
			case stepMode == "":
				stepMode = engine.ExecutionModeBlocked
				stepReason = fmt.Sprintf("statement for table %q is a shape SchemaBot's PostgreSQL support does not execute yet; rewriting the change cannot make it eligible", table)
			}
			metadata, unrecordedDrop, err := stepDropMetadata(statement, sql)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("record the reviewed drop for table %q: %w", report.Table, err)
			}
			validationMetadata, unrecordedValidation, err := stepValidationMetadata(statement, sql)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("record the reviewed validation for table %q: %w", report.Table, err)
			}
			if validationMetadata != nil {
				if metadata == nil {
					metadata = validationMetadata
				} else {
					maps.Copy(metadata, validationMetadata)
				}
			}
			if unrecordedDrop && stepMode == "" {
				stepMode = engine.ExecutionModeBlocked
				stepReason = fmt.Sprintf("statement for table %q drops an object the planner did not record, so the apply cannot check that it is still the object this plan reviewed; make the change outside SchemaBot", table)
			}
			if unrecordedValidation && stepMode == "" {
				stepMode = engine.ExecutionModeBlocked
				stepReason = fmt.Sprintf("statement for table %q validates a constraint the planner did not record, so the apply cannot check that it is still the constraint this plan reviewed; make the change outside SchemaBot", table)
			}
			// Verdict reasons quote the table name, a planner-sourced
			// identifier, so the reserved cause separator is neutralized
			// before the reason becomes one cause of the verdict.
			changes = append(changes, engine.TableChange{
				Table:         table,
				Operation:     operation,
				DDL:           sql,
				IsUnsafe:      statement.Destructive,
				UnsafeReason:  destructiveReason(statement.Destructive, table),
				ExecutionMode: stepMode,
				ModeReason:    engine.SanitizeBlockedCause(stepReason),
				Metadata:      metadata,
			})
			tiers = append(tiers, stepTier)
		}
	}
	return changes, tiers, unrecognized, nil
}

// stepVerdict is the execution mode and operator-facing reason derived for
// one planned statement; an empty mode means the statement is executable.
// unrecognized is set when the verdict is a placeholder for planner
// vocabulary this build does not map, so the caller can log what moved.
type stepVerdict struct {
	mode         string
	reason       string
	unrecognized *unrecognizedPlannerVocabulary
}

// unrecognizedPlannerVocabulary names a planner value outside the set this
// build renders in its own words: kind says which vocabulary grew (the plan
// contract, a disposition, a create shape cause, or a guidance code) and value
// is the entry the planner returned. The operator sees a blocked placeholder
// either way; this record exists so the server log names the value and the
// schema file behind it.
type unrecognizedPlannerVocabulary struct {
	kind  string
	value string
}

// isGreenfieldCreateSet reports whether the report describes a table that
// does not exist yet and whose every statement is executable, so the table
// and its indexes can ship as one apply unit. A report carrying any verdict
// or destructive step keeps its per-statement rendering so each verdict
// stays visible to the reviewer.
func isGreenfieldCreateSet(report pgplan.Report, verdicts []stepVerdict) bool {
	if !isGreenfieldTable(report) {
		return false
	}
	if len(report.Statements) == 0 {
		return false
	}
	for i, statement := range report.Statements {
		if verdicts[i].mode != "" || statement.Destructive {
			return false
		}
	}
	return true
}

// isGreenfieldTable reports whether the planner proved the table absent.
func isGreenfieldTable(report pgplan.Report) bool {
	return report.TableExists != nil && !*report.TableExists
}

// greenfieldCreateSet renders a greenfield report as a single CREATE TABLE
// change whose DDL carries the table and its indexes in execution order, and
// derives the privilege tier the whole set needs. Every statement is
// re-classified against the planner's own table so a report that is not a
// create set fails closed here rather than at apply time.
func greenfieldCreateSet(report pgplan.Report, parser ddl.StatementParser) (engine.TableChange, preflight.Tier, error) {
	statements := make([]string, len(report.Statements))
	for i, statement := range report.Statements {
		// pg-sprite may render ExecSQL with CONCURRENTLY, which ExecuteCreate
		// refuses for a table born in the run; canonical SQL is deliberate
		// regardless of renderer behavior.
		statements[i] = statement.SQL
	}
	createSet, err := ddl.CreateSetStatements(parser, strings.Join(statements, ";\n"))
	if err != nil {
		return engine.TableChange{}, 0, fmt.Errorf("validate greenfield create set for table %q: %w", report.Table, err)
	}
	_, table, err := parser.Classify(createSet[0])
	if err != nil {
		return engine.TableChange{}, 0, fmt.Errorf("classify greenfield CREATE TABLE for table %q: %w", report.Table, err)
	}
	if table != report.Table {
		return engine.TableChange{}, 0, fmt.Errorf("greenfield create set targets table %q, expected planner table %q", table, report.Table)
	}
	tier, err := preflight.RequiredTier(createSet)
	if err != nil {
		return engine.TableChange{}, 0, fmt.Errorf("derive privilege tier for greenfield table %q: %w", report.Table, err)
	}
	if err := ensureGreenfieldCreateTier(report.Table, tier); err != nil {
		return engine.TableChange{}, 0, err
	}
	return engine.TableChange{
		Table:     report.Table,
		Operation: ddl.StatementCreateTable,
		DDL:       strings.Join(createSet, ";\n"),
	}, tier, nil
}

func ensureGreenfieldCreateTier(table string, tier preflight.Tier) error {
	if tier != preflight.TierCreateTable {
		return fmt.Errorf("greenfield table %q requires tier %s, expected %s", table, tier, preflight.TierCreateTable)
	}
	return nil
}

// blockMissingPrivileges verifies the connected role holds the access each
// step needs, at that step's own tier, so a missing grant surfaces
// on the plan the operator reviews — with the exact provisioning statement —
// instead of failing only after apply is requested. Tiers are checked
// per-step rather than aggregated so a refusal's remediation names only the
// access the blocked step actually needs: an index build's missing schema
// CREATE must not mislabel an in-place ALTER the role can already run. The
// apply path re-runs the same per-statement check before executing, so a
// grant revoked between plan and apply still fails closed there. A privilege
// refusal blocks the steps at its tier; a table-scoped refusal blocks every
// executable step; any other failure fails the plan — an executable plan
// must never be produced while the check's answer is unknown. When every step
// is already blocked, a refusal the operator answers by provisioning — a
// grant, a role, a schema — is appended as an independent cause; any other
// answer leaves the safe verdicts intact and is logged. Reasons come
// from classifyRefusal, so the same failure carries the same detail at plan
// and apply time; here it is prefixed with the statement it blocks. The
// returned slice is the input with verdicts marked.
func blockMissingPrivileges(ctx context.Context, pool *pgxpool.Pool, database string, report pgplan.Report, changes []engine.TableChange, tiers []preflight.Tier, tableOwner string) ([]engine.TableChange, error) {
	checkTier := func(ctx context.Context, pool *pgxpool.Pool, report pgplan.Report, tier preflight.Tier, tableOwner string) error {
		if tier == preflight.TierCreateTable {
			// The off-ladder create tier is checked against the schema, not
			// the table: CheckPrivileges' ladder walks facts about an
			// existing table, and a greenfield target has none.
			_, err := preflight.CheckCreatePrivilegesAs(ctx, pool, report.Schema, tableOwner)
			return err
		}
		_, err := preflight.CheckPrivileges(ctx, pool, report.Schema, report.Table, preflight.Requirement{Tier: tier})
		return err
	}
	return blockMissingPrivilegesWithCheck(ctx, pool, database, report, changes, tiers, tableOwner, checkTier)
}

// privilegeTierCheck answers whether the connected role holds one tier's
// access for the report's target; the error is the preflight's own so
// classifyRefusal can render it.
type privilegeTierCheck func(ctx context.Context, pool *pgxpool.Pool, report pgplan.Report, tier preflight.Tier, tableOwner string) error

// blockMissingPrivilegesWithCheck is blockMissingPrivileges with the per-tier
// check injected, so the verdict rewriting can be exercised without a target.
func blockMissingPrivilegesWithCheck(ctx context.Context, pool *pgxpool.Pool, database string, report pgplan.Report, changes []engine.TableChange, tiers []preflight.Tier, tableOwner string, checkTier privilegeTierCheck) ([]engine.TableChange, error) {
	if len(changes) != len(tiers) {
		return nil, fmt.Errorf("verify privileges for table %q: %d planned changes carry %d privilege tiers", report.Table, len(changes), len(tiers))
	}
	fullyBlocked := !hasExecutableChanges(changes)
	// Checked before any verdict rewriting below: blocking dependent steps
	// must never launder a report that carries executable work but names no
	// target into a plan that skips the privilege check entirely.
	if report.Table == "" {
		if fullyBlocked {
			return changes, nil
		}
		return nil, fmt.Errorf("plan report carries executable steps but names no target table")
	}
	if isGreenfieldTable(report) {
		// A privilege probe against a table that provably does not exist can
		// only answer "table not found" — a dead end for the operator. Only
		// the CREATE TABLE step's off-ladder tier states facts an absent
		// target can satisfy; every other executable step depends on the
		// table's creation, so that dependency is its accurate reason.
		blockAbsentTableDependents(changes, tiers, report.Table)
		// Blocking the dependents can leave no executable step: a plan whose
		// CREATE TABLE the planner refused and whose index build was the
		// only executable work is now blocked throughout, and its create
		// step is probed below like any other fully blocked plan's, so the
		// schema grant is named beside the shape refusal here too.
		fullyBlocked = !hasExecutableChanges(changes)
	}
	// Executable steps are always checked. On a plan every gate has already
	// refused, the blocked steps' tiers are checked too, so a grant the
	// operator will also need is named in the same verdict rather than after
	// the planner refusal is fixed.
	greenfield := isGreenfieldTable(report)
	required := make(map[preflight.Tier]bool)
	for i, change := range changes {
		switch {
		case change.ExecutionMode == "":
			if tiers[i] == tierUnderived {
				return nil, fmt.Errorf("verify privileges for table %q: executable statement %q carries no privilege tier", report.Table, change.DDL)
			}
			required[tiers[i]] = true
		case fullyBlocked && change.ExecutionMode == engine.ExecutionModeBlocked && blockedTierIsProbeable(tiers[i], greenfield):
			required[tiers[i]] = true
		}
	}
	if len(required) == 0 {
		return changes, nil
	}
	for _, tier := range slices.Sorted(maps.Keys(required)) {
		err := checkTier(ctx, pool, report, tier, tableOwner)
		if err == nil {
			continue
		}
		r := classifyRefusal(err, report.Table)
		if fullyBlocked {
			// The plan is already safe as it stands, so the probe's answer
			// can only add to the verdicts, never replace them. A refusal
			// that names something to provision — a grant, a role, a
			// schema — is work the operator will have to do anyway, so it
			// is appended beside the earlier cause. Every other answer goes
			// to the log: a refusal about the table itself names nothing to
			// provision on a plan nothing will run, and an operational
			// failure is not a fact about the target at all.
			switch {
			case r != nil && r.provisionable:
				appendBlockedCauseAtTier(changes, tiers, tier, r.detail)
			case r != nil:
				slog.Warn("PostgreSQL privilege check refused the table; existing blocked plan verdicts remain unchanged",
					"database", database,
					"namespace", report.Schema,
					"table", report.Table,
					"refusal", r.reason,
					"detail", r.detail)
			default:
				slog.Warn("PostgreSQL privilege check failed; existing blocked plan verdicts remain unchanged",
					"database", database,
					"namespace", report.Schema,
					"table", report.Table,
					"error", err)
			}
			continue
		}
		if r == nil {
			return nil, fmt.Errorf("check privileges for table %q: %w", report.Table, err)
		}
		var privilegeErr *preflight.PrivilegeError
		if errors.As(err, &privilegeErr) {
			// Missing access is a property of this tier alone: steps at the
			// other tiers keep their own verdicts, from their own checks.
			blockChangesAtTier(changes, tiers, tier, r.detail)
			continue
		}
		// Any other refusal is a property of the table itself — vanished,
		// not an ordinary table, or to be created for an owner the target
		// has no role for — so it holds for every executable step.
		blockExecutableChanges(changes, r.detail)
		return changes, nil
	}
	return changes, nil
}

// blockedTierIsProbeable reports whether a blocked step's tier can be checked
// against the target on a plan every gate has already refused. A step whose
// shape has no tier has no access to probe. On a table the planner proved
// absent, only the CREATE TABLE tier states facts the target can answer:
// probing an absent table for any other tier can only say "table not found",
// which names nothing the operator can provision.
func blockedTierIsProbeable(tier preflight.Tier, greenfield bool) bool {
	if tier == tierUnderived {
		return false
	}
	return !greenfield || tier == preflight.TierCreateTable
}

// blockOversizedTable applies the native-safe table size ceiling to executable
// steps whose native cost scales with the existing table. The preflight
// measures the table and proves it exists and is an ordinary or partitioned
// table; which steps a size refusal blocks is decided in blockRewriteSteps,
// where concurrent index builds keep their verdict because their bound is a
// duration envelope, not the table size. A typed refusal is rendered as a
// blocked verdict; an operational failure fails planning rather than producing
// an executable plan while the table size is unknown. When an earlier gate has
// already blocked every step, an operational failure or a refusal unrelated to
// size leaves those safe verdicts intact and is logged for operator triage.
func blockOversizedTable(ctx context.Context, pool *pgxpool.Pool, database string, report pgplan.Report, changes []engine.TableChange, tableSizeLimit int64) ([]engine.TableChange, error) {
	return blockOversizedTableWithCheck(ctx, pool, database, report, changes, tableSizeLimit, preflight.CheckTable)
}

func blockOversizedTableWithCheck(ctx context.Context, pool *pgxpool.Pool, database string, report pgplan.Report, changes []engine.TableChange, tableSizeLimit int64, checkTable func(context.Context, *pgxpool.Pool, string, string, int64) (preflight.PreflightedTable, error)) ([]engine.TableChange, error) {
	// A table the plan leaves untouched has no step for a size verdict to
	// land on, so it is not charged a catalog round trip.
	if len(changes) == 0 {
		return changes, nil
	}
	// When an earlier gate has already blocked every step the plan is safe as
	// it stands; the size check then runs only so the operator sees both
	// causes at once, and nothing it finds may turn that safe plan into a
	// planning failure.
	fullyBlocked := !hasExecutableChanges(changes)
	if report.Table == "" {
		if fullyBlocked {
			return changes, nil
		}
		return nil, fmt.Errorf("plan report carries executable steps but names no target table")
	}
	if isGreenfieldTable(report) {
		return changes, nil
	}
	proof, err := checkTable(ctx, pool, report.Schema, report.Table, tableSizeLimit)
	if err == nil {
		// The size the ceiling measured is also the plan's display estimate.
		attachTableSize(changes, report.Table, sizeFromPreflight(database, report.Schema, proof))
		return changes, nil
	}
	r := classifyRefusal(err, report.Table)
	if r == nil {
		if fullyBlocked {
			slog.Warn("PostgreSQL table size check failed; existing blocked plan verdicts remain unchanged",
				"database", database,
				"namespace", report.Schema,
				"table", report.Table,
				"error", err)
			return changes, nil
		}
		return nil, fmt.Errorf("check size for table %q: %w", report.Table, err)
	}
	// The table name is a database-sourced identifier. Quoting escapes control
	// characters but not whitespace runs, so the composed reason is collapsed
	// to one line as a whole; the Markdown surfaces that render it escape
	// their own delimiters at the rendering boundary, so the name reaches the
	// operator as the relation it is. The blocked-cause separator is the one
	// delimiter decoded before rendering, so it is neutralized here.
	reason := engine.SanitizeBlockedCause(singleLine(fmt.Sprintf("statement for table %q: %s", report.Table, r.detail)))
	var sizeErr *preflight.SizeError
	if errors.As(err, &sizeErr) {
		if err := blockRewriteSteps(changes, reason, partitionedIndexBuildSteps(report), catalogOnlySteps(report), fullCopySteps(report)); err != nil {
			if fullyBlocked {
				slog.Warn("PostgreSQL table size check could not classify a blocked step; existing blocked plan verdicts remain unchanged",
					"database", database,
					"namespace", report.Schema,
					"table", report.Table,
					"error", err)
				return changes, nil
			}
			return nil, err
		}
		// The refused table is blocked, and its plan still shows the size
		// the ceiling measured.
		attachTableSize(changes, report.Table, sizeFromSizeRefusal(database, report.Schema, report.Table, sizeErr))
		return changes, nil
	}
	if fullyBlocked {
		// A refusal about the table itself, not its size, has no executable
		// step left to land on; the verdicts already carry a cause the
		// operator can act on, so the answer goes to the log instead of
		// being lost.
		slog.Warn("PostgreSQL table size check refused the table; existing blocked plan verdicts remain unchanged",
			"database", database,
			"namespace", report.Schema,
			"table", report.Table,
			"refusal", r.reason,
			"detail", r.detail)
		return changes, nil
	}
	blockExecutableChanges(changes, reason)
	return changes, nil
}

// blockRewriteSteps marks every step whose native cost scales with the existing
// table blocked with the reason. Two index builds keep their verdict, because
// each is bounded by the concurrent index duration envelope rather than the
// table size ceiling: a concurrent index build, and a step the planner routed
// to the partitioned index build, named by partitionedBuilds, which builds one
// partition at a time under that envelope per partition. A concurrent index
// drop keeps its verdict because it reads no rows and rewrites nothing: it
// waits out the table's open transactions and removes the index's files. A
// step pg-sprite proved changes only the catalog, named by catalogOnly, keeps
// its verdict too: it reads and writes no rows, so its cost does not grow
// with the table. A reviewed VALIDATE CONSTRAINT, one carrying the planner's
// record of the constraint it validates, keeps its verdict as well: its scan
// grows with the table but holds SHARE UPDATE EXCLUSIVE, so reads and writes
// continue through it, and it is bounded by the validation duration instead
// of the size ceiling. An earlier blocked verdict retains its reason and gains
// the independent size cause so the operator can resolve both without another
// planning round trip. A step that needs a full copy of the table, named by
// fullCopy, does not: SchemaBot does not run it at any size, so raising the
// limit would not unblock it.
// parser.Classify in tableChanges parsed every rendered statement, including
// blocked steps, with the same single-statement requirements used here. A
// statement that now fails to classify is therefore an invariant violation.
// Every step is classified before any verdict is written, so a caller that
// keeps the plan on that error keeps it exactly as it was rather than with
// the size cause on some steps and not others.
func blockRewriteSteps(changes []engine.TableChange, reason string, partitionedBuilds, catalogOnly, fullCopy map[string]bool) error {
	durationBounded := make([]bool, len(changes))
	indexDrop := make([]bool, len(changes))
	for i := range changes {
		if changes[i].ExecutionMode != "" && changes[i].ExecutionMode != engine.ExecutionModeBlocked {
			continue
		}
		concurrentIndex, err := concurrentIndexStatement(changes[i].DDL)
		if err != nil {
			return fmt.Errorf("classify planned statement for table %q: %w", changes[i].Table, err)
		}
		reviewedValidation, err := reviewedValidationStep(changes[i])
		if err != nil {
			return fmt.Errorf("classify planned statement for table %q: %w", changes[i].Table, err)
		}
		durationBounded[i] = concurrentIndex || partitionedBuilds[changes[i].DDL] || reviewedValidation
		if indexDrop[i], err = concurrentIndexDropStatement(changes[i].DDL); err != nil {
			return fmt.Errorf("classify planned statement for table %q: %w", changes[i].Table, err)
		}
	}
	for i := range changes {
		if changes[i].ExecutionMode != "" && changes[i].ExecutionMode != engine.ExecutionModeBlocked {
			continue
		}
		if durationBounded[i] {
			continue
		}
		if indexDrop[i] {
			continue
		}
		if catalogOnly[changes[i].DDL] {
			continue
		}
		if fullCopyKeepsItsVerdict(changes[i], fullCopy) {
			continue
		}
		if changes[i].ExecutionMode == engine.ExecutionModeBlocked {
			causes := engine.BlockedCauses(changes[i].ModeReason)
			if !slices.Contains(causes, reason) {
				changes[i].ModeReason = engine.JoinBlockedCauses(append(causes, reason))
			}
			continue
		}
		changes[i].ExecutionMode = engine.ExecutionModeBlocked
		changes[i].ModeReason = reason
	}
	return nil
}

// fullCopyKeepsItsVerdict reports whether a step is one the planner routed to
// a full copy and already carries a blocked verdict, which is the only case
// the size cause skips. A full-copy step that reaches the size check without
// one takes the size verdict like any other step, so the exemption never
// leaves a step on an oversized table runnable.
func fullCopyKeepsItsVerdict(change engine.TableChange, fullCopy map[string]bool) bool {
	return fullCopy[change.DDL] && change.ExecutionMode == engine.ExecutionModeBlocked
}

// fullCopySteps returns the planned steps of every statement the planner
// routed to its copy-and-swap backend, keyed by the DDL tableChanges renders
// for them. SchemaBot blocks each of them as needing a full copy of the table.
func fullCopySteps(report pgplan.Report) map[string]bool {
	steps := make(map[string]bool)
	for _, statement := range report.Statements {
		if statement.Backend != router.BackendCopyAndSwap {
			continue
		}
		rendered := statement.ExecSQL
		if len(rendered) == 0 {
			rendered = []string{statement.SQL}
		}
		for _, sql := range rendered {
			steps[sql] = true
		}
	}
	return steps
}

// partitionedIndexBuildSteps returns the planned steps the planner routed to
// the partitioned index build, keyed by the DDL tableChanges renders for them.
func partitionedIndexBuildSteps(report pgplan.Report) map[string]bool {
	steps := make(map[string]bool)
	for _, statement := range report.Statements {
		if statement.Execution != planner.ExecutionPartitionedIndexBuild {
			continue
		}
		for _, sql := range statement.ExecSQL {
			steps[sql] = true
		}
	}
	return steps
}

// catalogOnlySteps returns the planned steps of every statement pg-sprite
// proved changes only the catalog, keyed by the DDL tableChanges renders for
// them. A statement is proven only when every operation in it is
// (planner.ProvenCatalogOnly); one unproven operation leaves all of its steps
// under the size ceiling.
func catalogOnlySteps(report pgplan.Report) map[string]bool {
	steps := make(map[string]bool)
	for _, statement := range report.Statements {
		if !planner.ProvenCatalogOnly(statement.Decisions) {
			continue
		}
		for _, sql := range statement.ExecSQL {
			steps[sql] = true
		}
	}
	return steps
}

// concurrentIndexStatement reports whether sql is a single CREATE INDEX
// CONCURRENTLY statement — the one shape SchemaBot's PostgreSQL support runs
// under the concurrent index duration envelope instead of the table size
// ceiling. Every other concurrent form the parser recognizes, including a
// concurrent partition detach, stays inside the ceiling. The plan and apply
// paths share this classification so the two can never disagree about which
// bound a statement is admitted under.
func concurrentIndexStatement(sql string) (bool, error) {
	statement, err := pgstatement.ParseOne(sql)
	if err != nil {
		return false, fmt.Errorf("parse PostgreSQL statement: %w", err)
	}
	return statement.Kind() == pgstatement.KindCreateIndex && statement.Concurrent(), nil
}

// concurrentIndexDropStatement reports whether sql is a single DROP INDEX
// CONCURRENTLY of one index, the shape the apply runs through pg-sprite's
// concurrent index drop. The plan and apply paths share this classification
// so the two agree on which drops run outside the table size ceiling.
func concurrentIndexDropStatement(sql string) (bool, error) {
	statement, err := pgstatement.ParseOne(sql)
	if err != nil {
		return false, fmt.Errorf("parse PostgreSQL statement: %w", err)
	}
	return statement.Kind() == pgstatement.KindDropIndex && statement.Concurrent() &&
		statement.IndexTarget() == pgstatement.IndexTargetSingleRelation, nil
}

// undeclaredTableDrops surfaces every live table in the namespace that no
// schema file declares, whether its file was deleted or it was never declared
// at all. Desired state is declarative: an undeclared table converges only by
// being dropped, so the plan shows that drop rather than stay silent while the
// table lingers on the target. Each drop is destructive, so the apply runs it
// only with the operator's unsafe acknowledgment, as it does a MySQL table
// drop. The plan records the table's definition with the drop, and the apply
// drops the table only while it still has that definition.
//
// Drops come in the order the apply must run them: a table that another
// dropped table's foreign key references comes after that table. A drop the
// plan cannot make safely is blocked with the reason, after the executable
// ones; planTableDrops says which those are.
//
// Archive tables (<name>_archive_YYYY[_MM[_DD]]) are the one naming
// convention exempt from the drop, as they are in the MySQL engine's view of
// its live schema: an archive is a retired copy kept outside declarative
// schema files. The naming convention is the per-table exemption;
// ignore_namespaces is the per-namespace one. Each exempt table is logged
// and returned on the plan so the PR comment discloses what the plan skipped.
func undeclaredTableDrops(ctx context.Context, pool *pgxpool.Pool, database, namespace string, declared map[string]bool, ignored engine.IgnoredTables, parser ddl.StatementParser) ([]engine.TableChange, []string, []string, error) {
	// ListManagedTables omits partitions, which follow their parent, and
	// extension-owned tables, which belong to their extension: no file can
	// declare either, and the server refuses to drop an extension's table.
	names, err := schemadiff.ListManagedTables(ctx, pool, namespace)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("list declarable tables in namespace %q: %w", namespace, err)
	}
	models := map[string]schemadiff.Model{}
	refused := map[string][]string{}
	var exempt []string
	var withheld []string
	for _, name := range names {
		if declared[name] {
			continue
		}
		// The config's own entry is reported ahead of the naming conventions:
		// an operator who wrote the table down deserves to read their own
		// reason back, not an incidental one the table also happens to match.
		if ignored.Withholds(name) {
			slog.Info("live table withheld from the plan by ignore_tables",
				"database", database,
				"namespace", namespace,
				"table", name)
			withheld = append(withheld, name)
			continue
		}
		if engine.IsArchiveTable(name) {
			slog.Info("PostgreSQL archive table has no schema file and is left in place rather than dropped",
				"database", database,
				"namespace", namespace,
				"table", name)
			exempt = append(exempt, name)
			continue
		}
		m, err := schemadiff.Introspect(ctx, pool, namespace, name)
		if errors.Is(err, schemadiff.ErrTableNotFound) {
			slog.Info("undeclared PostgreSQL table left the catalog while the plan was reading it; there is nothing to drop",
				"database", database,
				"namespace", namespace,
				"table", name)
			continue
		}
		if err != nil {
			return nil, nil, nil, fmt.Errorf("read undeclared table %q: %w", name, err)
		}
		models[name] = m
		if refused[name], err = tableDropRefusals(ctx, pool, namespace, name); err != nil {
			return nil, nil, nil, err
		}
	}
	plan := planTableDrops(namespace, models, refused)
	drops := make([]engine.TableChange, 0, len(models))
	for _, name := range plan.order {
		change, err := undeclaredTableDrop(namespace, name, parser)
		if err != nil {
			return nil, nil, nil, err
		}
		change.Metadata = map[string]string{reviewedTableDropMetadataKey: plan.definitions[name]}
		drops = append(drops, change)
	}
	for _, name := range sortedKeys(plan.blocked) {
		slog.Info("undeclared PostgreSQL table drop is blocked",
			"database", database,
			"namespace", namespace,
			"table", name,
			"reasons", plan.blocked[name])
		change, err := undeclaredTableDrop(namespace, name, parser)
		if err != nil {
			return nil, nil, nil, err
		}
		change.ExecutionMode = engine.ExecutionModeBlocked
		causes := make([]string, len(plan.blocked[name]))
		for i, cause := range plan.blocked[name] {
			causes[i] = sanitizeReasonText(cause)
		}
		change.ModeReason = engine.JoinBlockedCauses(causes)
		drops = append(drops, change)
	}
	return drops, exempt, withheld, nil
}

// undeclaredTableDrop is the unsafe DROP TABLE the plan shows for an
// undeclared table.
func undeclaredTableDrop(namespace, table string, parser ddl.StatementParser) (engine.TableChange, error) {
	sql := parser.Canonicalize("DROP TABLE " + pgx.Identifier{namespace, table}.Sanitize())
	operation, _, err := parser.Classify(sql)
	if err != nil {
		return engine.TableChange{}, fmt.Errorf("classify drop for undeclared table %q: %w", table, err)
	}
	return engine.TableChange{
		Table:        table,
		Operation:    operation,
		DDL:          sql,
		IsUnsafe:     true,
		UnsafeReason: sanitizeReasonText(fmt.Sprintf("DROP TABLE removes all data from table %q", table)),
	}, nil
}

// quoteAll wraps each name in double quotes so a list of identifiers reads
// unambiguously inside a reason sentence.
func quoteAll(names []string) []string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = fmt.Sprintf("%q", name)
	}
	return quoted
}

// tableForeignKeys is one table's foreign key constraint names on each side,
// as the catalog reports them.
type tableForeignKeys struct {
	name                      string
	foreignKeys               []string
	crossNamespaceForeignKeys []string
	referencedBy              []string
}

// foreignKeysFor reads the foreign key constraint names on each side of every
// named table in the namespace.
func foreignKeysFor(ctx context.Context, pool *pgxpool.Pool, namespace string, names []string) ([]tableForeignKeys, error) {
	rows, err := pool.Query(ctx, `
		SELECT c.relname,
		       COALESCE((SELECT pg_catalog.array_agg(con.conname ORDER BY con.conname)
		                 FROM pg_catalog.pg_constraint con
		                 WHERE con.conrelid OPERATOR(pg_catalog.=) c.oid
		                   AND con.contype OPERATOR(pg_catalog.=) 'f'), '{}'),
		       COALESCE((SELECT pg_catalog.array_agg(con.conname ORDER BY con.conname)
		                 FROM pg_catalog.pg_constraint con
		                 JOIN pg_catalog.pg_class ref ON ref.oid OPERATOR(pg_catalog.=) con.confrelid
		                 WHERE con.conrelid OPERATOR(pg_catalog.=) c.oid
		                   AND con.contype OPERATOR(pg_catalog.=) 'f'
		                   AND ref.relnamespace OPERATOR(pg_catalog.<>) c.relnamespace), '{}'),
		       COALESCE((SELECT pg_catalog.array_agg(con.conname ORDER BY con.conname)
		                 FROM pg_catalog.pg_constraint con
		                 WHERE con.confrelid OPERATOR(pg_catalog.=) c.oid
		                   AND con.conrelid OPERATOR(pg_catalog.<>) c.oid
		                   AND con.contype OPERATOR(pg_catalog.=) 'f'), '{}')
		FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) c.relnamespace
		WHERE n.nspname OPERATOR(pg_catalog.=) $1
		  AND c.relname OPERATOR(pg_catalog.=) ANY($2::pg_catalog.text[])`, namespace, names)
	if err != nil {
		return nil, fmt.Errorf("list foreign keys for declarable tables in namespace %q: %w", namespace, err)
	}
	defer rows.Close()
	keys := make([]tableForeignKeys, 0, len(names))
	for rows.Next() {
		var key tableForeignKeys
		if err := rows.Scan(&key.name, &key.foreignKeys, &key.crossNamespaceForeignKeys, &key.referencedBy); err != nil {
			return nil, fmt.Errorf("scan foreign keys for declarable table in namespace %q: %w", namespace, err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read foreign keys for declarable tables in namespace %q: %w", namespace, err)
	}
	return keys, nil
}

func hasExecutableChanges(changes []engine.TableChange) bool {
	return slices.ContainsFunc(changes, func(change engine.TableChange) bool {
		return change.ExecutionMode == ""
	})
}

// blockAbsentTableDependents marks every still-executable step blocked,
// except the CREATE TABLE tier's own step: a privilege probe against a table
// that provably does not exist can only answer "table not found" — a dead end
// for the operator — so each dependent step's accurate reason is its
// dependency on the table's creation. The reason stays neutral about the
// create step's own fate, which the privilege loop has not yet decided.
// Steps already carrying a verdict keep it.
func blockAbsentTableDependents(changes []engine.TableChange, tiers []preflight.Tier, table string) {
	for i := range changes {
		if changes[i].ExecutionMode == "" && tiers[i] != preflight.TierCreateTable {
			changes[i].ExecutionMode = engine.ExecutionModeBlocked
			changes[i].ModeReason = sanitizeReasonText(fmt.Sprintf(
				"table %q does not exist on the target; this statement depends on the statement that creates it — see that statement's verdict", table))
		}
	}
}

// blockExecutableChanges marks every still-executable change blocked with the
// given reason, leaving changes that already carry a verdict untouched.
func blockExecutableChanges(changes []engine.TableChange, reason string) {
	for i := range changes {
		if changes[i].ExecutionMode == "" {
			changes[i].ExecutionMode = engine.ExecutionModeBlocked
			changes[i].ModeReason = reason
		}
	}
}

// blockChangesAtTier marks every still-executable change whose step requires
// the given tier blocked with the reason, leaving every other change — steps
// at other tiers and steps already carrying a verdict — untouched.
func blockChangesAtTier(changes []engine.TableChange, tiers []preflight.Tier, tier preflight.Tier, reason string) {
	for i := range changes {
		if changes[i].ExecutionMode == "" && tiers[i] == tier {
			changes[i].ExecutionMode = engine.ExecutionModeBlocked
			changes[i].ModeReason = reason
		}
	}
}

// appendBlockedCauseAtTier adds an independent cause to every blocked step
// whose statement requires the given tier, leaving steps at other tiers and
// steps that already carry this cause untouched. The step's existing verdict
// stays first: it is the refusal the operator saw before this gate ran.
func appendBlockedCauseAtTier(changes []engine.TableChange, tiers []preflight.Tier, tier preflight.Tier, reason string) {
	for i := range changes {
		if changes[i].ExecutionMode == engine.ExecutionModeBlocked && tiers[i] == tier && !strings.Contains(changes[i].ModeReason, reason) {
			changes[i].ModeReason += clauseSeparator + reason
		}
	}
}

// sanitizeReasonText makes text that embeds database-sourced identifiers safe
// for the single-line Markdown surfaces that render a blocked reason: control
// and format characters (including bidi overrides usable for visual spoofing)
// are stripped, whitespace runs — including newlines — collapse to one space,
// and the table cell separator is neutralized so a crafted identifier cannot
// break comment layout. The blocked-cause separator is neutralized for the
// same reason: an identifier carrying it would otherwise decode as causes the
// engine never issued.
func sanitizeReasonText(s string) string {
	return engine.SanitizeBlockedCause(strings.ReplaceAll(singleLine(s), "|", "/"))
}

// maxStatementMetadataLen bounds the statement text carried in progress
// metadata. The value is stored and rendered alongside other clamped
// operator-facing summaries, and a statement is unbounded input — a create
// set's index definition can run to any length — so it is cut on a rune
// boundary with an ellipsis rather than trusted to fit.
const maxStatementMetadataLen = 255

// sanitizeStatementText prepares the SQL the executor is running for
// progress metadata. Unlike a reason, which is prose SchemaBot composes, a
// statement is quoted back to the operator as the SQL it is: control and
// format characters are stripped and whitespace collapses to one line, but
// the text is otherwise left as written, so `||` stays concatenation and
// `1 | 2` stays a bitwise or. A surface that embeds the value in Markdown
// backslash-escapes the delimiters it cares about at render time, the way
// the comment templates already do for engine-influenced inline text; the
// metadata carries the statement, not one surface's escaping of it.
func sanitizeStatementText(s string) string {
	s = singleLine(s)
	runes := []rune(s)
	if len(runes) > maxStatementMetadataLen {
		return string(runes[:maxStatementMetadataLen-1]) + "…"
	}
	return s
}

// singleLine strips control and format characters and collapses every
// whitespace run — newlines included — to one space, so the result cannot
// span lines or carry a bidi override.
func singleLine(s string) string {
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return ' '
		}
		return r
	}, s)
	return strings.Join(strings.Fields(s), " ")
}

// executionVerdict maps one planned statement to the verdict the operator
// reviews. A planner value outside the vocabulary this mapping knows — a plan
// contract, a disposition, a create shape cause, or a guidance code the planner
// added after the mapping was written — still renders a blocked placeholder,
// and is reported on the verdict so the caller can log it with the
// identifiers this function does not have.
func executionVerdict(formatVersion int, statement pgplan.Statement, table string, crossNamespaceForeignKeys []string) stepVerdict {
	if formatVersion != pgplan.FormatVersion {
		return stepVerdict{
			mode:         engine.ExecutionModeBlocked,
			reason:       fmt.Sprintf("statement for table %q has an unrecognized plan contract", table),
			unrecognized: &unrecognizedPlannerVocabulary{kind: "plan_contract", value: strconv.Itoa(formatVersion)},
		}
	}
	if verdict, ok := executionContractVerdict(statement.Execution, table); !ok {
		return verdict
	}
	if statement.Execution == planner.ExecutionPartitionedIndexBuild {
		if verdict, ok := partitionedIndexShapeVerdict(statement.ExecSQL, table); !ok {
			return verdict
		}
	}
	if statement.Disposition == router.DispositionExecute && statement.Route == planner.RouteNative &&
		statement.Backend == router.BackendNative && len(statement.ExecSQL) > 0 {
		return stepVerdict{}
	}

	// Planner explanations are deliberately not copied to operator-facing
	// text, and neither is the planner's own vocabulary: each known
	// disposition maps to a sentence in SchemaBot's words, since the operator
	// reading it has never heard of the planning library.
	blocked := func(reason string) stepVerdict {
		return stepVerdict{mode: engine.ExecutionModeBlocked, reason: reason}
	}
	switch statement.Disposition {
	case router.DispositionUnavailable:
		if statement.Backend == router.BackendCopyAndSwap {
			return blocked(copyAndSwapRefusalReason(table))
		}
		return blocked(fmt.Sprintf("statement for table %q requires an execution path SchemaBot's PostgreSQL support does not provide yet", table))
	case router.DispositionRewriteRequired:
		if reason, ok := rewriteRequiredReason(statement.Guidance, table); ok {
			return blocked(reason)
		}
		rewrite := blocked(fmt.Sprintf("statement for table %q must be rewritten into a form the engine can execute natively, then re-planned", table))
		// An absent guidance code leaves the generic sentence as the right
		// one; only a code the mapping has no sentence for is vocabulary
		// that moved.
		if statement.Guidance != "" {
			rewrite.unrecognized = &unrecognizedPlannerVocabulary{kind: "guidance", value: string(statement.Guidance)}
		}
		return rewrite
	case router.DispositionRefuse:
		if reason, ok := createShapeRefusalReason(statement.Cause, table); ok {
			return blocked(reason)
		}
		if statement.Cause == "" && statement.Reason == verdict.ReasonDeclarativeForeignKey {
			return blocked(foreignKeyChangeRefusalReason(statement.Kind, table, crossNamespaceForeignKeys))
		}
		if statement.Kind == schemadiff.ChangeSetGeneratedExpression {
			return blocked(generatedExpressionRefusalReason(table))
		}
		if reason, ok := dependentObjectRefusalReason(statement.Decisions, table); ok {
			return blocked(reason)
		}
		refused := blocked(fmt.Sprintf("statement for table %q is refused: it cannot be executed safely as written", table))
		// An absent cause is a refusal from outside the create path and the
		// generic sentence is the right one; only a cause the mapping has no
		// sentence for is vocabulary that moved.
		if statement.Cause != "" {
			refused.unrecognized = &unrecognizedPlannerVocabulary{kind: "create_shape_cause", value: string(statement.Cause)}
		}
		return refused
	case router.DispositionExecute:
		// The planning library carries this change through its shadow-table
		// engine, but SchemaBot does not drive that engine yet, so the
		// statement is blocked in SchemaBot's words rather than reported as
		// vocabulary that moved.
		if statement.Backend == router.BackendCopyAndSwap {
			return blocked(copyAndSwapRefusalReason(table))
		}
		// Execute on anything but the native route with native steps is a
		// route or backend this build does not run, or a plan with nothing
		// to run; the disposition itself is known, so the shape is what the
		// triager needs to see.
		verdict := blocked(fmt.Sprintf("statement for table %q has an unrecognized planner verdict", table))
		verdict.unrecognized = &unrecognizedPlannerVocabulary{
			kind:  "execute_shape",
			value: fmt.Sprintf("route=%s backend=%s steps=%d", statement.Route, statement.Backend, len(statement.ExecSQL)),
		}
		return verdict
	default:
		verdict := blocked(fmt.Sprintf("statement for table %q has an unrecognized planner verdict", table))
		verdict.unrecognized = &unrecognizedPlannerVocabulary{kind: "disposition", value: string(statement.Disposition)}
		return verdict
	}
}

// executionContractVerdict checks the execution contract the planner attached
// to a statement's steps, which says how they must run. SchemaBot runs a
// statement's steps one at a time, each in its own transaction, and it runs a
// partitioned index build's one step through pg-sprite's partition-by-partition
// build, so either contract, or no steps at all, leaves the verdict to the
// disposition and reports ok. A blocking index drop runs only when the operator
// accepts a lock on the whole partition tree, which SchemaBot has no way to
// accept, so it is blocked with its remedy. A contract this build does not know
// is a blocked verdict reported as vocabulary that moved, so the caller logs
// it: running its steps one at a time may not be what the contract asks for.
func executionContractVerdict(execution planner.Execution, table string) (stepVerdict, bool) {
	switch execution {
	case "", planner.ExecutionAutocommit, planner.ExecutionPartitionedIndexBuild:
		return stepVerdict{}, true
	case planner.ExecutionBlockingIndexDrop:
		return stepVerdict{
			mode:   engine.ExecutionModeBlocked,
			reason: blockingIndexDropReason(table),
		}, false
	case planner.ExecutionPartitionedKeyBuild:
		return stepVerdict{
			mode:   engine.ExecutionModeBlocked,
			reason: partitionedKeyBuildReason(table),
		}, false
	case planner.ExecutionPartitionedForeignKeyBuild:
		return stepVerdict{
			mode:   engine.ExecutionModeBlocked,
			reason: partitionedForeignKeyBuildReason(table),
		}, false
	default:
		return stepVerdict{
			mode:         engine.ExecutionModeBlocked,
			reason:       fmt.Sprintf("statement for table %q has an unrecognized planner verdict", table),
			unrecognized: &unrecognizedPlannerVocabulary{kind: "execution", value: string(execution)},
		}, false
	}
}

// blockingIndexDropReason is the sentence for dropping an index of a
// partitioned table. PostgreSQL cannot drop that index concurrently, so the
// drop blocks reads and writes on the table and every partition until it
// finishes.
func blockingIndexDropReason(table string) string {
	return fmt.Sprintf("statement for table %q drops an index of a partitioned table, which PostgreSQL cannot do without blocking reads and writes on the table and every partition until it finishes; drop the index outside SchemaBot when that lock is acceptable, then re-plan", table)
}

// partitionedKeyBuildReason is the sentence for adding a primary key or
// unique constraint to a partitioned table. Only the engine's partition by
// partition build adds it online, and SchemaBot does not run that build, so
// the step is blocked rather than run as written, which would build every
// partition's index while reads and writes on the whole tree wait.
func partitionedKeyBuildReason(table string) string {
	return fmt.Sprintf("statement for table %q adds a primary key or unique constraint to a partitioned table, which SchemaBot cannot yet add without blocking reads and writes on the table and every partition while each partition's index builds; add the key outside SchemaBot, then re-plan", table)
}

// partitionedForeignKeyBuildReason is the sentence for adding a foreign key
// to a partitioned table on a server that cannot add it NOT VALID. Only the
// engine's partition by partition build adds it online, and SchemaBot does
// not run that build, so the step is blocked rather than run as written,
// which would validate every partition while writes to the whole tree wait.
func partitionedForeignKeyBuildReason(table string) string {
	return fmt.Sprintf("statement for table %q adds a foreign key to a partitioned table, which SchemaBot cannot yet add on this PostgreSQL version without blocking writes to the table and every partition while each partition is checked; add the foreign key outside SchemaBot, then re-plan", table)
}

// partitionedIndexShapeVerdict runs the partitioned index build's own shape
// admission over the planned step, so a statement the build would refuse
// before touching the target, such as one naming a TABLESPACE, is blocked on
// the plan instead of failing at apply. The build takes exactly one statement;
// any other step count means the plan contract moved under this build.
func partitionedIndexShapeVerdict(execSQL []string, table string) (stepVerdict, bool) {
	if len(execSQL) != 1 {
		return stepVerdict{
			mode:         engine.ExecutionModeBlocked,
			reason:       fmt.Sprintf("statement for table %q has an unrecognized planner verdict", table),
			unrecognized: &unrecognizedPlannerVocabulary{kind: "partitioned_index_build_steps", value: strconv.Itoa(len(execSQL))},
		}, false
	}
	err := executor.AdmitPartitionedIndexShape(execSQL[0])
	if err == nil {
		return stepVerdict{}, true
	}
	if r := classifyRefusal(err, table); r != nil {
		return stepVerdict{mode: engine.ExecutionModeBlocked, reason: fmt.Sprintf("statement for table %q: %s", table, r.detail)}, false
	}
	return stepVerdict{
		mode:   engine.ExecutionModeBlocked,
		reason: fmt.Sprintf("statement for table %q builds an index on a partitioned table in a shape SchemaBot's PostgreSQL support does not run", table),
	}, false
}

// createShapeRefusalReason renders the create path's typed refusal cause in
// SchemaBot's words, naming the clause the author wrote and what to do about
// it. The mapping covers the planner's whole closed set of causes, so the
// completeness test over that set fails the day the planner grows a new one.
// Two entries are reachable only through a front door other than this
// engine's: pgstatement.ParseDesired refuses CONCURRENTLY and every statement
// kind other than CREATE TABLE and CREATE INDEX before a plan exists, so
// through planSchemas those shapes fail parsing rather than reaching a verdict.
func createShapeRefusalReason(cause executor.CreateShapeCause, table string) (string, bool) {
	var reason string
	switch cause {
	case executor.CreateShapePartitionOf:
		reason = "declares PARTITION OF against a live parent, which SchemaBot's PostgreSQL create path does not support; create the partition outside SchemaBot or re-model the table"
	case executor.CreateShapeInherits:
		reason = "declares INHERITS against a live parent, which SchemaBot's PostgreSQL create path does not support; create the inheritance relationship outside SchemaBot or model a standalone table"
	case executor.CreateShapeLike:
		reason = "declares LIKE against a live source table, which SchemaBot's PostgreSQL create path does not support; declare the new table's columns and constraints explicitly"
	case executor.CreateShapeOfType:
		reason = "declares OF against a live composite type, which SchemaBot's PostgreSQL create path does not support; declare the new table's columns explicitly"
	case executor.CreateShapeIfNotExists:
		reason = "declares IF NOT EXISTS, which cannot verify that an existing relation has the requested shape; remove the clause and ensure the relation name is available"
	case executor.CreateShapeConcurrently:
		reason = "declares CONCURRENTLY for an index on a new table, which SchemaBot's PostgreSQL create path does not support; remove CONCURRENTLY so the index can be built before the table receives traffic"
	case executor.CreateShapeDuplicateName:
		reason = "declares the same relation name more than once; give every table, index, constraint, and sequence a unique name"
	case executor.CreateShapeMultipleOperations:
		reason = "contains multiple create operations in one statement; split them into separate CREATE TABLE or CREATE INDEX statements"
	case executor.CreateShapeUnsupportedKind:
		reason = "uses a statement kind SchemaBot's PostgreSQL create path does not support; use CREATE TABLE or CREATE INDEX, or create the object outside SchemaBot"
	default:
		return "", false
	}
	return fmt.Sprintf("statement for table %q %s", table, reason), true
}

// rebuildWithoutIntegerKeyReason is the sentence for a change that needs a
// table rebuild on a table without an integer primary key.
const rebuildWithoutIntegerKeyReason = "has to rebuild the table, which needs an integer primary key the table does not have, and SchemaBot's PostgreSQL support does not run table rebuilds yet; make the change outside SchemaBot"

// rewriteRequiredReason renders the planner's typed guidance for a statement
// that must not run as written, in SchemaBot's words, naming what the
// operator does instead. The mapping covers the planner's whole closed set of
// guidance codes, so the completeness test over that set fails the day the
// planner grows a new one.
//
// The wording is SchemaBot's own rather than suggest.ManualPath, because it
// says where each step happens (the schema file, outside SchemaBot, a
// re-plan). It keeps every qualifier ManualPath carries: a remedy that drops
// one has the operator build a different constraint, or take the lock the
// remedy exists to avoid. For the same reason, a NOT VALID constraint is
// committed before it is validated: validating it in the same transaction
// keeps the table locked for the whole scan.
func rewriteRequiredReason(guidance suggest.Guidance, table string) (string, bool) {
	var reason string
	switch guidance {
	case suggest.GuidanceSplitStatement:
		reason = "combines several changes that each need their own safer form; make one of the changes in the schema file, apply it, then make the next"
	case suggest.GuidanceAddColumnThenConstraint:
		reason = "adds a column together with a constraint, which builds or checks the constraint while the table is locked; add the column without the constraint and apply it, then add the constraint with a name as its own change"
	case suggest.GuidancePrevalidatedCheck:
		reason = "attaches a partition, which scans it while the parent table is locked; outside SchemaBot, add a CHECK constraint to the partition that matches its bound as NOT VALID and commit it, then validate it in its own transaction and re-plan, and drop that CHECK once the partition is attached"
	case suggest.GuidanceNotNullScaffold:
		reason = "sets NOT NULL, which scans the table while it is locked; outside SchemaBot, add a CHECK (column IS NOT NULL) constraint as NOT VALID and commit it, then validate it in its own transaction and re-plan"
	case suggest.GuidanceNameConstraintThenValidate:
		reason = "adds a constraint, which checks every row while the table is locked; outside SchemaBot, add the constraint with a name as NOT VALID and commit it, then validate it in its own transaction and re-plan"
	case suggest.GuidanceUniqueIndexThenConstraint:
		reason = "adds a primary key or unique constraint, which builds its index while the table is locked; outside SchemaBot, build a unique index on the same columns with CREATE UNIQUE INDEX CONCURRENTLY, keeping the statement's INCLUDE, NULLS NOT DISTINCT, WITH (...) and TABLESPACE, and attach it with ADD CONSTRAINT ... USING INDEX, keeping any DEFERRABLE and INITIALLY DEFERRED, then re-plan"
	case suggest.GuidanceExpandContractColumn:
		reason = "renames a column, which breaks running application code that still uses the old name; add the new column, have the application write to both and backfill it, switch reads to the new column, then drop the old column as its own change"
	case suggest.GuidanceCoordinatedTableRename:
		reason = "renames a table, which breaks running application code that still uses the old name; run the rename outside SchemaBot together with the application deploy that uses the new name, or keep the old name readable through a view until every instance has moved, then re-plan"
	case suggest.GuidanceCoordinatedConstraintRename:
		reason = "renames an index that backs a constraint, which also renames the constraint and breaks application code that still names the old one; run the rename outside SchemaBot together with the application deploy that stops using the old constraint name, then re-plan"
	case suggest.GuidanceAlterColumnTypeInPlace:
		// The planner does not attach this code to a statement it refuses to
		// run as written; pg-sprite's own run path carries it on its
		// destructive-change refusal. The case keeps the mapping complete
		// for the day the planner attaches it here.
		reason = "drops a column and adds it back, which discards every value the column holds; change the column's type in place with ALTER COLUMN ... TYPE ... USING ..., which converts its values"
	case suggest.GuidanceAddIntegerPrimaryKey:
		// The planner does not attach this code to a statement; pg-sprite's
		// own run path carries it on its refusal of a table rebuild on a
		// table with no primary key. The case keeps the mapping complete for
		// the day the planner attaches it here. Adding the key would not make
		// the change run here, since SchemaBot does not run table rebuilds
		// on PostgreSQL yet, so the sentence names both gaps and sends the
		// change outside SchemaBot rather than to a re-plan that would refuse
		// it again.
		reason = rebuildWithoutIntegerKeyReason
	default:
		return "", false
	}
	return fmt.Sprintf("statement for table %q %s", table, reason), true
}

// foreignKeyChangeRefusalReason renders the planner's refusal of a
// foreign-key drop. The plan shows the drop so it can be reviewed, but the
// engine does not run it, so the operator runs it themselves. A dropped
// foreign key is usually one the file left out rather than one the operator
// means to remove, so the remedy offers declaring it first, which plans no
// change and keeps the constraint. A changed foreign key plans as a drop and
// an add, so it is refused here too. A foreign key into another namespace
// cannot be declared, so when the table carries any, the remedy names them
// and says removal is the only way forward for those.
func foreignKeyChangeRefusalReason(kind schemadiff.ChangeKind, table string, crossNamespaceForeignKeys []string) string {
	const unsupported = "which SchemaBot plans but does not apply on PostgreSQL yet"
	if kind != schemadiff.ChangeDropForeignKey {
		return fmt.Sprintf("statement for table %q changes a foreign key, %s; leave the change out of the schema file to apply the rest of the plan, then run this statement outside SchemaBot and put the change back in the file", table, unsupported)
	}
	reason := fmt.Sprintf("statement for table %q drops a foreign key, %s; to keep the foreign key, declare it in the schema file and re-plan; to remove it, run this statement outside SchemaBot, then re-plan", table, unsupported)
	if len(crossNamespaceForeignKeys) > 0 {
		reason += fmt.Sprintf("; foreign key constraint(s) %s reference another namespace and cannot be declared, so the plan stays blocked until they are removed through a separately reviewed process",
			strings.Join(quoteAll(crossNamespaceForeignKeys), ", "))
	}
	return reason
}

// copyAndSwapRefusalReason refuses a statement only a full copy of the table
// can run. Whether the planning library has that route open or not, SchemaBot
// does not drive it, so both read the same and name the two ways forward.
func copyAndSwapRefusalReason(table string) string {
	return fmt.Sprintf("statement for table %q: the change needs a full copy of the table, which SchemaBot's PostgreSQL support does not run yet; "+
		"reach the same schema without rewriting the table, such as a new column filled in batches, or make this change outside SchemaBot", table)
}

// generatedExpressionRefusalReason renders the planner's refusal of a changed
// stored generated column expression. Replacing the expression recomputes
// every row under a lock that blocks reads and writes, and there is no online
// path for it, so the plan shows the change and nothing runs it. The drift is
// often a live expression the file never caught up with, so the remedy offers
// declaring the live expression first, which plans no change. A blocked step
// blocks the whole plan, and a new expression may use a column the same plan
// adds, so changing it has the rest of the plan applied first.
func generatedExpressionRefusalReason(table string) string {
	return fmt.Sprintf("statement for table %q changes a stored generated column's expression, which rewrites every row under a lock that blocks reads and writes, so SchemaBot does not apply it; to keep the live expression, declare it in the schema file and re-plan; to change it, declare the live expression to apply the rest of the plan first, then run this statement (PostgreSQL 17 or later) outside SchemaBot through a separately reviewed process and put the new expression back in the file", table)
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

// RegistersWorkSynchronously reports that Apply records the accepted schema
// change on this engine before it returns, so there is no window in which the
// engine has accepted work it cannot yet describe. The statement executes in a
// goroutine of this process with nothing to provision first, and the tracked
// progress is claimed under the engine mutex before Apply returns. Because
// progress is tracked per apply, a second apply on the same target cannot
// displace a running one's entry, so the only ways an entry disappears are
// Drain and the retirement of an already-terminal entry — neither of which can
// erase work still in flight. A pending progress report for a task a driver
// believes is in flight is therefore conclusive rather than a phase to wait out.
func (e *Engine) RegistersWorkSynchronously() bool {
	return true
}

// Drain blocks until every background apply goroutine has finished, then stops
// tracking the schema changes it waited out so the next Progress reports the
// idle sentinel.
// Resume and recovery paths call this before re-planning so a statement still
// in flight from a lost lease cannot race the next drive's view of the schema,
// and so the next poll reads a clean engine instead of the previous change's
// terminal snapshot.
//
// Drain waits without a bound, for a caller that owns the engine outright. A
// drive waits through DrainContext instead, so it never outlives its claim.
func (e *Engine) Drain() {
	drained := e.trackedAtDrainStart()
	e.wg.Wait()
	e.clearDrainedProgress(drained)
}

// DrainContext is Drain bounded by ctx. When ctx ends before every apply
// goroutine has finished, it returns an error and leaves the schema changes
// tracked, so the engine still reports them as holding the target.
//
// The snapshot scopes only what a finished drain clears. The wait itself is on
// every apply goroutine, including one accepted after the drain began, so a
// drive draining behind a newly started apply waits for that apply too, up to
// ctx. Giving up does not release the wait: its goroutine stays parked until
// the applies it was waiting on exit, then closes a channel no one reads.
func (e *Engine) DrainContext(ctx context.Context) error {
	drained := e.trackedAtDrainStart()
	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		e.clearDrainedProgress(drained)
		return nil
	case <-ctx.Done():
		return fmt.Errorf("drain PostgreSQL schema changes: still running after %w", ctx.Err())
	}
}

// trackedAtDrainStart snapshots the schema changes a drain is waiting out.
// Only these are the drain's to clear: an apply accepted while the drain waits
// belongs to the drive that started it, and its poller must still find it.
func (e *Engine) trackedAtDrainStart() map[string]*trackedApply {
	e.mu.Lock()
	defer e.mu.Unlock()
	drained := make(map[string]*trackedApply, len(e.progress))
	maps.Copy(drained, e.progress)
	return drained
}

// clearDrainedProgress stops tracking the schema changes the drain started
// with whose apply goroutine has exited. An entry replaced since the snapshot
// is a newer apply and stays, as does one whose goroutine is still running.
func (e *Engine) clearDrainedProgress(drained map[string]*trackedApply) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for key, tracked := range drained {
		if e.progress[key] == tracked && applyGoroutineExited(tracked) {
			delete(e.progress, key)
		}
	}
}

// applyGoroutineExited reports whether the goroutine executing a tracked apply
// has returned. An entry with no goroutine has nothing left to run.
func applyGoroutineExited(tracked *trackedApply) bool {
	if tracked.done == nil {
		return true
	}
	select {
	case <-tracked.done:
		return true
	default:
		return false
	}
}

// HaltForShutdown brings this instance's in-flight concurrent index builds
// and constraint validations down so the process can exit without leaving
// them running on the target under a lease nobody renews. Neither has a
// checkpoint: halting one cancels the statement. The invalid index a build
// leaves is what the next driver to claim the apply meets as abandoned debris
// and recovers before its own build; a cancelled validation rolls back and
// leaves the constraint NOT VALID, still checking new writes, for the next
// driver to validate. Plain DDL is left to finish — its statements are
// bounded by their own lock and statement timeouts, and interrupting a create
// set mid-sequence would leave a committed prefix no later drive can complete.
//
// This is not an operator cancel: no cancel is recorded on the tracked
// applies, so the outcome the drive publishes for halted work is a retryable
// failure, and the apply stays active for another driver to claim. The wait is bounded by ctx so a build that will
// not come down reports that the target may still be held instead of holding
// shutdown open.
func (e *Engine) HaltForShutdown(ctx context.Context) error {
	e.mu.Lock()
	halted := e.haltLongRunningWorkLocked(func(*trackedApply) bool { return true })
	e.mu.Unlock()
	if halted > 0 {
		slog.Info("PostgreSQL engine halting index builds and constraint validations for shutdown", "applies", halted)
	}

	// Wait off the calling goroutine so work that will not come down bounds
	// shutdown at ctx rather than blocking it forever. The wait goroutine ends
	// with the drives it is waiting on.
	settled := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(settled)
	}()
	select {
	case <-settled:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("halt PostgreSQL engine for shutdown: schema change work is still running on the target: %w", ctx.Err())
	}
}

// HaltWorkOwnedBy halts like HaltForShutdown, but only the applies started
// for owner, and waits only for those. Another drive's applies on the same
// engine are left running and are not waited on.
func (e *Engine) HaltWorkOwnedBy(ctx context.Context, owner string) error {
	e.mu.Lock()
	owned := func(tracked *trackedApply) bool { return tracked.owner == owner }
	halted := e.haltLongRunningWorkLocked(owned)
	var pending []chan struct{}
	for _, tracked := range e.progress {
		if owned(tracked) {
			pending = append(pending, tracked.done)
		}
	}
	e.mu.Unlock()
	if halted > 0 {
		slog.Info("PostgreSQL engine halting the index builds and constraint validations a drive started as it returns", "applies", halted)
	}

	for _, done := range pending {
		select {
		case <-done:
		case <-ctx.Done():
			return fmt.Errorf("halt PostgreSQL engine work a drive started: schema change work is still running on the target: %w", ctx.Err())
		}
	}
	return nil
}

// haltLongRunningWorkLocked ends the running index builds and constraint
// validations that selected picks and returns how many it ended. The caller
// holds e.mu.
func (e *Engine) haltLongRunningWorkLocked(selected func(*trackedApply) bool) int {
	halted := 0
	for _, tracked := range e.progress {
		if selected(tracked) && tracked.haltable && !tracked.result.State.IsTerminal() && tracked.cancelApply != nil {
			tracked.cancelApply()
			halted++
		}
	}
	return halted
}

// stopUnsupportedReason is why PostgreSQL declines a stop that reaches the
// engine. The control plane refuses a stop before recording it, with the
// command to use instead, so this is the record for a stop that got past it.
const stopUnsupportedReason = "PostgreSQL can't pause a schema change: an index build has no point to resume from, and other statements commit or fail on their own"

// Stop declines: a concurrent index build has no resumable midpoint, so stop
// would be a permanent cancel under a misleading name. Other PostgreSQL DDL
// also has no engine phase to pause. The typed decline lets the control path
// resolve a durable stop request terminally instead of retrying it.
func (e *Engine) Stop(ctx context.Context, req *engine.ControlRequest) (*engine.ControlResult, error) {
	return nil, engine.NewUnsupportedOperationError(stopUnsupportedReason)
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

// Compile-time check that Engine implements engine.Engine.
var _ engine.Engine = (*Engine)(nil)

// Compile-time check that Engine implements engine.Drainer.
var _ engine.Drainer = (*Engine)(nil)

// Compile-time check that Engine implements engine.ShutdownHalter and
// engine.OwnedWorkHalter.
var _ engine.ShutdownHalter = (*Engine)(nil)
var _ engine.OwnedWorkHalter = (*Engine)(nil)
