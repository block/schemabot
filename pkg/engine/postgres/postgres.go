// Package postgres implements the Engine interface for PostgreSQL databases.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"unicode"

	"github.com/block/pg-sprite/pkg/dbconn"
	"github.com/block/pg-sprite/pkg/diffplan"
	pgplan "github.com/block/pg-sprite/pkg/plan"
	"github.com/block/pg-sprite/pkg/planner"
	"github.com/block/pg-sprite/pkg/preflight"
	"github.com/block/pg-sprite/pkg/router"
	pgstatement "github.com/block/pg-sprite/pkg/statement"
	"github.com/block/spirit/pkg/table"
	"github.com/block/spirit/pkg/utils"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/postgresconn"
	"github.com/block/schemabot/pkg/schema"
)

// Engine implements engine.Engine for PostgreSQL databases.
type Engine struct {
	mu              sync.Mutex
	wg              sync.WaitGroup
	pullDatabase    string
	pullCredentials *engine.Credentials
	// progress holds the latest state of every schema change this engine is
	// tracking, keyed by the apply's identity (its
	// ResumeState.MigrationContext). One engine is shared for the lifetime of a
	// target, so Progress must answer for the apply the caller identifies — and
	// accepting a second apply on the same target must not evict the first
	// one's state while it is still running, or the running apply's driver
	// would be told its work no longer exists.
	progress       map[string]*engine.ProgressResult
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
// table size ceiling expressed in bytes. Zero means unset and adopts
// DefaultNativeSafeTableSizeLimitBytes. A negative value is kept as-is
// rather than silently replaced with a ceiling the caller did not choose:
// the plan-time preflight check rejects a non-positive limit before apply,
// and server config validation rejects it at startup.
func NewWithTableSizeLimit(tableSizeLimit int64) *Engine {
	if tableSizeLimit == 0 {
		tableSizeLimit = DefaultNativeSafeTableSizeLimitBytes
	}
	return &Engine{tableSizeLimit: tableSizeLimit}
}

// NewForTarget creates a PostgreSQL engine with the target information needed
// by capabilities whose request does not carry resolved credentials.
func NewForTarget(tableSizeLimit int64, database string, credentials *engine.Credentials) *Engine {
	e := NewWithTableSizeLimit(tableSizeLimit)
	e.pullDatabase = database
	e.pullCredentials = credentials
	return e
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
	return planSchemas(ctx, pool, req, e.tableSizeLimit)
}

func planSchemas(ctx context.Context, pool *pgxpool.Pool, req *engine.PlanRequest, tableSizeLimit int64) (*engine.PlanResult, error) {
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
		desiredTables := make(map[string]bool, len(files))
		for _, filename := range files {
			desired, err := pgstatement.ParseDesired(ns.Files[filename])
			if err != nil {
				return nil, fmt.Errorf("parse desired PostgreSQL schema in %q/%q: %w", namespace, filename, err)
			}
			desiredTables[desired.Table()] = true
			report, err := diffplan.Plan(ctx, pool, diffplan.Request{Schema: namespace, Desired: desired})
			if err != nil {
				return nil, fmt.Errorf("diff PostgreSQL table %q in namespace %q from file %q: %w", desired.Table(), namespace, filename, err)
			}
			changes, tiers, err := tableChanges(report, parser)
			if err != nil {
				return nil, fmt.Errorf("render PostgreSQL plan for table %q in namespace %q: %w", desired.Table(), namespace, err)
			}
			changes, err = blockMissingPrivileges(ctx, pool, report, changes, tiers)
			if err != nil {
				return nil, fmt.Errorf("verify privileges for table %q in namespace %q: %w", desired.Table(), namespace, err)
			}
			changes, err = blockOversizedTable(ctx, pool, report, changes, tableSizeLimit)
			if err != nil {
				return nil, fmt.Errorf("verify size for table %q in namespace %q: %w", desired.Table(), namespace, err)
			}
			schemaChange.TableChanges = append(schemaChange.TableChanges, changes...)
		}
		drops, err := undeclaredTableDrops(ctx, pool, namespace, desiredTables, parser)
		if err != nil {
			return nil, fmt.Errorf("compare live tables against schema files in namespace %q: %w", namespace, err)
		}
		schemaChange.TableChanges = append(schemaChange.TableChanges, drops...)
		if len(schemaChange.TableChanges) > 0 {
			result.Changes = append(result.Changes, schemaChange)
		}
	}
	result.NoChanges = len(result.Changes) == 0
	result.PlanID = engine.NewPlanID()
	return result, nil
}

// tableChanges renders the report's statements as planned table changes and
// derives each executable step's privilege tier alongside them. The returned
// slices are parallel: tiers[i] is the access changes[i] needs from the
// engine role, and is meaningful only while changes[i] carries no verdict —
// a blocked step never reaches a privilege check.
func tableChanges(report pgplan.Report, parser ddl.StatementParser) ([]engine.TableChange, []preflight.Tier, error) {
	verdicts := make([]string, len(report.Statements))
	for i, statement := range report.Statements {
		verdicts[i], _ = executionVerdict(report.FormatVersion, statement, report.Table)
	}
	if isGreenfieldCreateSet(report, verdicts) {
		change, tier, err := greenfieldCreateSet(report, parser)
		if err != nil {
			return nil, nil, err
		}
		return []engine.TableChange{change}, []preflight.Tier{tier}, nil
	}

	changes := make([]engine.TableChange, 0, len(report.Statements))
	tiers := make([]preflight.Tier, 0, len(report.Statements))
	for _, statement := range report.Statements {
		mode, reason := executionVerdict(report.FormatVersion, statement, report.Table)
		rendered := statement.ExecSQL
		if len(rendered) == 0 {
			rendered = []string{statement.SQL}
		}
		for _, sql := range rendered {
			operation, table, err := parser.Classify(sql)
			if err != nil {
				return nil, nil, fmt.Errorf("classify planned statement for table %q: %w", report.Table, err)
			}
			if table == "" {
				table = report.Table
			}
			stepMode, stepReason := mode, reason
			var stepTier preflight.Tier
			if stepMode == "" {
				// The apply path derives a privilege tier for every statement
				// it executes, and that derivation refuses shapes outside the
				// native-safe set. Run the same authority here so the verdict
				// the operator reviews matches what the engine will do,
				// instead of emitting an executable plan that deterministically
				// fails at apply.
				tier, tierErr := preflight.RequiredTier([]string{sql})
				if tierErr != nil {
					stepMode = engine.ExecutionModeBlocked
					stepReason = fmt.Sprintf("statement for table %q is a shape SchemaBot's PostgreSQL support does not execute yet; rewriting the change cannot make it eligible", table)
				} else {
					stepTier = tier
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
			tiers = append(tiers, stepTier)
		}
	}
	return changes, tiers, nil
}

// isGreenfieldCreateSet reports whether the report describes a table that
// does not exist yet and whose every statement is executable, so the table
// and its indexes can ship as one apply unit. A report carrying any verdict
// or destructive step keeps its per-statement rendering so each verdict
// stays visible to the reviewer.
func isGreenfieldCreateSet(report pgplan.Report, verdicts []string) bool {
	if !isGreenfieldTable(report) {
		return false
	}
	if len(report.Statements) == 0 {
		return false
	}
	for i, statement := range report.Statements {
		if verdicts[i] != "" || statement.Destructive {
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
// executable step needs, at that step's own tier, so a missing grant surfaces
// on the plan the operator reviews — with the exact provisioning statement —
// instead of failing only after apply is requested. Tiers are checked
// per-step rather than aggregated so a refusal's remediation names only the
// access the blocked step actually needs: an index build's missing schema
// CREATE must not mislabel an in-place ALTER the role can already run. The
// apply path re-runs the same per-statement check before executing, so a
// grant revoked between plan and apply still fails closed there. A privilege
// refusal blocks the steps at its tier; a table-scoped refusal blocks every
// executable step; any other failure fails the plan — an executable plan
// must never be produced while the check's answer is unknown. Reasons come
// from classifyRefusal, so the same failure reads identically at plan and
// apply time. The returned slice is the input with verdicts marked.
func blockMissingPrivileges(ctx context.Context, pool *pgxpool.Pool, report pgplan.Report, changes []engine.TableChange, tiers []preflight.Tier) ([]engine.TableChange, error) {
	if len(changes) != len(tiers) {
		return nil, fmt.Errorf("verify privileges for table %q: %d planned changes carry %d privilege tiers", report.Table, len(changes), len(tiers))
	}
	// Checked before any verdict rewriting below: blocking dependent steps
	// must never launder a report that carries executable work but names no
	// target into a plan that skips the privilege check entirely.
	if hasExecutableChanges(changes) && report.Table == "" {
		return nil, fmt.Errorf("plan report carries executable steps but names no target table")
	}
	if isGreenfieldTable(report) {
		// A privilege probe against a table that provably does not exist can
		// only answer "table not found" — a dead end for the operator. Only
		// the CREATE TABLE step's off-ladder tier states facts an absent
		// target can satisfy; every other executable step depends on the
		// table's creation, so that dependency is its accurate reason.
		blockAbsentTableDependents(changes, tiers, report.Table)
	}
	required := make(map[preflight.Tier]bool)
	for i, change := range changes {
		if change.ExecutionMode == "" {
			required[tiers[i]] = true
		}
	}
	if len(required) == 0 {
		return changes, nil
	}
	for _, tier := range slices.Sorted(maps.Keys(required)) {
		var err error
		if tier == preflight.TierCreateTable {
			// The off-ladder create tier is checked against the schema, not
			// the table: CheckPrivileges' ladder walks facts about an
			// existing table, and a greenfield target has none.
			_, err = preflight.CheckCreatePrivileges(ctx, pool, report.Schema)
		} else {
			_, err = preflight.CheckPrivileges(ctx, pool, report.Schema, report.Table, preflight.Requirement{Tier: tier})
		}
		if err == nil {
			continue
		}
		r := classifyRefusal(err, report.Table)
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
		// Any other refusal is a property of the table itself — vanished, or
		// not an ordinary table — so it holds for every executable step.
		blockExecutableChanges(changes, r.detail)
		return changes, nil
	}
	return changes, nil
}

// blockOversizedTable applies the native-safe table size ceiling to every
// still-executable step. The apply path repeats the same CheckTable call, so
// growth between plan and apply cannot bypass the ceiling. A typed refusal is
// rendered as a blocked verdict; an operational failure fails planning rather
// than producing an executable plan while the table size is unknown.
func blockOversizedTable(ctx context.Context, pool *pgxpool.Pool, report pgplan.Report, changes []engine.TableChange, tableSizeLimit int64) ([]engine.TableChange, error) {
	if !hasExecutableChanges(changes) {
		return changes, nil
	}
	if report.Table == "" {
		return nil, fmt.Errorf("plan report carries executable steps but names no target table")
	}
	if isGreenfieldTable(report) {
		return changes, nil
	}
	_, err := preflight.CheckTable(ctx, pool, report.Schema, report.Table, tableSizeLimit)
	if err == nil {
		return changes, nil
	}
	r := classifyRefusal(err, report.Table)
	if r == nil {
		return nil, fmt.Errorf("check size for table %q: %w", report.Table, err)
	}
	blockExecutableChanges(changes, fmt.Sprintf("statement for table %q: %s", report.Table, r.detail))
	return changes, nil
}

// undeclaredTableDrops surfaces every live table in the namespace that no
// schema file declares, whether its file was deleted or it was never declared
// at all. Desired state is declarative: an undeclared table converges only by
// being dropped, so the plan must show that drop rather than stay silent while
// the table lingers on the target. Each drop is both destructive and blocked —
// SchemaBot's PostgreSQL support never executes DROP TABLE — so the operator
// either brings the table under management by declaring it in a schema file
// or removes it through a separately reviewed process; a plan that hid the
// drop would let a merge pass with the target still diverged from the
// repository.
//
// The enumeration follows the conventions the MySQL engine applies to its view
// of the live schema: tables whose names begin with "_" (engine scratch,
// checkpoint, and shadow relations) and archive tables
// (<name>_archive_YYYY[_MM[_DD]]) are maintained outside declarative schema
// files and are not reported. Those naming conventions are the per-table
// exemption; ignore_namespaces is the per-namespace one.
//
// The verdict names only remedies the operator can follow. A table carrying
// foreign key constraints cannot be declared — the declarative format refuses
// foreign keys, and a file omitting them would plan their removal — so its
// reason says the constraints stand in the way instead of pointing at a file
// that cannot be written. The catalog is read directly because the
// namespace's schema may not exist yet, in which case the answer is an empty
// set, not an error.
func undeclaredTableDrops(ctx context.Context, pool *pgxpool.Pool, namespace string, declared map[string]bool, parser ddl.StatementParser) ([]engine.TableChange, error) {
	tables, err := liveTables(ctx, pool, namespace)
	if err != nil {
		return nil, err
	}
	var drops []engine.TableChange
	for _, live := range tables {
		if declared[live.name] {
			continue
		}
		if isConventionallyUnmanagedTable(live.name) {
			continue
		}
		sql := parser.Canonicalize("DROP TABLE " + pgx.Identifier{namespace, live.name}.Sanitize())
		operation, _, err := parser.Classify(sql)
		if err != nil {
			return nil, fmt.Errorf("classify drop for undeclared table %q: %w", live.name, err)
		}
		drops = append(drops, engine.TableChange{
			Table:         live.name,
			Operation:     operation,
			DDL:           sql,
			IsUnsafe:      true,
			UnsafeReason:  sanitizeReasonText(fmt.Sprintf("DROP TABLE removes all data from table %q", live.name)),
			ExecutionMode: engine.ExecutionModeBlocked,
			ModeReason:    sanitizeReasonText(undeclaredTableReason(namespace, live)),
		})
	}
	return drops, nil
}

// isConventionallyUnmanagedTable reports whether a table's name marks it as
// maintained outside declarative schema files: an underscore prefix (engine
// scratch, checkpoint, and shadow relations) or the archive naming convention
// shared with the MySQL engine.
func isConventionallyUnmanagedTable(name string) bool {
	return strings.HasPrefix(name, "_") || table.IsArchiveTable(name)
}

// undeclaredTableReason explains why the drop is blocked and what the
// operator can do about it, tailored to whether the table can be declared.
func undeclaredTableReason(namespace string, live liveTable) string {
	preamble := fmt.Sprintf(
		"table %q exists on the target but no schema file in namespace %q declares it; converging would drop the table, which SchemaBot's PostgreSQL support never executes",
		live.name, namespace)
	if len(live.foreignKeys) == 0 {
		return preamble + " — declare the table in a schema file to keep it under management, or drop it through a separately reviewed process"
	}
	return fmt.Sprintf(
		"%s; the table cannot be declared while it carries foreign key constraint(s) %s, which schema files do not support — drop the table, or remove its foreign keys before declaring it, through a separately reviewed process",
		preamble, strings.Join(quoteAll(live.foreignKeys), ", "))
}

func quoteAll(names []string) []string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = fmt.Sprintf("%q", name)
	}
	return quoted
}

// liveTable is a table the namespace holds on the target, with the foreign
// key constraint names that decide whether a schema file could declare it.
type liveTable struct {
	name        string
	foreignKeys []string
}

// liveTables lists the tables in the namespace that a schema file is expected
// to declare, in name order, as the catalog names them.
//
// Tables whose declaration lives elsewhere are omitted. Partitions and
// inheritance children are declared through their parent: neither has a file
// of its own, so a declared parent's members are not undeclared. Extension-
// owned tables belong to their extension: no file can declare them and the
// server refuses to drop them while the extension is installed, so no operator
// remedy exists for the verdict they would otherwise produce. Unlogged tables
// are ordinary tables with definitions of their own, so they are listed and
// must carry their own file.
//
// The query names every catalog relation and operator with an explicit
// pg_catalog qualification: search_path may list a user schema before
// pg_catalog, and a user relation named pg_class — or a user operator named
// = — would otherwise shadow the catalog and turn a fail-closed enumeration
// into a silent empty set.
func liveTables(ctx context.Context, pool *pgxpool.Pool, namespace string) ([]liveTable, error) {
	rows, err := pool.Query(ctx, `
		SELECT c.relname,
		       COALESCE((SELECT pg_catalog.array_agg(con.conname ORDER BY con.conname)
		                 FROM pg_catalog.pg_constraint con
		                 WHERE con.conrelid OPERATOR(pg_catalog.=) c.oid
		                   AND con.contype OPERATOR(pg_catalog.=) 'f'), '{}')
		FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) c.relnamespace
		WHERE n.nspname OPERATOR(pg_catalog.=) $1
		  AND (c.relkind OPERATOR(pg_catalog.=) 'r' OR c.relkind OPERATOR(pg_catalog.=) 'p')
		  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i
		                  WHERE i.inhrelid OPERATOR(pg_catalog.=) c.oid)
		  AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend d
		                  WHERE d.classid OPERATOR(pg_catalog.=) 'pg_catalog.pg_class'::pg_catalog.regclass
		                    AND d.objid OPERATOR(pg_catalog.=) c.oid
		                    AND d.deptype OPERATOR(pg_catalog.=) 'e')
		ORDER BY c.relname`, namespace)
	if err != nil {
		return nil, fmt.Errorf("list live tables in namespace %q: %w", namespace, err)
	}
	defer rows.Close()
	var tables []liveTable
	for rows.Next() {
		var live liveTable
		if err := rows.Scan(&live.name, &live.foreignKeys); err != nil {
			return nil, fmt.Errorf("scan live table in namespace %q: %w", namespace, err)
		}
		tables = append(tables, live)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate live tables in namespace %q: %w", namespace, err)
	}
	return tables, nil
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

// sanitizeReasonText makes text that embeds database-sourced identifiers safe
// for the single-line Markdown surfaces that render a blocked reason: control
// and format characters (including bidi overrides usable for visual spoofing)
// are stripped, whitespace runs — including newlines — collapse to one space,
// and the table cell separator is neutralized so a crafted identifier cannot
// break comment layout.
func sanitizeReasonText(s string) string {
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return ' '
		}
		return r
	}, s)
	s = strings.Join(strings.Fields(s), " ")
	return strings.ReplaceAll(s, "|", "/")
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
// tracking every schema change so the next Progress reports the idle sentinel.
// Resume and recovery paths call this before re-planning so a statement still
// in flight from a lost lease cannot race the next drive's view of the schema,
// and so the next poll reads a clean engine instead of the previous change's
// terminal snapshot.
func (e *Engine) Drain() {
	e.wg.Wait()
	e.mu.Lock()
	e.progress = nil
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

// Compile-time check that Engine implements engine.Engine.
var _ engine.Engine = (*Engine)(nil)

// Compile-time check that Engine implements engine.Drainer.
var _ engine.Drainer = (*Engine)(nil)
