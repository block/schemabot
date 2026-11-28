package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/block/pg-sprite/pkg/executor"
	pgplan "github.com/block/pg-sprite/pkg/plan"
	"github.com/block/pg-sprite/pkg/planner"
	"github.com/block/pg-sprite/pkg/preflight"
	"github.com/block/pg-sprite/pkg/router"
	"github.com/block/pg-sprite/pkg/schemadiff"
	pgstatement "github.com/block/pg-sprite/pkg/statement"
	"github.com/block/pg-sprite/pkg/suggest"
	"github.com/block/pg-sprite/pkg/verdict"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/engine/enginetest"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
)

// mustIgnoredTables indexes ignore_tables entries the test knows are valid.
func mustIgnoredTables(t *testing.T, entries []string) engine.IgnoredTables {
	t.Helper()
	ignored, err := engine.NewIgnoredTables(entries)
	require.NoError(t, err)
	return ignored
}

func TestBaselinePolicyTableMembership(t *testing.T) {
	declared := map[string]bool{"users": true, "audit_log_archive_2019": true}
	ignored := mustIgnoredTables(t, []string{"flyway_schema_history", "audit_log_archive_2018"})
	tests := []struct {
		table              string
		wantRollbackReason string
	}{
		{table: "users"},
		{table: "legacy_users"},
		{table: "flyway_schema_history", wantRollbackReason: engine.ExemptReasonIgnoreTables},
		{table: "Flyway_schema_history"},
		{table: "audit_log_archive_2019"},
		{table: "audit_log_archive_2020", wantRollbackReason: engine.ExemptReasonArchiveNaming},
		{table: "audit_log_archive_2018", wantRollbackReason: engine.ExemptReasonIgnoreTables},
	}
	for _, tt := range tests {
		t.Run(tt.table, func(t *testing.T) {
			assert.Equal(t, tt.wantRollbackReason, rollbackBaseline(declared, nil, ignored).exclusionReason(tt.table))
			assert.Empty(t, pulledBaseline.exclusionReason(tt.table), "pull exports every enumerated table")
		})
	}
	assert.Equal(t, engine.ExemptReasonIgnoreTables, rollbackBaseline(declared, nil, mustIgnoredTables(t, []string{"audit_log_archive_2019"})).exclusionReason("audit_log_archive_2019"),
		"the exact ignore policy withholds a named table before introspection; the plan separately refuses declared contradictions")
}

func TestExecutionVerdict(t *testing.T) {
	tests := []struct {
		name             string
		version          int
		statement        pgplan.Statement
		wantMode         string
		wantReason       string
		wantUnrecognized *unrecognizedPlannerVocabulary
	}{
		{
			name:             "unrecognized plan contract",
			version:          pgplan.FormatVersion + 1,
			statement:        pgplan.Statement{Disposition: router.DispositionExecute},
			wantMode:         engine.ExecutionModeBlocked,
			wantReason:       `statement for table "users" has an unrecognized plan contract`,
			wantUnrecognized: &unrecognizedPlannerVocabulary{kind: "plan_contract", value: strconv.Itoa(pgplan.FormatVersion + 1)},
		},
		{
			name: "native safe",
			statement: pgplan.Statement{Route: planner.RouteNative, Backend: router.BackendNative,
				Disposition: router.DispositionExecute, ExecSQL: []string{"ALTER TABLE public.users ADD COLUMN email text"}},
		},
		{
			name: "copy and swap the engine does not carry",
			statement: pgplan.Statement{Route: planner.RouteCopyAndSwap, Backend: router.BackendCopyAndSwap,
				Disposition: router.DispositionUnavailable},
			wantMode:   engine.ExecutionModeBlocked,
			wantReason: `statement for table "users": the change needs a full copy of the table, which SchemaBot's PostgreSQL support does not run yet; reach the same schema without rewriting the table, such as a new column filled in batches, or make this change outside SchemaBot`,
		},
		{
			name: "copy and swap the engine carries",
			statement: pgplan.Statement{Route: planner.RouteCopyAndSwap, Backend: router.BackendCopyAndSwap,
				Disposition: router.DispositionExecute},
			wantMode:   engine.ExecutionModeBlocked,
			wantReason: `statement for table "users": the change needs a full copy of the table, which SchemaBot's PostgreSQL support does not run yet; reach the same schema without rewriting the table, such as a new column filled in batches, or make this change outside SchemaBot`,
		},
		{
			name: "copy and swap for a stored generated column",
			statement: pgplan.Statement{Route: planner.RouteCopyAndSwap, Backend: router.BackendCopyAndSwap,
				Disposition: router.DispositionUnavailable, Decisions: []planner.Decision{{Route: planner.RouteCopyAndSwap, Reason: planner.ReasonGeneratedStored}}},
			wantMode:   engine.ExecutionModeBlocked,
			wantReason: `statement for table "users" adds a stored generated column, which computes every row while a lock blocks reads and writes, and SchemaBot's PostgreSQL support does not run that yet; add the column outside SchemaBot through a separately reviewed process, then plan again`,
		},
		{
			name: "unavailable without copy-and-swap backend",
			statement: pgplan.Statement{Route: planner.RouteNative, Backend: router.BackendNative,
				Disposition: router.DispositionUnavailable},
			wantMode:   engine.ExecutionModeBlocked,
			wantReason: `statement for table "users" requires an execution path SchemaBot's PostgreSQL support does not provide yet`,
		},
		{
			name:       "rewrite required",
			statement:  pgplan.Statement{Disposition: router.DispositionRewriteRequired},
			wantMode:   engine.ExecutionModeBlocked,
			wantReason: `statement for table "users" must be rewritten into a form the engine can execute natively, then re-planned`,
		},
		{
			name:       "refuse",
			statement:  pgplan.Statement{Route: planner.RouteRefuse, Disposition: router.DispositionRefuse},
			wantMode:   engine.ExecutionModeBlocked,
			wantReason: `statement for table "users" is refused: it cannot be executed safely as written`,
		},
		{
			name:             "unrecognized disposition",
			statement:        pgplan.Statement{Disposition: router.Disposition("future")},
			wantMode:         engine.ExecutionModeBlocked,
			wantReason:       `statement for table "users" has an unrecognized planner verdict`,
			wantUnrecognized: &unrecognizedPlannerVocabulary{kind: "disposition", value: "future"},
		},
		{
			name: "execute without authoritative steps",
			statement: pgplan.Statement{Route: planner.RouteNative, Backend: router.BackendNative,
				Disposition: router.DispositionExecute},
			wantMode:         engine.ExecutionModeBlocked,
			wantReason:       `statement for table "users" has an unrecognized planner verdict`,
			wantUnrecognized: &unrecognizedPlannerVocabulary{kind: "execute_shape", value: "route=native backend=native steps=0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			version := tt.version
			if version == 0 {
				version = pgplan.FormatVersion
			}
			assert.Equal(t, stepVerdict{
				mode:         tt.wantMode,
				reason:       tt.wantReason,
				unrecognized: tt.wantUnrecognized,
			}, executionVerdict(version, tt.statement, "users", nil))
		})
	}
}

// createShapeRefusalSentences is the operator-facing sentence for every create
// shape cause the planner can report, keyed by the cause so each one is pinned
// to its own text: an author told to remove a clause is told about the clause
// their statement actually has, never a neighbouring cause's remedy. The test
// below derives completeness from the planner's closed set rather than from
// this table, so a cause added upstream fails with the cause's name.
var createShapeRefusalSentences = map[executor.CreateShapeCause]string{
	executor.CreateShapePartitionOf:        `statement for table "users" declares PARTITION OF against a live parent, which SchemaBot's PostgreSQL create path does not support; create the partition outside SchemaBot or re-model the table`,
	executor.CreateShapeInherits:           `statement for table "users" declares INHERITS against a live parent, which SchemaBot's PostgreSQL create path does not support; create the inheritance relationship outside SchemaBot or model a standalone table`,
	executor.CreateShapeLike:               `statement for table "users" declares LIKE against a live source table, which SchemaBot's PostgreSQL create path does not support; declare the new table's columns and constraints explicitly`,
	executor.CreateShapeOfType:             `statement for table "users" declares OF against a live composite type, which SchemaBot's PostgreSQL create path does not support; declare the new table's columns explicitly`,
	executor.CreateShapeIfNotExists:        `statement for table "users" declares IF NOT EXISTS, which cannot verify that an existing relation has the requested shape; remove the clause and ensure the relation name is available`,
	executor.CreateShapeConcurrently:       `statement for table "users" declares CONCURRENTLY for an index on a new table, which SchemaBot's PostgreSQL create path does not support; remove CONCURRENTLY so the index can be built before the table receives traffic`,
	executor.CreateShapeDuplicateName:      `statement for table "users" declares the same relation name more than once; give every table, index, constraint, and sequence a unique name`,
	executor.CreateShapeMultipleOperations: `statement for table "users" contains multiple create operations in one statement; split them into separate CREATE TABLE or CREATE INDEX statements`,
	executor.CreateShapeUnsupportedKind:    `statement for table "users" uses a statement kind SchemaBot's PostgreSQL create path does not support; use CREATE TABLE or CREATE INDEX, or create the object outside SchemaBot`,
}

// Every create shape cause the planner can report renders as the sentence
// written for that cause, through the same verdict path the plan uses, and
// without the placeholder record an unmapped cause would carry.
func TestCreateShapeRefusalReasonCoversEveryCause(t *testing.T) {
	causes := executor.CreateShapeCauses()
	assert.Len(t, createShapeRefusalSentences, len(causes), "golden table carries a cause the planner no longer reports")
	for _, cause := range causes {
		t.Run(string(cause), func(t *testing.T) {
			want, ok := createShapeRefusalSentences[cause]
			require.True(t, ok, "no expected sentence for cause %q", cause)

			got, ok := createShapeRefusalReason(cause, "users")
			assert.True(t, ok)
			assert.Equal(t, want, got)

			assert.Equal(t, stepVerdict{mode: engine.ExecutionModeBlocked, reason: want},
				executionVerdict(pgplan.FormatVersion, pgplan.Statement{
					Disposition: router.DispositionRefuse,
					Cause:       cause,
				}, "users", nil))
		})
	}
}

// A cause this build has no sentence for renders the generic refusal and
// reports the cause for the server log, since the planner's vocabulary grew.
func TestExecutionVerdictUnknownCreateShapeCause(t *testing.T) {
	assert.Equal(t, stepVerdict{
		mode:         engine.ExecutionModeBlocked,
		reason:       `statement for table "users" is refused: it cannot be executed safely as written`,
		unrecognized: &unrecognizedPlannerVocabulary{kind: "create_shape_cause", value: "unknown"},
	}, executionVerdict(pgplan.FormatVersion, pgplan.Statement{
		Disposition: router.DispositionRefuse,
		Cause:       executor.CreateShapeCause("unknown"),
	}, "users", nil))
}

// A refusal with no cause comes from outside the create path; the generic
// sentence is the right one and nothing about the vocabulary has moved.
func TestExecutionVerdictEmptyCreateShapeCause(t *testing.T) {
	assert.Equal(t, stepVerdict{
		mode:   engine.ExecutionModeBlocked,
		reason: `statement for table "users" is refused: it cannot be executed safely as written`,
	}, executionVerdict(pgplan.FormatVersion, pgplan.Statement{
		Disposition: router.DispositionRefuse,
	}, "users", nil))
}

// TestExecutionVerdictRunsPartitionedIndexBuild pins that the planner's
// partitioned index build is an executable step: SchemaBot runs it through
// pg-sprite's partition-by-partition build rather than as the statement.
func TestExecutionVerdictRunsPartitionedIndexBuild(t *testing.T) {
	assert.Equal(t, stepVerdict{}, executionVerdict(pgplan.FormatVersion, pgplan.Statement{
		Disposition: router.DispositionExecute,
		Route:       planner.RouteNative,
		Backend:     router.BackendNative,
		Execution:   planner.ExecutionPartitionedIndexBuild,
		ExecSQL:     []string{"CREATE INDEX events_created_at_idx ON public.events (created_at)"},
	}, "events", nil))
}

// TestExecutionVerdictBlocksPartitionedIndexShapeTheBuildRefuses pins that a
// partitioned index build the executor would refuse at admission is blocked
// on the plan, in the same words the apply would fail with, so the refusal
// gates the apply instead of surfacing after it starts.
func TestExecutionVerdictBlocksPartitionedIndexShapeTheBuildRefuses(t *testing.T) {
	statement := pgplan.Statement{
		Disposition: router.DispositionExecute,
		Route:       planner.RouteNative,
		Backend:     router.BackendNative,
		Execution:   planner.ExecutionPartitionedIndexBuild,
		ExecSQL:     []string{"CREATE INDEX events_created_at_idx ON public.events (created_at) TABLESPACE fast"},
	}
	assert.Equal(t, stepVerdict{
		mode:   engine.ExecutionModeBlocked,
		reason: `statement for table "events": the index on "events" names a TABLESPACE, which the partitions' indexes would not use; remove the TABLESPACE clause from the schema file, then re-plan against the current schema`,
	}, executionVerdict(pgplan.FormatVersion, statement, "events", nil))

	statement.ExecSQL = []string{"CREATE INDEX a_idx ON public.events (a)", "CREATE INDEX b_idx ON public.events (b)"}
	assert.Equal(t, stepVerdict{
		mode:         engine.ExecutionModeBlocked,
		reason:       `statement for table "events" has an unrecognized planner verdict`,
		unrecognized: &unrecognizedPlannerVocabulary{kind: "partitioned_index_build_steps", value: "2"},
	}, executionVerdict(pgplan.FormatVersion, statement, "events", nil))
}

// Every execution contract the planner can attach either runs or is blocked
// with a sentence written for it; only a contract outside the planner's
// closed set is reported as vocabulary that moved.
func TestExecutionContractVerdictCoversEveryExecution(t *testing.T) {
	blocked := map[planner.Execution]string{
		planner.ExecutionBlockingIndexDrop:          blockingIndexDropReason("users"),
		planner.ExecutionPartitionedKeyBuild:        partitionedKeyBuildReason("users"),
		planner.ExecutionPartitionedForeignKeyBuild: partitionedForeignKeyBuildReason("users"),
	}
	for _, execution := range planner.Executions() {
		t.Run(string(execution), func(t *testing.T) {
			verdict, runs := executionContractVerdict(execution, "users")
			reason, isBlocked := blocked[execution]
			if !isBlocked {
				assert.True(t, runs, "a contract the planner names must run or have its own sentence")
				assert.Equal(t, stepVerdict{}, verdict)
				return
			}
			assert.False(t, runs)
			assert.Equal(t, stepVerdict{mode: engine.ExecutionModeBlocked, reason: reason}, verdict)
			assert.Nil(t, verdict.unrecognized, "a contract the planner names is not vocabulary that moved")
		})
	}

	verdict, runs := executionContractVerdict(planner.Execution("unknown"), "users")
	assert.False(t, runs)
	assert.Equal(t, stepVerdict{
		mode:         engine.ExecutionModeBlocked,
		reason:       `statement for table "users" has an unrecognized planner verdict`,
		unrecognized: &unrecognizedPlannerVocabulary{kind: "execution", value: "unknown"},
	}, verdict)
}

// rewriteRequiredSentences is the operator-facing sentence for every guidance
// code the planner can attach to a statement it will not run as written,
// keyed by the code so each one is pinned to its own remedy. The test below
// derives completeness from the planner's closed set rather than from this
// table, so a code added upstream fails with the code's name.
var rewriteRequiredSentences = map[suggest.Guidance]string{
	suggest.GuidanceSplitStatement:              `statement for table "users" combines several changes that each need their own safer form; make one of the changes in the schema file, apply it, then make the next`,
	suggest.GuidanceAddColumnThenConstraint:     `statement for table "users" adds a column together with a constraint, which builds or checks the constraint while the table is locked; add the column without the constraint and apply it, then add the constraint with a name as its own change`,
	suggest.GuidancePrevalidatedCheck:           `statement for table "users" attaches a partition, which scans it while the parent table is locked; outside SchemaBot, add a CHECK constraint to the partition that matches its bound as NOT VALID and commit it, then validate it in its own transaction and re-plan, and drop that CHECK once the partition is attached`,
	suggest.GuidanceNotNullScaffold:             `statement for table "users" sets NOT NULL, which scans the table while it is locked; outside SchemaBot, add a CHECK (column IS NOT NULL) constraint as NOT VALID and commit it, then validate it in its own transaction and re-plan`,
	suggest.GuidanceNameConstraintThenValidate:  `statement for table "users" adds a constraint, which checks every row while the table is locked; outside SchemaBot, add the constraint with a name as NOT VALID and commit it, then validate it in its own transaction and re-plan`,
	suggest.GuidanceUniqueIndexThenConstraint:   `statement for table "users" adds a primary key or unique constraint, which builds its index while the table is locked; outside SchemaBot, build a unique index on the same columns with CREATE UNIQUE INDEX CONCURRENTLY, keeping the statement's INCLUDE, NULLS NOT DISTINCT, WITH (...) and TABLESPACE, and attach it with ADD CONSTRAINT ... USING INDEX, keeping any DEFERRABLE and INITIALLY DEFERRED, then re-plan`,
	suggest.GuidanceExpandContractColumn:        `statement for table "users" renames a column, which breaks running application code that still uses the old name; add the new column, have the application write to both and backfill it, switch reads to the new column, then drop the old column as its own change`,
	suggest.GuidanceCoordinatedTableRename:      `statement for table "users" renames a table, which breaks running application code that still uses the old name; run the rename outside SchemaBot together with the application deploy that uses the new name, or keep the old name readable through a view until every instance has moved, then re-plan`,
	suggest.GuidanceCoordinatedConstraintRename: `statement for table "users" renames an index that backs a constraint, which also renames the constraint and breaks application code that still names the old one; run the rename outside SchemaBot together with the application deploy that stops using the old constraint name, then re-plan`,
	suggest.GuidanceAlterColumnTypeInPlace:      `statement for table "users" drops a column and adds it back, which discards every value the column holds; change the column's type in place with ALTER COLUMN ... TYPE ... USING ..., which converts its values`,
	suggest.GuidanceAddIntegerPrimaryKey:        `statement for table "users" has to rebuild the table, which needs an integer primary key the table does not have, and SchemaBot's PostgreSQL support does not run table rebuilds yet; make the change outside SchemaBot`,
}

// Every guidance code the planner can attach to a statement it will not run
// as written renders as the remedy written for that code, through the same
// verdict path the plan uses, and without the placeholder record an unmapped
// code would carry.
func TestRewriteRequiredReasonCoversEveryGuidance(t *testing.T) {
	guidances := suggest.Guidances()
	assert.Len(t, rewriteRequiredSentences, len(guidances), "golden table carries a guidance code the planner no longer reports")
	for _, guidance := range guidances {
		t.Run(string(guidance), func(t *testing.T) {
			want, ok := rewriteRequiredSentences[guidance]
			require.True(t, ok, "no expected sentence for guidance %q", guidance)

			got, ok := rewriteRequiredReason(guidance, "users")
			assert.True(t, ok)
			assert.Equal(t, want, got)

			assert.Equal(t, stepVerdict{mode: engine.ExecutionModeBlocked, reason: want},
				executionVerdict(pgplan.FormatVersion, pgplan.Statement{
					Disposition: router.DispositionRewriteRequired,
					Guidance:    guidance,
				}, "users", nil))
		})
	}
}

// A guidance code this build has no remedy for renders the generic
// rewrite-required sentence and reports the code for the server log, since
// the planner's vocabulary grew.
func TestExecutionVerdictUnknownGuidance(t *testing.T) {
	assert.Equal(t, stepVerdict{
		mode:         engine.ExecutionModeBlocked,
		reason:       `statement for table "users" must be rewritten into a form the engine can execute natively, then re-planned`,
		unrecognized: &unrecognizedPlannerVocabulary{kind: "guidance", value: "unknown"},
	}, executionVerdict(pgplan.FormatVersion, pgplan.Statement{
		Disposition: router.DispositionRewriteRequired,
		Guidance:    suggest.Guidance("unknown"),
	}, "users", nil))
}

// A foreign-key change the planner refuses is shown for review and the engine
// does not run it. A dropped foreign key's remedy offers declaring it before
// removing it, so an operator whose file left it out keeps the constraint.
// Only a table that carries foreign keys into another namespace gets the
// clause naming them, since those cannot be declared and removal is the only
// way forward. Any other foreign-key change the planner refuses gets the
// general remedy, and nothing is reported as unrecognized vocabulary.
func TestExecutionVerdictForeignKeyChange(t *testing.T) {
	const dropReason = `statement for table "users" drops a foreign key, which SchemaBot plans but does not apply on PostgreSQL yet; to keep the foreign key, declare it in the schema file and re-plan; to remove it, run this statement outside SchemaBot, then re-plan`
	for _, tt := range []struct {
		name           string
		kind           schemadiff.ChangeKind
		crossNamespace []string
		want           string
	}{
		{"another foreign-key change", schemadiff.ChangeAddForeignKey, nil, `statement for table "users" changes a foreign key, which SchemaBot plans but does not apply on PostgreSQL yet; leave the change out of the schema file to apply the rest of the plan, then run this statement outside SchemaBot and put the change back in the file`},
		{"drop", schemadiff.ChangeDropForeignKey, nil, dropReason},
		{"drop with cross-namespace foreign keys", schemadiff.ChangeDropForeignKey, []string{"users_account_id_fkey", "users_region_id_fkey"},
			dropReason + `; foreign key constraint(s) "users_account_id_fkey", "users_region_id_fkey" reference another namespace and cannot be declared, so the plan stays blocked until they are removed through a separately reviewed process`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, stepVerdict{mode: engine.ExecutionModeBlocked, reason: tt.want},
				executionVerdict(pgplan.FormatVersion, pgplan.Statement{
					Kind:        tt.kind,
					Disposition: router.DispositionRefuse,
					Reason:      verdict.ReasonDeclarativeForeignKey,
				}, "users", tt.crossNamespace))
		})
	}
}

// A changed stored generated expression is refused with its own remedy:
// declaring the live expression plans no change, and changing it is a
// rewrite the operator runs outside SchemaBot.
func TestExecutionVerdictGeneratedExpressionChange(t *testing.T) {
	assert.Equal(t, stepVerdict{
		mode:   engine.ExecutionModeBlocked,
		reason: `statement for table "events" changes a stored generated column's expression, which rewrites every row under a lock that blocks reads and writes, so SchemaBot does not apply it; to keep the live expression, declare it in the schema file and re-plan; to change it, declare the live expression to apply the rest of the plan first, then run this statement (PostgreSQL 17 or later) outside SchemaBot through a separately reviewed process and put the new expression back in the file`,
	}, executionVerdict(pgplan.FormatVersion, pgplan.Statement{
		Kind:        schemadiff.ChangeSetGeneratedExpression,
		Disposition: router.DispositionRefuse,
		Reason:      verdict.ReasonUnsupportedStatement,
	}, "events", nil))
}

// A foreign key the planner cannot bind fails the plan with the fix appended:
// the referenced table must already exist on the target, and the planning
// role must be able to read and reference it. The remedy matches through wrapping, and any
// other failure gets none.
func TestReferencedTableRemedy(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want string
	}{
		{"not found", fmt.Errorf("plan: %w: public.customers", schemadiff.ErrReferencedTableNotFound),
			"; a foreign key can reference only a table that already exists on the target, so apply the referenced table first, then add the foreign key to the file in a later change"},
		{"not readable", fmt.Errorf("plan: %w: public.customers", schemadiff.ErrReferencedTableNotReadable),
			"; grant the planning role SELECT on the referenced table, then re-plan"},
		{"not referenceable", fmt.Errorf("plan: %w: public.customers", schemadiff.ErrReferencedTableNotReferenceable),
			"; grant the planning role REFERENCES on the referenced table, then re-plan"},
		{"other failure", errors.New("connection reset"), ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, referencedTableRemedy(tt.err))
		})
	}
}

func TestTableChangesMapsDestructiveSafety(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	report.Statements = []pgplan.Statement{
		{
			SQL:         "ALTER TABLE public.users DROP COLUMN legacy",
			Route:       planner.RouteNative,
			Backend:     router.BackendNative,
			Disposition: router.DispositionExecute,
			ExecSQL:     []string{"ALTER TABLE public.users DROP COLUMN legacy"},
			Destructive: true,
		},
		{
			SQL:         "ALTER TABLE public.users ADD COLUMN email text",
			Route:       planner.RouteNative,
			Backend:     router.BackendNative,
			Disposition: router.DispositionExecute,
			ExecSQL:     []string{"ALTER TABLE public.users ADD COLUMN email text"},
		},
	}

	changes, _, _, err := tableChanges(report, nil, parser)
	require.NoError(t, err)
	require.Len(t, changes, 2)
	assert.True(t, changes[0].IsUnsafe)
	assert.NotEmpty(t, changes[0].UnsafeReason)
	assert.False(t, changes[1].IsUnsafe)
	assert.Empty(t, changes[1].UnsafeReason)
}

func TestTableChangesRendersOrderedExecutionSteps(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	exists := true
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	report.TableExists = &exists
	report.Statements = []pgplan.Statement{{
		SQL:         "ALTER TABLE public.users ADD COLUMN email text",
		Route:       planner.RouteNative,
		Backend:     router.BackendNative,
		Disposition: router.DispositionExecute,
		ExecSQL: []string{
			"ALTER TABLE public.users ADD COLUMN email text",
			"CREATE INDEX CONCURRENTLY users_email_idx ON public.users (email)",
		},
	}}

	changes, tiers, _, err := tableChanges(report, nil, parser)
	require.NoError(t, err)
	require.Len(t, changes, 2)
	assert.Equal(t, "ALTER TABLE public.users ADD COLUMN email text", changes[0].DDL)
	assert.Equal(t, ddl.StatementAlterTable, changes[0].Operation)
	assert.Equal(t, "CREATE INDEX CONCURRENTLY users_email_idx ON public.users (email)", changes[1].DDL)
	assert.Equal(t, ddl.StatementCreateIndex, changes[1].Operation)
	assert.Empty(t, changes[0].ExecutionMode)
	assert.Empty(t, changes[1].ExecutionMode)
	require.Len(t, tiers, 2)
	assert.Equal(t, preflight.TierAlterInPlace, tiers[0],
		"an in-place ALTER needs only owner membership, never the index tier's schema CREATE")
	assert.Equal(t, preflight.TierIndexBuild, tiers[1])
}

func TestTableChangesCollapsesGreenfieldCreateSet(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	exists := false
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "widgets"
	report.TableExists = &exists
	report.Statements = []pgplan.Statement{
		{SQL: "CREATE TABLE public.widgets (id bigint PRIMARY KEY, name text)", Route: planner.RouteNative, Backend: router.BackendNative, Disposition: router.DispositionExecute, ExecSQL: []string{"CREATE TABLE public.widgets (id bigint PRIMARY KEY, name text)"}},
		{SQL: "CREATE UNIQUE INDEX widgets_name_key ON public.widgets (name)", Route: planner.RouteNative, Backend: router.BackendNative, Disposition: router.DispositionExecute, ExecSQL: []string{"CREATE UNIQUE INDEX CONCURRENTLY widgets_name_key ON public.widgets (name)"}},
		{SQL: "CREATE INDEX widgets_id_idx ON public.widgets (id)", Route: planner.RouteNative, Backend: router.BackendNative, Disposition: router.DispositionExecute, ExecSQL: []string{"CREATE INDEX CONCURRENTLY widgets_id_idx ON public.widgets (id)"}},
	}

	changes, tiers, _, err := tableChanges(report, nil, parser)
	require.NoError(t, err)
	require.Len(t, changes, 1)
	assert.Equal(t, "CREATE TABLE public.widgets (id bigint PRIMARY KEY, name text);\nCREATE UNIQUE INDEX widgets_name_key ON public.widgets (name);\nCREATE INDEX widgets_id_idx ON public.widgets (id)", changes[0].DDL)
	assert.Equal(t, "widgets", changes[0].Table)
	assert.Equal(t, ddl.StatementCreateTable, changes[0].Operation)
	assert.False(t, changes[0].IsUnsafe)
	assert.Empty(t, changes[0].ExecutionMode)
	require.Equal(t, []preflight.Tier{preflight.TierCreateTable}, tiers)
}

func TestTableChangesKeepsGreenfieldVerdictsPerStatement(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	exists := false
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "widgets"
	report.TableExists = &exists
	report.Statements = []pgplan.Statement{
		{SQL: "CREATE TABLE public.widgets (id bigint PRIMARY KEY, name text)", Route: planner.RouteNative, Backend: router.BackendNative, Disposition: router.DispositionExecute, ExecSQL: []string{"CREATE TABLE public.widgets (id bigint PRIMARY KEY, name text)"}},
		{SQL: "CREATE INDEX widgets_name_idx ON public.widgets (name)", Route: planner.RouteCopyAndSwap, Backend: router.BackendCopyAndSwap, Disposition: router.DispositionUnavailable},
		{SQL: "CREATE INDEX widgets_id_idx ON public.widgets (id)", Route: planner.RouteNative, Backend: router.BackendNative, Disposition: router.DispositionExecute, ExecSQL: []string{"CREATE INDEX CONCURRENTLY widgets_id_idx ON public.widgets (id)"}},
	}

	changes, tiers, _, err := tableChanges(report, nil, parser)
	require.NoError(t, err)
	require.Len(t, changes, 3)
	blockAbsentTableDependents(changes, tiers, report.Table)
	assert.Empty(t, changes[0].ExecutionMode)
	assert.Equal(t, engine.ExecutionModeBlocked, changes[1].ExecutionMode)
	assert.Equal(t, engine.ExecutionModeBlocked, changes[2].ExecutionMode)
	assert.Contains(t, changes[2].ModeReason, "depends on the statement that creates it")
}

func TestIsGreenfieldCreateSetRejectsUnsafeStatements(t *testing.T) {
	exists := false
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "widgets"
	report.TableExists = &exists
	report.Statements = []pgplan.Statement{{SQL: "CREATE TABLE public.widgets (id bigint PRIMARY KEY)"}}

	tests := []struct {
		name     string
		verdicts []stepVerdict
		mutate   func(*pgplan.Report)
	}{
		{name: "destructive verdict", verdicts: []stepVerdict{{mode: engine.ExecutionModeBlocked, reason: "destructive statement"}}},
		{name: "destructive term", verdicts: []stepVerdict{{}}, mutate: func(report *pgplan.Report) {
			report.Statements[0].Destructive = true
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidate := report
			candidate.Statements = append([]pgplan.Statement(nil), report.Statements...)
			if tt.mutate != nil {
				tt.mutate(&candidate)
			}
			assert.False(t, isGreenfieldCreateSet(candidate, tt.verdicts))
		})
	}
}

func TestGreenfieldCreateSetRejectsNonCreateTier(t *testing.T) {
	err := ensureGreenfieldCreateTier("widgets", preflight.TierIndexBuild)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected create a new table")
}

func TestGreenfieldCreateSetRejectsIndexForAnotherTable(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "widgets"
	report.Statements = []pgplan.Statement{
		{SQL: "CREATE TABLE public.widgets (id bigint PRIMARY KEY)"},
		{SQL: "CREATE INDEX gadgets_id_idx ON public.gadgets (id)"},
	}

	_, _, err = greenfieldCreateSet(report, parser)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `creates an index on table "public"."gadgets", not CREATE TABLE target "public"."widgets"`)
}

func TestTableChangesMixedVerdictsFailClosedPerStatement(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	report.Statements = []pgplan.Statement{
		{SQL: "ALTER TABLE public.users ADD COLUMN email text", Route: planner.RouteNative,
			Backend: router.BackendNative, Disposition: router.DispositionExecute,
			ExecSQL: []string{"ALTER TABLE public.users ADD COLUMN email text"}},
		{SQL: "ALTER TABLE public.users ALTER COLUMN email TYPE bigint", Route: planner.RouteCopyAndSwap,
			Backend: router.BackendCopyAndSwap, Disposition: router.DispositionUnavailable},
	}

	changes, _, _, err := tableChanges(report, nil, parser)
	require.NoError(t, err)
	require.Len(t, changes, 2)
	assert.Empty(t, changes[0].ExecutionMode)
	assert.Equal(t, engine.ExecutionModeBlocked, changes[1].ExecutionMode)
	assert.NotContains(t, changes[1].ModeReason, changes[1].DDL)
}

// A statement whose verdict is a placeholder for planner vocabulary this
// build does not map is reported exactly once, in statement order, alongside
// the rendered changes; statements with a known verdict report nothing. A
// statement that renders as several steps still counts once, since the
// vocabulary moved once per planned statement, not once per rendered line.
func TestTableChangesReportsUnrecognizedPlannerVocabularyOncePerStatement(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	report.Statements = []pgplan.Statement{
		{SQL: "CREATE TABLE public.users (id bigint)", Disposition: router.DispositionRefuse,
			Cause: executor.CreateShapeCause("future-cause")},
		{SQL: "ALTER TABLE public.users ADD COLUMN email text", Route: planner.RouteNative,
			Backend: router.BackendNative, Disposition: router.DispositionExecute,
			ExecSQL: []string{"ALTER TABLE public.users ADD COLUMN email text"}},
		{SQL: "ALTER TABLE public.users ALTER COLUMN email TYPE bigint", Disposition: router.Disposition("future"),
			ExecSQL: []string{
				"ALTER TABLE public.users ADD COLUMN email_new bigint",
				"ALTER TABLE public.users DROP COLUMN email",
			}},
	}

	changes, _, unrecognized, err := tableChanges(report, nil, parser)
	require.NoError(t, err)
	require.Len(t, changes, 4)
	assert.Equal(t, engine.ExecutionModeBlocked, changes[0].ExecutionMode)
	assert.Empty(t, changes[1].ExecutionMode)
	assert.Equal(t, engine.ExecutionModeBlocked, changes[2].ExecutionMode)
	assert.Equal(t, engine.ExecutionModeBlocked, changes[3].ExecutionMode)
	assert.Equal(t, []unrecognizedPlannerVocabulary{
		{kind: "create_shape_cause", value: "future-cause"},
		{kind: "disposition", value: "future"},
	}, unrecognized)
}

func TestTableChangesRejectsUnparseablePlanSQL(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	report.Statements = []pgplan.Statement{{SQL: "not ddl", Disposition: router.DispositionRefuse}}

	_, _, _, err = tableChanges(report, nil, parser)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `classify planned statement for table "users"`)
}

// TestBlockMissingPrivilegesSkipsMissingTable proves that executable steps on
// a table the target provably does not have are blocked with their dependency
// on the table's creation as the reason — never with a privilege probe's
// "table not found", which reads as a re-plan instruction no re-plan can
// satisfy. Only the CREATE TABLE step's tier is probed, since the schema it
// asks about exists whether or not the table does.
func TestBlockMissingPrivilegesSkipsMissingTable(t *testing.T) {
	var checked []preflight.Tier
	checkTier := func(_ context.Context, _ *pgxpool.Pool, _ pgplan.Report, tier preflight.Tier, _ string) error {
		checked = append(checked, tier)
		return nil
	}
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Schema = "public"
	report.Table = "users"
	exists := false
	report.TableExists = &exists
	changes := []engine.TableChange{
		{
			Table:         "users",
			DDL:           "CREATE TABLE public.users (id bigint PRIMARY KEY)",
			ExecutionMode: engine.ExecutionModeBlocked,
			ModeReason:    "creation shape verdict",
		},
		{
			Table: "users",
			DDL:   "CREATE INDEX CONCURRENTLY idx_users_email ON public.users (email)",
		},
	}

	changes, err := blockMissingPrivilegesWithCheck(t.Context(), nil, "orders_db", report, changes, []preflight.Tier{preflight.TierCreateTable, preflight.TierIndexBuild}, "", checkTier)
	require.NoError(t, err)
	assert.Equal(t, []preflight.Tier{preflight.TierCreateTable}, checked)
	assert.Equal(t, "creation shape verdict", changes[0].ModeReason)
	assert.Equal(t, engine.ExecutionModeBlocked, changes[1].ExecutionMode)
	assert.Contains(t, changes[1].ModeReason, `table "users" does not exist on the target`)
	assert.Contains(t, changes[1].ModeReason, "depends on the statement that creates it")
}

func TestBlockMissingPrivilegesAppendsCauseToFullyBlockedPlan(t *testing.T) {
	privilegeErr := &preflight.PrivilegeError{
		Tier:  preflight.TierAlterInPlace,
		Check: "pg_has_role(schemabot, app_owner, 'USAGE')",
		Grant: `GRANT "app_owner" TO "schemabot" WITH INHERIT TRUE`,
		Hint:  `if the owner is an administrative role, transfer the table instead: ALTER TABLE "public"."users" OWNER TO <role>`,
	}
	checkTier := func(context.Context, *pgxpool.Pool, pgplan.Report, preflight.Tier, string) error {
		return privilegeErr
	}
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Schema = "public"
	report.Table = "users"
	changes := []engine.TableChange{
		{Table: "users", DDL: "ALTER TABLE public.users DROP COLUMN legacy", ExecutionMode: engine.ExecutionModeBlocked, ModeReason: "planner refused a table rewrite"},
		{Table: "users", DDL: "ALTER TABLE public.users ADD COLUMN email text", ExecutionMode: engine.ExecutionModeBlocked, ModeReason: "planner refused a lock shape"},
	}
	tiers := []preflight.Tier{preflight.TierAlterInPlace, preflight.TierAlterInPlace}
	reason := classifyRefusal(privilegeErr, report.Table).detail

	changes, err := blockMissingPrivilegesWithCheck(t.Context(), nil, "orders_db", report, changes, tiers, "app_owner", checkTier)
	require.NoError(t, err)
	assert.Equal(t, "planner refused a table rewrite"+clauseSeparator+reason, changes[0].ModeReason)
	assert.Equal(t, "planner refused a lock shape"+clauseSeparator+reason, changes[1].ModeReason)
	assert.Equal(t, changes[0].ModeReason, sanitizeReasonText(changes[0].ModeReason), "the combined reason must render whole rather than being clamped")
	assert.Greater(t, len(changes[0].ModeReason), maxStatementMetadataLen)
}

// TestBlockMissingPrivilegesChecksOnlyTheCreateTierOnBlockedGreenfieldPlan
// proves that on a fully blocked plan for a table the target does not have,
// only the CREATE TABLE tier is probed: a privilege probe against an absent
// table can only answer "table not found", so the dependents keep their
// verdicts and the create step alone gains the missing-grant cause.
func TestBlockMissingPrivilegesChecksOnlyTheCreateTierOnBlockedGreenfieldPlan(t *testing.T) {
	privilegeErr := &preflight.PrivilegeError{Tier: preflight.TierCreateTable, Check: "has_schema_privilege(schemabot, public, 'CREATE')", Grant: `GRANT CREATE ON SCHEMA "public" TO "schemabot"`}
	var checked []preflight.Tier
	checkTier := func(_ context.Context, _ *pgxpool.Pool, _ pgplan.Report, tier preflight.Tier, _ string) error {
		checked = append(checked, tier)
		return privilegeErr
	}
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Schema = "public"
	report.Table = "users"
	exists := false
	report.TableExists = &exists
	changes := []engine.TableChange{
		{Table: "users", DDL: "CREATE TABLE public.users (id bigint PRIMARY KEY)", ExecutionMode: engine.ExecutionModeBlocked, ModeReason: "creation shape verdict"},
		{Table: "users", DDL: "CREATE INDEX CONCURRENTLY idx_users_email ON public.users (email)", ExecutionMode: engine.ExecutionModeBlocked, ModeReason: "depends on the create"},
	}
	tiers := []preflight.Tier{preflight.TierCreateTable, preflight.TierIndexBuild}

	changes, err := blockMissingPrivilegesWithCheck(t.Context(), nil, "orders_db", report, changes, tiers, "app_owner", checkTier)
	require.NoError(t, err)
	assert.Equal(t, []preflight.Tier{preflight.TierCreateTable}, checked)
	assert.Equal(t, "creation shape verdict"+clauseSeparator+classifyRefusal(privilegeErr, report.Table).detail, changes[0].ModeReason)
	assert.Equal(t, "depends on the create", changes[1].ModeReason)
}

// TestTableChangesDerivesTiersForBlockedSteps proves a step the planner
// already blocked still carries the tier its statement needs, so the privilege
// gate can name the grant the operator will also need; a blocked step whose
// shape the engine does not execute carries no tier at all.
func TestTableChangesDerivesTiersForBlockedSteps(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	exists := true
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	report.TableExists = &exists
	report.Statements = []pgplan.Statement{
		{SQL: "ALTER TABLE public.users ALTER COLUMN email TYPE bigint", Route: planner.RouteCopyAndSwap, Backend: router.BackendCopyAndSwap, Disposition: router.DispositionUnavailable},
		{SQL: "ALTER TABLE public.users ADD CONSTRAINT users_email_key UNIQUE (email)", Route: planner.RouteNative, Backend: router.BackendNative, Disposition: router.DispositionRewriteRequired},
		{SQL: "REINDEX INDEX CONCURRENTLY public.users_legacy_idx", Route: planner.RouteNative, Backend: router.BackendNative, Disposition: router.DispositionUnavailable},
	}

	changes, tiers, _, err := tableChanges(report, nil, parser)
	require.NoError(t, err)
	require.Len(t, changes, 3)
	for _, change := range changes {
		assert.Equal(t, engine.ExecutionModeBlocked, change.ExecutionMode)
	}
	assert.Equal(t, []preflight.Tier{preflight.TierAlterInPlace, preflight.TierIndexBuild, tierUnderived}, tiers)
	// The planner's own reason is the specific one, so a step it already
	// blocked keeps it even when the shape has no tier of its own.
	assert.Equal(t, `statement for table "users" requires an execution path SchemaBot's PostgreSQL support does not provide yet`, changes[2].ModeReason)
}

// TestTableChangesCarriesGuidancePerStatement proves each blocked step on a
// table shows the remedy for its own statement: two statements the planner
// refuses for different reasons each get their own sentence, and neither is
// recorded as vocabulary SchemaBot does not know.
func TestTableChangesCarriesGuidancePerStatement(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	exists := true
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	report.TableExists = &exists
	report.Statements = []pgplan.Statement{
		{SQL: "ALTER TABLE public.users ADD COLUMN ref text UNIQUE", Route: planner.RouteNative, Backend: router.BackendNative, Disposition: router.DispositionRewriteRequired, Guidance: suggest.GuidanceAddColumnThenConstraint},
		{SQL: "ALTER TABLE public.users RENAME COLUMN name TO full_name", Route: planner.RouteNative, Backend: router.BackendNative, Disposition: router.DispositionRewriteRequired, Guidance: suggest.GuidanceExpandContractColumn},
	}

	changes, _, unrecognized, err := tableChanges(report, nil, parser)
	require.NoError(t, err)
	require.Len(t, changes, 2)
	assert.Empty(t, unrecognized)
	assert.Equal(t, rewriteRequiredSentences[suggest.GuidanceAddColumnThenConstraint], changes[0].ModeReason)
	assert.Equal(t, rewriteRequiredSentences[suggest.GuidanceExpandContractColumn], changes[1].ModeReason)
}

// TestBlockMissingPrivilegesNamesGrantOnPlannerBlockedPlan runs the planner's
// blocked verdicts through the same rendering the engine uses and proves the
// privilege gate probes each blocked step's own tier: an operator whose role
// lacks owner membership sees that grant beside the planner's refusal on
// every step that needs it, while a step whose shape has no tier is neither
// probed nor annotated.
func TestBlockMissingPrivilegesNamesGrantOnPlannerBlockedPlan(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	exists := true
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Schema = "public"
	report.Table = "users"
	report.TableExists = &exists
	report.Statements = []pgplan.Statement{
		{SQL: "ALTER TABLE public.users ALTER COLUMN email TYPE bigint", Route: planner.RouteCopyAndSwap, Backend: router.BackendCopyAndSwap, Disposition: router.DispositionUnavailable},
		{SQL: "ALTER TABLE public.users ADD CONSTRAINT users_email_key UNIQUE (email)", Route: planner.RouteNative, Backend: router.BackendNative, Disposition: router.DispositionRewriteRequired},
		{SQL: "REINDEX INDEX CONCURRENTLY public.users_legacy_idx", Route: planner.RouteNative, Backend: router.BackendNative, Disposition: router.DispositionUnavailable},
	}
	changes, tiers, _, err := tableChanges(report, nil, parser)
	require.NoError(t, err)
	plannerReasons := []string{changes[0].ModeReason, changes[1].ModeReason, changes[2].ModeReason}

	// Owner membership is the ladder's first rung, so every tier above it
	// fails the same way.
	privilegeErr := &preflight.PrivilegeError{
		Tier:  preflight.TierAlterInPlace,
		Check: "pg_has_role(schemabot, app_owner, 'USAGE')",
		Grant: `GRANT "app_owner" TO "schemabot" WITH INHERIT TRUE`,
	}
	var checked []preflight.Tier
	checkTier := func(_ context.Context, _ *pgxpool.Pool, _ pgplan.Report, tier preflight.Tier, _ string) error {
		checked = append(checked, tier)
		return privilegeErr
	}

	changes, err = blockMissingPrivilegesWithCheck(t.Context(), nil, "orders_db", report, changes, tiers, "app_owner", checkTier)
	require.NoError(t, err)
	assert.Equal(t, []preflight.Tier{preflight.TierAlterInPlace, preflight.TierIndexBuild}, checked)
	grant := classifyRefusal(privilegeErr, report.Table).detail
	assert.Equal(t, plannerReasons[0]+clauseSeparator+grant, changes[0].ModeReason)
	assert.Equal(t, plannerReasons[1]+clauseSeparator+grant, changes[1].ModeReason)
	assert.Equal(t, plannerReasons[2], changes[2].ModeReason)
}

// TestBlockMissingPrivilegesNamesSchemaCreateOnPlannerBlockedGreenfieldPlan
// proves that when the planner refuses a CREATE TABLE's shape and the table
// does not exist yet, the create step is still probed at the create tier so
// a missing schema CREATE grant is named beside the shape refusal, while the
// index that depends on the table is left alone.
func TestBlockMissingPrivilegesNamesSchemaCreateOnPlannerBlockedGreenfieldPlan(t *testing.T) {
	report, changes, tiers := plannerBlockedGreenfieldPlan(t, false)
	plannerReasons := []string{changes[0].ModeReason, changes[1].ModeReason}

	privilegeErr := &preflight.PrivilegeError{Tier: preflight.TierCreateTable, Check: "has_schema_privilege(schemabot, public, 'CREATE')", Grant: `GRANT CREATE ON SCHEMA "public" TO "schemabot"`}
	var checked []preflight.Tier
	checkTier := func(_ context.Context, _ *pgxpool.Pool, _ pgplan.Report, tier preflight.Tier, _ string) error {
		checked = append(checked, tier)
		return privilegeErr
	}

	changes, err := blockMissingPrivilegesWithCheck(t.Context(), nil, "orders_db", report, changes, tiers, "app_owner", checkTier)
	require.NoError(t, err)
	assert.Equal(t, []preflight.Tier{preflight.TierCreateTable}, checked)
	assert.Equal(t, plannerReasons[0]+clauseSeparator+classifyRefusal(privilegeErr, report.Table).detail, changes[0].ModeReason)
	assert.Equal(t, plannerReasons[1], changes[1].ModeReason)
}

// plannerBlockedGreenfieldPlan renders a greenfield plan whose CREATE TABLE
// the planner refused for its shape. The index that depends on the table is
// blocked by the planner too unless indexExecutable is set, in which case it
// is left for the privilege gate to judge.
func plannerBlockedGreenfieldPlan(t *testing.T, indexExecutable bool) (pgplan.Report, []engine.TableChange, []preflight.Tier) {
	t.Helper()
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	exists := false
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Schema = "public"
	report.Table = "widgets"
	report.TableExists = &exists
	index := pgplan.Statement{SQL: "CREATE INDEX widgets_id_idx ON public.widgets (id)", Route: planner.RouteCopyAndSwap, Backend: router.BackendCopyAndSwap, Disposition: router.DispositionUnavailable}
	if indexExecutable {
		index = pgplan.Statement{SQL: "CREATE INDEX widgets_id_idx ON public.widgets (id)", Route: planner.RouteNative, Backend: router.BackendNative, Disposition: router.DispositionExecute, ExecSQL: []string{"CREATE INDEX widgets_id_idx ON public.widgets (id)"}}
	}
	report.Statements = []pgplan.Statement{
		{SQL: "CREATE TABLE public.widgets PARTITION OF public.gadgets FOR VALUES IN (1)", Route: planner.RouteNative, Backend: router.BackendNative, Disposition: router.DispositionRefuse, Cause: executor.CreateShapePartitionOf},
		index,
	}
	changes, tiers, _, err := tableChanges(report, nil, parser)
	require.NoError(t, err)
	require.Equal(t, []preflight.Tier{preflight.TierCreateTable, preflight.TierIndexBuild}, tiers)
	return report, changes, tiers
}

// TestBlockMissingPrivilegesNamesProvisionableRefusalOnPlannerBlockedGreenfieldPlan
// proves the create-tier probe's other answers reach the operator the same
// way a missing grant does: a schema the target lacks, or a configured table
// owner that is not a role there, is something the operator provisions
// whatever becomes of the shape refusal, so it is named beside that refusal
// rather than left to the log.
func TestBlockMissingPrivilegesNamesProvisionableRefusalOnPlannerBlockedGreenfieldPlan(t *testing.T) {
	for name, checkErr := range map[string]error{
		"schema absent":     fmt.Errorf("%w: schema public does not exist", preflight.ErrSchemaNotFound),
		"owner role absent": fmt.Errorf("%w: create owner role %q does not exist", preflight.ErrCreateOwnerNotFound, "app_owner"),
	} {
		t.Run(name, func(t *testing.T) {
			report, changes, tiers := plannerBlockedGreenfieldPlan(t, false)
			plannerReasons := []string{changes[0].ModeReason, changes[1].ModeReason}
			var checked []preflight.Tier
			checkTier := func(_ context.Context, _ *pgxpool.Pool, _ pgplan.Report, tier preflight.Tier, _ string) error {
				checked = append(checked, tier)
				return checkErr
			}

			changes, err := blockMissingPrivilegesWithCheck(t.Context(), nil, "orders_db", report, changes, tiers, "app_owner", checkTier)
			require.NoError(t, err)
			assert.Equal(t, []preflight.Tier{preflight.TierCreateTable}, checked)
			assert.Equal(t, plannerReasons[0]+clauseSeparator+classifyRefusal(checkErr, report.Table).detail, changes[0].ModeReason)
			assert.Equal(t, plannerReasons[1], changes[1].ModeReason)
		})
	}
}

// TestBlockMissingPrivilegesProbesCreateWhenDependentsLeaveNothingExecutable
// proves a greenfield plan that becomes fully blocked inside the gate — the
// planner refused the CREATE TABLE and the gate blocked the index for
// depending on it — is treated like any other fully blocked plan: the create
// step is still probed at its own tier and the schema grant named beside the
// shape refusal, so the operator is not sent around again once the shape is
// fixed.
func TestBlockMissingPrivilegesProbesCreateWhenDependentsLeaveNothingExecutable(t *testing.T) {
	report, changes, tiers := plannerBlockedGreenfieldPlan(t, true)
	require.Empty(t, changes[1].ExecutionMode)
	plannerReason := changes[0].ModeReason

	privilegeErr := &preflight.PrivilegeError{Tier: preflight.TierCreateTable, Check: "has_schema_privilege(schemabot, public, 'CREATE')", Grant: `GRANT CREATE ON SCHEMA "public" TO "schemabot"`}
	var checked []preflight.Tier
	checkTier := func(_ context.Context, _ *pgxpool.Pool, _ pgplan.Report, tier preflight.Tier, _ string) error {
		checked = append(checked, tier)
		return privilegeErr
	}

	changes, err := blockMissingPrivilegesWithCheck(t.Context(), nil, "orders_db", report, changes, tiers, "app_owner", checkTier)
	require.NoError(t, err)
	assert.Equal(t, []preflight.Tier{preflight.TierCreateTable}, checked)
	assert.Equal(t, plannerReason+clauseSeparator+classifyRefusal(privilegeErr, report.Table).detail, changes[0].ModeReason)
	assert.Equal(t, engine.ExecutionModeBlocked, changes[1].ExecutionMode)
	assert.Contains(t, changes[1].ModeReason, "depends on the statement that creates it")
}

// When every step is already blocked, a refusal about the table itself —
// vanished, or not a table — names nothing the operator can provision, so
// the existing verdicts stand and the refusal is logged with its
// classification rather than appended or lost.
func TestBlockMissingPrivilegesLogsNonProvisionableRefusalOnFullyBlockedPlan(t *testing.T) {
	for reason, checkErr := range map[string]error{
		"not-a-table":     fmt.Errorf("%w: public.users has relkind %q", preflight.ErrNotTable, "v"),
		"table-not-found": fmt.Errorf("%w: public.users", preflight.ErrTableNotFound),
	} {
		t.Run(reason, func(t *testing.T) {
			checkTier := func(context.Context, *pgxpool.Pool, pgplan.Report, preflight.Tier, string) error { return checkErr }
			var logs bytes.Buffer
			originalLogger := slog.Default()
			t.Cleanup(func() { slog.SetDefault(originalLogger) })
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			report := pgplan.NewReport(pgplan.SourceDiff)
			report.Schema = "public"
			report.Table = "users"
			want := []engine.TableChange{{Table: "users", ExecutionMode: engine.ExecutionModeBlocked, ModeReason: "planner refusal"}}
			changes := append([]engine.TableChange(nil), want...)

			changes, err := blockMissingPrivilegesWithCheck(t.Context(), nil, "orders_db", report, changes, []preflight.Tier{preflight.TierAlterInPlace}, "", checkTier)
			require.NoError(t, err)
			assert.Equal(t, want, changes)
			assert.Contains(t, logs.String(), "level=WARN")
			assert.Contains(t, logs.String(), "privilege check refused the table")
			assert.Contains(t, logs.String(), "database=orders_db")
			assert.Contains(t, logs.String(), "refusal="+reason)
		})
	}
}

// TestBlockMissingPrivilegesKeepsFullyBlockedPlanWithoutTarget proves a
// report that names no target but carries no executable step is returned as
// it stands: there is nothing to probe and no unnamed table for a probe to
// ask about, so no verdict changes and no check runs.
func TestBlockMissingPrivilegesKeepsFullyBlockedPlanWithoutTarget(t *testing.T) {
	checkTier := func(context.Context, *pgxpool.Pool, pgplan.Report, preflight.Tier, string) error {
		t.Fatal("no probe may run for a plan that names no target")
		return nil
	}
	report := pgplan.NewReport(pgplan.SourceDiff)
	want := []engine.TableChange{{Table: "users", ExecutionMode: engine.ExecutionModeBlocked, ModeReason: "planner refusal"}}
	changes := append([]engine.TableChange(nil), want...)

	changes, err := blockMissingPrivilegesWithCheck(t.Context(), nil, "orders_db", report, changes, []preflight.Tier{preflight.TierAlterInPlace}, "", checkTier)
	require.NoError(t, err)
	assert.Equal(t, want, changes)
}

func TestBlockMissingPrivilegesRejectsExecutableStepWithoutTier(t *testing.T) {
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	changes := []engine.TableChange{{Table: "users", DDL: "ALTER TABLE public.users ADD COLUMN email text"}}

	_, err := blockMissingPrivileges(t.Context(), nil, "orders_db", report, changes, []preflight.Tier{tierUnderived}, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "carries no privilege tier")
}

func TestBlockMissingPrivilegesKeepsFullyBlockedReasonsWhenPrivilegesPresent(t *testing.T) {
	checkTier := func(context.Context, *pgxpool.Pool, pgplan.Report, preflight.Tier, string) error { return nil }
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	want := []engine.TableChange{{Table: "users", ExecutionMode: engine.ExecutionModeBlocked, ModeReason: "planner refusal"}}
	changes := append([]engine.TableChange(nil), want...)

	changes, err := blockMissingPrivilegesWithCheck(t.Context(), nil, "orders_db", report, changes, []preflight.Tier{preflight.TierAlterInPlace}, "", checkTier)
	require.NoError(t, err)
	assert.Equal(t, want, changes)
}

func TestBlockMissingPrivilegesKeepsFullyBlockedReasonsWhenCheckFails(t *testing.T) {
	checkTier := func(context.Context, *pgxpool.Pool, pgplan.Report, preflight.Tier, string) error {
		return errors.New("catalog unavailable")
	}
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Schema = "public"
	report.Table = "users"
	want := []engine.TableChange{{Table: "users", ExecutionMode: engine.ExecutionModeBlocked, ModeReason: "planner refusal"}}
	changes := append([]engine.TableChange(nil), want...)

	changes, err := blockMissingPrivilegesWithCheck(t.Context(), nil, "orders_db", report, changes, []preflight.Tier{preflight.TierAlterInPlace}, "", checkTier)
	require.NoError(t, err)
	assert.Equal(t, want, changes)
}

func TestBlockMissingPrivilegesDoesNotAppendCauseTwice(t *testing.T) {
	privilegeErr := &preflight.PrivilegeError{Tier: preflight.TierAlterInPlace, Check: "has_table_privilege", Grant: "GRANT app_owner TO schemabot"}
	checkTier := func(context.Context, *pgxpool.Pool, pgplan.Report, preflight.Tier, string) error { return privilegeErr }
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	changes := []engine.TableChange{{Table: "users", ExecutionMode: engine.ExecutionModeBlocked, ModeReason: "planner refusal"}}
	tiers := []preflight.Tier{preflight.TierAlterInPlace}

	changes, err := blockMissingPrivilegesWithCheck(t.Context(), nil, "orders_db", report, changes, tiers, "", checkTier)
	require.NoError(t, err)
	want := changes[0].ModeReason
	changes, err = blockMissingPrivilegesWithCheck(t.Context(), nil, "orders_db", report, changes, tiers, "", checkTier)
	require.NoError(t, err)
	assert.Equal(t, want, changes[0].ModeReason)
}

func TestBlockMissingPrivilegesBlocksExecutableStepInMixedPlan(t *testing.T) {
	privilegeErr := &preflight.PrivilegeError{Tier: preflight.TierAlterInPlace, Check: "has_table_privilege", Grant: "GRANT app_owner TO schemabot"}
	checkTier := func(context.Context, *pgxpool.Pool, pgplan.Report, preflight.Tier, string) error { return privilegeErr }
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	changes := []engine.TableChange{
		{Table: "users", DDL: "ALTER TABLE public.users ADD COLUMN email text"},
		{Table: "users", DDL: "ALTER TABLE public.users DROP COLUMN legacy", ExecutionMode: engine.ExecutionModeBlocked, ModeReason: "planner refusal"},
	}
	tiers := []preflight.Tier{preflight.TierAlterInPlace, preflight.TierAlterInPlace}

	changes, err := blockMissingPrivilegesWithCheck(t.Context(), nil, "orders_db", report, changes, tiers, "", checkTier)
	require.NoError(t, err)
	assert.Equal(t, engine.ExecutionModeBlocked, changes[0].ExecutionMode)
	assert.Equal(t, classifyRefusal(privilegeErr, report.Table).detail, changes[0].ModeReason)
	assert.Equal(t, "planner refusal", changes[1].ModeReason)
}

// TestBlockMissingPrivilegesRequiresTargetTable proves a report that carries
// executable steps without naming its target fails the plan closed: the
// privilege check cannot answer for an unnamed table, and an executable plan
// must never be produced while the answer is unknown.
func TestBlockMissingPrivilegesRequiresTargetTable(t *testing.T) {
	report := pgplan.NewReport(pgplan.SourceDiff)
	changes := []engine.TableChange{{
		Table: "users",
		DDL:   "ALTER TABLE public.users ADD COLUMN email text",
	}}

	_, err := blockMissingPrivileges(t.Context(), nil, "orders_db", report, changes, []preflight.Tier{preflight.TierAlterInPlace}, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "names no target table")
}

// TestBlockMissingPrivilegesRequiresTargetTableWhenAbsent proves the unnamed-
// target guard fires before dependent-step blocking rewrites verdicts: a
// report that provably lacks its table but carries executable steps and no
// target name must fail the plan closed, never launder the missing name into
// a plan whose every step was blocked for a fabricated dependency.
func TestBlockMissingPrivilegesRequiresTargetTableWhenAbsent(t *testing.T) {
	report := pgplan.NewReport(pgplan.SourceDiff)
	exists := false
	report.TableExists = &exists
	changes := []engine.TableChange{
		{
			Table:         "users",
			DDL:           "CREATE TABLE public.users (id bigint PRIMARY KEY)",
			ExecutionMode: engine.ExecutionModeBlocked,
			ModeReason:    "creation shape verdict",
		},
		{
			Table: "users",
			DDL:   "CREATE INDEX CONCURRENTLY idx_users_email ON public.users (email)",
		},
	}

	_, err := blockMissingPrivileges(t.Context(), nil, "orders_db", report, changes, []preflight.Tier{preflight.TierCreateTable, preflight.TierIndexBuild}, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "names no target table")
}

// TestConcurrentIndexStatement pins the one statement shape admitted under the
// concurrent index duration envelope instead of the table size ceiling, over
// the shapes the privilege tier derivation lets reach the ceiling: a blocking
// index build and a plain ALTER stay inside it, and so does a concurrent
// partition detach — CONCURRENTLY alone does not move a statement out of the
// ceiling, the statement must build an index. A statement the parser rejects
// is an error, never a silent classification either way.
func TestConcurrentIndexStatement(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want bool
	}{
		{name: "concurrent index build", sql: "CREATE INDEX CONCURRENTLY users_email_idx ON public.users (email)", want: true},
		{name: "blocking index build", sql: "CREATE INDEX users_email_idx ON public.users (email)"},
		{name: "alter table", sql: "ALTER TABLE public.users ADD COLUMN email text"},
		{name: "concurrent partition detach", sql: "ALTER TABLE public.events DETACH PARTITION public.events_2024 CONCURRENTLY"},
		{name: "concurrent index drop", sql: "DROP INDEX CONCURRENTLY public.users_email_idx"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := concurrentIndexStatement(tt.sql)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	_, err := concurrentIndexStatement("CREATE INDEX CONCURRENTLY ON")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse PostgreSQL statement")
}

// TestBlockRewriteStepsKeepsEarlierVerdictsAndIndexBuilds proves the size
// verdict lands only on the steps it is about: a step an earlier gate already
// blocked keeps that gate's reason before the appended size cause, and a
// concurrent index build and a partitioned index build stay executable under
// their own bound.
func TestBlockRewriteStepsKeepsEarlierVerdictsAndIndexBuilds(t *testing.T) {
	changes := []engine.TableChange{
		{
			Table:         "users",
			DDL:           "ALTER TABLE public.users ADD CONSTRAINT users_email_key UNIQUE (email)",
			ExecutionMode: engine.ExecutionModeBlocked,
			ModeReason:    "privilege verdict",
		},
		{Table: "users", DDL: "ALTER TABLE public.users ADD COLUMN email text"},
		{Table: "users", DDL: "CREATE INDEX CONCURRENTLY users_email_idx ON public.users (email)"},
		{Table: "users", DDL: "CREATE INDEX users_name_idx ON public.users (name)"},
		{Table: "users", DDL: "CREATE INDEX users_age_idx ON public.users (age)"},
	}
	partitionedBuilds := map[string]bool{"CREATE INDEX users_name_idx ON public.users (name)": true}

	require.NoError(t, blockRewriteSteps(changes, "size verdict", partitionedBuilds, nil, nil))
	assert.Equal(t, engine.ExecutionModeBlocked, changes[0].ExecutionMode)
	assert.Equal(t, []string{"privilege verdict", "size verdict"}, engine.BlockedCauses(changes[0].ModeReason))
	assert.Equal(t, engine.ExecutionModeBlocked, changes[1].ExecutionMode)
	assert.Equal(t, "size verdict", changes[1].ModeReason)
	assert.Empty(t, changes[2].ExecutionMode)
	assert.Empty(t, changes[2].ModeReason)
	assert.Empty(t, changes[3].ExecutionMode, "a partitioned index build is bounded per partition, not by the table size")
	assert.Empty(t, changes[3].ModeReason)
	assert.Equal(t, engine.ExecutionModeBlocked, changes[4].ExecutionMode, "a plain index build the planner did not route to the partitioned build stays under the ceiling")
	assert.Equal(t, "size verdict", changes[4].ModeReason)

	require.NoError(t, blockRewriteSteps(changes, "size verdict", partitionedBuilds, nil, nil))
	assert.Equal(t, []string{"privilege verdict", "size verdict"}, engine.BlockedCauses(changes[0].ModeReason))
	assert.Equal(t, "size verdict", changes[1].ModeReason)
}

// TestBlockRewriteStepsKeepsConcurrentIndexDrops proves the size verdict
// leaves a concurrent index drop executable on a table over the ceiling: the
// drop reads no rows and rewrites nothing, so the table's size does not bound
// it. A blocking index drop and a drop of several indexes stay under the
// ceiling.
func TestBlockRewriteStepsKeepsConcurrentIndexDrops(t *testing.T) {
	changes := []engine.TableChange{
		{Table: "orders", DDL: "DROP INDEX CONCURRENTLY public.orders_status_idx"},
		{Table: "orders", DDL: "DROP INDEX public.orders_status_idx"},
		{Table: "orders", DDL: "DROP INDEX public.orders_a_idx, public.orders_b_idx"},
	}

	require.NoError(t, blockRewriteSteps(changes, "size verdict", nil, nil, nil))
	assert.Empty(t, changes[0].ExecutionMode)
	assert.Empty(t, changes[0].ModeReason)
	assert.Equal(t, engine.ExecutionModeBlocked, changes[1].ExecutionMode)
	assert.Equal(t, "size verdict", changes[1].ModeReason)
	assert.Equal(t, engine.ExecutionModeBlocked, changes[2].ExecutionMode)
	assert.Equal(t, "size verdict", changes[2].ModeReason)
}

// A step that needs a full copy of the table is blocked at any size, so an
// oversized table adds no size cause to it: raising the limit alone would not
// let it run. Another blocked step on the same table still gains the size
// cause beside its own, and a full-copy step that carries no verdict yet takes
// the size verdict rather than running.
func TestBlockRewriteStepsLeavesAFullCopyStepItsOwnCause(t *testing.T) {
	copyDDL := "ALTER TABLE public.orders ALTER COLUMN total TYPE numeric(12,2)"
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Statements = []pgplan.Statement{
		{SQL: copyDDL, Backend: router.BackendCopyAndSwap},
		{SQL: "ALTER TABLE public.orders ADD COLUMN note text", ExecSQL: []string{"ALTER TABLE public.orders ADD COLUMN note text"}},
	}
	fullCopy := fullCopySteps(report)
	require.Equal(t, map[string]bool{copyDDL: true}, fullCopy)

	changes := []engine.TableChange{
		{Table: "orders", DDL: copyDDL, ExecutionMode: engine.ExecutionModeBlocked, ModeReason: "needs a full copy"},
		{Table: "orders", DDL: "ALTER TABLE public.orders ALTER COLUMN note SET NOT NULL", ExecutionMode: engine.ExecutionModeBlocked, ModeReason: "rewrite it"},
	}
	require.NoError(t, blockRewriteSteps(changes, "size verdict", nil, nil, fullCopy))
	assert.Equal(t, "needs a full copy", changes[0].ModeReason)
	assert.Equal(t, []string{"rewrite it", "size verdict"}, engine.BlockedCauses(changes[1].ModeReason))

	unblocked := []engine.TableChange{{Table: "orders", DDL: copyDDL}}
	require.NoError(t, blockRewriteSteps(unblocked, "size verdict", nil, nil, fullCopy))
	assert.Equal(t, engine.ExecutionModeBlocked, unblocked[0].ExecutionMode, "a full-copy step that carries no verdict yet is still blocked on an oversized table")
	assert.Equal(t, "size verdict", unblocked[0].ModeReason)
}

// A reviewed VALIDATE CONSTRAINT keeps its verdict on a table past the size
// ceiling: its scan holds SHARE UPDATE EXCLUSIVE, so reads and writes continue,
// and the validation bound limits it instead of the table's size. A
// validation with no planner record is not exempted on its statement alone.
func TestBlockRewriteStepsKeepsReviewedValidations(t *testing.T) {
	record, err := json.Marshal(reviewedUserKey())
	require.NoError(t, err)
	changes := []engine.TableChange{
		{Table: "orders", DDL: reviewedValidationSQL, Metadata: map[string]string{reviewedValidationMetadataKey: string(record)}},
		{Table: "orders", DDL: reviewedValidationSQL},
		{Table: "orders", DDL: "ALTER TABLE public.orders ALTER COLUMN user_id TYPE numeric", Metadata: map[string]string{reviewedValidationMetadataKey: string(record)}},
	}

	require.NoError(t, blockRewriteSteps(changes, "size verdict", nil, nil, nil))
	assert.Empty(t, changes[0].ExecutionMode, changes[0].ModeReason)
	assert.Equal(t, engine.ExecutionModeBlocked, changes[1].ExecutionMode)
	assert.Equal(t, "size verdict", changes[1].ModeReason)
	assert.Equal(t, engine.ExecutionModeBlocked, changes[2].ExecutionMode, "a record does not exempt a statement that is not a validation")
}

// TestTableChangesRendersAnIndexDropAsUnsafe proves a planned index drop on
// an existing table is an executable, unsafe step that carries the planner's
// record of the index and the owner tier, so the apply can check the index
// before dropping it and the operator acknowledges it with --allow-unsafe.
func TestTableChangesRendersAnIndexDropAsUnsafe(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	exists := true
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Schema = "public"
	report.Table = "orders"
	report.TableExists = &exists
	dropped := schemadiff.DroppedObject{
		Kind:       schemadiff.DroppedIndex,
		Name:       "orders_status_idx",
		Definition: "CREATE INDEX orders_status_idx ON public.orders USING btree (status)",
	}
	report.Statements = []pgplan.Statement{{
		SQL:         "DROP INDEX public.orders_status_idx",
		ExecSQL:     []string{"DROP INDEX CONCURRENTLY public.orders_status_idx"},
		Kind:        schemadiff.ChangeDropIndex,
		Route:       planner.RouteNative,
		Backend:     router.BackendNative,
		Disposition: router.DispositionExecute,
		Destructive: true,
		Drops:       &dropped,
	}}

	changes, tiers, _, err := tableChanges(report, nil, parser)
	require.NoError(t, err)
	require.Len(t, changes, 1)
	assert.Empty(t, changes[0].ExecutionMode, changes[0].ModeReason)
	assert.Equal(t, "DROP INDEX CONCURRENTLY public.orders_status_idx", changes[0].DDL)
	assert.True(t, changes[0].IsUnsafe)
	assert.Equal(t, []preflight.Tier{preflight.TierAlterInPlace}, tiers)
	recorded, err := reviewedDropFromMetadata(changes[0].Metadata)
	require.NoError(t, err)
	require.NotNil(t, recorded)
	assert.Equal(t, dropped, *recorded)
}

// A changed unique index is replaced in three steps: the new definition is
// built under a temporary name, the old index is dropped, and the new one is
// renamed to the old name. All three are executable. The drop still needs
// --allow-unsafe, since the old definition stops applying, and its reason
// names the index that replaces it rather than calling it a removal. The drop
// carries the plan's record of the replacement, which the apply checks before
// dropping the old index.
func TestTableChangesRendersAnIndexReplacement(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	exists := true
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Schema = "public"
	report.Table = "orders"
	report.TableExists = &exists
	dropped := schemadiff.DroppedObject{
		Kind:       schemadiff.DroppedIndex,
		Name:       "orders_email_key",
		Definition: "CREATE UNIQUE INDEX orders_email_key ON public.orders USING btree (email)",
	}
	replacement := schemadiff.IndexReplacement{
		Index: "orders_email_key_new",
		Def:   "CREATE UNIQUE INDEX orders_email_key_new ON public.orders USING btree (lower(email))",
	}
	native := func(s pgplan.Statement) pgplan.Statement {
		s.Route, s.Backend, s.Disposition = planner.RouteNative, router.BackendNative, router.DispositionExecute
		return s
	}
	report.Statements = []pgplan.Statement{
		native(pgplan.Statement{
			SQL:     "CREATE UNIQUE INDEX orders_email_key_new ON public.orders USING btree (lower(email))",
			ExecSQL: []string{"CREATE UNIQUE INDEX CONCURRENTLY orders_email_key_new ON public.orders USING btree (lower(email))"},
			Kind:    schemadiff.ChangeCreateIndex,
		}),
		native(pgplan.Statement{
			SQL:         "DROP INDEX public.orders_email_key",
			ExecSQL:     []string{"DROP INDEX CONCURRENTLY public.orders_email_key"},
			Kind:        schemadiff.ChangeDropIndex,
			Destructive: true,
			Drops:       &dropped,
			ReplacedBy:  "orders_email_key_new",
			Replacement: &replacement,
		}),
		native(pgplan.Statement{
			SQL:     "ALTER INDEX public.orders_email_key_new RENAME TO orders_email_key",
			ExecSQL: []string{"ALTER INDEX public.orders_email_key_new RENAME TO orders_email_key"},
			Kind:    schemadiff.ChangeRenameIndex,
		}),
	}

	changes, tiers, unrecognized, err := tableChanges(report, nil, parser)
	require.NoError(t, err)
	assert.Empty(t, unrecognized)
	require.Len(t, changes, 3)
	for _, change := range changes {
		assert.Empty(t, change.ExecutionMode, change.ModeReason)
		assert.Equal(t, "orders", change.Table)
	}
	assert.Equal(t, []preflight.Tier{preflight.TierIndexBuild, preflight.TierAlterInPlace, preflight.TierAlterInPlace}, tiers)

	assert.Equal(t, ddl.StatementCreateIndex, changes[0].Operation)
	assert.False(t, changes[0].IsUnsafe)

	drop := changes[1]
	assert.Equal(t, ddl.StatementDropIndex, drop.Operation)
	assert.True(t, drop.IsUnsafe)
	assert.Equal(t, `statement replaces index "orders_email_key" on table "orders" with the definition built as "orders_email_key_new": the old index is dropped only while that index is valid, and from then on its old definition no longer applies`, drop.UnsafeReason)
	recorded, err := reviewedDropFromMetadata(drop.Metadata)
	require.NoError(t, err)
	require.NotNil(t, recorded)
	assert.Equal(t, dropped, *recorded, "the apply still checks the old index before dropping it")
	admitted, err := indexReplacementForApply("app", drop, recorded)
	require.NoError(t, err)
	require.NotNil(t, admitted)
	assert.Equal(t, replacement, *admitted, "the apply checks the replacement before dropping the old index")
	assert.NotContains(t, changes[0].Metadata, indexReplacementMetadataKey, "only the drop carries the replacement record")
	assert.NotContains(t, changes[2].Metadata, indexReplacementMetadataKey, "only the drop carries the replacement record")

	rename := changes[2]
	assert.Equal(t, ddl.StatementAlterTable, rename.Operation)
	assert.Equal(t, "ALTER INDEX public.orders_email_key_new RENAME TO orders_email_key", rename.DDL)
	assert.False(t, rename.IsUnsafe)
}

// TestBlockRewriteStepsFailsClosedOnUnparseableStep proves an executable step
// whose statement the parser rejects fails the plan rather than being blocked
// or exempted on a guess: the tier derivation parsed every executable step
// already, so reaching one here that does not parse is an invariant violation.
func TestBlockRewriteStepsFailsClosedOnUnparseableStep(t *testing.T) {
	changes := []engine.TableChange{{Table: "users", DDL: "ALTER TABLE public.users ADD"}}

	err := blockRewriteSteps(changes, "size verdict", nil, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `classify planned statement for table "users"`)
	assert.Empty(t, changes[0].ExecutionMode)
}

// TestBlockMissingPrivilegesRequiresMatchingTiers proves a tier slice that
// does not pair one-to-one with the planned changes fails the plan closed
// instead of guessing which step needs which access.
func TestBlockMissingPrivilegesRequiresMatchingTiers(t *testing.T) {
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	changes := []engine.TableChange{{
		Table: "users",
		DDL:   "ALTER TABLE public.users ADD COLUMN email text",
	}}

	_, err := blockMissingPrivileges(t.Context(), nil, "orders_db", report, changes, nil, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "privilege tiers")
}

// TestBlockChangesAtTier proves a privilege refusal lands only on the steps
// that need the refused tier: steps at other tiers and steps already carrying
// a verdict keep their own reasons.
func TestBlockChangesAtTier(t *testing.T) {
	changes := []engine.TableChange{
		{Table: "users", DDL: "ALTER TABLE public.users ADD COLUMN email text"},
		{Table: "users", DDL: "CREATE INDEX CONCURRENTLY idx_users_email ON public.users (email)"},
		{Table: "users", DDL: "CREATE TABLE public.users (id bigint PRIMARY KEY)",
			ExecutionMode: engine.ExecutionModeBlocked, ModeReason: "creation shape verdict"},
	}
	tiers := []preflight.Tier{preflight.TierAlterInPlace, preflight.TierIndexBuild, 0}

	blockChangesAtTier(changes, tiers, preflight.TierIndexBuild, "index tier refused")

	assert.Empty(t, changes[0].ExecutionMode, "a step at another tier must keep its own verdict")
	assert.Equal(t, engine.ExecutionModeBlocked, changes[1].ExecutionMode)
	assert.Equal(t, "index tier refused", changes[1].ModeReason)
	assert.Equal(t, "creation shape verdict", changes[2].ModeReason,
		"an existing verdict must never be overwritten")
}

func TestBlockOversizedTableAppendsSizeCauseToPrivilegeBlocks(t *testing.T) {
	checkTable := func(context.Context, *pgxpool.Pool, string, string, int64) (preflight.PreflightedTable, error) {
		return preflight.PreflightedTable{}, &preflight.SizeError{TotalBytes: 2048, LimitBytes: 1024}
	}
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	changes := []engine.TableChange{
		{Table: "users", DDL: "ALTER TABLE public.users ADD COLUMN email text", ExecutionMode: engine.ExecutionModeBlocked, ModeReason: "missing ALTER privilege"},
		{Table: "users", DDL: "ALTER TABLE public.users ADD COLUMN phone text", ExecutionMode: engine.ExecutionModeBlocked, ModeReason: "missing ALTER privilege"},
	}

	changes, err := blockOversizedTableWithCheck(t.Context(), nil, "orders_db", report, changes, 1024, checkTable)
	require.NoError(t, err)
	changes, err = blockOversizedTableWithCheck(t.Context(), nil, "orders_db", report, changes, 1024, checkTable)
	require.NoError(t, err)
	for _, change := range changes {
		causes := engine.BlockedCauses(change.ModeReason)
		require.Len(t, causes, 2)
		assert.Equal(t, "missing ALTER privilege", causes[0])
		assert.True(t, strings.HasPrefix(causes[1], `statement for table "users": the table is 2.0 KiB with its indexes, over the 1.0 KiB limit`))
	}
}

// A table whose name contains the reserved cause separator is a schema
// author's choice, so a verdict that quotes it must still decode as the one
// cause the engine issued — not as that cause plus a forged second one.
func TestTableChangesNeutralizesCauseSeparatorInTableName(t *testing.T) {
	parser, err := ddl.ParserForDialect(schema.DialectPostgres)
	require.NoError(t, err)
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users ‖ forged cause"
	report.Statements = []pgplan.Statement{
		{SQL: `ALTER TABLE public."users ‖ forged cause" ALTER COLUMN email TYPE bigint`, Route: planner.RouteCopyAndSwap,
			Backend: router.BackendCopyAndSwap, Disposition: router.DispositionUnavailable},
	}

	changes, _, _, err := tableChanges(report, nil, parser)
	require.NoError(t, err)
	require.Len(t, changes, 1)
	assert.Equal(t, engine.ExecutionModeBlocked, changes[0].ExecutionMode)
	causes := engine.BlockedCauses(changes[0].ModeReason)
	require.Len(t, causes, 1)
	assert.Contains(t, causes[0], `"users // forged cause"`)
}

func TestBlockOversizedTableNeutralizesCauseSeparatorInTableName(t *testing.T) {
	checkTable := func(context.Context, *pgxpool.Pool, string, string, int64) (preflight.PreflightedTable, error) {
		return preflight.PreflightedTable{}, &preflight.SizeError{TotalBytes: 2048, LimitBytes: 1024}
	}
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users ‖ forged cause"
	changes := []engine.TableChange{
		{Table: report.Table, DDL: `ALTER TABLE public."users ‖ forged cause" ADD COLUMN email text`},
	}

	changes, err := blockOversizedTableWithCheck(t.Context(), nil, "orders_db", report, changes, 1024, checkTable)
	require.NoError(t, err)
	require.Len(t, changes, 1)
	causes := engine.BlockedCauses(changes[0].ModeReason)
	require.Len(t, causes, 1)
	assert.True(t, strings.HasPrefix(causes[0], `statement for table "users // forged cause": the table is 2.0 KiB with its indexes, over the 1.0 KiB limit`), causes[0])
}

// The dependency verdict quotes the absent table's name, which a schema author
// chose, so a name carrying the reserved separator still decodes as one cause.
func TestBlockAbsentTableDependentsNeutralizesCauseSeparatorInTableName(t *testing.T) {
	const table = "users ‖ forged cause"
	changes := []engine.TableChange{
		{Table: table, DDL: `ALTER TABLE public."users ‖ forged cause" ADD COLUMN email text`},
	}
	tiers := []preflight.Tier{preflight.TierAlterInPlace}

	blockAbsentTableDependents(changes, tiers, table)

	require.Equal(t, engine.ExecutionModeBlocked, changes[0].ExecutionMode)
	causes := engine.BlockedCauses(changes[0].ModeReason)
	require.Len(t, causes, 1, "the dependency verdict is one cause: %q", changes[0].ModeReason)
	assert.Contains(t, causes[0], `"users // forged cause"`)
}

func TestBlockOversizedTableKeepsBlockedConcurrentIndexReason(t *testing.T) {
	checkTable := func(context.Context, *pgxpool.Pool, string, string, int64) (preflight.PreflightedTable, error) {
		return preflight.PreflightedTable{}, &preflight.SizeError{TotalBytes: 2048, LimitBytes: 1024}
	}
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	changes := []engine.TableChange{
		{Table: "users", DDL: "ALTER TABLE public.users ADD COLUMN email text", ExecutionMode: engine.ExecutionModeBlocked, ModeReason: "missing ALTER privilege"},
		{Table: "users", DDL: "CREATE INDEX CONCURRENTLY users_email_idx ON public.users (email)", ExecutionMode: engine.ExecutionModeBlocked, ModeReason: "missing CREATE privilege"},
	}

	changes, err := blockOversizedTableWithCheck(t.Context(), nil, "orders_db", report, changes, 1024, checkTable)
	require.NoError(t, err)
	assert.Len(t, engine.BlockedCauses(changes[0].ModeReason), 2)
	assert.Equal(t, "missing CREATE privilege", changes[1].ModeReason)
}

func TestBlockOversizedTableKeepsBlockedReasonsWhenLookupFails(t *testing.T) {
	checkTable := func(context.Context, *pgxpool.Pool, string, string, int64) (preflight.PreflightedTable, error) {
		return preflight.PreflightedTable{}, errors.New("permission denied")
	}
	var logs bytes.Buffer
	originalLogger := slog.Default()
	t.Cleanup(func() { slog.SetDefault(originalLogger) })
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Schema = "public"
	report.Table = "users"
	changes := []engine.TableChange{{Table: "users", DDL: "ALTER TABLE public.users ADD COLUMN email text", ExecutionMode: engine.ExecutionModeBlocked, ModeReason: "missing ALTER privilege"}}

	changes, err := blockOversizedTableWithCheck(t.Context(), nil, "orders_db", report, changes, 1024, checkTable)
	require.NoError(t, err)
	assert.Equal(t, "missing ALTER privilege", changes[0].ModeReason)
	assert.Contains(t, logs.String(), "level=WARN")
	assert.Contains(t, logs.String(), "existing blocked plan verdicts remain unchanged")
}

// A table the plan leaves untouched has no step a size verdict could land on,
// so the gate must not spend a catalog round trip on it.
func TestBlockOversizedTableSkipsUntouchedTable(t *testing.T) {
	checkTable := func(context.Context, *pgxpool.Pool, string, string, int64) (preflight.PreflightedTable, error) {
		t.Fatal("size lookup must not run for a table with no planned steps")
		return preflight.PreflightedTable{}, nil
	}
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Schema = "public"
	report.Table = "users"

	changes, err := blockOversizedTableWithCheck(t.Context(), nil, "orders_db", report, nil, 1024, checkTable)
	require.NoError(t, err)
	assert.Empty(t, changes)
}

// When every step is already blocked, a refusal about the table itself rather
// than its size has nothing executable left to block; the existing verdicts
// stand and the refusal is logged so the catalog answer is not lost.
func TestBlockOversizedTableLogsNonSizeRefusalOnFullyBlockedPlan(t *testing.T) {
	checkTable := func(context.Context, *pgxpool.Pool, string, string, int64) (preflight.PreflightedTable, error) {
		return preflight.PreflightedTable{}, fmt.Errorf("%w: public.users has relkind %q", preflight.ErrNotTable, "v")
	}
	var logs bytes.Buffer
	originalLogger := slog.Default()
	t.Cleanup(func() { slog.SetDefault(originalLogger) })
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Schema = "public"
	report.Table = "users"
	changes := []engine.TableChange{{Table: "users", DDL: "ALTER TABLE public.users ADD COLUMN email text", ExecutionMode: engine.ExecutionModeBlocked, ModeReason: "missing ALTER privilege"}}

	changes, err := blockOversizedTableWithCheck(t.Context(), nil, "orders_db", report, changes, 1024, checkTable)
	require.NoError(t, err)
	assert.Equal(t, "missing ALTER privilege", changes[0].ModeReason)
	assert.Contains(t, logs.String(), "level=WARN")
	assert.Contains(t, logs.String(), "size check refused the table")
	assert.Contains(t, logs.String(), "database=orders_db")
	assert.Contains(t, logs.String(), "refusal=not-a-table")
}

func TestBlockOversizedTableKeepsFullyBlockedPlanWhenStepCannotBeClassified(t *testing.T) {
	checkTable := func(context.Context, *pgxpool.Pool, string, string, int64) (preflight.PreflightedTable, error) {
		return preflight.PreflightedTable{}, &preflight.SizeError{TotalBytes: 2048, LimitBytes: 1024}
	}
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Schema = "public"
	report.Table = "users"
	want := []engine.TableChange{{
		Table:         "users",
		DDL:           "ALTER TABLE public.users ADD COLUMN email text; ALTER TABLE public.users ADD COLUMN phone text",
		ExecutionMode: engine.ExecutionModeBlocked,
		ModeReason:    "earlier gate verdict",
	}}
	changes := append([]engine.TableChange(nil), want...)

	changes, err := blockOversizedTableWithCheck(t.Context(), nil, "orders_db", report, changes, 1024, checkTable)
	require.NoError(t, err)
	assert.Equal(t, want, changes)
}

func TestBlockOversizedTableFailsWhenExecutableStepCannotBeClassified(t *testing.T) {
	checkTable := func(context.Context, *pgxpool.Pool, string, string, int64) (preflight.PreflightedTable, error) {
		return preflight.PreflightedTable{}, &preflight.SizeError{TotalBytes: 2048, LimitBytes: 1024}
	}
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	changes := []engine.TableChange{{
		Table: "users",
		DDL:   "ALTER TABLE public.users ADD COLUMN email text; ALTER TABLE public.users ADD COLUMN phone text",
	}}

	_, err := blockOversizedTableWithCheck(t.Context(), nil, "orders_db", report, changes, 1024, checkTable)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `classify planned statement for table "users"`)
}

func TestBlockOversizedTableBlocksExecutableRewrite(t *testing.T) {
	checkTable := func(context.Context, *pgxpool.Pool, string, string, int64) (preflight.PreflightedTable, error) {
		return preflight.PreflightedTable{}, &preflight.SizeError{TotalBytes: 2048, LimitBytes: 1024}
	}
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	changes := []engine.TableChange{{Table: "users", DDL: "ALTER TABLE public.users ADD COLUMN email text"}}

	changes, err := blockOversizedTableWithCheck(t.Context(), nil, "orders_db", report, changes, 1024, checkTable)
	require.NoError(t, err)
	assert.Equal(t, engine.ExecutionModeBlocked, changes[0].ExecutionMode)
	assert.Equal(t, `statement for table "users": the table is 2.0 KiB with its indexes, over the 1.0 KiB limit SchemaBot sets for a native-safe apply, not a PostgreSQL limit; raise postgres.native_safe_table_size_limit_bytes to change a table this size, then plan again`, changes[0].ModeReason)
}

// A step whose every planner decision is proven catalog-only stays executable
// on an oversized table: it rewrites no rows, so its cost does not grow with
// the table the ceiling measures.
func TestBlockOversizedTableAdmitsProvenCatalogOnlyStep(t *testing.T) {
	checkTable := func(context.Context, *pgxpool.Pool, string, string, int64) (preflight.PreflightedTable, error) {
		return preflight.PreflightedTable{}, &preflight.SizeError{TotalBytes: 2048, LimitBytes: 1024}
	}
	const ddl = "ALTER TABLE public.users ADD COLUMN email text"
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	report.Statements = []pgplan.Statement{{
		ExecSQL:   []string{ddl},
		Decisions: []planner.Decision{{ProvenCatalogOnly: true}},
	}}
	changes := []engine.TableChange{{Table: "users", DDL: ddl}}

	changes, err := blockOversizedTableWithCheck(t.Context(), nil, "orders_db", report, changes, 1024, checkTable)
	require.NoError(t, err)
	assert.Empty(t, changes[0].ExecutionMode)
	assert.Empty(t, changes[0].ModeReason)
}

// One unproven decision is enough to keep the ceiling on a step, since the
// exemption holds only when the whole statement writes nothing but the catalog.
func TestBlockOversizedTableBlocksStepWithAnUnprovenDecision(t *testing.T) {
	checkTable := func(context.Context, *pgxpool.Pool, string, string, int64) (preflight.PreflightedTable, error) {
		return preflight.PreflightedTable{}, &preflight.SizeError{TotalBytes: 2048, LimitBytes: 1024}
	}
	const ddl = "ALTER TABLE public.users ADD COLUMN email text, ALTER COLUMN id TYPE numeric"
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	report.Statements = []pgplan.Statement{{
		ExecSQL:   []string{ddl},
		Decisions: []planner.Decision{{ProvenCatalogOnly: true}, {ProvenCatalogOnly: false}},
	}}
	changes := []engine.TableChange{{Table: "users", DDL: ddl}}

	changes, err := blockOversizedTableWithCheck(t.Context(), nil, "orders_db", report, changes, 1024, checkTable)
	require.NoError(t, err)
	assert.Equal(t, engine.ExecutionModeBlocked, changes[0].ExecutionMode)
	assert.Contains(t, changes[0].ModeReason, "over the 1.0 KiB limit")
}

// catalogOnlySteps exempts only statements whose decisions are all proven, and
// a statement with no decisions proves nothing.
func TestCatalogOnlyStepsRequiresEveryDecisionProven(t *testing.T) {
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Statements = []pgplan.Statement{
		{ExecSQL: []string{"ALTER TABLE a ADD COLUMN x text"}, Decisions: []planner.Decision{{ProvenCatalogOnly: true}}},
		{ExecSQL: []string{"ALTER TABLE b ADD COLUMN y text"}, Decisions: []planner.Decision{{ProvenCatalogOnly: true}, {}}},
		{ExecSQL: []string{"ALTER TABLE c ADD COLUMN z text"}},
	}

	assert.Equal(t, map[string]bool{"ALTER TABLE a ADD COLUMN x text": true}, catalogOnlySteps(report))
}

// An executable step without a named target fails the plan closed because a
// size check cannot answer for an unidentified table.
func TestBlockOversizedTableRequiresTargetTable(t *testing.T) {
	report := pgplan.NewReport(pgplan.SourceDiff)
	changes := []engine.TableChange{{
		Table: "users",
		DDL:   "ALTER TABLE public.users ADD COLUMN email text",
	}}

	_, err := blockOversizedTable(t.Context(), nil, "orders_db", report, changes, 1)
	require.Error(t, err)
	assert.EqualError(t, err, "plan report carries executable steps but names no target table")
}

func TestBlockOversizedTableKeepsFullyBlockedPlanWithoutTargetTable(t *testing.T) {
	checkTable := func(context.Context, *pgxpool.Pool, string, string, int64) (preflight.PreflightedTable, error) {
		t.Fatal("size lookup must not run without a target table")
		return preflight.PreflightedTable{}, nil
	}
	report := pgplan.NewReport(pgplan.SourceDiff)
	want := []engine.TableChange{{
		Table:         "users",
		DDL:           "ALTER TABLE public.users ADD COLUMN email text",
		ExecutionMode: engine.ExecutionModeBlocked,
		ModeReason:    "earlier gate verdict",
	}}
	changes := append([]engine.TableChange(nil), want...)

	changes, err := blockOversizedTableWithCheck(t.Context(), nil, "orders_db", report, changes, 1024, checkTable)
	require.NoError(t, err)
	assert.Equal(t, want, changes)
}

// An operational size-check failure fails the plan closed instead of leaving
// the step executable. A non-positive limit is rejected before catalog access.
func TestBlockOversizedTableFailsClosedOnCheckError(t *testing.T) {
	report := pgplan.NewReport(pgplan.SourceDiff)
	report.Table = "users"
	changes := []engine.TableChange{{
		Table: "users",
		DDL:   "ALTER TABLE public.users ADD COLUMN email text",
	}}

	_, err := blockOversizedTable(t.Context(), nil, "orders_db", report, changes, -1)
	require.Error(t, err)
	assert.EqualError(t, err, `check size for table "users": size limit must be positive, got -1`)
}

func TestPlanRejectsInvalidInputsBeforeConnecting(t *testing.T) {
	eng := New()

	_, err := eng.Plan(t.Context(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "request is required")

	_, err = eng.Plan(t.Context(), &engine.PlanRequest{Database: "app"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DSN credentials are required")
}

// Two schema files in one namespace that declare the same table give the plan
// no single desired definition: diffing each on its own would drop from the
// live table whatever only the other file declares. The namespace planner
// fails before diffing any file, naming the table and both files.
func TestPlanSchemasRefusesTableDeclaredByTwoFiles(t *testing.T) {
	req := &engine.PlanRequest{
		Database: "orders_db",
		SchemaFiles: schema.SchemaFiles{
			"public": {Files: map[string]string{
				"users.sql":         "CREATE TABLE users (id bigint PRIMARY KEY, email text NOT NULL)",
				"users_profile.sql": "CREATE TABLE users (id bigint PRIMARY KEY, display_name text)",
			}},
		},
	}

	// The nil pool proves the refusal lands before any file is diffed.
	_, err := planSchemas(t.Context(), nil, req, DefaultNativeSafeTableSizeLimitBytes, "")
	require.Error(t, err)
	assert.EqualError(t, err, `plan PostgreSQL namespace "public": table "users" is declared by both schema files "users.sql" and "users_profile.sql". Declare each table in exactly one schema file`)
}

// Table identity follows PostgreSQL's parser: an unquoted name folds to lower
// case, so Users and users are one table, while a quoted "Users" is a
// different table. A file declaring row security names its table through the
// same admission parse, so it collides with a plain file for that table too.
func TestRefuseTableDeclaredTwice(t *testing.T) {
	tests := []struct {
		name    string
		files   map[string]string
		wantErr string
	}{
		{
			name: "different tables plan independently",
			files: map[string]string{
				"orders.sql": "CREATE TABLE orders (id bigint PRIMARY KEY)",
				"users.sql":  "CREATE TABLE users (id bigint PRIMARY KEY)",
			},
		},
		{
			name: "quoted mixed-case name is a different table",
			files: map[string]string{
				"a.sql": `CREATE TABLE "Users" (id bigint PRIMARY KEY)`,
				"b.sql": "CREATE TABLE users (id bigint PRIMARY KEY)",
			},
		},
		{
			name: "unquoted names fold to the same table",
			files: map[string]string{
				"a.sql": "CREATE TABLE Users (id bigint PRIMARY KEY)",
				"b.sql": "CREATE TABLE users (id bigint PRIMARY KEY, email text)",
			},
			wantErr: `plan PostgreSQL namespace "public": table "users" is declared by both schema files "a.sql" and "b.sql". Declare each table in exactly one schema file`,
		},
		{
			name: "row security file collides with a plain file",
			files: map[string]string{
				"accounts.sql":     "CREATE TABLE accounts (id bigint PRIMARY KEY)",
				"accounts_rls.sql": "CREATE TABLE accounts (id bigint PRIMARY KEY); ALTER TABLE accounts ENABLE ROW LEVEL SECURITY",
			},
			wantErr: `plan PostgreSQL namespace "public": table "accounts" is declared by both schema files "accounts.sql" and "accounts_rls.sql". Declare each table in exactly one schema file`,
		},
		{
			name: "unparseable file names the file",
			files: map[string]string{
				"users.sql": "CREATE TABLE users (id bigint PRIMARY KEY",
			},
			wantErr: `plan PostgreSQL schema in "public"/"users.sql"`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := parseDesiredFiles("public", tc.files)
			if err == nil {
				err = refuseTableDeclaredTwice("public", parsed)
			}
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// Lifecycle controls without a PostgreSQL execution phase decline with a typed unsupported-operation error:
// PostgreSQL DDL runs each statement as a single transactional statement with
// no engine phase to pause, resume, swap, revert, or retune. The typed decline
// is what lets the durable control path resolve a request terminally instead
// of retrying a rejection forever while the schema change keeps executing.
func TestLifecycleControlsDeclineAsUnsupported(t *testing.T) {
	eng := New()

	tests := []struct {
		name string
		call func(t *testing.T) error
	}{
		{"stop", func(t *testing.T) error {
			result, err := eng.Stop(t.Context(), &engine.ControlRequest{})
			assert.Nil(t, result)
			return err
		}},
		{"start", func(t *testing.T) error {
			result, err := eng.Start(t.Context(), &engine.ControlRequest{})
			assert.Nil(t, result)
			return err
		}},
		{"cutover", func(t *testing.T) error {
			result, err := eng.Cutover(t.Context(), &engine.ControlRequest{})
			assert.Nil(t, result)
			return err
		}},
		{"revert", func(t *testing.T) error {
			result, err := eng.Revert(t.Context(), &engine.ControlRequest{})
			assert.Nil(t, result)
			return err
		}},
		{"skip-revert", func(t *testing.T) error {
			result, err := eng.SkipRevert(t.Context(), &engine.ControlRequest{})
			assert.Nil(t, result)
			return err
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call(t)
			require.Error(t, err)
			assert.True(t, engine.IsUnsupportedOperation(err),
				"the decline must be typed so durable control consumers resolve it terminally")
		})
	}
}

// The PostgreSQL engine runs each statement in a goroutine of this process and
// claims the tracked progress before Apply returns, so it declares its work
// registration synchronous. A driver reads that declaration to decide whether
// a pending progress report about work it believes is in flight is conclusive.
func TestRegistersWorkSynchronously(t *testing.T) {
	eng := New()

	assert.True(t, eng.RegistersWorkSynchronously(),
		"the engine claims the tracked schema change before Apply returns")
	assert.True(t, engine.RegistersWorkSynchronously(eng),
		"the package helper resolves the engine's declaration")
}

// optionalCapabilityVerdicts records, per optional interface package engine
// declares, whether the PostgreSQL engine implements it. The engine capability
// mapping in docs/postgresql.md states the same verdicts in prose.
var optionalCapabilityVerdicts = map[reflect.Type]bool{
	reflect.TypeFor[engine.Drainer]():                         true,
	reflect.TypeFor[engine.ShutdownHalter]():                  true,
	reflect.TypeFor[engine.OwnedWorkHalter]():                 true,
	reflect.TypeFor[engine.SynchronousWorkRegistration]():     true,
	reflect.TypeFor[engine.DeferredCutoverSignalChecker]():    false,
	reflect.TypeFor[engine.ExternallyAuthoritativeProgress](): false,
	reflect.TypeFor[engine.CancelledArtifactReleaser]():       true,
	reflect.TypeFor[engine.FailedArtifactReleaser]():          true,
	reflect.TypeFor[engine.CancelledResumeStateReclaimer]():   false,
	reflect.TypeFor[engine.ControlResumeValidator]():          false,
	reflect.TypeFor[engine.ShardKeyedPlanning]():              false,
	reflect.TypeFor[engine.TargetDrainer]():                   false,
}

// TestOptionalCapabilitySet checks the recorded verdicts against the engine's
// method set and against the optional interfaces package engine declares: a
// verdict the engine does not hold fails, and so does an optional interface
// without a verdict, so the verdict table cannot drift from package engine
// unnoticed. Whether docs/postgresql.md agrees with the verdicts is checked
// separately by TestCapabilityMappingMatchesEngine.
func TestOptionalCapabilitySet(t *testing.T) {
	inventory := enginetest.OptionalCapabilities(t)
	engineType := reflect.TypeOf(New())
	for typ, implements := range optionalCapabilityVerdicts {
		assert.True(t, inventory[typ.Name()],
			"verdict names %s, which package engine no longer declares as an optional interface", typ.Name())
		delete(inventory, typ.Name())
		assert.Equal(t, implements, engineType.Implements(typ), "PostgreSQL engine verdict for %s", typ.Name())
	}
	assert.Empty(t, inventory,
		"optional engine capabilities without a PostgreSQL verdict; add the verdict here and its row to the capability mapping in docs/postgresql.md")
}

// TestCapabilityMappingMatchesEngine reads the engine capability mapping table
// in docs/postgresql.md and checks its verdict column against the engine: an
// optional interface's row must say "Implemented" exactly when the engine
// implements it and "Not applicable; not implemented" otherwise, and every
// method of the required engine.Engine interface must have a row. The Why and
// Where columns are prose and are not checked.
func TestCapabilityMappingMatchesEngine(t *testing.T) {
	rows := capabilityMappingRows(t)

	for typ, implements := range optionalCapabilityVerdicts {
		prefix := "engine." + typ.Name() + "."
		var matched []string
		for method, verdict := range rows {
			if strings.HasPrefix(method, prefix) {
				matched = append(matched, method)
				if implements {
					assert.True(t, strings.HasPrefix(verdict, "Implemented"),
						"docs/postgresql.md row %s says %q but the engine implements %s", method, verdict, typ.Name())
				} else {
					assert.True(t, strings.HasPrefix(verdict, "Not applicable; not implemented"),
						"docs/postgresql.md row %s says %q but the engine does not implement %s", method, verdict, typ.Name())
				}
			}
		}
		assert.NotEmpty(t, matched, "docs/postgresql.md has no capability mapping row for %s", typ.Name())
	}

	engineType := reflect.TypeFor[engine.Engine]()
	for method := range engineType.Methods() {
		method := "engine.Engine." + method.Name
		assert.Contains(t, rows, method, "docs/postgresql.md has no capability mapping row for %s", method)
	}
}

// capabilityMappingRows returns the engine capability mapping table from
// docs/postgresql.md keyed by the interface method in its first column, with
// the PostgreSQL behavior column as the value.
func capabilityMappingRows(t *testing.T) map[string]string {
	t.Helper()

	doc, err := os.ReadFile(filepath.Join("..", "..", "..", "docs", "postgresql.md"))
	require.NoError(t, err)

	rows := make(map[string]string)
	inSection := false
	for line := range strings.SplitSeq(string(doc), "\n") {
		if strings.HasPrefix(line, "## ") {
			inSection = line == "## Engine capability mapping"
			continue
		}
		if !inSection || !strings.HasPrefix(line, "| `engine.") {
			continue
		}
		cells := strings.Split(line, "|")
		require.GreaterOrEqual(t, len(cells), 4, "capability mapping row has too few columns: %s", line)
		method := strings.Trim(strings.TrimSpace(cells[1]), "`")
		rows[method] = strings.TrimSpace(cells[2])
	}
	require.NotEmpty(t, rows, "no engine capability mapping rows found in docs/postgresql.md")
	return rows
}

// A zero ceiling means unset and adopts the default, so a zero-valued client
// config preserves the stock ceiling instead of disabling the size guard.
func TestNewWithTableSizeLimitTreatsZeroAsUnset(t *testing.T) {
	assert.Equal(t, DefaultNativeSafeTableSizeLimitBytes, NewWithTableSizeLimit(0).TableSizeLimit())
	assert.Equal(t, int64(42), NewWithTableSizeLimit(42).TableSizeLimit())
}

// A baseline render never runs more introspections than the pool can hold
// connections for, and never more than the cap, whichever is smaller.
func TestBaselineIntrospectionLimitIsBoundedByPoolAndCap(t *testing.T) {
	tests := []struct {
		name     string
		maxConns int32
		want     int
	}{
		{name: "pool smaller than cap", maxConns: 2, want: 2},
		{name: "pool equal to cap", maxConns: baselineIntrospectionConcurrency, want: baselineIntrospectionConcurrency},
		{name: "pool larger than cap", maxConns: baselineIntrospectionConcurrency * 4, want: baselineIntrospectionConcurrency},
		{name: "single-connection pool", maxConns: 1, want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := pgxpool.ParseConfig("postgres://baseline@localhost:1/limit_test")
			require.NoError(t, err)
			cfg.MaxConns = tt.maxConns
			cfg.MinConns = 0
			pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
			require.NoError(t, err)
			defer pool.Close()

			assert.Equal(t, tt.want, baselineIntrospectionLimit(pool))
		})
	}
}

func TestPullNamespacesRejectsReservedSchema(t *testing.T) {
	_, err := pullNamespaces(t.Context(), nil, "pg_catalog")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `schema "pg_catalog" is reserved and cannot be pulled`)
}

func TestPullSchemaRequiresConfiguredCredentials(t *testing.T) {
	_, err := New().PullSchema(t.Context(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "request is required")

	_, err = New().PullSchema(t.Context(), &ternv1.PullSchemaRequest{Database: "orders"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DSN credentials are required")
}

func TestPullSchemaRejectsDetailedCatalog(t *testing.T) {
	_, err := New().PullSchema(t.Context(), &ternv1.PullSchemaRequest{
		Database:      "orders",
		CatalogDetail: ternv1.PullCatalogDetail_PULL_CATALOG_DETAIL_DETAILED,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "catalog detail")
	assert.Contains(t, err.Error(), "unsupported; use basic")
}

// PostgreSQL does not honor drop_tables, so a plan that carries the list is
// refused before the engine connects to the target, rather than planned as if
// the list were empty and the listed tables silently left in place.
func TestPlanRefusesDropTablesBeforeConnecting(t *testing.T) {
	result, err := New().Plan(t.Context(), &engine.PlanRequest{
		Database:    "app",
		Credentials: &engine.Credentials{DSN: "postgres://localhost/app"},
		DropTables:  []string{"_orders_old"},
	})
	require.EqualError(t, err, `plan PostgreSQL database "app": drop_tables is supported only on MySQL, and this is a PostgreSQL target. Remove drop_tables from schemabot.yaml`)
	assert.Nil(t, result)
}

// A column change PostgreSQL refuses because other objects read the column
// names those objects and what to do with them, so the operator does not
// have to search the catalog for what blocks the change.
func TestExecutionVerdictNamesTheObjectsThatBlockAColumnChange(t *testing.T) {
	refused := func(d planner.Decision) pgplan.Statement {
		return pgplan.Statement{Disposition: router.DispositionRefuse, Decisions: []planner.Decision{d}}
	}
	cases := []struct {
		name     string
		decision planner.Decision
		want     string
	}{
		{
			name:     "a view",
			decision: planner.Decision{Reason: planner.ReasonDependentView, DependentViews: []string{`"public"."order_remarks"`}},
			want:     `statement for table "orders" is refused: it changes or drops a column that view "public"."order_remarks" reads; drop or redefine the view outside SchemaBot, then plan again`,
		},
		{
			name:     "two views",
			decision: planner.Decision{Reason: planner.ReasonDependentView, DependentViews: []string{`"public"."a"`, `"public"."b"`}},
			want:     `statement for table "orders" is refused: it changes or drops a column that views "public"."a", "public"."b" read; drop or redefine the views outside SchemaBot, then plan again`,
		},
		{
			name: "a policy on a dropped column",
			decision: planner.Decision{Reason: planner.ReasonDependentPolicy, Kind: pgstatement.OpDropColumn, DependentFix: planner.DependentFixRewriteDependents,
				DependentPolicies: []string{`"owner_only" ON "public"."orders"`}},
			want: `statement for table "orders" is refused: it drops a column that row security policy "owner_only" ON "public"."orders" reads; change the policy so it no longer reads the column and apply that first, then plan again`,
		},
		{
			name: "a policy on a type change that runs in place",
			decision: planner.Decision{Reason: planner.ReasonDependentPolicy, Kind: pgstatement.OpAlterColumnType, DependentFix: planner.DependentFixRecreateAroundChange,
				DependentPolicies: []string{`"owner_only" ON "public"."orders"`}},
			want: `statement for table "orders" is refused: it changes the type of a column that row security policy "owner_only" ON "public"."orders" reads, which SchemaBot cannot run yet; the change does not rewrite the table, so outside SchemaBot, through a separately reviewed process, drop the policy, change the column, and recreate it in one transaction, which never leaves the table without it, then plan again`,
		},
		{
			name: "a policy and a trigger on a type change that rewrites the table",
			decision: planner.Decision{Reason: planner.ReasonDependentPolicy, Kind: pgstatement.OpAlterColumnType, DependentFix: planner.DependentFixExpandContract,
				DependentPolicies: []string{`"owner_only" ON "public"."orders"`}, DependentTriggers: []string{`"audit" ON "public"."orders"`}},
			want: `statement for table "orders" is refused: it changes the type of a column that row security policy "owner_only" ON "public"."orders" and trigger "audit" ON "public"."orders" read and rewrites the table, which SchemaBot cannot run yet; add a column of the new type, have the application write to both while you backfill it, move the policy and the trigger (policies in the file, triggers outside SchemaBot) and reads to it, and drop the old column once unused, each as its own change, then plan again`,
		},
		{
			name: "a trigger on a type change that rewrites the table",
			decision: planner.Decision{Reason: planner.ReasonDependentTrigger, Kind: pgstatement.OpAlterColumnType, DependentFix: planner.DependentFixExpandContract,
				DependentTriggers: []string{`"audit" ON "public"."orders"`}},
			want: `statement for table "orders" is refused: it changes the type of a column that trigger "audit" ON "public"."orders" reads and rewrites the table, which SchemaBot cannot run yet; add a column of the new type, have the application write to both while you backfill it, move the trigger outside SchemaBot and reads to it, and drop the old column once unused, each as its own change, then plan again`,
		},
		{
			name: "two triggers on a dropped column",
			decision: planner.Decision{Reason: planner.ReasonDependentTrigger, Kind: pgstatement.OpDropColumn, DependentFix: planner.DependentFixRewriteDependents,
				DependentTriggers: []string{`"audit" ON "public"."orders"`, `"stamp" ON "public"."orders"`}},
			want: `statement for table "orders" is refused: it drops a column that triggers "audit" ON "public"."orders", "stamp" ON "public"."orders" read; outside SchemaBot, change the triggers so they no longer read the column first, then plan again`,
		},
		{
			name: "a policy and a trigger on a dropped column",
			decision: planner.Decision{Reason: planner.ReasonDependentPolicy, Kind: pgstatement.OpDropColumn, DependentFix: planner.DependentFixRewriteDependents,
				DependentPolicies: []string{`"owner_only" ON "public"."orders"`}, DependentTriggers: []string{`"audit" ON "public"."orders"`}},
			want: `statement for table "orders" is refused: it drops a column that row security policy "owner_only" ON "public"."orders" and trigger "audit" ON "public"."orders" read; change the policy and the trigger so they no longer read the column first, the policies in the schema file and the triggers outside SchemaBot, then plan again`,
		},
		{
			name: "a policy with a manual path this build has no sentence for",
			decision: planner.Decision{Reason: planner.ReasonDependentPolicy, Kind: pgstatement.OpAlterColumnType, DependentFix: planner.DependentFix("a-later-path"),
				DependentPolicies: []string{`"owner_only" ON "public"."orders"`}},
			want: `statement for table "orders" is refused: it changes or drops a column that row security policy "owner_only" ON "public"."orders" reads, which SchemaBot cannot run yet; make the change outside SchemaBot through a separately reviewed process, then plan again`,
		},
		{
			name: "a generated column on a type change",
			decision: planner.Decision{Reason: planner.ReasonDependentGeneratedColumn, Kind: pgstatement.OpAlterColumnType,
				DependentGeneratedColumns: []string{`"public"."orders"."total"`}},
			want: `statement for table "orders" is refused: it changes the type of a column that generated column "public"."orders"."total" reads, which SchemaBot cannot run yet: PostgreSQL refuses the change while it stands, and adding a stored generated column back rewrites the table; to keep it, outside SchemaBot, through a separately reviewed process, drop it, change the column, and add it back in one transaction, which blocks reads and writes while the table is rewritten, then plan again`,
		},
		{
			name: "a generated column on a dropped column",
			decision: planner.Decision{Reason: planner.ReasonDependentGeneratedColumn, Kind: pgstatement.OpDropColumn,
				DependentGeneratedColumns: []string{`"public"."orders"."total"`}},
			want: `statement for table "orders" is refused: it drops a column that generated column "public"."orders"."total" reads; if it should go too, remove it from the file and apply that first, or to keep it, make the change outside SchemaBot through a separately reviewed process, then plan again`,
		},
		{
			name:     "a partition key",
			decision: planner.Decision{Reason: planner.ReasonPartitionKey, PartitionKeyTables: []string{`"public"."orders"`}},
			want:     `statement for table "orders" is refused: it changes or drops a column in the partition key of "public"."orders", which PostgreSQL never allows; create a new partitioned table with the new key and move the rows into it, then plan again`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := executionVerdict(pgplan.FormatVersion, refused(tc.decision), "orders", nil)
			assert.Equal(t, engine.ExecutionModeBlocked, got.mode)
			assert.Equal(t, tc.want, got.reason)
			assert.Nil(t, got.unrecognized)
		})
	}
}
