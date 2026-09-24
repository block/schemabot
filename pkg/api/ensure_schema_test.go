package api

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/schema"
)

// closedPortDSN points at a closed loopback port: any connection attempt fails
// instantly with a refused connection rather than hanging, so tests using it
// never depend on a real database and stay fast.
const closedPortDSN = "user:pass@tcp(127.0.0.1:1)/schemabot"

// EnsureSchema routes to the schema bootstrapper for the storage database's
// dialect and fails closed for a dialect that has none: it must return an
// error naming the dialect before touching the database, never fall back to
// running another family's flow against the storage database. The DSN
// points at a closed loopback port so any accidental connection attempt fails
// the test rather than hanging.
func TestEnsureSchemaFailsClosedForUnsupportedDialect(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.DiscardHandler)

	unsupported := schema.Dialect("sqlite")
	err := EnsureSchema(closedPortDSN, logger, WithDialect(unsupported))

	require.Error(t, err)
	require.ErrorContains(t, err, "no schema bootstrapper")
	require.ErrorContains(t, err, string(unsupported))
}

// EnsureSchema with the postgres dialect routes into the PostgreSQL
// bootstrapper. The closed-port DSN makes its connection attempt fail
// instantly, and the resulting error proves the dialect routed into the
// PostgreSQL flow rather than the fail-closed branch.
func TestEnsureSchemaRoutesPostgresDialect(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.DiscardHandler)

	err := EnsureSchema("postgres://user:pass@127.0.0.1:1/schemabot", logger, WithDialect(schema.DialectPostgres))

	require.Error(t, err)
	require.NotContains(t, err.Error(), "no schema bootstrapper")
	require.ErrorContains(t, err, "ping storage database")
}

// EnsureSchema without a dialect option defaults to the MySQL bootstrapper —
// the behavior every existing call site relies on. The closed-port DSN makes
// the MySQL flow's connection attempt fail instantly, and the resulting error
// proves the default routed into the MySQL flow rather than the fail-closed
// branch: a zero-value dialect would return "no schema bootstrapper" and
// crash-loop every pod at startup.
func TestEnsureSchemaDefaultsToMySQLDialect(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.DiscardHandler)

	err := EnsureSchema(closedPortDSN, logger)

	require.Error(t, err)
	require.NotContains(t, err.Error(), "no schema bootstrapper")
	require.ErrorContains(t, err, "plan schema")
}

// partitionDestructiveChanges gates on the verdict the plan already carries,
// so these cases pin what it does with that verdict rather than restating the
// linter registry that produces it: an unsafe statement is refused whole and
// carries the plan's own reason, a safe one executes, and the two are sorted
// per statement so one refusal does not withhold another table's change.
// Whether a given statement is unsafe is the engine's answer, exercised
// end-to-end against a real plan in the integration tests.
func TestPartitionDestructiveChangesHonorsThePlanVerdict(t *testing.T) {
	t.Parallel()

	const mixedDDL = "ALTER TABLE `applies` ADD COLUMN `caller` VARCHAR(64), DROP INDEX `idx_state`"

	unsafeChange := engine.TableChange{
		Table:        "applies",
		Operation:    ddl.StatementAlterTable,
		DDL:          mixedDDL,
		IsUnsafe:     true,
		UnsafeReason: "Unsafe operation detected: \"DROP INDEX `idx_state`\"",
	}
	safeChange := engine.TableChange{
		Table:     "plans",
		Operation: ddl.StatementAlterTable,
		DDL:       "ALTER TABLE `plans` ADD COLUMN `caller` VARCHAR(64)",
	}

	// The additive clause is what the starting binary needs to run at all, and
	// it is not a consequence of the drop it was bundled with, so it executes
	// while the drop waits for an operator.
	t.Run("an unsafe statement runs the clauses that only add", func(t *testing.T) {
		t.Parallel()
		allowed, refused := partitionDestructiveChanges([]engine.SchemaChange{{TableChanges: []engine.TableChange{unsafeChange}}})
		require.Len(t, allowed, 1)
		require.Len(t, allowed[0].TableChanges, 1)
		executed := allowed[0].TableChanges[0]
		assert.Equal(t, "ALTER TABLE `applies` ADD COLUMN `caller` VARCHAR(64)", executed.DDL,
			"only the additive clause executes")
		assert.False(t, executed.IsUnsafe, "the partition that executes carries no unsafe verdict")

		require.Len(t, refused, 1)
		assert.Equal(t, "ALTER TABLE `applies` DROP INDEX `idx_state`", refused[0].change.DDL,
			"the refusal carries the clauses that did not run")
		assert.True(t, refused[0].partial, "the statement was split, not refused whole")
		assert.Equal(t, unsafeChange.UnsafeReason, refused[0].reason, "the reason is the plan's, not this package's")
	})

	// A statement with nothing to keep is all or nothing: there is no partition
	// to report as having run.
	t.Run("a statement whose every clause is withheld is refused whole", func(t *testing.T) {
		t.Parallel()
		dropOnly := engine.TableChange{
			Table:        "applies",
			Operation:    ddl.StatementAlterTable,
			DDL:          "ALTER TABLE `applies` DROP INDEX `idx_state`",
			IsUnsafe:     true,
			UnsafeReason: "Unsafe operation detected: \"DROP INDEX `idx_state`\"",
		}
		allowed, refused := partitionDestructiveChanges([]engine.SchemaChange{{TableChanges: []engine.TableChange{dropOnly}}})
		assert.Empty(t, allowed, "no part of the statement executes")
		require.Len(t, refused, 1)
		assert.Equal(t, dropOnly.DDL, refused[0].change.DDL)
		assert.False(t, refused[0].partial, "nothing ran, so this is not a split")
	})

	t.Run("a statement with no clauses is refused whole", func(t *testing.T) {
		t.Parallel()
		dropTable := engine.TableChange{
			Table:        "applies",
			Operation:    ddl.StatementDropTable,
			DDL:          "DROP TABLE `applies`",
			IsUnsafe:     true,
			UnsafeReason: "Unsafe operation detected: \"DROP TABLE\"",
		}
		allowed, refused := partitionDestructiveChanges([]engine.SchemaChange{{TableChanges: []engine.TableChange{dropTable}}})
		assert.Empty(t, allowed)
		require.Len(t, refused, 1)
		assert.Equal(t, dropTable.DDL, refused[0].change.DDL)
		assert.False(t, refused[0].partial)
		assert.NoError(t, refused[0].splitErr, "a statement with no clauses is not a split failure")
	})

	// An index whose definition changed is diffed as a drop and an add of one
	// name. Executing the add against the index the refusal leaves in place
	// fails on a duplicate name and takes the whole convergence down, so the
	// add is withheld with the drop it depends on.
	t.Run("an addition that needs a withheld clause to have run is withheld with it", func(t *testing.T) {
		t.Parallel()
		redefinedIndex := engine.TableChange{
			Table:        "applies",
			Operation:    ddl.StatementAlterTable,
			DDL:          "ALTER TABLE `applies` DROP INDEX `idx_state`, ADD INDEX `idx_state` (`state`, `deployment`)",
			IsUnsafe:     true,
			UnsafeReason: "Unsafe operation detected: \"DROP INDEX `idx_state`\"",
		}
		allowed, refused := partitionDestructiveChanges([]engine.SchemaChange{{TableChanges: []engine.TableChange{redefinedIndex}}})
		assert.Empty(t, allowed, "the add cannot run without the drop")
		require.Len(t, refused, 1)
		assert.Equal(t, redefinedIndex.DDL, refused[0].change.DDL,
			"both clauses wait for an operator together, as the plan wrote them")
	})

	t.Run("a safe statement executes", func(t *testing.T) {
		t.Parallel()
		allowed, refused := partitionDestructiveChanges([]engine.SchemaChange{{TableChanges: []engine.TableChange{safeChange}}})
		assert.Empty(t, refused)
		require.Len(t, allowed, 1)
		require.Len(t, allowed[0].TableChanges, 1)
		assert.Equal(t, safeChange.DDL, allowed[0].TableChanges[0].DDL)
	})

	// One table's refusal must not withhold another's convergence: a pod
	// starting against newer storage should still add the columns its own
	// binary needs.
	t.Run("a refusal withholds only its own clauses", func(t *testing.T) {
		t.Parallel()
		allowed, refused := partitionDestructiveChanges([]engine.SchemaChange{{
			TableChanges: []engine.TableChange{unsafeChange, safeChange},
		}})
		require.Len(t, allowed, 1)
		require.Len(t, allowed[0].TableChanges, 2)
		assert.Equal(t, "ALTER TABLE `applies` ADD COLUMN `caller` VARCHAR(64)", allowed[0].TableChanges[0].DDL)
		assert.Equal(t, safeChange.DDL, allowed[0].TableChanges[1].DDL, "the other table still converges")
		require.Len(t, refused, 1)
		assert.Equal(t, "ALTER TABLE `applies` DROP INDEX `idx_state`", refused[0].change.DDL)
	})

	// The warning is what an operator reads during a rolling deploy, so it
	// carries the DDL that did not run and the reason it did not.
	t.Run("the refusal warning names the withheld clauses and the reason", func(t *testing.T) {
		t.Parallel()
		_, refused := partitionDestructiveChanges([]engine.SchemaChange{{TableChanges: []engine.TableChange{unsafeChange}}})
		require.Len(t, refused, 1)
		message, attrs := refused[0].refusalTelemetry()
		assert.Contains(t, message, "allow_destructive_schema_changes", "the warning names the option that runs them")
		assert.Contains(t, message, "additions ran", "a split says what did run, not only what did not")
		assert.Contains(t, attrs, "ddl")
		assert.Contains(t, attrs, "ALTER TABLE `applies` DROP INDEX `idx_state`")
		assert.NotContains(t, attrs, mixedDDL, "the whole statement did not fail to run")
		assert.Contains(t, attrs, "reason")
		assert.Contains(t, attrs, unsafeChange.UnsafeReason)
		assert.Contains(t, attrs, "applies")
	})
}

// stalledCanceller stands in for an engine whose cancel does not come back:
// the copy it is waiting on has stopped answering, or the target has. It is
// the case the release budget exists for, and the only way to reach it is an
// engine that never returns.
type stalledCanceller struct {
	called chan struct{}
}

func (s *stalledCanceller) Cancel(ctx context.Context, _ *engine.ControlRequest) (*engine.ControlResult, error) {
	close(s.called)
	<-ctx.Done()
	return nil, ctx.Err()
}

// A stop reports even when the release it asked for does not come back. The
// operator pressed Ctrl-C: a terminal that sits there indefinitely is the one
// outcome that reads as the stop having been ignored, and the artifacts the
// release was dropping are uncommitted copies that the next boot's stale-table
// cleanup reclaims.
func TestReleaseStoppedConvergence_ReportsWhenTheReleaseStalls(t *testing.T) {
	t.Parallel()
	canceller := &stalledCanceller{called: make(chan struct{})}
	stopped, cancel := context.WithCancel(t.Context())
	cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		releaseStoppedConvergence(stopped, canceller, closedPortDSN, 50*time.Millisecond, slog.New(slog.DiscardHandler))
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("a release that does not come back must not keep the stop from returning")
	}

	select {
	case <-canceller.called:
	default:
		t.Fatal("the stop must ask the engine to release the schema change before giving up on it")
	}
}

// recordingCanceller answers a cancel the way an engine does when the change
// it was asked to cancel had already finished.
type recordingCanceller struct {
	ctx context.Context
	err error
}

func (r *recordingCanceller) Cancel(ctx context.Context, _ *engine.ControlRequest) (*engine.ControlResult, error) {
	r.ctx = ctx
	if r.err != nil {
		return nil, r.err
	}
	return &engine.ControlResult{Accepted: true}, nil
}

// The release runs on a context the stop cannot cancel. Handing it the context
// that just ended would cancel the cleanup along with the thing being cleaned
// up, leaving the artifacts of every stopped convergence behind.
func TestReleaseStoppedConvergence_RunsOnALiveContext(t *testing.T) {
	t.Parallel()
	canceller := &recordingCanceller{}
	stopped, cancel := context.WithCancel(t.Context())
	cancel()

	releaseStoppedConvergence(stopped, canceller, closedPortDSN, time.Second, slog.New(slog.DiscardHandler))

	require.NotNil(t, canceller.ctx, "the engine must be asked to release the schema change")
	assert.NoError(t, canceller.ctx.Err(), "the release must not run on the context that just ended")
}

// A change that finished between the last progress poll and the stop is not a
// failed release: there is nothing left to release, the statement is applied,
// and the next plan is what says so. Reporting it as a leak would send an
// operator looking for artifacts that were never created.
func TestReleaseStoppedConvergence_AcceptsAChangeThatAlreadyFinished(t *testing.T) {
	t.Parallel()
	canceller := &recordingCanceller{err: engine.NewAlreadyCompletedError("cancel rejected: already completed")}
	logs := &strings.Builder{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	releaseStoppedConvergence(t.Context(), canceller, closedPortDSN, time.Second, logger)

	assert.Contains(t, logs.String(), "completed before the stop reached it")
	assert.NotContains(t, logs.String(), "level=WARN",
		"a change that finished on its own leaves nothing behind to warn about")
}

// A convergence hands the engine one table at a time. The engine attempts
// native DDL only for a change confined to a single table, so a delta spanning
// several tables that arrived as one run would copy every one of them on the
// path a pod starts on. The runs are ordered by phase and then by table name,
// so a convergence that stops partway converges the same prefix every time.
func TestStorageConvergenceRuns(t *testing.T) {
	t.Parallel()

	alter := func(table string) engine.TableChange {
		return engine.TableChange{
			Table:     table,
			Operation: ddl.StatementAlterTable,
			DDL:       "ALTER TABLE `" + table + "` ADD COLUMN `caller` VARCHAR(64) NULL",
		}
	}
	create := func(table string) engine.TableChange {
		return engine.TableChange{
			Table:     table,
			Operation: ddl.StatementCreateTable,
			DDL:       "CREATE TABLE `" + table + "` (`id` BIGINT UNSIGNED NOT NULL PRIMARY KEY)",
		}
	}
	drop := func(table string) engine.TableChange {
		return engine.TableChange{
			Table:     table,
			Operation: ddl.StatementDropTable,
			DDL:       "DROP TABLE `" + table + "`",
		}
	}
	runTables := func(runs []storageConvergenceRun) []string {
		tables := make([]string, 0, len(runs))
		for _, r := range runs {
			tables = append(tables, r.table)
		}
		return tables
	}

	t.Run("each table is its own run", func(t *testing.T) {
		t.Parallel()
		runs := storageConvergenceRuns([]engine.SchemaChange{{
			Namespace:    "schemabot",
			TableChanges: []engine.TableChange{alter("plans"), alter("applies"), alter("tasks")},
		}})

		require.Len(t, runs, 3, "three tables must converge as three engine runs, not one")
		assert.Equal(t, []string{"applies", "plans", "tasks"}, runTables(runs),
			"tables converge in name order within a phase")
		for i, r := range runs {
			require.Len(t, r.changes, 1, "a per-table run carries one plan")
			require.Len(t, r.changes[0].TableChanges, 1, "a run carries one table's statements")
			assert.Equal(t, r.table, r.changes[0].TableChanges[0].Table)
			assert.Equal(t, "schemabot", r.changes[0].Namespace, "the plan's namespace carries onto every run")
			assert.Equal(t, i+1, r.position)
			assert.Equal(t, 3, r.runCount)
			assert.Equal(t, i, r.doneDDL, "each run knows how many statements finished before it")
			assert.Equal(t, 1, r.runDDL)
			assert.Equal(t, 3, r.totalDDL)
		}
	})

	t.Run("creates run before alters and alters before drops", func(t *testing.T) {
		t.Parallel()
		runs := storageConvergenceRuns([]engine.SchemaChange{{
			Namespace: "schemabot",
			TableChanges: []engine.TableChange{
				drop("zeta"), alter("plans"), create("nu"), drop("alpha"), create("beta"),
			},
		}})

		assert.Equal(t, []string{"beta", "nu", "plans", "alpha", "zeta"}, runTables(runs),
			"a table created by this convergence exists before a later run drops another one")
	})

	t.Run("a table's statements stay in one run", func(t *testing.T) {
		t.Parallel()
		second := alter("plans")
		second.DDL = "ALTER TABLE `plans` ADD INDEX `idx_caller` (`caller`)"
		runs := storageConvergenceRuns([]engine.SchemaChange{{
			Namespace:    "schemabot",
			TableChanges: []engine.TableChange{alter("plans"), alter("tasks"), second},
		}})

		require.Len(t, runs, 2)
		require.Equal(t, "plans", runs[0].table)
		require.Len(t, runs[0].changes, 1)
		assert.Len(t, runs[0].changes[0].TableChanges, 2, "one table converges once, with all of its statements")
		assert.Equal(t, 2, runs[0].runDDL)
		assert.Equal(t, 3, runs[0].totalDDL)
		assert.Equal(t, 2, runs[1].doneDDL, "the second run starts after both of the first run's statements")
	})

	t.Run("an empty plan has no runs", func(t *testing.T) {
		t.Parallel()
		assert.Empty(t, storageConvergenceRuns(nil))
	})

	// Splitting a delta apart means a convergence that fails partway leaves
	// some of it applied. That is harmless while every statement adds, and is
	// not harmless once one of them removes something: a storage schema left
	// missing an object the fleet still reads is not a state a later boot
	// repairs, because the next diff reads the removal as already done. A
	// delta holding any statement the engine called unsafe therefore converges
	// in one run, the way every delta did before tables were split apart.
	t.Run("a delta that removes something converges in one run", func(t *testing.T) {
		t.Parallel()
		removal := alter("applies")
		removal.DDL = "ALTER TABLE `applies` DROP COLUMN `caller`"
		removal.IsUnsafe = true
		removal.UnsafeReason = "Unsafe operation detected: \"DROP COLUMN `caller`\""

		changes := []engine.SchemaChange{{
			Namespace:    "schemabot",
			TableChanges: []engine.TableChange{alter("plans"), removal, alter("tasks")},
		}}
		require.True(t, removesSchemaObjects(changes))

		runs := storageConvergenceRuns(changes)
		require.Len(t, runs, 1, "a delta that removes something must not be split across runs")
		assert.Equal(t, changes, runs[0].changes, "the one run carries the whole delta")
		assert.Empty(t, runs[0].table, "a run over the whole delta is about no single table")
		assert.Equal(t, 1, runs[0].position)
		assert.Equal(t, 1, runs[0].runCount)
		assert.Equal(t, 3, runs[0].runDDL)
		assert.Equal(t, 3, runs[0].totalDDL)
	})

	t.Run("a delta that only adds is split", func(t *testing.T) {
		t.Parallel()
		changes := []engine.SchemaChange{{
			Namespace:    "schemabot",
			TableChanges: []engine.TableChange{alter("plans"), alter("applies")},
		}}
		assert.False(t, removesSchemaObjects(changes))
		assert.Len(t, storageConvergenceRuns(changes), 2)
	})
}

// A watcher is told how far along the whole convergence is, not how far along
// the run in front of it. Reporting the engine's own percentage would reach
// 100% once per table and say the convergence had finished while most of it
// was still ahead.
func TestStorageConvergenceRunObserve(t *testing.T) {
	t.Parallel()

	runs := storageConvergenceRuns([]engine.SchemaChange{{
		Namespace: "schemabot",
		TableChanges: []engine.TableChange{
			{Table: "applies", Operation: ddl.StatementAlterTable, DDL: "ALTER TABLE `applies` ADD COLUMN `caller` VARCHAR(64) NULL"},
			{Table: "plans", Operation: ddl.StatementAlterTable, DDL: "ALTER TABLE `plans` ADD COLUMN `caller` VARCHAR(64) NULL"},
			{Table: "tasks", Operation: ddl.StatementAlterTable, DDL: "ALTER TABLE `tasks` ADD COLUMN `caller` VARCHAR(64) NULL"},
		},
	}})
	require.Len(t, runs, 3)

	t.Run("the percentage spans the convergence", func(t *testing.T) {
		t.Parallel()
		first := runs[0].observe(&engine.ProgressResult{State: engine.StateRunning, Progress: 60})
		assert.Equal(t, 20, first.Percent, "60% of the first of three runs is 20% of the convergence")
		assert.Equal(t, 3, first.DDLCount)

		done := runs[0].observe(&engine.ProgressResult{State: engine.StateCompleted, Progress: 100})
		assert.Equal(t, 33, done.Percent, "the first run finishing is not the convergence finishing")

		last := runs[2].observe(&engine.ProgressResult{State: engine.StateCompleted, Progress: 100})
		assert.Equal(t, 100, last.Percent, "the convergence is done when its last run is")

		// The engine stops measuring a run it has finished, so its last poll
		// reads zero. A finished run is all of its own share regardless.
		silent := runs[2].observe(&engine.ProgressResult{State: engine.StateCompleted})
		assert.Equal(t, 100, silent.Percent,
			"a convergence must not report itself short of done on the observation that says it finished")
	})

	t.Run("the state is the convergence's, not the run's", func(t *testing.T) {
		t.Parallel()
		done := runs[0].observe(&engine.ProgressResult{State: engine.StateCompleted, Progress: 100})
		assert.Equal(t, string(engine.StateRunning), done.State,
			"a watcher told the convergence completed at its first table stops reading")

		last := runs[2].observe(&engine.ProgressResult{State: engine.StateCompleted, Progress: 100})
		assert.Equal(t, string(engine.StateCompleted), last.State,
			"the last run completing is the convergence completing")

		// A run that failed ends the convergence with it, so its state is the
		// convergence's however early it happened.
		failed := runs[0].observe(&engine.ProgressResult{State: engine.StateFailed, ErrorMessage: "boom"})
		assert.Equal(t, string(engine.StateFailed), failed.State)
	})

	t.Run("a finished run naming no table is reported under its own", func(t *testing.T) {
		t.Parallel()
		// A metadata-only statement can finish before the engine ever reports a
		// table for it. That the run is over is still something to say about
		// the table it converged, and a watcher keyed on table names needs it.
		o := runs[1].observe(&engine.ProgressResult{State: engine.StateCompleted})
		require.Len(t, o.Tables, 1)
		assert.Equal(t, "plans", o.Tables[0].Table)
		assert.Equal(t, string(engine.StateCompleted), o.Tables[0].State)

		// Mid-run there is nothing honest to say: how far in the statement got
		// is a measurement nobody took, and the per-table phases are the
		// engine's vocabulary, not this package's.
		assert.Empty(t, runs[1].observe(&engine.ProgressResult{State: engine.StateRunning}).Tables)
	})

	t.Run("a finished run's tables are reported finished", func(t *testing.T) {
		t.Parallel()
		// A statement the server takes natively copies nothing, so the engine
		// never marks its table done and the last poll catches whatever phase
		// the run was winding down through. Passing that through would show an
		// operator every table starting and none of them finishing.
		o := runs[0].observe(&engine.ProgressResult{
			State: engine.StateCompleted,
			Tables: []engine.TableProgress{
				{Table: "applies", State: "close"},
			},
		})
		require.Len(t, o.Tables, 1)
		assert.Equal(t, "applies", o.Tables[0].Table)
		assert.Equal(t, string(engine.StateCompleted), o.Tables[0].State)
		assert.Equal(t, 100, o.Tables[0].Percent)
	})

	t.Run("the engine's own table progress is passed through", func(t *testing.T) {
		t.Parallel()
		o := runs[0].observe(&engine.ProgressResult{
			State:    engine.StateRunning,
			Message:  "12.5% copyRows ETA 1h30m",
			Progress: 12,
			Tables: []engine.TableProgress{
				{Table: "applies", State: "copyRows", Progress: 12, RowsCopied: 4096},
			},
		})
		assert.Equal(t, "12.5% copyRows ETA 1h30m", o.Message)
		require.Len(t, o.Tables, 1)
		assert.Equal(t, "applies", o.Tables[0].Table)
		assert.Equal(t, "copyRows", o.Tables[0].State)
		assert.Equal(t, 12, o.Tables[0].Percent)
		assert.Equal(t, int64(4096), o.Tables[0].RowsCopied)
	})
}

// recordingApplyEngine counts the applies a convergence issues. Progress and
// Cancel are the rest of the path a stop takes, so a run that does start
// reports the count rather than panicking on an unimplemented method. Every
// other method is the embedded nil interface.
type recordingApplyEngine struct {
	engine.Engine
	applies int
}

func (e *recordingApplyEngine) Apply(context.Context, *engine.ApplyRequest) (*engine.ApplyResult, error) {
	e.applies++
	return &engine.ApplyResult{}, nil
}

func (e *recordingApplyEngine) Progress(ctx context.Context, _ *engine.ProgressRequest) (*engine.ProgressResult, error) {
	return nil, ctx.Err()
}

func (e *recordingApplyEngine) Cancel(context.Context, *engine.ControlRequest) (*engine.ControlResult, error) {
	return &engine.ControlResult{}, nil
}

// A convergence stopped between two runs stops there. The engine executes a
// run's statements on a context of its own, so a stop does not reach DDL that
// has not been issued yet: a run started after the budget expired or the
// instance was told to stop would go on changing the storage database after
// the convergence ended (AV-13, AV-14).
func TestApplyStorageConvergenceRunStartsNothingAfterAStop(t *testing.T) {
	t.Parallel()

	runs := storageConvergenceRuns([]engine.SchemaChange{{
		Namespace: "schemabot",
		TableChanges: []engine.TableChange{{
			Table:     "applies",
			Operation: ddl.StatementAlterTable,
			DDL:       "ALTER TABLE `applies` ADD COLUMN `caller` VARCHAR(64) NULL",
		}},
	}})
	require.Len(t, runs, 1)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	eng := &recordingApplyEngine{}
	err := applyStorageConvergenceRun(ctx, eng, closedPortDSN, runs[0],
		ensureSchemaOptions{convergenceTimeout: time.Minute}, slog.New(slog.DiscardHandler))

	require.Error(t, err, "a stopped convergence reports the stop rather than returning as if it converged")
	assert.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, eng.applies, "no statement may be issued after the convergence was stopped")
}
