package api

import (
	"log/slog"
	"testing"

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
