package spirit

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
)

// engineRun is a planned ALTER with no execution-mode verdict on it: the
// ordinary case, where the engine copies the table itself and so writes the
// checkpoint an existing copy is measured against.
func engineRun(table, alter string) engine.TableChange {
	return engine.TableChange{Table: table, Operation: ddl.StatementAlterTable, DDL: alter}
}

// The batch is the statement Spirit's checkpoint identity is compared against,
// so plannedSpiritBatch has to produce exactly what the apply hands the engine:
// every engine-run ALTER, joined in plan order, and nothing else.
func TestPlannedSpiritBatch(t *testing.T) {
	const xfers = "ALTER TABLE `xfers` ADD INDEX r_token (r_token)"
	const ledger = "ALTER TABLE `ledger_entries` ADD COLUMN note VARCHAR(64)"

	t.Run("joins every engine-run ALTER in plan order", func(t *testing.T) {
		batch, tables, ok := plannedSpiritBatch([]engine.TableChange{
			engineRun("xfers", xfers),
			engineRun("ledger_entries", ledger),
		})
		assert.True(t, ok)
		assert.Equal(t, xfers+"; "+ledger, batch, "the joined batch is what Spirit stores and compares")
		assert.Equal(t, []string{"xfers", "ledger_entries"}, tables,
			"Spirit reads the shadow table of every table in the batch, so all of them are looked up")
	})

	t.Run("a single ALTER is the whole batch", func(t *testing.T) {
		batch, tables, ok := plannedSpiritBatch([]engine.TableChange{engineRun("xfers", xfers)})
		assert.True(t, ok)
		assert.Equal(t, xfers, batch, "one statement joins to itself; a trailing separator would never match")
		assert.Equal(t, []string{"xfers"}, tables)
	})

	// A statement the direct execution policy routes to native DDL runs on the
	// target itself and never touches a checkpoint, so it is not part of the
	// batch whose identity decides an existing copy's fate.
	t.Run("skips a directly executed ALTER", func(t *testing.T) {
		direct := engineRun("ledger_entries", ledger)
		direct.ExecutionMode = engine.ExecutionModeDirect
		batch, tables, ok := plannedSpiritBatch([]engine.TableChange{engineRun("xfers", xfers), direct})
		assert.True(t, ok)
		assert.Equal(t, xfers, batch, "the batch is the engine-run ALTER alone")
		assert.Equal(t, []string{"xfers"}, tables)
	})

	// CREATE and DROP do not copy a table, so they carry no checkpoint and
	// cannot be part of what a copy was made for.
	t.Run("skips changes that are not ALTERs", func(t *testing.T) {
		batch, tables, ok := plannedSpiritBatch([]engine.TableChange{
			{Table: "audits", Operation: ddl.StatementCreateTable, DDL: "CREATE TABLE `audits` (id BIGINT)"},
			engineRun("xfers", xfers),
			{Table: "legacy", Operation: ddl.StatementDropTable, DDL: "DROP TABLE `legacy`"},
		})
		assert.True(t, ok)
		assert.Equal(t, xfers, batch)
		assert.Equal(t, []string{"xfers"}, tables)
	})

	// A blocked statement fails the apply during routing, before Spirit reads a
	// checkpoint, so nothing on the target is at stake and the plan must not
	// disclose a copy as continued or destroyed.
	t.Run("reports nothing at stake when any statement is blocked", func(t *testing.T) {
		blocked := engineRun("ledger_entries", ledger)
		blocked.ExecutionMode = engine.ExecutionModeBlocked
		batch, tables, ok := plannedSpiritBatch([]engine.TableChange{engineRun("xfers", xfers), blocked})
		assert.False(t, ok, "the apply never gets far enough to touch the copy")
		assert.Empty(t, batch)
		assert.Empty(t, tables)
	})

	t.Run("reports nothing at stake with no engine-run ALTER", func(t *testing.T) {
		direct := engineRun("xfers", xfers)
		direct.ExecutionMode = engine.ExecutionModeDirect
		batch, tables, ok := plannedSpiritBatch([]engine.TableChange{
			{Table: "audits", Operation: ddl.StatementCreateTable, DDL: "CREATE TABLE `audits` (id BIGINT)"},
			direct,
		})
		assert.False(t, ok)
		assert.Empty(t, batch)
		assert.Empty(t, tables)
	})

	t.Run("reports nothing at stake for a plan with no changes", func(t *testing.T) {
		batch, tables, ok := plannedSpiritBatch(nil)
		assert.False(t, ok)
		assert.Empty(t, batch)
		assert.Empty(t, tables)
	})
}
