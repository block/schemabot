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

// A grouped apply hands Spirit every ALTER at once, and that joined string is
// the checkpoint identity Spirit compares against, so the grouped batch has to
// be exactly what the apply hands the engine: every engine-run ALTER, joined in
// plan order, and nothing else.
func TestPlannedSpiritBatchesGrouped(t *testing.T) {
	const xfers = "ALTER TABLE `xfers` ADD INDEX r_token (r_token)"
	const ledger = "ALTER TABLE `ledger_entries` ADD COLUMN note VARCHAR(64)"

	t.Run("joins every engine-run ALTER in plan order", func(t *testing.T) {
		batches, ok := plannedSpiritBatches([]engine.TableChange{
			engineRun("xfers", xfers),
			engineRun("ledger_entries", ledger),
		}, true)
		assert.True(t, ok)
		assert.Equal(t, []spiritBatch{{
			Statement: xfers + "; " + ledger,
			Tables:    []string{"xfers", "ledger_entries"},
		}}, batches, "the joined batch is what Spirit stores and compares, and it reads the shadow table of every table in it")
	})

	t.Run("a single ALTER is the whole batch", func(t *testing.T) {
		batches, ok := plannedSpiritBatches([]engine.TableChange{engineRun("xfers", xfers)}, true)
		assert.True(t, ok)
		assert.Equal(t, []spiritBatch{{Statement: xfers, Tables: []string{"xfers"}}}, batches,
			"one statement joins to itself; a trailing separator would never match")
	})

	// A statement the direct execution policy routes to native DDL runs on the
	// target itself and never touches a checkpoint, so it is not part of the
	// batch whose identity decides an existing copy's fate.
	t.Run("skips a directly executed ALTER", func(t *testing.T) {
		direct := engineRun("ledger_entries", ledger)
		direct.ExecutionMode = engine.ExecutionModeDirect
		batches, ok := plannedSpiritBatches([]engine.TableChange{engineRun("xfers", xfers), direct}, true)
		assert.True(t, ok)
		assert.Equal(t, []spiritBatch{{Statement: xfers, Tables: []string{"xfers"}}}, batches,
			"the batch is the engine-run ALTER alone")
	})

	// CREATE and DROP do not copy a table, so they carry no checkpoint and
	// cannot be part of what a copy was made for.
	t.Run("skips changes that are not ALTERs", func(t *testing.T) {
		batches, ok := plannedSpiritBatches([]engine.TableChange{
			{Table: "audits", Operation: ddl.StatementCreateTable, DDL: "CREATE TABLE `audits` (id BIGINT)"},
			engineRun("xfers", xfers),
			{Table: "legacy", Operation: ddl.StatementDropTable, DDL: "DROP TABLE `legacy`"},
		}, true)
		assert.True(t, ok)
		assert.Equal(t, []spiritBatch{{Statement: xfers, Tables: []string{"xfers"}}}, batches)
	})

	// A blocked statement fails the apply during routing, before Spirit reads a
	// checkpoint, so nothing on the target is at stake and the plan must not
	// disclose a copy as continued or destroyed.
	t.Run("reports nothing at stake when any statement is blocked", func(t *testing.T) {
		blocked := engineRun("ledger_entries", ledger)
		blocked.ExecutionMode = engine.ExecutionModeBlocked
		batches, ok := plannedSpiritBatches([]engine.TableChange{engineRun("xfers", xfers), blocked}, true)
		assert.False(t, ok, "the apply never gets far enough to touch the copy")
		assert.Empty(t, batches)
	})

	t.Run("reports nothing at stake with no engine-run ALTER", func(t *testing.T) {
		direct := engineRun("xfers", xfers)
		direct.ExecutionMode = engine.ExecutionModeDirect
		batches, ok := plannedSpiritBatches([]engine.TableChange{
			{Table: "audits", Operation: ddl.StatementCreateTable, DDL: "CREATE TABLE `audits` (id BIGINT)"},
			direct,
		}, true)
		assert.False(t, ok)
		assert.Empty(t, batches)
	})

	t.Run("reports nothing at stake for a plan with no changes", func(t *testing.T) {
		batches, ok := plannedSpiritBatches(nil, true)
		assert.False(t, ok)
		assert.Empty(t, batches)
	})
}

// An ungrouped apply drives one table at a time, so each table is its own
// batch: its own statement, its own checkpoint, and its own fate. Predicting
// the grouped shape here would compare every table's copy against a joined
// statement no ungrouped apply ever runs.
func TestPlannedSpiritBatchesUngrouped(t *testing.T) {
	const xfers = "ALTER TABLE `xfers` ADD INDEX r_token (r_token)"
	const ledger = "ALTER TABLE `ledger_entries` ADD COLUMN note VARCHAR(64)"

	t.Run("gives every engine-run ALTER a batch of its own in plan order", func(t *testing.T) {
		batches, ok := plannedSpiritBatches([]engine.TableChange{
			engineRun("xfers", xfers),
			engineRun("ledger_entries", ledger),
		}, false)
		assert.True(t, ok)
		assert.Equal(t, []spiritBatch{
			{Statement: xfers, Tables: []string{"xfers"}},
			{Statement: ledger, Tables: []string{"ledger_entries"}},
		}, batches, "each table's own ALTER is the whole statement Spirit compares for it")
	})

	t.Run("a single ALTER is one batch either way", func(t *testing.T) {
		batches, ok := plannedSpiritBatches([]engine.TableChange{engineRun("xfers", xfers)}, false)
		assert.True(t, ok)
		assert.Equal(t, []spiritBatch{{Statement: xfers, Tables: []string{"xfers"}}}, batches)
	})

	t.Run("skips statements that never reach the engine's copy path", func(t *testing.T) {
		direct := engineRun("ledger_entries", ledger)
		direct.ExecutionMode = engine.ExecutionModeDirect
		batches, ok := plannedSpiritBatches([]engine.TableChange{
			{Table: "audits", Operation: ddl.StatementCreateTable, DDL: "CREATE TABLE `audits` (id BIGINT)"},
			engineRun("xfers", xfers),
			direct,
		}, false)
		assert.True(t, ok)
		assert.Equal(t, []spiritBatch{{Statement: xfers, Tables: []string{"xfers"}}}, batches)
	})

	t.Run("reports nothing at stake when any statement is blocked", func(t *testing.T) {
		blocked := engineRun("ledger_entries", ledger)
		blocked.ExecutionMode = engine.ExecutionModeBlocked
		batches, ok := plannedSpiritBatches([]engine.TableChange{engineRun("xfers", xfers), blocked}, false)
		assert.False(t, ok, "the apply never gets far enough to touch any table's copy")
		assert.Empty(t, batches)
	})
}

// The checkpoint a batch reads follows the batch's size, so the two grouping
// shapes look in different places for the same plan. This is what makes the
// grouping part of the prediction rather than a detail of how it runs.
func TestPlannedBatchesReadTheCheckpointTheirShapeWrites(t *testing.T) {
	changes := []engine.TableChange{
		engineRun("xfers", "ALTER TABLE `xfers` ADD INDEX r_token (r_token)"),
		engineRun("ledger_entries", "ALTER TABLE `ledger_entries` ADD COLUMN note VARCHAR(64)"),
	}

	grouped, ok := plannedSpiritBatches(changes, true)
	assert.True(t, ok)
	assert.Equal(t, []string{sharedCheckpointTable}, checkpointTablesFor(grouped),
		"a batch of two or more shares the schema-level checkpoint")

	ungrouped, ok := plannedSpiritBatches(changes, false)
	assert.True(t, ok)
	assert.Equal(t, []string{"_xfers_chkpnt", "_ledger_entries_chkpnt"}, checkpointTablesFor(ungrouped),
		"a batch of one reads its own table's checkpoint, which is the only one an ungrouped apply ever writes")
}

func checkpointTablesFor(batches []spiritBatch) []string {
	names := make([]string, len(batches))
	for i, b := range batches {
		names[i] = checkpointTableForBatch(b.Tables)
	}
	return names
}

// A discard is the promise that work is about to be destroyed, and the batch
// size is what decides whether a partial copy means that. An ungrouped apply
// never asks about more than one table, so a plan mid-way through one — some
// tables copied, some not — must not read every table's copy as doomed.
func TestDispositionDoesNotCallASingleTableBatchIncomplete(t *testing.T) {
	const alter = "ALTER TABLE `xfers` ADD INDEX r_token (r_token)"
	copied := &existingCopy{
		CopiedTables:    []string{"xfers"},
		BatchTables:     []string{"xfers"},
		CheckpointTable: "_xfers_chkpnt",
		CheckpointFound: true,
		Statement:       alter,
	}

	disposition, reason := copied.Disposition(alter, DefaultCheckpointMaxAge)
	assert.Equal(t, engine.CopyAdopt, disposition, "the batch is one table and that table is copied, so there is nothing missing")
	assert.Empty(t, reason)
}

// The same copy inside a grouped batch that also covers an uncopied table is a
// genuine discard: Spirit rebuilds every table in the batch when any one of
// them has no readable shadow table.
func TestDispositionDiscardsAPartiallyCopiedGroupedBatch(t *testing.T) {
	const batch = "ALTER TABLE `xfers` ADD INDEX r_token (r_token); ALTER TABLE `ledger_entries` ADD COLUMN note VARCHAR(64)"
	copied := &existingCopy{
		CopiedTables:    []string{"xfers"},
		BatchTables:     []string{"xfers", "ledger_entries"},
		CheckpointTable: sharedCheckpointTable,
		CheckpointFound: true,
		Statement:       batch,
	}

	disposition, reason := copied.Disposition(batch, DefaultCheckpointMaxAge)
	assert.Equal(t, engine.CopyDiscard, disposition)
	assert.Equal(t, engine.DiscardCopyIncomplete, reason,
		"the batch's other table has no copy, so Spirit rebuilds this one too")
}
