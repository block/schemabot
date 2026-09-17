package ddl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSplitAdditiveAlter(t *testing.T) {
	tests := []struct {
		name     string
		stmt     string
		additive string
		withheld string
	}{
		{
			name:     "an addition beside a removal is separated from it",
			stmt:     "ALTER TABLE `applies` ADD COLUMN `caller` VARCHAR(64) NOT NULL, DROP INDEX `idx_state`",
			additive: "ALTER TABLE `applies` ADD COLUMN `caller` VARCHAR(64) NOT NULL",
			withheld: "ALTER TABLE `applies` DROP INDEX `idx_state`",
		},
		{
			name:     "a statement that only adds is kept whole",
			stmt:     "ALTER TABLE `applies` ADD COLUMN `caller` VARCHAR(64) NOT NULL, ADD INDEX `idx_caller` (`caller`)",
			additive: "ALTER TABLE `applies` ADD COLUMN `caller` VARCHAR(64) NOT NULL, ADD INDEX `idx_caller`(`caller`)",
			withheld: "",
		},
		{
			name:     "a statement that only removes keeps nothing",
			stmt:     "ALTER TABLE `applies` DROP COLUMN `caller`, DROP INDEX `idx_state`",
			additive: "",
			withheld: "ALTER TABLE `applies` DROP COLUMN `caller`, DROP INDEX `idx_state`",
		},
		{
			name:     "a change that is not an addition is withheld even though it removes nothing",
			stmt:     "ALTER TABLE `applies` ADD COLUMN `caller` VARCHAR(64) NOT NULL, MODIFY COLUMN `state` BIGINT NOT NULL",
			additive: "ALTER TABLE `applies` ADD COLUMN `caller` VARCHAR(64) NOT NULL",
			withheld: "ALTER TABLE `applies` MODIFY COLUMN `state` BIGINT NOT NULL",
		},
		{
			name:     "a table option is withheld",
			stmt:     "ALTER TABLE `applies` ADD COLUMN `caller` VARCHAR(64) NOT NULL, ENGINE=InnoDB",
			additive: "ALTER TABLE `applies` ADD COLUMN `caller` VARCHAR(64) NOT NULL",
			withheld: "ALTER TABLE `applies` ENGINE = InnoDB",
		},
		{
			// A redefined index is diffed as a drop and an add of one name, and
			// the add cannot run while the drop does not.
			name:     "an index re-added under a name the withheld drop frees is withheld with it",
			stmt:     "ALTER TABLE `applies` DROP INDEX `idx_state`, ADD INDEX `idx_state` (`state`, `deployment`)",
			additive: "",
			withheld: "ALTER TABLE `applies` DROP INDEX `idx_state`, ADD INDEX `idx_state`(`state`, `deployment`)",
		},
		{
			name:     "a column re-added under a name the withheld drop frees is withheld with it",
			stmt:     "ALTER TABLE `applies` DROP COLUMN `caller`, ADD COLUMN `caller` BIGINT NOT NULL",
			additive: "",
			withheld: "ALTER TABLE `applies` DROP COLUMN `caller`, ADD COLUMN `caller` BIGINT NOT NULL",
		},
		{
			name:     "a widened primary key is withheld with the drop that frees it",
			stmt:     "ALTER TABLE `applies` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`, `deployment`)",
			additive: "",
			withheld: "ALTER TABLE `applies` DROP PRIMARY KEY, ADD PRIMARY KEY(`id`, `deployment`)",
		},
		{
			// MySQL compares identifiers case-insensitively, so the coupling has
			// to as well or the add would be executed against the live index.
			name:     "a name is matched the way MySQL matches it",
			stmt:     "ALTER TABLE `applies` DROP INDEX `IDX_STATE`, ADD INDEX `idx_state` (`state`)",
			additive: "",
			withheld: "ALTER TABLE `applies` DROP INDEX `IDX_STATE`, ADD INDEX `idx_state`(`state`)",
		},
		{
			name:     "an addition that collides with nothing survives beside one that does",
			stmt:     "ALTER TABLE `applies` DROP INDEX `idx_state`, ADD INDEX `idx_state` (`state`), ADD COLUMN `caller` VARCHAR(64) NOT NULL",
			additive: "ALTER TABLE `applies` ADD COLUMN `caller` VARCHAR(64) NOT NULL",
			withheld: "ALTER TABLE `applies` DROP INDEX `idx_state`, ADD INDEX `idx_state`(`state`)",
		},
		{
			name:     "a renamed column frees the name it renames away from",
			stmt:     "ALTER TABLE `applies` RENAME COLUMN `caller` TO `requested_by`, ADD COLUMN `caller` VARCHAR(64) NOT NULL",
			additive: "",
			withheld: "ALTER TABLE `applies` RENAME COLUMN `caller` TO `requested_by`, ADD COLUMN `caller` VARCHAR(64) NOT NULL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			additive, withheld, err := SplitAdditiveAlter(tt.stmt)
			require.NoError(t, err)
			assert.Equal(t, tt.additive, additive, "additive partition")
			assert.Equal(t, tt.withheld, withheld, "withheld partition")
		})
	}
}

func TestSplitAdditiveAlterRejectsWhatItCannotPartition(t *testing.T) {
	t.Run("a statement with no clauses is not an ALTER TABLE", func(t *testing.T) {
		_, _, err := SplitAdditiveAlter("DROP TABLE `applies`")
		require.ErrorIs(t, err, ErrNotAlterTable)
	})

	t.Run("an unparseable statement is an error", func(t *testing.T) {
		_, _, err := SplitAdditiveAlter("ALTER TABLE `applies` ADD COLUMN")
		require.Error(t, err)
		assert.NotErrorIs(t, err, ErrNotAlterTable,
			"a parse failure must not read as a statement with no clauses")
	})
}

// The split decides what may run inside a statement the plan already reported
// as unsafe, so an additive partition that is itself unsafe would execute the
// clause the plan objected to. The allowlist is what rules that out, and this
// holds it to it: the shapes Spirit's unsafe vocabulary covers must never reach
// the additive side, whichever clause they are bundled with.
func TestSplitAdditiveAlterNeverKeepsAnUnsafeClause(t *testing.T) {
	for _, stmt := range []string{
		"ALTER TABLE `applies` ADD COLUMN `caller` VARCHAR(64) NOT NULL, DROP COLUMN `state`",
		"ALTER TABLE `applies` ADD COLUMN `caller` VARCHAR(64) NOT NULL, DROP INDEX `idx_state`",
		"ALTER TABLE `applies` ADD COLUMN `caller` VARCHAR(64) NOT NULL, DROP PRIMARY KEY",
		"ALTER TABLE `applies` ADD INDEX `idx_caller` (`caller`), DROP COLUMN `state`, MODIFY COLUMN `deployment` VARCHAR(32) NOT NULL",
	} {
		additive, _, err := SplitAdditiveAlter(stmt)
		require.NoError(t, err, stmt)
		require.NotEmpty(t, additive, "%s: the additions are what the bootstrap needs", stmt)

		unsafe, reason, err := UnsafeStatement(additive)
		require.NoError(t, err, "the additive partition must be parseable: %s", additive)
		assert.False(t, unsafe, "additive partition %q of %q is unsafe: %s", additive, stmt, reason)
	}
}
