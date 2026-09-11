package ddl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// UnsafeStatement delegates the unsafe vocabulary to Spirit's UnsafeLinter, so
// these cases pin the boundary SchemaBot relies on: operations that destroy
// data are unsafe, structural operations that lose nothing (DROP INDEX,
// renames) are safe, and the verdict cannot be relaxed by configuration. The
// structural cases are where this vocabulary stops and the storage bootstrap's
// stricter one starts (TestStorageDestructiveStatement).
func TestUnsafeStatement(t *testing.T) {
	unsafeCases := []struct {
		name     string
		stmt     string
		contains []string
	}{
		{name: "DROP TABLE", stmt: "DROP TABLE `users`", contains: []string{"DROP TABLE"}},
		{name: "ALTER TABLE DROP COLUMN names the column", stmt: "ALTER TABLE `users` DROP COLUMN `email`", contains: []string{"DROP COLUMN", "email"}},
		{name: "DROP COLUMN mixed with additive clauses", stmt: "ALTER TABLE `users` ADD COLUMN `phone` VARCHAR(20), DROP COLUMN `fax`", contains: []string{"DROP COLUMN", "fax"}},
		{name: "DROP PARTITION", stmt: "ALTER TABLE `events` DROP PARTITION p2020", contains: []string{"DROP PARTITION"}},
	}
	for _, tt := range unsafeCases {
		t.Run(tt.name, func(t *testing.T) {
			unsafe, reason, err := UnsafeStatement(tt.stmt)
			require.NoError(t, err)
			assert.True(t, unsafe)
			for _, want := range tt.contains {
				assert.Contains(t, reason, want)
			}
		})
	}

	safeCases := []struct {
		name string
		stmt string
	}{
		{name: "additive ALTER TABLE", stmt: "ALTER TABLE `users` ADD COLUMN `phone` VARCHAR(20)"},
		{name: "ALTER TABLE ADD INDEX", stmt: "ALTER TABLE `users` ADD INDEX `idx_email` (`email`)"},
		{name: "ALTER TABLE DROP INDEX loses no data", stmt: "ALTER TABLE `users` DROP INDEX `idx_email`"},
		{name: "CREATE TABLE", stmt: "CREATE TABLE `audit` (`id` BIGINT UNSIGNED AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"},
	}
	for _, tt := range safeCases {
		t.Run(tt.name, func(t *testing.T) {
			unsafe, reason, err := UnsafeStatement(tt.stmt)
			require.NoError(t, err)
			assert.False(t, unsafe)
			assert.Empty(t, reason)
		})
	}

	t.Run("an unparseable statement returns an error", func(t *testing.T) {
		_, _, err := UnsafeStatement("THIS IS NOT SQL")
		require.Error(t, err)
	})

	// Spirit's statement parser refuses statement types Spirit does not run,
	// so they surface as errors rather than verdicts — callers treat an error
	// as unsafe, which is the fail-closed direction.
	t.Run("a statement type Spirit does not run returns an error", func(t *testing.T) {
		_, _, err := UnsafeStatement("TRUNCATE TABLE `users`")
		require.Error(t, err)
	})
}

// StorageDestructiveStatement is the storage bootstrap's vocabulary: Spirit's
// unsafe set plus every statement that removes or renames a schema object
// while losing no data. These cases pin both halves of that boundary — what
// the bootstrap refuses beyond Spirit's set, and what it still executes,
// which must include the additive statements a starting binary needs and the
// table-option convergence a diff routinely emits.
func TestStorageDestructiveStatement(t *testing.T) {
	destructiveCases := []struct {
		name     string
		stmt     string
		contains []string
	}{
		{name: "DROP TABLE loses data", stmt: "DROP TABLE `users`", contains: []string{"DROP TABLE"}},
		{name: "DROP COLUMN loses data", stmt: "ALTER TABLE `users` DROP COLUMN `email`", contains: []string{"DROP COLUMN", "email"}},
		{name: "DROP INDEX", stmt: "ALTER TABLE `users` DROP INDEX `idx_email`", contains: []string{"DROP INDEX", "idx_email"}},
		{name: "DROP INDEX as its own statement", stmt: "DROP INDEX `idx_email` ON `users`", contains: []string{"DROP INDEX", "idx_email"}},
		{name: "DROP INDEX bundled with additive clauses", stmt: "ALTER TABLE `users` ADD COLUMN `phone` VARCHAR(20), DROP INDEX `idx_email`", contains: []string{"DROP INDEX", "idx_email"}},
		{name: "DROP FOREIGN KEY", stmt: "ALTER TABLE `users` DROP FOREIGN KEY `fk_org`", contains: []string{"DROP FOREIGN KEY", "fk_org"}},
		{name: "DROP CHECK", stmt: "ALTER TABLE `users` DROP CHECK `chk_age`", contains: []string{"DROP CHECK", "chk_age"}},
		{name: "DROP CONSTRAINT", stmt: "ALTER TABLE `users` DROP CONSTRAINT `uq_email`", contains: []string{"DROP CONSTRAINT", "uq_email"}},
		{name: "RENAME COLUMN", stmt: "ALTER TABLE `users` RENAME COLUMN `fax` TO `phone`", contains: []string{"RENAME COLUMN", "fax"}},
		{name: "RENAME INDEX", stmt: "ALTER TABLE `users` RENAME INDEX `idx_email` TO `idx_addr`", contains: []string{"RENAME INDEX", "idx_email"}},
		{name: "ALTER TABLE RENAME", stmt: "ALTER TABLE `users` RENAME TO `people`", contains: []string{"RENAME TABLE", "people"}},
		{name: "RENAME TABLE as its own statement", stmt: "RENAME TABLE `users` TO `people`", contains: []string{"RENAME TABLE", "users"}},
	}
	for _, tt := range destructiveCases {
		t.Run("refuses "+tt.name, func(t *testing.T) {
			destructive, reason, err := StorageDestructiveStatement(tt.stmt)
			require.NoError(t, err)
			assert.True(t, destructive)
			for _, want := range tt.contains {
				assert.Contains(t, reason, want)
			}
		})
	}

	allowedCases := []struct {
		name string
		stmt string
	}{
		{name: "ADD COLUMN", stmt: "ALTER TABLE `users` ADD COLUMN `phone` VARCHAR(20)"},
		{name: "ADD INDEX", stmt: "ALTER TABLE `users` ADD INDEX `idx_email` (`email`)"},
		{name: "MODIFY COLUMN", stmt: "ALTER TABLE `users` MODIFY COLUMN `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT"},
		{name: "CREATE TABLE", stmt: "CREATE TABLE `audit` (`id` BIGINT UNSIGNED AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"},
		// Table options converge properties rather than removing objects, and
		// the diff emits them routinely; refusing them would strand the
		// storage schema on a property mismatch forever.
		{name: "a table option", stmt: "ALTER TABLE `users` ENGINE=InnoDB"},
		{name: "a charset table option", stmt: "ALTER TABLE `users` DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"},
	}
	for _, tt := range allowedCases {
		t.Run("allows "+tt.name, func(t *testing.T) {
			destructive, reason, err := StorageDestructiveStatement(tt.stmt)
			require.NoError(t, err)
			assert.False(t, destructive)
			assert.Empty(t, reason)
		})
	}

	t.Run("an unparseable statement returns an error", func(t *testing.T) {
		_, _, err := StorageDestructiveStatement("THIS IS NOT SQL")
		require.Error(t, err)
	})
}

// SplitStorageDestructiveAlter partitions a single ALTER TABLE by clause using
// the storage bootstrap's vocabulary, so these cases pin the split the
// bootstrap relies on: a mixed ALTER yields a safe statement carrying the
// additive clauses and a destructive statement carrying only the refused
// ones, single-partition inputs leave the other side empty, and anything
// that is not exactly one parseable ALTER TABLE is an error.
func TestSplitStorageDestructiveAlter(t *testing.T) {
	t.Run("mixed ALTER splits into safe and destructive statements", func(t *testing.T) {
		safeDDL, destructiveDDL, err := SplitStorageDestructiveAlter("ALTER TABLE `users` ADD COLUMN `phone` VARCHAR(20), DROP COLUMN `fax`, ADD INDEX `idx_phone` (`phone`)")
		require.NoError(t, err)
		assert.Equal(t, "ALTER TABLE `users` ADD COLUMN `phone` VARCHAR(20), ADD INDEX `idx_phone`(`phone`)", safeDDL)
		assert.Equal(t, "ALTER TABLE `users` DROP COLUMN `fax`", destructiveDDL)

		destructive, _, err := StorageDestructiveStatement(safeDDL)
		require.NoError(t, err)
		assert.False(t, destructive, "safe partition must re-classify safe")
		destructive, reason, err := StorageDestructiveStatement(destructiveDDL)
		require.NoError(t, err)
		assert.True(t, destructive, "destructive partition must re-classify destructive")
		assert.Contains(t, reason, "fax")
	})

	t.Run("all-safe ALTER leaves the destructive statement empty", func(t *testing.T) {
		safeDDL, destructiveDDL, err := SplitStorageDestructiveAlter("ALTER TABLE `users` ADD COLUMN `phone` VARCHAR(20)")
		require.NoError(t, err)
		assert.Equal(t, "ALTER TABLE `users` ADD COLUMN `phone` VARCHAR(20)", safeDDL)
		assert.Empty(t, destructiveDDL)
	})

	t.Run("all-destructive ALTER leaves the safe statement empty", func(t *testing.T) {
		safeDDL, destructiveDDL, err := SplitStorageDestructiveAlter("ALTER TABLE `users` DROP COLUMN `fax`, DROP COLUMN `pager`")
		require.NoError(t, err)
		assert.Empty(t, safeDDL)
		assert.Equal(t, "ALTER TABLE `users` DROP COLUMN `fax`, DROP COLUMN `pager`", destructiveDDL)
	})

	t.Run("a primary key change refuses the ADD PRIMARY KEY with its DROP", func(t *testing.T) {
		safeDDL, destructiveDDL, err := SplitStorageDestructiveAlter("ALTER TABLE `users` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`, `org_id`)")
		require.NoError(t, err)
		assert.Empty(t, safeDDL, "an ADD PRIMARY KEY cannot run while the refused DROP leaves the old key in place")
		assert.Equal(t, "ALTER TABLE `users` DROP PRIMARY KEY, ADD PRIMARY KEY(`id`, `org_id`)", destructiveDDL)
	})

	t.Run("a mixed primary key change keeps only independently executable clauses", func(t *testing.T) {
		safeDDL, destructiveDDL, err := SplitStorageDestructiveAlter("ALTER TABLE `users` ADD COLUMN `phone` VARCHAR(20), DROP PRIMARY KEY, ADD PRIMARY KEY (`id`)")
		require.NoError(t, err)
		assert.Equal(t, "ALTER TABLE `users` ADD COLUMN `phone` VARCHAR(20)", safeDDL)
		assert.Equal(t, "ALTER TABLE `users` DROP PRIMARY KEY, ADD PRIMARY KEY(`id`)", destructiveDDL)
	})

	t.Run("ADD PRIMARY KEY without a DROP stays in the safe statement", func(t *testing.T) {
		safeDDL, destructiveDDL, err := SplitStorageDestructiveAlter("ALTER TABLE `users` ADD PRIMARY KEY (`id`)")
		require.NoError(t, err)
		assert.Equal(t, "ALTER TABLE `users` ADD PRIMARY KEY(`id`)", safeDDL)
		assert.Empty(t, destructiveDDL)
	})

	t.Run("a DROP INDEX bundled with additive clauses is refused on its own", func(t *testing.T) {
		safeDDL, destructiveDDL, err := SplitStorageDestructiveAlter("ALTER TABLE `users` ADD COLUMN `phone` VARCHAR(20), DROP INDEX `idx_email`, ADD INDEX `idx_phone` (`phone`)")
		require.NoError(t, err)
		assert.Equal(t, "ALTER TABLE `users` ADD COLUMN `phone` VARCHAR(20), ADD INDEX `idx_phone`(`phone`)", safeDDL)
		assert.Equal(t, "ALTER TABLE `users` DROP INDEX `idx_email`", destructiveDDL)
	})

	t.Run("every structural drop and rename lands in the destructive statement", func(t *testing.T) {
		safeDDL, destructiveDDL, err := SplitStorageDestructiveAlter(
			"ALTER TABLE `users` ADD COLUMN `phone` VARCHAR(20), DROP INDEX `idx_email`, DROP FOREIGN KEY `fk_org`, DROP CHECK `chk_age`, RENAME COLUMN `fax` TO `pager`, RENAME INDEX `idx_name` TO `idx_full_name`")
		require.NoError(t, err)
		assert.Equal(t, "ALTER TABLE `users` ADD COLUMN `phone` VARCHAR(20)", safeDDL)
		assert.Equal(t,
			"ALTER TABLE `users` DROP INDEX `idx_email`, DROP FOREIGN KEY `fk_org`, DROP CHECK `chk_age`, RENAME COLUMN `fax` TO `pager`, RENAME INDEX `idx_name` TO `idx_full_name`",
			destructiveDDL)
	})

	// An index whose definition changed diffs to a drop and a re-add under the
	// same name. The add is only executable once the drop has run, so refusing
	// the drop must refuse the add with it — otherwise the safe partition
	// fails on a duplicate key name and takes the whole bootstrap DDL down.
	t.Run("an index redefinition refuses the ADD with its DROP", func(t *testing.T) {
		safeDDL, destructiveDDL, err := SplitStorageDestructiveAlter("ALTER TABLE `users` DROP INDEX `idx_email`, ADD INDEX `idx_email` (`email`, `org_id`)")
		require.NoError(t, err)
		assert.Empty(t, safeDDL, "an ADD INDEX cannot run while the refused DROP leaves the old index in place")
		assert.Equal(t, "ALTER TABLE `users` DROP INDEX `idx_email`, ADD INDEX `idx_email`(`email`, `org_id`)", destructiveDDL)
	})

	t.Run("a name collision is matched case-insensitively as MySQL matches it", func(t *testing.T) {
		safeDDL, destructiveDDL, err := SplitStorageDestructiveAlter("ALTER TABLE `users` DROP INDEX `IDX_Email`, ADD UNIQUE `idx_email` (`email`)")
		require.NoError(t, err)
		assert.Empty(t, safeDDL)
		assert.Equal(t, "ALTER TABLE `users` DROP INDEX `IDX_Email`, ADD UNIQUE `idx_email`(`email`)", destructiveDDL)
	})

	t.Run("an ADD INDEX under an unrelated name stays in the safe statement", func(t *testing.T) {
		safeDDL, destructiveDDL, err := SplitStorageDestructiveAlter("ALTER TABLE `users` DROP INDEX `idx_email`, ADD INDEX `idx_org` (`org_id`)")
		require.NoError(t, err)
		assert.Equal(t, "ALTER TABLE `users` ADD INDEX `idx_org`(`org_id`)", safeDDL)
		assert.Equal(t, "ALTER TABLE `users` DROP INDEX `idx_email`", destructiveDDL)
	})

	t.Run("a foreign key redefinition refuses the ADD with its DROP", func(t *testing.T) {
		safeDDL, destructiveDDL, err := SplitStorageDestructiveAlter("ALTER TABLE `users` DROP FOREIGN KEY `fk_org`, ADD CONSTRAINT `fk_org` FOREIGN KEY (`org_id`) REFERENCES `orgs` (`id`)")
		require.NoError(t, err)
		assert.Empty(t, safeDDL)
		assert.Contains(t, destructiveDDL, "DROP FOREIGN KEY `fk_org`")
		assert.Contains(t, destructiveDDL, "ADD CONSTRAINT `fk_org`")
	})

	t.Run("a non-ALTER statement is an error", func(t *testing.T) {
		_, _, err := SplitStorageDestructiveAlter("DROP TABLE `users`")
		require.Error(t, err)
	})

	t.Run("an unparseable statement is an error", func(t *testing.T) {
		_, _, err := SplitStorageDestructiveAlter("THIS IS NOT SQL")
		require.Error(t, err)
	})

	t.Run("multi-statement input is an error", func(t *testing.T) {
		_, _, err := SplitStorageDestructiveAlter("ALTER TABLE `users` ADD COLUMN `a` INT; ALTER TABLE `users` ADD COLUMN `b` INT")
		require.Error(t, err)
	})
}
