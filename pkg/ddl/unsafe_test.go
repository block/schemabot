package ddl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// UnsafeStatement delegates the unsafe vocabulary to Spirit's UnsafeLinter, so
// these cases pin the boundary SchemaBot relies on: operations that destroy
// data are unsafe, structural operations that lose nothing (DROP INDEX,
// renames) are safe, and the verdict cannot be relaxed by configuration.
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

// SplitUnsafeAlter partitions a single ALTER TABLE by clause using Spirit's
// unsafe vocabulary, so these cases pin the split the storage-schema
// bootstrap relies on: a mixed ALTER yields a safe statement carrying the
// additive clauses and an unsafe statement carrying only the destructive
// ones, single-partition inputs leave the other side empty, and anything
// that is not exactly one parseable ALTER TABLE is an error.
func TestSplitUnsafeAlter(t *testing.T) {
	t.Run("mixed ALTER splits into safe and unsafe statements", func(t *testing.T) {
		safeDDL, unsafeDDL, err := SplitUnsafeAlter("ALTER TABLE `users` ADD COLUMN `phone` VARCHAR(20), DROP COLUMN `fax`, ADD INDEX `idx_phone` (`phone`)")
		require.NoError(t, err)
		assert.Equal(t, "ALTER TABLE `users` ADD COLUMN `phone` VARCHAR(20), ADD INDEX `idx_phone`(`phone`)", safeDDL)
		assert.Equal(t, "ALTER TABLE `users` DROP COLUMN `fax`", unsafeDDL)

		unsafe, _, err := UnsafeStatement(safeDDL)
		require.NoError(t, err)
		assert.False(t, unsafe, "safe partition must re-classify safe")
		unsafe, reason, err := UnsafeStatement(unsafeDDL)
		require.NoError(t, err)
		assert.True(t, unsafe, "unsafe partition must re-classify unsafe")
		assert.Contains(t, reason, "fax")
	})

	t.Run("all-safe ALTER leaves the unsafe statement empty", func(t *testing.T) {
		safeDDL, unsafeDDL, err := SplitUnsafeAlter("ALTER TABLE `users` ADD COLUMN `phone` VARCHAR(20)")
		require.NoError(t, err)
		assert.Equal(t, "ALTER TABLE `users` ADD COLUMN `phone` VARCHAR(20)", safeDDL)
		assert.Empty(t, unsafeDDL)
	})

	t.Run("all-unsafe ALTER leaves the safe statement empty", func(t *testing.T) {
		safeDDL, unsafeDDL, err := SplitUnsafeAlter("ALTER TABLE `users` DROP COLUMN `fax`, DROP COLUMN `pager`")
		require.NoError(t, err)
		assert.Empty(t, safeDDL)
		assert.Equal(t, "ALTER TABLE `users` DROP COLUMN `fax`, DROP COLUMN `pager`", unsafeDDL)
	})

	t.Run("DROP INDEX loses no data and lands in the safe statement", func(t *testing.T) {
		safeDDL, unsafeDDL, err := SplitUnsafeAlter("ALTER TABLE `users` DROP INDEX `idx_email`, DROP COLUMN `email`")
		require.NoError(t, err)
		assert.Equal(t, "ALTER TABLE `users` DROP INDEX `idx_email`", safeDDL)
		assert.Equal(t, "ALTER TABLE `users` DROP COLUMN `email`", unsafeDDL)
	})

	t.Run("a non-ALTER statement is an error", func(t *testing.T) {
		_, _, err := SplitUnsafeAlter("DROP TABLE `users`")
		require.Error(t, err)
	})

	t.Run("an unparseable statement is an error", func(t *testing.T) {
		_, _, err := SplitUnsafeAlter("THIS IS NOT SQL")
		require.Error(t, err)
	})

	t.Run("multi-statement input is an error", func(t *testing.T) {
		_, _, err := SplitUnsafeAlter("ALTER TABLE `users` ADD COLUMN `a` INT; ALTER TABLE `users` ADD COLUMN `b` INT")
		require.Error(t, err)
	})
}
