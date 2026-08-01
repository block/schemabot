package ddl

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The default parser must be the TiDB/Spirit implementation so MySQL and Vitess
// behavior is unchanged after routing the package helpers through the seam.
func TestDefaultParserIsTiDB(t *testing.T) {
	_, ok := defaultParser.(tidbStatementParser)
	assert.True(t, ok, "the package default parser must be the TiDB/Spirit implementation")
}

// The exported package helpers must be thin delegations to the default parser,
// i.e. they must return exactly what the TiDB implementation returns.
func TestPackageHelpersDelegateToDefaultParser(t *testing.T) {
	tidb := tidbStatementParser{}

	t.Run("Split matches SplitStatements", func(t *testing.T) {
		content := "CREATE TABLE `a` (`id` INT PRIMARY KEY);\nALTER TABLE `a` ADD COLUMN `b` INT;"

		got, gotErr := SplitStatements(content)
		want, wantErr := tidb.Split(content)

		require.NoError(t, wantErr)
		require.NoError(t, gotErr)
		assert.Equal(t, want, got)
	})

	t.Run("Classify matches ClassifyStatement", func(t *testing.T) {
		stmt := "ALTER TABLE `users` ADD COLUMN `email` VARCHAR(255)"

		gotType, gotTable, gotErr := ClassifyStatement(stmt)
		wantType, wantTable, wantErr := tidb.Classify(stmt)

		require.NoError(t, wantErr)
		require.NoError(t, gotErr)
		assert.Equal(t, wantType, gotType)
		assert.Equal(t, wantTable, gotTable)
		assert.Equal(t, statement.StatementAlterTable, gotType)
	})

	t.Run("Canonicalize matches", func(t *testing.T) {
		ddl := "alter table users add column email varchar(255)"

		got := Canonicalize(ddl)
		want := tidb.Canonicalize(ddl)

		assert.Equal(t, want, got)
	})
}

// Classify must still reject multi-statement input through the seam, so a
// destructive statement cannot hide behind the classification of the first one.
func TestSeamClassifyRejectsMultiStatement(t *testing.T) {
	_, _, err := ClassifyStatement("CREATE TABLE `a` (`id` INT); DROP TABLE `b`;")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "split with SplitStatements before classifying")
}
