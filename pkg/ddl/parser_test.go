package ddl

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The default parser must be the TiDB/Spirit implementation so MySQL and Vitess
// behavior is unchanged after routing the package helpers through the seam.
func TestDefaultParserIsTiDB(t *testing.T) {
	_, ok := defaultParser.(tidbStatementParser)
	assert.True(t, ok, "the package default parser must be the TiDB/Spirit implementation")
}

// fakeStatementParser is a canned-response StatementParser whose results are
// distinguishable from anything the TiDB implementation would produce, so a
// test can prove the package helpers reach whatever parser is installed as the
// default rather than a hardcoded implementation.
type fakeStatementParser struct {
	splitResult []string
	splitErr    error

	classifyType  StatementType
	classifyTable string
	classifyErr   error

	canonicalized string
}

func (f fakeStatementParser) Split(string) ([]string, error) {
	return f.splitResult, f.splitErr
}

func (f fakeStatementParser) Classify(string) (StatementType, string, error) {
	return f.classifyType, f.classifyTable, f.classifyErr
}

func (f fakeStatementParser) Canonicalize(string) string {
	return f.canonicalized
}

// The exported package helpers must route through the package's default
// StatementParser: with a fake parser installed, the helpers must return the
// fake's canned results — including its errors — not the TiDB parser's view of
// the input.
func TestPackageHelpersDelegateToDefaultParser(t *testing.T) {
	fake := fakeStatementParser{
		splitResult:   []string{"FAKE SPLIT ONE", "FAKE SPLIT TWO"},
		classifyType:  StatementRenameTable,
		classifyTable: "fake_table",
		canonicalized: "FAKE CANONICAL",
	}

	orig := defaultParser
	defaultParser = fake
	t.Cleanup(func() { defaultParser = orig })

	t.Run("SplitStatements returns the default parser's result", func(t *testing.T) {
		got, err := SplitStatements("CREATE TABLE `a` (`id` INT PRIMARY KEY);")
		require.NoError(t, err)
		assert.Equal(t, []string{"FAKE SPLIT ONE", "FAKE SPLIT TWO"}, got)
	})

	t.Run("ClassifyStatement returns the default parser's result", func(t *testing.T) {
		gotType, gotTable, err := ClassifyStatement("CREATE TABLE `a` (`id` INT PRIMARY KEY)")
		require.NoError(t, err)
		assert.Equal(t, StatementRenameTable, gotType)
		assert.Equal(t, "fake_table", gotTable)
	})

	t.Run("ClassifyStatementOp converts the default parser's type", func(t *testing.T) {
		op, table, err := ClassifyStatementOp("CREATE TABLE `a` (`id` INT PRIMARY KEY)")
		require.NoError(t, err)
		assert.Equal(t, "rename", op)
		assert.Equal(t, "fake_table", table)
	})

	t.Run("Canonicalize returns the default parser's result", func(t *testing.T) {
		assert.Equal(t, "FAKE CANONICAL", Canonicalize("alter table users add column email varchar(255)"))
	})
}

// Errors from the default parser must propagate through the package helpers
// unchanged, so callers see the installed parser's diagnostics.
func TestPackageHelpersPropagateDefaultParserErrors(t *testing.T) {
	splitErr := errors.New("fake split failure")
	classifyErr := errors.New("fake classify failure")
	fake := fakeStatementParser{splitErr: splitErr, classifyErr: classifyErr}

	orig := defaultParser
	defaultParser = fake
	t.Cleanup(func() { defaultParser = orig })

	_, err := SplitStatements("CREATE TABLE `a` (`id` INT PRIMARY KEY);")
	assert.ErrorIs(t, err, splitErr)

	_, _, err = ClassifyStatement("CREATE TABLE `a` (`id` INT PRIMARY KEY)")
	assert.ErrorIs(t, err, classifyErr)

	_, _, err = ClassifyStatementOp("CREATE TABLE `a` (`id` INT PRIMARY KEY)")
	assert.ErrorIs(t, err, classifyErr)
}

// Classify must still reject multi-statement input through the seam, so a
// destructive statement cannot hide behind the classification of the first one.
func TestSeamClassifyRejectsMultiStatement(t *testing.T) {
	_, _, err := ClassifyStatement("CREATE TABLE `a` (`id` INT); DROP TABLE `b`;")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "split with SplitStatements before classifying")
}

// The TiDB implementation must translate every Spirit statement type it can
// return into the pkg/ddl-owned vocabulary, preserving the classification a
// caller branches on.
func TestTiDBClassifyTranslatesToOwnedTypes(t *testing.T) {
	tests := []struct {
		name      string
		stmt      string
		wantType  StatementType
		wantTable string
	}{
		{
			name:      "create table",
			stmt:      "CREATE TABLE `users` (`id` INT PRIMARY KEY)",
			wantType:  StatementCreateTable,
			wantTable: "users",
		},
		{
			name:      "alter table",
			stmt:      "ALTER TABLE `users` ADD COLUMN `email` VARCHAR(255)",
			wantType:  StatementAlterTable,
			wantTable: "users",
		},
		{
			name:      "drop table",
			stmt:      "DROP TABLE `users`",
			wantType:  StatementDropTable,
			wantTable: "users",
		},
		{
			name:      "rename table",
			stmt:      "RENAME TABLE `users` TO `customers`",
			wantType:  StatementRenameTable,
			wantTable: "users",
		},
		{
			name:      "truncate table",
			stmt:      "TRUNCATE TABLE `users`",
			wantType:  StatementTruncateTable,
			wantTable: "users",
		},
		{
			name:      "create index",
			stmt:      "CREATE INDEX `idx_email` ON `users` (`email`)",
			wantType:  StatementCreateIndex,
			wantTable: "users",
		},
		{
			name:      "drop index",
			stmt:      "DROP INDEX `idx_email` ON `users`",
			wantType:  StatementDropIndex,
			wantTable: "users",
		},
		{
			name:     "insert",
			stmt:     "INSERT INTO `users` (`id`) VALUES (1)",
			wantType: StatementInsert,
		},
		{
			name:     "update",
			stmt:     "UPDATE `users` SET `email` = 'a@b.c'",
			wantType: StatementUpdate,
		},
		{
			name:     "delete",
			stmt:     "DELETE FROM `users`",
			wantType: StatementDelete,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotType, gotTable, err := tidbStatementParser{}.Classify(tt.stmt)
			require.NoError(t, err)
			assert.Equal(t, tt.wantType, gotType)
			if tt.wantTable != "" {
				assert.Equal(t, tt.wantTable, gotTable)
			}
		})
	}
}
