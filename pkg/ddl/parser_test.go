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

func (f fakeStatementParser) CreateTableColumns(string) ([]string, error) {
	return nil, nil
}

func (f fakeStatementParser) CreateIndex(string) (string, string, bool, error) {
	return "", "", false, nil
}

func (f fakeStatementParser) CostScalesWithTableSize(string) (bool, error) {
	return false, nil
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
			name:      "create view",
			stmt:      "CREATE VIEW `active_users` AS SELECT `id` FROM `users`",
			wantType:  StatementCreateView,
			wantTable: "active_users",
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

// restoreCanonical must never truncate its input: multi-statement input is
// returned unchanged instead of restoring only the first statement.
func TestRestoreCanonicalMultiStatementUnchanged(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "two create statements",
			input: "CREATE TABLE users (id INT); CREATE TABLE orders (id INT)",
		},
		{
			name:  "create followed by drop",
			input: "CREATE TABLE users (id INT); DROP TABLE orders",
		},
		{
			name:  "two drop statements",
			input: "DROP TABLE users; DROP TABLE orders",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.input, restoreCanonical(tt.input))
		})
	}
}

// CostScalesWithTableSize scopes the plan comment's table-size section: a
// statement reports true when it builds an index, copies or rebuilds the
// table, or scans it to validate a constraint, so a size line renders exactly
// for the changes whose cost grows with the table. Provably metadata-only
// clauses stay quiet.
func TestTiDBCostScalesWithTableSize(t *testing.T) {
	p := tidbStatementParser{}

	tests := []struct {
		name string
		stmt string
		want bool
	}{
		{"add index", "ALTER TABLE `mutes` ADD INDEX `idx_created_at` (`created_at`)", true},
		{"add key", "ALTER TABLE `mutes` ADD KEY `idx_created_at` (`created_at`)", true},
		{"add unique key", "ALTER TABLE `mutes` ADD UNIQUE KEY `uniq_slug` (`slug`)", true},
		{"add fulltext index", "ALTER TABLE `mutes` ADD FULLTEXT INDEX `ft_body` (`body`)", true},
		{"add spatial index", "ALTER TABLE `mutes` ADD SPATIAL INDEX `sp_location` (`location`)", true},
		{"add primary key", "ALTER TABLE `mutes` ADD PRIMARY KEY (`id`)", true},
		{"index add among metadata-only clauses", "ALTER TABLE `mutes` ADD COLUMN `reason` varchar(255), ADD INDEX `idx_reason` (`reason`)", true},
		{"standalone create index", "CREATE INDEX `idx_created_at` ON `mutes` (`created_at`)", true},
		{"modify column widening", "ALTER TABLE `mutes` MODIFY COLUMN `reason` varchar(500)", true},
		{"modify column type change", "ALTER TABLE `mutes` MODIFY COLUMN `count` bigint", true},
		{"change column", "ALTER TABLE `mutes` CHANGE COLUMN `reason` `cause` varchar(255)", true},
		{"add foreign key", "ALTER TABLE `mutes` ADD CONSTRAINT `fk_user` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)", true},
		{"add check constraint", "ALTER TABLE `mutes` ADD CONSTRAINT `chk_positive` CHECK (`count` > 0)", true},
		{"convert charset", "ALTER TABLE `mutes` CONVERT TO CHARACTER SET utf8mb4", true},
		{"engine rebuild", "ALTER TABLE `mutes` ENGINE=InnoDB", true},
		{"add column with inline unique", "ALTER TABLE `mutes` ADD COLUMN `slug` varchar(64) UNIQUE", true},
		{"add stored generated column", "ALTER TABLE `mutes` ADD COLUMN `total` int AS (`a` + `b`) STORED", true},
		{"drop column", "ALTER TABLE `mutes` DROP COLUMN `reason`", true},
		{"add hash partitions", "ALTER TABLE `mutes` ADD PARTITION PARTITIONS 4", true},
		{"add range partition", "ALTER TABLE `mutes` ADD PARTITION (PARTITION `p2027` VALUES LESS THAN (2028))", false},
		{"drop partition", "ALTER TABLE `mutes` DROP PARTITION `p2020`", false},
		{"truncate partition", "ALTER TABLE `mutes` TRUNCATE PARTITION `p2020`", false},
		{"add column only", "ALTER TABLE `mutes` ADD COLUMN `reason` varchar(255)", false},
		{"add virtual generated column", "ALTER TABLE `mutes` ADD COLUMN `total` int AS (`a` + `b`) VIRTUAL", false},
		{"rename column", "ALTER TABLE `mutes` RENAME COLUMN `reason` TO `cause`", false},
		{"rename table", "ALTER TABLE `mutes` RENAME TO `silences`", false},
		{"set default", "ALTER TABLE `mutes` ALTER COLUMN `count` SET DEFAULT 0", false},
		{"drop index", "ALTER TABLE `mutes` DROP INDEX `idx_created_at`", false},
		{"drop foreign key", "ALTER TABLE `mutes` DROP FOREIGN KEY `fk_user`", false},
		{"index invisible", "ALTER TABLE `mutes` ALTER INDEX `idx_created_at` INVISIBLE", false},
		{"comment only", "ALTER TABLE `mutes` COMMENT='muted things'", false},
		{"create table with index", "CREATE TABLE `mutes` (`id` bigint, KEY `idx_id` (`id`))", false},
		{"drop table", "DROP TABLE `mutes`", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.CostScalesWithTableSize(tt.stmt)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	t.Run("unparseable statement is an error", func(t *testing.T) {
		_, err := p.CostScalesWithTableSize("ALTER TABLE ADD INDEX WHAT")
		assert.Error(t, err)
	})

	t.Run("multi-statement input is an error", func(t *testing.T) {
		_, err := p.CostScalesWithTableSize("ALTER TABLE `a` ADD INDEX `i` (`c`); DROP TABLE `b`")
		assert.Error(t, err)
	})
}
