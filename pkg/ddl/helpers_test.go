package ddl

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSplitStatements(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    int
		wantErr bool
	}{
		{"single create table", "CREATE TABLE t1 (id INT)", 1, false},
		{"single alter table", "ALTER TABLE t1 ADD COLUMN x INT", 1, false},
		{"single drop table", "DROP TABLE t1", 1, false},
		{"trailing semicolon", "CREATE TABLE t1 (id INT);", 1, false},
		{"empty", "", 0, false},
		{"whitespace only", "   ", 0, false},
		{
			"multiple alter tables",
			"ALTER TABLE t1 ADD COLUMN x INT; ALTER TABLE t2 ADD COLUMN y INT",
			2,
			false,
		},
		{
			"multiline create table",
			`CREATE TABLE t1 (
				id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
				name VARCHAR(255) NOT NULL
			)`,
			1,
			false,
		},
		{
			"multiple create tables",
			"CREATE TABLE t1 (id INT); CREATE TABLE t2 (id INT)",
			2,
			false,
		},
		{
			"mixed create and alter",
			"CREATE TABLE t1 (id INT); ALTER TABLE t1 ADD COLUMN x INT",
			2,
			false,
		},
		{
			"mixed create alter drop",
			"CREATE TABLE t1 (id INT); ALTER TABLE t1 ADD COLUMN x INT; DROP TABLE t2",
			3,
			false,
		},
		{
			"invalid sql",
			"CREATE TABLE t1 (",
			0,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, err := SplitStatements(tt.content)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Len(t, stmts, tt.want)
		})
	}
}

func TestSplitStatements_Content(t *testing.T) {
	content := "CREATE TABLE t1 (id INT); ALTER TABLE t2 ADD COLUMN y INT"
	stmts, err := SplitStatements(content)
	require.NoError(t, err)
	require.Len(t, stmts, 2)
	assert.Contains(t, stmts[0], "t1")
	assert.Contains(t, stmts[1], "t2")
}

// A parse failure identifies the offending input (bounded) so an operator can
// see which statement failed without the full blob flooding logs.
func TestSplitStatements_ParseErrorIncludesPreview(t *testing.T) {
	_, err := SplitStatements("this is not valid SQL @@@")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `failed to parse SQL statements "this is not valid SQL @@@"`)
}

// TestClassifyStatement verifies the single-statement contract: each DDL type
// classifies to its typed StatementType and table, while compound input —
// where a destructive statement could ride behind the first statement's
// classification — is rejected with an error that tells the caller to split
// first. Empty and unparseable input also error.
func TestClassifyStatement(t *testing.T) {
	tests := []struct {
		name       string
		stmt       string
		wantType   statement.StatementType
		wantTable  string
		wantErrMsg string
	}{
		{
			name:      "create table",
			stmt:      "CREATE TABLE t1 (id INT)",
			wantType:  statement.StatementCreateTable,
			wantTable: "t1",
		},
		{
			name:      "alter table",
			stmt:      "ALTER TABLE t1 ADD COLUMN x INT",
			wantType:  statement.StatementAlterTable,
			wantTable: "t1",
		},
		{
			name:      "drop table",
			stmt:      "DROP TABLE t1",
			wantType:  statement.StatementDropTable,
			wantTable: "t1",
		},
		{
			name:      "rename table",
			stmt:      "RENAME TABLE t1 TO t2",
			wantType:  statement.StatementRenameTable,
			wantTable: "t1",
		},
		{
			name:      "single statement with trailing semicolon",
			stmt:      "ALTER TABLE t1 ADD COLUMN x INT;",
			wantType:  statement.StatementAlterTable,
			wantTable: "t1",
		},
		{
			name:       "alter followed by drop",
			stmt:       "ALTER TABLE t1 ADD COLUMN x INT; DROP TABLE t2",
			wantErrMsg: "parsed as 2 statements",
		},
		{
			name:       "two creates",
			stmt:       "CREATE TABLE t1 (id INT); CREATE TABLE t2 (id INT)",
			wantErrMsg: "parsed as 2 statements",
		},
		{
			name:       "three statements",
			stmt:       "CREATE TABLE t1 (id INT); ALTER TABLE t1 ADD COLUMN x INT; DROP TABLE t2",
			wantErrMsg: "parsed as 3 statements",
		},
		{
			name:       "empty",
			stmt:       "",
			wantErrMsg: "classify statement",
		},
		{
			name:       "whitespace only",
			stmt:       "   ",
			wantErrMsg: "classify statement",
		},
		{
			name:       "unparseable",
			stmt:       "not valid sql at all",
			wantErrMsg: "classify statement",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotType, gotTable, err := ClassifyStatement(tt.stmt)
			if tt.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrMsg)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantType, gotType, "statement type")
			assert.Equal(t, tt.wantTable, gotTable, "table")
		})
	}
}

// TestClassifyStatement_MultiStatementErrorNamesLeadingText verifies that the
// multi-statement error includes the leading text of the input (truncated for
// long blobs) so an operator can identify the offending content from the error
// alone.
func TestClassifyStatement_MultiStatementErrorNamesLeadingText(t *testing.T) {
	_, _, err := ClassifyStatement("ALTER TABLE t1 ADD COLUMN x INT; DROP TABLE t2")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ALTER TABLE t1 ADD COLUMN x INT")
	assert.Contains(t, err.Error(), "split with SplitStatements")

	long := "ALTER TABLE some_very_long_table_name ADD COLUMN a_very_long_column_name_here INT; DROP TABLE t2"
	_, _, err = ClassifyStatement(long)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "...")
	assert.NotContains(t, err.Error(), "DROP TABLE t2")
}

// TestStatementPreview_TruncatesOnRuneBoundary verifies that the preview
// truncates whole runes, so statements with multi-byte identifiers never
// produce invalid UTF-8 in error messages.
func TestStatementPreview_TruncatesOnRuneBoundary(t *testing.T) {
	got := statementPreview(strings.Repeat("é", 100))
	assert.Equal(t, strings.Repeat("é", 80)+"...", got)
	assert.True(t, utf8.ValidString(got))
}

func TestClassifyStatementOp(t *testing.T) {
	tests := []struct {
		stmt      string
		wantOp    string
		wantTable string
		wantErr   bool
	}{
		{"CREATE TABLE t1 (id INT)", "create", "t1", false},
		{"create table t1 (id int)", "create", "t1", false},
		{"ALTER TABLE t1 ADD COLUMN x INT", "alter", "t1", false},
		{"DROP TABLE t1", "drop", "t1", false},
		{"  ALTER TABLE t1 DROP COLUMN x", "alter", "t1", false},
		{"ALTER TABLE `my_table` ADD INDEX idx_name (name)", "alter", "my_table", false},
		{"CREATE TABLE `backticked` (id INT)", "create", "backticked", false},
		{"DROP TABLE IF EXISTS t1", "drop", "t1", false},
		{"not valid sql at all", "", "", true},
		{"ALTER TABLE t1 ADD COLUMN x INT; DROP TABLE t2", "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.stmt, func(t *testing.T) {
			gotOp, gotTable, err := ClassifyStatementOp(tt.stmt)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantOp, gotOp, "operation")
			assert.Equal(t, tt.wantTable, gotTable, "table")
		})
	}
}
