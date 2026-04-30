package ddl

import (
	"testing"

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
