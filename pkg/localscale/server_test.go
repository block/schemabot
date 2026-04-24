package localscale

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddAlgorithmInstant(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		// ALTER TABLE — should inject ALGORITHM=INSTANT
		{
			name: "add column",
			in:   "ALTER TABLE users ADD COLUMN age INT",
			want: "ALTER TABLE users ALGORITHM=INSTANT, ADD COLUMN age INT",
		},
		{
			name: "drop column",
			in:   "ALTER TABLE users DROP COLUMN bio",
			want: "ALTER TABLE users ALGORITHM=INSTANT, DROP COLUMN bio",
		},
		{
			name: "modify column",
			in:   "ALTER TABLE orders MODIFY COLUMN status ENUM('pending','active','done')",
			want: "ALTER TABLE orders ALGORITHM=INSTANT, MODIFY COLUMN status ENUM('pending','active','done')",
		},
		{
			name: "add index",
			in:   "ALTER TABLE users ADD INDEX idx_email (email)",
			want: "ALTER TABLE users ALGORITHM=INSTANT, ADD INDEX idx_email (email)",
		},
		{
			name: "backtick-quoted table",
			in:   "ALTER TABLE `my_table` DROP COLUMN x",
			want: "ALTER TABLE `my_table` ALGORITHM=INSTANT, DROP COLUMN x",
		},
		{
			name: "case-insensitive alter",
			in:   "alter table users ADD COLUMN x INT",
			want: "ALTER TABLE users ALGORITHM=INSTANT, ADD COLUMN x INT",
		},
		{
			name: "leading whitespace",
			in:   "  ALTER TABLE users ADD COLUMN x INT  ",
			want: "ALTER TABLE users ALGORITHM=INSTANT, ADD COLUMN x INT",
		},
		{
			name: "rename column",
			in:   "ALTER TABLE users RENAME COLUMN old_name TO new_name",
			want: "ALTER TABLE users ALGORITHM=INSTANT, RENAME COLUMN old_name TO new_name",
		},

		// Non-ALTER — should return ""
		{
			name: "create table",
			in:   "CREATE TABLE foo (id INT PRIMARY KEY)",
			want: "",
		},
		{
			name: "drop table",
			in:   "DROP TABLE bar",
			want: "",
		},
		{
			name: "create index",
			in:   "CREATE INDEX idx_name ON users (name)",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := addAlgorithmInstant(tt.in)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestHasAlterTableStatements(t *testing.T) {
	require.False(t, hasAlterTableStatements(map[string][]string{
		"testapp": {"CREATE TABLE foo (id INT PRIMARY KEY)", "DROP TABLE bar"},
	}))
	require.True(t, hasAlterTableStatements(map[string][]string{
		"testapp":         {"CREATE TABLE foo (id INT PRIMARY KEY)"},
		"testapp_sharded": {"ALTER TABLE users ADD COLUMN age INT"},
	}))
}

func TestQualifyAlterTableName(t *testing.T) {
	tests := []struct {
		name   string
		stmt   string
		schema string
		want   string
	}{
		{
			name:   "unquoted table",
			stmt:   "ALTER TABLE users ALGORITHM=INSTANT, ADD COLUMN age INT",
			schema: "_scratch",
			want:   "ALTER TABLE `_scratch`.`users` ALGORITHM=INSTANT, ADD COLUMN age INT",
		},
		{
			name:   "quoted table",
			stmt:   "ALTER TABLE `users` ALGORITHM=INSTANT, ADD COLUMN age INT",
			schema: "_scratch",
			want:   "ALTER TABLE `_scratch`.`users` ALGORITHM=INSTANT, ADD COLUMN age INT",
		},
		{
			name:   "escaped schema",
			stmt:   "ALTER TABLE `users` ALGORITHM=INSTANT, ADD COLUMN age INT",
			schema: "_scratch`x",
			want:   "ALTER TABLE `_scratch``x`.`users` ALGORITHM=INSTANT, ADD COLUMN age INT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, qualifyAlterTableName(tt.stmt, tt.schema))
		})
	}
}
