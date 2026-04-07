package ddl

import (
	"testing"

	"github.com/block/spirit/pkg/statement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateIndexColumns(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid index on existing column",
			sql:         "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name))",
			expectError: false,
		},
		{
			name:        "valid composite index on existing columns",
			sql:         "CREATE TABLE users (id INT PRIMARY KEY, first_name VARCHAR(50), last_name VARCHAR(50), INDEX idx_full (first_name, last_name))",
			expectError: false,
		},
		{
			name:        "valid unique index on existing column",
			sql:         "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255), UNIQUE INDEX idx_email (email))",
			expectError: false,
		},
		{
			name:        "valid primary key",
			sql:         "CREATE TABLE users (id INT, name VARCHAR(100), PRIMARY KEY (id))",
			expectError: false,
		},
		{
			name:        "invalid index references non-existent column - typo",
			sql:         "CREATE TABLE users (id INT PRIMARY KEY, full_name VARCHAR(100), INDEX idx_name (full_name1))",
			expectError: true,
			errorMsg:    "non-existent column",
		},
		{
			name:        "invalid composite index - one column missing",
			sql:         "CREATE TABLE users (id INT PRIMARY KEY, first_name VARCHAR(50), INDEX idx_full (first_name, last_name))",
			expectError: true,
			errorMsg:    "last_name",
		},
		{
			name:        "case insensitive column matching - should pass",
			sql:         "CREATE TABLE users (id INT PRIMARY KEY, Name VARCHAR(100), INDEX idx_name (NAME))",
			expectError: false,
		},
		{
			name:        "no indexes - should pass",
			sql:         "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))",
			expectError: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ct, err := statement.ParseCreateTable(tc.sql)
			require.NoError(t, err, "failed to parse SQL")

			err = ValidateIndexColumns(ct)

			if tc.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errorMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateDuplicateColumns(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "no duplicates",
			sql:         "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))",
			expectError: false,
		},
		{
			name:        "duplicate column",
			sql:         "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), name VARCHAR(50))",
			expectError: true,
			errorMsg:    "duplicate column",
		},
		{
			name:        "case insensitive duplicate",
			sql:         "CREATE TABLE users (id INT PRIMARY KEY, Name VARCHAR(100), NAME VARCHAR(50))",
			expectError: true,
			errorMsg:    "duplicate column",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ct, err := statement.ParseCreateTable(tc.sql)
			require.NoError(t, err, "failed to parse SQL")

			err = ValidateDuplicateColumns(ct)

			if tc.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errorMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateCreateTable(t *testing.T) {
	t.Run("catches invalid index column", func(t *testing.T) {
		sql := "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_typo (namee))"
		ct, err := statement.ParseCreateTable(sql)
		require.NoError(t, err)

		err = ValidateCreateTable(ct)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "non-existent column")
	})

	t.Run("catches duplicate column", func(t *testing.T) {
		sql := "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), name VARCHAR(50))"
		ct, err := statement.ParseCreateTable(sql)
		require.NoError(t, err)

		err = ValidateCreateTable(ct)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate column")
	})
}
