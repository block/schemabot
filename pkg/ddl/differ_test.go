package ddl

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiffTable_AddColumn(t *testing.T) {
	d := NewDiffer()

	source := "CREATE TABLE t1 (id INT PRIMARY KEY)"
	target := "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))"

	alters, err := d.DiffTable(source, target)
	require.NoError(t, err)
	require.Len(t, alters, 1)
	assert.Equal(t, "ALTER TABLE `t1` ADD COLUMN `name` varchar(100) NULL", alters[0])
}

func TestDiffTable_DropColumn(t *testing.T) {
	d := NewDiffer()

	source := "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))"
	target := "CREATE TABLE t1 (id INT PRIMARY KEY)"

	alters, err := d.DiffTable(source, target)
	require.NoError(t, err)
	require.Len(t, alters, 1)
	assert.Equal(t, "ALTER TABLE `t1` DROP COLUMN `name`", alters[0])
}

func TestDiffTable_ModifyColumn(t *testing.T) {
	d := NewDiffer()

	source := "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(50))"
	target := "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))"

	alters, err := d.DiffTable(source, target)
	require.NoError(t, err)
	require.Len(t, alters, 1)
	assert.Equal(t, "ALTER TABLE `t1` MODIFY COLUMN `name` varchar(100) NULL", alters[0])
}

func TestDiffTable_NoChanges(t *testing.T) {
	d := NewDiffer()

	source := "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))"
	target := "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))"

	alters, err := d.DiffTable(source, target)
	require.NoError(t, err)
	assert.Empty(t, alters)
}

func TestDiffTable_AddIndex(t *testing.T) {
	d := NewDiffer()

	source := "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))"
	target := "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_name (name))"

	alters, err := d.DiffTable(source, target)
	require.NoError(t, err)
	require.Len(t, alters, 1)
	assert.Equal(t, "ALTER TABLE `t1` ADD INDEX `idx_name` (`name`)", alters[0])
}

func TestDiffStatements_MultipleChanges(t *testing.T) {
	d := NewDiffer()

	sourceStmts := []string{
		"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50))",
		"CREATE TABLE orders (id INT PRIMARY KEY, user_id INT)",
	}
	targetStmts := []string{
		"CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), email VARCHAR(255))",
		"CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(100))",
	}

	result, err := d.DiffStatements(sourceStmts, targetStmts)
	require.NoError(t, err)

	// Should have:
	// 1. CREATE TABLE products (new table)
	// 2. DROP TABLE orders (removed table)
	// 3. ALTER TABLE users (modified columns)
	assert.Len(t, result.Tables, 3)
	assert.GreaterOrEqual(t, len(result.Statements), 3)

	// Check that expected statements are included
	foundProducts := false
	foundDropOrders := false
	foundAlterUsers := false
	for _, stmt := range result.Statements {
		if stmt == "DROP TABLE `orders`" {
			foundDropOrders = true
		}
		if strings.Contains(stmt, "products") {
			foundProducts = true
		}
		if strings.Contains(stmt, "ALTER TABLE `users`") {
			foundAlterUsers = true
		}
	}

	assert.True(t, foundDropOrders, "expected DROP TABLE orders statement")
	assert.True(t, foundProducts, "expected CREATE TABLE products statement")
	assert.True(t, foundAlterUsers, "expected ALTER TABLE users statement")
}

func TestDiffTable_ParseError(t *testing.T) {
	d := NewDiffer()

	_, err := d.DiffTable("invalid sql", "CREATE TABLE t1 (id INT)")
	require.Error(t, err)
}

func TestDiffStatements_ValidationIntegration(t *testing.T) {
	d := NewDiffer()

	// Verify that DiffStatements calls validation
	stmt := "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), INDEX idx_typo (namee))"
	_, err := d.DiffStatements(nil, []string{stmt})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SQL usage error")
}

func TestDiffStatements_ParseError(t *testing.T) {
	d := NewDiffer()

	// TiDB parser catches syntax errors
	stmt := "CREATE TABL users (id INT PRIMARY KEY)" // missing 'E' in TABLE
	_, err := d.DiffStatements(nil, []string{stmt})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SQL syntax error")
}
