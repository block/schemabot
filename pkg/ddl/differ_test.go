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

// mysqlExpressionTable is what MySQL 8.0 returns from SHOW CREATE TABLE for a
// table declaring a generated column and a range of DEFAULT expressions. It is
// the source side of a plan: whatever a user writes, this is what the differ
// compares against.
const mysqlExpressionTable = "CREATE TABLE `p1` (\n" +
	"  `id` int NOT NULL,\n" +
	"  `c` timestamp NULL DEFAULT (now()),\n" +
	"  `d` timestamp NULL DEFAULT CURRENT_TIMESTAMP,\n" +
	"  `u` char(36) DEFAULT (uuid()),\n" +
	"  `j` json DEFAULT (json_array()),\n" +
	"  `n` int DEFAULT ((1 + 2)),\n" +
	"  `price` int DEFAULT NULL,\n" +
	"  `qty` int DEFAULT NULL,\n" +
	"  `total` int GENERATED ALWAYS AS ((`price` * `qty`)) STORED,\n" +
	"  PRIMARY KEY (`id`)\n" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"

// A converged table plans no work. MySQL preserves the exact parenthesization
// a user wrote around generated column and DEFAULT expressions, and it renders
// nullability and DEFAULT NULL that a hand-written schema file leaves implicit,
// so the differ has to compare the parsed expressions rather than their text.
// Otherwise every plan against such a table would propose an ALTER that changes
// nothing.
func TestDiffTable_ExpressionColumnsConvergedPlanNoChange(t *testing.T) {
	d := NewDiffer()

	declared := "CREATE TABLE `p1` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `c` timestamp DEFAULT (now()),\n" +
		"  `d` timestamp DEFAULT CURRENT_TIMESTAMP,\n" +
		"  `u` char(36) DEFAULT (uuid()),\n" +
		"  `j` json DEFAULT (json_array()),\n" +
		"  `n` int DEFAULT ((1 + 2)),\n" +
		"  `price` int,\n" +
		"  `qty` int,\n" +
		"  `total` int GENERATED ALWAYS AS ((`price` * `qty`)) STORED,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"

	alters, err := d.DiffTable(mysqlExpressionTable, declared)
	require.NoError(t, err)
	assert.Empty(t, alters, "converged expression columns must not plan an ALTER")
}

// Redundant parentheses are formatting, not schema. A user who writes the
// expression with or without an outer pair gets the same table, so neither
// spelling may plan an ALTER against the other.
func TestDiffTable_RedundantParenthesesAreNotAChange(t *testing.T) {
	d := NewDiffer()

	tests := []struct {
		name   string
		source string
		target string
	}{
		{
			name:   "generated column",
			source: "CREATE TABLE t1 (id INT PRIMARY KEY, price INT, qty INT, total INT GENERATED ALWAYS AS ((price * qty)) STORED)",
			target: "CREATE TABLE t1 (id INT PRIMARY KEY, price INT, qty INT, total INT GENERATED ALWAYS AS (price * qty) STORED)",
		},
		{
			name:   "DEFAULT expression",
			source: "CREATE TABLE t1 (id INT PRIMARY KEY, n INT DEFAULT ((1 + 2)))",
			target: "CREATE TABLE t1 (id INT PRIMARY KEY, n INT DEFAULT (1 + 2))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			alters, err := d.DiffTable(tt.source, tt.target)
			require.NoError(t, err)
			assert.Empty(t, alters)

			alters, err = d.DiffTable(tt.target, tt.source)
			require.NoError(t, err)
			assert.Empty(t, alters)
		})
	}
}

// A generated column's storage kind is part of its definition: switching
// between STORED and VIRTUAL rewrites the table, so the differ must report it
// rather than treat the two as the same expression.
func TestDiffTable_GeneratedColumnStorageChange(t *testing.T) {
	d := NewDiffer()

	source := "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT GENERATED ALWAYS AS (a * 2) VIRTUAL)"
	target := "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT GENERATED ALWAYS AS (a * 2) STORED)"

	alters, err := d.DiffTable(source, target)
	require.NoError(t, err)
	require.Len(t, alters, 1)
	assert.Contains(t, alters[0], "`b`")
	assert.Contains(t, alters[0], "STORED")
}

// Changing a generated column's expression is a real schema change.
func TestDiffTable_GeneratedColumnExpressionChange(t *testing.T) {
	d := NewDiffer()

	source := "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT GENERATED ALWAYS AS (a * 2) STORED)"
	target := "CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b INT GENERATED ALWAYS AS (a * 3) STORED)"

	alters, err := d.DiffTable(source, target)
	require.NoError(t, err)
	require.Len(t, alters, 1)
	assert.Contains(t, alters[0], "`a`*3")
}

// DEFAULT CURRENT_TIMESTAMP and a DEFAULT expression are different column
// definitions in MySQL, which reports each back exactly as it was declared.
// Treating them as interchangeable would let a schema file drift from the
// table it describes without any plan ever showing it.
func TestDiffTable_ExpressionDefaultDiffersFromCurrentTimestamp(t *testing.T) {
	d := NewDiffer()

	source := "CREATE TABLE t1 (id INT PRIMARY KEY, c TIMESTAMP NULL DEFAULT (now()))"
	target := "CREATE TABLE t1 (id INT PRIMARY KEY, c TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP)"

	alters, err := d.DiffTable(source, target)
	require.NoError(t, err)
	require.Len(t, alters, 1)
	assert.Equal(t, "ALTER TABLE `t1` MODIFY COLUMN `c` timestamp NULL DEFAULT current_timestamp", alters[0])
}

// MySQL emits nested parentheses of its own accord for a parenthesized DEFAULT
// expression, so a schema pulled straight from SHOW CREATE TABLE contains them
// and must parse.
func TestDiffTable_NestedParenthesizedDefaultParses(t *testing.T) {
	d := NewDiffer()

	source := "CREATE TABLE t1 (id INT PRIMARY KEY)"
	target := "CREATE TABLE t1 (id INT PRIMARY KEY, n INT DEFAULT ((1 + 2)))"

	alters, err := d.DiffTable(source, target)
	require.NoError(t, err)
	require.Len(t, alters, 1)
	assert.Equal(t, "ALTER TABLE `t1` ADD COLUMN `n` int NULL DEFAULT (1+2)", alters[0])
}

// Expression defaults that call a function keep the call syntax when the
// differ renders them, so the ALTER it plans is valid DDL.
func TestDiffTable_FunctionCallDefaultRendersAsACall(t *testing.T) {
	d := NewDiffer()

	tests := []struct {
		name     string
		column   string
		expected string
	}{
		{
			name:     "uuid",
			column:   "CHAR(36) DEFAULT (uuid())",
			expected: "ALTER TABLE `t1` ADD COLUMN `c` char(36) NULL DEFAULT (uuid())",
		},
		{
			name:     "json_array",
			column:   "JSON DEFAULT (json_array())",
			expected: "ALTER TABLE `t1` ADD COLUMN `c` json NULL DEFAULT (json_array())",
		},
		{
			name:     "concat",
			column:   "VARCHAR(10) DEFAULT (concat('a', 'b'))",
			expected: "ALTER TABLE `t1` ADD COLUMN `c` varchar(10) NULL DEFAULT (concat('a', 'b'))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			alters, err := d.DiffTable(
				"CREATE TABLE t1 (id INT PRIMARY KEY)",
				"CREATE TABLE t1 (id INT PRIMARY KEY, c "+tt.column+")",
			)
			require.NoError(t, err)
			require.Len(t, alters, 1)
			assert.Equal(t, tt.expected, alters[0])
		})
	}
}
