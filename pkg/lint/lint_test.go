package lint

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLintStatements_DropTable(t *testing.T) {
	linter := New()

	results, hasUnsafe, err := linter.LintStatements([]string{
		"DROP TABLE users",
	})
	require.NoError(t, err)

	assert.True(t, hasUnsafe, "expected hasUnsafe to be true for DROP TABLE")
	require.NotEmpty(t, results, "expected at least one lint result")

	found := false
	for _, r := range results {
		if r.IsUnsafe && r.Linter == "unsafe" && r.Table == "users" {
			found = true
			break
		}
	}
	assert.True(t, found, "expected unsafe result for DROP TABLE users, got: %+v", results)
}

func TestLintStatements_DropColumn(t *testing.T) {
	linter := New()

	results, hasUnsafe, err := linter.LintStatements([]string{
		"ALTER TABLE users DROP COLUMN email",
	})
	require.NoError(t, err)

	assert.True(t, hasUnsafe, "expected hasUnsafe to be true for DROP COLUMN")
	require.NotEmpty(t, results, "expected at least one lint result")

	found := false
	for _, r := range results {
		if r.IsUnsafe && r.Linter == "unsafe" && r.Table == "users" {
			found = true
			// Spirit's unsafe linter doesn't populate column name
			break
		}
	}
	assert.True(t, found, "expected unsafe result for DROP COLUMN, got: %+v", results)
}

func TestLintStatements_DropIndex(t *testing.T) {
	linter := New()

	results, hasUnsafe, err := linter.LintStatements([]string{
		"ALTER TABLE users DROP INDEX idx_email",
	})
	require.NoError(t, err)

	// With raiseError=true on invisible_index_before_drop, DROP INDEX is now unsafe.
	// This requires the user to first make the index invisible to verify it's not needed,
	// then drop it with --allow-unsafe.
	assert.True(t, hasUnsafe, "expected hasUnsafe=true for DROP INDEX (invisible_index_before_drop with raiseError=true)")

	// Should have an error from invisible_index_before_drop linter
	found := false
	for _, r := range results {
		if r.Linter == "invisible_index_before_drop" {
			found = true
			assert.Equal(t, "error", r.Severity, "expected severity='error' for invisible_index_before_drop")
			assert.True(t, r.IsUnsafe, "expected IsUnsafe=true for invisible_index_before_drop with raiseError=true")
			break
		}
	}
	assert.True(t, found, "expected invisible_index_before_drop error for DROP INDEX, got: %+v", results)
}

func TestLintStatements_SafeAlter(t *testing.T) {
	linter := New()

	// Adding a column is safe
	results, hasUnsafe, err := linter.LintStatements([]string{
		"ALTER TABLE users ADD COLUMN age INT",
	})
	require.NoError(t, err)

	assert.False(t, hasUnsafe, "expected hasUnsafe to be false for ADD COLUMN, got results: %+v", results)
}

func TestLintStatements_CreateTable(t *testing.T) {
	linter := New()

	// CREATE TABLE is safe
	results, hasUnsafe, err := linter.LintStatements([]string{
		"CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR(100))",
	})
	require.NoError(t, err)

	assert.False(t, hasUnsafe, "expected hasUnsafe to be false for CREATE TABLE, got results: %+v", results)
}

func TestLintStatements_MultipleStatements(t *testing.T) {
	linter := New()

	results, hasUnsafe, err := linter.LintStatements([]string{
		"ALTER TABLE users ADD COLUMN phone VARCHAR(20)", // safe
		"ALTER TABLE users DROP COLUMN old_phone",        // unsafe
		"CREATE TABLE orders (id BIGINT PRIMARY KEY)",    // safe
		"DROP TABLE legacy_data",                         // unsafe
	})
	require.NoError(t, err)

	assert.True(t, hasUnsafe, "expected hasUnsafe to be true when any statement is unsafe")

	// Should have at least 2 unsafe results (DROP COLUMN and DROP TABLE)
	unsafeCount := 0
	for _, r := range results {
		if r.IsUnsafe {
			unsafeCount++
		}
	}
	assert.GreaterOrEqual(t, unsafeCount, 2, "expected at least 2 unsafe results, got: %+v", results)
}

func TestLintStatements_InvalidSQL(t *testing.T) {
	linter := New()

	_, _, err := linter.LintStatements([]string{
		"NOT VALID SQL AT ALL",
	})
	assert.Error(t, err)
}

func TestLintSchema_InvalidSQL(t *testing.T) {
	linter := New()

	_, err := linter.LintSchema(map[string]string{
		"bad.sql": "CREATE TABLE t1 (",
	})
	assert.Error(t, err)
}

func TestToEngineWarnings(t *testing.T) {
	results := []Result{
		{Table: "users", Column: "email", Linter: "unsafe", Message: "DROP COLUMN", Severity: "error", IsUnsafe: true},
		{Table: "orders", Linter: "primary_key", Message: "INT PK", Severity: "warning", IsUnsafe: false},
	}

	warnings := ToEngineWarnings(results)

	require.Len(t, warnings, 2)

	assert.Equal(t, "users", warnings[0].Table)
	assert.Equal(t, "email", warnings[0].Column)
	assert.Equal(t, "orders", warnings[1].Table)
}
