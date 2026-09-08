package lint

import (
	"sync"
	"testing"

	"github.com/block/spirit/pkg/table"
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

func TestPlanChangesConcurrent(t *testing.T) {
	current := []table.TableSchema{{
		Name:   "users",
		Schema: "CREATE TABLE `users` (`id` bigint NOT NULL, `email` varchar(255) NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB",
	}}
	desired := []table.TableSchema{{
		Name:   "users",
		Schema: "CREATE TABLE `users` (`id` bigint NOT NULL, `email` varchar(255) NOT NULL, `full_name` varchar(255) NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB",
	}}

	errs := make(chan error, 32)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			_, err := PlanChanges(current, desired, nil, New().SpiritConfig())
			errs <- err
		})
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
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

// A declared column written as `boolean NOT NULL DEFAULT FALSE` is stored by
// MySQL as `tinyint(1) NOT NULL DEFAULT '0'`, so the planner has to read the
// keyword and the integer as the same default. Reading them as different is the
// worst shape of planner bug in front of a live database: every plan emits an
// ALTER that re-stores the value it already holds, the ALTER succeeds, and the
// next plan emits it again — a schema that never converges and a merge gate
// that never clears on its own.
func TestPlanChangesBooleanKeywordDefaultConverges(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		live     string
	}{
		{
			name:     "FALSE against the stored 0",
			declared: "`flag` boolean NOT NULL DEFAULT FALSE",
			live:     "`flag` tinyint(1) NOT NULL DEFAULT '0'",
		},
		{
			name:     "TRUE against the stored 1",
			declared: "`flag` boolean NOT NULL DEFAULT TRUE",
			live:     "`flag` tinyint(1) NOT NULL DEFAULT '1'",
		},
		{
			name:     "a nullable column declared with the keyword",
			declared: "`flag` boolean DEFAULT FALSE",
			live:     "`flag` tinyint(1) DEFAULT '0'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			current := []table.TableSchema{{Name: "widgets", Schema: booleanDefaultTable(tt.live)}}
			desired := []table.TableSchema{{Name: "widgets", Schema: booleanDefaultTable(tt.declared)}}

			plan, err := PlanChanges(current, desired, nil, New().SpiritConfig())
			require.NoError(t, err)
			assert.False(t, plan.HasChanges())
			assert.Empty(t, plan.Statements())
		})
	}

	t.Run("a default that really did change still plans an alter", func(t *testing.T) {
		current := []table.TableSchema{{Name: "widgets", Schema: booleanDefaultTable("`flag` tinyint(1) NOT NULL DEFAULT '1'")}}
		desired := []table.TableSchema{{Name: "widgets", Schema: booleanDefaultTable("`flag` boolean NOT NULL DEFAULT FALSE")}}

		plan, err := PlanChanges(current, desired, nil, New().SpiritConfig())
		require.NoError(t, err)
		require.True(t, plan.HasChanges())
		require.Len(t, plan.Statements(), 1)
		assert.Contains(t, plan.Statements()[0], "MODIFY COLUMN `flag`")
	})
}

// booleanDefaultTable renders a one-column table around the column definition
// under test, so each case states only the default it is about.
func booleanDefaultTable(column string) string {
	return "CREATE TABLE `widgets` (`id` bigint unsigned NOT NULL AUTO_INCREMENT, " + column +
		", PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
}
