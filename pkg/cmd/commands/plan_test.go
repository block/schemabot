package commands

import (
	"bytes"
	"io"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/templates"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/ui"
)

// stripAnsi removes ANSI escape codes from a string.
var ansiRegex = regexp.MustCompile(`\x1b\[[0-9;]*m`)

func stripAnsi(s string) string {
	return ansiRegex.ReplaceAllString(s, "")
}

// captureStdout captures stdout during a function call.
// Reads from the pipe concurrently to avoid deadlock when output exceeds the pipe buffer.
func captureStdout(f func()) string {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	var buf bytes.Buffer
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(&buf, r)
		close(done)
	}()

	f()

	utils.CloseAndLog(w)
	os.Stdout = old
	<-done
	return buf.String()
}

// planWithTables creates a PlanResponse with tables in a single "default" namespace.
func planWithTables(tables ...*apitypes.TableChangeResponse) *apitypes.PlanResponse {
	return &apitypes.PlanResponse{
		Changes: []*apitypes.SchemaChangeResponse{
			{Namespace: "default", TableChanges: tables},
		},
	}
}

// planWithTablesAndEngine creates a PlanResponse with an engine and tables in a single "default" namespace.
func planWithTablesAndEngine(engine string, tables ...*apitypes.TableChangeResponse) *apitypes.PlanResponse {
	return &apitypes.PlanResponse{
		Engine: engine,
		Changes: []*apitypes.SchemaChangeResponse{
			{Namespace: "default", TableChanges: tables},
		},
	}
}

func TestPlanFingerprint_IdenticalPlans(t *testing.T) {
	plan1 := planWithTables(
		&apitypes.TableChangeResponse{
			DDL:        "CREATE TABLE users (id BIGINT PRIMARY KEY)",
			ChangeType: "CREATE",
			TableName:  "users",
		},
	)

	plan2 := planWithTables(
		&apitypes.TableChangeResponse{
			DDL:        "CREATE TABLE users (id BIGINT PRIMARY KEY)",
			ChangeType: "CREATE",
			TableName:  "users",
		},
	)

	fp1 := planFingerprint(plan1)
	fp2 := planFingerprint(plan2)

	assert.Equal(t, fp2, fp1, "Expected identical fingerprints for identical plans")
}

func TestPlanFingerprint_DifferentPlans(t *testing.T) {
	plan1 := planWithTables(
		&apitypes.TableChangeResponse{
			DDL:        "CREATE TABLE users (id BIGINT PRIMARY KEY)",
			ChangeType: "CREATE",
			TableName:  "users",
		},
	)

	plan2 := planWithTables(
		&apitypes.TableChangeResponse{
			DDL:        "ALTER TABLE users ADD COLUMN name VARCHAR(255)",
			ChangeType: "ALTER",
			TableName:  "users",
		},
	)

	fp1 := planFingerprint(plan1)
	fp2 := planFingerprint(plan2)

	assert.NotEqual(t, fp1, fp2, "Expected different fingerprints for different plans")
}

func TestPlanFingerprint_NoChanges(t *testing.T) {
	plan := &apitypes.PlanResponse{}

	fp := planFingerprint(plan)
	assert.Equal(t, "no-changes", fp, "Expected 'no-changes' fingerprint for empty plan")
}

func TestPlanFingerprint_Errors(t *testing.T) {
	plan := &apitypes.PlanResponse{
		Errors: []string{"syntax error in schema"},
	}

	fp := planFingerprint(plan)
	assert.True(t, strings.HasPrefix(fp, "errors:"), "Expected fingerprint to start with 'errors:', got %q", fp)
}

func TestOutputMultiEnvPlanResult_IdenticalPlans(t *testing.T) {
	// Both staging and production have the same plan
	results := map[string]*apitypes.PlanResponse{
		"staging": planWithTablesAndEngine("mysql",
			&apitypes.TableChangeResponse{
				DDL:        "CREATE TABLE users (id BIGINT PRIMARY KEY)",
				ChangeType: "CREATE",
				TableName:  "users",
			},
		),
		"production": planWithTablesAndEngine("mysql",
			&apitypes.TableChangeResponse{
				DDL:        "CREATE TABLE users (id BIGINT PRIMARY KEY)",
				ChangeType: "CREATE",
				TableName:  "users",
			},
		),
	}

	output := captureStdout(func() {
		outputMultiEnvPlanResult(results, "testapp", "testapp")
	})

	// Should NOT show separate environment headers for identical plans
	assert.NotContains(t, output, "Staging\n", "Expected no separate 'Staging' header for identical plans")
	assert.NotContains(t, output, "Production\n", "Expected no separate 'Production' header for identical plans")
}

func TestOutputMultiEnvPlanResult_DifferentPlans(t *testing.T) {
	// Staging has CREATE, production has ALTER (different plans)
	results := map[string]*apitypes.PlanResponse{
		"staging": planWithTablesAndEngine("mysql",
			&apitypes.TableChangeResponse{
				DDL:        "CREATE TABLE users (id BIGINT PRIMARY KEY)",
				ChangeType: "CREATE",
				TableName:  "users",
			},
		),
		"production": planWithTablesAndEngine("mysql",
			&apitypes.TableChangeResponse{
				DDL:        "ALTER TABLE users ADD COLUMN name VARCHAR(255)",
				ChangeType: "ALTER",
				TableName:  "users",
			},
		),
	}

	output := captureStdout(func() {
		outputMultiEnvPlanResult(results, "testapp", "testapp")
	})

	// Different plans get separate environment headers
	// Should show separate Staging and Production sections
	// Note: Headers may have ANSI codes, so we don't check for trailing newline
	assert.Contains(t, output, "Staging", "Expected 'Staging' header for different plans")
	assert.Contains(t, output, "Production", "Expected 'Production' header for different plans")

	// Verify the correct DDL appears in each section
	// Strip ANSI codes since the output is colorized
	plainOutput := stripAnsi(output)
	assert.Contains(t, plainOutput, "CREATE TABLE", "Expected CREATE TABLE in output")
	assert.Contains(t, plainOutput, "ALTER TABLE", "Expected ALTER TABLE in output")
}

func TestOutputMultiEnvPlanResult_OneEnvNoChanges(t *testing.T) {
	// Staging has changes, production has no changes
	results := map[string]*apitypes.PlanResponse{
		"staging": planWithTablesAndEngine("mysql",
			&apitypes.TableChangeResponse{
				DDL:        "CREATE TABLE users (id BIGINT PRIMARY KEY)",
				ChangeType: "CREATE",
				TableName:  "users",
			},
		),
		"production": {
			Engine: "mysql",
		},
	}

	output := captureStdout(func() {
		outputMultiEnvPlanResult(results, "testapp", "testapp")
	})

	// Only staging has changes — separate sections shown
	// Should show separate sections
	// Note: Headers may have ANSI codes, so we don't check for trailing newline
	assert.Contains(t, output, "Staging", "Expected 'Staging' header")
	assert.Contains(t, output, "Production", "Expected 'Production' header")

	// Production should show no changes message
	assert.Contains(t, output, "No schema changes detected", "Expected 'No schema changes detected' for production")
}

func TestOutputMultiEnvPlanResult_BothNoChanges(t *testing.T) {
	// Both environments have no changes
	results := map[string]*apitypes.PlanResponse{
		"staging": {
			Engine: "mysql",
		},
		"production": {
			Engine: "mysql",
		},
	}

	output := captureStdout(func() {
		outputMultiEnvPlanResult(results, "testapp", "testapp")
	})

	// Both have no changes, so they're treated as "different" (no-changes fingerprint)
	// The implementation shows separate sections in this case
	// Count how many times we see the no-changes message
	noChangesCount := strings.Count(output, "No schema changes detected")
	assert.Equal(t, 2, noChangesCount, "Expected 2 'No schema changes detected' messages")
}

func TestSortEnvironments(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "staging and production",
			input:    []string{"production", "staging"},
			expected: []string{"staging", "production"},
		},
		{
			name:     "with other environments",
			input:    []string{"production", "dev", "staging"},
			expected: []string{"staging", "production", "dev"},
		},
		{
			name:     "alphabetical when equal priority",
			input:    []string{"gamma", "alpha", "beta"},
			expected: []string{"alpha", "beta", "gamma"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := make([]string, len(tt.input))
			copy(input, tt.input)
			sortEnvironments(input)

			for i, exp := range tt.expected {
				assert.Equal(t, exp, input[i], "index %d: expected %q, got %q (full: %v)", i, exp, input[i], input)
			}
		})
	}
}

func TestWritePlanHeader(t *testing.T) {
	output := captureStdout(func() {
		templates.WritePlanHeader(templates.PlanHeaderData{
			Database:   "mydb",
			SchemaName: "myschema",
			IsMySQL:    true,
		})
	})

	// Check for required elements
	assert.Contains(t, output, "MySQL Schema Change Plan", "Expected 'MySQL Schema Change Plan' in header")
	assert.Contains(t, output, "Database: mydb", "Expected 'Database: mydb' in header")
	assert.Contains(t, output, "Schema name: myschema", "Expected 'Schema name: myschema' in header")
	// Check for box characters
	assert.Contains(t, output, "╭", "Expected opening box-drawing character in header")
	assert.Contains(t, output, "╯", "Expected closing box-drawing character in header")
}

func TestWritePlanHeader_Vitess(t *testing.T) {
	output := captureStdout(func() {
		templates.WritePlanHeader(templates.PlanHeaderData{
			Database:   "mydb",
			SchemaName: "myschema",
			IsMySQL:    false,
		})
	})

	assert.Contains(t, output, "Vitess Schema Change Plan", "Expected 'Vitess Schema Change Plan' in header")
}

func TestWritePlanHeader_Apply(t *testing.T) {
	output := captureStdout(func() {
		templates.WritePlanHeader(templates.PlanHeaderData{
			Database:   "mydb",
			SchemaName: "myschema",
			IsMySQL:    true,
			IsApply:    true,
		})
	})

	assert.Contains(t, output, "MySQL Schema Change Apply", "Expected 'MySQL Schema Change Apply' in header")
}

func TestWriteSQLChanges(t *testing.T) {
	changes := []templates.DDLChange{
		{ChangeType: "CREATE", TableName: "users", DDL: "CREATE TABLE users (id BIGINT PRIMARY KEY)"},
		{ChangeType: "ALTER", TableName: "orders", DDL: "ALTER TABLE orders ADD COLUMN total INT"},
		{ChangeType: "DROP", TableName: "legacy", DDL: "DROP TABLE legacy"},
	}

	output := captureStdout(func() {
		templates.WriteSQLChanges(changes)
	})

	// Check table names with symbols on their own line, DDL indented below
	plainOutput := stripAnsi(output)
	assert.Contains(t, plainOutput, "+ users", "Expected '+ users' (create symbol with table name)")
	assert.Contains(t, plainOutput, "~ orders", "Expected '~ orders' (alter symbol with table name)")
	assert.Contains(t, plainOutput, "- legacy", "Expected '- legacy' (drop symbol with table name)")
	assert.Contains(t, plainOutput, "CREATE TABLE", "Expected CREATE TABLE DDL")
	assert.Contains(t, plainOutput, "ALTER TABLE", "Expected ALTER TABLE DDL")
	assert.Contains(t, plainOutput, "DROP TABLE", "Expected DROP TABLE DDL")
}

func TestWritePlanSummary(t *testing.T) {
	tests := []struct {
		name     string
		changes  []templates.DDLChange
		expected string
	}{
		{
			name: "single create",
			changes: []templates.DDLChange{
				{ChangeType: "CREATE"},
			},
			expected: "1 table to create",
		},
		{
			name: "multiple creates",
			changes: []templates.DDLChange{
				{ChangeType: "CREATE"},
				{ChangeType: "CREATE"},
			},
			expected: "2 tables to create",
		},
		{
			name: "single alter",
			changes: []templates.DDLChange{
				{ChangeType: "ALTER"},
			},
			expected: "1 table to alter",
		},
		{
			name: "multiple alters",
			changes: []templates.DDLChange{
				{ChangeType: "ALTER"},
				{ChangeType: "ALTER"},
			},
			expected: "2 tables to alter",
		},
		{
			name: "single drop",
			changes: []templates.DDLChange{
				{ChangeType: "DROP"},
			},
			expected: "1 table to drop",
		},
		{
			name: "multiple drops",
			changes: []templates.DDLChange{
				{ChangeType: "DROP"},
				{ChangeType: "DROP"},
				{ChangeType: "DROP"},
			},
			expected: "3 tables to drop",
		},
		{
			name: "mixed changes",
			changes: []templates.DDLChange{
				{ChangeType: "CREATE"},
				{ChangeType: "ALTER"},
				{ChangeType: "DROP"},
			},
			expected: "1 table to create, 1 table to alter, 1 table to drop",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := captureStdout(func() {
				templates.WritePlanSummary(tt.changes)
			})

			assert.Contains(t, output, tt.expected, "Expected summary to contain %q", tt.expected)
		})
	}
}

func TestWriteNoChanges(t *testing.T) {
	output := captureStdout(func() {
		templates.WriteNoChanges()
	})

	assert.Contains(t, output, "✓", "Expected checkmark in no-changes output")
	assert.Contains(t, output, "No schema changes detected", "Expected 'No schema changes detected'")
}

func TestFormatTimeAgo(t *testing.T) {
	tests := []struct {
		name     string
		offset   time.Duration
		expected string
	}{
		{"just now", 30 * time.Second, "just now"},
		{"1 minute", 1 * time.Minute, "1 minute ago"},
		{"5 minutes", 5 * time.Minute, "5 minutes ago"},
		{"1 hour", 1 * time.Hour, "1 hour ago"},
		{"3 hours", 3 * time.Hour, "3 hours ago"},
		{"1 day", 25 * time.Hour, "1 day ago"},
		{"3 days", 73 * time.Hour, "3 days ago"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a time in the past
			pastTime := time.Now().Add(-tt.offset)
			result := ui.FormatTimeAgo(pastTime)
			assert.Equal(t, tt.expected, result, "FormatTimeAgo()")
		})
	}
}

func TestFormatHumanDuration(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		expected string
	}{
		{"sub-second", 500 * time.Millisecond, "< 1s"},
		{"5 seconds", 5 * time.Second, "5s"},
		{"30 seconds", 30 * time.Second, "30s"},
		{"1 minute exact", 1 * time.Minute, "1m"},
		{"1m 30s", 90 * time.Second, "1m 30s"},
		{"5 minutes exact", 5 * time.Minute, "5m"},
		{"5m 30s", 5*time.Minute + 30*time.Second, "5m 30s"},
		{"1 hour exact", 1 * time.Hour, "1h"},
		{"1h 30m", 90 * time.Minute, "1h 30m"},
		{"2h 15m", 2*time.Hour + 15*time.Minute, "2h 15m"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ui.FormatHumanDuration(tt.duration)
			assert.Equal(t, tt.expected, result, "FormatHumanDuration(%v)", tt.duration)
		})
	}
}

func TestWriteMultiTablePlanOutput(t *testing.T) {
	// Test that multi-table schema changes are displayed clearly
	output := captureStdout(func() {
		// Header
		templates.WritePlanHeader(templates.PlanHeaderData{
			Database:    "testapp",
			SchemaName:  "testapp",
			Environment: "staging",
			IsMySQL:     true,
			IsApply:     false,
		})

		// Multiple changes: create users, create orders, alter products
		changes := []templates.DDLChange{
			{ChangeType: "CREATE", TableName: "users", DDL: "CREATE TABLE `users` (`id` bigint NOT NULL AUTO_INCREMENT, `email` varchar(255) NOT NULL, PRIMARY KEY (`id`))"},
			{ChangeType: "CREATE", TableName: "orders", DDL: "CREATE TABLE `orders` (`id` bigint NOT NULL AUTO_INCREMENT, `user_id` bigint NOT NULL, PRIMARY KEY (`id`))"},
			{ChangeType: "ALTER", TableName: "products", DDL: "ALTER TABLE `products` ADD INDEX `idx_category` (`category`)"},
		}

		templates.WriteSQLChanges(changes)
		templates.WritePlanSummary(changes)
	})

	// Verify header is present
	assert.Contains(t, output, "MySQL Schema Change Plan", "Expected 'MySQL Schema Change Plan' header")

	// Verify database info
	assert.Contains(t, output, "Database: testapp", "Expected 'Database: testapp'")

	// Verify each table name with symbol on its own line
	plainOutput := stripAnsi(output)
	assert.Contains(t, plainOutput, "+ users", "Expected '+ users' (create symbol)")
	assert.Contains(t, plainOutput, "+ orders", "Expected '+ orders' (create symbol)")
	assert.Contains(t, plainOutput, "~ products", "Expected '~ products' (alter symbol)")

	// Verify summary shows correct counts with proper pluralization
	assert.Contains(t, output, "2 tables to create", "Expected '2 tables to create' (plural)")
	assert.Contains(t, output, "1 table to alter", "Expected '1 table to alter' (singular)")

	// Verify changes are visually separated (blank lines between)
	// For multi-line DDL statements, blank lines appear after the closing )
	// Just check that there are double newlines in the output (blank lines exist)
	assert.Contains(t, plainOutput, ");\n\n", "Expected blank lines between DDL statements for readability")
}

func TestWriteTimeInfo(t *testing.T) {
	t.Run("empty started_at", func(t *testing.T) {
		output := captureStdout(func() {
			templates.WriteTimeInfo("", "", "")
		})
		assert.Empty(t, output, "Expected empty output for empty started_at")
	})

	t.Run("running schema change", func(t *testing.T) {
		startedAt := time.Now().Add(-5 * time.Minute).Format(time.RFC3339)
		output := captureStdout(func() {
			templates.WriteTimeInfo(startedAt, "", state.Apply.Running)
		})
		assert.Contains(t, output, "Running for", "Expected 'Running for' in output")
	})

	t.Run("completed schema change", func(t *testing.T) {
		startedAt := time.Now().Add(-10 * time.Minute).Format(time.RFC3339)
		completedAt := time.Now().Add(-1 * time.Minute).Format(time.RFC3339)
		output := captureStdout(func() {
			templates.WriteTimeInfo(startedAt, completedAt, state.Apply.Completed)
		})
		assert.Contains(t, output, "Completed in", "Expected 'Completed in' in output")
	})

	t.Run("failed schema change", func(t *testing.T) {
		startedAt := time.Now().Add(-10 * time.Minute).Format(time.RFC3339)
		completedAt := time.Now().Add(-1 * time.Minute).Format(time.RFC3339)
		output := captureStdout(func() {
			templates.WriteTimeInfo(startedAt, completedAt, state.Apply.Failed)
		})
		assert.Contains(t, output, "Failed after", "Expected 'Failed after' in output")
	})
}
