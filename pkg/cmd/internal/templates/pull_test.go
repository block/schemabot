package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ui"
)

// setColors pins the interactive-terminal color detection for the test, so
// assertions on styled or plain output don't depend on where the test runs.
func setColors(t *testing.T, on bool) {
	t.Helper()
	prev := ui.Colors
	ui.Colors = on
	t.Cleanup(func() { ui.Colors = prev })
}

// The pretty pull rendering is a summary box followed by the schema: tables
// print in name order as valid SQL with a ";" terminator whether or not the
// engine included one, and non-table artifacts follow under a comment header
// with their body printed raw so it can be copy-pasted from the terminal.
// JSON artifact bodies (often stored compactly) are re-indented for reading;
// non-JSON artifacts pass through unchanged.
func TestWritePullSchema_RendersSchemaAsExecutableSQL(t *testing.T) {
	setColors(t, false)
	out := captureStdout(t, func() {
		WritePullSchema(&apitypes.PullSchemaResponse{
			Database:    "orders-db",
			Type:        "mysql",
			Environment: "staging",
			TableCount:  2,
			Namespaces: map[string]*apitypes.PulledNamespace{
				"orders": {
					Tables: map[string]string{
						"users":  "CREATE TABLE `users` (`id` bigint NOT NULL);\n",
						"orders": "CREATE TABLE `orders` (`order_id` bigint NOT NULL)\n",
					},
					Artifacts: map[string]string{
						"vschema.json": "{\"sharded\": false}\n",
						"README.txt":   "usage notes\r\nsecond line\n",
					},
				},
			},
		})
	})

	assert.Contains(t, out, "Database:")
	assert.Contains(t, out, "orders-db")
	assert.Contains(t, out, "Environment:")
	assert.Contains(t, out, "staging")
	assert.Contains(t, out, "Tables:")

	assert.Contains(t, out, "-- Namespace `orders` — 2 tables\n")
	assert.Contains(t, out, "CREATE TABLE `orders` (`order_id` bigint NOT NULL);\n")
	assert.Contains(t, out, "CREATE TABLE `users` (`id` bigint NOT NULL);\n")
	assert.NotContains(t, out, ");;", "an existing terminator must not be doubled")
	assert.Less(t, strings.Index(out, "CREATE TABLE `orders`"), strings.Index(out, "CREATE TABLE `users`"),
		"tables render in name order")

	assert.Contains(t, out, "-- Artifact `vschema.json`\n")
	assert.Contains(t, out, "{\n  \"sharded\": false\n}\n",
		"a compact JSON artifact re-indents and prints raw for copy-paste")
	assert.Contains(t, out, "-- Artifact `README.txt`\n")
	assert.Contains(t, out, "usage notes\nsecond line\n",
		"a non-JSON artifact passes through unchanged, with CRLF normalized")
}

// A pull that asked for linting renders the audit as SQL comments under the
// namespace header, so the output stays executable. An explicit empty audit
// reads as clean; a pull without linting shows no lint line at all.
func TestWritePullSchema_LintAuditRendersAsComments(t *testing.T) {
	setColors(t, false)
	response := func(lint []*apitypes.LintViolationResponse) *apitypes.PullSchemaResponse {
		return &apitypes.PullSchemaResponse{
			Database:    "orders-db",
			Type:        "mysql",
			Environment: "staging",
			TableCount:  1,
			Namespaces: map[string]*apitypes.PulledNamespace{
				"orders": {
					Tables: map[string]string{"users": "CREATE TABLE `users` (`id` bigint NOT NULL)\n"},
					Lint:   lint,
				},
			},
		}
	}

	violations := captureStdout(t, func() {
		WritePullSchema(response([]*apitypes.LintViolationResponse{
			{Table: "users", Severity: "warning", Message: `Column "created_at" uses TIMESTAMP which overflows on 2038-01-19. Consider using DATETIME instead.`},
			{Table: "users", Severity: "error", Message: "Primary key column \"id\" has type \"int\"\nUse BIGINT UNSIGNED instead."},
		}))
	})
	assert.Contains(t, violations, "-- Lint: 2 violations\n")
	assert.Contains(t, violations, "--   [warning] users: Column \"created_at\" uses TIMESTAMP which overflows on 2038-01-19. Consider using DATETIME instead.\n")
	assert.Contains(t, violations, "--   [error] users: Primary key column \"id\" has type \"int\"\n--   Use BIGINT UNSIGNED instead.\n",
		"every line of a multi-line message is a SQL comment")

	clean := captureStdout(t, func() {
		WritePullSchema(response([]*apitypes.LintViolationResponse{}))
	})
	assert.Contains(t, clean, "-- Lint: no violations\n")

	unaudited := captureStdout(t, func() {
		WritePullSchema(response(nil))
	})
	assert.NotContains(t, unaudited, "-- Lint")
}

// A multi-namespace pull renders each namespace in name order and surfaces
// the namespace count in the summary box.
func TestWritePullSchema_MultipleNamespacesRenderInOrder(t *testing.T) {
	setColors(t, false)
	out := captureStdout(t, func() {
		WritePullSchema(&apitypes.PullSchemaResponse{
			Database:    "orders-db",
			Type:        "vitess",
			Environment: "production",
			TableCount:  2,
			Namespaces: map[string]*apitypes.PulledNamespace{
				"shipping": {Tables: map[string]string{"shipments": "CREATE TABLE `shipments` (`id` bigint NOT NULL)\n"}},
				"billing":  {Tables: map[string]string{"invoices": "CREATE TABLE `invoices` (`id` bigint NOT NULL)\n"}},
			},
		})
	})

	assert.Contains(t, out, "Namespaces:")
	assert.Contains(t, out, "-- Namespace `billing` — 1 table\n")
	assert.Contains(t, out, "-- Namespace `shipping` — 1 table\n")
	assert.Less(t, strings.Index(out, "-- Namespace `billing`"), strings.Index(out, "-- Namespace `shipping`"),
		"namespaces render in name order")
}

// On an interactive terminal the rendering is styled: annotation lines dim
// with the names in bold, DDL syntax-highlighted, lint severities colored,
// and JSON artifacts colored jq-style. Piped output carries no escapes at
// all, so redirecting to a file stays byte-clean.
func TestWritePullSchema_StylesOutputOnInteractiveTerminals(t *testing.T) {
	response := &apitypes.PullSchemaResponse{
		Database:    "orders-db",
		Type:        "vitess",
		Environment: "production",
		TableCount:  1,
		Namespaces: map[string]*apitypes.PulledNamespace{
			"commerce": {
				Tables:    map[string]string{"users": "CREATE TABLE `users` (`id` bigint NOT NULL)\n"},
				Artifacts: map[string]string{"vschema.json": "{\"sharded\": true}\n"},
				Lint: []*apitypes.LintViolationResponse{
					{Table: "users", Severity: "warning", Message: "message"},
					{Table: "users", Severity: "error", Message: "message"},
				},
			},
		},
	}

	setColors(t, true)
	styled := captureStdout(t, func() { WritePullSchema(response) })
	assert.Contains(t, styled, ANSIDim+"-- Namespace ", "annotation lines dim")
	assert.Contains(t, styled, ANSIBold+"`commerce`", "namespace name bold")
	assert.Contains(t, styled, ANSIBold+"`vschema.json`", "artifact name bold")
	assert.Contains(t, styled, ANSIYellow+"[warning]"+ANSIReset, "warning severity yellow")
	assert.Contains(t, styled, ANSIRed+"[error]"+ANSIReset, "error severity red")
	assert.Contains(t, styled, ANSIMagenta+"`users`"+ANSIReset, "DDL table name highlighted")
	assert.Contains(t, styled, ANSICyan+"\"sharded\""+ANSIReset, "JSON key cyan")

	setColors(t, false)
	plain := captureStdout(t, func() { WritePullSchema(response) })
	assert.NotContains(t, plain, "\033[", "piped output carries no ANSI escapes")
	assert.Contains(t, plain, "{\n  \"sharded\": true\n}\n")
}
