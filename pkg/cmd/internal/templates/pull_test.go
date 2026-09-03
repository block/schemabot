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
			{Table: "users", Severity: "warning", Message: `Column "created_at" uses "TIMESTAMP" which overflows on 2038-01-19. Consider using "DATETIME" instead.`},
			{Table: "users", Severity: "error", Message: "Primary key column \"id\" has type \"int\"\nUse BIGINT UNSIGNED instead."},
		}))
	})
	assert.Contains(t, violations, "-- Lint: 2 violations\n")
	assert.Contains(t, violations, "--   [warning] users: Column \"created_at\" uses \"TIMESTAMP\" which overflows on 2038-01-19. Consider using \"DATETIME\" instead.\n")
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

// A pull that asked for a detailed catalog surfaces the part of it the DDL
// cannot carry — the engine's row-count and size estimates — as a comment
// above each table's CREATE statement, so the output stays executable SQL.
// A view has no rows or storage of its own and gets no estimate line.
func TestWritePullSchema_DetailedCatalogRendersTableEstimates(t *testing.T) {
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
						"users":        "CREATE TABLE `users` (`id` bigint NOT NULL)\n",
						"active_users": "CREATE VIEW `active_users` AS select 1\n",
					},
					NamespaceCatalog: &apitypes.NamespaceCatalog{Name: "orders", Engine: "mysql", TableCount: 2},
					TableCatalog: map[string]*apitypes.TableCatalog{
						"users":        {Name: "users", Kind: "table", EstimatedRowCount: 18402551, DataSizeBytes: 4294967296},
						"active_users": {Name: "active_users", Kind: "view"},
					},
				},
			},
		})
	})

	assert.Contains(t, out, "-- Table `users` — rows ~18,402,551, size ~4.0 GiB (engine estimates)\nCREATE TABLE `users`",
		"the estimate line sits directly above the table's DDL")
	assert.NotContains(t, out, "-- Table `active_users`", "a view carries no row or size estimate")
	for line := range strings.SplitSeq(out, "\n") {
		if strings.Contains(line, "rows ~") {
			assert.True(t, strings.HasPrefix(line, "--"), "an estimate line must be a SQL comment: %q", line)
		}
	}
}

// A basic pull carries no catalog, so the schema renders without estimate
// lines rather than with zeroed ones.
func TestWritePullSchema_BasicPullRendersNoEstimates(t *testing.T) {
	setColors(t, false)
	out := captureStdout(t, func() {
		WritePullSchema(&apitypes.PullSchemaResponse{
			Database:    "orders-db",
			Type:        "mysql",
			Environment: "staging",
			TableCount:  1,
			Namespaces: map[string]*apitypes.PulledNamespace{
				"orders": {Tables: map[string]string{"users": "CREATE TABLE `users` (`id` bigint NOT NULL)\n"}},
			},
		})
	})

	assert.Contains(t, out, "CREATE TABLE `users`")
	assert.NotContains(t, out, "rows ~")
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

// A pull of an environment whose targets each hold their own schema reports how
// every other target differs from the primary, whose schema is the DDL printed
// below. A converged target says so rather than being omitted, so "they agree"
// is distinguishable from "not checked". The whole section renders as "--"
// comments, so a redirected pull stays valid SQL.
func TestWritePullSchema_RendersPerTargetDivergence(t *testing.T) {
	setColors(t, false)
	out := captureStdout(t, func() {
		WritePullSchema(&apitypes.PullSchemaResponse{
			Database:    "orders-db",
			Type:        "mysql",
			Environment: "production",
			TableCount:  1,
			Namespaces: map[string]*apitypes.PulledNamespace{
				"orders": {Tables: map[string]string{"users": "CREATE TABLE `users` (`id` bigint NOT NULL);\n"}},
			},
			Targets: []*apitypes.TargetDivergence{
				{Deployment: "eu", Target: "orders-002", TableCount: 2, DivergedTables: []apitypes.DivergedTable{
					{Namespace: "orders", Table: "audits", Difference: apitypes.DivergenceOnlyOnTarget},
					{Namespace: "orders", Table: "users", Difference: apitypes.DivergenceDiffers},
				}},
				{Deployment: "eu", Target: "orders-003", TableCount: 1},
			},
		})
	})

	assert.Contains(t, out, "-- Target `orders-002` — 2 tables differ from the primary target")
	assert.Contains(t, out, "--   orders.audits: extra")
	assert.Contains(t, out, "--   orders.users: differs")
	assert.Contains(t, out, "-- Target `orders-003` — same schema as the primary target")
	for line := range strings.SplitSeq(out, "\n") {
		if strings.Contains(line, "orders-002") || strings.Contains(line, "orders-003") || strings.Contains(line, "orders.audits") {
			assert.True(t, strings.HasPrefix(strings.TrimSpace(line), "--"),
				"divergence line %q must be a SQL comment", line)
		}
	}
}

// An environment whose targets are expected to hold the same schema carries no
// divergence, and a pull of it renders exactly as it always did.
func TestWritePullSchema_NoDivergenceSectionWithoutTargets(t *testing.T) {
	setColors(t, false)
	out := captureStdout(t, func() {
		WritePullSchema(&apitypes.PullSchemaResponse{
			Database:    "orders-db",
			Type:        "mysql",
			Environment: "production",
			TableCount:  1,
			Namespaces: map[string]*apitypes.PulledNamespace{
				"orders": {Tables: map[string]string{"users": "CREATE TABLE `users` (`id` bigint NOT NULL);\n"}},
			},
		})
	})

	assert.NotContains(t, out, "-- Target ")
}
