package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/apitypes"
)

// The pretty pull rendering is a summary box followed by executable SQL:
// tables print in name order with a ";" terminator whether or not the engine
// included one, and non-table artifacts follow under a comment header.
func TestWritePullSchema_RendersSchemaAsExecutableSQL(t *testing.T) {
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
	assert.Contains(t, out, "{\"sharded\": false}\n")
}

// A pull that asked for linting renders the audit as SQL comments under the
// namespace header, so the output stays executable. An explicit empty audit
// reads as clean; a pull without linting shows no lint line at all.
func TestWritePullSchema_LintAuditRendersAsComments(t *testing.T) {
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
			{Table: "users", Severity: "error", Message: `Primary key column "id" has type "int"`},
		}))
	})
	assert.Contains(t, violations, "-- Lint: 2 violations\n")
	assert.Contains(t, violations, "--   [warning] users: Column \"created_at\" uses TIMESTAMP which overflows on 2038-01-19. Consider using DATETIME instead.\n")
	assert.Contains(t, violations, "--   [error] users: Primary key column \"id\" has type \"int\"\n")

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
