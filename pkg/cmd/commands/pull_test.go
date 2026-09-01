package commands

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
)

func TestWritePullSchemaResponseReturnsSchemaAsJSON(t *testing.T) {
	var out bytes.Buffer
	require.NoError(t, writePullSchemaResponse(&out, validPullSchemaResponse()))

	var got apitypes.PullSchemaResponse
	require.NoError(t, json.Unmarshal(out.Bytes(), &got))
	assert.Equal(t, "orders", got.Database)
	assert.Equal(t, "mysql", got.Type)
	assert.Equal(t, "production", got.Environment)
	assert.Equal(t, "CREATE TABLE `users` (`id` bigint NOT NULL);\n", got.Namespaces["orders"].Tables["users"])
}

// The pull command resolves the database type from the server's registered
// config, so --type is optional (defaults to empty) and accepts any engine the
// server supports rather than being pinned to one type.
func TestPullCmdTypeIsOptionalAndAcceptsAnyEngine(t *testing.T) {
	var cli struct {
		Pull PullCmd `cmd:""`
	}
	parser, err := kong.New(&cli)
	require.NoError(t, err)

	_, err = parser.Parse([]string{"pull", "-d", "boardgames", "-e", "staging"})
	require.NoError(t, err)
	assert.Empty(t, cli.Pull.Type, "type must default to empty so the server resolves it from config")

	_, err = parser.Parse([]string{"pull", "-d", "boardgames", "-e", "staging", "--type", "vitess"})
	require.NoError(t, err, "a non-mysql type must be accepted")
	assert.Equal(t, "vitess", cli.Pull.Type)
}

// Linting on pull is opt-in: the flag defaults to off so a plain pull never
// pays the lint cost, and --lint turns it on.
func TestPullCmdLintIsOptIn(t *testing.T) {
	var cli struct {
		Pull PullCmd `cmd:""`
	}
	parser, err := kong.New(&cli)
	require.NoError(t, err)

	_, err = parser.Parse([]string{"pull", "-d", "boardgames", "-e", "staging"})
	require.NoError(t, err)
	assert.False(t, cli.Pull.Lint, "lint must default to off")

	_, err = parser.Parse([]string{"pull", "-d", "boardgames", "-e", "staging", "--lint"})
	require.NoError(t, err)
	assert.True(t, cli.Pull.Lint)
}

// Pull renders readable SQL for humans by default; -o json emits the full
// API response for scripts and tooling.
func TestPullCmdOutputDefaultsToPretty(t *testing.T) {
	var cli struct {
		Pull PullCmd `cmd:""`
	}
	parser, err := kong.New(&cli)
	require.NoError(t, err)

	_, err = parser.Parse([]string{"pull", "-d", "boardgames", "-e", "staging"})
	require.NoError(t, err)
	assert.Equal(t, "pretty", cli.Pull.Output)

	_, err = parser.Parse([]string{"pull", "-d", "boardgames", "-e", "staging", "-o", "json"})
	require.NoError(t, err)
	assert.Equal(t, "json", cli.Pull.Output)

	_, err = parser.Parse([]string{"pull", "-d", "boardgames", "-e", "staging", "-o", "yaml"})
	require.Error(t, err, "only pretty and json are valid output formats")
}

// The --table filter matches the way the databases --name filter does: a
// case-insensitive substring, so a prefix selects a whole table family and an
// exact name selects one table. Matching tables keep their catalog entries and
// lint findings, non-matching namespaces drop out entirely, namespace-scoped
// artifacts are omitted, and the table count reflects the selection.
func TestFilterPullSchemaTablesKeepsSubstringMatches(t *testing.T) {
	resp := &apitypes.PullSchemaResponse{
		Database:    "orders",
		Type:        "mysql",
		Environment: "production",
		TableCount:  3,
		Namespaces: map[string]*apitypes.PulledNamespace{
			"orders": {
				Tables: map[string]string{
					"users":         "CREATE TABLE `users` (`id` bigint NOT NULL);",
					"user_settings": "CREATE TABLE `user_settings` (`id` bigint NOT NULL);",
					"payments":      "CREATE TABLE `payments` (`id` bigint NOT NULL);",
				},
				Artifacts: map[string]string{"vschema": "{}"},
				TableCatalog: map[string]*apitypes.TableCatalog{
					"users":    {Name: "users"},
					"payments": {Name: "payments"},
				},
				Lint: []*apitypes.LintViolationResponse{
					{Table: "users", Message: "violation on users"},
					{Table: "payments", Message: "violation on payments"},
				},
			},
			"billing": {
				Tables: map[string]string{"invoices": "CREATE TABLE `invoices` (`id` bigint NOT NULL);"},
			},
		},
	}

	require.NoError(t, filterPullSchemaTables(resp, "USER"))

	require.Contains(t, resp.Namespaces, "orders")
	assert.NotContains(t, resp.Namespaces, "billing", "a namespace with no matching table must drop out")
	ns := resp.Namespaces["orders"]
	assert.Equal(t, map[string]string{
		"users":         "CREATE TABLE `users` (`id` bigint NOT NULL);",
		"user_settings": "CREATE TABLE `user_settings` (`id` bigint NOT NULL);",
	}, ns.Tables)
	assert.Nil(t, ns.Artifacts, "namespace-scoped artifacts must be omitted from a table-filtered pull")
	assert.Equal(t, map[string]*apitypes.TableCatalog{"users": {Name: "users"}}, ns.TableCatalog)
	require.Len(t, ns.Lint, 1)
	assert.Equal(t, "users", ns.Lint[0].Table)
	assert.Equal(t, int32(2), resp.TableCount)
}

// A filtered pull that requested lint keeps the explicit empty audit when the
// selected tables are clean, so "no violations" stays distinguishable from
// lint not being requested.
func TestFilterPullSchemaTablesPreservesExplicitCleanLint(t *testing.T) {
	resp := &apitypes.PullSchemaResponse{
		Database:    "orders",
		Environment: "production",
		Namespaces: map[string]*apitypes.PulledNamespace{
			"orders": {
				Tables: map[string]string{
					"users":    "CREATE TABLE `users` (`id` bigint NOT NULL);",
					"payments": "CREATE TABLE `payments` (`id` bigint NOT NULL);",
				},
				Lint: []*apitypes.LintViolationResponse{{Table: "payments", Message: "violation on payments"}},
			},
		},
	}

	require.NoError(t, filterPullSchemaTables(resp, "users"))

	ns := resp.Namespaces["orders"]
	require.NotNil(t, ns.Lint, "a requested lint audit must stay explicit after filtering")
	assert.Empty(t, ns.Lint)
}

// A filter that matches nothing is an error naming the filter, the database,
// and the environment, and lists the available tables so a typo is a
// one-round-trip fix rather than an empty-but-successful pull.
func TestFilterPullSchemaTablesErrorsWhenNothingMatches(t *testing.T) {
	resp := validPullSchemaResponse()

	err := filterPullSchemaTables(resp, "odres")

	require.Error(t, err)
	assert.Contains(t, err.Error(), `no table matching "odres" in database orders environment production`)
	assert.Contains(t, err.Error(), "available tables: users")
}
