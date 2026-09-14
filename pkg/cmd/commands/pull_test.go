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
				Artifacts:        map[string]string{"vschema": "{}"},
				NamespaceCatalog: &apitypes.NamespaceCatalog{Name: "orders", Engine: "mysql", TableCount: 3},
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
	require.NotNil(t, ns.NamespaceCatalog)
	assert.Equal(t, int32(len(ns.Tables)), ns.NamespaceCatalog.TableCount, "the namespace catalog count must match the filtered tables")
	assert.Equal(t, map[string]*apitypes.TableCatalog{"users": {Name: "users"}}, ns.TableCatalog)
	require.Len(t, ns.Lint, 1)
	assert.Equal(t, "users", ns.Lint[0].Table)
	assert.Equal(t, int32(2), resp.TableCount)
}

// A table filter narrows what the pull describes, so the divergence report has
// to be narrowed with it. Left alone it would name divergence in tables the
// operator did not ask about, beside a schema that no longer contains them, and
// each target's count would describe a different table set than the header.
func TestFilterPullSchemaTablesNarrowsTargetDivergence(t *testing.T) {
	resp := &apitypes.PullSchemaResponse{
		Database:    "orders",
		Environment: "production",
		Namespaces: map[string]*apitypes.PulledNamespace{
			"orders": {
				Tables: map[string]string{
					"users":         "CREATE TABLE `users` (`id` bigint NOT NULL);",
					"user_settings": "CREATE TABLE `user_settings` (`id` bigint NOT NULL);",
					"payments":      "CREATE TABLE `payments` (`id` bigint NOT NULL);",
				},
			},
		},
		TableCount: 3,
		Targets: []*apitypes.TargetDivergence{
			{Deployment: "eu", Target: "orders-001", TableCount: 3, Primary: true},
			{Deployment: "eu", Target: "orders-002", TableCount: 3, DivergedTables: []apitypes.DivergedTable{
				{Namespace: "orders", Table: "users", Difference: apitypes.DivergenceDiffers},
				{Namespace: "orders", Table: "user_settings", Difference: apitypes.DivergenceOnlyOnPrimary},
				{Namespace: "orders", Table: "receipts", Difference: apitypes.DivergenceOnlyOnTarget},
				{Namespace: "orders", Table: "payments", Difference: apitypes.DivergenceDiffers},
			}},
			{Deployment: "eu", Target: "orders-003", TableCount: 3, DivergedTables: []apitypes.DivergedTable{
				{Namespace: "orders", Table: "payments", Difference: apitypes.DivergenceDiffers},
			}},
		},
	}

	require.NoError(t, filterPullSchemaTables(resp, "user"))

	require.Len(t, resp.Targets, 3)
	assert.Equal(t, int32(2), resp.Targets[0].TableCount, "the primary's count is the filtered schema's")

	diverged := resp.Targets[1]
	assert.Equal(t, []apitypes.DivergedTable{
		{Namespace: "orders", Table: "users", Difference: apitypes.DivergenceDiffers},
		{Namespace: "orders", Table: "user_settings", Difference: apitypes.DivergenceOnlyOnPrimary},
	}, diverged.DivergedTables, "divergence in tables the filter excluded must not be reported")
	assert.Equal(t, int32(1), diverged.TableCount,
		"the target holds the filtered tables the primary holds, less the one only the primary has")

	converged := resp.Targets[2]
	assert.Nil(t, converged.DivergedTables,
		"a target whose only divergence was filtered out holds the same filtered schema as the primary")
	assert.Equal(t, int32(2), converged.TableCount)
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
