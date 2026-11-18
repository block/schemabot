package github

import (
	"encoding/base64"
	"fmt"
	"log/slog"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChangedPRSchemasUsesLogicalNamespaces(t *testing.T) {
	cfg := DiscoveredConfig{
		Config: &SchemabotConfig{Database: "orders", IgnoreNamespaces: []string{"ignored_{env}"}},
		Path:   "schema/schemabot.yaml",
	}
	matches := make(map[PRSchema]struct{})
	changed := map[string]bool{"shared/orders.sql": true, "schema/staging/old/items.sql": true}
	addChangedPRSchemas(matches, cfg, "staging", "schema/staging", "", []schemaFileLocator{
		{fetchPath: "shared/orders.sql", displayPath: "schema/staging/orders_{env}/orders.sql"},
		{fetchPath: "shared/orders.sql", displayPath: "schema/staging/returns_{env}/orders.sql"},
		{fetchPath: "shared/orders.sql", displayPath: "schema/staging/ignored_{env}/orders.sql"},
		{fetchPath: "schema/staging/old/items.sql", displayPath: "schema/staging/old/items.sql"},
	}, changed)
	addChangedPRSchemas(matches, cfg, "production", "schema/production", "", []schemaFileLocator{
		{fetchPath: "shared/orders.sql", displayPath: "schema/production/orders_$ENV/orders.sql"},
	}, changed)
	require.Equal(t, map[PRSchema]struct{}{
		{Database: "orders", Namespace: "orders_{env}"}:  {},
		{Database: "orders", Namespace: "returns_{env}"}: {},
		{Database: "orders", Namespace: "old"}:           {},
	}, matches)
}

func TestChangedPRSchemasCountsFlatEnvironmentRootsOnce(t *testing.T) {
	cfg := DiscoveredConfig{Config: &SchemabotConfig{Database: "payments"}, Path: "schema/schemabot.yaml"}
	matches := make(map[PRSchema]struct{})
	changed := map[string]bool{cfg.Path: true}
	addChangedPRSchemas(matches, cfg, "staging", "schema/staging", "", []schemaFileLocator{
		{fetchPath: "schema/staging/charges.sql", displayPath: "schema/staging/charges.sql"},
		{fetchPath: "schema/staging/refunds.sql", displayPath: "schema/staging/refunds.sql"},
	}, changed)
	addChangedPRSchemas(matches, cfg, "production", "schema/production", "", []schemaFileLocator{
		{fetchPath: "schema/production/charges.sql", displayPath: "schema/production/charges.sql"},
	}, changed)
	require.Equal(t, map[PRSchema]struct{}{{Database: "payments"}: {}}, matches)
}

// One shared input feeds three namespaces on each control plane. Its deletion
// still resolves the consumers from the base tree without reading its SQL blob.
func TestPRSchemaDiscoveryFindsSharedConsumersOfDeletedInput(t *testing.T) {
	github, mux := setupConfigTestGitHubServer(t)
	const repo = "test/repo"

	// Serve both the recursive config inventory and the shallow path lookups.
	trees := map[string]string{
		"head":        `[{"path":"db","sha":"db-tree","type":"tree"},{"path":"shared","sha":"empty","type":"tree"}]`,
		"base":        `[{"path":"db","sha":"db-tree","type":"tree"},{"path":"shared","sha":"shared-tree","type":"tree"}]`,
		"db-tree":     `[{"path":"first","sha":"schemas","type":"tree"},{"path":"second","sha":"schemas","type":"tree"}]`,
		"schemas":     `[{"path":"schemabot.yaml","sha":"config","type":"blob"},{"path":"a","sha":"link","type":"blob","mode":"120000"},{"path":"b","sha":"link","type":"blob","mode":"120000"},{"path":"c","sha":"link","type":"blob","mode":"120000"}]`,
		"shared-tree": `[{"path":"orders.sql","sha":"sql","type":"blob","mode":"100644"}]`,
		"empty":       `[]`,
	}
	mux.HandleFunc("/repos/"+repo+"/git/trees/{ref}", func(w http.ResponseWriter, r *http.Request) {
		ref := r.PathValue("ref")
		entries := trees[ref]
		if r.URL.Query().Get("recursive") == "1" {
			entries = `[{"path":"db/first/schemabot.yaml","sha":"config-first","type":"blob"},{"path":"db/second/schemabot.yaml","sha":"config-second","type":"blob"}]`
		}
		if entries == "" {
			t.Errorf("unexpected tree request for %s", ref)
			http.NotFound(w, r)
			return
		}
		_, err := fmt.Fprintf(w, `{"sha":%q,"tree":%s}`, ref, entries)
		assert.NoError(t, err)
	})
	for _, database := range []string{"first", "second"} {
		mux.HandleFunc("/repos/"+repo+"/contents/db/"+database+"/schemabot.yaml", func(w http.ResponseWriter, _ *http.Request) {
			content := base64.StdEncoding.EncodeToString([]byte("database: " + database + "\ntype: mysql\n"))
			_, err := fmt.Fprintf(w, `{"type":"file","encoding":"base64","content":%q}`, content)
			assert.NoError(t, err)
		})
	}
	mux.HandleFunc("/repos/"+repo+"/git/blobs/{sha}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("sha") != "link" {
			t.Errorf("discovery fetched schema blob %s", r.PathValue("sha"))
			http.NotFound(w, r)
			return
		}
		_, err := fmt.Fprint(w, `{"content":"../../shared","encoding":"utf-8"}`)
		assert.NoError(t, err)
	})

	client := NewInstallationClient(github, slog.Default())
	schemas, configs, err := client.DiscoverPRSchemas(t.Context(), repo, []string{"head", "base"}, nil,
		[]PRFile{{Filename: "shared/orders.sql", Status: "removed"}},
		func(DiscoveredConfig) []string { return []string{"staging", "production"} })
	require.NoError(t, err)
	require.Equal(t, []PRSchema{
		{Database: "first", Namespace: "a"}, {Database: "first", Namespace: "b"}, {Database: "first", Namespace: "c"},
		{Database: "second", Namespace: "a"}, {Database: "second", Namespace: "b"}, {Database: "second", Namespace: "c"},
	}, schemas)
	require.Len(t, configs, 2)
	require.Equal(t, "first", configs[0].Config.Database)
	require.Equal(t, "second", configs[1].Config.Database)
}
