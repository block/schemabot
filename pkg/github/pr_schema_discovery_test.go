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
	changed := map[string]bool{
		"schema/staging/orders_{env}/orders.sql":   true,
		"schema/production/orders_$ENV/orders.sql": true,
		"schema/staging/ignored_{env}/orders.sql":  true,
		"schema/staging/returns_{env}":             true,
		"schema/staging/old/items.sql":             true,
	}
	addChangedPRSchemas(matches, cfg, "staging", "schema/staging", false, []string{
		"schema/staging/orders_{env}/orders.sql",
		"schema/staging/returns_{env}/orders.sql",
		"schema/staging/ignored_{env}/orders.sql",
		"schema/staging/old/items.sql",
	}, changed)
	addChangedPRSchemas(matches, cfg, "production", "schema/production", false, []string{
		"schema/production/orders_$ENV/orders.sql",
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
	addChangedPRSchemas(matches, cfg, "staging", "schema/staging", true, []string{
		"schema/staging/charges.sql",
		"schema/staging/refunds.sql",
	}, changed)
	addChangedPRSchemas(matches, cfg, "production", "schema/production", true, []string{
		"schema/production/charges.sql",
	}, changed)
	require.Equal(t, map[PRSchema]struct{}{{Database: "payments"}: {}}, matches)
}

// The base still owns a deleted input. Its unchanged aliases, both in the
// same config and in another registration, are outside the changed paths.
func TestPRSchemaDiscoveryCountsOwnerOfDeletedInput(t *testing.T) {
	github, mux := setupConfigTestGitHubServer(t)
	const repo = "test/repo"
	trees := map[string]string{
		"head":    `[{"path":"owner","sha":"new","type":"tree"},{"path":"other","sha":"other","type":"tree"}]`,
		"base":    `[{"path":"owner","sha":"old","type":"tree"},{"path":"other","sha":"other","type":"tree"}]`,
		"backing": `[{"path":"nested","sha":"nested","type":"tree"}]`,
		"nested":  `[{"path":"items.sql","type":"blob","mode":"100644"}]`,
		"old":     `[{"path":"backing","sha":"backing","type":"tree"},{"path":"items.sql","type":"blob","mode":"100644"},{"path":"staging","sha":"stage","type":"tree"},{"path":"alias","sha":"link","type":"blob","mode":"120000"}]`,
		"new":     `[{"path":"staging","sha":"stage","type":"tree"},{"path":"alias","sha":"link","type":"blob","mode":"120000"}]`,
	}
	mux.HandleFunc("/repos/"+repo+"/git/trees/{ref}", func(w http.ResponseWriter, r *http.Request) {
		assert.Empty(t, r.URL.Query().Get("recursive"))
		ref := r.PathValue("ref")
		entries, ok := trees[ref]
		if !ok {
			t.Errorf("unexpected tree request for %s", ref)
			http.NotFound(w, r)
			return
		}
		_, err := fmt.Fprintf(w, `{"sha":%q,"tree":%s}`, ref, entries)
		assert.NoError(t, err)
	})
	mux.HandleFunc("/repos/"+repo+"/contents/owner/schemabot.yaml", func(w http.ResponseWriter, _ *http.Request) {
		content := base64.StdEncoding.EncodeToString([]byte("database: orders\ntype: mysql\n"))
		_, err := fmt.Fprintf(w, `{"type":"file","encoding":"base64","content":%q}`, content)
		assert.NoError(t, err)
	})
	for _, dir := range []string{"owner/backing", "owner/backing/nested"} {
		mux.HandleFunc("/repos/"+repo+"/contents/"+dir+"/schemabot.yaml", http.NotFound)
	}
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected discovery request: %s", r.URL.Path)
		http.NotFound(w, r)
	})
	client := NewInstallationClient(github, slog.Default())
	for _, tc := range []struct{ file, env string }{
		{"owner/items.sql", "production"},
		{"owner/items.sql", "staging"},
		{"owner/backing/nested/items.sql", "production"},
	} {
		schemas, err := client.DiscoverPRSchemas(t.Context(), repo, []string{"head", "base"},
			[]PRFile{{Filename: tc.file, Status: "removed"}},
			func(DiscoveredConfig) []string { return []string{tc.env} })
		require.NoError(t, err)
		require.Equal(t, []PRSchema{{Database: "orders"}}, schemas)
	}
}
