package commands

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/schema"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/stretchr/testify/require"
)

func TestPrepareInitFirstChangeRechecksLiveSchema(t *testing.T) {
	for _, populated := range []bool{false, true} {
		t.Run(map[bool]string{false: "still empty", true: "table appeared"}[populated], func(t *testing.T) {
			root := t.TempDir()
			require.NoError(t, os.Mkdir(filepath.Join(root, "shop"), 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(root, "shop", "schema.sql"), []byte(schema.EmptyNamespaceDeclaration), 0o644))
			require.NoError(t, os.WriteFile(filepath.Join(root, "shop", "vschema.json"), []byte(`{}`), 0o644))
			snapshot, err := initSchemaSnapshot(root)
			require.NoError(t, err)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/api/pull", r.URL.Path)
				var request apitypes.PullSchemaRequest
				require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
				require.Equal(t, "shop", request.Database)
				require.Equal(t, "production", request.Environment)
				require.Empty(t, request.Namespaces, "inspect the whole database, not just imported namespaces")
				ns := &apitypes.PulledNamespace{Artifacts: map[string]string{"vschema.json": `{}`}}
				if populated {
					ns.Tables = map[string]string{"orders": "CREATE TABLE orders (id bigint)"}
				}
				require.NoError(t, json.NewEncoder(w).Encode(apitypes.PullSchemaResponse{Namespaces: map[string]*apitypes.PulledNamespace{"shop": ns}}))
			}))
			defer server.Close()
			result := &initResult{SchemaDir: root, Database: "shop", Environment: "production"}
			_, err = prepareInitFirstChange(t.Context(), server.URL, result, "shop", snapshot)
			if populated {
				require.ErrorContains(t, err, "database changed")
				require.NoFileExists(t, filepath.Join(root, "shop", "schemabot_example.sql"))
			} else {
				require.NoError(t, err)
				require.FileExists(t, filepath.Join(root, "shop", "schemabot_example.sql"))
			}
		})
	}
}

func TestInitFirstChangeRequiresOptIn(t *testing.T) {
	for _, key := range []tea.KeyType{tea.KeyEnter, tea.KeyEsc, tea.KeyCtrlC} {
		m := &initFirstChangeMenu{}
		_, quit := m.Update(tea.KeyMsg{Type: key})
		require.NotNil(t, quit)
		require.False(t, m.accepted)
	}
	m := &initFirstChangeMenu{}
	m.Update(tea.KeyMsg{Type: tea.KeyDown})
	m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	require.True(t, m.accepted)
}

func TestInitExampleOnlyForEmptyUnshardedKeyspace(t *testing.T) {
	for _, tc := range []struct {
		name  string
		files map[string]string
		want  string
	}{
		{"empty", map[string]string{"shop/schema.sql": schema.EmptyNamespaceDeclaration, "shop/vschema.json": `{}`}, "shop"},
		{"existing table", map[string]string{"shop/schema.sql": initExampleDDL, "shop/vschema.json": `{}`}, ""},
		{"multiple keyspaces", map[string]string{"shop/schema.sql": schema.EmptyNamespaceDeclaration, "other/schema.sql": schema.EmptyNamespaceDeclaration, "shop/vschema.json": `{}`}, ""},
		{"sharded", map[string]string{"shop/schema.sql": schema.EmptyNamespaceDeclaration, "shop/vschema.json": `{"sharded":true}`}, ""},
		{"missing routing", map[string]string{"shop/schema.sql": schema.EmptyNamespaceDeclaration}, ""},
		{"invalid routing", map[string]string{"shop/schema.sql": schema.EmptyNamespaceDeclaration, "shop/vschema.json": `null`}, ""},
		{"template", map[string]string{"shop_{env}/schema.sql": schema.EmptyNamespaceDeclaration, "shop_{env}/vschema.json": `{}`}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.want, initExampleNamespace(tc.files)) })
	}
}

func TestWriteInitExamplePreservesEdits(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(root, "shop"), 0o755))
	placeholder := filepath.Join(root, "shop", "schema.sql")
	require.NoError(t, os.WriteFile(placeholder, []byte(schema.EmptyNamespaceDeclaration), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "shop", "vschema.json"), []byte(`{}`), 0o644))
	before, err := initSchemaSnapshot(root)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(placeholder, []byte("-- my edits"), 0o644))
	_, err = writeInitExample(root, "shop", before)
	require.ErrorContains(t, err, "schema files changed")
	require.NoFileExists(t, filepath.Join(root, "shop", "schemabot_example.sql"))
	require.NoError(t, os.WriteFile(placeholder, []byte(schema.EmptyNamespaceDeclaration), 0o644))
	path, err := writeInitExample(root, "shop", before)
	require.NoError(t, err)
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	typ, name, err := ddl.ClassifyStatement(string(content))
	require.NoError(t, err)
	require.Equal(t, ddl.StatementCreateTable, typ)
	require.Equal(t, "schemabot_example", name)
	_, err = writeInitExample(root, "shop", before)
	require.Error(t, err)
	unchanged, err := os.ReadFile(placeholder)
	require.NoError(t, err)
	require.Equal(t, schema.EmptyNamespaceDeclaration, string(unchanged))
}

func TestInitFirstChangeDoesNotPromptForAutomationOrExistingSchema(t *testing.T) {
	for _, cmd := range []InitCmd{
		{Type: "vitess"},
		{Type: "vitess", interactive: true, JSON: true},
		{Type: "vitess", interactive: true, NonInteractive: true},
		{Type: "vitess", interactive: true, ReuseSchema: true},
		{Type: "mysql", interactive: true},
	} {
		require.NoError(t, cmd.offerFirstChange(t.Context(), &Globals{}, &initResult{Verified: true, SchemaDir: "does-not-exist"}))
	}
	cmd := InitCmd{Type: "vitess", interactive: true}
	require.NoError(t, cmd.offerFirstChange(t.Context(), &Globals{}, &initResult{Verified: true, Tables: 1}))
	require.NoError(t, cmd.offerFirstChange(t.Context(), &Globals{}, &initResult{}))
}
