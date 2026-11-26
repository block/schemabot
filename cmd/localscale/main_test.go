package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/block/schemabot/pkg/localscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A keyspace directory's .sql files are read in name order, so a schema whose
// statements depend on each other can fix their order by naming. Anything that
// is not a .sql file, and any file holding only whitespace, is left out.
func TestKeyspaceDDLReadsSQLFilesInNameOrder(t *testing.T) {
	dir := t.TempDir()
	for name, content := range map[string]string{
		"20-orders.sql":    "CREATE TABLE orders (id bigint unsigned NOT NULL, PRIMARY KEY (id))\n",
		"10-customers.sql": "CREATE TABLE customers (id bigint unsigned NOT NULL, PRIMARY KEY (id))\n",
		"vschema.json":     `{"sharded": false}`,
		"notes.txt":        "not DDL",
		"empty.sql":        "   \n",
	} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0600))
	}
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "nested.sql"), 0700))

	stmts, err := keyspaceDDL(dir)
	require.NoError(t, err)
	require.Len(t, stmts, 2)
	require.Contains(t, stmts[0], "customers")
	require.Contains(t, stmts[1], "orders")

	_, err = keyspaceDDL(filepath.Join(dir, "absent"))
	require.Error(t, err)
}

// Seeding applies every keyspace's vschema before any of its tables: vtgate has
// to know how to route a keyspace's queries before anything creates tables in
// it, and a keyspace's vindexes can reference a table in another keyspace. Each
// keyspace is seeded into every database that declares it.
func TestSeedSchemaDirAppliesVSchemasBeforeTables(t *testing.T) {
	dir := t.TempDir()
	shop := filepath.Join(dir, "shop")
	require.NoError(t, os.MkdirAll(shop, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(shop, "vschema.json"), []byte(`{"sharded": true}`), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(shop, "customers.sql"), []byte("CREATE TABLE customers (id bigint unsigned NOT NULL, PRIMARY KEY (id))"), 0600))
	// A keyspace with no vschema.json is skipped by the first pass and still
	// has its tables seeded by the second.
	reports := filepath.Join(dir, "reports")
	require.NoError(t, os.MkdirAll(reports, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(reports, "daily.sql"), []byte("CREATE TABLE daily (id bigint unsigned NOT NULL, PRIMARY KEY (id))"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "README.md"), []byte("not a keyspace"), 0600))

	type seeded struct {
		Path     string `json:"-"`
		Org      string `json:"org"`
		Database string `json:"database"`
		Keyspace string `json:"keyspace"`
	}
	var calls []seeded
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		var call seeded
		require.NoError(t, json.Unmarshal(body, &call))
		call.Path = r.URL.Path
		calls = append(calls, call)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	orgs := map[string]localscale.OrgConfig{
		"sample": {Databases: map[string]localscale.DatabaseConfig{"commerce": {Keyspaces: []localscale.KeyspaceConfig{
			{Name: "shop", Shards: 2},
			{Name: "reports", Shards: 1},
		}}}},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	require.NoError(t, seedSchemaDir(t.Context(), server.URL, orgs, dir, logger))

	require.Equal(t, []seeded{
		{Path: "/admin/seed-vschema", Org: "sample", Database: "commerce", Keyspace: "shop"},
		{Path: "/admin/seed-ddl", Org: "sample", Database: "commerce", Keyspace: "reports"},
		{Path: "/admin/seed-ddl", Org: "sample", Database: "commerce", Keyspace: "shop"},
	}, calls)
}

// One schema_dir serves every database the server holds, but each database
// declares only the keyspaces it actually has. A keyspace directory is seeded
// into the databases that declare it and left alone everywhere else, so a
// database without that keyspace is not sent DDL its cluster would reject.
func TestSeedSchemaDirSeedsOnlyDatabasesDeclaringTheKeyspace(t *testing.T) {
	dir := t.TempDir()
	shop := filepath.Join(dir, "shop")
	require.NoError(t, os.MkdirAll(shop, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(shop, "vschema.json"), []byte(`{"sharded": true}`), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(shop, "customers.sql"), []byte("CREATE TABLE customers (id bigint unsigned NOT NULL, PRIMARY KEY (id))"), 0600))

	type seeded struct {
		Database string `json:"database"`
		Keyspace string `json:"keyspace"`
	}
	var calls []seeded
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		var call seeded
		require.NoError(t, json.Unmarshal(body, &call))
		calls = append(calls, call)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	orgs := map[string]localscale.OrgConfig{
		"sample": {Databases: map[string]localscale.DatabaseConfig{
			"commerce":  {Keyspaces: []localscale.KeyspaceConfig{{Name: "shop", Shards: 2}}},
			"analytics": {Keyspaces: []localscale.KeyspaceConfig{{Name: "reports", Shards: 1}}},
		}},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	require.NoError(t, seedSchemaDir(t.Context(), server.URL, orgs, dir, logger))

	// Only commerce declares shop, so analytics is never contacted at all.
	require.Equal(t, []seeded{
		{Database: "commerce", Keyspace: "shop"},
		{Database: "commerce", Keyspace: "shop"},
	}, calls)
}

// Seeding fails rather than starting a cluster whose schema is half applied, so
// an unreadable directory or a rejected statement is surfaced to the operator.
func TestSeedSchemaDirReportsFailures(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	orgs := map[string]localscale.OrgConfig{
		"sample": {Databases: map[string]localscale.DatabaseConfig{"commerce": {
			Keyspaces: []localscale.KeyspaceConfig{{Name: "shop", Shards: 2}},
		}}},
	}
	require.ErrorContains(t, seedSchemaDir(t.Context(), "http://127.0.0.1:0", orgs, filepath.Join(t.TempDir(), "absent"), logger), "read schema dir")

	dir := t.TempDir()
	shop := filepath.Join(dir, "shop")
	require.NoError(t, os.MkdirAll(shop, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(shop, "vschema.json"), []byte(`{"sharded": true}`), 0600))
	rejecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "keyspace not found", http.StatusBadRequest)
	}))
	t.Cleanup(rejecting.Close)
	require.ErrorContains(t, seedSchemaDir(t.Context(), rejecting.URL, orgs, dir, logger), "apply vschema for shop")
}

// A schema_dir assembled out of symbolic links seeds exactly as a directory of
// real directories does. Pointing a link at a keyspace held elsewhere is how a
// schema_dir is composed out of schemas a repository already keeps apart, and a
// link that was skipped would start the cluster silently empty.
func TestSeedSchemaDirFollowsSymlinkedKeyspaces(t *testing.T) {
	elsewhere := t.TempDir()
	shop := filepath.Join(elsewhere, "shop")
	require.NoError(t, os.MkdirAll(shop, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(shop, "vschema.json"), []byte(`{"sharded": true}`), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(shop, "customers.sql"), []byte("CREATE TABLE customers (id bigint unsigned NOT NULL, PRIMARY KEY (id))"), 0600))

	dir := t.TempDir()
	require.NoError(t, os.Symlink(shop, filepath.Join(dir, "shop")))

	type seeded struct {
		Path     string `json:"-"`
		Keyspace string `json:"keyspace"`
	}
	var calls []seeded
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		var call seeded
		require.NoError(t, json.Unmarshal(body, &call))
		call.Path = r.URL.Path
		calls = append(calls, call)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	orgs := map[string]localscale.OrgConfig{
		"sample": {Databases: map[string]localscale.DatabaseConfig{"commerce": {
			Keyspaces: []localscale.KeyspaceConfig{{Name: "shop", Shards: 2}},
		}}},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	require.NoError(t, seedSchemaDir(t.Context(), server.URL, orgs, dir, logger))

	require.Equal(t, []seeded{
		{Path: "/admin/seed-vschema", Keyspace: "shop"},
		{Path: "/admin/seed-ddl", Keyspace: "shop"},
	}, calls)
}

// A keyspace directory no configured database declares seeds nothing, which is
// how a misspelled directory name presents. Seeding still succeeds, so the
// operator is told at a level the binary prints rather than left with a cluster
// that came up healthy and empty.
func TestSeedSchemaDirWarnsOnKeyspaceNoDatabaseDeclares(t *testing.T) {
	dir := t.TempDir()
	typo := filepath.Join(dir, "shopp")
	require.NoError(t, os.MkdirAll(typo, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(typo, "vschema.json"), []byte(`{"sharded": true}`), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(typo, "customers.sql"), []byte("CREATE TABLE customers (id bigint unsigned NOT NULL, PRIMARY KEY (id))"), 0600))

	var contacted bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		contacted = true
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	orgs := map[string]localscale.OrgConfig{
		"sample": {Databases: map[string]localscale.DatabaseConfig{"commerce": {
			Keyspaces: []localscale.KeyspaceConfig{{Name: "shop", Shards: 2}},
		}}},
	}
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	require.NoError(t, seedSchemaDir(t.Context(), server.URL, orgs, dir, logger))

	assert.False(t, contacted, "a keyspace no database declares sends nothing")
	assert.Contains(t, logs.String(), "keyspace directory matches no configured database")
	assert.Contains(t, logs.String(), "keyspace=shopp")
}
