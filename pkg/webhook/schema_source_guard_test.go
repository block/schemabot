package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
)

func guardTestHandler(t *testing.T, databases map[string]api.DatabaseConfig) *Handler {
	t.Helper()
	service := api.New(nil, &api.ServerConfig{Databases: databases}, nil, testLogger())
	return &Handler{service: service, logger: testLogger()}
}

// A directory listed in databases.<db>.allowed_dirs is server-managed. A PR that
// changes schema files there must always be gated by SchemaBot, even if the
// in-repo schemabot.yaml is missing or was removed — otherwise dropping the
// config would silently land DDL ungated. unmanagedServerManagedSchemaChanges
// is the detection that keeps that case failing closed.
func TestUnmanagedServerManagedSchemaChanges(t *testing.T) {
	const repo = "octocat/hello-world"
	managed := map[string]api.DatabaseConfig{
		"orders": {
			Type:         "mysql",
			AllowedRepos: []string{repo},
			AllowedDirs:  []string{"db/orders/schema"},
		},
	}
	coveringConfig := []ghclient.DiscoveredConfig{{SchemaDir: "db/orders/schema"}}
	managedTwoDirs := map[string]api.DatabaseConfig{
		"orders": {
			Type:         "mysql",
			AllowedRepos: []string{repo},
			AllowedDirs:  []string{"db/orders/schema", "db/orders/v2"},
		},
	}

	cases := []struct {
		name      string
		databases map[string]api.DatabaseConfig
		files     []ghclient.PRFile
		configs   []ghclient.DiscoveredConfig
		want      []string
	}{
		{
			name:      "managed dir, no config -> flagged",
			databases: managed,
			files:     []ghclient.PRFile{{Filename: "db/orders/schema/users.sql", Status: "modified"}},
			want:      []string{"db/orders/schema/users.sql"},
		},
		{
			name:      "managed dir descendant, no config -> flagged",
			databases: managed,
			files:     []ghclient.PRFile{{Filename: "db/orders/schema/sub/users.sql", Status: "added"}},
			want:      []string{"db/orders/schema/sub/users.sql"},
		},
		{
			name:      "managed dir, covered by config -> not flagged",
			databases: managed,
			files:     []ghclient.PRFile{{Filename: "db/orders/schema/users.sql", Status: "modified"}},
			configs:   coveringConfig,
			want:      nil,
		},
		{
			name:      "managed dir descendant, covered by ancestor config -> not flagged",
			databases: managed,
			files:     []ghclient.PRFile{{Filename: "db/orders/schema/sub/users.sql", Status: "modified"}},
			configs:   coveringConfig,
			want:      nil,
		},
		{
			// A repo-root schemabot.yaml (SchemaDir ".") is an ancestor of every
			// directory, so discovery resolves it for schema files in managed
			// subdirectories — it must not be flagged as missing config.
			name:      "root config covers managed subdir file -> not flagged",
			databases: managed,
			files:     []ghclient.PRFile{{Filename: "db/orders/schema/users.sql", Status: "modified"}},
			configs:   []ghclient.DiscoveredConfig{{SchemaDir: "."}},
			want:      nil,
		},
		{
			name:      "removed schema file -> not flagged",
			databases: managed,
			files:     []ghclient.PRFile{{Filename: "db/orders/schema/users.sql", Status: "removed"}},
			want:      nil,
		},
		{
			name:      "outside managed dirs -> not flagged",
			databases: managed,
			files:     []ghclient.PRFile{{Filename: "db/other/schema/x.sql", Status: "modified"}},
			want:      nil,
		},
		{
			// Moving a schema directory: the old config and files are removed and
			// the schema files reappear at the destination alongside the moved
			// config. The destination files are covered by the moved config, so the
			// move is not mistaken for an unmanaged schema change.
			name:      "clean move with config moved alongside -> not flagged",
			databases: managedTwoDirs,
			files: []ghclient.PRFile{
				{Filename: "db/orders/schema/schemabot.yaml", Status: "removed"},
				{Filename: "db/orders/schema/users.sql", Status: "removed"},
				{Filename: "db/orders/v2/users.sql", Status: "added"},
			},
			configs: []ghclient.DiscoveredConfig{{SchemaDir: "db/orders/v2"}},
			want:    nil,
		},
		{
			// Moving schema files to another server-managed directory without moving
			// the config leaves the destination files uncovered, which fails closed.
			name:      "move to managed dir without its config -> flagged",
			databases: managedTwoDirs,
			files: []ghclient.PRFile{
				{Filename: "db/orders/schema/users.sql", Status: "removed"},
				{Filename: "db/orders/v2/users.sql", Status: "added"},
			},
			want: []string{"db/orders/v2/users.sql"},
		},
		{
			name:      "non-schema file -> not flagged",
			databases: managed,
			files:     []ghclient.PRFile{{Filename: "db/orders/schema/README.md", Status: "modified"}},
			want:      nil,
		},
		{
			name:      "open mode (no allowlist) -> not flagged",
			databases: map[string]api.DatabaseConfig{"orders": {Type: "mysql"}},
			files:     []ghclient.PRFile{{Filename: "db/orders/schema/users.sql", Status: "modified"}},
			want:      nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := guardTestHandler(t, tc.databases)
			got := h.unmanagedServerManagedSchemaChanges(repo, tc.files, tc.configs)
			assert.Equal(t, tc.want, got)
		})
	}
}

// The block message must name each unmanaged file and the managed database that
// owns its directory so an operator knows exactly what to restore or offboard.
func TestUnmanagedSchemaDirMessage(t *testing.T) {
	const repo = "octocat/hello-world"
	h := guardTestHandler(t, map[string]api.DatabaseConfig{
		"orders": {Type: "mysql", AllowedRepos: []string{repo}, AllowedDirs: []string{"db/orders/schema"}},
	})

	msg := h.unmanagedSchemaDirMessage(repo, []string{"db/orders/schema/users.sql"})
	assert.Contains(t, msg, "db/orders/schema/users.sql")
	assert.Contains(t, msg, "database `orders`")
	assert.Contains(t, msg, "allowed_dirs")
	assert.Contains(t, msg, "schemabot.yaml")
}

func TestDatabaseForSchemaPath(t *testing.T) {
	const repo = "octocat/hello-world"
	cfg := &api.ServerConfig{Databases: map[string]api.DatabaseConfig{
		"orders": {Type: "mysql", AllowedRepos: []string{repo}, AllowedDirs: []string{"db/orders/schema"}},
	}}

	name, ok := cfg.DatabaseForSchemaPath(repo, "db/orders/schema/users.sql")
	require.True(t, ok)
	assert.Equal(t, "orders", name)

	_, ok = cfg.DatabaseForSchemaPath(repo, "db/other/schema/x.sql")
	assert.False(t, ok)

	_, ok = cfg.DatabaseForSchemaPath("other/repo", "db/orders/schema/users.sql")
	assert.False(t, ok)
}
