//go:build integration

package localscale_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/localscale"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// Pulling a live Vitess schema exports each keyspace as a namespace with table
// DDL in tables and VSchema JSON in namespace artifacts.
func TestPullSchemaLoadsLiveVitessSchema(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := t.Context()
	const (
		org      = "test-org"
		database = "pullvitess"
	)
	lsc, err := localscale.RunContainer(ctx, localscale.ContainerConfig{
		Orgs: map[string]localscale.ContainerOrgConfig{
			org: {Databases: map[string]localscale.ContainerDatabaseConfig{
				database: {Keyspaces: []localscale.ContainerKeyspaceConfig{
					{Name: "commerce", Shards: 1},
					{Name: "commerce_sharded", Shards: 2},
				}},
			}},
		},
	})
	require.NoError(t, err, "start LocalScale")
	if os.Getenv("DEBUG") != "1" {
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			assert.NoError(t, lsc.Terminate(cleanupCtx), "terminate LocalScale")
		})
	}

	require.NoError(t, lsc.SeedVSchema(ctx, org, database, "commerce", []byte("{\"sharded\":false}")))
	require.NoError(t, lsc.SeedVSchema(ctx, org, database, "commerce_sharded", []byte("{\"sharded\":true,\"vindexes\":{\"hash\":{\"type\":\"hash\"}},\"tables\":{\"users\":{\"column_vindexes\":[{\"column\":\"id\",\"name\":\"hash\"}]}}}")))
	require.NoError(t, lsc.SeedDDL(ctx, org, database, "commerce",
		"CREATE TABLE IF NOT EXISTS `settings` (`id` bigint NOT NULL, `name` varchar(255), PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"))
	require.NoError(t, lsc.SeedDDL(ctx, org, database, "commerce_sharded",
		"CREATE TABLE IF NOT EXISTS `users` (`id` bigint NOT NULL, `email` varchar(255), PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"))

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	client, err := tern.NewLocalClient(tern.LocalConfig{
		Database: database,
		Type:     storage.DatabaseTypeVitess,
		Metadata: map[string]string{
			"organization": org,
			"token_name":   "test",
			"token_value":  "test",
			"api_url":      lsc.URL(),
		},
	}, nil, logger)
	require.NoError(t, err, "create client")
	defer utils.CloseAndLog(client)

	resp, err := client.PullSchema(ctx, &ternv1.PullSchemaRequest{
		Database:    database,
		Type:        storage.DatabaseTypeVitess,
		Environment: "production",
	})

	require.NoError(t, err, "pull Vitess schema")
	require.NotNil(t, resp)
	assert.Equal(t, database, resp.Database)
	assert.Equal(t, storage.DatabaseTypeVitess, resp.Type)
	assert.Equal(t, int32(2), resp.TableCount)
	require.Contains(t, resp.Namespaces, "commerce")
	require.Contains(t, resp.Namespaces, "commerce_sharded")
	assert.Contains(t, resp.Namespaces["commerce"].Tables["settings"], "CREATE TABLE `settings`")
	assert.Contains(t, resp.Namespaces["commerce_sharded"].Tables["users"], "CREATE TABLE `users`")
	assert.JSONEq(t, "{}", resp.Namespaces["commerce"].Artifacts["vschema.json"])
	assert.JSONEq(t, "{\"sharded\":true,\"vindexes\":{\"hash\":{\"type\":\"hash\"}},\"tables\":{\"users\":{\"column_vindexes\":[{\"column\":\"id\",\"name\":\"hash\"}]}}}", resp.Namespaces["commerce_sharded"].Artifacts["vschema.json"])
}
