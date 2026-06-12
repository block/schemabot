package commands

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
)

func TestBuildOnboardWritePlanWritesConfigAndNamespaceFiles(t *testing.T) {
	root := t.TempDir()
	plan, err := buildOnboardWritePlan(root, &apitypes.PullSchemaResponse{
		Database:    "orders",
		Type:        "mysql",
		Environment: "production",
		TableCount:  2,
		SchemaFiles: map[string]*apitypes.SchemaFiles{
			"orders": {
				Files: map[string]string{
					"users.sql":  "CREATE TABLE `users` (`id` bigint NOT NULL);\n",
					"orders.sql": "CREATE TABLE `orders` (`id` bigint NOT NULL);\n",
				},
			},
		},
	})
	require.NoError(t, err)
	require.NoError(t, plan.checkConflicts(false))
	require.NoError(t, plan.write())

	config, err := os.ReadFile(filepath.Join(root, "schemabot.yaml"))
	require.NoError(t, err)
	assert.Equal(t, "database: orders\ntype: mysql\n", string(config))

	users, err := os.ReadFile(filepath.Join(root, "orders", "users.sql"))
	require.NoError(t, err)
	assert.Equal(t, "CREATE TABLE `users` (`id` bigint NOT NULL);\n", string(users))

	orders, err := os.ReadFile(filepath.Join(root, "orders", "orders.sql"))
	require.NoError(t, err)
	assert.Equal(t, "CREATE TABLE `orders` (`id` bigint NOT NULL);\n", string(orders))
}

func TestOnboardWritePlanRefusesExistingFilesWithoutForce(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "schemabot.yaml"), []byte("database: old\ntype: mysql\n"), 0o644))
	plan, err := buildOnboardWritePlan(root, &apitypes.PullSchemaResponse{
		Database:    "orders",
		Type:        "mysql",
		Environment: "production",
		SchemaFiles: map[string]*apitypes.SchemaFiles{
			"orders": {Files: map[string]string{"users.sql": "CREATE TABLE `users` (`id` bigint NOT NULL);\n"}},
		},
	})
	require.NoError(t, err)

	err = plan.checkConflicts(false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refusing to overwrite existing files")
	assert.Contains(t, err.Error(), filepath.Join(root, "schemabot.yaml"))

	require.NoError(t, plan.checkConflicts(true))
}

func TestBuildOnboardWritePlanRejectsUnsafeResponsePaths(t *testing.T) {
	_, err := buildOnboardWritePlan(t.TempDir(), &apitypes.PullSchemaResponse{
		Database:    "orders",
		Type:        "mysql",
		Environment: "production",
		SchemaFiles: map[string]*apitypes.SchemaFiles{
			"orders": {Files: map[string]string{"../users.sql": "CREATE TABLE `users` (`id` bigint NOT NULL);\n"}},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema file")
}
