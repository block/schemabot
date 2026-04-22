//go:build integration

package integration

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/cmd/commands"
	"github.com/block/schemabot/pkg/local"
)

// TestPull verifies the pull command pulls schema from a live database
// and writes canonical .sql files plus schemabot.yaml.
func TestPull(t *testing.T) {
	ctx := t.Context()

	db, err := sql.Open("mysql", targetDSN+"&multiStatements=true")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	dbName := "pull_test"
	_, err = db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+dbName+"`")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(ctx, "DROP DATABASE IF EXISTS `"+dbName+"`")
	})

	appDSN := strings.Replace(targetDSN, "/target_test", "/"+dbName, 1)
	appDB, err := sql.Open("mysql", appDSN+"&multiStatements=true")
	require.NoError(t, err)
	defer func() { _ = appDB.Close() }()

	_, err = appDB.ExecContext(ctx, "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  `email` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `email` (`email`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)

	_, err = appDB.ExecContext(ctx, "CREATE TABLE `orders` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `user_id` bigint unsigned NOT NULL,\n  `total` decimal(10,2) NOT NULL,\n  PRIMARY KEY (`id`),\n  KEY `idx_user_id` (`user_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)

	outputDir := t.TempDir()
	pullCmd := commands.PullCmd{
		DSN:         appDSN,
		OutputDir:   outputDir,
		Environment: "staging",
		Type:        "mysql",
	}
	err = pullCmd.Run(&commands.Globals{})
	require.NoError(t, err)

	// Verify .sql files
	usersSQL, err := os.ReadFile(filepath.Join(outputDir, "users.sql"))
	require.NoError(t, err)
	assert.Contains(t, string(usersSQL), "CREATE TABLE `users`")
	assert.Contains(t, string(usersSQL), "`email`")
	assert.True(t, strings.HasSuffix(string(usersSQL), ";\n"), "should end with semicolon + newline")
	assert.NotContains(t, string(usersSQL), "AUTO_INCREMENT=", "AUTO_INCREMENT should be stripped")

	ordersSQL, err := os.ReadFile(filepath.Join(outputDir, "orders.sql"))
	require.NoError(t, err)
	assert.Contains(t, string(ordersSQL), "CREATE TABLE `orders`")

	// Verify schemabot.yaml (no DSN — just database + type)
	configData, err := os.ReadFile(filepath.Join(outputDir, "schemabot.yaml"))
	require.NoError(t, err)
	assert.Contains(t, string(configData), "database: "+dbName)
	assert.Contains(t, string(configData), "type: mysql")
	assert.NotContains(t, string(configData), "dsn:", "DSN should not be in schemabot.yaml")

	// Verify local config was updated
	cfg, err := local.LoadCLIConfig()
	require.NoError(t, err)
	require.NotNil(t, cfg.Local)
	localDB, ok := cfg.Local[dbName]
	require.True(t, ok, "database should be in local config")
	assert.Equal(t, "mysql", localDB.Type)
	require.Contains(t, localDB.Environments, "staging")
	assert.Equal(t, appDSN, localDB.Environments["staging"].DSN)
}

// TestPullSkipsExistingConfig verifies that pull doesn't overwrite
// an existing schemabot.yaml.
func TestPullSkipsExistingConfig(t *testing.T) {
	ctx := t.Context()

	db, err := sql.Open("mysql", targetDSN+"&multiStatements=true")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	dbName := "pull_skip_config_test"
	_, err = db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+dbName+"`")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(ctx, "DROP DATABASE IF EXISTS `"+dbName+"`")
	})

	appDSN := strings.Replace(targetDSN, "/target_test", "/"+dbName, 1)
	appDB, err := sql.Open("mysql", appDSN)
	require.NoError(t, err)
	defer func() { _ = appDB.Close() }()

	_, err = appDB.ExecContext(ctx, "CREATE TABLE `t1` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)

	outputDir := t.TempDir()

	// Pre-create schemabot.yaml with custom content
	existing := "database: custom\ntype: mysql\n"
	err = os.WriteFile(filepath.Join(outputDir, "schemabot.yaml"), []byte(existing), 0644)
	require.NoError(t, err)

	pullCmd := commands.PullCmd{
		DSN:         appDSN,
		OutputDir:   outputDir,
		Environment: "staging",
		Type:        "mysql",
	}
	err = pullCmd.Run(&commands.Globals{})
	require.NoError(t, err)

	// Config should not be overwritten
	configData, err := os.ReadFile(filepath.Join(outputDir, "schemabot.yaml"))
	require.NoError(t, err)
	assert.Equal(t, existing, string(configData))

	// But .sql file should still be written
	_, err = os.ReadFile(filepath.Join(outputDir, "t1.sql"))
	require.NoError(t, err)
}

// TestPullMultiEnvironment verifies that pulling a second environment
// upserts into the existing local config without overwriting the first.
func TestPullMultiEnvironment(t *testing.T) {
	ctx := t.Context()

	db, err := sql.Open("mysql", targetDSN+"&multiStatements=true")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	dbName := "pull_multi_env_test"
	_, err = db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+dbName+"`")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(ctx, "DROP DATABASE IF EXISTS `"+dbName+"`")
	})

	appDSN := strings.Replace(targetDSN, "/target_test", "/"+dbName, 1)
	appDB, err := sql.Open("mysql", appDSN)
	require.NoError(t, err)
	defer func() { _ = appDB.Close() }()

	_, err = appDB.ExecContext(ctx, "CREATE TABLE `t1` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci")
	require.NoError(t, err)

	outputDir := t.TempDir()

	// Pull staging
	err = (&commands.PullCmd{
		DSN:         appDSN,
		OutputDir:   outputDir,
		Environment: "staging",
		Type:        "mysql",
	}).Run(&commands.Globals{})
	require.NoError(t, err)

	// Pull production (same DB, different env name, same DSN)
	err = (&commands.PullCmd{
		DSN:         appDSN,
		OutputDir:   outputDir,
		Environment: "production",
		Type:        "mysql",
	}).Run(&commands.Globals{})
	require.NoError(t, err)

	// Verify both environments exist in local config
	cfg, err := local.LoadCLIConfig()
	require.NoError(t, err)
	localDB := cfg.Local[dbName]
	require.Contains(t, localDB.Environments, "staging")
	require.Contains(t, localDB.Environments, "production")
	assert.Equal(t, appDSN, localDB.Environments["staging"].DSN)
	assert.Equal(t, appDSN, localDB.Environments["production"].DSN)
}

// TestExtractDatabaseFromDSN verifies database name extraction from various DSN formats.
func TestExtractDatabaseFromDSN(t *testing.T) {
	tests := []struct {
		dsn      string
		expected string
	}{
		{"root:pass@tcp(localhost:3306)/mydb", "mydb"},
		{"root:pass@tcp(localhost:3306)/mydb?parseTime=true", "mydb"},
		{"user@tcp(host)/db_name?charset=utf8", "db_name"},
		{"root@/testdb", "testdb"},
		{"root:pass@tcp(localhost:3306)/", ""},
		{"no-slash-at-all", ""},
	}

	for _, tt := range tests {
		t.Run(tt.dsn, func(t *testing.T) {
			assert.Equal(t, tt.expected, commands.ExtractDatabaseFromDSN(tt.dsn))
		})
	}
}
