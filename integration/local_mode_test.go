//go:build integration

package integration

import (
	"bytes"
	"database/sql"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/e2eutil"
)

var cliFinder e2eutil.CLIFinder

// TestLocalMode_PullPlanApply exercises the full local mode UX:
// pull schema from a live database, edit a file, plan, and apply —
// with the background daemon auto-starting just like a real user.
func TestLocalMode_PullPlanApply(t *testing.T) {
	ctx := t.Context()

	// Build a storage DSN from the target MySQL container.
	// The testcontainer uses root:testpassword, so we need the full DSN.
	storageDSN := strings.Replace(targetDSN, "/target_test", "/_schemabot", 1)

	// Build the CLI binary (cached across tests in this package)
	binPath := cliFinder.FindOrBuild(t, "..", "./pkg/cmd", "../bin/schemabot")

	// Create a test database with a table
	db, err := sql.Open("mysql", targetDSN+"&multiStatements=true")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	dbName := fmt.Sprintf("local_mode_%d", time.Now().UnixNano()%100000)
	_, err = db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+dbName+"`")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(ctx, "DROP DATABASE IF EXISTS `"+dbName+"`")
		// Clean up _schemabot storage database created by the daemon
		_, _ = db.ExecContext(ctx, "DROP DATABASE IF EXISTS `_schemabot`")
	})

	appDSN := strings.Replace(targetDSN, "/target_test", "/"+dbName, 1)
	appDB, err := sql.Open("mysql", appDSN+"&multiStatements=true")
	require.NoError(t, err)
	defer func() { _ = appDB.Close() }()

	_, err = appDB.ExecContext(ctx, `CREATE TABLE users (
  id bigint unsigned NOT NULL AUTO_INCREMENT,
  name varchar(255) NOT NULL,
  email varchar(255) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`)
	require.NoError(t, err)

	// Set up isolated HOME and schema directory
	tmpHome := t.TempDir()
	schemaDir := t.TempDir()

	// Find a free port for the daemon
	daemonPort := freePort(t)

	// Environment for all CLI commands — isolated HOME, custom storage DSN/port
	env := append(os.Environ(),
		"HOME="+tmpHome,
		"SCHEMABOT_LOCAL_STORAGE_DSN="+storageDSN,
		"SCHEMABOT_LOCAL_PORT="+fmt.Sprintf("%d", daemonPort),
	)

	// Helper to run CLI commands with the test environment
	run := func(args ...string) string {
		t.Helper()
		cmd := exec.CommandContext(ctx, binPath, args...)
		cmd.Env = env
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		err := cmd.Run()
		output := stdout.String() + stderr.String()
		require.NoErrorf(t, err, "CLI %v failed:\n%s", args, output)
		return output
	}

	runInDir := func(dir string, args ...string) string {
		t.Helper()
		cmd := exec.CommandContext(ctx, binPath, args...)
		cmd.Dir = dir
		cmd.Env = env
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		err := cmd.Run()
		output := stdout.String() + stderr.String()
		require.NoErrorf(t, err, "CLI %v failed:\n%s", args, output)
		return output
	}

	// Step 1: Pull schema from the live database
	pullOutput := run("pull", "--dsn", appDSN, "-o", schemaDir, "-e", "staging", "-d", dbName)
	t.Logf("Pull output:\n%s", pullOutput)
	assert.Contains(t, pullOutput, "Pulled 1 table")
	assert.Contains(t, pullOutput, "users.sql")

	// Verify schema files
	usersSQL, err := os.ReadFile(filepath.Join(schemaDir, "users.sql"))
	require.NoError(t, err)
	assert.Contains(t, string(usersSQL), "CREATE TABLE `users`")

	configData, err := os.ReadFile(filepath.Join(schemaDir, "schemabot.yaml"))
	require.NoError(t, err)
	assert.Contains(t, string(configData), "database: "+dbName)

	// Verify local config was created
	localConfig, err := os.ReadFile(filepath.Join(tmpHome, ".schemabot", "config.yaml"))
	require.NoError(t, err)
	assert.Contains(t, string(localConfig), dbName)
	assert.Contains(t, string(localConfig), "staging")

	// Step 2: Edit a .sql file — add a column
	modifiedSQL := strings.Replace(string(usersSQL),
		"PRIMARY KEY (`id`)",
		"`phone` varchar(20) DEFAULT NULL,\n  PRIMARY KEY (`id`)",
		1)
	require.NoError(t, os.WriteFile(filepath.Join(schemaDir, "users.sql"), []byte(modifiedSQL), 0644))

	// Step 3: Plan — this triggers daemon auto-start
	planOutput := runInDir(schemaDir, "plan", "-s", schemaDir, "-e", "staging", "--json")
	t.Logf("Plan output:\n%s", planOutput)
	assert.Contains(t, planOutput, "phone")
	assert.Contains(t, planOutput, "plan_id")

	// Verify daemon is running
	t.Cleanup(func() {
		// Stop the daemon after the test
		cmd := exec.CommandContext(ctx, binPath, "local", "stop")
		cmd.Env = env
		_ = cmd.Run()
	})

	// Step 4: Apply — the daemon is already running
	applyOutput := runInDir(schemaDir, "apply", "-s", schemaDir, "-e", "staging", "-y", "-w", "-o", "log")
	t.Logf("Apply output:\n%s", applyOutput)

	// Step 5: Verify the column was added to the target database
	var columnExists bool
	err = appDB.QueryRowContext(ctx,
		"SELECT COUNT(*) > 0 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'users' AND COLUMN_NAME = 'phone'",
		dbName,
	).Scan(&columnExists)
	require.NoError(t, err)
	assert.True(t, columnExists, "phone column should exist after apply")
	t.Log("Verified: phone column exists in target database")

	// Step 6: Verify local server status
	statusOutput := run("local", "status")
	assert.Contains(t, statusOutput, "running")
}

// freePort finds a free TCP port by binding to :0 and returning the assigned port.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}
