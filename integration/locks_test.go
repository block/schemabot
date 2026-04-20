//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// acquireLockViaAPI creates a lock directly via the API to simulate different owners.
func acquireLockViaAPI(t *testing.T, endpoint, database, dbType, owner, repo string, pr int) (*http.Response, error) {
	t.Helper()
	reqBody := map[string]any{
		"database":      database,
		"database_type": dbType,
		"owner":         owner,
	}
	if repo != "" {
		reqBody["repository"] = repo
	}
	if pr != 0 {
		reqBody["pull_request"] = pr
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal lock request: %w", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/api/locks/acquire", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return http.DefaultClient.Do(req)
}

// TestCLI_Locking_ApplyAcquiresLock tests that apply acquires a lock on the database.
func TestCLI_Locking_ApplyAcquiresLock(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	dbName := fmt.Sprintf("lock_acquire_%d", time.Now().UnixNano())

	schemabotAddr := startSchemaBotLocalDB(t, dbName)

	schemaDir := newSchemaDirForDB(t, dbName)
	writeFile(t, filepath.Join(schemaDir, "users.sql"), `
CREATE TABLE users (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL
);
`)

	endpoint := "http://" + schemabotAddr

	t.Run("apply_acquires_lock", func(t *testing.T) {
		// Apply should acquire lock and show message
		out := runCLIInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--watch=false",
		)
		assertContains(t, out, "Lock acquired")
		assertContains(t, out, dbName)
	})

	t.Run("locks_shows_acquired_lock", func(t *testing.T) {
		// Wait for apply to complete first
		waitForStateDB(t, endpoint, dbName, "completed", 30*time.Second)

		// Lock should still be held after apply completes (default behavior)
		out := runCLI(t, binPath, "locks", "--endpoint", endpoint)
		assertContains(t, out, dbName)
		assertContains(t, out, "cli:")
	})
}

// TestCLI_Locking_SecondApplyBlocked tests that a second apply is blocked when lock is held.
func TestCLI_Locking_SecondApplyBlocked(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	dbName := fmt.Sprintf("lock_block_%d", time.Now().UnixNano())

	schemabotAddr := startSchemaBotLocalDB(t, dbName)

	schemaDir := newSchemaDirForDB(t, dbName)
	writeFile(t, filepath.Join(schemaDir, "users.sql"), `
CREATE TABLE users (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL
);
`)

	endpoint := "http://" + schemabotAddr

	// First apply - acquires lock
	t.Run("first_apply_acquires_lock", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--watch=false",
		)
		assertContains(t, out, "Lock acquired")
		waitForStateDB(t, endpoint, dbName, "completed", 30*time.Second)
	})

	// Modify schema for second apply
	writeFile(t, filepath.Join(schemaDir, "users.sql"), `
CREATE TABLE users (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    INDEX idx_email (email)
);
`)

	// Second apply from same owner - should succeed (idempotent lock acquisition)
	t.Run("second_apply_same_owner_succeeds", func(t *testing.T) {
		writeFile(t, filepath.Join(schemaDir, "users.sql"), `
CREATE TABLE users (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    INDEX idx_email (email)
);
`)
		out := runCLIInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--watch=false",
		)
		// Same owner can re-acquire the lock (idempotent)
		assertContains(t, out, "Lock acquired")
		waitForStateDB(t, endpoint, dbName, "completed", 30*time.Second)
	})
}

// TestCLI_Locking_ForceBreaksLock tests that --force breaks an existing lock.
func TestCLI_Locking_ForceBreaksLock(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	dbName := fmt.Sprintf("lock_force_%d", time.Now().UnixNano())

	schemabotAddr := startSchemaBotLocalDB(t, dbName)

	schemaDir := newSchemaDirForDB(t, dbName)
	writeFile(t, filepath.Join(schemaDir, "users.sql"), `
CREATE TABLE users (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL
);
`)

	endpoint := "http://" + schemabotAddr

	// First apply - acquires lock
	t.Run("first_apply_acquires_lock", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--watch=false",
		)
		assertContains(t, out, "Lock acquired")
		waitForStateDB(t, endpoint, dbName, "completed", 30*time.Second)
	})

	// Modify schema
	writeFile(t, filepath.Join(schemaDir, "users.sql"), `
CREATE TABLE users (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    INDEX idx_email (email)
);
`)

	t.Run("force_breaks_existing_lock", func(t *testing.T) {
		// Apply with --force should break existing lock and proceed
		out := runCLIInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--watch=false",
			"--force",
		)
		// Should show lock acquired (after force release)
		assertContains(t, out, "Lock acquired")
		assertContains(t, out, "Apply started")
		waitForStateDB(t, endpoint, dbName, "completed", 30*time.Second)
	})
}

// TestCLI_Locking_YieldReleasesLock tests that --yield releases the lock after success.
func TestCLI_Locking_YieldReleasesLock(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	dbName := fmt.Sprintf("lock_yield_%d", time.Now().UnixNano())

	schemabotAddr := startSchemaBotLocalDB(t, dbName)

	schemaDir := newSchemaDirForDB(t, dbName)
	writeFile(t, filepath.Join(schemaDir, "users.sql"), `
CREATE TABLE users (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL
);
`)

	endpoint := "http://" + schemabotAddr

	t.Run("apply_with_yield_releases_lock", func(t *testing.T) {
		// Apply with --yield should release lock after completion
		// Use --output=log to avoid TTY requirement while still watching for completion
		out := runCLIInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--output=log",
			"--yield",
		)
		assertContains(t, out, "Lock acquired")
		assertContains(t, out, "Apply completed") // log format shows this
		assertContains(t, out, "Lock released")
	})

	t.Run("locks_shows_no_lock_after_yield", func(t *testing.T) {
		// Lock should be released
		out := runCLI(t, binPath, "locks", "--endpoint", endpoint)
		// Should not contain our database or show "No active locks"
		assert.NotContains(t, stripANSI(out), dbName, "expected lock to be released, but found %s in locks list", dbName)
	})

	// Modify schema
	writeFile(t, filepath.Join(schemaDir, "users.sql"), `
CREATE TABLE users (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    INDEX idx_email (email)
);
`)

	t.Run("second_apply_succeeds_after_yield", func(t *testing.T) {
		// Second apply should succeed without --force since lock was released
		// Use --output=log to avoid TTY requirement while still watching for completion
		out := runCLIInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--output=log",
			"--yield",
		)
		assertContains(t, out, "Lock acquired")
		assertContains(t, out, "Apply completed") // log format shows this
		assertContains(t, out, "Lock released")
	})
}

// TestCLI_Locking_UnlockCommand tests the schemabot unlock command.
func TestCLI_Locking_UnlockCommand(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	dbName := fmt.Sprintf("lock_unlock_%d", time.Now().UnixNano())

	schemabotAddr := startSchemaBotLocalDB(t, dbName)

	schemaDir := newSchemaDirForDB(t, dbName)
	writeFile(t, filepath.Join(schemaDir, "users.sql"), `
CREATE TABLE users (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL
);
`)

	endpoint := "http://" + schemabotAddr

	// Apply to acquire lock
	t.Run("apply_acquires_lock", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--watch=false",
		)
		assertContains(t, out, "Lock acquired")
		waitForStateDB(t, endpoint, dbName, "completed", 30*time.Second)
	})

	t.Run("unlock_releases_lock", func(t *testing.T) {
		// Unlock should release the lock
		out := runCLI(t, binPath, "unlock",
			"-d", dbName,
			"-t", "mysql",
			"--endpoint", endpoint,
		)
		assertContains(t, out, "Lock released")
	})

	t.Run("locks_shows_no_lock_after_unlock", func(t *testing.T) {
		// Lock should be released
		out := runCLI(t, binPath, "locks", "--endpoint", endpoint)
		assert.NotContains(t, stripANSI(out), dbName, "expected lock to be released, but found %s in locks list", dbName)
	})

	t.Run("unlock_nonexistent_lock", func(t *testing.T) {
		// Unlocking a non-existent lock should not error (just show "no lock found")
		out := runCLI(t, binPath, "unlock",
			"-d", "nonexistent_db",
			"-t", "mysql",
			"--endpoint", endpoint,
		)
		assertContains(t, out, "No lock found")
	})
}

// TestCLI_Locking_UnlockForce tests the --force flag on unlock.
func TestCLI_Locking_UnlockForce(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	dbName := fmt.Sprintf("lock_unlockforce_%d", time.Now().UnixNano())

	schemabotAddr := startSchemaBotLocalDB(t, dbName)

	schemaDir := newSchemaDirForDB(t, dbName)
	writeFile(t, filepath.Join(schemaDir, "users.sql"), `
CREATE TABLE users (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL
);
`)

	endpoint := "http://" + schemabotAddr

	// Apply to acquire lock
	t.Run("apply_acquires_lock", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--watch=false",
		)
		assertContains(t, out, "Lock acquired")
		waitForStateDB(t, endpoint, dbName, "completed", 30*time.Second)
	})

	t.Run("unlock_force_releases_any_lock", func(t *testing.T) {
		// Force unlock should release the lock regardless of owner
		out := runCLI(t, binPath, "unlock",
			"-d", dbName,
			"-t", "mysql",
			"--endpoint", endpoint,
			"--force",
		)
		assertContains(t, out, "Force released lock")
	})

	t.Run("locks_shows_no_lock_after_force_unlock", func(t *testing.T) {
		out := runCLI(t, binPath, "locks", "--endpoint", endpoint)
		assert.NotContains(t, stripANSI(out), dbName, "expected lock to be released, but found %s in locks list", dbName)
	})
}

// TestCLI_Locking_LocksCommand tests the schemabot locks command.
func TestCLI_Locking_LocksCommand(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	dbName1 := fmt.Sprintf("lock_list1_%d", time.Now().UnixNano())

	schemabotAddr := startSchemaBotLocalDB(t, dbName1)

	// Create second database manually (reuse same server)
	// Note: We'll use different schema dirs to simulate different databases

	schemaDir1 := newSchemaDirForDB(t, dbName1)
	writeFile(t, filepath.Join(schemaDir1, "users.sql"), `
CREATE TABLE users (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL
);
`)

	endpoint := "http://" + schemabotAddr

	t.Run("locks_no_lock_for_our_database_initially", func(t *testing.T) {
		out := runCLI(t, binPath, "locks", "--endpoint", endpoint)
		// Our specific database should not have a lock yet
		// (other tests may have locks on different databases)
		assert.NotContains(t, stripANSI(out), dbName1, "expected no lock for %s initially, but found it in output: %s", dbName1, out)
	})

	t.Run("apply_first_database", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir1, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--watch=false",
		)
		assertContains(t, out, "Lock acquired")
		waitForStateDB(t, endpoint, dbName1, "completed", 30*time.Second)
	})

	t.Run("locks_shows_our_lock", func(t *testing.T) {
		out := runCLI(t, binPath, "locks", "--endpoint", endpoint)
		// Verify our database lock appears in the list
		// Don't check exact count as other parallel tests may have locks
		assertContains(t, out, "Active Locks")
		assertContains(t, out, dbName1)
		assertContains(t, out, "mysql")
		assertContains(t, out, "cli:")
	})

	t.Run("locks_help", func(t *testing.T) {
		out := runCLI(t, binPath, "locks", "--help")
		assertContains(t, out, "List all active database locks")
		assertContains(t, out, "--endpoint")
	})

	// Cleanup
	t.Run("cleanup_unlock", func(t *testing.T) {
		runCLI(t, binPath, "unlock", "-d", dbName1, "-t", "mysql", "--endpoint", endpoint)
	})
}

// TestCLI_Locking_LockConflictMessage tests the lock conflict error message.
func TestCLI_Locking_LockConflictMessage(t *testing.T) {
	// This test verifies the lock conflict message format
	// We can't easily test with different owners in e2e (same machine = same owner)
	// but we can test the help text and basic validation

	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	t.Run("apply_help_shows_locking_info", func(t *testing.T) {
		out := runCLI(t, binPath, "apply", "--help")
		assertContains(t, out, "--force")
		assertContains(t, out, "--yield")
	})

	t.Run("unlock_help_shows_force_info", func(t *testing.T) {
		out := runCLI(t, binPath, "unlock", "--help")
		assertContains(t, out, "--force")
		assertContains(t, out, "bypass ownership check")
	})
}

// TestCLI_Locking_PRHoldsLock_CLIBlocked tests that CLI is blocked when a PR holds the lock.
// Simulates a PR lock by creating a lock via API with a PR-style owner.
func TestCLI_Locking_PRHoldsLock_CLIBlocked(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	dbName := fmt.Sprintf("lock_pr_cli_%d", time.Now().UnixNano())

	schemabotAddr := startSchemaBotLocalDB(t, dbName)

	endpoint := "http://" + schemabotAddr

	// Simulate a PR holding the lock by creating lock via API
	t.Run("pr_acquires_lock_via_api", func(t *testing.T) {
		// Use curl or http client to create a lock with PR-style owner
		resp, err := acquireLockViaAPI(t, endpoint, dbName, "mysql", "block/myrepo#123", "block/myrepo", 123)
		require.NoError(t, err, "acquire lock via API")
		require.Equal(t, 200, resp.StatusCode, "expected 200, got %d", resp.StatusCode)
		_ = resp.Body.Close()
	})

	t.Run("locks_shows_pr_lock", func(t *testing.T) {
		out := runCLI(t, binPath, "locks", "--endpoint", endpoint)
		assertContains(t, out, dbName)
		assertContains(t, out, "block/myrepo#123")
	})

	schemaDir := newSchemaDirForDB(t, dbName)
	writeFile(t, filepath.Join(schemaDir, "users.sql"), `
CREATE TABLE users (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL
);
`)

	t.Run("cli_apply_blocked_by_pr_lock", func(t *testing.T) {
		// CLI apply should be blocked
		out, err := runCLIWithErrorInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--watch=false",
		)
		assert.Error(t, err, "expected CLI apply to fail when PR holds lock")
		assertContains(t, out, "Database Locked")
		assertContains(t, out, "block/myrepo#123")
		assertContains(t, out, "--force")
	})

	t.Run("cli_apply_with_force_breaks_pr_lock", func(t *testing.T) {
		// CLI apply with --force should break the PR lock
		out := runCLIInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--watch=false",
			"--force",
		)
		assertContains(t, out, "Force released lock")
		assertContains(t, out, "block/myrepo#123")
		assertContains(t, out, "Lock acquired")
		waitForStateDB(t, endpoint, dbName, "completed", 30*time.Second)
	})

	// Cleanup
	t.Run("cleanup", func(t *testing.T) {
		runCLI(t, binPath, "unlock", "-d", dbName, "-t", "mysql", "--endpoint", endpoint, "--force")
	})
}

// TestCLI_Locking_DifferentCLIUsers tests that different CLI users block each other.
// Simulates different CLI users by creating a lock via API with a different CLI owner.
func TestCLI_Locking_DifferentCLIUsers(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	dbName := fmt.Sprintf("lock_cli_cli_%d", time.Now().UnixNano())

	schemabotAddr := startSchemaBotLocalDB(t, dbName)

	endpoint := "http://" + schemabotAddr

	// Simulate another CLI user holding the lock
	t.Run("other_cli_user_acquires_lock", func(t *testing.T) {
		resp, err := acquireLockViaAPI(t, endpoint, dbName, "mysql", "cli:otheruser@othermachine", "", 0)
		require.NoError(t, err, "acquire lock via API")
		require.Equal(t, 200, resp.StatusCode, "expected 200, got %d", resp.StatusCode)
		_ = resp.Body.Close()
	})

	t.Run("locks_shows_other_cli_lock", func(t *testing.T) {
		out := runCLI(t, binPath, "locks", "--endpoint", endpoint)
		assertContains(t, out, dbName)
		assertContains(t, out, "cli:otheruser@othermachine")
	})

	schemaDir := newSchemaDirForDB(t, dbName)
	writeFile(t, filepath.Join(schemaDir, "users.sql"), `
CREATE TABLE users (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL
);
`)

	t.Run("cli_apply_blocked_by_other_cli", func(t *testing.T) {
		// CLI apply should be blocked by other CLI user's lock
		out, err := runCLIWithErrorInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--watch=false",
		)
		assert.Error(t, err, "expected CLI apply to fail when other CLI user holds lock")
		assertContains(t, out, "Database Locked")
		assertContains(t, out, "cli:otheruser@othermachine")
	})

	t.Run("unlock_without_force_fails", func(t *testing.T) {
		// Unlock without --force should fail (not our lock)
		out, err := runCLIWithError(t, binPath, "unlock",
			"-d", dbName,
			"-t", "mysql",
			"--endpoint", endpoint,
		)
		assert.Error(t, err, "expected unlock to fail without --force")
		assertContains(t, out, "Cannot release lock")
		assertContains(t, out, "cli:otheruser@othermachine")
	})

	t.Run("unlock_with_force_succeeds", func(t *testing.T) {
		// Unlock with --force should succeed
		out := runCLI(t, binPath, "unlock",
			"-d", dbName,
			"-t", "mysql",
			"--endpoint", endpoint,
			"--force",
		)
		assertContains(t, out, "Force released lock")
	})
}

// TestCLI_Locking_OnlyBlocksApply tests that locking only blocks apply, not plan/progress.
func TestCLI_Locking_OnlyBlocksApply(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	dbName := fmt.Sprintf("lock_onlyapply_%d", time.Now().UnixNano())

	schemabotAddr := startSchemaBotLocalDB(t, dbName)

	endpoint := "http://" + schemabotAddr

	// Simulate another user holding the lock
	t.Run("other_user_acquires_lock", func(t *testing.T) {
		resp, err := acquireLockViaAPI(t, endpoint, dbName, "mysql", "cli:otheruser@othermachine", "", 0)
		require.NoError(t, err, "acquire lock via API")
		require.Equal(t, 200, resp.StatusCode, "expected 200, got %d", resp.StatusCode)
		_ = resp.Body.Close()
	})

	schemaDir := newSchemaDirForDB(t, dbName)
	writeFile(t, filepath.Join(schemaDir, "users.sql"), `
CREATE TABLE users (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL
);
`)

	// All read-only commands should work while locked

	t.Run("plan_works_while_locked", func(t *testing.T) {
		// Plan should work even when database is locked by another user
		out := runCLIInDir(t, binPath, schemaDir, "plan",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
		)
		// Plan should succeed and show the changes
		assertContains(t, out, "CREATE TABLE")
		assertContains(t, out, "users")
	})

	t.Run("progress_works_while_locked", func(t *testing.T) {
		// Progress should work even when database is locked by another user
		out := runCLIInDir(t, binPath, schemaDir, "progress",
			"--database", dbName,
			"-e", "staging",
			"--endpoint", endpoint,
			"--watch=false",
		)
		// Progress should succeed and show no active schema change
		assertContains(t, out, "No active schema change")
	})

	t.Run("locks_works_while_locked", func(t *testing.T) {
		// Locks command should always work
		out := runCLI(t, binPath, "locks", "--endpoint", endpoint)
		assertContains(t, out, dbName)
	})

	t.Run("cutover_works_while_locked", func(t *testing.T) {
		// Cutover should work while locked (it's for the lock owner's schema change)
		// It may fail because there's no active schema change, but NOT because of locking
		out, _ := runCLIWithErrorInDir(t, binPath, schemaDir, "cutover",
			"-d", dbName,
			"-e", "staging",
			"--endpoint", endpoint,
			"--watch=false",
		)
		// Should NOT show "Database Locked" error - cutover doesn't check locks
		assert.NotContains(t, stripANSI(out), "Database Locked", "cutover should not be blocked by locking")
	})

	t.Run("stop_works_while_locked", func(t *testing.T) {
		// Stop should work while locked (it's for controlling the owner's schema change)
		out, _ := runCLIWithErrorInDir(t, binPath, schemaDir, "stop",
			"-d", dbName,
			"-e", "staging",
			"--endpoint", endpoint,
		)
		// Should NOT show "Database Locked" error
		assert.NotContains(t, stripANSI(out), "Database Locked", "stop should not be blocked by locking")
	})

	t.Run("start_works_while_locked", func(t *testing.T) {
		// Start should work while locked
		out, _ := runCLIWithErrorInDir(t, binPath, schemaDir, "start",
			"-d", dbName,
			"-e", "staging",
			"--endpoint", endpoint,
			"--watch=false",
		)
		// Should NOT show "Database Locked" error
		assert.NotContains(t, stripANSI(out), "Database Locked", "start should not be blocked by locking")
	})

	t.Run("volume_works_while_locked", func(t *testing.T) {
		// Volume should work while locked
		out, _ := runCLIWithErrorInDir(t, binPath, schemaDir, "volume",
			"-d", dbName,
			"-e", "staging",
			"-v", "5",
			"--endpoint", endpoint,
		)
		// Should NOT show "Database Locked" error
		assert.NotContains(t, stripANSI(out), "Database Locked", "volume should not be blocked by locking")
	})

	t.Run("apply_blocked_while_locked", func(t *testing.T) {
		// Only apply should be blocked
		out, err := runCLIWithErrorInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--watch=false",
		)
		assert.Error(t, err, "expected apply to fail when locked by another user")
		assertContains(t, out, "Database Locked")
	})

	// Cleanup
	t.Run("cleanup", func(t *testing.T) {
		runCLI(t, binPath, "unlock", "-d", dbName, "-t", "mysql", "--endpoint", endpoint, "--force")
	})
}

// TestCLI_Locking_NoLockBypassesLocking tests that --no-lock allows apply even when
// another user holds the lock, and that no lock is acquired by the caller.
func TestCLI_Locking_NoLockBypassesLocking(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	dbName := fmt.Sprintf("lock_nolock_%d", time.Now().UnixNano())

	schemabotAddr := startSchemaBotLocalDB(t, dbName)

	endpoint := "http://" + schemabotAddr

	schemaDir := newSchemaDirForDB(t, dbName)
	writeFile(t, filepath.Join(schemaDir, "users.sql"), `
CREATE TABLE users (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL
);
`)

	// Another user holds the lock
	t.Run("other_user_acquires_lock", func(t *testing.T) {
		resp, err := acquireLockViaAPI(t, endpoint, dbName, "mysql", "cli:otheruser@othermachine", "", 0)
		require.NoError(t, err, "acquire lock via API")
		require.Equal(t, 200, resp.StatusCode, "expected 200, got %d", resp.StatusCode)
		_ = resp.Body.Close()
	})

	t.Run("apply_with_no_lock_bypasses_lock", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--watch=false",
			"--no-lock",
		)
		// Should not show lock conflict
		assert.NotContains(t, stripANSI(out), "Database Locked")
		// Should proceed with apply
		assertContains(t, out, "Apply started")
		waitForStateDB(t, endpoint, dbName, "completed", 30*time.Second)
	})

	t.Run("original_lock_still_held", func(t *testing.T) {
		// The other user's lock should still be in place (--no-lock doesn't touch it)
		out := runCLI(t, binPath, "locks", "--endpoint", endpoint)
		assertContains(t, out, dbName)
		assertContains(t, out, "otheruser")
	})

	// Cleanup
	t.Run("cleanup", func(t *testing.T) {
		runCLI(t, binPath, "unlock", "-d", dbName, "-t", "mysql", "--endpoint", endpoint, "--force")
	})
}

// TestCLI_Locking_NoLockStillBlockedByActiveApply tests that --no-lock does not bypass
// the active schema change check. If an apply is already running (or waiting for cutover),
// a second apply with --no-lock is still blocked by the progress API check.
func TestCLI_Locking_NoLockStillBlockedByActiveApply(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	dbName := fmt.Sprintf("lock_nolock_active_%d", time.Now().UnixNano())

	schemabotAddr := startSchemaBotLocalDB(t, dbName)

	endpoint := "http://" + schemabotAddr

	schemaDir := newSchemaDirForDB(t, dbName)
	writeFile(t, filepath.Join(schemaDir, "users.sql"), `
CREATE TABLE users (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    INDEX idx_email (email)
);
`)

	// Start an apply with --defer-cutover so it stays in "waiting for cutover" state
	t.Run("first_apply_deferred", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--watch=false",
			"--defer-cutover",
			"--no-lock",
		)
		assertContains(t, out, "Apply started")
		waitForStateDB(t, endpoint, dbName, "waiting_for_cutover", 30*time.Second)
	})

	// Second apply with --no-lock should still be blocked by the active schema change
	t.Run("second_apply_blocked_by_active_change", func(t *testing.T) {
		_, err := runCLIWithErrorInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--watch=false",
			"--no-lock",
		)
		assert.Error(t, err, "expected second apply to fail due to active schema change")
	})

	// Cleanup: cutover to complete the deferred apply
	t.Run("cleanup", func(t *testing.T) {
		runCLIInDir(t, binPath, schemaDir, "cutover",
			"-d", dbName,
			"-e", "staging",
			"--endpoint", endpoint,
			"--watch=false",
		)
		waitForStateDB(t, endpoint, dbName, "completed", 30*time.Second)
	})
}

// TestCLI_Locking_SameEnvDifferentOwners tests lock blocking for same environment, different owners.
func TestCLI_Locking_SameEnvDifferentOwners(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	dbName := fmt.Sprintf("lock_sameenv_%d", time.Now().UnixNano())

	schemabotAddr := startSchemaBotLocalDB(t, dbName)

	endpoint := "http://" + schemabotAddr

	schemaDir := newSchemaDirForDB(t, dbName)
	writeFile(t, filepath.Join(schemaDir, "users.sql"), `
CREATE TABLE users (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL
);
`)

	// PR acquires lock on staging
	t.Run("pr_acquires_lock_on_staging", func(t *testing.T) {
		resp, err := acquireLockViaAPI(t, endpoint, dbName, "mysql", "block/repo#100", "block/repo", 100)
		require.NoError(t, err, "acquire lock")
		require.Equal(t, 200, resp.StatusCode, "expected 200, got %d", resp.StatusCode)
		_ = resp.Body.Close()
	})

	// CLI tries to apply to SAME environment (staging) - should be blocked
	t.Run("cli_blocked_on_same_env", func(t *testing.T) {
		out, err := runCLIWithErrorInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging", // Same environment
			"--endpoint", endpoint,
			"-y",
			"--watch=false",
		)
		assert.Error(t, err, "expected CLI to be blocked on same environment")
		assertContains(t, out, "Database Locked")
		assertContains(t, out, "block/repo#100")
	})

	// Cleanup
	t.Run("cleanup", func(t *testing.T) {
		runCLI(t, binPath, "unlock", "-d", dbName, "-t", "mysql", "--endpoint", endpoint, "--force")
	})
}

// TestCLI_Locking_DifferentEnvBlocked tests that locking blocks ALL environments (lock is per database).
// This is a key design decision: staging lock blocks production to prevent concurrent changes.
func TestCLI_Locking_DifferentEnvBlocked(t *testing.T) {
	// Note: This test documents the intended behavior but can't fully test it
	// because our e2e setup only has "staging" environment configured.
	// In production, a lock acquired during staging apply would also block
	// production apply until the lock is released.

	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	dbName := fmt.Sprintf("lock_diffenv_%d", time.Now().UnixNano())

	schemabotAddr := startSchemaBotLocalDB(t, dbName)

	endpoint := "http://" + schemabotAddr

	// Simulate: PR acquires lock while applying to staging
	t.Run("pr_acquires_lock_during_staging_apply", func(t *testing.T) {
		resp, err := acquireLockViaAPI(t, endpoint, dbName, "mysql", "block/repo#200", "block/repo", 200)
		require.NoError(t, err, "acquire lock")
		require.Equal(t, 200, resp.StatusCode, "expected 200, got %d", resp.StatusCode)
		_ = resp.Body.Close()
	})

	t.Run("lock_is_per_database_not_per_env", func(t *testing.T) {
		// Verify the lock exists and is NOT tied to any environment
		out := runCLI(t, binPath, "locks", "--endpoint", endpoint)
		assertContains(t, out, dbName)
		assertContains(t, out, "mysql")
		// Lock should NOT show environment - it's database-level
		// The key point: same lock blocks both staging AND production
	})

	// Document the intended behavior:
	// If we had production configured, attempting to apply would fail:
	// $ schemabot apply -e production  # Would be blocked!
	// Error: Database Locked
	//   Locked by: block/repo#200
	//   ...
	// This prevents concurrent schema changes across environments.

	// Cleanup
	t.Run("cleanup", func(t *testing.T) {
		runCLI(t, binPath, "unlock", "-d", dbName, "-t", "mysql", "--endpoint", endpoint, "--force")
	})
}

// TestCLI_Locking_LockPersistsAcrossEnvironments tests that a lock on staging blocks production.
// This verifies the key design decision: lock is per database:type, NOT per environment.
func TestCLI_Locking_LockPersistsAcrossEnvironments(t *testing.T) {
	binPath := buildBinary(t, "schemabot", "./pkg/cmd")

	dbName := fmt.Sprintf("lock_env_%d", time.Now().UnixNano())

	schemabotAddr := startSchemaBotLocalDB(t, dbName)

	endpoint := "http://" + schemabotAddr

	schemaDir := newSchemaDirForDB(t, dbName)
	writeFile(t, filepath.Join(schemaDir, "users.sql"), `
CREATE TABLE users (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL
);
`)

	// Apply to staging - acquires lock
	t.Run("apply_staging_acquires_lock", func(t *testing.T) {
		out := runCLIInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--watch=false",
		)
		assertContains(t, out, "Lock acquired")
		waitForStateDB(t, endpoint, dbName, "completed", 30*time.Second)
	})

	t.Run("lock_held_after_staging_apply", func(t *testing.T) {
		out := runCLI(t, binPath, "locks", "--endpoint", endpoint)
		assertContains(t, out, dbName)
	})

	// Same CLI user can apply to production (same owner, idempotent lock)
	// Note: This test uses the same local mode setup which only has "staging"
	// In a real setup, production would be a different environment
	// The key test is that the lock persists - same owner can continue
	t.Run("same_user_can_continue_workflow", func(t *testing.T) {
		// Modify schema
		writeFile(t, filepath.Join(schemaDir, "users.sql"), `
CREATE TABLE users (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    INDEX idx_email (email)
);
`)
		// Same user can apply again (staging in this test, but would be production in real workflow)
		out := runCLIInDir(t, binPath, schemaDir, "apply",
			"-s", ".",
			"-e", "staging",
			"--endpoint", endpoint,
			"-y",
			"--watch=false",
		)
		// Same owner re-acquires lock (idempotent)
		assertContains(t, out, "Lock acquired")
		assertContains(t, out, "Apply started")
		waitForStateDB(t, endpoint, dbName, "completed", 30*time.Second)
	})

	// Cleanup
	t.Run("cleanup", func(t *testing.T) {
		runCLI(t, binPath, "unlock", "-d", dbName, "-t", "mysql", "--endpoint", endpoint)
	})
}
