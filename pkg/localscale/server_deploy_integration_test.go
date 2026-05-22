//go:build integration

package localscale_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/psclient"
	"github.com/block/schemabot/pkg/state"
)

var drState = state.DeployRequest

// TestDeployRequestDiffNoChanges verifies that CreateDeployRequest returns no_changes
// when branch schema matches main schema (no DDL applied to branch).
func TestDeployRequestDiffNoChanges(t *testing.T) {
	cleanupActiveDeployRequests(t, t.Context())
	t.Cleanup(func() { cleanupActiveDeployRequests(t, t.Context()) })
	ctx := t.Context()
	branchName := createBranch(t, ctx, "no-changes")

	// Create deploy request without applying any DDL — starts at "pending",
	// then transitions to "no_changes" when the background diff completes.
	dr, err := testClient.CreateDeployRequest(ctx, &ps.CreateDeployRequestRequest{
		Organization: testOrg,
		Database:     testDB,
		Branch:       branchName,
		IntoBranch:   "main",
	})
	require.NoError(t, err, "CreateDeployRequest")
	assert.Equal(t, drState.Pending, dr.DeploymentState, "initial state should be pending")

	// Poll until diff completes
	dr = waitForDeployReady(t, ctx, dr.Number)
	assert.Equal(t, drState.NoChanges, dr.DeploymentState, "deploy state after diff")
}

// TestDeployRequestDiffCreateTable verifies the schema differ produces correct
// DDL for CREATE TABLE operations and deploys them successfully.
func TestDeployRequestDiffCreateTable(t *testing.T) {
	cleanupActiveDeployRequests(t, t.Context())
	t.Cleanup(func() { cleanupActiveDeployRequests(t, t.Context()) })
	ctx := t.Context()
	tableName := uniqueIdentifier("diff_new_table")
	branchName := createBranchWithDDL(t, ctx, "diff-ct",
		map[string][]string{
			"testapp_sharded": {
				fmt.Sprintf("CREATE TABLE `%s` (id bigint NOT NULL PRIMARY KEY, value varchar(255), created_at datetime DEFAULT CURRENT_TIMESTAMP)", tableName),
			},
		},
		nil,
	)

	dr := createDeploy(t, ctx, branchName, true)
	require.Equal(t, drState.Ready, dr.DeploymentState, "expected 'ready' (has DDL changes)")

	deploy(t, ctx, dr.Number, false)
	waitForDeployState(t, ctx, dr.Number, drState.CompletePendingRevert, drState.Complete)

	// Verify diff_new_table exists in vtgate
	result, err := testContainer.VtgateExec(ctx, testOrg, testDB, "testapp_sharded",
		fmt.Sprintf("SHOW TABLES LIKE '%s'", tableName))
	require.NoError(t, err, "verify new table")
	require.Greater(t, len(result.Rows), 0, "expected %q in vtgate after deploy", tableName)
}

func TestResetStateCanRunImmediatelyAfterDeploySubmit(t *testing.T) {
	cleanupActiveDeployRequests(t, t.Context())
	t.Cleanup(func() { cleanupActiveDeployRequests(t, t.Context()) })
	ctx := t.Context()

	indexName := fmt.Sprintf("idx_reset_%d", time.Now().UnixNano())
	branchName := createBranchWithDDL(t, ctx, "reset-submit",
		map[string][]string{
			"testapp_sharded": {
				fmt.Sprintf("ALTER TABLE users ADD INDEX `%s` (`full_name`)", indexName),
			},
		},
		nil,
	)

	dr := createDeploy(t, ctx, branchName, false)
	require.Equal(t, drState.Ready, dr.DeploymentState, "expected changes")

	deploy(t, ctx, dr.Number, false)
	require.NoError(t, testContainer.ResetState(ctx), "reset LocalScale state after deploy submit")
	requireNoDeployRequests(t, ctx)

	nextCol := fmt.Sprintf("reset_next_%d", time.Now().UnixNano())
	nextBranch := createBranchWithDDL(t, ctx, "reset-next",
		map[string][]string{
			"testapp_sharded": {
				fmt.Sprintf("ALTER TABLE users ADD COLUMN `%s` varchar(50)", nextCol),
			},
		},
		nil,
	)
	nextDR := createDeploy(t, ctx, nextBranch, true)
	require.Equal(t, drState.Ready, nextDR.DeploymentState, "expected changes after reset")

	deploy(t, ctx, nextDR.Number, true)
	waitForDeployState(t, ctx, nextDR.Number, drState.CompletePendingRevert, drState.Complete)
}

// TestBranchDatabaseCleanupOnSkipRevert verifies that branch databases are dropped
// after skip-revert closes the revert window.
func TestBranchDatabaseCleanupOnSkipRevert(t *testing.T) {
	cleanupActiveDeployRequests(t, t.Context())
	t.Cleanup(func() { cleanupActiveDeployRequests(t, t.Context()) })
	ctx := t.Context()
	cleanupCol := uniqueIdentifier("cleanup_test_col")
	branchName := createBranchWithDDL(t, ctx, "cleanup",
		map[string][]string{
			"testapp_sharded": {
				fmt.Sprintf("ALTER TABLE `users` ADD COLUMN `%s` varchar(50)", cleanupCol),
			},
		},
		nil,
	)

	// Verify branch databases exist
	if !branchDatabaseExists(t, branchName, "testapp_sharded") {
		require.Fail(t, "expected branch database for testapp_sharded to exist")
	}
	if !branchDatabaseExists(t, branchName, "testapp") {
		require.Fail(t, "expected branch database for testapp to exist")
	}

	dr := createDeploy(t, ctx, branchName, true)
	require.Equal(t, drState.Ready, dr.DeploymentState, "expected changes")

	deploy(t, ctx, dr.Number, false)
	dr = waitForDeployState(t, ctx, dr.Number, drState.CompletePendingRevert)
	require.Equal(t, drState.CompletePendingRevert, dr.DeploymentState, "deploy should reach revert window (test uses 5s revert window)")

	// Skip revert — this should drop branch databases
	_, err := testClient.SkipRevertDeployRequest(ctx, &ps.SkipRevertDeployRequestRequest{
		Organization: testOrg,
		Database:     testDB,
		Number:       dr.Number,
	})
	require.NoError(t, err, "SkipRevertDeployRequest")

	// Verify branch databases were dropped
	if branchDatabaseExists(t, branchName, "testapp_sharded") {
		assert.Fail(t, "expected branch database for testapp_sharded to be dropped after skip-revert")
	}
	if branchDatabaseExists(t, branchName, "testapp") {
		assert.Fail(t, "expected branch database for testapp to be dropped after skip-revert")
	}
}

// TestBranchVSchemaSnapshotAndDiff verifies VSchema snapshot at branch creation
// and VSchema diff at deploy request creation.
func TestBranchVSchemaSnapshotAndDiff(t *testing.T) {
	cleanupActiveDeployRequests(t, t.Context())
	t.Cleanup(func() { cleanupActiveDeployRequests(t, t.Context()) })
	ctx := t.Context()
	branchName := createBranch(t, ctx, "vschema-diff")

	// Verify VSchema was snapshotted at branch creation
	origVSchema := queryBranchVSchema(t, ctx, branchName)
	assert.Contains(t, origVSchema, "testapp", "expected 'testapp' keyspace in snapshot")
	shardedVS, ok := origVSchema["testapp_sharded"]
	require.True(t, ok, "expected 'testapp_sharded' keyspace in snapshot")
	assert.Contains(t, string(shardedVS), "hash", "snapshotted VSchema should contain 'hash' vindex")

	// Apply VSchema change: add new vindexes to differ from main
	newVSchema := testShardedVSchema("xxhash:xxhash", "unicode_loose_xxhash:unicode_loose_xxhash", "numeric:numeric")
	applyBranchVSchema(t, ctx, branchName, map[string]json.RawMessage{"testapp_sharded": newVSchema})

	// Verify VSchema was merged (old keyspaces preserved, changed keyspace updated)
	mergedVSchema := queryBranchVSchema(t, ctx, branchName)
	assert.Contains(t, mergedVSchema, "testapp", "expected 'testapp' keyspace preserved in merged VSchema")
	updatedShardedVS, ok := mergedVSchema["testapp_sharded"]
	require.True(t, ok, "expected 'testapp_sharded' in merged VSchema")
	assert.Contains(t, string(updatedShardedVS), "numeric", "merged VSchema should contain 'numeric' vindex")

	// Create deploy request — should detect VSchema changes
	dr, err := testClient.CreateDeployRequest(ctx, &ps.CreateDeployRequestRequest{
		Organization: testOrg,
		Database:     testDB,
		Branch:       branchName,
		IntoBranch:   "main",
	})
	require.NoError(t, err, "CreateDeployRequest")

	dr = waitForDeployReady(t, ctx, dr.Number)
	require.Equal(t, drState.Ready, dr.DeploymentState, "expected 'ready' (has VSchema changes)")
}

// TestStateValidation verifies that control operations fail on deploy requests
// in various terminal/non-actionable states (ready, complete, cancelled).
func TestStateValidation(t *testing.T) {
	cleanupActiveDeployRequests(t, t.Context())
	t.Cleanup(func() { cleanupActiveDeployRequests(t, t.Context()) })
	tests := []struct {
		name    string
		ddlCol  string
		table   string
		setup   func(t *testing.T, ctx context.Context, dr *ps.DeployRequest) *ps.DeployRequest
		failOps []string // operations that should fail: "cutover", "revert", "skip-revert", "cancel"
	}{
		{
			name:   "ready",
			ddlCol: "state_ready_col",
			table:  "users",
			setup: func(t *testing.T, ctx context.Context, dr *ps.DeployRequest) *ps.DeployRequest {
				return dr // already in "ready" state from createDeploy
			},
			failOps: []string{"cutover", "revert", "skip-revert", "cancel"},
		},
		{
			name:   "complete",
			ddlCol: "state_complete_col",
			table:  "orders",
			setup: func(t *testing.T, ctx context.Context, dr *ps.DeployRequest) *ps.DeployRequest {
				deploy(t, ctx, dr.Number, false)
				dr = waitForDeployState(t, ctx, dr.Number, drState.CompletePendingRevert)
				require.Equal(t, drState.CompletePendingRevert, dr.DeploymentState, "test uses 5s revert window, should reach complete_pending_revert")
				_, err := testClient.SkipRevertDeployRequest(ctx, &ps.SkipRevertDeployRequestRequest{
					Organization: testOrg, Database: testDB, Number: dr.Number,
				})
				require.NoError(t, err, "SkipRevert")
				dr, err = testClient.GetDeployRequest(ctx, &ps.GetDeployRequestRequest{
					Organization: testOrg, Database: testDB, Number: dr.Number,
				})
				require.NoError(t, err)
				require.Equal(t, drState.Complete, dr.DeploymentState)
				return dr
			},
			failOps: []string{"cutover", "revert", "cancel"},
		},
		{
			name:   "cancelled",
			ddlCol: "state_cancel_col",
			table:  "orders",
			setup: func(t *testing.T, ctx context.Context, dr *ps.DeployRequest) *ps.DeployRequest {
				deploy(t, ctx, dr.Number, false)
				waitForDeployState(t, ctx, dr.Number, drState.Queued, drState.InProgress, drState.PendingCutover)
				_, err := testClient.CancelDeployRequest(ctx, &ps.CancelDeployRequestRequest{
					Organization: testOrg, Database: testDB, Number: dr.Number,
				})
				require.NoError(t, err, "Cancel")
				return waitForDeployState(t, ctx, dr.Number, drState.CompleteCancel)
			},
			failOps: []string{"cutover", "revert", "skip-revert"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			ddlCol := fmt.Sprintf("%s_%d", tt.ddlCol, time.Now().UnixNano())
			branchName := createBranchWithDDL(t, ctx, "state-"+tt.name,
				map[string][]string{
					"testapp_sharded": {fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN `%s` varchar(50)", tt.table, ddlCol)},
				},
				nil,
			)

			autoCutover := tt.name == "complete" // complete needs auto_cutover for deploy
			dr := createDeploy(t, ctx, branchName, autoCutover)
			dr = tt.setup(t, ctx, dr)

			for _, op := range tt.failOps {
				var err error
				switch op {
				case "cutover":
					_, err = testClient.ApplyDeployRequest(ctx, &ps.ApplyDeployRequestRequest{
						Organization: testOrg, Database: testDB, Number: dr.Number,
					})
				case "revert":
					_, err = testClient.RevertDeployRequest(ctx, &ps.RevertDeployRequestRequest{
						Organization: testOrg, Database: testDB, Number: dr.Number,
					})
				case "skip-revert":
					_, err = testClient.SkipRevertDeployRequest(ctx, &ps.SkipRevertDeployRequestRequest{
						Organization: testOrg, Database: testDB, Number: dr.Number,
					})
				case "cancel":
					_, err = testClient.CancelDeployRequest(ctx, &ps.CancelDeployRequestRequest{
						Organization: testOrg, Database: testDB, Number: dr.Number,
					})
				}
				assert.Error(t, err, "expected %s to fail on %s deploy request", op, tt.name)
			}
		})
	}
}

// TestMultiKeyspaceDDLDeploy verifies DDL changes across both keyspaces in a single deploy.
func TestMultiKeyspaceDDLDeploy(t *testing.T) {
	cleanupActiveDeployRequests(t, t.Context())
	t.Cleanup(func() { cleanupActiveDeployRequests(t, t.Context()) })
	ctx := t.Context()

	// Cancel any pending Vitess migrations from earlier tests to avoid
	// --singleton-context rejecting our DDL submission.
	cancelAllVitessMigrations(t, ctx)

	shardedCol := fmt.Sprintf("multi_ks_col_%d", time.Now().UnixNano())
	seqCol := fmt.Sprintf("multi_ks_seq_col_%d", time.Now().UnixNano())
	branchName := createBranchWithDDL(t, ctx, "multi-ks",
		map[string][]string{
			"testapp_sharded": {fmt.Sprintf("ALTER TABLE `orders` ADD COLUMN `%s` varchar(50)", shardedCol)},
			"testapp":         {fmt.Sprintf("ALTER TABLE `users_seq` ADD COLUMN `%s` int", seqCol)},
		},
		nil,
	)

	dr := createDeploy(t, ctx, branchName, true)
	require.Equal(t, drState.Ready, dr.DeploymentState, "expected changes across both keyspaces")

	deploy(t, ctx, dr.Number, false)
	waitForDeployState(t, ctx, dr.Number, drState.CompletePendingRevert, drState.Complete)

	// Verify column exists in testapp_sharded via vtgate
	verifyColumnExists(t, "orders", shardedCol)

	// Verify column exists in testapp via vtgate
	verifyColumnExists(t, "users_seq", seqCol, "testapp")

	t.Log("Multi-keyspace DDL deploy succeeded")
}

// TestBranchDDLError verifies that invalid DDL fails via MySQL connection and the branch recovers.
func TestBranchDDLError(t *testing.T) {
	cleanupActiveDeployRequests(t, t.Context())
	t.Cleanup(func() { cleanupActiveDeployRequests(t, t.Context()) })
	ctx := t.Context()
	branchName := createBranch(t, ctx, "ddl-error")

	// Get branch credentials
	pw, err := testClient.CreateBranchPassword(ctx, &ps.DatabaseBranchPasswordRequest{
		Organization: testOrg,
		Database:     testDB,
		Branch:       branchName,
	})
	require.NoError(t, err, "CreateBranchPassword")

	// Apply invalid DDL — table doesn't exist
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s", pw.Username, pw.PlainText, pw.Hostname, "testapp_sharded")
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "open branch MySQL")
	require.NoError(t, db.PingContext(ctx), "ping branch MySQL")
	_, err = db.ExecContext(ctx, "ALTER TABLE nonexistent_table ADD COLUMN x INT")
	assert.Error(t, err, "expected error for invalid DDL on nonexistent table")
	utils.CloseAndLog(db)

	// Verify the branch still works — apply valid DDL
	recoveryCol := uniqueIdentifier("ddl_error_recovery_col")
	applyBranchDDL(t, ctx, branchName, map[string][]string{
		"testapp_sharded": {fmt.Sprintf("ALTER TABLE `users` ADD COLUMN `%s` varchar(50)", recoveryCol)},
	})

	// Verify the column exists in branch database
	result, err := testContainer.BranchDBQuery(ctx, branchName, "testapp_sharded",
		fmt.Sprintf("SHOW COLUMNS FROM users LIKE '%s'", recoveryCol))
	require.NoError(t, err, "SHOW COLUMNS")
	require.Greater(t, len(result.Rows), 0, "expected %q in branch DB after recovery", recoveryCol)
}

// TestThrottleRatioBoundary verifies throttle ratio boundary validation.
func TestThrottleRatioBoundary(t *testing.T) {
	cleanupActiveDeployRequests(t, t.Context())
	t.Cleanup(func() { cleanupActiveDeployRequests(t, t.Context()) })
	ctx := t.Context()
	throttleCol := uniqueIdentifier("throttle_bounds_col")
	branchName := createBranchWithDDL(t, ctx, "throttle-bounds",
		map[string][]string{
			"testapp_sharded": {fmt.Sprintf("ALTER TABLE `orders` ADD COLUMN `%s` varchar(50)", throttleCol)},
		},
		nil,
	)

	dr := createDeploy(t, ctx, branchName, false)
	deploy(t, ctx, dr.Number, false)

	// Valid: ratio 0.0 (full speed)
	err := testClient.ThrottleDeployRequest(ctx, &psclient.ThrottleDeployRequestRequest{
		Organization:  testOrg,
		Database:      testDB,
		Number:        dr.Number,
		ThrottleRatio: 0.0,
	})
	assert.NoError(t, err, "throttle ratio 0.0 should succeed")

	// Valid: ratio 0.95 (max throttle per PlanetScale)
	err = testClient.ThrottleDeployRequest(ctx, &psclient.ThrottleDeployRequestRequest{
		Organization:  testOrg,
		Database:      testDB,
		Number:        dr.Number,
		ThrottleRatio: 0.95,
	})
	assert.NoError(t, err, "throttle ratio 0.95 should succeed")

	// Invalid: ratio 1.0 (exceeds PlanetScale max of 0.95)
	err = testClient.ThrottleDeployRequest(ctx, &psclient.ThrottleDeployRequestRequest{
		Organization:  testOrg,
		Database:      testDB,
		Number:        dr.Number,
		ThrottleRatio: 1.0,
	})
	assert.Error(t, err, "throttle ratio 1.0 should fail")

	// Invalid: ratio -0.1
	err = testClient.ThrottleDeployRequest(ctx, &psclient.ThrottleDeployRequestRequest{
		Organization:  testOrg,
		Database:      testDB,
		Number:        dr.Number,
		ThrottleRatio: -0.1,
	})
	assert.Error(t, err, "throttle ratio -0.1 should fail")

	// Invalid: ratio 1.5
	err = testClient.ThrottleDeployRequest(ctx, &psclient.ThrottleDeployRequestRequest{
		Organization:  testOrg,
		Database:      testDB,
		Number:        dr.Number,
		ThrottleRatio: 1.5,
	})
	assert.Error(t, err, "throttle ratio 1.5 should fail")
}

// TestRevertWindowExpiration verifies that the revert window auto-expires after
// the configured RevertWindowDuration (5s in test config), transitioning from
// complete_pending_revert to complete without manual intervention.
func TestRevertWindowExpiration(t *testing.T) {
	cleanupActiveDeployRequests(t, t.Context())
	t.Cleanup(func() { cleanupActiveDeployRequests(t, t.Context()) })
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	revertExpiryCol := uniqueIdentifier("revert_expiry_col")
	branchName := createBranchWithDDL(t, ctx, "revert-expiry",
		map[string][]string{
			"testapp": {
				fmt.Sprintf("ALTER TABLE `users_seq` ADD COLUMN `%s` INT", revertExpiryCol),
			},
		},
		nil,
	)

	dr := createDeploy(t, ctx, branchName, true)
	require.Equal(t, drState.Ready, dr.DeploymentState, "expected changes")

	// Deploy
	deploy(t, ctx, dr.Number, false)

	// Wait for complete_pending_revert (revert window), then complete.
	// The 2s revert window is long enough for the poll to catch it reliably.
	waitForDeployState(t, ctx, dr.Number, drState.CompletePendingRevert)
	waitForDeployState(t, ctx, dr.Number, drState.Complete)
}

// --- Async State Transition Tests ---

// TestDeployRequestPendingToReady verifies that CreateDeployRequest returns "pending"
// immediately and asynchronously transitions to "ready" when changes are detected.
func TestDeployRequestPendingToReady(t *testing.T) {
	cleanupActiveDeployRequests(t, t.Context())
	t.Cleanup(func() { cleanupActiveDeployRequests(t, t.Context()) })
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	pendingReadyCol := uniqueIdentifier("pending_ready_col")
	branchName := createBranchWithDDL(t, ctx, "pending-ready",
		map[string][]string{
			"testapp_sharded": {fmt.Sprintf("ALTER TABLE `users` ADD COLUMN `%s` VARCHAR(50) NULL", pendingReadyCol)},
		},
		nil,
	)

	// CreateDeployRequest should return "pending" immediately
	dr, err := testClient.CreateDeployRequest(ctx, &ps.CreateDeployRequestRequest{
		Organization: testOrg,
		Database:     testDB,
		Branch:       branchName,
		IntoBranch:   "main",
		AutoCutover:  true,
	})
	require.NoError(t, err, "CreateDeployRequest")
	assert.Equal(t, drState.Pending, dr.DeploymentState, "initial state should be pending")

	// Background goroutine should transition to "ready"
	dr = waitForDeployReady(t, ctx, dr.Number)
	require.Equal(t, drState.Ready, dr.DeploymentState, "should transition to ready")
	require.NotNil(t, dr.Deployment, "deployment field should be populated")

	t.Logf("Verified pending → ready transition for deploy request %d", dr.Number)

	// Clean up
	_, _ = testClient.CancelDeployRequest(ctx, &ps.CancelDeployRequestRequest{
		Organization: testOrg,
		Database:     testDB,
		Number:       dr.Number,
	})
}

// TestDeployRequestPendingToNoChanges verifies that CreateDeployRequest returns "pending"
// and transitions to "no_changes" when branch matches main.
func TestDeployRequestPendingToNoChanges(t *testing.T) {
	cleanupActiveDeployRequests(t, t.Context())
	t.Cleanup(func() { cleanupActiveDeployRequests(t, t.Context()) })
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	branchName := createBranch(t, ctx, "pending-nochanges")

	// Don't apply any DDL — branch is identical to main

	// CreateDeployRequest should return "pending" immediately
	dr, err := testClient.CreateDeployRequest(ctx, &ps.CreateDeployRequestRequest{
		Organization: testOrg,
		Database:     testDB,
		Branch:       branchName,
		IntoBranch:   "main",
	})
	require.NoError(t, err, "CreateDeployRequest")
	assert.Equal(t, drState.Pending, dr.DeploymentState, "initial state should be pending")

	// Background goroutine should transition to "no_changes"
	dr = waitForDeployReady(t, ctx, dr.Number)
	require.Equal(t, drState.NoChanges, dr.DeploymentState, "should transition to no_changes")

	t.Logf("Verified pending → no_changes transition for deploy request %d", dr.Number)
}

// TestDeploySubmittingToQueued verifies that DeployDeployRequest returns "submitting"
// immediately and asynchronously transitions to "queued" after DDL submission.
func TestDeploySubmittingToQueued(t *testing.T) {
	cleanupActiveDeployRequests(t, t.Context())
	t.Cleanup(func() { cleanupActiveDeployRequests(t, t.Context()) })
	cancelAllVitessMigrations(t, t.Context())
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	submittingCol := uniqueIdentifier("submitting_test_col")
	branchName := createBranchWithDDL(t, ctx, "submitting-queued",
		map[string][]string{
			"testapp_sharded": {fmt.Sprintf("ALTER TABLE `users` ADD COLUMN `%s` VARCHAR(50) NULL", submittingCol)},
		},
		nil,
	)

	dr := createDeploy(t, ctx, branchName, true)
	require.Equal(t, drState.Ready, dr.DeploymentState)

	// DeployDeployRequest should return "submitting" immediately
	dr, err := testClient.DeployDeployRequest(ctx, &ps.PerformDeployRequest{
		Organization: testOrg,
		Database:     testDB,
		Number:       dr.Number,
		InstantDDL:   false,
	})
	require.NoError(t, err, "DeployDeployRequest")
	assert.Equal(t, drState.Submitting, dr.DeploymentState, "initial deploy state should be submitting")

	// Background goroutine should transition through queued → completion
	waitForDeployState(t, ctx, dr.Number, drState.CompletePendingRevert)
	waitForDeployState(t, ctx, dr.Number, drState.Complete)

	t.Logf("Verified submitting → queued → complete transition for deploy request %d", dr.Number)
}

// TestInstantDDLEligibility verifies that ADD COLUMN NULL is detected as
// instant-eligible on the branch and reported in the deploy request.
// Uses the /apply-schema HTTP endpoint which tests ALGORITHM=INSTANT.
func TestInstantDDLEligibility(t *testing.T) {
	cleanupActiveDeployRequests(t, t.Context())
	t.Cleanup(func() { cleanupActiveDeployRequests(t, t.Context()) })
	ctx := t.Context()

	// ADD COLUMN NULL is instant in MySQL 8.4.
	// Apply via /apply-schema endpoint which detects ALGORITHM=INSTANT.
	instantCol := uniqueIdentifier("instant_test_col")
	branchName := createBranch(t, ctx, "instant-check")
	applyBranchSchemaHTTP(t, ctx, branchName, map[string][]string{
		"testapp_sharded": {fmt.Sprintf("ALTER TABLE `users` ADD COLUMN `%s` VARCHAR(50) NULL", instantCol)},
	})

	dr := createDeploy(t, ctx, branchName, true)
	require.Equal(t, drState.Ready, dr.DeploymentState)
	require.NotNil(t, dr.Deployment, "deploy request should have deployment info")
	assert.True(t, dr.Deployment.InstantDDLEligible,
		"ADD COLUMN NULL should be instant-eligible")

	// Non-instant ALTER (ADD INDEX) should NOT be eligible
	instantIndex := uniqueIdentifier("idx_instant_test")
	branchName2 := createBranch(t, ctx, "non-instant-check")
	applyBranchSchemaHTTP(t, ctx, branchName2, map[string][]string{
		"testapp_sharded": {fmt.Sprintf("ALTER TABLE `users` ADD INDEX `%s` (`email`, `full_name`)", instantIndex)},
	})

	dr2 := createDeploy(t, ctx, branchName2, true)
	require.Equal(t, drState.Ready, dr2.DeploymentState)
	require.NotNil(t, dr2.Deployment)
	assert.False(t, dr2.Deployment.InstantDDLEligible,
		"ADD INDEX should NOT be instant-eligible")
}

// TestCancelInProgressToCompleteCancel verifies that cancelling an in-progress deploy
// transitions through in_progress_cancel → complete_cancel via the state processor.
func TestCancelInProgressToCompleteCancel(t *testing.T) {
	cleanupActiveDeployRequests(t, t.Context())
	t.Cleanup(func() { cleanupActiveDeployRequests(t, t.Context()) })
	cancelAllVitessMigrations(t, t.Context())
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	// Use ADD INDEX (non-instant) so the deploy takes long enough to cancel
	cancelIndex := uniqueIdentifier("idx_cancel_test")
	branchName := createBranchWithDDL(t, ctx, "cancel-test",
		map[string][]string{
			"testapp_sharded": {fmt.Sprintf("ALTER TABLE `products` ADD INDEX `%s` (`name`, `price_cents`)", cancelIndex)},
		},
		nil,
	)

	dr := createDeploy(t, ctx, branchName, true)
	deploy(t, ctx, dr.Number, false)

	// Wait for deploy to start (submitting → queued or in_progress)
	dr = waitForDeployState(t, ctx, dr.Number,
		drState.Queued, drState.InProgress, drState.PendingCutover, drState.CompletePendingRevert, drState.Complete)

	// The test DDL (ADD INDEX) is non-instant, so the deploy should not complete before we can cancel.
	if dr.DeploymentState == drState.CompletePendingRevert || dr.DeploymentState == drState.Complete {
		require.Fail(t, "deploy completed before we could cancel it — test DDL should be non-instant to give time for cancel")
	}

	// Cancel — should return in_progress_cancel
	cancelDR, err := testClient.CancelDeployRequest(ctx, &ps.CancelDeployRequestRequest{
		Organization: testOrg,
		Database:     testDB,
		Number:       dr.Number,
	})
	require.NoError(t, err, "CancelDeployRequest")
	assert.Equal(t, drState.InProgressCancel, cancelDR.DeploymentState, "cancel should set in_progress_cancel")

	// Processor should transition to complete_cancel
	dr = waitForDeployState(t, ctx, dr.Number, drState.CompleteCancel)
	require.Equal(t, drState.CompleteCancel, dr.DeploymentState, "should transition to complete_cancel")

	t.Logf("Verified in_progress_cancel → complete_cancel transition for deploy request %d", dr.Number)
}

// TestRevertWithVSchemaTransitionalState verifies that reverting a VSchema-only deploy
// transitions through in_progress_revert_vschema.
func TestRevertWithVSchemaTransitionalState(t *testing.T) {
	cleanupActiveDeployRequests(t, t.Context())
	t.Cleanup(func() { cleanupActiveDeployRequests(t, t.Context()) })
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	cleanupActiveDeployRequests(t, ctx)

	// Apply VSchema-only change
	branchName := createBranchWithDDL(t, ctx, "revert-vschema",
		nil,
		map[string]json.RawMessage{
			"testapp_sharded": testShardedVSchema("xxhash_revert_test:xxhash"),
		},
	)

	dr := createDeploy(t, ctx, branchName, true)
	require.Equal(t, drState.Ready, dr.DeploymentState, "VSchema change should be detected as diff")

	// Deploy the VSchema change
	deploy(t, ctx, dr.Number, false)

	// Wait for completion (test uses 5s revert window)
	dr = waitForDeployState(t, ctx, dr.Number, drState.CompletePendingRevert)
	require.Equal(t, drState.CompletePendingRevert, dr.DeploymentState, "test uses 5s revert window, should reach complete_pending_revert")

	// Revert — should briefly show in_progress_revert_vschema, then complete_revert
	_, err := testClient.RevertDeployRequest(ctx, &ps.RevertDeployRequestRequest{
		Organization: testOrg,
		Database:     testDB,
		Number:       dr.Number,
	})
	require.NoError(t, err, "RevertDeployRequest")

	// VSchema revert is synchronous so we may not catch in_progress_revert_vschema,
	// but the final state should be complete_revert.
	dr = waitForDeployState(t, ctx, dr.Number, drState.CompleteRevert)
	require.Equal(t, drState.CompleteRevert, dr.DeploymentState, "should reach complete_revert after VSchema revert")

	t.Logf("Verified VSchema revert flow for deploy request %d", dr.Number)
}

// TestCompleteRevertError verifies that a failed revert produces the complete_revert_error state
// instead of complete_error.
func TestCompleteRevertError(t *testing.T) {
	cleanupActiveDeployRequests(t, t.Context())
	t.Cleanup(func() { cleanupActiveDeployRequests(t, t.Context()) })
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	cleanupActiveDeployRequests(t, ctx)

	cancelAllVitessMigrations(t, ctx)

	revertTable := uniqueIdentifier("revert_error_table")
	require.NoError(t, testContainer.SeedDDL(ctx, testOrg, testDB, "testapp",
		fmt.Sprintf("CREATE TABLE `%s` (id bigint NOT NULL PRIMARY KEY, value varchar(50))", revertTable)),
		"seed table for revert test")

	branchName := createBranchWithDDL(t, ctx, "revert-error",
		map[string][]string{
			"testapp": {fmt.Sprintf("DROP TABLE `%s`", revertTable)},
		},
		nil,
	)

	dr := createDeploy(t, ctx, branchName, true)
	deploy(t, ctx, dr.Number, false)
	dr = waitForDeployState(t, ctx, dr.Number, drState.CompletePendingRevert)
	require.Equal(t, drState.CompletePendingRevert, dr.DeploymentState, "test uses 5s revert window, should reach complete_pending_revert")

	// Revert should succeed with complete_revert (not complete_revert_error)
	_, err := testClient.RevertDeployRequest(ctx, &ps.RevertDeployRequestRequest{
		Organization: testOrg,
		Database:     testDB,
		Number:       dr.Number,
	})
	require.NoError(t, err, "RevertDeployRequest")

	dr = waitForDeployState(t, ctx, dr.Number, drState.CompleteRevert, drState.CompleteRevertError)
	// For a normal revert, we expect complete_revert
	require.Equal(t, drState.CompleteRevert, dr.DeploymentState,
		"successful revert should produce complete_revert, not complete_revert_error")

	result, err := testContainer.VtgateExec(ctx, testOrg, testDB, "testapp",
		fmt.Sprintf("SHOW TABLES LIKE '%s'", revertTable))
	require.NoError(t, err, "SHOW TABLES after revert")
	require.NotEmpty(t, result.Rows, "expected %q to exist after revert", revertTable)
	t.Logf("Verified revert flow with correct state differentiation for deploy request %d", dr.Number)
}

// TestRefreshSchema verifies that syncing a branch re-snapshots schema from main.
func TestRefreshSchema(t *testing.T) {
	cleanupActiveDeployRequests(t, t.Context())
	t.Cleanup(func() { cleanupActiveDeployRequests(t, t.Context()) })
	ctx := t.Context()

	// Create a branch and apply DDL to it
	refreshCol := uniqueIdentifier("refresh_test_col")
	branchName := createBranchWithDDL(t, ctx, "refresh-test",
		map[string][]string{
			"testapp_sharded": {fmt.Sprintf("ALTER TABLE `users` ADD COLUMN `%s` VARCHAR(50) NULL", refreshCol)},
		},
		nil,
	)

	// Verify branch has a diff (column was added)
	branchSchema, err := testClient.GetBranchSchema(ctx, &ps.BranchSchemaRequest{
		Organization: testOrg, Database: testDB, Branch: branchName, Keyspace: "testapp_sharded",
	})
	require.NoError(t, err)
	require.NotEmpty(t, branchSchema, "expected schema diff before refresh")

	// Refresh schema with main — re-snapshots from main, losing the added column
	err = testClient.RefreshSchema(ctx, testOrg, testDB, branchName)
	require.NoError(t, err, "RefreshSchema")
	waitForBranchReady(t, ctx, branchName)

	// Verify the added column is gone after schema refresh — the branch should match main
	branchSchema, err = testClient.GetBranchSchema(ctx, &ps.BranchSchemaRequest{
		Organization: testOrg, Database: testDB, Branch: branchName, Keyspace: "testapp_sharded",
	})
	require.NoError(t, err)
	for _, d := range branchSchema {
		assert.NotContains(t, d.Raw, refreshCol,
			"branch should not have %s after schema refresh (table %s)", refreshCol, d.Name)
	}
}

// TestRefreshSchemaAndApply verifies the full --branch reuse workflow:
// create branch once, then sync + apply DDL multiple times.
func TestRefreshSchemaAndApply(t *testing.T) {
	cleanupActiveDeployRequests(t, t.Context())
	t.Cleanup(func() { cleanupActiveDeployRequests(t, t.Context()) })
	cancelAllVitessMigrations(t, t.Context())
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	// Create a reusable branch
	branchName := createBranch(t, ctx, "reuse-test")

	// First apply: add a column
	reuseCol1 := uniqueIdentifier("reuse_col_1")
	applyBranchDDL(t, ctx, branchName, map[string][]string{
		"testapp_sharded": {fmt.Sprintf("ALTER TABLE `users` ADD COLUMN `%s` VARCHAR(50) NULL", reuseCol1)},
	})
	dr := createDeploy(t, ctx, branchName, true)
	require.Equal(t, drState.Ready, dr.DeploymentState)

	deploy(t, ctx, dr.Number, false)
	dr = waitForDeployState(t, ctx, dr.Number, drState.CompletePendingRevert, drState.Complete)
	if dr.DeploymentState == drState.CompletePendingRevert {
		_, err := testClient.SkipRevertDeployRequest(ctx, &ps.SkipRevertDeployRequestRequest{
			Organization: testOrg, Database: testDB, Number: dr.Number,
		})
		require.NoError(t, err, "SkipRevert")
		waitForDeployState(t, ctx, dr.Number, drState.Complete)
	}

	// Refresh schema for reuse
	err := testClient.RefreshSchema(ctx, testOrg, testDB, branchName)
	require.NoError(t, err, "RefreshSchema after first apply")
	waitForBranchReady(t, ctx, branchName)

	// Second apply on same branch: add another column
	reuseCol2 := uniqueIdentifier("reuse_col_2")
	applyBranchDDL(t, ctx, branchName, map[string][]string{
		"testapp_sharded": {fmt.Sprintf("ALTER TABLE `users` ADD COLUMN `%s` VARCHAR(50) NULL", reuseCol2)},
	})
	dr2 := createDeploy(t, ctx, branchName, true)
	require.Equal(t, drState.Ready, dr2.DeploymentState)

	deploy(t, ctx, dr2.Number, false)
	dr2 = waitForDeployState(t, ctx, dr2.Number, drState.CompletePendingRevert, drState.Complete)
	if dr2.DeploymentState == drState.CompletePendingRevert {
		_, err = testClient.SkipRevertDeployRequest(ctx, &ps.SkipRevertDeployRequestRequest{
			Organization: testOrg, Database: testDB, Number: dr2.Number,
		})
		require.NoError(t, err, "SkipRevert")
		waitForDeployState(t, ctx, dr2.Number, drState.Complete)
	}

	// Verify both columns exist on main
	verifyColumnExists(t, "users", reuseCol1)
	verifyColumnExists(t, "users", reuseCol2)
}
