//go:build e2e

package grpc

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"net/http"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/block/schemabot/e2e/testutil"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Multi-deployment fan-out fixture.
//
// Scenario: a single (database, environment) — testapp/production — fans out to
// two deployments (eu, us), each backed by its own remote Tern over gRPC, with
// deployment_order [eu, us] and cutover_policy: barrier (see
// deploy/local/docker-compose.grpc-multideploy.yml and
// deploy/local/config/grpc-schemabot-multideploy.yaml).
//
// This file only verifies that the multi-deployment stack boots and that
// SchemaBot resolves both remote deployments — the foundation the ordered
// cutover acceptance test builds on.
//
// The fixture cannot boot until the server supports deployments maps with more
// than one entry, so these tests are gated behind E2E_MULTIDEPLOY=1 and are
// skipped in the standard gRPC e2e run. The make target
// test-e2e-grpc-multideploy sets the flag and stands up the multi-deployment
// stack.

func requireMultiDeploy(t *testing.T) {
	t.Helper()
	if os.Getenv("E2E_MULTIDEPLOY") != "1" {
		t.Skip("multi-deployment fixture gated behind E2E_MULTIDEPLOY=1 (requires server support for deployments maps with >1 entry)")
	}
}

func TestGRPCMultiDeploy_SchemaBot_Health(t *testing.T) {
	requireMultiDeploy(t)
	resp := grpcGet(t, "/health") //nolint:bodyclose // closed via utils.CloseAndLog
	defer utils.CloseAndLog(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var result map[string]string
	grpcDecodeJSON(t, resp, &result)
	assert.Equal(t, "ok", result["status"])
}

func TestGRPCMultiDeploy_TernHealth_BothDeployments(t *testing.T) {
	requireMultiDeploy(t)
	for _, deployment := range []string{"eu", "us"} {
		resp := grpcGet(t, "/tern-health/"+deployment+"/production") //nolint:bodyclose // closed via utils.CloseAndLog
		func() {
			defer utils.CloseAndLog(resp.Body)
			require.Equalf(t, http.StatusOK, resp.StatusCode, "tern-health for deployment %q", deployment)
			var result map[string]string
			grpcDecodeJSON(t, resp, &result)
			assert.Equalf(t, "ok", result["status"], "tern-health status for deployment %q", deployment)
		}()
	}
}

// TestGRPCMultiDeploy_Plan_FansOut verifies a plan against the fanned-out
// (testapp, production) environment succeeds, proving SchemaBot loaded the
// >1-deployment config and resolved both remote deployments.
func TestGRPCMultiDeploy_Plan_FansOut(t *testing.T) {
	requireMultiDeploy(t)
	tableName := uniqueGRPCTableName("md_fanout")
	plan := grpcPlan(t, "testapp", "production", map[string]string{
		tableName + ".sql": "CREATE TABLE " + tableName + " (\n" +
			"  id BIGINT UNSIGNED AUTO_INCREMENT,\n" +
			"  PRIMARY KEY (id)\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
	})
	require.Empty(t, plan.Errors, "plan errors")
	require.NotEmpty(t, plan.PlanID, "plan_id")
}

// orderedCutoverDeadline bounds the full multi-deployment ordered-cutover
// sequence: two concurrent full copy-swaps followed by two cutovers driven
// strictly in deployment_order. This exceeds the shared PollDeadline because it
// observes several phases across two remote deployments end to end.
const orderedCutoverDeadline = 5 * time.Minute

// multiDeployTernMySQLDSN returns the MySQL DSN for a deployment's remote Tern
// target database (the testapp schema each deployment fans out to).
func multiDeployTernMySQLDSN(t *testing.T, deployment string) string {
	t.Helper()
	var key string
	switch deployment {
	case "eu":
		key = "E2E_TERN_EU_MYSQL_DSN"
	case "us":
		key = "E2E_TERN_US_MYSQL_DSN"
	default:
		require.Failf(t, "unknown deployment", "%s", deployment)
	}
	dsn := os.Getenv(key)
	require.NotEmptyf(t, dsn, "%s environment variable not set", key)
	return dsn
}

// multiDeployCreateTestTable creates a table on one deployment's target MySQL
// and registers cleanup of the table and any Spirit shadow/checkpoint tables.
func multiDeployCreateTestTable(t *testing.T, deployment, tableName, ddl string) {
	t.Helper()
	dsn := multiDeployTernMySQLDSN(t, deployment)
	db, err := sql.Open("mysql", dsn)
	require.NoErrorf(t, err, "open tern mysql (%s)", deployment)
	// Defer close immediately so the handle is reclaimed even if the create
	// below fails a require.* assertion, and so close errors are logged.
	defer utils.CloseAndLog(db)
	_, err = db.ExecContext(t.Context(), ddl)
	require.NoErrorf(t, err, "create table %s on %s", tableName, deployment)

	t.Cleanup(func() {
		db2, err := sql.Open("mysql", dsn)
		if err != nil {
			t.Logf("cleanup: open tern mysql (%s): %v", deployment, err)
			return
		}
		defer utils.CloseAndLog(db2)
		// t.Context() is cancelled once the test finishes, so derive a
		// cancellation-immune context with a bounded timeout for cleanup DDL.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()
		for _, suffix := range []string{"_new", "_old", "_chkpnt", ""} {
			name := tableName
			if suffix != "" {
				name = "_" + tableName + suffix
			}
			if _, err := db2.ExecContext(ctx, "DROP TABLE IF EXISTS `"+name+"`"); err != nil {
				t.Logf("cleanup: drop table %s on %s: %v", name, deployment, err)
			}
		}
	})
}

// multiDeploySeedRows inserts rows into a table on one deployment's target MySQL
// using an efficient SQL cross-join, mirroring grpcSeedRows.
func multiDeploySeedRows(t *testing.T, deployment, tableName, columns, valueTemplate string, rowCount int) {
	t.Helper()
	dsn := multiDeployTernMySQLDSN(t, deployment)
	db, err := sql.Open("mysql", dsn)
	require.NoErrorf(t, err, "open tern mysql (%s)", deployment)
	defer utils.CloseAndLog(db)

	seqGen := `(SELECT @row := @row + 1 as seq FROM
		(SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a`
	if rowCount >= 100 {
		seqGen += `, (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b`
	}
	if rowCount >= 1000 {
		seqGen += `, (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c`
	}
	if rowCount >= 10000 {
		seqGen += `, (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) d`
	}
	seqGen += `, (SELECT @row := 0) r) nums`

	query := fmt.Sprintf(`INSERT INTO %s (%s) SELECT %s FROM %s LIMIT %d`,
		tableName, columns, valueTemplate, seqGen, rowCount)
	_, err = db.ExecContext(t.Context(), query)
	require.NoErrorf(t, err, "seed %s on %s", tableName, deployment)
}

// multiDeployOperationStates returns the per-deployment operation rows of a
// fan-out apply, keyed by deployment name.
func multiDeployOperationStates(t *testing.T, applyID string) map[string]grpcOperationProgress {
	t.Helper()
	prog := grpcProgressByApplyID(t, applyID)
	byDeployment := make(map[string]grpcOperationProgress, len(prog.Operations))
	for _, op := range prog.Operations {
		byDeployment[op.Deployment] = op
	}
	return byDeployment
}

// multiDeployOps fetches per-deployment operation states and fails fast if
// either expected deployment is missing. An apply commits all of its
// apply_operations rows in one transaction, so once the apply is accepted a
// missing row is a real defect — not a transient — and the test should fail
// immediately with a clear message rather than poll for minutes against empty
// state strings.
func multiDeployOps(t *testing.T, applyID string, deployments ...string) map[string]grpcOperationProgress {
	t.Helper()
	ops := multiDeployOperationStates(t, applyID)
	for _, d := range deployments {
		_, ok := ops[d]
		require.Truef(t, ok, "progress missing operation for deployment %q; present: %v",
			d, slices.Sorted(maps.Keys(ops)))
	}
	return ops
}

// failedApplyState reports whether a state is a failed terminal apply state.
func failedApplyState(s string) bool {
	return state.IsState(s, state.Apply.Failed) || state.IsState(s, state.Apply.FailedRetryable)
}

// TestGRPCMultiDeploy_OrderedCutover verifies that a single fan-out apply over
// two deployments honours barrier-policy ordered cutover.
//
// Scenario: testapp/production fans out to deployments [eu, us] with
// deployment_order [eu, us] and cutover_policy: barrier. Both deployments copy
// concurrently and park at the cutover barrier; the operator then drives cutover
// strictly in order — eu cuts over and completes before us is allowed to cut
// over. The later deployment must never reach a terminal completed state ahead
// of the earlier one, and the whole apply settles to completed.
func TestGRPCMultiDeploy_OrderedCutover(t *testing.T) {
	requireMultiDeploy(t)

	const (
		database = "testapp"
		env      = "production"
	)
	// Matches deployment_order in grpc-schemabot-multideploy.yaml.
	first, second := "eu", "us"

	tableName := uniqueGRPCTableName("md_ordered")
	createDDL := fmt.Sprintf(
		"CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, data TEXT)", tableName)

	// Seed both deployments so each runs a full copy-swap (not instant DDL),
	// giving the barrier a real window in which the later deployment parks.
	for _, d := range []string{first, second} {
		multiDeployCreateTestTable(t, d, tableName, createDDL)
		multiDeploySeedRows(t, d, tableName, "name, data",
			"CONCAT('user_', seq), REPEAT('x', 200)", 10000)
	}

	grpcEnsureNoActiveChange(t, database, env)

	// Widen the primary key from INT to BIGINT to force a full table copy on
	// every deployment.
	plan := grpcPlan(t, database, env, map[string]string{
		tableName + ".sql": fmt.Sprintf(
			"CREATE TABLE %s (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, data TEXT);", tableName),
	})
	require.Empty(t, plan.Errors, "plan errors: %v", plan.Errors)
	require.NotEmpty(t, plan.PlanID, "plan_id")

	apply := grpcApply(t, plan.PlanID, env, nil)
	require.True(t, apply.Accepted, "apply not accepted: %s", apply.ErrorMessage)

	var (
		sawSecondParked      bool // second deployment reached waiting_for_cutover
		secondParkedPreFirst bool // second parked while first had not yet completed
	)
	testutil.Poll(t, orderedCutoverDeadline, testutil.PollInterval,
		func() bool {
			ops := multiDeployOps(t, apply.ApplyID, first, second)
			firstOp, secondOp := ops[first], ops[second]

			// Happy path: neither deployment should fail.
			require.Falsef(t, failedApplyState(firstOp.State), "%s operation failed: %s", first, firstOp.ErrorMessage)
			require.Falsef(t, failedApplyState(secondOp.State), "%s operation failed: %s", second, secondOp.ErrorMessage)

			if state.IsState(secondOp.State, state.Apply.WaitingForCutover) {
				sawSecondParked = true
				if !state.IsState(firstOp.State, state.Apply.Completed) {
					secondParkedPreFirst = true
				}
			}

			// Barrier ordering invariant: the later deployment must never
			// complete before the earlier one.
			if state.IsState(secondOp.State, state.Apply.Completed) {
				require.Truef(t, state.IsState(firstOp.State, state.Apply.Completed),
					"barrier violated: %s completed while %s was %q", second, first, firstOp.State)
			}

			return state.IsState(firstOp.State, state.Apply.Completed) &&
				state.IsState(secondOp.State, state.Apply.Completed)
		},
		func() string {
			ops := multiDeployOperationStates(t, apply.ApplyID)
			return fmt.Sprintf("waiting for both deployments completed; %s=%q %s=%q",
				first, ops[first].State, second, ops[second].State)
		},
	)

	// Barrier engaged: the later deployment parked at the cutover barrier, and
	// did so before the earlier deployment completed (ordered cutover).
	require.Truef(t, sawSecondParked,
		"expected %s to park at waiting_for_cutover under barrier policy", second)
	assert.Truef(t, secondParkedPreFirst,
		"expected %s to be parked at the barrier before %s completed", second, first)

	// Completion timestamps confirm the earlier deployment cut over first.
	final := multiDeployOperationStates(t, apply.ApplyID)
	firstDone, err := time.Parse(time.RFC3339, final[first].CompletedAt)
	require.NoErrorf(t, err, "parse %s completed_at %q", first, final[first].CompletedAt)
	secondDone, err := time.Parse(time.RFC3339, final[second].CompletedAt)
	require.NoErrorf(t, err, "parse %s completed_at %q", second, final[second].CompletedAt)
	assert.Falsef(t, secondDone.Before(firstDone),
		"expected %s (completed %s) to cut over no earlier than %s (completed %s)",
		second, final[second].CompletedAt, first, final[first].CompletedAt)

	grpcEnsureNoActiveChange(t, database, env)
}

// TestGRPCMultiDeploy_FailureHaltsRollout verifies that a deployment failure
// halts a barrier-policy fan-out rollout.
//
// Scenario: testapp/production fans out to [eu, us] with deployment_order
// [eu, us] and cutover_policy: barrier. The earlier deployment (eu) is seeded
// with duplicate values so adding a UNIQUE key fails during its copy; the later
// deployment (us) has unique values and would succeed on its own. Because eu —
// first in the cutover order — fails, the rollout halts: us must never cut over
// to completed, and the apply settles to a failed terminal state.
func TestGRPCMultiDeploy_FailureHaltsRollout(t *testing.T) {
	requireMultiDeploy(t)

	const (
		database = "testapp"
		env      = "production"
	)
	// Matches deployment_order in grpc-schemabot-multideploy.yaml.
	first, second := "eu", "us"

	tableName := uniqueGRPCTableName("md_halt")
	createDDL := fmt.Sprintf(
		"CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, data TEXT)", tableName)
	for _, d := range []string{first, second} {
		multiDeployCreateTestTable(t, d, tableName, createDDL)
	}
	// The earlier deployment gets identical names so adding UNIQUE(name) fails
	// during its copy; the later deployment gets unique names so it would
	// succeed if the rollout were allowed to proceed past the failure.
	multiDeploySeedRows(t, first, tableName, "name, data", "'dup', REPEAT('x', 200)", 10000)
	multiDeploySeedRows(t, second, tableName, "name, data", "CONCAT('user_', seq), REPEAT('x', 200)", 10000)

	grpcEnsureNoActiveChange(t, database, env)

	// Add a UNIQUE index on name; the earlier deployment's duplicate data makes
	// its copy fail.
	plan := grpcPlan(t, database, env, map[string]string{
		tableName + ".sql": fmt.Sprintf(
			"CREATE TABLE %s (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, data TEXT, UNIQUE KEY uniq_name (name));", tableName),
	})
	require.Empty(t, plan.Errors, "plan errors: %v", plan.Errors)
	require.NotEmpty(t, plan.PlanID, "plan_id")

	apply := grpcApply(t, plan.PlanID, env, nil)
	require.True(t, apply.Accepted, "apply not accepted: %s", apply.ErrorMessage)

	// Wait for the earlier deployment to fail, asserting throughout that the
	// later deployment never reaches completed — the halt policy must keep it
	// from cutting over once a sibling has failed.
	testutil.Poll(t, orderedCutoverDeadline, testutil.PollInterval,
		func() bool {
			ops := multiDeployOps(t, apply.ApplyID, first, second)
			require.Falsef(t, state.IsState(ops[second].State, state.Apply.Completed),
				"halt violated: %s completed despite %s failing (state %q)", second, first, ops[first].State)
			return failedApplyState(ops[first].State)
		},
		func() string {
			ops := multiDeployOperationStates(t, apply.ApplyID)
			return fmt.Sprintf("waiting for %s to fail; %s=%q %s=%q",
				first, first, ops[first].State, second, ops[second].State)
		},
	)

	// The earlier deployment failed; the rollout must have halted — the later
	// deployment is not completed and the apply itself is failed.
	final := multiDeployOperationStates(t, apply.ApplyID)
	assert.Truef(t, failedApplyState(final[first].State), "%s should be failed, was %q", first, final[first].State)
	assert.Falsef(t, state.IsState(final[second].State, state.Apply.Completed),
		"%s must not complete after %s failed, was %q", second, first, final[second].State)
	prog := grpcProgressByApplyID(t, apply.ApplyID)
	assert.Truef(t, failedApplyState(prog.State), "aggregate apply state should be failed, was %q", prog.State)

	grpcEnsureNoActiveChange(t, database, env)
}
