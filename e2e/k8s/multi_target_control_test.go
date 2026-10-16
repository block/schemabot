//go:build e2e

package k8s

import (
	"strings"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/e2e/testutil"
	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/state"
)

// mtDataPlaneInstance is the Helm release of the multi-target data plane.
const mtDataPlaneInstance = "data-plane-mt"

// mtCopyRows sizes orders so its index add is still copying when a control
// operation lands.
const mtCopyRows = 600_000

// startCopyingRollout seeds every shard, applies an index add on orders and a
// column add on users, and returns once some target is copying orders.
func startCopyingRollout(t *testing.T) (endpoint, applyID string, dsns []string) {
	t.Helper()
	endpoint = testutil.Endpoint(t)
	dsns = shardDSNs(t, mtShardApp)
	resetSchema(t, dsns)
	cleanupRolloutState(t)
	execOn(t, dsns, mtUsersV1, mtOrdersV1)
	seedOrders(t, dsns, mtCopyRows)

	plan := planRollout(t, endpoint, mtShardApp, shardFiles(mtShardApp, map[string]string{"users": mtUsersV2, "orders": mtOrdersV2}))
	applyID = applyRollout(t, endpoint, plan.PlanID, nil)
	waitForOrdersCopying(t, endpoint, applyID)
	return endpoint, applyID, dsns
}

// waitForOrdersCopying waits until some target reports orders rows copied
// short of the total.
func waitForOrdersCopying(t *testing.T, endpoint, applyID string) {
	t.Helper()
	var last *apitypes.ProgressResponse
	testutil.Poll(t, 2*time.Minute, testutil.PollInterval,
		func() bool {
			p, err := testutil.FetchProgress(endpoint, applyID)
			if err != nil {
				return false
			}
			last = p
			require.False(t, state.IsTerminalApplyState(p.State), "apply finished before the copy was observed:\n%s", describeProgress(p))
			for _, tbl := range p.Tables {
				if tbl.TableName == "orders" && tbl.RowsCopied > 0 && tbl.RowsCopied < tbl.RowsTotal {
					return true
				}
			}
			return false
		},
		func() string { return "orders never observed copying:\n" + describeProgress(last) },
	)
}

// shadowTables lists Spirit's leftover tables for table on a shard.
func shadowTables(t *testing.T, dsn, table string) []string {
	t.Helper()
	db := testutil.OpenMySQL(t, dsn)
	rows, err := db.QueryContext(t.Context(),
		"SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name LIKE ?", "\\_"+table+"\\_%")
	require.NoError(t, err)
	defer utils.CloseAndLog(rows)
	var out []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		out = append(out, name)
	}
	require.NoError(t, rows.Err())
	return out
}

// A stop lands while orders is copying on the shards. The apply reports
// stopped, a start resumes it, and the rollout completes on every shard.
func TestK8sMultiTarget_StopStartMidCopyCompletes(t *testing.T) {
	ep, applyID, dsns := startCopyingRollout(t)

	stop, err := client.CallStopAPI(ep, mtEnvironment, applyID)
	require.NoError(t, err, "stop")
	require.True(t, stop.Accepted, "stop not accepted: %s", stop.ErrorMessage)
	p := waitForStates(t, ep, applyID, 2*time.Minute, state.Apply.Stopped)
	t.Log("after stop:\n" + describeProgress(p))

	start, err := client.CallStartAPI(ep, mtEnvironment, applyID)
	require.NoError(t, err, "start")
	require.True(t, start.Accepted, "start not accepted: %s %s", start.ErrorCode, start.ErrorMessage)

	p = requireCompleted(t, ep, applyID)
	t.Log("after start:\n" + describeProgress(p))
	for i, dsn := range dsns {
		assert.True(t, indexOn(t, dsn, "orders", "idx_user_id"), "orders.idx_user_id on %s", mtTargets[i])
		assert.True(t, testutil.ColumnExists(t, dsn, "users", "email"), "users.email on %s", mtTargets[i])
	}
}

// Shard 1 is still copying orders while shard 2 has finished its copy and holds
// at the cutover barrier for it. A stop reaches every target: the copying one,
// the parked one, and shard 3 wherever it is. The apply reports stopped, and a
// start resumes every target through the barrier to completion.
func TestK8sMultiTarget_StopWithTargetsParkedAtBarrierThenStartCompletes(t *testing.T) {
	ep := testutil.Endpoint(t)
	dsns := shardDSNs(t, mtShardApp)
	resetSchema(t, dsns)
	cleanupRolloutState(t)
	execOn(t, dsns, mtUsersV1, mtOrdersV1)
	seedOrders(t, dsns[:1], 3*mtCopyRows)
	seedOrders(t, dsns[1:], 1_000)

	plan := planRollout(t, ep, mtShardApp, shardFiles(mtShardApp, map[string]string{"users": mtUsersV2, "orders": mtOrdersV2}))
	applyID := applyRollout(t, ep, plan.PlanID, nil)

	var parked *apitypes.ProgressResponse
	testutil.Poll(t, 2*time.Minute, testutil.PollInterval,
		func() bool {
			p, err := testutil.FetchProgress(ep, applyID)
			if err != nil {
				return false
			}
			parked = p
			require.False(t, state.IsTerminalApplyState(p.State), "apply finished before shard 2 was observed parked:\n%s", describeProgress(p))
			ops := operationsByStep(p)[1]
			return operationStateOn(ops, mtTargets[0]) == state.ApplyOperation.Running &&
				operationStateOn(ops, mtTargets[1]) == state.ApplyOperation.WaitingForCutover
		},
		func() string { return "shard 2 never parked while shard 1 copied:\n" + describeProgress(parked) },
	)
	t.Log("parked:\n" + describeProgress(parked))

	stop, err := client.CallStopAPI(ep, mtEnvironment, applyID)
	require.NoError(t, err, "stop")
	require.True(t, stop.Accepted, "stop not accepted: %s", stop.ErrorMessage)
	p := waitForStates(t, ep, applyID, 2*time.Minute, state.Apply.Stopped)
	t.Log("after stop:\n" + describeProgress(p))
	for _, op := range operationsByStep(p)[1] {
		assert.Equal(t, state.ApplyOperation.Stopped, state.NormalizeState(op.State), "orders on %s is stopped", op.Target)
	}
	for i, dsn := range dsns {
		assert.False(t, indexOn(t, dsn, "orders", "idx_user_id"), "no target cut over while stopped: %s", mtTargets[i])
	}

	start, err := client.CallStartAPI(ep, mtEnvironment, applyID)
	require.NoError(t, err, "start")
	require.True(t, start.Accepted, "start not accepted: %s %s", start.ErrorCode, start.ErrorMessage)

	p = requireCompleted(t, ep, applyID)
	t.Log("after start:\n" + describeProgress(p))
	for i, dsn := range dsns {
		assert.True(t, indexOn(t, dsn, "orders", "idx_user_id"), "orders.idx_user_id on %s", mtTargets[i])
		assert.True(t, testutil.ColumnExists(t, dsn, "users", "email"), "users.email on %s", mtTargets[i])
	}
}

// A cancel lands while orders is copying. The apply ends cancelled, no target
// starts the next table, and no shard keeps a shadow copy of orders.
func TestK8sMultiTarget_CancelMidCopyStopsRollout(t *testing.T) {
	ep, applyID, dsns := startCopyingRollout(t)

	cancelResp, err := client.CallCancelAPI(ep, mtEnvironment, applyID)
	require.NoError(t, err, "cancel")
	require.True(t, cancelResp.Accepted, "cancel not accepted: %s", cancelResp.ErrorMessage)

	p := waitForSettled(t, ep, applyID, 2*time.Minute, nil)
	t.Log(describeProgress(p))
	require.Equal(t, state.Apply.Cancelled, p.State, describeProgress(p))

	for _, op := range p.Operations {
		assert.True(t, state.IsTerminalApplyState(op.State), "operation %s is terminal after cancel, got %s", op.OperationKey, op.State)
	}
	for i, dsn := range dsns {
		assert.False(t, indexOn(t, dsn, "orders", "idx_user_id"), "cancelled index is absent on %s", mtTargets[i])
		assert.False(t, testutil.ColumnExists(t, dsn, "users", "email"), "the table after orders never started on %s", mtTargets[i])
	}
	// Cleanup of the cancelled copy is asynchronous; give it a bounded window.
	testutil.Poll(t, testutil.PollDeadline, time.Second,
		func() bool {
			for _, dsn := range dsns {
				if len(shadowTables(t, dsn, "orders")) > 0 {
					return false
				}
			}
			return true
		},
		func() string {
			var b strings.Builder
			for i, dsn := range dsns {
				b.WriteString(mtTargets[i] + ": " + strings.Join(shadowTables(t, dsn, "orders"), ",") + "\n")
			}
			return "shadow tables left after cancel:\n" + b.String()
		},
	)
}

// Both data plane pods are deleted while orders copies on the shards. The
// replacement pods reclaim every operation and the rollout completes.
func TestK8sMultiTarget_DataPlanePodRestartCompletes(t *testing.T) {
	ep, applyID, dsns := startCopyingRollout(t)

	crashed := crashPods(t, mtDataPlaneInstance)
	markAllDataPlaneAppliesStale(t)
	waitForPodsReadyAfterDeletion(t, mtDataPlaneInstance, crashed, 2*time.Minute)
	waitForTernHealth(t, ep, mtDataPlaneInstance, mtEnvironment, testutil.PollDeadline)

	p := requireCompleted(t, ep, applyID)
	t.Log(describeProgress(p))
	for i, dsn := range dsns {
		assert.True(t, indexOn(t, dsn, "orders", "idx_user_id"), "orders.idx_user_id on %s", mtTargets[i])
		assert.True(t, testutil.ColumnExists(t, dsn, "users", "email"), "users.email on %s", mtTargets[i])
	}
}

// markAllDataPlaneAppliesStale ages every unfinished data plane apply so the
// replacement pods reclaim them without waiting out the production window.
func markAllDataPlaneAppliesStale(t *testing.T) {
	t.Helper()
	db := testutil.OpenMySQL(t, storageDSNs(t)[0])
	settled := strings.TrimSuffix(strings.Repeat("?, ", len(state.SettledApplyStates)), ", ")
	args := make([]any, len(state.SettledApplyStates))
	for i, s := range state.SettledApplyStates {
		args[i] = s
	}
	_, err := db.ExecContext(t.Context(),
		"UPDATE applies SET updated_at = NOW() - INTERVAL 2 MINUTE WHERE state NOT IN ("+settled+")", args...)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(),
		`UPDATE apply_operations ao JOIN applies a ON ao.apply_id = a.id
			SET ao.updated_at = NOW() - INTERVAL 2 MINUTE
			WHERE a.state NOT IN (`+settled+`)`, args...)
	require.NoError(t, err)
}

// An apply of a multi-target rollout with a deferred cutover is refused
// before it starts, since each target cuts over as its table finishes.
func TestK8sMultiTarget_DeferCutoverRefused(t *testing.T) {
	ep := testutil.Endpoint(t)
	dsns := shardDSNs(t, mtShardApp)
	resetSchema(t, dsns)
	cleanupRolloutState(t)
	execOn(t, dsns, mtUsersV1)

	plan := planRollout(t, ep, mtShardApp, shardFiles(mtShardApp, map[string]string{"users": mtUsersV2}))
	resp, err := client.CallApplyAPIForTarget(ep, plan.PlanID, mtEnvironment, "", "", true, map[string]string{"defer_cutover": "true"})
	if err == nil {
		require.False(t, resp.Accepted, "deferred cutover across targets was accepted as %s", resp.ApplyID)
		t.Logf("refusal: %s %s", resp.ErrorCode, resp.ErrorMessage)
	} else {
		t.Logf("refusal: %v", err)
		assert.Contains(t, strings.ToLower(err.Error()), "cutover")
	}
	for i, dsn := range dsns {
		assert.False(t, testutil.ColumnExists(t, dsn, "users", "email"), "nothing ran on %s", mtTargets[i])
	}
}

// After the plan is reviewed, someone adds the column to shard 2 out of band.
// The apply refuses to run the reviewed DDL against the drifted shard and
// fails naming it, without retrying.
func TestK8sMultiTarget_DriftAfterPlanFailsNamingTarget(t *testing.T) {
	ep := testutil.Endpoint(t)
	dsns := shardDSNs(t, mtShardApp)
	resetSchema(t, dsns)
	cleanupRolloutState(t)
	execOn(t, dsns, mtUsersV1)

	plan := planRollout(t, ep, mtShardApp, shardFiles(mtShardApp, map[string]string{"users": mtUsersV2}))
	execOn(t, dsns[1:2], "ALTER TABLE users ADD COLUMN extra int DEFAULT NULL")

	applyID := applyRollout(t, ep, plan.PlanID, nil)
	p := waitForSettled(t, ep, applyID, 2*time.Minute, nil)
	t.Log(describeProgress(p))
	require.Equal(t, state.Apply.Failed, p.State, describeProgress(p))

	var drifted []string
	for _, op := range p.Operations {
		if state.IsState(op.State, state.Apply.Failed) {
			drifted = append(drifted, op.Target)
			assert.Contains(t, op.ErrorMessage, "drift", "operation %s says why it refused", op.OperationKey)
		}
	}
	assert.Equal(t, []string{mtTargets[1]}, drifted, describeProgress(p))
}

// Rollback isn't currently supported for schema changes across multiple targets. A completed rollout
// across the shards is refused before anything is planned, and the refusal
// names the remedy that restores each shard: an apply of schema files without
// the change.
func TestK8sMultiTarget_RollbackOfIndependentRolloutIsRefused(t *testing.T) {
	ep := testutil.Endpoint(t)
	dsns := shardDSNs(t, mtShardApp)
	resetSchema(t, dsns)
	cleanupRolloutState(t)
	execOn(t, dsns, mtUsersV1)

	plan := planRollout(t, ep, mtShardApp, shardFiles(mtShardApp, map[string]string{"users": mtUsersV2}))
	applyID := applyRollout(t, ep, plan.PlanID, nil)
	requireCompleted(t, ep, applyID)

	_, err := client.CallRollbackPlanAPI(ep, applyID, mtEnvironment)
	require.Error(t, err, "rollback of an apply across multiple targets")
	t.Log(err)
	assert.Contains(t, err.Error(), "rollback isn't currently supported for schema changes across multiple targets; undo apply "+applyID+" by applying schema files without the change")
	for i, dsn := range dsns {
		assert.True(t, testutil.ColumnExists(t, dsn, "users", "email"), "nothing was rolled back on %s", mtTargets[i])
	}
}

// waitForStates waits until the apply reaches one of states and returns its
// progress.
func waitForStates(t *testing.T, endpoint, applyID string, timeout time.Duration, states ...string) *apitypes.ProgressResponse {
	t.Helper()
	var last *apitypes.ProgressResponse
	testutil.Poll(t, timeout, testutil.PollInterval,
		func() bool {
			p, err := testutil.FetchProgress(endpoint, applyID)
			if err != nil {
				return false
			}
			last = p
			return state.IsState(p.State, states...)
		},
		func() string {
			return "apply never reached " + strings.Join(states, "/") + ":\n" + describeProgress(last)
		},
	)
	return last
}
