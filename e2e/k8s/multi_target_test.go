//go:build e2e

package k8s

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/e2e/testutil"
	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/state"
)

// The multi-target suite runs against e2e-multi-target-test.sh's stack: one
// data plane deployment whose targets are three DSIDs, each resolved through
// Etre to its own MySQL server. shardapp lists the three targets bare; nsapp
// gives each one its own namespace.
const (
	mtEnvironment = "staging"
	mtShardApp    = "shardapp"
	mtNSApp       = "nsapp"
	// mtRolloutDeadline bounds a whole table-by-table rollout across the
	// shards, which runs several data plane round trips per step.
	mtRolloutDeadline = 4 * time.Minute
)

// mtTargets are the rollout members in configured order.
var mtTargets = []string{"dsid-shard-001", "dsid-shard-002", "dsid-shard-003"}

// mtNamespaces is the namespace each target selects in the nsapp environment.
var mtNamespaces = []string{"nsapp_a", "nsapp_b", "nsapp_c"}

// shardDSNs returns one DSN per target, in target order, scoped to schema.
func shardDSNs(t *testing.T, schema string) []string {
	t.Helper()
	raw := os.Getenv("E2E_MULTI_TARGET_SHARD_DSNS")
	require.NotEmpty(t, raw, "E2E_MULTI_TARGET_SHARD_DSNS not set")
	parts := strings.Split(raw, ",")
	require.Len(t, parts, len(mtTargets), "one shard DSN per target")
	dsns := make([]string, len(parts))
	for i, p := range parts {
		cfg, err := mysql.ParseDSN(p)
		require.NoError(t, err, "parse shard DSN %d", i)
		cfg.DBName = schema
		dsns[i] = cfg.FormatDSN()
	}
	return dsns
}

// resetSchema drops every table in schema on every shard, now and when the
// test ends. The schema files a test plans are the whole desired state of the
// namespace, so a table left by an earlier test would plan as a drop.
func resetSchema(t *testing.T, dsns []string) {
	t.Helper()
	drop := func(ctx context.Context) {
		for _, dsn := range dsns {
			db, err := sql.Open("block-mysql", dsn)
			if err != nil {
				t.Logf("warning: open shard to reset: %v", err)
				continue
			}
			dropAllTables(ctx, t, db)
			utils.CloseAndLog(db)
		}
	}
	drop(t.Context())
	t.Cleanup(func() {
		ctx, cancel := testutil.CleanupContext(30 * time.Second)
		defer cancel()
		drop(ctx)
	})
}

func dropAllTables(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.QueryContext(ctx, "SHOW TABLES")
	if err != nil {
		t.Logf("warning: list shard tables: %v", err)
		return
	}
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Logf("warning: scan shard table: %v", err)
			break
		}
		tables = append(tables, name)
	}
	utils.CloseAndLog(rows)
	for _, tbl := range tables {
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS `"+tbl+"`"); err != nil {
			t.Logf("warning: drop shard table %s: %v", tbl, err)
		}
	}
}

// execOn runs each statement on the given shard DSNs.
func execOn(t *testing.T, dsns []string, stmts ...string) {
	t.Helper()
	for _, dsn := range dsns {
		db := testutil.OpenMySQL(t, dsn)
		for _, stmt := range stmts {
			_, err := db.ExecContext(t.Context(), stmt)
			require.NoError(t, err, "exec on shard: %s", stmt)
		}
	}
}

// planRollout plans the whole rollout the way the CLI does, declaring that it
// renders every member's plan.
func planRollout(t *testing.T, endpoint, database string, files map[string]*apitypes.SchemaFiles) *apitypes.PlanResponse {
	t.Helper()
	body, err := json.Marshal(apitypes.PlanRequest{
		Database:       database,
		Type:           "mysql",
		Environment:    mtEnvironment,
		SchemaFiles:    files,
		RendersRollout: true,
	})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), testutil.PollDeadline)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/api/plan", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "post plan")
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "plan %s: %s", database, raw)
	var plan apitypes.PlanResponse
	require.NoError(t, json.Unmarshal(raw, &plan), "decode plan: %s", raw)
	require.Empty(t, plan.Errors, "plan errors")
	return &plan
}

// applyRollout applies a rollout-wide plan and returns the apply ID.
func applyRollout(t *testing.T, endpoint, planID string, options map[string]string) string {
	t.Helper()
	resp, err := client.CallApplyAPIForTarget(endpoint, planID, mtEnvironment, "", "", true, options)
	require.NoError(t, err, "apply %s", planID)
	require.True(t, resp.Accepted, "apply %s not accepted: %s %s", planID, resp.ErrorCode, resp.ErrorMessage)
	require.NotEmpty(t, resp.ApplyID)
	return resp.ApplyID
}

// describeProgress renders an apply's operations for failure messages.
func describeProgress(p *apitypes.ProgressResponse) string {
	if p == nil {
		return "<no progress>"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "apply %s state=%s err=%q\n", p.ApplyID, p.State, p.ErrorMessage)
	for _, op := range p.Operations {
		fmt.Fprintf(&b, "  op key=%s target=%s step=%d state=%s converged=%v err=%q\n",
			op.OperationKey, op.Target, op.RolloutStep, op.State, op.AlreadyConverged, op.ErrorMessage)
	}
	for _, tbl := range p.Tables {
		fmt.Fprintf(&b, "  table %s target=%s status=%s rows=%d/%d\n",
			tbl.TableName, tbl.Target, tbl.Status, tbl.RowsCopied, tbl.RowsTotal)
	}
	return b.String()
}

// cleanupRolloutState clears both planes' storage when the test finishes, and
// first, when the test failed, logs the rollout rows of each plane, so a
// failure can be triaged from its output after the rows are gone.
func cleanupRolloutState(t *testing.T) {
	t.Helper()
	cleanupState(t)
	dsns := storageDSNs(t)
	since := time.Now().UTC()
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		for i, plane := range []string{"data plane", "control plane"} {
			t.Logf("%s storage at failure:\n%s", plane, dumpRolloutRows(dsns[i]))
		}
		for _, instance := range []string{"control-plane-mt", "data-plane-mt"} {
			t.Logf("%s logs since the test started:\n%s", instance, rolloutPodLogs(instance, since))
		}
	})
}

// rolloutPodLogLine reports whether a pod log line belongs in a failure dump:
// everything at info and above, and the debug lines that say why an operation
// did or did not start.
func rolloutPodLogLine(line string) bool {
	if !strings.Contains(line, `"level":"DEBUG"`) {
		return true
	}
	for _, keep := range []string{"conflict check", "claim", "dispatch", "potentially stale"} {
		if strings.Contains(line, keep) {
			return true
		}
	}
	return false
}

// rolloutPodLogs renders the logs every pod of instance wrote since since,
// filtered by rolloutPodLogLine. It runs in test cleanup, after the test's own
// context is cancelled, so it carries its own deadline.
func rolloutPodLogs(instance string, since time.Time) string {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pods, err := exec.CommandContext(ctx, "kubectl", "get", "pod", "-n", k8sNamespace, "-l", "app.kubernetes.io/instance="+instance, "-o", "name").Output()
	if err != nil {
		return fmt.Sprintf("list pods: %v", err)
	}
	var b strings.Builder
	for pod := range strings.FieldsSeq(string(pods)) {
		out, err := exec.CommandContext(ctx, "kubectl", "logs", "-n", k8sNamespace, pod, "--since-time="+since.Format(time.RFC3339)).CombinedOutput()
		if err != nil {
			fmt.Fprintf(&b, "  %s: kubectl logs: %v\n", pod, err)
			continue
		}
		fmt.Fprintf(&b, "  %s\n", pod)
		for line := range strings.SplitSeq(string(out), "\n") {
			if line != "" && rolloutPodLogLine(line) {
				fmt.Fprintf(&b, "    %s\n", line)
			}
		}
	}
	return b.String()
}

// dumpRolloutRows renders the applies, operations, tasks, and control
// requests stored at dsn, one row per line.
func dumpRolloutRows(dsn string) string {
	db, err := sql.Open("block-mysql", dsn)
	if err != nil {
		return fmt.Sprintf("open %s: %v", dsn, err)
	}
	defer utils.CloseAndLog(db)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	queries := []string{
		"SELECT id, apply_identifier, state, external_id, lease_owner, attempt, updated_at, error_message FROM applies ORDER BY id",
		"SELECT id, apply_id, operation_key, state, cutover_policy, on_failure, external_id, external_operation_id, lease_owner, lease_token, attempt, created_at, started_at, updated_at, error_message FROM apply_operations ORDER BY id",
		"SELECT id, apply_id, apply_operation_id, table_name, state, rows_copied, rows_total, attempt, updated_at FROM tasks ORDER BY id",
		"SELECT id, apply_id, operation, status, error_message, updated_at FROM apply_control_requests ORDER BY id",
	}
	var b strings.Builder
	for _, q := range queries {
		fmt.Fprintf(&b, "  %s\n", q)
		rows, err := db.QueryContext(ctx, q)
		if err != nil {
			fmt.Fprintf(&b, "    error: %v\n", err)
			continue
		}
		cols, err := rows.Columns()
		if err != nil {
			fmt.Fprintf(&b, "    columns: %v\n", err)
			utils.CloseAndLog(rows)
			continue
		}
		for rows.Next() {
			vals := make([]sql.NullString, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				fmt.Fprintf(&b, "    scan: %v\n", err)
				break
			}
			fields := make([]string, len(cols))
			for i, c := range cols {
				fields[i] = c + "=" + vals[i].String
			}
			fmt.Fprintf(&b, "    %s\n", strings.Join(fields, " "))
		}
		if err := rows.Err(); err != nil {
			fmt.Fprintf(&b, "    rows: %v\n", err)
		}
		utils.CloseAndLog(rows)
	}
	return b.String()
}

// waitForSettled polls until the apply reaches a state its outcome can no
// longer leave, calling observe on every progress snapshot. Stopped is not
// settled: a start resumes it.
func waitForSettled(t *testing.T, endpoint, applyID string, timeout time.Duration, observe func(*apitypes.ProgressResponse)) *apitypes.ProgressResponse {
	t.Helper()
	var last *apitypes.ProgressResponse
	testutil.Poll(t, timeout, testutil.PollInterval,
		func() bool {
			p, err := testutil.FetchProgress(endpoint, applyID)
			if err != nil {
				return false
			}
			last = p
			if observe != nil {
				observe(p)
			}
			return state.IsState(p.State, state.SettledApplyStates...)
		},
		func() string { return "apply never settled:\n" + describeProgress(last) },
	)
	return last
}

// requireCompleted waits for the apply and fails with its operations unless
// it completed.
func requireCompleted(t *testing.T, endpoint, applyID string) *apitypes.ProgressResponse {
	t.Helper()
	p := waitForSettled(t, endpoint, applyID, mtRolloutDeadline, nil)
	require.Equal(t, state.Apply.Completed, p.State, describeProgress(p))
	return p
}

// operationsByStep groups an apply's operations by rollout step.
func operationsByStep(p *apitypes.ProgressResponse) map[int][]*apitypes.ProgressOperationResponse {
	out := map[int][]*apitypes.ProgressOperationResponse{}
	for _, op := range p.Operations {
		out[op.RolloutStep] = append(out[op.RolloutStep], op)
	}
	return out
}

func indexOn(t *testing.T, dsn, table, index string) bool {
	t.Helper()
	db := testutil.OpenMySQL(t, dsn)
	var n int
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?",
		table, index).Scan(&n))
	return n > 0
}

func tableOn(t *testing.T, dsn, table string) bool {
	t.Helper()
	db := testutil.OpenMySQL(t, dsn)
	var n int
	require.NoError(t, db.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?", table).Scan(&n))
	return n > 0
}

const (
	mtUsersV1  = "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	mtUsersV2  = "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  `email` varchar(255) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	mtOrdersV1 = "CREATE TABLE `orders` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `user_id` bigint unsigned NOT NULL,\n  `amount` int NOT NULL,\n  `note` varchar(64) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	mtOrdersV2 = "CREATE TABLE `orders` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `user_id` bigint unsigned NOT NULL,\n  `amount` int NOT NULL,\n  `note` varchar(64) NOT NULL,\n  PRIMARY KEY (`id`),\n  KEY `idx_user_id` (`user_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	mtOrdersUq = "CREATE TABLE `orders` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `user_id` bigint unsigned NOT NULL,\n  `amount` int NOT NULL,\n  `note` varchar(64) NOT NULL,\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `uq_note` (`note`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	// mtOrdersUqShortNote adds the unique index and narrows note, so a row
	// whose note no longer fits fails the copy itself rather than the checksum
	// after it.
	mtOrdersUqShortNote = "CREATE TABLE `orders` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `user_id` bigint unsigned NOT NULL,\n  `amount` int NOT NULL,\n  `note` varchar(16) NOT NULL,\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `uq_note` (`note`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	mtAuditV1           = "CREATE TABLE `audit` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `event` varchar(64) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	mtOrderRows         = 200_000
)

func shardFiles(namespace string, tables map[string]string) map[string]*apitypes.SchemaFiles {
	files := make(map[string]string, len(tables))
	for name, ddl := range tables {
		files[name+".sql"] = ddl + ";\n"
	}
	return map[string]*apitypes.SchemaFiles{namespace: {Files: files}}
}

// seedOrders fills orders on every shard so its index add copies long enough
// for a poll to observe it mid-flight.
func seedOrders(t *testing.T, dsns []string, rows int) {
	t.Helper()
	for _, dsn := range dsns {
		testutil.SeedRows(t, dsn, "orders", "user_id, amount, note",
			"seq % 5000, seq % 997, CONCAT('n', seq)", rows)
	}
}

// Three shards hold users and a seeded orders table. One apply adds a column
// to users and an index to orders. The rollout runs table by table, one
// operation per target and table, and every shard ends with both changes.
func TestK8sMultiTarget_TwoTableRolloutCompletes(t *testing.T) {
	ep := testutil.Endpoint(t)
	dsns := shardDSNs(t, mtShardApp)
	resetSchema(t, dsns)
	cleanupRolloutState(t)
	execOn(t, dsns, mtUsersV1, mtOrdersV1)
	seedOrders(t, dsns, mtOrderRows)

	plan := planRollout(t, ep, mtShardApp, shardFiles(mtShardApp, map[string]string{"users": mtUsersV2, "orders": mtOrdersV2}))
	require.NotNil(t, plan.Rollout, "multi-target plan carries a rollout block")
	assert.Equal(t, len(mtTargets), plan.Rollout.Members)
	assert.True(t, plan.Rollout.MultiTarget)
	assert.Empty(t, plan.Rollout.Attention, "every member planned")

	applyID := applyRollout(t, ep, plan.PlanID, nil)
	p := requireCompleted(t, ep, applyID)

	steps := operationsByStep(p)
	require.Len(t, steps, 2, "one step per table: %s", describeProgress(p))
	for step, ops := range steps {
		targets := make([]string, 0, len(ops))
		for _, op := range ops {
			targets = append(targets, op.Target)
			assert.Equal(t, state.Apply.Completed, op.State, "step %d target %s", step, op.Target)
		}
		slices.Sort(targets)
		assert.Equal(t, mtTargets, targets, "step %d runs on every target", step)
	}
	for i, dsn := range dsns {
		assert.True(t, testutil.ColumnExists(t, dsn, "users", "email"), "users.email on %s", mtTargets[i])
		assert.True(t, indexOn(t, dsn, "orders", "idx_user_id"), "orders.idx_user_id on %s", mtTargets[i])
	}
}

// A copy of orders on every shard runs long enough to poll. No target starts
// the second table while any target is still on the first: the step gate
// holds a table until every target finishes the one before it.
func TestK8sMultiTarget_StepGateHoldsNextTable(t *testing.T) {
	ep := testutil.Endpoint(t)
	dsns := shardDSNs(t, mtShardApp)
	resetSchema(t, dsns)
	cleanupRolloutState(t)
	execOn(t, dsns, mtUsersV1, mtOrdersV1)
	seedOrders(t, dsns, mtOrderRows)

	// orders is listed first in the reviewed plan only if the planner orders
	// it so; the gate holds whichever table comes second.
	plan := planRollout(t, ep, mtShardApp, shardFiles(mtShardApp, map[string]string{"users": mtUsersV2, "orders": mtOrdersV2}))
	applyID := applyRollout(t, ep, plan.PlanID, nil)

	var violations []string
	sawFirstStepRunning := false
	p := waitForSettled(t, ep, applyID, mtRolloutDeadline, func(p *apitypes.ProgressResponse) {
		steps := operationsByStep(p)
		firstDone := true
		for _, op := range steps[1] {
			if state.IsRunningApplyState(op.State) {
				sawFirstStepRunning = true
			}
			if !state.IsState(op.State, state.Apply.Completed) {
				firstDone = false
			}
		}
		if firstDone {
			return
		}
		for _, op := range steps[2] {
			if !state.IsState(op.State, state.Apply.Pending) {
				violations = append(violations, fmt.Sprintf("step 2 target %s is %s while step 1 is unfinished:\n%s", op.Target, op.State, describeProgress(p)))
			}
		}
	})
	require.Equal(t, state.Apply.Completed, p.State, describeProgress(p))
	assert.True(t, sawFirstStepRunning, "the first step was observed copying")
	assert.Empty(t, violations)
}

// Each shard starts from a different schema: shard 1 already has users.email,
// shard 3 has no audit table. Every target is planned against its own live
// schema, so the rollout runs only what each one is missing and every shard
// converges on the same schema.
func TestK8sMultiTarget_PerTargetDriftConverges(t *testing.T) {
	ep := testutil.Endpoint(t)
	dsns := shardDSNs(t, mtShardApp)
	resetSchema(t, dsns)
	cleanupRolloutState(t)
	execOn(t, dsns[:1], mtUsersV2, mtAuditV1)
	execOn(t, dsns[1:2], mtUsersV1, mtAuditV1)
	execOn(t, dsns[2:], mtUsersV1)

	plan := planRollout(t, ep, mtShardApp, shardFiles(mtShardApp, map[string]string{"users": mtUsersV2, "audit": mtAuditV1}))
	require.NotNil(t, plan.Rollout)
	assert.True(t, plan.Rollout.Independent, "members planned against their own schemas")
	assert.Empty(t, plan.Rollout.Attention)

	applyID := applyRollout(t, ep, plan.PlanID, nil)
	p := requireCompleted(t, ep, applyID)
	t.Log(describeProgress(p))

	for i, dsn := range dsns {
		assert.True(t, testutil.ColumnExists(t, dsn, "users", "email"), "users.email on %s", mtTargets[i])
		assert.True(t, tableOn(t, dsn, "audit"), "audit on %s", mtTargets[i])
	}
}

// One shard already holds the change. The apply records it converged and
// still completes the rollout on the other two.
func TestK8sMultiTarget_ConvergedMemberCompletes(t *testing.T) {
	ep := testutil.Endpoint(t)
	dsns := shardDSNs(t, mtShardApp)
	resetSchema(t, dsns)
	cleanupRolloutState(t)
	execOn(t, dsns[:2], mtUsersV1)
	execOn(t, dsns[2:], mtUsersV2)

	plan := planRollout(t, ep, mtShardApp, shardFiles(mtShardApp, map[string]string{"users": mtUsersV2}))
	applyID := applyRollout(t, ep, plan.PlanID, nil)
	p := requireCompleted(t, ep, applyID)

	converged := 0
	for _, op := range p.Operations {
		if op.AlreadyConverged {
			converged++
			assert.Equal(t, mtTargets[2], op.Target, describeProgress(p))
		}
	}
	assert.Equal(t, 1, converged, describeProgress(p))
	for i, dsn := range dsns {
		assert.True(t, testutil.ColumnExists(t, dsn, "users", "email"), "users.email on %s", mtTargets[i])
	}
}

// After the plan is reviewed, shard 2's orders drifts, so shard 2 refuses the
// reviewed DDL and fails as soon as it is dispatched, under the default
// on_failure: halt. Shard 1 cuts over as soon as its own copy is ready. Shard
// 3 holds a larger table and is claimed at about the moment shard 2 refuses,
// so the rollout halts it in one of two places: either shard 3 never starts,
// or it starts, finishes its copy and holds at the cutover barrier until a
// cancel releases it without cutting it over. Either way nothing cuts over
// behind the failure, no shard starts the next table, and the apply settles
// failed, naming shard 2.
func TestK8sMultiTarget_FailedTargetHaltsRollout(t *testing.T) {
	ep := testutil.Endpoint(t)
	dsns := shardDSNs(t, mtShardApp)
	resetSchema(t, dsns)
	cleanupRolloutState(t)
	execOn(t, dsns, mtOrdersV1, mtUsersV1)
	seedOrders(t, dsns[2:], 3*mtCopyRows)

	plan := planRollout(t, ep, mtShardApp, shardFiles(mtShardApp, map[string]string{"orders": mtOrdersUq, "users": mtUsersV2}))
	execOn(t, dsns[1:2], "ALTER TABLE orders ADD COLUMN extra int DEFAULT NULL")
	applyID := applyRollout(t, ep, plan.PlanID, nil)

	var halted *apitypes.ProgressResponse
	testutil.Poll(t, mtRolloutDeadline, testutil.PollInterval,
		func() bool {
			p, err := testutil.FetchProgress(ep, applyID)
			if err != nil {
				return false
			}
			halted = p
			return state.IsState(p.State, state.SettledApplyStates...) || heldAtBarrierBehindFailure(p)
		},
		func() string { return "rollout never halted behind the failure:\n" + describeProgress(halted) },
	)
	t.Log("halted:\n" + describeProgress(halted))
	assert.Equal(t, state.ApplyOperation.Failed, operationStateOn(operationsByStep(halted)[1], mtTargets[1]), describeProgress(halted))
	assert.Equal(t, state.ApplyOperation.Completed, operationStateOn(operationsByStep(halted)[1], mtTargets[0]), describeProgress(halted))
	for _, op := range operationsByStep(halted)[2] {
		assert.True(t, state.IsState(op.State, state.ApplyOperation.Pending), "step 2 on %s did not start behind the failure, got %s", op.Target, op.State)
	}
	assert.False(t, indexOn(t, dsns[2], "orders", "uq_note"), "shard 3 does not cut over behind the failure")

	p := halted
	if heldAtBarrierBehindFailure(halted) {
		cancelResp, err := client.CallCancelAPI(ep, mtEnvironment, applyID)
		require.NoError(t, err, "cancel")
		require.True(t, cancelResp.Accepted, "cancel not accepted: %s", cancelResp.ErrorMessage)
		p = waitForSettled(t, ep, applyID, mtRolloutDeadline, nil)
		assert.Equal(t, state.ApplyOperation.Cancelled, operationStateOn(operationsByStep(p)[1], mtTargets[2]), describeProgress(p))
	} else {
		assert.Equal(t, state.ApplyOperation.Pending, operationStateOn(operationsByStep(p)[1], mtTargets[2]), "shard 3 never started behind the failure: %s", describeProgress(p))
	}
	t.Log("settled:\n" + describeProgress(p))
	require.Equal(t, state.Apply.Failed, p.State, describeProgress(p))
	assert.Contains(t, p.ErrorMessage, mtTargets[1], "the failure names the target")
	assert.True(t, indexOn(t, dsns[0], "orders", "uq_note"), "shard 1 cut over ahead of the failure")
	assert.False(t, indexOn(t, dsns[2], "orders", "uq_note"), "shard 3 never cut over behind the failure")
	for i, dsn := range dsns {
		assert.False(t, testutil.ColumnExists(t, dsn, "users", "email"), "users is held after the failed table on %s", mtTargets[i])
	}
	// Shard 1 cut over, and a cancel releases a copy shard 3 started,
	// asynchronously. Shard 2 refused the reviewed DDL before it copied
	// anything.
	released := []int{0, 1, 2}
	testutil.Poll(t, testutil.PollDeadline, time.Second,
		func() bool {
			for _, i := range released {
				if len(shadowTables(t, dsns[i], "orders")) > 0 {
					return false
				}
			}
			return true
		},
		func() string {
			var b strings.Builder
			for _, i := range released {
				b.WriteString(mtTargets[i] + ": " + strings.Join(shadowTables(t, dsns[i], "orders"), ",") + "\n")
			}
			return "shadow tables left on a shard that cut over, refused, or was cancelled:\n" + b.String()
		},
	)
	assert.False(t, indexOn(t, dsns[1], "orders", "uq_note"), "shard 2 refused the reviewed DDL")
}

// mtFailedCopyRows sizes the failing shard's table so its copy is still
// running when the drive starts the next shard, and reaches the failing row,
// inserted last, well before the larger shard's copy is ready.
const mtFailedCopyRows = mtCopyRows

// Shard 2 has a note too long for the narrowed column in its last row, so its
// copy of orders fails near the end, under the default on_failure: halt. A
// duplicate would only fail the checksum after the copy, whose retries are
// paced in seconds and outlast a larger shard's copy. The drive starts the
// shards in order. Shard 1 has nothing to copy and cuts over ahead of the
// failure. Shard 2's copy fails while shard 3 is still copying a table three
// times its size. Once shard 3's copy is ready it holds at the cutover barrier
// behind the failure instead of cutting over. A cancel releases shard 3
// without cutting it over, and the apply settles failed, naming shard 2, whose
// failed copy stays on the target for a new apply to adopt.
func TestK8sMultiTarget_FailedCopyHoldsLaterTargetAtBarrier(t *testing.T) {
	ep := testutil.Endpoint(t)
	dsns := shardDSNs(t, mtShardApp)
	resetSchema(t, dsns)
	cleanupRolloutState(t)
	execOn(t, dsns, mtOrdersV1, mtUsersV1)
	seedOrders(t, dsns[1:2], mtFailedCopyRows)
	execOn(t, dsns[1:2], "INSERT INTO orders (user_id, amount, note) VALUES (1, 1, 'a note longer than sixteen')")
	seedOrders(t, dsns[2:], 3*mtCopyRows)

	plan := planRollout(t, ep, mtShardApp, shardFiles(mtShardApp, map[string]string{"orders": mtOrdersUqShortNote, "users": mtUsersV2}))
	applyID := applyRollout(t, ep, plan.PlanID, nil)

	var held *apitypes.ProgressResponse
	testutil.Poll(t, mtRolloutDeadline, testutil.PollInterval,
		func() bool {
			p, err := testutil.FetchProgress(ep, applyID)
			if err != nil {
				return false
			}
			held = p
			return heldAtBarrierBehindFailure(p)
		},
		func() string {
			return "rollout never held shard 3 behind shard 2's failed copy:\n" + describeProgress(held)
		},
	)
	t.Log("held:\n" + describeProgress(held))
	assert.Equal(t, state.ApplyOperation.Completed, operationStateOn(operationsByStep(held)[1], mtTargets[0]), describeProgress(held))
	for _, op := range operationsByStep(held)[2] {
		assert.True(t, state.IsState(op.State, state.ApplyOperation.Pending), "step 2 on %s did not start behind the failure, got %s", op.Target, op.State)
	}
	assert.False(t, indexOn(t, dsns[2], "orders", "uq_note"), "shard 3 holds its cutover behind the failure")

	cancelResp, err := client.CallCancelAPI(ep, mtEnvironment, applyID)
	require.NoError(t, err, "cancel")
	require.True(t, cancelResp.Accepted, "cancel not accepted: %s", cancelResp.ErrorMessage)

	p := waitForSettled(t, ep, applyID, mtRolloutDeadline, nil)
	t.Log("settled:\n" + describeProgress(p))
	require.Equal(t, state.Apply.Failed, p.State, describeProgress(p))
	assert.Contains(t, p.ErrorMessage, mtTargets[1], "the failure names the target")
	assert.Equal(t, state.ApplyOperation.Cancelled, operationStateOn(operationsByStep(p)[1], mtTargets[2]), describeProgress(p))
	assert.True(t, indexOn(t, dsns[0], "orders", "uq_note"), "shard 1 cut over ahead of the failure")
	assert.False(t, indexOn(t, dsns[2], "orders", "uq_note"), "the cancel released shard 3 without cutting it over")
	for i, dsn := range dsns {
		assert.False(t, testutil.ColumnExists(t, dsn, "users", "email"), "users is held after the failed table on %s", mtTargets[i])
	}
	// Shard 1 cut over and the cancel released shard 3's copy, asynchronously.
	// Shard 2's failed copy stays on the target for a new apply to adopt.
	released := []int{0, 2}
	testutil.Poll(t, testutil.PollDeadline, time.Second,
		func() bool {
			for _, i := range released {
				if len(shadowTables(t, dsns[i], "orders")) > 0 {
					return false
				}
			}
			return true
		},
		func() string {
			var b strings.Builder
			for _, i := range released {
				b.WriteString(mtTargets[i] + ": " + strings.Join(shadowTables(t, dsns[i], "orders"), ",") + "\n")
			}
			return "shadow tables left on a shard that cut over or was cancelled:\n" + b.String()
		},
	)
	assert.NotEmpty(t, shadowTables(t, dsns[1], "orders"), "the failed copy stays on %s for a new apply to adopt", mtTargets[1])
}

// heldAtBarrierBehindFailure reports whether p shows a halt rollout holding
// shard 3 at the cutover barrier behind shard 2's failure.
func heldAtBarrierBehindFailure(p *apitypes.ProgressResponse) bool {
	ops := operationsByStep(p)[1]
	return state.IsState(p.State, state.Apply.RunningDegraded) &&
		operationStateOn(ops, mtTargets[1]) == state.ApplyOperation.Failed &&
		operationStateOn(ops, mtTargets[2]) == state.ApplyOperation.WaitingForCutover
}

// operationStateOn returns the state of the operation in ops on target, or ""
// when ops has none for it.
func operationStateOn(ops []*apitypes.ProgressOperationResponse, target string) string {
	for _, op := range ops {
		if op.Target == target {
			return state.NormalizeState(op.State)
		}
	}
	return ""
}

// After a rollout completes, planning the same schema again finds nothing to
// do on any member.
func TestK8sMultiTarget_ReplanAfterCompletionIsEmpty(t *testing.T) {
	ep := testutil.Endpoint(t)
	dsns := shardDSNs(t, mtShardApp)
	resetSchema(t, dsns)
	cleanupRolloutState(t)
	execOn(t, dsns, mtUsersV1)

	files := shardFiles(mtShardApp, map[string]string{"users": mtUsersV2})
	plan := planRollout(t, ep, mtShardApp, files)
	requireCompleted(t, ep, applyRollout(t, ep, plan.PlanID, nil))

	again := planRollout(t, ep, mtShardApp, files)
	assert.Empty(t, again.Changes, "primary has nothing left to change")
	if again.Rollout != nil {
		for _, g := range again.Rollout.Groups {
			assert.Empty(t, g.Changes, "group %v has nothing left to change", g.Members)
		}
		assert.Empty(t, again.Rollout.Attention)
	}
}

// Each target selects its own namespace. One apply adds a column in every
// namespace, and each shard changes only the namespace it holds.
func TestK8sMultiTarget_NamespacesPerTarget(t *testing.T) {
	ep := testutil.Endpoint(t)
	cleanupRolloutState(t)
	files := map[string]*apitypes.SchemaFiles{}
	for i, ns := range mtNamespaces {
		dsn := shardDSNs(t, ns)[i]
		resetSchema(t, []string{dsn})
		execOn(t, []string{dsn}, mtUsersV1)
		files[ns] = &apitypes.SchemaFiles{Files: map[string]string{"users.sql": mtUsersV2 + ";\n"}}
	}

	plan := planRollout(t, ep, mtNSApp, files)
	require.NotNil(t, plan.Rollout)
	assert.Empty(t, plan.Rollout.Attention)
	p := requireCompleted(t, ep, applyRollout(t, ep, plan.PlanID, nil))
	t.Log(describeProgress(p))

	for i, ns := range mtNamespaces {
		dsn := shardDSNs(t, ns)[i]
		assert.True(t, testutil.ColumnExists(t, dsn, "users", "email"), "%s.users.email on %s", ns, mtTargets[i])
	}
}
