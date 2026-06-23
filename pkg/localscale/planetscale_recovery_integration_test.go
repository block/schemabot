//go:build integration

package localscale_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/engine/planetscale"
	"github.com/block/schemabot/pkg/psclient"
)

// A PlanetScale deploy's per-shard progress keys off the Vitess migration_context
// discovered by diffing SHOW VITESS_MIGRATIONS against a pre-deploy baseline. The
// baseline is persisted in the deploy operation's engine resume metadata, so a
// process that never captured it — an API progress poll on another pod, or a
// resume — can still recover the context instead of leaving per-shard progress
// empty.
//
// This drives the real recovery path against LocalScale: a first deploy seeds a
// terminal context (the durable baseline); a second deploy with deferred cutover
// parks an in-flight context; then Progress is called with the stored context
// blanked but the baseline present, and must rediscover the in-flight context.
func TestPlanetScaleProgressRecoversMigrationContextFromBaseline(t *testing.T) {
	cleanupActiveDeployRequests(t, t.Context())
	t.Cleanup(func() { cleanupActiveDeployRequests(t, t.Context()) })
	ctx := t.Context()

	const keyspace = "testapp_sharded"

	// First deploy: completes and leaves a terminal context behind. It stands in
	// for the contexts that already exist before the change under test deploys —
	// the durable baseline a later process diffs against.
	baselineBranch := createBranchWithDDL(t, ctx, "recover-baseline",
		map[string][]string{keyspace: {"ALTER TABLE `users` ADD COLUMN `recover_baseline_col` varchar(16) NULL"}}, nil)
	baselineDR := createDeploy(t, ctx, baselineBranch, true)
	deploy(t, ctx, baselineDR.Number, false)
	waitForDeployState(t, ctx, baselineDR.Number, drState.CompletePendingRevert, drState.Complete)

	baselineContexts := captureMigrationContexts(t, ctx, keyspace)
	require.NotEmpty(t, baselineContexts, "first deploy should leave a baseline migration_context")

	// Second deploy with deferred cutover: the change under test. Its migration
	// parks in flight (waiting for cutover) rather than completing, so its context
	// is a live, non-terminal candidate for discovery.
	changeBranch := createBranchWithDDL(t, ctx, "recover-change",
		map[string][]string{keyspace: {"ALTER TABLE `users` ADD COLUMN `recover_change_col` varchar(16) NULL"}}, nil)
	changeDR := createDeploy(t, ctx, changeBranch, false)
	deploy(t, ctx, changeDR.Number, false)

	eng := planetscale.NewWithClient(
		slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})),
		func(_, _ string) (psclient.PSClient, error) { return testClient, nil },
	)
	creds := newRecoveryCredentials(t, ctx)

	// The stored context is blank (the bug condition), but the baseline is present
	// in engine resume metadata, so Progress must rediscover the in-flight context.
	rs, err := planetscale.BuildResumeState(planetscale.ResumeData{
		BranchName:            changeBranch,
		DeployRequestID:       changeDR.Number,
		MigrationContext:      "",
		ExistingMigrationCtxs: baselineContexts,
	})
	require.NoError(t, err, "build resume state")

	var recovered string
	require.Eventually(t, func() bool {
		result, perr := eng.Progress(ctx, &engine.ProgressRequest{
			Database:    testDB,
			Credentials: creds,
			ResumeState: rs,
		})
		if perr != nil {
			t.Logf("progress poll error (will retry): %v", perr)
			return false
		}
		if result.ResumeState != nil && result.ResumeState.MigrationContext != "" {
			recovered = result.ResumeState.MigrationContext
			return true
		}
		return false
	}, 30*time.Second, 500*time.Millisecond, "Progress should recover the in-flight migration context from the baseline")

	assert.Contains(t, recovered, ":", "recovered value must be a real Vitess context, not a tern apply identifier")
	_, inBaseline := baselineContexts[recovered]
	assert.False(t, inBaseline, "recovered context must be the new in-flight change, not a baseline context")
}

// captureMigrationContexts snapshots the migration_context values currently in
// SHOW VITESS_MIGRATIONS for a keyspace, standing in for the durable baseline a
// later process diffs against.
func captureMigrationContexts(t *testing.T, ctx context.Context, keyspace string) map[string]planetscale.MigrationContextTimestamps {
	t.Helper()
	result, err := testContainer.VtgateExec(ctx, testOrg, testDB, keyspace, "SHOW VITESS_MIGRATIONS")
	require.NoError(t, err, "SHOW VITESS_MIGRATIONS for baseline")
	idx := -1
	for i, col := range result.Columns {
		if col == "migration_context" {
			idx = i
			break
		}
	}
	require.GreaterOrEqual(t, idx, 0, "SHOW VITESS_MIGRATIONS must expose a migration_context column")
	contexts := make(map[string]planetscale.MigrationContextTimestamps)
	for _, row := range result.Rows {
		if idx >= len(row) {
			continue
		}
		if mc, ok := row[idx].(string); ok && mc != "" {
			contexts[mc] = planetscale.MigrationContextTimestamps{}
		}
	}
	return contexts
}

func newRecoveryCredentials(t *testing.T, ctx context.Context) *engine.Credentials {
	t.Helper()
	pw, err := testClient.CreateBranchPassword(ctx, &ps.DatabaseBranchPasswordRequest{
		Organization: testOrg,
		Database:     testDB,
		Branch:       "main",
	})
	require.NoError(t, err, "CreateBranchPassword for main vtgate")
	return &engine.Credentials{
		DSN: fmt.Sprintf("%s:%s@tcp(%s)/", pw.Username, pw.PlainText, pw.Hostname),
		Metadata: map[string]string{
			"organization": testOrg,
			"main_branch":  "main",
			"token_name":   "test",
			"token_value":  "test",
		},
	}
}
