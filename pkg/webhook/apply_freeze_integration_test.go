//go:build integration

package webhook

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/storage"
)

// A freeze in effect refuses a PR apply before it starts. The comment names
// the freeze, no apply is created, and the lock the command took is released,
// so the next apply after the freeze ends finds nothing to clear.
func TestE2EApplyIsRefusedDuringAFreeze(t *testing.T) {
	dbName := "webhook_apply_frozen"
	svc := setupE2ERolloutService(t, dbName, []deploymentSpec{
		{name: "us", liveSchema: usersBaseSchema},
	}, api.PlanIndependent)

	// The freeze covers only this test's database, and is lifted when the
	// test ends, because the storage is shared with the other tests. A
	// fresh identifier lets the test run again against the same storage.
	freezeID := engine.NewFreezeID()
	_, err := svc.Storage().Freezes().Create(t.Context(), &storage.EnvironmentFreeze{
		Identifier:  freezeID,
		Environment: driftEnv,
		Databases:   []string{dbName},
		Kind:        storage.FreezeKindOnDemand,
		Running:     storage.FreezeRunningContinue,
		StartsAt:    time.Now().Add(-time.Hour),
		Reason:      "incident 42",
		CreatedBy:   "alice",
		Source:      storage.FreezeSourceManual,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = svc.Storage().Freezes().Lift(context.WithoutCancel(t.Context()), freezeID, "alice", "test done")
	})

	svc.Config().Freeze.GitHubTeams = []string{"acme/freeze-approvers"}
	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	refusal := awaitCommentContaining(t, apply, "| `"+freezeID+"` | When lifted | incident 42 |")
	assert.Contains(t, refusal, "## ❄️ Apply Blocked by a Freeze\n")
	assert.Contains(t, refusal, "Run the same command with `--override")
	assert.Contains(t, refusal, "must approve it: a member of `acme/freeze-approvers`.")

	requireNoApplies(t, svc, dbName)
	requireNoApplyLock(t, svc, dbName)
}

// apply-confirm reaches the gate holding the pending confirmation's lock. A
// freeze refusal releases it too, so the database is not left locked for
// other pull requests while the freeze lasts.
func TestE2EApplyConfirmIsRefusedDuringAFreeze(t *testing.T) {
	dbName := "webhook_confirm_frozen"
	svc := setupE2EService(t, dbName)
	seedCheck(t, svc, dbName, "staging", "action_required")
	seedPendingConfirmation(t, svc, dbName, "plan-pending-"+dbName, "staging")

	freezeID := engine.NewFreezeID()
	_, err := svc.Storage().Freezes().Create(t.Context(), &storage.EnvironmentFreeze{
		Identifier:  freezeID,
		Environment: "staging",
		Databases:   []string{dbName},
		Kind:        storage.FreezeKindOnDemand,
		Running:     storage.FreezeRunningContinue,
		StartsAt:    time.Now().Add(-time.Hour),
		Reason:      "incident 43",
		CreatedBy:   "alice",
		Source:      storage.FreezeSourceManual,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = svc.Storage().Freezes().Lift(context.WithoutCancel(t.Context()), freezeID, "alice", "test done")
	})

	svc.Config().Freeze.GitHubTeams = []string{"acme/freeze-approvers"}
	result := sendApplyConfirm(t, svc, dbName, "schemabot apply-confirm -e staging")
	refusal := awaitCommentContaining(t, result, "| `"+freezeID+"` | When lifted | incident 43 |")
	assert.Contains(t, refusal, "Nothing was applied, and the pending confirmation was released.")
	assert.Contains(t, refusal, "Run `apply` again, then run `apply-confirm` with `--override")

	requireNoApplies(t, svc, dbName)
	requireNoApplyLock(t, svc, dbName)

	// This is why the refusal says to run apply first: apply-confirm again,
	// --override or not, finds nothing to confirm.
	again := sendApplyConfirm(t, svc, dbName, `schemabot apply-confirm -e staging --override "hotfix"`)
	awaitCommentContaining(t, again, "No apply lock is held for this database. Run `apply` first")
}
