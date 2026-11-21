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

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	refusal := awaitCommentContaining(t, apply, "This change did not start: "+driftEnv+" is frozen by freeze "+freezeID+" until someone lifts it. Reason: incident 42.")
	assert.Contains(t, refusal, "run the same command again with `--override` followed by your reason in quotes")

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

	result := sendApplyConfirm(t, svc, dbName, "schemabot apply-confirm -e staging")
	refusal := awaitCommentContaining(t, result, "This change did not start: staging is frozen by freeze "+freezeID+" until someone lifts it. Reason: incident 43.")
	assert.Contains(t, refusal, "The pending confirmation was released. To ask a freeze approver to let it through, run `apply` again, then run `apply-confirm` with `--override`")

	requireNoApplies(t, svc, dbName)
	requireNoApplyLock(t, svc, dbName)

	// This is why the refusal says to run apply first: apply-confirm again,
	// --override or not, finds nothing to confirm.
	again := sendApplyConfirm(t, svc, dbName, `schemabot apply-confirm -e staging --override "hotfix"`)
	awaitCommentContaining(t, again, "No apply lock is held for this database. Run `apply` first")
}
