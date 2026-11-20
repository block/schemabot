//go:build integration

package webhook

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
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
	// test ends, because the storage is shared with the other tests.
	_, err := svc.Storage().Freezes().Create(t.Context(), &storage.EnvironmentFreeze{
		Identifier:  "freeze-e2e",
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
		_, _ = svc.Storage().Freezes().Lift(context.WithoutCancel(t.Context()), "freeze-e2e", "alice", "test done")
	})

	apply := runRolloutCommand(t, svc, dbName, "schemabot apply -e "+driftEnv)
	awaitCommentContaining(t, apply, "This change did not start: it is blocked in "+driftEnv+" by freeze freeze-e2e (incident 42, until lifted)")

	requireNoApplies(t, svc, dbName)
	requireNoApplyLock(t, svc, dbName)
}
