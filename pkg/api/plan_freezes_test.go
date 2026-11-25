package api

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/storage"
)

// A plan learns the freezes its apply will meet: those in effect now and
// those scheduled, scoped as the change gate scopes them, so a freeze of
// another database or environment, or one already over, is not named.
func TestFreezesCoveringPlanSplitsInEffectFromScheduled(t *testing.T) {
	inEffect := gateTestFreeze("freeze-0000000000000001")
	scheduledEnd := freezeTestNow.Add(48 * time.Hour)
	scheduled := gateTestFreeze("freeze-0000000000000002")
	scheduled.Kind = storage.FreezeKindPlanned
	scheduled.StartsAt = freezeTestNow.Add(24 * time.Hour)
	scheduled.EndsAt = &scheduledEnd
	otherDatabase := gateTestFreeze("freeze-0000000000000003")
	otherDatabase.Databases = []string{"otherdb"}
	otherEnvironment := gateTestFreeze("freeze-0000000000000004")
	otherEnvironment.Environment = "production"
	endedAt := freezeTestNow.Add(-time.Minute)
	ended := gateTestFreeze("freeze-0000000000000005")
	ended.EndsAt = &endedAt
	svc, _ := newGateApplyTestService(t, inEffect, scheduled, otherDatabase, otherEnvironment, ended)

	freezes, err := svc.FreezesCoveringPlan(t.Context(), "staging", "testdb", "default", false)

	require.NoError(t, err)
	assert.Equal(t, freezeTestNow, freezes.Now)
	require.Len(t, freezes.InEffect, 1)
	assert.Equal(t, inEffect.Identifier, freezes.InEffect[0].Identifier)
	require.Len(t, freezes.Scheduled, 1)
	assert.Equal(t, scheduled.Identifier, freezes.Scheduled[0].Identifier)
}

// A freeze store that cannot be read is an error for the caller to handle;
// the plan comment then shows no notice, and the apply checks again.
func TestFreezesCoveringPlanReportsAStorageError(t *testing.T) {
	svc, _ := newGateApplyTestService(t, gateTestFreeze("freeze-0000000000000001"))
	svc.storage.(*mockStorageWithApplyStores).freezes.(*memoryFreezeStore).nowErr = errors.New("connection refused")

	_, err := svc.FreezesCoveringPlan(t.Context(), "staging", "testdb", "default", false)

	require.ErrorContains(t, err, "connection refused")
}

// A server without storage has no freezes to name.
func TestFreezesCoveringPlanWithoutStorage(t *testing.T) {
	freezes, err := (&Service{}).FreezesCoveringPlan(t.Context(), "staging", "testdb", "default", false)

	require.NoError(t, err)
	assert.Empty(t, freezes.InEffect)
	assert.Empty(t, freezes.Scheduled)
}

// The plan response names the freezes in effect, then those scheduled, so a
// CLI or API caller learns what the apply will meet.
func TestPlanResponseFreezesListsInEffectFirst(t *testing.T) {
	scheduledEnd := freezeTestNow.Add(48 * time.Hour)
	scheduled := gateTestFreeze("freeze-0000000000000001")
	scheduled.Kind = storage.FreezeKindPlanned
	scheduled.StartsAt = freezeTestNow.Add(24 * time.Hour)
	scheduled.EndsAt = &scheduledEnd
	inEffect := gateTestFreeze("freeze-0000000000000002")
	svc, _ := newGateApplyTestService(t, scheduled, inEffect)

	freezes := svc.planResponseFreezes(t.Context(), "testdb", "staging", "acme/shop", &apitypes.PlanResponse{Deployment: "default"})

	require.Len(t, freezes, 2)
	assert.Equal(t, inEffect.Identifier, freezes[0].ID)
	assert.True(t, freezes[0].InEffect)
	assert.Equal(t, scheduled.Identifier, freezes[1].ID)
	assert.False(t, freezes[1].InEffect)
}

// A freeze store that cannot be read leaves the freezes off the plan rather
// than failing it: the apply checks them again.
func TestPlanResponseFreezesOmitsThemOnAStorageError(t *testing.T) {
	svc, _ := newGateApplyTestService(t, gateTestFreeze("freeze-0000000000000001"))
	svc.storage.(*mockStorageWithApplyStores).freezes.(*memoryFreezeStore).nowErr = errors.New("connection refused")

	assert.Empty(t, svc.planResponseFreezes(t.Context(), "testdb", "staging", "acme/shop", &apitypes.PlanResponse{Deployment: "default"}))
}
