//go:build integration

package webhook

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/storage"
)

// freezeForPlan freezes dbName in environment from startsAt, and lifts the
// freeze when the test ends, because the storage is shared with other tests.
func freezeForPlan(t *testing.T, h *Handler, environment, dbName, reason string, startsAt time.Time, endsAt *time.Time) string {
	t.Helper()
	kind := storage.FreezeKindOnDemand
	if endsAt != nil {
		kind = storage.FreezeKindPlanned
	}
	freezeID := engine.NewFreezeID()
	_, err := h.service.Storage().Freezes().Create(t.Context(), &storage.EnvironmentFreeze{
		Identifier:  freezeID,
		Environment: environment,
		Databases:   []string{dbName},
		Kind:        kind,
		Running:     storage.FreezeRunningContinue,
		StartsAt:    startsAt,
		EndsAt:      endsAt,
		Reason:      reason,
		CreatedBy:   "alice",
		Source:      storage.FreezeSourceManual,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = h.service.Storage().Freezes().Lift(context.WithoutCancel(t.Context()), freezeID, "alice", "test done")
	})
	return freezeID
}

// A plan of a frozen database says, before its author comments an apply,
// that the apply would be refused now, and names a scheduled code freeze
// with when it starts.
func TestE2EPlanCommandShowsTheFreezes(t *testing.T) {
	dbName := "webhook_plan_frozen"
	h, _, result := newBaseSchemaPlanFlow(t, dbName)
	inEffect := freezeForPlan(t, h, "staging", dbName, "incident 42", time.Now().Add(-time.Hour), nil)
	startsAt := time.Now().Add(24 * time.Hour).UTC().Truncate(time.Second)
	endsAt := startsAt.Add(24 * time.Hour)
	scheduled := freezeForPlan(t, h, "staging", dbName, "launch week code freeze", startsAt, &endsAt)

	body := requestPlan(t, h, result, "schemabot plan -e staging")

	assert.Contains(t, body, "❄️ **`staging` is frozen, so applying this plan would be refused now.**")
	assert.Contains(t, body, "| `"+inEffect+"` | until lifted | incident 42 |")
	assert.Contains(t, body, "SchemaBot lets it through and records your reason.")
	assert.Contains(t, body, "❄️ **A freeze is scheduled in `staging`.**")
	assert.Contains(t, body, "| `"+scheduled+"` | "+startsAt.Format(time.RFC3339)+" | until "+endsAt.Format(time.RFC3339)+" | launch week code freeze |")
	assert.Contains(t, body, "CREATE TABLE `users`", "the plan itself still posts")
}

// The plan for every environment, which a bare `schemabot plan` and a push
// post, shows the notice too.
func TestE2EMultiEnvPlanShowsTheFreeze(t *testing.T) {
	dbName := "webhook_multienv_plan_frozen"
	h, _, result := newBaseSchemaPlanFlow(t, dbName)
	freezeID := freezeForPlan(t, h, "staging", dbName, "incident 43", time.Now().Add(-time.Hour), nil)

	body := requestPlan(t, h, result, "schemabot plan")

	assert.Contains(t, body, "`staging` is frozen, so applying this plan would be refused now.")
	assert.Contains(t, body, "| `"+freezeID+"` | until lifted | incident 43 |")
}

// A freeze of another database is not named.
func TestE2EPlanCommandIgnoresAFreezeOfAnotherDatabase(t *testing.T) {
	dbName := "webhook_plan_other_frozen"
	h, _, result := newBaseSchemaPlanFlow(t, dbName)
	freezeForPlan(t, h, "staging", dbName+"_other", "incident 44", time.Now().Add(-time.Hour), nil)

	body := requestPlan(t, h, result, "schemabot plan -e staging")

	assert.NotContains(t, body, "❄️")
}
