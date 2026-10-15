package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/action"
	"github.com/block/schemabot/pkg/webhook/templates"
)

type missingApplyIDStorage struct {
	storage.Storage
	applies storage.ApplyStore
}

func (s *missingApplyIDStorage) Applies() storage.ApplyStore { return s.applies }

func missingApplyIDHandler(t *testing.T, applies storage.ApplyStore, config *api.ServerConfig) *Handler {
	t.Helper()
	return &Handler{
		service: api.New(&missingApplyIDStorage{applies: applies}, config, nil, testLogger()),
		logger:  testLogger(),
	}
}

func missingApplyIDApply(identifier, database, environment, applyState string) *storage.Apply {
	apply := prClosedApply(database, applyState)
	apply.ApplyIdentifier = identifier
	apply.Environment = environment
	return apply
}

// An operator comments `schemabot cutover -e production` without the apply ID.
// The reply is built from the PR's applies: it names the production apply that
// has not settled, newest first per database, and leaves out staging, settled
// applies, and older applies to the same database.
func TestMissingApplyIDDataNamesTheAppliesTheCommandCanActOn(t *testing.T) {
	applies := &prClosedApplyStore{applies: []*storage.Apply{
		missingApplyIDApply("apply-new", "orders", "production", state.Apply.WaitingForCutover),
		missingApplyIDApply("apply-staging", "orders", "staging", state.Apply.Running),
		missingApplyIDApply("apply-done", "users", "production", state.Apply.Completed),
		missingApplyIDApply("apply-stopped", "ledger", "production", state.Apply.Stopped),
		missingApplyIDApply("apply-old", "orders", "production", state.Apply.Running),
	}}
	h := missingApplyIDHandler(t, applies, &api.ServerConfig{})

	data := h.missingApplyIDData(t.Context(), "octocat/hello-world", 1,
		CommandResult{Action: action.Cutover, Environment: "production"}, action.Cutover, controlCommandActsOn)

	assert.Equal(t, []templates.ApplyIDCandidate{
		{ApplyID: "apply-new", Database: "orders", Environment: "production"},
		{ApplyID: "apply-stopped", Database: "ledger", Environment: "production"},
	}, data.Candidates)
	assert.True(t, data.Complete)
}

func TestMissingApplyIDDataForRollbackNamesCompletedApplies(t *testing.T) {
	applies := &prClosedApplyStore{applies: []*storage.Apply{
		missingApplyIDApply("apply-running", "orders", "staging", state.Apply.Running),
		missingApplyIDApply("apply-done", "users", "staging", state.Apply.Completed),
	}}
	h := missingApplyIDHandler(t, applies, &api.ServerConfig{})

	data := h.missingApplyIDData(t.Context(), "octocat/hello-world", 1,
		CommandResult{Action: action.Rollback, Environment: "staging"}, action.Rollback, rollbackActsOn)

	assert.Equal(t, []templates.ApplyIDCandidate{{ApplyID: "apply-done", Database: "users", Environment: "staging"}}, data.Candidates)
}

// A failed lookup leaves the data incomplete, so the reply points at the
// progress comment instead of claiming the PR has no apply.
func TestMissingApplyIDDataWhenTheLookupFails(t *testing.T) {
	h := missingApplyIDHandler(t, &failingPRApplyLookupStore{}, &api.ServerConfig{})

	data := h.missingApplyIDData(t.Context(), "octocat/hello-world", 1,
		CommandResult{Action: action.Stop, Environment: "staging"}, action.Stop, controlCommandActsOn)

	assert.Empty(t, data.Candidates)
	assert.False(t, data.Complete)
}

// On an aggregate repo each deployment stores only its own applies, so an
// unscoped command's reply from the leader is incomplete, while a command
// scoped to this deployment's tenant is answered in full.
func TestMissingApplyIDDataOnAnAggregateRepo(t *testing.T) {
	applies := &prClosedApplyStore{applies: []*storage.Apply{
		missingApplyIDApply("apply-a", "orders", "staging", state.Apply.Running),
	}}
	h := missingApplyIDHandler(t, applies, aggregateLeaderConfig())

	unscoped := h.missingApplyIDData(t.Context(), "octocat/hello-world", 1,
		CommandResult{Action: action.Stop, Environment: "staging"}, action.Stop, controlCommandActsOn)
	assert.Len(t, unscoped.Candidates, 1)
	assert.False(t, unscoped.Complete)

	scoped := h.missingApplyIDData(t.Context(), "octocat/hello-world", 1,
		CommandResult{Action: action.Stop, Environment: "staging", Tenant: "tenant-a"}, action.Stop, controlCommandActsOn)
	assert.True(t, scoped.Complete)
}
