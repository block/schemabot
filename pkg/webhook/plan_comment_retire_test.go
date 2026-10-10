package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/webhook/templates"
)

func TestPlanCommentSupersedes(t *testing.T) {
	tests := []struct {
		name       string
		posted     *storage.PlanComment
		prior      *storage.PlanComment
		supersedes bool
	}{
		{
			name:       "different head always supersedes",
			posted:     &storage.PlanComment{HeadSHA: "sha2", EnvironmentScope: "staging"},
			prior:      &storage.PlanComment{HeadSHA: "sha1", EnvironmentScope: "production,staging"},
			supersedes: true,
		},
		{
			name:       "same head and same scope is a refresh",
			posted:     &storage.PlanComment{HeadSHA: "sha1", EnvironmentScope: "production,staging"},
			prior:      &storage.PlanComment{HeadSHA: "sha1", EnvironmentScope: "production,staging"},
			supersedes: true,
		},
		{
			name:       "same head with different scope keeps the prior visible",
			posted:     &storage.PlanComment{HeadSHA: "sha1", EnvironmentScope: "staging"},
			prior:      &storage.PlanComment{HeadSHA: "sha1", EnvironmentScope: "production,staging"},
			supersedes: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.supersedes, planCommentSupersedes(tt.posted, tt.prior))
		})
	}
}

func TestPlanCommentSlotEnvironmentScope(t *testing.T) {
	slot := planCommentSlot{Environments: []string{"staging", "production"}}
	assert.Equal(t, "production,staging", slot.environmentScope(),
		"environments are sorted so the same set always compares equal")
	assert.Equal(t, []string{"staging", "production"}, slot.Environments,
		"canonicalizing must not reorder the caller's display-ordered slice")

	assert.Equal(t, "staging", planCommentSlot{Environments: []string{"staging"}}.environmentScope())
	assert.Equal(t, "", planCommentSlot{}.environmentScope())
}

func TestPlanCommentUpToDate(t *testing.T) {
	noChanges := func() *templates.PlanCommentData { return &templates.PlanCommentData{} }
	tests := []struct {
		name           string
		data           templates.MultiEnvPlanCommentData
		rolloutHasWork bool
		upToDate       bool
	}{
		{
			name:     "no changes, errors, or drift",
			data:     templates.MultiEnvPlanCommentData{Plans: map[string]*templates.PlanCommentData{"staging": noChanges()}},
			upToDate: true,
		},
		{
			name: "a plan with changes",
			data: templates.MultiEnvPlanCommentData{Plans: map[string]*templates.PlanCommentData{
				"staging": {Changes: []templates.KeyspaceChangeData{{Keyspace: "orders"}}},
			}},
		},
		{
			name: "an environment that failed to plan",
			data: templates.MultiEnvPlanCommentData{
				Plans:  map[string]*templates.PlanCommentData{"staging": noChanges()},
				Errors: map[string]string{"production": "plan failed"},
			},
		},
		{
			name: "a plan the engine reported errors on",
			data: templates.MultiEnvPlanCommentData{Plans: map[string]*templates.PlanCommentData{
				"staging": {Errors: []string{"plan is incomplete for this shard"}},
			}},
		},
		{
			name:           "a rollout member that still has work",
			data:           templates.MultiEnvPlanCommentData{Plans: map[string]*templates.PlanCommentData{"staging": noChanges()}},
			rolloutHasWork: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.upToDate, planCommentUpToDate(tt.data, tt.rolloutHasWork))
		})
	}
}
