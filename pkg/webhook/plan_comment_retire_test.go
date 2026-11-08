package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ghclient "github.com/block/schemabot/pkg/github"

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
		{
			name: "a branch missing newer base-branch schema changes",
			data: templates.MultiEnvPlanCommentData{Plans: map[string]*templates.PlanCommentData{
				"staging": {BaseSchema: &templates.BaseSchemaData{BaseBranch: "main", ChangedPaths: []string{"schema/staging"}}},
			}},
		},
		{
			name: "a branch whose base schema could not be checked",
			data: templates.MultiEnvPlanCommentData{Plans: map[string]*templates.PlanCommentData{
				"staging": {BaseSchema: &templates.BaseSchemaData{BaseBranch: "main", Unverified: true}},
			}},
		},
		{
			name: "a branch with every base-branch schema change",
			data: templates.MultiEnvPlanCommentData{Plans: map[string]*templates.PlanCommentData{
				"staging": {BaseSchema: &templates.BaseSchemaData{BaseBranch: "main"}},
			}},
			upToDate: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.upToDate, planCommentUpToDate(tt.data, tt.rolloutHasWork))
		})
	}
}

// A stale plan comment sweep whose run has ended retires nothing and logs
// at Info: the sweep is cleanup, the next supersede sweep finishes it, and
// an operator has nothing to act on. A head read that fails on a live run is
// still an error.
func TestPlanSweepHeadIsCurrentWhenTheRunHasEnded(t *testing.T) {
	tests := []struct {
		name      string
		ctx       func(t *testing.T) context.Context
		wantLevel string
	}{
		{name: "run ended", ctx: func(t *testing.T) context.Context {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			return ctx
		}, wantLevel: "INFO"},
		{name: "run live", ctx: func(t *testing.T) context.Context { return t.Context() }, wantLevel: "ERROR"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mux := setupGitHubServer(t)
			mux.HandleFunc("GET /repos/octocat/hello-world/pulls/1", func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "denied", http.StatusForbidden)
			})
			var logs bytes.Buffer
			h := &Handler{logger: slog.New(slog.NewJSONHandler(&logs, nil))}
			installClient := ghclient.NewInstallationClient(client, h.logger)

			current := h.planSweepHeadIsCurrent(tt.ctx(t), installClient, "octocat/hello-world", 1, "abc123")
			assert.False(t, current, "nothing is retired without a verified head")

			var entry map[string]any
			require.NoError(t, json.Unmarshal(logs.Bytes(), &entry), "exactly one log line: %s", logs.String())
			assert.Equal(t, tt.wantLevel, entry["level"])
			assert.Equal(t, "abc123", entry["head_sha"])
		})
	}
}
