package webhook

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
)

// unregisteredRepoResponderConfig is a deployment that answers unscoped
// commands and serves production, with its GitHub App installed on the
// test repository (octocat/hello-world) but without that repository in its
// repos configuration, as when a sibling deployment serves the repository for
// another environment.
func unregisteredRepoResponderConfig() *api.ServerConfig {
	return &api.ServerConfig{
		AllowedEnvironments: []string{"production"},
		Repos: map[string]api.RepoConfig{
			"octocat/registered-repo": {},
		},
	}
}

// serveUnregisteredRepoComment delivers comment on the unregistered test
// repository and returns the handler's response message and every PR
// comment posted while handling it.
func serveUnregisteredRepoComment(t *testing.T, config *api.ServerConfig, comment string) (string, []string) {
	t.Helper()
	h, comments, _ := newTestHandlerWithConfig(t, config)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{comment: comment, isPR: true}, nil))
	require.Equal(t, http.StatusOK, rr.Code)

	var posted []string
	for len(comments) > 0 {
		posted = append(posted, <-comments)
	}
	return rr.Body.String(), posted
}

// A repository served only by a sibling deployment still gets an answer from
// the deployment that answers unscoped commands, whose App is installed there
// too: help, usage errors no sibling would report, and commands for the
// environment it serves. It never plans or applies there, and it leaves
// commands for a sibling's environment to that sibling.
func TestUnregisteredRepoResponderReplies(t *testing.T) {
	tests := []struct {
		name        string
		comment     string
		wantMessage string
		wantComment []string
	}{
		{
			name:        "help",
			comment:     "schemabot help",
			wantMessage: "help posted",
			wantComment: []string{"SchemaBot Help", "schemabot plan"},
		},
		{
			name:        "unrecognized command",
			comment:     "schemabot foobar",
			wantMessage: "invalid command",
			wantComment: []string{"Invalid Command", "wasn't recognized"},
		},
		{
			name:        "unknown environment",
			comment:     "schemabot apply -e prodction",
			wantMessage: "unknown environment",
			wantComment: []string{"Invalid Environment", "`production`", "`staging`", "schemabot apply -e <environment>"},
		},
		{
			name:        "malformed environment",
			comment:     "schemabot apply -e production--allow-unsafe",
			wantMessage: "invalid environment value",
			wantComment: []string{"Invalid Environment", "schemabot apply -e <environment>"},
		},
		{
			name:        "missing environment",
			comment:     "schemabot apply",
			wantMessage: "missing environment flag",
			wantComment: []string{"Missing Argument", "schemabot apply -e <environment>"},
		},
		{
			name:        "environment this deployment serves",
			comment:     "schemabot plan -e production",
			wantMessage: "repository not registered for environment",
			wantComment: []string{"Repository Not Registered", "**Environment**: `production`", "no entry under `repos`"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			message, posted := serveUnregisteredRepoComment(t, unregisteredRepoResponderConfig(), tt.comment)

			assert.Contains(t, message, tt.wantMessage)
			require.Len(t, posted, 1, "exactly one reply")
			for _, want := range tt.wantComment {
				assert.Contains(t, posted[0], want)
			}
		})
	}
}

// Work that the deployments registering the repository run — a plan across
// every environment, a command for a sibling's environment, a command that
// takes no environment — draws no reply from a deployment that has not
// registered the repository.
func TestUnregisteredRepoResponderLeavesRegisteredDeploymentsWork(t *testing.T) {
	for _, comment := range []string{
		"schemabot plan",
		"schemabot plan -e staging",
		"schemabot unlock",
	} {
		t.Run(comment, func(t *testing.T) {
			message, posted := serveUnregisteredRepoComment(t, unregisteredRepoResponderConfig(), comment)

			assert.Contains(t, message, "repository not registered")
			assert.Empty(t, posted)
		})
	}
}

// Deployments that do not answer unscoped commands stay silent on a
// repository they have not registered. That covers a silenced sibling and
// every tenant deployment, whether or not it sets respond_to_unscoped, and a
// -t command sent to the untenanted responder.
func TestUnregisteredRepoSilentDeploymentsStaySilent(t *testing.T) {
	silenced := false
	tests := []struct {
		name    string
		config  func() *api.ServerConfig
		comment string
	}{
		{
			name: "respond_to_unscoped false",
			config: func() *api.ServerConfig {
				config := unregisteredRepoResponderConfig()
				config.AllowedEnvironments = []string{"staging"}
				config.RespondToUnscoped = &silenced
				return config
			},
			comment: "schemabot help",
		},
		{
			name: "tenant deployment with respond_to_unscoped unset",
			config: func() *api.ServerConfig {
				config := unregisteredRepoResponderConfig()
				config.Tenant = "tenant-a"
				return config
			},
			comment: "schemabot help",
		},
		{
			name: "tenant deployment answering a command for its tenant",
			config: func() *api.ServerConfig {
				config := unregisteredRepoResponderConfig()
				config.Tenant = "tenant-a"
				config.RespondToUnscoped = &silenced
				return config
			},
			comment: "schemabot plan -e production -t tenant-a",
		},
		{
			name:    "tenant-scoped command on the untenanted responder",
			config:  unregisteredRepoResponderConfig,
			comment: "schemabot help -t tenant-a",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			message, posted := serveUnregisteredRepoComment(t, tt.config(), tt.comment)

			assert.Contains(t, message, "repository not registered")
			assert.Empty(t, posted)
		})
	}
}
