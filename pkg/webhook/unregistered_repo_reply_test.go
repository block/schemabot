package webhook

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
)

// unregisteredRepoResponderConfig is a deployment an operator named as the
// fleet's unscoped responder, serving production, with its GitHub App
// installed on the test repository (octocat/hello-world) but without that
// repository in its repos configuration, as when a sibling deployment serves
// the repository for another environment.
func unregisteredRepoResponderConfig() *api.ServerConfig {
	responder := true
	return &api.ServerConfig{
		AllowedEnvironments: []string{"production"},
		RespondToUnscoped:   &responder,
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
// the fleet's unscoped responder, whose App is installed there too: help and
// the usage errors no sibling would report. Each reply is a fact about the
// comment, true whichever deployment registered the repository.
func TestUnregisteredRepoResponderReplies(t *testing.T) {
	servesEveryEnvironment := func() *api.ServerConfig {
		config := unregisteredRepoResponderConfig()
		config.AllowedEnvironments = nil
		return config
	}
	tests := []struct {
		name        string
		config      func() *api.ServerConfig
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
			name:        "unknown environment on a deployment that allows every environment",
			config:      servesEveryEnvironment,
			comment:     "schemabot apply -e prodction",
			wantMessage: "unknown environment",
			wantComment: []string{"Invalid Environment", "`production`", "schemabot apply -e <environment>"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := unregisteredRepoResponderConfig
			if tt.config != nil {
				config = tt.config
			}
			message, posted := serveUnregisteredRepoComment(t, config(), tt.comment)

			assert.Contains(t, message, tt.wantMessage)
			require.Len(t, posted, 1, "exactly one reply")
			for _, want := range tt.wantComment {
				assert.Contains(t, posted[0], want)
			}
		})
	}
}

// Work that the deployments registering the repository run draws no reply
// from a deployment that has not registered it: a plan across every
// environment, a command for any real environment, including one this
// deployment serves, and a command that takes no environment. This
// deployment cannot see which repositories its siblings registered, so it
// never tells a user the repository is unserved.
func TestUnregisteredRepoResponderLeavesRegisteredDeploymentsWork(t *testing.T) {
	for _, comment := range []string{
		"schemabot plan",
		"schemabot plan -e staging",
		"schemabot plan -e production",
		"schemabot apply -e production",
		"schemabot unlock",
	} {
		t.Run(comment, func(t *testing.T) {
			message, posted := serveUnregisteredRepoComment(t, unregisteredRepoResponderConfig(), comment)

			assert.Contains(t, message, "repository not registered")
			assert.Empty(t, posted)
		})
	}
}

// Deployments that are not the fleet's explicit unscoped responder stay
// silent on a repository they have not registered. That covers a silenced
// sibling, an untenanted deployment that leaves respond_to_unscoped unset,
// every tenant deployment whether or not it sets the flag, and a -t command
// sent to the untenanted responder.
func TestUnregisteredRepoSilentDeploymentsStaySilent(t *testing.T) {
	silenced := false
	tests := []struct {
		name    string
		config  func() *api.ServerConfig
		comment string
	}{
		{
			name: "respond_to_unscoped unset",
			config: func() *api.ServerConfig {
				config := unregisteredRepoResponderConfig()
				config.RespondToUnscoped = nil
				return config
			},
			comment: "schemabot foobar",
		},
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

// serveComment delivers comment on the test repository to h and returns the
// handler's response message and every PR comment posted while handling it.
func serveComment(t *testing.T, h *Handler, comments chan string, comment string) (string, []string) {
	t.Helper()
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, buildWebhookRequest(t, webhookPayloadOpts{comment: comment, isPR: true}, nil))
	require.Equal(t, http.StatusOK, rr.Code)

	var posted []string
	for len(comments) > 0 {
		posted = append(posted, <-comments)
	}
	return rr.Body.String(), posted
}

// A fleet where a tenant deployment registers the repository and an
// untenanted deployment's App is installed on it without registering it. The
// comment reaches both. Help gets exactly one reply across the two, from
// whichever deployment is configured to give it, and a command for an
// environment both serve is left to the tenant that registered the
// repository: the untenanted deployment posts nothing that would contradict
// the tenant's work.
func TestUnregisteredRepoResponderBesideARegisteringTenant(t *testing.T) {
	responder, silenced := true, false
	tests := []struct {
		name               string
		tenantFlag         *bool
		untenantedFlag     *bool
		wantHelpFromTenant bool
	}{
		{name: "untenanted deployment is the explicit responder", tenantFlag: &silenced, untenantedFlag: &responder, wantHelpFromTenant: false},
		{name: "neither deployment sets respond_to_unscoped", tenantFlag: nil, untenantedFlag: nil, wantHelpFromTenant: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tenant, tenantComments, _ := newTestHandlerWithConfig(t, &api.ServerConfig{
				Tenant:              "tenant-a",
				AllowedEnvironments: []string{"production"},
				RespondToUnscoped:   tt.tenantFlag,
				Repos:               map[string]api.RepoConfig{"octocat/hello-world": {}},
			})
			untenanted, untenantedComments, _ := newTestHandlerWithConfig(t, &api.ServerConfig{
				AllowedEnvironments: []string{"production"},
				RespondToUnscoped:   tt.untenantedFlag,
				Repos:               map[string]api.RepoConfig{"octocat/registered-repo": {}},
			})

			_, tenantHelp := serveComment(t, tenant, tenantComments, "schemabot help")
			_, untenantedHelp := serveComment(t, untenanted, untenantedComments, "schemabot help")
			require.Len(t, append(tenantHelp, untenantedHelp...), 1, "exactly one help reply across the fleet")
			if tt.wantHelpFromTenant {
				assert.Len(t, tenantHelp, 1, "the registering tenant answers help")
			} else {
				assert.Len(t, untenantedHelp, 1, "the explicit responder answers help")
			}

			message, posted := serveComment(t, untenanted, untenantedComments, "schemabot plan -e production")
			assert.Contains(t, message, "repository not registered")
			assert.Empty(t, posted, "the untenanted deployment leaves the tenant's environment to the tenant")
		})
	}
}
