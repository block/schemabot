package webhook

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/webhook/action"
)

// serveNoRepositoryRoutes registers nothing for the PR: not its metadata, not
// its changed files, not the tree. Every GitHub read a handler attempts fails
// and surfaces as a generic error comment — which is how these tests prove the
// registry answered before the first read, ahead of the reconciliation
// preflight that fetches the PR's files.
func serveNoRepositoryRoutes(_ *testing.T, _ *http.ServeMux) {}

// withRegisteredDatabase adds a database to the server config's registry so
// the deployment is the owner of some database, just not the one named.
func withRegisteredDatabase(cfg *api.ServerConfig, database string) *api.ServerConfig {
	cfg.Databases = map[string]api.DatabaseConfig{
		database: {Environments: map[string]api.EnvironmentConfig{"staging": {}}},
	}
	return cfg
}

// A database-scoped command names a database this deployment's registry has
// no entry for. The registry alone decides that, before any GitHub read, so on
// a single-deployment repo the user gets the Database Not Configured comment —
// never a Database Not Found that blames a schemabot.yaml SchemaBot did not
// look for, and never a reconciliation answer that depends on what the PR
// touches. The same short-circuit holds for a -t-scoped command on an
// aggregate participant, which named this deployment and owes it an answer.
func TestDatabaseScopedCommandOnUnconfiguredDatabaseAnswersFromRegistry(t *testing.T) {
	commands := []struct {
		name string
		run  func(*Handler, string)
	}{
		{"plan", func(h *Handler, tenant string) {
			h.handleMultiEnvPlan("octocat/hello-world", 1, "payments", tenant, 12345, "hubot", false, true, 0)
		}},
		{"apply", func(h *Handler, tenant string) {
			h.handleApplyCommand("octocat/hello-world", 1, "staging", "payments", 12345, "hubot",
				CommandResult{Action: action.Apply, Tenant: tenant})
		}},
		{"apply-confirm", func(h *Handler, tenant string) {
			h.handleApplyConfirmCommand("octocat/hello-world", 1, "staging", "payments", 12345, "hubot",
				CommandResult{Action: action.ApplyConfirm, Tenant: tenant})
		}},
	}

	for _, command := range commands {
		t.Run(command.name, func(t *testing.T) {
			t.Run("single-deployment repo posts Database Not Configured", func(t *testing.T) {
				h, mux, comments := newFanOutSkipHandler(t, withRegisteredDatabase(nonAggregateConfig(), "inventory"))
				serveNoRepositoryRoutes(t, mux)

				command.run(h, "")

				body := requireComment(t, comments, "database-not-configured "+command.name+" error")
				assert.Contains(t, body, "Database Not Configured")
				assert.Contains(t, body, "`payments`")
				assert.NotContains(t, body, "Database Not Found")
				assert.NotContains(t, body, "Failed", "the registry answered; no GitHub read may have run into the missing routes")
			})

			t.Run("tenant-scoped participant posts Database Not Configured", func(t *testing.T) {
				h, mux, comments := newFanOutSkipHandler(t, withRegisteredDatabase(aggregateParticipantConfig(), "inventory"))
				serveNoRepositoryRoutes(t, mux)

				command.run(h, "tenant-b")

				body := requireComment(t, comments, "database-not-configured "+command.name+" error")
				assert.Contains(t, body, "Database Not Configured")
				assert.Contains(t, body, "`payments`")
				assert.NotContains(t, body, "Failed", "the registry answered; no GitHub read may have run into the missing routes")
			})

			// Unscoped fan-out: the database belongs to whichever deployment
			// registers it, so a participant that does not stays silent —
			// and, deciding from its registry, without asking GitHub.
			t.Run("unscoped participant stays silent", func(t *testing.T) {
				h, mux, comments := newFanOutSkipHandler(t, withRegisteredDatabase(aggregateParticipantConfig(), "inventory"))
				serveNoRepositoryRoutes(t, mux)

				command.run(h, "")

				assert.Empty(t, comments, "a participant answers an unconfigured -d database with silence, not a comment")
			})
		})
	}
}

// An aggregate leader's registry covers only its own slice of the fleet, so it
// cannot answer a -d database from the registry: it searches the repository,
// and on a complete tree with no matching config it publishes the
// fleet-authoritative Database Not Found. When GitHub truncates the tree, the
// leader has no configured directories to bound the search for a database it
// does not register, so it fails closed with Repository Too Large to Search
// instead of asserting the config is absent from a repository it never read.
func TestAggregateLeaderDatabaseScopedCommandKeepsSearchingTheRepository(t *testing.T) {
	commands := []struct {
		name string
		run  func(*Handler)
	}{
		{"plan", func(h *Handler) {
			h.handleMultiEnvPlan("octocat/hello-world", 1, "payments", "", 12345, "hubot", false, true, 0)
		}},
		{"apply", func(h *Handler) {
			h.handleApplyCommand("octocat/hello-world", 1, "staging", "payments", 12345, "hubot",
				CommandResult{Action: action.Apply})
		}},
	}

	for _, command := range commands {
		t.Run(command.name, func(t *testing.T) {
			t.Run("complete tree without the config reports Database Not Found", func(t *testing.T) {
				h, mux, comments := newFanOutSkipHandler(t, withRegisteredDatabase(aggregateLeaderConfig(), "inventory"))
				serveSchemaConfigForDatabase(t, mux, "inventory")
				serveCompleteRootConfigTree(t, mux)

				command.run(h)

				body := requireComment(t, comments, "database-not-found leader "+command.name+" error")
				assert.Contains(t, body, "Database Not Found")
				assert.Contains(t, body, "`payments`")
				assert.NotContains(t, body, "Database Not Configured")
			})

			t.Run("truncated tree fails closed instead of Database Not Found", func(t *testing.T) {
				h, mux, comments := newFanOutSkipHandler(t, withRegisteredDatabase(aggregateLeaderConfig(), "inventory"))
				serveTruncatedRepoWithChangedSchemaFile(t, mux)

				command.run(h)

				body := requireComment(t, comments, "truncated-tree leader "+command.name+" error")
				assert.Contains(t, body, "Repository Too Large to Search")
				assert.Contains(t, body, "`payments`")
				assert.NotContains(t, body, "Database Not Found")
			})
		})
	}
}

// A deployment that resolves databases dynamically (a data-plane server with a
// target resolver) does not describe what it manages in the databases
// registry, so an empty registry says nothing about a -d database there. Such
// a deployment keeps discovering from the repository, the same exception the
// schema source policy makes for open mode.
func TestTargetResolverDeploymentKeepsDiscoveringDatabaseScopedCommands(t *testing.T) {
	cfg := nonAggregateConfig()
	cfg.TargetResolver = api.TargetResolverConfig{Etre: []api.EtreConfig{{}}}
	h, mux, comments := newFanOutSkipHandler(t, cfg)
	serveSchemaConfigForDatabase(t, mux, "inventory")
	serveCompleteRootConfigTree(t, mux)

	h.handleMultiEnvPlan("octocat/hello-world", 1, "payments", "", 12345, "hubot", false, true, 0)

	body := requireComment(t, comments, "database-not-found plan error on a target-resolver deployment")
	assert.Contains(t, body, "Database Not Found")
	assert.NotContains(t, body, "Database Not Configured")
}
