package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// A plan whose repository config withheld namespaces via ignore_namespaces
// discloses the exclusion on the comment, so a reviewer sees which namespaces
// the plan deliberately did not reconcile.
func TestRenderPlanComment_IgnoredNamespacesDisclosed(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		IgnoredNamespaces: []string{"fixtures_staging", "local_fixtures"},
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}},
	})

	assert.Contains(t, out, "ℹ️ Namespaces excluded from this plan by `ignore_namespaces`: `fixtures_staging`, `local_fixtures`")
}

// The disclosure renders on a no-changes plan too: without it, "no schema
// changes detected" is indistinguishable from "a namespace's changes were
// withheld by config" — the exact ambiguity the disclosure exists to remove.
func TestRenderPlanComment_IgnoredNamespacesDisclosedOnNoChanges(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		IgnoredNamespaces: []string{"local_fixtures"},
	})

	assert.Contains(t, out, "ℹ️ Namespaces excluded from this plan by `ignore_namespaces`: `local_fixtures`")
	assert.Contains(t, out, "✅ **No schema changes detected**")
}

// Plans from repositories without ignore_namespaces render unchanged.
func TestRenderPlanComment_NoIgnoredNamespacesNoDisclosure(t *testing.T) {
	out := RenderPlanComment(PlanCommentData{
		Database: "testapp", Environment: "staging", IsMySQL: true,
		Changes: []KeyspaceChangeData{{
			Keyspace:   "testapp",
			Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
		}},
	})

	assert.NotContains(t, out, "ignore_namespaces")
}

// ignore_namespaces entries can resolve differently per environment ($ENV
// substitution), so environments whose exclusions differ must not deduplicate
// into one shared section — that would show one environment's disclosure for
// both. Each environment's section carries its own list.
func TestRenderMultiEnvPlanComment_IgnoredNamespacesBlockDedup(t *testing.T) {
	changes := []KeyspaceChangeData{{
		Keyspace:   "testapp",
		Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
	}}
	out := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database: "testapp", DatabaseType: "mysql", IsMySQL: true,
		Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{
			"staging":    {Environment: "staging", IsMySQL: true, Changes: changes, IgnoredNamespaces: []string{"fixtures_staging"}},
			"production": {Environment: "production", IsMySQL: true, Changes: changes, IgnoredNamespaces: []string{"fixtures_production"}},
		},
	})

	assert.Contains(t, out, "### Staging")
	assert.Contains(t, out, "### Production")
	assert.Contains(t, out, "ℹ️ Namespaces excluded from this plan by `ignore_namespaces`: `fixtures_staging`")
	assert.Contains(t, out, "ℹ️ Namespaces excluded from this plan by `ignore_namespaces`: `fixtures_production`")
}

// Environments with identical DDL and identical exclusions still deduplicate
// into one combined section, disclosing the shared exclusion once.
func TestRenderMultiEnvPlanComment_IgnoredNamespacesIdenticalDedup(t *testing.T) {
	changes := []KeyspaceChangeData{{
		Keyspace:   "testapp",
		Statements: []string{"ALTER TABLE `users` ADD COLUMN `email` varchar(255)"},
	}}
	out := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database: "testapp", DatabaseType: "mysql", IsMySQL: true,
		Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{
			"staging":    {Environment: "staging", IsMySQL: true, Changes: changes, IgnoredNamespaces: []string{"local_fixtures"}},
			"production": {Environment: "production", IsMySQL: true, Changes: changes, IgnoredNamespaces: []string{"local_fixtures"}},
		},
	})

	assert.Contains(t, out, "### Staging & Production")
	assert.Equal(t, 1, strings.Count(out, "ℹ️ Namespaces excluded from this plan by `ignore_namespaces`: `local_fixtures`"))
}

// The all-environments-clean short circuit has no per-environment sections, so
// the disclosure renders alongside the combined no-changes message — an
// all-clean result is exactly where a reviewer needs to see that a namespace
// was withheld rather than genuinely unchanged.
func TestRenderMultiEnvPlanComment_IgnoredNamespacesOnAllClean(t *testing.T) {
	out := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database: "testapp", DatabaseType: "mysql", IsMySQL: true,
		Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{
			"staging":    {Environment: "staging", IsMySQL: true, IgnoredNamespaces: []string{"local_fixtures"}},
			"production": {Environment: "production", IsMySQL: true, IgnoredNamespaces: []string{"local_fixtures"}},
		},
	})

	assert.Contains(t, out, "✅ **No schema changes detected** for any environment.")
	assert.Equal(t, 1, strings.Count(out, "ℹ️ Namespaces excluded from this plan by `ignore_namespaces`: `local_fixtures`"))
}

// When the all-clean environments excluded different namespaces, the combined
// message discloses each environment's own list, labeled by environment.
func TestRenderMultiEnvPlanComment_IgnoredNamespacesOnAllCleanPerEnv(t *testing.T) {
	out := RenderMultiEnvPlanComment(MultiEnvPlanCommentData{
		Database: "testapp", DatabaseType: "mysql", IsMySQL: true,
		Environments: []string{"staging", "production"},
		Plans: map[string]*PlanCommentData{
			"staging":    {Environment: "staging", IsMySQL: true, IgnoredNamespaces: []string{"fixtures_staging"}},
			"production": {Environment: "production", IsMySQL: true, IgnoredNamespaces: []string{"fixtures_production"}},
		},
	})

	assert.Contains(t, out, "✅ **No schema changes detected** for any environment.")
	assert.Contains(t, out, "ℹ️ **Staging**: namespaces excluded from this plan by `ignore_namespaces`: `fixtures_staging`")
	assert.Contains(t, out, "ℹ️ **Production**: namespaces excluded from this plan by `ignore_namespaces`: `fixtures_production`")
}
