package webhook

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"

	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

// TestAutoPlanPullRequestPredicate pins which deliveries trigger auto-plan.
// A retarget changes the diff the PR proposes and so must re-plan; the title
// and body edits that arrive under the same action must not.
func TestAutoPlanPullRequestPredicate(t *testing.T) {
	retarget := func(from string) pullRequestPayload {
		var p pullRequestPayload
		p.Action = "edited"
		p.Changes.Base.Ref.From = from
		return p
	}

	tests := []struct {
		name    string
		payload pullRequestPayload
		want    bool
	}{
		{"opened", pullRequestPayload{Action: "opened"}, true},
		{"synchronize", pullRequestPayload{Action: "synchronize"}, true},
		{"reopened", pullRequestPayload{Action: "reopened"}, true},
		{"edited after a retarget", retarget("stacked-parent"), true},
		{"edited without a base change", pullRequestPayload{Action: "edited"}, false},
		{"closed", pullRequestPayload{Action: "closed"}, false},
		{"labeled", pullRequestPayload{Action: "labeled"}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isAutoPlannablePullRequest(tc.payload))
		})
	}
}

// TestAutoPlanCoverageActionsAreUnconditional pins that every action the inbox
// coverage query treats as proof a head was planned is one that plans its head
// on the action alone. HasEventForHead matches on the action column, so an
// action that only sometimes plans — a retarget shares "edited" with title and
// body edits — would let an unrelated delivery stand in as coverage and mask
// the lost delivery the reconciler exists to recover.
func TestAutoPlanCoverageActionsAreUnconditional(t *testing.T) {
	for _, action := range storage.AutoPlanPullRequestActions {
		assert.True(t, isAutoPlannablePullRequest(pullRequestPayload{Action: action}),
			"coverage action %q must plan its head on the action alone", action)
	}
	assert.False(t, slices.Contains(storage.AutoPlanPullRequestActions, "edited"),
		"a retarget plans conditionally, so it cannot be counted as coverage")
}

// TestCheckDatabaseKeysMatchNameAndType pins that a stored check row counts as
// still in the PR only when a discovered config plans the same database under
// the same type. A row recorded under a database's previous type is stale, so
// stale cleanup settles it instead of leaving it to hold the aggregate open.
func TestCheckDatabaseKeysMatchNameAndType(t *testing.T) {
	affected := checkDatabaseKeysForConfigs([]ghclient.DiscoveredConfig{{
		Config: &ghclient.SchemabotConfig{Database: "orders", Type: ghclient.DatabaseTypeStrata},
	}})

	tests := []struct {
		name  string
		check *storage.Check
		want  bool
	}{
		{"same name and type", &storage.Check{DatabaseName: "orders", DatabaseType: storage.DatabaseTypeStrata}, true},
		{"same name and type in another case", &storage.Check{DatabaseName: "Orders", DatabaseType: "STRATA"}, true},
		{"same name under the previous type", &storage.Check{DatabaseName: "orders", DatabaseType: storage.DatabaseTypeMySQL}, false},
		{"another database of the same type", &storage.Check{DatabaseName: "payments", DatabaseType: storage.DatabaseTypeStrata}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, affected[checkDatabaseKeyForCheck(tc.check)])
		})
	}
}

// TestPlannedDatabasesWithoutCheckState pins which databases the PR plans but
// has no stored check row for yet. Stale cleanup leaves the aggregate to their
// plans rather than folding it from the rows it cleaned alone.
func TestPlannedDatabasesWithoutCheckState(t *testing.T) {
	planned := checkDatabaseKeysForConfigs([]ghclient.DiscoveredConfig{
		{Config: &ghclient.SchemabotConfig{Database: "orders", Type: ghclient.DatabaseTypeStrata}},
		{Config: &ghclient.SchemabotConfig{Database: "payments", Type: ghclient.DatabaseTypeMySQL}},
		{Config: &ghclient.SchemabotConfig{Database: "ledger", Type: ghclient.DatabaseTypeMySQL}},
	})
	checks := []*storage.Check{
		{DatabaseName: "orders", DatabaseType: storage.DatabaseTypeMySQL, Environment: "staging"},
		{DatabaseName: "Payments", DatabaseType: "MYSQL", Environment: "staging"},
		{DatabaseName: aggregateSentinel, DatabaseType: aggregateSentinel, Environment: "staging"},
	}

	assert.Equal(t, []checkDatabaseKey{
		{databaseName: "ledger", databaseType: storage.DatabaseTypeMySQL},
		{databaseName: "orders", databaseType: storage.DatabaseTypeStrata},
	}, plannedDatabasesWithoutCheckState(checks, planned),
		"a row under a database's previous type does not stand in for the type the PR plans")
	assert.Empty(t, plannedDatabasesWithoutCheckState(checks, nil))
}
