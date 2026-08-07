//go:build integration

// Merge-gate recovery when an apply finishes on a commit the PR has moved past.

package webhook

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	gh "github.com/google/go-github/v86/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

const staleGateSchemaSQL = "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT,\n  `name` varchar(255) NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"

// seedTerminalApplyOnSupersededCommit records what an apply leaves behind when
// the PR moves on while it runs: a terminal apply, and the stored check row it
// owns still pinned to the commit it ran against. The fake PR head is a later
// commit, so the row is superseded the moment it is written.
func seedTerminalApplyOnSupersededCommit(t *testing.T, svc *api.Service, dbName, applyIdentifier, storedHeadSHA string) *storage.Apply {
	t.Helper()
	ctx := t.Context()

	apply := &storage.Apply{
		ApplyIdentifier: applyIdentifier,
		Database:        dbName,
		DatabaseType:    "mysql",
		Environment:     "staging",
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		State:           state.Apply.Completed,
		Engine:          storage.EngineSpirit,
	}
	applyID, err := svc.Storage().Applies().Create(ctx, apply)
	require.NoError(t, err)
	apply.ID = applyID

	require.NoError(t, svc.Storage().Checks().Upsert(ctx, &storage.Check{
		Repository:   "octocat/hello-world",
		PullRequest:  1,
		HeadSHA:      storedHeadSHA,
		Environment:  "staging",
		DatabaseType: "mysql",
		DatabaseName: dbName,
		CheckRunID:   42,
		ApplyID:      applyID,
		HasChanges:   true,
		Status:       checkStatusInProgress,
	}))
	return apply
}

// startFakeGitHubForStaleGate wires a fake GitHub that serves a plannable PR
// whose head is "abc123", so every stored row keyed to an earlier commit is
// superseded.
func startFakeGitHubForStaleGate(t *testing.T, dbName string) (*gh.Client, *planFlowResult) {
	t.Helper()
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	client := gh.NewClient(nil)
	var err error
	client.BaseURL, err = url.Parse(server.URL + "/")
	require.NoError(t, err)

	schemabotConfig := fmt.Sprintf("database: %s\ntype: mysql\n", dbName)
	result := setupFakeGitHubForPlan(t, mux, map[string]string{"users.sql": staleGateSchemaSQL}, schemabotConfig, dbName)
	return client, result
}

// An apply that reaches a terminal state after the PR has moved on leaves its
// stored check row pinned to the commit it ran against. That row contributes a
// blocking placeholder to the aggregate, and nothing else re-keys it: the push
// that moved the head left the row alone because an apply still owned it, and
// folding again on the superseded commit only republishes the same placeholder.
// The apply's own terminal refresh re-plans on the current commit instead, so
// the gate re-opens with no operator command.
func TestE2EMergeGateReopensAfterTerminalApplyOnSupersededCommit(t *testing.T) {
	dbName := "webhook_stale_gate_replan"
	svc := setupE2EService(t, dbName)
	ctx := t.Context()

	apply := seedTerminalApplyOnSupersededCommit(t, svc, dbName, "apply-superseded-head", "oldsha999")
	client, _ := startFakeGitHubForStaleGate(t, dbName)

	h := newE2EHandler(t, svc, client)
	h.refreshChecksForTerminalApply(ctx, apply, "terminal apply on superseded commit")

	// The re-plan's per-database work is dispatched, so poll for the row to be
	// re-keyed. Re-keying is the whole point: while the row names the old commit
	// the aggregate can only publish a blocking placeholder over it.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		check, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", "mysql", dbName)
		assert.NoError(c, err)
		if !assert.NotNil(c, check) {
			return
		}
		assert.Equal(c, "abc123", check.HeadSHA,
			"the terminal apply's check row is still pinned to the commit it ran against")
	}, webhookIntegrationPollDeadline, 200*time.Millisecond)
}

// A started apply is authoritative for its PR's check state, so the re-plan
// must not run while another apply on the same PR is still in flight —
// replaying the plan flow over one could replace an apply-owned merge block
// with a fresh passing plan. The gate stays shut, which is the safe direction.
func TestE2ETerminalApplyOnSupersededCommitDefersToInFlightApply(t *testing.T) {
	dbName := "webhook_stale_gate_inflight"
	svc := setupE2EService(t, dbName)
	ctx := t.Context()

	apply := seedTerminalApplyOnSupersededCommit(t, svc, dbName, "apply-superseded-terminal", "oldsha999")

	_, err := svc.Storage().Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply-superseded-inflight",
		Database:        dbName,
		DatabaseType:    "mysql",
		Environment:     "staging",
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		State:           state.Apply.Running,
		Engine:          storage.EngineSpirit,
	})
	require.NoError(t, err)

	client, _ := startFakeGitHubForStaleGate(t, dbName)
	h := newE2EHandler(t, svc, client)
	installClient := ghclient.NewInstallationClient(client, h.logger)

	assert.False(t, h.replanAfterTerminalApplyOnSupersededHead(ctx, installClient, apply, "oldsha999"),
		"a re-plan ran while another apply owned the PR's check state")

	check, err := svc.Storage().Checks().Get(ctx, "octocat/hello-world", 1, "staging", "mysql", dbName)
	require.NoError(t, err)
	require.NotNil(t, check)
	assert.Equal(t, "oldsha999", check.HeadSHA, "the in-flight apply's PR was re-planned anyway")
}

// A row already keyed to the PR's current commit is not superseded, so the
// ordinary fold owns it and the re-plan stands down. Without this the terminal
// refresh of every apply that finishes before its PR moves would replay the
// plan flow for no reason.
func TestE2ETerminalApplyOnCurrentCommitDoesNotReplan(t *testing.T) {
	dbName := "webhook_stale_gate_current"
	svc := setupE2EService(t, dbName)
	ctx := t.Context()

	apply := seedTerminalApplyOnSupersededCommit(t, svc, dbName, "apply-current-head", "abc123")
	client, _ := startFakeGitHubForStaleGate(t, dbName)

	h := newE2EHandler(t, svc, client)
	installClient := ghclient.NewInstallationClient(client, h.logger)

	assert.False(t, h.replanAfterTerminalApplyOnSupersededHead(ctx, installClient, apply, "abc123"),
		"a re-plan ran for a row already keyed to the PR's current commit")
}
