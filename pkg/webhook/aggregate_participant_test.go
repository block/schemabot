package webhook

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
)

// A deployment configured as an aggregate participant for a repo stays silent on
// PRs that touch none of its schema (the leader owns the required check), while
// a leader or a sole SchemaBot with no aggregate config keeps posting its
// passing aggregate so branch protection is never wedged.
func TestIsAggregateParticipant(t *testing.T) {
	cfg := &api.ServerConfig{
		Repos: map[string]api.RepoConfig{
			"octocat/shared-repo": {Aggregate: &api.AggregateConfig{
				Role: api.AggregateRoleLeader,
				ExpectedTenants: []api.ExpectedTenant{
					{Tenant: "tenant-b", Paths: []string{"tenant-b/schema"}},
				},
			}},
			"octocat/participant-repo": {Aggregate: &api.AggregateConfig{
				Role: api.AggregateRoleParticipant,
			}},
			"octocat/plain-repo": {},
		},
	}
	h := &Handler{service: api.New(nil, cfg, nil, testLogger())}

	assert.True(t, h.isAggregateParticipant("octocat/participant-repo"),
		"a participant stays silent on non-schema PRs")
	assert.False(t, h.isAggregateParticipant("octocat/shared-repo"),
		"the leader owns the required check and keeps posting")
	assert.False(t, h.isAggregateParticipant("octocat/plain-repo"),
		"a repo with no aggregate config is a sole deployment and keeps posting")
	assert.False(t, h.isAggregateParticipant("octocat/unknown-repo"),
		"an unconfigured repo keeps posting")
}

// An aggregate leader expects a participant's Check Run when a PR renames one of
// that participant's schema files out of its paths: merging the rename stops
// the default branch declaring the participant's tables, so the leader's
// aggregate must fold the participant (fail-closed) rather than pass on "no
// managed schema changes".
func TestLeaderExpectsParticipantsForRenameOutOfParticipantPaths(t *testing.T) {
	cfg := &api.ServerConfig{
		Repos: map[string]api.RepoConfig{
			"octocat/shared-repo": {Aggregate: &api.AggregateConfig{
				Role: api.AggregateRoleLeader,
				ExpectedTenants: []api.ExpectedTenant{
					{Tenant: "tenant-b", Paths: []string{"tenant-b/schema"}},
				},
			}},
		},
	}
	h := &Handler{service: api.New(nil, cfg, nil, testLogger())}

	tests := []struct {
		name string
		file ghclient.PRFile
	}{
		{"moved out of the participant's directory", ghclient.PRFile{Filename: "archive/users.sql", PreviousFilename: "tenant-b/schema/widgets/users.sql", Status: "renamed"}},
		{"renamed to a non-schema file elsewhere", ghclient.PRFile{Filename: "docs/users.md", PreviousFilename: "tenant-b/schema/widgets/users.sql", Status: "renamed"}},
	}
	// listed and proposed are the same list, so no participant's run is read.
	participantsFor := func(t *testing.T, files []ghclient.PRFile) []api.ExpectedTenant {
		t.Helper()
		expected, err := h.leaderParticipantsForPR(t.Context(), nil, "octocat/shared-repo", 1, "abc123", files, files)
		require.NoError(t, err)
		return expected
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expected := participantsFor(t, []ghclient.PRFile{tt.file})
			require.Len(t, expected, 1)
			assert.Equal(t, "tenant-b", expected[0].Tenant)
		})
	}

	assert.Empty(t, participantsFor(t, []ghclient.PRFile{
		{Filename: "docs/b.md", PreviousFilename: "docs/a.md", Status: "renamed"},
	}), "a rename outside every participant's paths expects no participant")
}

// stubCheckRunFinder answers every participant Check Run read with the same
// result, untrusted App slugs and error, and counts the reads.
type stubCheckRunFinder struct {
	result    *ghclient.CheckRunResult
	untrusted []string
	err       error
	reads     int
}

func (f *stubCheckRunFinder) FindCheckRunByName(context.Context, string, string, string) (*ghclient.CheckRunResult, []string, error) {
	f.reads++
	return f.result, f.untrusted, f.err
}

// A participant that the PR's raw listing selects but its proposed files no
// longer do stays expected exactly when the fold would block on it: a run on
// the head that is not passing, a same-named run only from untrusted Apps, or
// no check name to read. It is dropped when it has no run or a passing one, and
// a failed read is an error so the caller publishes nothing.
func TestLeaderParticipantsForPRKeepsADroppedParticipantTheFoldWouldBlockOn(t *testing.T) {
	cfg := &api.ServerConfig{
		Repos: map[string]api.RepoConfig{
			"octocat/shared-repo": {Aggregate: &api.AggregateConfig{
				Role: api.AggregateRoleLeader,
				ExpectedTenants: []api.ExpectedTenant{
					{Tenant: "tenant-b", Paths: []string{"tenant-b/schema"}, CheckName: "SchemaBot Tenant B"},
					{Tenant: "tenant-c", Paths: []string{"tenant-c/schema"}},
				},
			}},
		},
	}
	h := &Handler{service: api.New(nil, cfg, nil, testLogger()), logger: testLogger()}
	listedB := []ghclient.PRFile{{Filename: "tenant-b/schema/schemabot.yaml", Status: "removed"}}

	tests := []struct {
		name       string
		finder     *stubCheckRunFinder
		listed     []ghclient.PRFile
		wantTenant string
		wantReads  int
		wantErr    bool
	}{
		{name: "no run is dropped", finder: &stubCheckRunFinder{}, listed: listedB, wantReads: 1},
		{name: "passing run is dropped", finder: &stubCheckRunFinder{result: &ghclient.CheckRunResult{Status: checkStatusCompleted, Conclusion: checkConclusionSuccess}}, listed: listedB, wantReads: 1},
		{name: "in-progress run is kept", finder: &stubCheckRunFinder{result: &ghclient.CheckRunResult{Status: checkStatusInProgress}}, listed: listedB, wantTenant: "tenant-b", wantReads: 1},
		{name: "failed run is kept", finder: &stubCheckRunFinder{result: &ghclient.CheckRunResult{Status: checkStatusCompleted, Conclusion: checkConclusionFailure}}, listed: listedB, wantTenant: "tenant-b", wantReads: 1},
		{name: "run only from an untrusted App is kept", finder: &stubCheckRunFinder{untrusted: []string{"some-other-app"}}, listed: listedB, wantTenant: "tenant-b", wantReads: 1},
		{name: "participant without a check name is kept without a read", finder: &stubCheckRunFinder{}, listed: []ghclient.PRFile{{Filename: "tenant-c/schema/schemabot.yaml", Status: "removed"}}, wantTenant: "tenant-c"},
		{name: "failed read is an error", finder: &stubCheckRunFinder{err: assert.AnError}, listed: listedB, wantReads: 1, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expected, err := h.leaderParticipantsForPR(t.Context(), tt.finder, "octocat/shared-repo", 1, "abc123", tt.listed, nil)
			assert.Equal(t, tt.wantReads, tt.finder.reads)
			if tt.wantErr {
				require.ErrorIs(t, err, assert.AnError)
				return
			}
			require.NoError(t, err)
			if tt.wantTenant == "" {
				assert.Empty(t, expected)
				return
			}
			require.Len(t, expected, 1)
			assert.Equal(t, tt.wantTenant, expected[0].Tenant)
		})
	}
}

// A leader that publishes one aggregate per environment resolves a participant
// the PR no longer proposes work for separately for each environment. The
// participant is dropped from an environment where its run on the head passed,
// and it is neither read nor expected for an environment it publishes no check
// for, even when a failed run exists under that environment's name.
func TestScopedLeaderResolvesADroppedParticipantForEachEnvironment(t *testing.T) {
	const repo = "octocat/shared-repo"
	stagingRun := aggregateCheckNameForEnv("SchemaBot Tenant B", "staging")
	productionRun := aggregateCheckNameForEnv("SchemaBot Tenant B", "production")
	passed := &ghclient.CheckRunResult{Status: checkStatusCompleted, Conclusion: checkConclusionSuccess}
	failed := &ghclient.CheckRunResult{Status: checkStatusCompleted, Conclusion: checkConclusionFailure}

	tests := []struct {
		name string
		// environments is the participant's configured coverage; nil covers
		// every environment.
		environments []string
		results      map[string]*ghclient.CheckRunResult
		wantReads    []string
		// wantExpected lists the environments the participant stays expected for.
		wantExpected []string
	}{
		{
			name:      "passing runs drop the participant from both environments",
			results:   map[string]*ghclient.CheckRunResult{stagingRun: passed, productionRun: passed},
			wantReads: []string{stagingRun, productionRun},
		},
		{
			name:         "a passing staging run drops only staging",
			results:      map[string]*ghclient.CheckRunResult{stagingRun: passed, productionRun: failed},
			wantReads:    []string{stagingRun, productionRun},
			wantExpected: []string{"production"},
		},
		{
			name:         "an environment the participant does not publish is not read",
			environments: []string{"production"},
			results:      map[string]*ghclient.CheckRunResult{stagingRun: failed, productionRun: failed},
			wantReads:    []string{productionRun},
			wantExpected: []string{"production"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &api.ServerConfig{
				AllowedEnvironments: []string{"staging", "production"},
				Repos: map[string]api.RepoConfig{
					repo: {Aggregate: &api.AggregateConfig{
						Role: api.AggregateRoleLeader,
						ExpectedTenants: []api.ExpectedTenant{{
							Tenant:       "tenant-b",
							Paths:        []string{"tenant-b/schema"},
							CheckName:    "SchemaBot Tenant B",
							Environments: tt.environments,
						}},
					}},
				},
			}
			h := &Handler{service: api.New(nil, cfg, nil, testLogger()), logger: testLogger()}
			finder := &fakeCheckRunFinder{results: tt.results}
			scope := &aggregatePRScope{listed: []ghclient.PRFile{{Filename: "tenant-b/schema/schemabot.yaml", Status: "removed"}}}

			byEnvironment, err := h.expectedParticipantsByEnvironment(t.Context(), finder, repo, 1, "abc123", scope)
			require.NoError(t, err)
			assert.Equal(t, tt.wantReads, finder.calls)
			for _, env := range cfg.AllowedEnvironments {
				if !slices.Contains(tt.wantExpected, env) {
					assert.Empty(t, byEnvironment[env], env)
					continue
				}
				require.Len(t, byEnvironment[env], 1, env)
				assert.Equal(t, "tenant-b", byEnvironment[env][0].Tenant, env)
			}
		})
	}
}

// The predicate tolerates a handler with no server config (returns false, so the
// deployment keeps its existing posting behaviour rather than going silent).
func TestIsAggregateParticipantNoConfig(t *testing.T) {
	h := &Handler{service: api.New(nil, nil, nil, testLogger())}
	assert.False(t, h.isAggregateParticipant("octocat/any-repo"))
}
