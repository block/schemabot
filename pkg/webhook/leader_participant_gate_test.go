package webhook

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
)

// fakeCheckRunFinder returns canned results per check name so the aggregate
// leader's participant fold can be exercised without GitHub.
type fakeCheckRunFinder struct {
	results   map[string]*ghclient.CheckRunResult
	untrusted map[string][]string
	errs      map[string]error
}

func (f *fakeCheckRunFinder) FindCheckRunByName(_ context.Context, _, _, checkName string) (*ghclient.CheckRunResult, []string, error) {
	if err, ok := f.errs[checkName]; ok {
		return nil, nil, err
	}
	return f.results[checkName], f.untrusted[checkName], nil
}

// The aggregate leader folds each expected participant's Check Run into its own
// aggregate, failing closed on anything that is not a trusted, completed,
// successful participant check.
func TestParticipantCheckOutcomesFold(t *testing.T) {
	const (
		repo    = "octocat/shared-repo"
		pr      = 7
		env     = "staging"
		headSHA = "abc123"
	)
	expected := []api.ExpectedTenant{{Tenant: "tenant-a", Paths: []string{"services/a"}, CheckName: "SchemaBot Tenant A"}}
	checkName := aggregateCheckNameForEnv("SchemaBot Tenant A", env)

	cases := []struct {
		name           string
		finder         *fakeCheckRunFinder
		wantConclusion string
		wantStatus     string
	}{
		{
			name: "trusted completed success passes",
			finder: &fakeCheckRunFinder{results: map[string]*ghclient.CheckRunResult{
				checkName: {Status: checkStatusCompleted, Conclusion: checkConclusionSuccess},
			}},
			wantConclusion: checkConclusionSuccess,
			wantStatus:     checkStatusCompleted,
		},
		{
			name: "in progress blocks",
			finder: &fakeCheckRunFinder{results: map[string]*ghclient.CheckRunResult{
				checkName: {Status: checkStatusInProgress},
			}},
			wantConclusion: "",
			wantStatus:     checkStatusInProgress,
		},
		{
			name: "queued blocks as in progress",
			finder: &fakeCheckRunFinder{results: map[string]*ghclient.CheckRunResult{
				checkName: {Status: checkStatusQueued},
			}},
			wantConclusion: "",
			wantStatus:     checkStatusInProgress,
		},
		{
			name: "completed failure blocks",
			finder: &fakeCheckRunFinder{results: map[string]*ghclient.CheckRunResult{
				checkName: {Status: checkStatusCompleted, Conclusion: checkConclusionFailure},
			}},
			wantConclusion: checkConclusionFailure,
			wantStatus:     checkStatusCompleted,
		},
		{
			name: "completed action_required blocks as failure",
			finder: &fakeCheckRunFinder{results: map[string]*ghclient.CheckRunResult{
				checkName: {Status: checkStatusCompleted, Conclusion: checkConclusionActionRequired},
			}},
			wantConclusion: checkConclusionFailure,
			wantStatus:     checkStatusCompleted,
		},
		{
			name:           "missing check blocks",
			finder:         &fakeCheckRunFinder{},
			wantConclusion: checkConclusionActionRequired,
			wantStatus:     checkStatusCompleted,
		},
		{
			name: "untrusted-only check blocks",
			finder: &fakeCheckRunFinder{untrusted: map[string][]string{
				checkName: {"some-other-app"},
			}},
			wantConclusion: checkConclusionActionRequired,
			wantStatus:     checkStatusCompleted,
		},
		{
			name: "lookup error blocks",
			finder: &fakeCheckRunFinder{errs: map[string]error{
				checkName: fmt.Errorf("boom"),
			}},
			wantConclusion: checkConclusionActionRequired,
			wantStatus:     checkStatusCompleted,
		},
	}

	h := &Handler{logger: testLogger()}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			checks := h.participantCheckOutcomes(t.Context(), tc.finder, repo, pr, env, headSHA, expected)
			require.Len(t, checks, 1)

			c := checks[0]
			assert.Equal(t, aggregateSentinel, c.DatabaseType, "synthesized row must use the aggregate sentinel type")
			assert.Equal(t, "tenant-a", c.DatabaseName, "synthesized row must be labeled by tenant")
			assert.Equal(t, env, c.Environment)
			assert.Equal(t, headSHA, c.HeadSHA)

			conclusion, status := computeAggregate(checks)
			assert.Equal(t, tc.wantConclusion, conclusion)
			assert.Equal(t, tc.wantStatus, status)
		})
	}
}

// An empty expected set folds to nothing, so a non-leader or a PR touching no
// participant paths never contributes participant rows to the aggregate.
func TestParticipantCheckOutcomesEmptyExpected(t *testing.T) {
	h := &Handler{logger: testLogger()}
	checks := h.participantCheckOutcomes(t.Context(), &fakeCheckRunFinder{}, "octocat/shared-repo", 1, "staging", "sha", nil)
	assert.Empty(t, checks)
}

// A missing participant check name is a fail-closed configuration error: the
// leader cannot resolve the Check Run, so the aggregate blocks.
func TestParticipantCheckOutcomesEmptyCheckNameBlocks(t *testing.T) {
	h := &Handler{logger: testLogger()}
	expected := []api.ExpectedTenant{{Tenant: "tenant-a", Paths: []string{"services/a"}}}
	checks := h.participantCheckOutcomes(t.Context(), &fakeCheckRunFinder{}, "octocat/shared-repo", 1, "staging", "sha", expected)
	require.Len(t, checks, 1)

	conclusion, status := computeAggregate(checks)
	assert.Equal(t, checkConclusionActionRequired, conclusion)
	assert.Equal(t, checkStatusCompleted, status)
}

// When the leader has no allowed environments the participant Check Run uses the
// unscoped base name, mirroring how the aggregate publisher names its own check.
func TestParticipantCheckNameSingleAggregate(t *testing.T) {
	assert.Equal(t, "SchemaBot Tenant A", participantCheckName("SchemaBot Tenant A", aggregateSentinel))
	assert.Equal(t, "SchemaBot Tenant A (production)", participantCheckName("SchemaBot Tenant A", "production"))
}
