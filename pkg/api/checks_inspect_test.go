package api

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/checkstate"
	ghclient "github.com/block/schemabot/pkg/github"
	"github.com/block/schemabot/pkg/storage"
)

type inspectCheckStore struct {
	storage.CheckStore
	checks []*storage.Check
	err    error
}

func (s *inspectCheckStore) GetByPR(context.Context, string, int) ([]*storage.Check, error) {
	return s.checks, s.err
}

type inspectApplyStore struct {
	storage.ApplyStore
	applies map[int64]*storage.Apply
	err     error
}

func (s *inspectApplyStore) Get(_ context.Context, id int64) (*storage.Apply, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.applies[id], nil
}

type inspectStorage struct {
	mockStorage
	checks  *inspectCheckStore
	applies *inspectApplyStore
}

func (s *inspectStorage) Checks() storage.CheckStore  { return s.checks }
func (s *inspectStorage) Applies() storage.ApplyStore { return s.applies }

type inspectGitHubClient struct {
	prInfo        *ghclient.PullRequestInfo
	prErr         error
	runs          map[string]*ghclient.CheckRunResult
	untrustedApps map[string][]string
	runErr        error
	prCalls       int
}

func (c *inspectGitHubClient) FetchPullRequestNoCache(context.Context, string, int) (*ghclient.PullRequestInfo, error) {
	c.prCalls++
	return c.prInfo, c.prErr
}

func (c *inspectGitHubClient) FindCheckRunByName(_ context.Context, _, _, name string) (*ghclient.CheckRunResult, []string, error) {
	if c.runErr != nil {
		return nil, nil, c.runErr
	}
	return c.runs[name], c.untrustedApps[name], nil
}

func inspectTestConfig() *ServerConfig {
	return &ServerConfig{AllowedEnvironments: []string{"staging", "production"}}
}

// A request shape that could not name real check state is refused before any
// GitHub work. A mistyped environment is the one worth refusing loudly: it
// would otherwise narrow the response to nothing and read as "this pull
// request has no check state", which is the opposite of the answer.
func TestExecuteChecksInspectValidation(t *testing.T) {
	t.Parallel()

	cfg := inspectTestConfig()
	store := &inspectStorage{checks: &inspectCheckStore{}, applies: &inspectApplyStore{}}

	_, err := executeChecksInspect(t.Context(), cfg, store, ChecksInspectRequest{PullRequest: 1}, discardLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "repo is required")

	_, err = executeChecksInspect(t.Context(), cfg, store, ChecksInspectRequest{Repo: "octo/repo"}, discardLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pull_request must be positive")

	_, err = executeChecksInspect(t.Context(), cfg, store, ChecksInspectRequest{Repo: "octo/repo", PullRequest: 7, Environment: "prod"}, discardLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), `environment "prod" is not one this instance handles`)

	_, err = executeChecksInspect(t.Context(), cfg, nil, ChecksInspectRequest{Repo: "octo/repo", PullRequest: 7}, discardLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "storage is not configured")
}

// The shape an operator reaches for this command to explain: an apply
// succeeded, the pull request head moved while it held the check, and the
// stored row still names the apply's commit. The row must come back marked as
// not covering the head, carrying the apply that recorded it, and read as a
// state SchemaBot resolves rather than one owing an operator anything.
func TestInspectChecksReportsSuccessfulApplyOnAnOlderCommit(t *testing.T) {
	t.Parallel()

	const (
		applySHA = "e22e4cefaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		headSHA  = "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	store := &inspectStorage{
		checks: &inspectCheckStore{checks: []*storage.Check{{
			Repository: "octo/repo", PullRequest: 709, HeadSHA: applySHA,
			Environment: "production", DatabaseType: "mysql", DatabaseName: "widgets",
			ApplyID: 42, CheckRunID: 99, Status: checkstate.StatusCompleted,
			Conclusion: checkstate.ConclusionSuccess,
			UpdatedAt:  time.Date(2026, 9, 10, 5, 27, 55, 0, time.UTC),
		}}},
		applies: &inspectApplyStore{applies: map[int64]*storage.Apply{
			42: {ApplyIdentifier: "apply-abc123", State: "completed"},
		}},
	}
	client := &inspectGitHubClient{prInfo: &ghclient.PullRequestInfo{HeadSHA: headSHA, State: "open"}}

	response, err := inspectChecks(t.Context(), inspectTestConfig(), store, client,
		ChecksInspectRequest{Repo: "octo/repo", PullRequest: 709}, discardLogger())
	require.NoError(t, err)

	assert.Equal(t, headSHA, response.HeadSHA)
	assert.Equal(t, "open", response.PRState)
	assert.Equal(t, 1, client.prCalls, "every row is read against one head, so the head is read once")

	require.Len(t, response.Rows, 1)
	row := response.Rows[0]
	assert.Equal(t, "widgets", row.Database)
	assert.Equal(t, applySHA, row.RecordedSHA)
	assert.False(t, row.CoversHead)
	assert.Equal(t, int64(99), row.CheckRunID)
	assert.Equal(t, "apply-abc123", row.ApplyIdentifier)
	assert.Equal(t, "completed", row.ApplyState)
	assert.Equal(t, "2026-09-10T05:27:55Z", row.UpdatedAt)
	assert.Equal(t, checkstate.ReasonAwaitingReplanAfterApply, row.Reason)
	assert.True(t, row.Blocking)
	assert.True(t, row.SelfConverging, "a successful apply's row converges once a plan lands for the head")
}

// A block a terminal apply left behind is the case where waiting is the wrong
// response, so it must be reported as owing an operator a reconciliation
// rather than as something still in flight.
func TestInspectChecksReportsReconciliationOwedForRetainedBlock(t *testing.T) {
	t.Parallel()

	const headSHA = "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	store := &inspectStorage{
		checks: &inspectCheckStore{checks: []*storage.Check{{
			Repository: "octo/repo", PullRequest: 709, HeadSHA: headSHA,
			Environment: "production", DatabaseType: "mysql", DatabaseName: "widgets",
			ApplyID: 42, Status: checkstate.StatusCompleted,
			Conclusion:     checkstate.ConclusionActionRequired,
			BlockingReason: "rollback_completed",
		}}},
		applies: &inspectApplyStore{applies: map[int64]*storage.Apply{
			42: {ApplyIdentifier: "apply-abc123", State: "rolled_back"},
		}},
	}
	client := &inspectGitHubClient{prInfo: &ghclient.PullRequestInfo{HeadSHA: headSHA, State: "open"}}

	response, err := inspectChecks(t.Context(), inspectTestConfig(), store, client,
		ChecksInspectRequest{Repo: "octo/repo", PullRequest: 709}, discardLogger())
	require.NoError(t, err)

	require.Len(t, response.Rows, 1)
	row := response.Rows[0]
	assert.True(t, row.CoversHead)
	assert.Equal(t, "rollback_completed", row.BlockingReason)
	assert.Equal(t, checkstate.ReasonReconciliationOwed, row.Reason)
	assert.True(t, row.Blocking)
	assert.False(t, row.SelfConverging)
}

// Rows come back in a stable order, an environment filter narrows them, and
// the aggregate row is reported alongside the per-database rows rather than
// hidden: what the aggregate is holding open is the question being asked.
func TestInspectChecksSortsRowsAndHonorsEnvironmentFilter(t *testing.T) {
	t.Parallel()

	const headSHA = "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	rows := []*storage.Check{
		{Environment: "staging", DatabaseType: "mysql", DatabaseName: "widgets", HeadSHA: headSHA, Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionSuccess},
		{Environment: "production", DatabaseType: "mysql", DatabaseName: "gadgets", HeadSHA: headSHA, Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionSuccess},
		{Environment: "production", DatabaseType: checkstate.AggregateSentinel, DatabaseName: checkstate.AggregateSentinel, HeadSHA: headSHA, Status: checkstate.StatusInProgress},
		{Environment: "production", DatabaseType: "mysql", DatabaseName: "widgets", HeadSHA: headSHA, Status: checkstate.StatusCompleted, Conclusion: checkstate.ConclusionSuccess},
	}
	store := &inspectStorage{checks: &inspectCheckStore{checks: rows}, applies: &inspectApplyStore{}}
	client := &inspectGitHubClient{prInfo: &ghclient.PullRequestInfo{HeadSHA: headSHA, State: "open"}}

	all, err := inspectChecks(t.Context(), inspectTestConfig(), store, client,
		ChecksInspectRequest{Repo: "octo/repo", PullRequest: 709}, discardLogger())
	require.NoError(t, err)
	require.Len(t, all.Rows, 4)
	assert.Equal(t, []string{"production/" + checkstate.AggregateSentinel, "production/gadgets", "production/widgets", "staging/widgets"},
		[]string{
			all.Rows[0].Environment + "/" + all.Rows[0].Database,
			all.Rows[1].Environment + "/" + all.Rows[1].Database,
			all.Rows[2].Environment + "/" + all.Rows[2].Database,
			all.Rows[3].Environment + "/" + all.Rows[3].Database,
		})
	assert.Equal(t, checkstate.ReasonAggregateRollup, all.Rows[0].Reason)

	staging, err := inspectChecks(t.Context(), inspectTestConfig(), store, client,
		ChecksInspectRequest{Repo: "octo/repo", PullRequest: 709, Environment: "staging"}, discardLogger())
	require.NoError(t, err)
	require.Len(t, staging.Rows, 1)
	assert.Equal(t, "staging", staging.Rows[0].Environment)
}

// Storage failures are the inspection's answer, not a partial one: a response
// missing rows would read as a pull request with less check state than it has.
func TestInspectChecksFailsWhenStoredStateCannotBeRead(t *testing.T) {
	t.Parallel()

	store := &inspectStorage{
		checks:  &inspectCheckStore{err: errors.New("storage down")},
		applies: &inspectApplyStore{},
	}
	client := &inspectGitHubClient{prInfo: &ghclient.PullRequestInfo{HeadSHA: "abc", State: "open"}}

	_, err := inspectChecks(t.Context(), inspectTestConfig(), store, client,
		ChecksInspectRequest{Repo: "octo/repo", PullRequest: 709}, discardLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read stored check state for octo/repo#709")
}

// An apply that cannot be resolved costs the row its identifier and nothing
// else. The row's own fields are what explain the state, and an inspection
// that returns nothing is worse than one returning everything but a name.
func TestInspectChecksReportsRowWhenItsApplyCannotBeResolved(t *testing.T) {
	t.Parallel()

	const headSHA = "43da12bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	store := &inspectStorage{
		checks: &inspectCheckStore{checks: []*storage.Check{{
			Environment: "production", DatabaseType: "mysql", DatabaseName: "widgets",
			HeadSHA: headSHA, ApplyID: 42, Status: checkstate.StatusInProgress,
		}}},
		applies: &inspectApplyStore{err: errors.New("storage down")},
	}
	client := &inspectGitHubClient{prInfo: &ghclient.PullRequestInfo{HeadSHA: headSHA, State: "open"}}

	response, err := inspectChecks(t.Context(), inspectTestConfig(), store, client,
		ChecksInspectRequest{Repo: "octo/repo", PullRequest: 709}, discardLogger())
	require.NoError(t, err)
	require.Len(t, response.Rows, 1)
	assert.Empty(t, response.Rows[0].ApplyIdentifier)
	assert.Equal(t, checkstate.ReasonApplyOwnerUnknown, response.Rows[0].Reason,
		"an owner that could not be read is reported as unknown, never as one more running apply")
	assert.True(t, response.Rows[0].Blocking)
	assert.False(t, response.Rows[0].SelfConverging)
}

// Every Check Run this deployment publishes is reported on its own. A
// deployment that publishes one check per environment has one required check
// per name, so a present run for one environment must never stand in for an
// absent run for another: that absence is what branch protection is waiting
// on, and the one backfill addresses.
func TestCheckRunsOnHeadReportEveryExpectedNameSeparately(t *testing.T) {
	t.Parallel()

	cfg := inspectTestConfig()
	names := webhookMissingCheckNames(cfg, "octo/repo", "", "")
	require.Len(t, names, 2, "this test needs a deployment publishing one check per environment")

	started := time.Date(2026, 9, 10, 5, 16, 44, 0, time.UTC)
	client := &inspectGitHubClient{runs: map[string]*ghclient.CheckRunResult{
		names[0]: {ID: 102754133862, Name: names[0], Status: checkstate.StatusInProgress, StartedAt: started},
	}}

	got := checkRunsOnHead(t.Context(), cfg, client, "octo/repo", "43da12bb", "", discardLogger())
	require.Len(t, got.found, 1)
	assert.Equal(t, names[0], got.found[0].Name)
	assert.Equal(t, int64(102754133862), got.found[0].CheckRunID)
	assert.Equal(t, checkstate.StatusInProgress, got.found[0].Status)
	assert.Equal(t, "2026-09-10T05:16:44Z", got.found[0].StartedAt)
	assert.Equal(t, []string{names[1]}, got.missing,
		"the run that is not there is the finding; the one that is must not hide it")
	assert.Empty(t, got.unreadable)
	assert.Empty(t, got.untrustedConflicts)

	got = checkRunsOnHead(t.Context(), cfg, &inspectGitHubClient{}, "octo/repo", "43da12bb", "", discardLogger())
	assert.Empty(t, got.found)
	assert.Equal(t, names, got.missing)
	assert.Empty(t, got.unreadable)

	got = checkRunsOnHead(t.Context(), cfg, client, "octo/repo", "", "", discardLogger())
	assert.Empty(t, got.found, "no head commit means no run to report")
	assert.Empty(t, got.missing)
	assert.Empty(t, got.unreadable)

	// A name GitHub could not be read for is unknown, not absent: reporting it
	// as missing would recommend recreating a Check Run that may be sitting on
	// the head, and dropping it would let a GitHub outage read as no gap.
	got = checkRunsOnHead(t.Context(), cfg, &inspectGitHubClient{runErr: errors.New("github down")}, "octo/repo", "43da12bb", "", discardLogger())
	assert.Empty(t, got.found)
	assert.Empty(t, got.missing)
	assert.Equal(t, names, got.unreadable)
}

// A name only an untrusted app has a run under is missing and conflicted at
// once. The trusted run really is absent, so the backfill is still the action;
// but branch protection may be reading the untrusted run, which no backfill
// touches, so an operator told only that the run is missing would recreate it
// and be left wondering why the gate did not move.
func TestCheckRunsOnHeadReportsAnUntrustedRunUnderAMissingName(t *testing.T) {
	t.Parallel()

	cfg := inspectTestConfig()
	names := webhookMissingCheckNames(cfg, "octo/repo", "", "")
	require.Len(t, names, 2, "this test needs a deployment publishing one check per environment")

	client := &inspectGitHubClient{untrustedApps: map[string][]string{names[0]: {"other-app"}}}
	got := checkRunsOnHead(t.Context(), cfg, client, "octo/repo", "43da12bb", "", discardLogger())

	assert.Empty(t, got.found)
	assert.Equal(t, names, got.missing, "an untrusted run does not make the trusted one present")
	assert.Equal(t, []string{names[0]}, got.untrustedConflicts,
		"only the name an untrusted app is sitting under is a conflict")
	assert.Empty(t, got.unreadable)
}

// A repository this deployment publishes no Check Runs for has no run to
// recreate, so no expected name is reported as missing there. Naming one
// would send an operator after a gap that is the configuration.
func TestCheckRunsOnHeadReportsNoMissingNameWhenChecksAreDisabled(t *testing.T) {
	t.Parallel()

	checksOff := false
	cfg := inspectTestConfig()
	cfg.Repos = map[string]RepoConfig{"octo/repo": {EnableChecks: &checksOff}}

	got := checkRunsOnHead(t.Context(), cfg, &inspectGitHubClient{}, "octo/repo", "43da12bb", "", discardLogger())
	assert.Empty(t, got.found)
	assert.Empty(t, got.missing, "absence is the configuration, not a gap the backfill closes")
	assert.Empty(t, got.unreadable)
}

// An inspection is a read, so it is asked for over GET with the target in the
// query string. The pull request may be named by number beside a repo, or by
// any address that carries both, so a caller holding a pull request URL never
// has to take it apart first.
func TestChecksInspectRequestFromQueryAcceptsEveryWayOfNamingAPullRequest(t *testing.T) {
	accepted := []struct {
		name        string
		query       url.Values
		repo        string
		pullRequest int
		environment string
	}{
		{
			name:        "repo and number",
			query:       url.Values{"repo": {"acme/store"}, "pull_request": {"412"}},
			repo:        "acme/store",
			pullRequest: 412,
		},
		{
			name:        "a pull request URL carries the repository",
			query:       url.Values{"pull_request": {"https://github.com/acme/store/pull/412"}},
			repo:        "acme/store",
			pullRequest: 412,
		},
		{
			name:        "the comment form carries the repository",
			query:       url.Values{"pull_request": {"acme/store#412"}},
			repo:        "acme/store",
			pullRequest: 412,
		},
		{
			name:        "a URL beside the repository it names",
			query:       url.Values{"repo": {"acme/store"}, "pull_request": {"https://github.com/acme/store/pull/412"}},
			repo:        "acme/store",
			pullRequest: 412,
		},
		{
			name:        "an environment narrows the answer",
			query:       url.Values{"repo": {"acme/store"}, "pull_request": {"412"}, "environment": {"staging"}},
			repo:        "acme/store",
			pullRequest: 412,
			environment: "staging",
		},
	}
	for _, tc := range accepted {
		t.Run(tc.name, func(t *testing.T) {
			req, err := checksInspectRequestFromQuery(tc.query)
			require.NoError(t, err)
			assert.Equal(t, tc.repo, req.Repo)
			assert.Equal(t, tc.pullRequest, req.PullRequest)
			assert.Equal(t, tc.environment, req.Environment)
		})
	}
}

// A repository named twice, differently, is refused rather than resolved by
// precedence: inspecting whichever one the code prefers would answer about a
// pull request the caller did not ask about.
func TestChecksInspectRequestFromQueryRefusesAnUnusableTarget(t *testing.T) {
	refused := []struct {
		name  string
		query url.Values
	}{
		{"no pull request", url.Values{"repo": {"acme/store"}}},
		{"a repository with no number", url.Values{"pull_request": {"acme/store"}}},
		{"a reference naming neither", url.Values{"pull_request": {"not a pull request"}}},
		{
			name:  "a repository disagreeing with the reference",
			query: url.Values{"repo": {"acme/warehouse"}, "pull_request": {"https://github.com/acme/store/pull/412"}},
		},
	}
	for _, tc := range refused {
		t.Run(tc.name, func(t *testing.T) {
			_, err := checksInspectRequestFromQuery(tc.query)
			require.Error(t, err)
			var reqErr *webhookOpsRequestError
			assert.ErrorAs(t, err, &reqErr, "an unusable target is the caller's error, not the server's")
		})
	}
}
