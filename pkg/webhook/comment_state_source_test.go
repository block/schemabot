package webhook

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/github"
)

type fakeCommentLister struct {
	comments []github.IssueComment
	err      error
}

func (f *fakeCommentLister) ListIssueComments(_ context.Context, _ string, _ int) ([]github.IssueComment, error) {
	return f.comments, f.err
}

func tenantStateComment(t *testing.T, id int64, ts TenantState) github.IssueComment {
	t.Helper()
	block, err := renderTenantStateBlock(ts)
	require.NoError(t, err)
	return github.IssueComment{
		ID:          id,
		Body:        "<details><summary>SchemaBot · " + ts.Tenant + "</summary>\n\n" + block + "\n</details>",
		AuthorLogin: "schemabot-block[bot]",
	}
}

// The reader returns one TenantState per participating tenant at the head SHA,
// ignoring ordinary comments and blocks stamped with a different SHA.
func TestCommentStateSourceStatesForPR(t *testing.T) {
	lister := &fakeCommentLister{comments: []github.IssueComment{
		{ID: 1, Body: "just a normal human comment, no block here"},
		tenantStateComment(t, 2, TenantState{Tenant: "tenant-a", Environment: "production", SHA: "abc", Rollup: "applied", Databases: []TenantDatabaseState{{Database: "orders", State: "applied"}}}),
		tenantStateComment(t, 3, TenantState{Tenant: "tenant-b", Environment: "production", SHA: "abc", Rollup: "pending", Databases: []TenantDatabaseState{{Database: "ledger", State: "running"}}}),
		tenantStateComment(t, 4, TenantState{Tenant: "tenant-c", Environment: "production", SHA: "stale0", Rollup: "applied"}),
	}}
	src := NewCommentStateSource(lister, testLogger())

	states, err := src.StatesForPR(t.Context(), "octocat/shared-repo", 1, "abc")
	require.NoError(t, err)
	require.Len(t, states, 2, "the stale-SHA and non-block comments are excluded")
	assert.ElementsMatch(t, []string{"tenant-a", "tenant-b"}, []string{states[0].Tenant, states[1].Tenant})
}

// A malformed block is skipped (its tenant treated as not reported) and does not
// abort the read of the other tenants — fail-closed per comment.
func TestCommentStateSourceSkipsMalformed(t *testing.T) {
	lister := &fakeCommentLister{comments: []github.IssueComment{
		{ID: 1, Body: "<!-- " + tenantStateMarker + " v=1\n{bad json}\n-->"},
		tenantStateComment(t, 2, TenantState{Tenant: "tenant-a", Environment: "production", SHA: "abc", Rollup: "applied"}),
	}}
	src := NewCommentStateSource(lister, testLogger())

	states, err := src.StatesForPR(t.Context(), "octocat/shared-repo", 1, "abc")
	require.NoError(t, err)
	require.Len(t, states, 1)
	assert.Equal(t, "tenant-a", states[0].Tenant)
}

// Two state comments for the same (tenant, environment) at the same SHA keep
// the first.
func TestCommentStateSourceDedupesTenant(t *testing.T) {
	lister := &fakeCommentLister{comments: []github.IssueComment{
		tenantStateComment(t, 1, TenantState{Tenant: "tenant-a", Environment: "production", SHA: "abc", Rollup: "pending", Databases: []TenantDatabaseState{{Database: "orders", State: "running"}}}),
		tenantStateComment(t, 2, TenantState{Tenant: "tenant-a", Environment: "production", SHA: "abc", Rollup: "applied", Databases: []TenantDatabaseState{{Database: "orders", State: "applied"}}}),
	}}
	src := NewCommentStateSource(lister, testLogger())

	states, err := src.StatesForPR(t.Context(), "octocat/shared-repo", 1, "abc")
	require.NoError(t, err)
	require.Len(t, states, 1)
	assert.Equal(t, "pending", states[0].Rollup, "keeps the first comment for a (tenant, environment)")
}

// The same tenant reporting for two different environments yields two states —
// (tenant, environment) is the key, so distinct environments are not collapsed.
func TestCommentStateSourceKeepsDistinctEnvironments(t *testing.T) {
	lister := &fakeCommentLister{comments: []github.IssueComment{
		tenantStateComment(t, 1, TenantState{Tenant: "tenant-a", Environment: "staging-1", SHA: "abc", Rollup: "applied", Databases: []TenantDatabaseState{{Database: "orders", State: "applied"}}}),
		tenantStateComment(t, 2, TenantState{Tenant: "tenant-a", Environment: "staging-2", SHA: "abc", Rollup: "pending", Databases: []TenantDatabaseState{{Database: "orders", State: "running"}}}),
	}}
	src := NewCommentStateSource(lister, testLogger())

	states, err := src.StatesForPR(t.Context(), "octocat/shared-repo", 1, "abc")
	require.NoError(t, err)
	require.Len(t, states, 2)
	assert.ElementsMatch(t, []string{"staging-1", "staging-2"}, []string{states[0].Environment, states[1].Environment})
}

// A nil logger is tolerated: the source defaults to slog.Default() so a code
// path that logs (here, a malformed block) does not panic.
func TestCommentStateSourceNilLogger(t *testing.T) {
	lister := &fakeCommentLister{comments: []github.IssueComment{
		{ID: 1, Body: "<!-- " + tenantStateMarker + " v=1\n{bad json}\n-->"},
		tenantStateComment(t, 2, TenantState{Tenant: "tenant-a", Environment: "production", SHA: "abc", Rollup: "applied"}),
	}}
	src := NewCommentStateSource(lister, nil)

	states, err := src.StatesForPR(t.Context(), "octocat/shared-repo", 1, "abc")
	require.NoError(t, err)
	require.Len(t, states, 1)
	assert.Equal(t, "tenant-a", states[0].Tenant)
}

// A list failure is surfaced so the caller can fail closed rather than fold a
// partial view of tenant state.
func TestCommentStateSourceListError(t *testing.T) {
	lister := &fakeCommentLister{err: errors.New("github unavailable")}
	src := NewCommentStateSource(lister, testLogger())

	_, err := src.StatesForPR(t.Context(), "octocat/shared-repo", 1, "abc")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read tenant-state comments")
}
