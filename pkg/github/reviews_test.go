package github

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmarr/codeowners"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A CODEOWNERS rule the parser accepts but cannot match, such as a bare
// `/ @owner`, yields an error rather than a panic, so the review gate can fail
// closed with a reason. The error text names the path and the fix but not the
// panic, which travels separately with its stack for server-side logging.
// Rules the parser handles still match normally.
func TestMatchCodeownersPathReturnsAnErrorForAnUnmatchableRule(t *testing.T) {
	broken, err := codeowners.ParseFile(strings.NewReader("/ @platform-admins\n"))
	require.NoError(t, err)
	owners, err := MatchCodeownersPath(broken, "schema/orders/.schema")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema/orders/.schema")
	assert.Contains(t, err.Error(), "fix the rule on the base branch")
	assert.NotContains(t, err.Error(), "index out of range")
	assert.Nil(t, owners)

	var unmatchable *UnmatchableCodeownersRuleError
	require.ErrorAs(t, err, &unmatchable)
	assert.Equal(t, "schema/orders/.schema", unmatchable.Path)
	require.NotNil(t, unmatchable.Panic)
	assert.Contains(t, fmt.Sprint(unmatchable.Panic.Value), "index out of range")
	assert.NotEmpty(t, unmatchable.Panic.Stack)

	working, err := codeowners.ParseFile(strings.NewReader("/schema/orders/ @orders-team\n"))
	require.NoError(t, err)
	owners, err = MatchCodeownersPath(working, "schema/orders/.schema")
	require.NoError(t, err)
	require.Len(t, owners, 1)
	assert.Equal(t, "orders-team", owners[0].Value)
}

func TestGetApprovedReviewers(t *testing.T) {
	sameSecond := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		reviews  []*ReviewInfo
		expected []string
	}{
		{
			name:     "empty reviews",
			reviews:  nil,
			expected: nil,
		},
		{
			name: "single approval",
			reviews: []*ReviewInfo{
				{User: "alice", State: ReviewApproved, SubmittedAt: time.Now()},
			},
			expected: []string{"alice"},
		},
		{
			name: "approval then changes requested - not approved",
			reviews: []*ReviewInfo{
				{User: "alice", State: ReviewApproved, SubmittedAt: time.Now().Add(-time.Hour)},
				{User: "alice", State: ReviewChangesRequested, SubmittedAt: time.Now()},
			},
			expected: nil,
		},
		{
			// A reviewer who approves and then replies on a thread (which GitHub
			// records as a COMMENTED review) keeps their approval, since COMMENTED
			// reviews are non-binding feedback.
			name: "approval then commented - still approved",
			reviews: []*ReviewInfo{
				{User: "alice", State: ReviewApproved, SubmittedAt: time.Now().Add(-time.Hour)},
				{User: "alice", State: ReviewCommented, SubmittedAt: time.Now()},
			},
			expected: []string{"alice"},
		},
		{
			// COMMENTED is non-binding feedback and never grants approval on its own.
			name: "commented only - not approved",
			reviews: []*ReviewInfo{
				{User: "alice", State: ReviewCommented, SubmittedAt: time.Now()},
			},
			expected: nil,
		},
		{
			// GitHub timestamps have second granularity, so a CHANGES_REQUESTED
			// that follows an APPROVED in the same second must still win.
			name: "approval then changes requested same timestamp - not approved",
			reviews: []*ReviewInfo{
				{User: "alice", State: ReviewApproved, SubmittedAt: sameSecond},
				{User: "alice", State: ReviewChangesRequested, SubmittedAt: sameSecond},
			},
			expected: nil,
		},
		{
			name: "changes requested then approved - approved",
			reviews: []*ReviewInfo{
				{User: "alice", State: ReviewChangesRequested, SubmittedAt: time.Now().Add(-time.Hour)},
				{User: "alice", State: ReviewApproved, SubmittedAt: time.Now()},
			},
			expected: []string{"alice"},
		},
		{
			name: "multiple reviewers mixed states",
			reviews: []*ReviewInfo{
				{User: "alice", State: ReviewApproved, SubmittedAt: time.Now()},
				{User: "bob", State: ReviewCommented, SubmittedAt: time.Now()},
				{User: "carol", State: ReviewApproved, SubmittedAt: time.Now()},
			},
			expected: []string{"alice", "carol"},
		},
		{
			name: "dismissed review not counted",
			reviews: []*ReviewInfo{
				{User: "alice", State: ReviewApproved, SubmittedAt: time.Now().Add(-time.Hour)},
				{User: "alice", State: ReviewDismissed, SubmittedAt: time.Now()},
			},
			expected: nil,
		},
		{
			name: "empty username skipped",
			reviews: []*ReviewInfo{
				{User: "", State: ReviewApproved, SubmittedAt: time.Now()},
			},
			expected: nil,
		},
		{
			name: "case insensitive user matching",
			reviews: []*ReviewInfo{
				{User: "Alice", State: ReviewChangesRequested, SubmittedAt: time.Now().Add(-time.Hour)},
				{User: "alice", State: ReviewApproved, SubmittedAt: time.Now()},
			},
			expected: []string{"alice"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetApprovedReviewers(tt.reviews)
			if tt.expected == nil {
				assert.Empty(t, result)
			} else {
				require.Len(t, result, len(tt.expected))
				for _, expected := range tt.expected {
					assert.Contains(t, result, expected)
				}
			}
		})
	}
}

// Each approval carries the commit it was given on, taken from the reviewer's
// latest decisive review rather than an earlier approval.
func TestGetApprovedReviewsCarriesCommit(t *testing.T) {
	now := time.Now()
	reviews := []*ReviewInfo{
		{User: "alice", State: ReviewApproved, SubmittedAt: now.Add(-2 * time.Hour), CommitID: "aaa111"},
		{User: "alice", State: ReviewApproved, SubmittedAt: now.Add(-time.Hour), CommitID: "bbb222"},
		{User: "alice", State: ReviewCommented, SubmittedAt: now, CommitID: "ccc333"},
		{User: "bob", State: ReviewApproved, SubmittedAt: now.Add(-time.Hour), CommitID: "aaa111"},
		{User: "bob", State: ReviewDismissed, SubmittedAt: now, CommitID: "bbb222"},
	}

	approvals := GetApprovedReviews(reviews)

	require.Len(t, approvals, 1)
	assert.Equal(t, "alice", approvals[0].User)
	assert.Equal(t, "bbb222", approvals[0].CommitID)
}

// Approvals come in the order they were given, with the login breaking ties,
// so a comment that names several approvers names them the same way each time.
func TestGetApprovedReviewsOrdersBySubmission(t *testing.T) {
	now := time.Now()
	reviews := []*ReviewInfo{
		{User: "dave", State: ReviewApproved, SubmittedAt: now},
		{User: "carol", State: ReviewApproved, SubmittedAt: now.Add(-time.Hour)},
		{User: "Bob", State: ReviewApproved, SubmittedAt: now},
		{User: "alice", State: ReviewApproved, SubmittedAt: now.Add(-2 * time.Hour)},
	}

	var users []string
	for _, approval := range GetApprovedReviews(reviews) {
		users = append(users, approval.User)
	}

	assert.Equal(t, []string{"alice", "carol", "Bob", "dave"}, users)
}

// IsTeamMember reads the one membership rather than listing the team, and a
// 404 counts as "not a member" only when the team itself reads back: GitHub
// returns 404 for a team the App cannot see too, and that must stay an
// unreadable error, never a denial dressed as an answer.
func TestIsTeamMember(t *testing.T) {
	setGitHubUnavailableReadRetryDelay(t, time.Millisecond)
	tests := []struct {
		name             string
		membershipStatus int
		membershipState  string
		rateLimited      bool // the membership read answers with an exhausted primary rate limit
		teamStatus       int  // 0 means the team endpoint must not be called
		wantMember       bool
		wantErr          bool
		wantUnreadable   bool
	}{
		{name: "active member allows", membershipStatus: http.StatusOK, membershipState: "active", wantMember: true},
		{name: "pending invitation is not a member", membershipStatus: http.StatusOK, membershipState: "pending"},
		{name: "no membership on a readable team is not a member", membershipStatus: http.StatusNotFound, teamStatus: http.StatusOK},
		{name: "team the App cannot see is unreadable", membershipStatus: http.StatusNotFound, teamStatus: http.StatusNotFound, wantErr: true, wantUnreadable: true},
		{name: "team read failing is a generic error", membershipStatus: http.StatusNotFound, teamStatus: http.StatusInternalServerError, wantErr: true},
		{name: "forbidden membership read is unreadable", membershipStatus: http.StatusForbidden, wantErr: true, wantUnreadable: true},
		{name: "server error is a generic error", membershipStatus: http.StatusInternalServerError, wantErr: true},
		{name: "exhausted rate limit is a generic error, not unreadable", membershipStatus: http.StatusForbidden, rateLimited: true, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mux := setupConfigTestGitHubServer(t)
			mux.HandleFunc("GET /orgs/octocat/teams/db-operators/memberships/mona", func(w http.ResponseWriter, _ *http.Request) {
				if tt.rateLimited {
					w.Header().Set("X-RateLimit-Limit", "5000")
					w.Header().Set("X-RateLimit-Remaining", "0")
					w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10))
				}
				w.WriteHeader(tt.membershipStatus)
				if tt.membershipStatus == http.StatusOK {
					require.NoError(t, json.NewEncoder(w).Encode(map[string]string{"state": tt.membershipState, "role": "member"}))
				}
			})
			var teamReads atomic.Int32
			mux.HandleFunc("GET /orgs/octocat/teams/db-operators", func(w http.ResponseWriter, _ *http.Request) {
				teamReads.Add(1)
				if tt.teamStatus == 0 {
					t.Error("the team is read only after a 404 on the membership read")
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				w.WriteHeader(tt.teamStatus)
				if tt.teamStatus == http.StatusOK {
					require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"id": 1, "slug": "db-operators"}))
				}
			})
			mux.HandleFunc("GET /orgs/octocat/teams/db-operators/members", func(w http.ResponseWriter, _ *http.Request) {
				t.Error("IsTeamMember must not list the team")
				w.WriteHeader(http.StatusInternalServerError)
			})

			ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
			member, err := ic.IsTeamMember(t.Context(), "octocat", "db-operators", "mona")
			if tt.teamStatus == 0 {
				assert.Zero(t, teamReads.Load(), "the team is read only to tell a missing membership from an unreadable team")
			}
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "check team membership octocat/db-operators for mona")
				if tt.wantUnreadable {
					assert.ErrorIs(t, err, ErrTeamMembershipUnreadable)
				} else {
					assert.NotErrorIs(t, err, ErrTeamMembershipUnreadable)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantMember, member)
		})
	}
}

func TestListTeamMembersErrorClassification(t *testing.T) {
	setGitHubUnavailableReadRetryDelay(t, time.Millisecond)
	tests := []struct {
		name           string
		statusCode     int
		wantUnreadable bool
	}{
		{
			name:           "not found is unreadable",
			statusCode:     http.StatusNotFound,
			wantUnreadable: true,
		},
		{
			name:           "unauthorized is unreadable",
			statusCode:     http.StatusUnauthorized,
			wantUnreadable: true,
		},
		{
			name:           "forbidden is unreadable",
			statusCode:     http.StatusForbidden,
			wantUnreadable: true,
		},
		{
			name:       "too many requests is generic",
			statusCode: http.StatusTooManyRequests,
		},
		{
			name:       "server error is generic",
			statusCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mux := setupConfigTestGitHubServer(t)
			mux.HandleFunc("GET /orgs/octocat/teams/db-operators/members", func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.statusCode)
			})

			ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
			_, err := ic.ListTeamMembers(t.Context(), "octocat", "db-operators")
			require.Error(t, err)
			if tt.wantUnreadable {
				assert.ErrorIs(t, err, ErrTeamMembershipUnreadable)
			} else {
				assert.NotErrorIs(t, err, ErrTeamMembershipUnreadable)
			}
		})
	}
}
