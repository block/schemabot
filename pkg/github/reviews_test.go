package github

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
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

// Within one delivery, every authorization check for a team answers from one
// read of its members. The next delivery reads the team again, so a member
// added or removed on GitHub takes effect on the next command. A failed read is
// not cached, so the next check in the same delivery reads the team again.
func TestListTeamMembersCachesWithinOneDelivery(t *testing.T) {
	setGitHubUnavailableReadRetryDelay(t, time.Millisecond)
	client, mux := setupConfigTestGitHubServer(t)
	var reads atomic.Int32
	var mu sync.Mutex
	members := []string{"hubot"}
	status := http.StatusOK
	mux.HandleFunc("GET /orgs/octocat/teams/db-operators/members", func(w http.ResponseWriter, _ *http.Request) {
		reads.Add(1)
		mu.Lock()
		defer mu.Unlock()
		w.WriteHeader(status)
		if status != http.StatusOK {
			return
		}
		users := make([]map[string]string, 0, len(members))
		for _, member := range members {
			users = append(users, map[string]string{"login": member})
		}
		require.NoError(t, json.NewEncoder(w).Encode(users))
	})
	ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))

	delivery := WithPRInfoCache(t.Context())
	member, err := ic.IsTeamMember(delivery, "octocat", "db-operators", "mona")
	require.NoError(t, err)
	assert.False(t, member)
	member, err = ic.IsTeamMember(delivery, "OctoCat", "DB-Operators", "mona")
	require.NoError(t, err)
	assert.False(t, member)
	assert.Equal(t, int32(1), reads.Load(), "a second check in the same delivery reuses the first read")

	mu.Lock()
	members = []string{"hubot", "mona"}
	mu.Unlock()
	member, err = ic.IsTeamMember(WithPRInfoCache(t.Context()), "octocat", "db-operators", "mona")
	require.NoError(t, err)
	assert.True(t, member, "the next delivery sees mona added to the team")
	assert.Equal(t, int32(2), reads.Load())

	failing := WithPRInfoCache(t.Context())
	mu.Lock()
	status = http.StatusNotFound
	mu.Unlock()
	_, err = ic.IsTeamMember(failing, "octocat", "db-operators", "mona")
	require.ErrorIs(t, err, ErrTeamMembershipUnreadable)
	mu.Lock()
	status = http.StatusOK
	mu.Unlock()
	member, err = ic.IsTeamMember(failing, "octocat", "db-operators", "mona")
	require.NoError(t, err)
	assert.True(t, member, "a failed read is not cached")
}

func TestIsTeamMember(t *testing.T) {
	setGitHubUnavailableReadRetryDelay(t, time.Millisecond)
	tests := []struct {
		name            string
		statusCode      int
		members         []string
		wantMember      bool
		wantErr         bool
		wantErrContains string
		wantUnreadable  bool
	}{
		{
			name:       "listed member allows",
			statusCode: http.StatusOK,
			members:    []string{"mona"},
			wantMember: true,
		},
		{
			name:       "listed member allows case-insensitively",
			statusCode: http.StatusOK,
			members:    []string{"Mona"},
			wantMember: true,
		},
		{
			name:       "readable team without actor is not a member",
			statusCode: http.StatusOK,
			members:    []string{"hubot"},
		},
		{
			name:            "unreadable team returns sentinel error",
			statusCode:      http.StatusNotFound,
			wantErr:         true,
			wantErrContains: "check team membership",
			wantUnreadable:  true,
		},
		{
			name:            "server error returns generic error",
			statusCode:      http.StatusInternalServerError,
			wantErr:         true,
			wantErrContains: "check team membership",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mux := setupConfigTestGitHubServer(t)
			mux.HandleFunc("GET /orgs/octocat/teams/db-operators/members", func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.statusCode)
				if tt.statusCode == http.StatusOK {
					users := make([]map[string]string, 0, len(tt.members))
					for _, member := range tt.members {
						users = append(users, map[string]string{"login": member})
					}
					require.NoError(t, json.NewEncoder(w).Encode(users))
				}
			})

			ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
			member, err := ic.IsTeamMember(t.Context(), "octocat", "db-operators", "mona")
			if tt.wantErr {
				require.Error(t, err)
				if tt.wantUnreadable {
					assert.ErrorIs(t, err, ErrTeamMembershipUnreadable)
				} else {
					assert.NotErrorIs(t, err, ErrTeamMembershipUnreadable)
				}
				if tt.wantErrContains != "" {
					assert.Contains(t, err.Error(), tt.wantErrContains)
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
