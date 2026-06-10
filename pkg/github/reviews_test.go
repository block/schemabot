package github

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCodeowners(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		expected []string
	}{
		{
			name:     "empty",
			content:  "",
			expected: nil,
		},
		{
			name:     "comments and blank lines",
			content:  "# This is a CODEOWNERS file\n\n# Another comment\n",
			expected: nil,
		},
		{
			name:     "single owner",
			content:  "* @alice\n",
			expected: []string{"alice"},
		},
		{
			name:     "multiple owners on one line",
			content:  "*.sql @alice @bob @org/dba-team\n",
			expected: []string{"alice", "bob", "org/dba-team"},
		},
		{
			name:     "multiple lines deduplicated",
			content:  "*.sql @alice @bob\n*.yaml @alice @carol\n",
			expected: []string{"alice", "bob", "carol"},
		},
		{
			name:     "case insensitive dedup",
			content:  "* @Alice\n*.sql @alice\n",
			expected: []string{"Alice"},
		},
		{
			name:     "team owners",
			content:  "schema/ @org/dba-team @org/platform\n",
			expected: []string{"org/dba-team", "org/platform"},
		},
		{
			name:     "mixed comments and rules",
			content:  "# Global owners\n* @alice\n\n# Schema owners\nschema/ @bob @org/dba\n",
			expected: []string{"alice", "bob", "org/dba"},
		},
		{
			name:     "no at prefix ignored",
			content:  "* alice bob\n",
			expected: nil, // CODEOWNERS requires @ prefix
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseCodeowners(tt.content)
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

func TestIsTeamMember(t *testing.T) {
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
