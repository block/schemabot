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
		name       string
		statusCode int
		body       map[string]string
		wantMember bool
		wantErr    bool
	}{
		{
			name:       "active membership allows",
			statusCode: http.StatusOK,
			body:       map[string]string{"state": "active"},
			wantMember: true,
		},
		{
			name:       "pending membership does not allow",
			statusCode: http.StatusOK,
			body:       map[string]string{"state": "pending"},
			wantMember: false,
		},
		{
			name:       "not found is not a member",
			statusCode: http.StatusNotFound,
			wantMember: false,
		},
		{
			name:       "server error is returned",
			statusCode: http.StatusInternalServerError,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, mux := setupConfigTestGitHubServer(t)
			mux.HandleFunc("GET /orgs/octocat/teams/db-operators/memberships/mona", func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.statusCode)
				if tt.body != nil {
					require.NoError(t, json.NewEncoder(w).Encode(tt.body))
				}
			})

			ic := NewInstallationClient(client, slog.New(slog.NewTextHandler(io.Discard, nil)))
			member, err := ic.IsTeamMember(t.Context(), "octocat", "db-operators", "mona")
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantMember, member)
		})
	}
}
