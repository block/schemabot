package api

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSourcePolicyReason(t *testing.T) {
	policyErr := &SourcePolicyError{
		Reason:  SourcePolicyReasonUnauthorizedRepo,
		Message: "repo is not authorized",
	}

	assert.Equal(t, SourcePolicyReasonUnauthorizedRepo, sourcePolicyReason(policyErr))
	assert.Equal(t, SourcePolicyReasonUnauthorizedRepo, sourcePolicyReason(fmt.Errorf("wrapped: %w", policyErr)))
	assert.Equal(t, SourcePolicyReasonUnknown, sourcePolicyReason(errors.New("storage failed")))
}

func TestRepoAllowed(t *testing.T) {
	tests := []struct {
		name         string
		allowedRepos []string
		repository   string
		want         bool
	}{
		{
			name:         "exact match",
			allowedRepos: []string{"octocat/hello-world"},
			repository:   "octocat/hello-world",
			want:         true,
		},
		{
			name:         "wildcard allows any repo",
			allowedRepos: []string{"*"},
			repository:   "octocat/orders",
			want:         true,
		},
		{
			name:         "request repository is trimmed",
			allowedRepos: []string{"octocat/hello-world"},
			repository:   " octocat/hello-world ",
			want:         true,
		},
		{
			name:         "similar repo name is blocked",
			allowedRepos: []string{"octocat/hello-world"},
			repository:   "octocat/hello-world-archive",
			want:         false,
		},
		{
			name:         "empty policy blocks",
			allowedRepos: nil,
			repository:   "octocat/hello-world",
			want:         false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, repoAllowed(tt.allowedRepos, tt.repository))
		})
	}
}

func TestSchemaPathAllowed(t *testing.T) {
	tests := []struct {
		name        string
		allowedDirs []string
		schemaPath  string
		want        bool
	}{
		{
			name:        "exact directory is allowed",
			allowedDirs: []string{"schema/payments"},
			schemaPath:  "schema/payments",
			want:        true,
		},
		{
			name:        "descendant directory is allowed",
			allowedDirs: []string{"schema/payments"},
			schemaPath:  "schema/payments/archive",
			want:        true,
		},
		{
			name:        "dot segments are normalized before matching",
			allowedDirs: []string{"schema/payments"},
			schemaPath:  "schema/./payments/archive",
			want:        true,
		},
		{
			name:        "wildcard allows any repo-relative path",
			allowedDirs: []string{"*"},
			schemaPath:  "schema/orders",
			want:        true,
		},
		{
			name:        "sibling directory is blocked",
			allowedDirs: []string{"schema/payments"},
			schemaPath:  "schema/payments-archive",
			want:        false,
		},
		{
			name:        "path escape is blocked",
			allowedDirs: []string{"schema/payments"},
			schemaPath:  "../schema/payments",
			want:        false,
		},
		{
			name:        "absolute path is blocked",
			allowedDirs: []string{"schema/payments"},
			schemaPath:  "/schema/payments",
			want:        false,
		},
		{
			name:        "empty allowed dirs block",
			allowedDirs: nil,
			schemaPath:  "schema/payments",
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, schemaPathAllowed(tt.allowedDirs, tt.schemaPath))
		})
	}
}

func TestNormalizeSchemaPath(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    string
		wantErr string
	}{
		{
			name:  "cleans relative path",
			value: "schema//payments/./archive",
			want:  "schema/payments/archive",
		},
		{
			name:  "trims surrounding whitespace",
			value: " schema/payments ",
			want:  "schema/payments",
		},
		{
			name:  "wildcard is preserved",
			value: "*",
			want:  "*",
		},
		{
			name:    "empty path is invalid",
			value:   " ",
			wantErr: "path is empty",
		},
		{
			name:    "absolute path is invalid",
			value:   "/schema/payments",
			wantErr: "path must be repo-relative",
		},
		{
			name:    "escape path is invalid",
			value:   "../schema/payments",
			wantErr: "path must not escape the repository",
		},
		{
			name:    "cleaned escape path is invalid",
			value:   "schema/../../payments",
			wantErr: "path must not escape the repository",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeSchemaPath(tt.value)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
