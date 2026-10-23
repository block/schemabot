package github

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A schema file whose content differs between the approved commit and the
// head is handed to the dialect's statement comparison only when it is a
// regular .sql file at all four commits, or one the PR adds (absent at both
// bases, passed to the dialect as ""), its bases are either the same or
// trusted default branch content, and the comparison budget is not spent.
// Every other case, and any comparison that does not prove the statements
// are the same, leaves the file changed without asking the dialect.
func TestSameStatements(t *testing.T) {
	file := func(sha string) gitSide {
		return gitSide{entry: TreeEntry{Mode: "100644", Type: "blob", SHA: sha}, found: true}
	}
	combined := [4]gitSide{file("approved"), file("approved-base"), file("head"), file("head-base")}
	tests := []struct {
		name                string
		path                string
		sides               [4]gitSide
		baseContentReviewed bool
		budgetSpent         bool
		noComparer          bool
		compareSame         bool
		compareErr          error
		wantCompared        bool
		wantSame            bool
	}{
		{
			name: "a file both sides edited on reviewed default branch content", path: "schema/db/orders.sql", sides: combined,
			baseContentReviewed: true, compareSame: true, wantCompared: true, wantSame: true,
		},
		{
			name: "a file the PR reformatted on the same base", path: "schema/db/orders.sql",
			sides:       [4]gitSide{file("approved"), file("base"), file("head"), file("base")},
			compareSame: true, wantCompared: true, wantSame: true,
		},
		{
			name: "statements the dialect finds different", path: "schema/db/orders.sql", sides: combined,
			baseContentReviewed: true, wantCompared: true,
		},
		{
			name: "files the dialect cannot compare", path: "schema/db/orders.sql", sides: combined,
			baseContentReviewed: true, compareSame: true, compareErr: errors.New("two statements"), wantCompared: true,
		},
		{
			name: "two bases that are not reviewed default branch content in order", path: "schema/db/orders.sql", sides: combined,
			compareSame: true,
		},
		{
			name: "a dialect with no statement comparison", path: "schema/db/orders.sql", sides: combined,
			baseContentReviewed: true, noComparer: true,
		},
		{
			name: "a spent comparison budget", path: "schema/db/orders.sql", sides: combined,
			baseContentReviewed: true, budgetSpent: true, compareSame: true,
		},
		{
			name: "a file that is not a .sql file", path: "schema/db/vschema.json", sides: combined,
			baseContentReviewed: true, compareSame: true,
		},
		{
			name: "a file the PR adds, absent at both bases", path: "schema/db/orders.sql",
			sides:       [4]gitSide{file("approved"), {}, file("head"), {}},
			compareSame: true, wantCompared: true, wantSame: true,
		},
		{
			name: "a file the PR adds whose statements the dialect finds different", path: "schema/db/orders.sql",
			sides:        [4]gitSide{file("approved"), {}, file("head"), {}},
			wantCompared: true,
		},
		{
			name: "a file absent at the approved commit's base only", path: "schema/db/orders.sql",
			sides:               [4]gitSide{file("approved"), {}, file("head"), file("head-base")},
			baseContentReviewed: true, compareSame: true,
		},
		{
			name: "a file the PR adds that is absent at the head", path: "schema/db/orders.sql",
			sides:       [4]gitSide{file("approved"), {}, {}, {}},
			compareSame: true,
		},
		{
			name: "a file the PR adds as a symlink at the head", path: "schema/db/orders.sql",
			sides: [4]gitSide{file("approved"), {},
				{entry: TreeEntry{Mode: gitSymlinkMode, Type: "blob", SHA: "head"}, found: true}, {}},
			compareSame: true,
		},
		{
			name: "a file that is a symlink at one commit", path: "schema/db/orders.sql",
			sides: [4]gitSide{file("approved"), file("approved-base"),
				{entry: TreeEntry{Mode: gitSymlinkMode, Type: "blob", SHA: "head"}, found: true}, file("head-base")},
			baseContentReviewed: true, compareSame: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var compared [][4]string
			compareStatements := SchemaFileChangeComparer(func(_ context.Context, approvedBase, approved, headBase, head string) (bool, string, error) {
				compared = append(compared, [4]string{approvedBase, approved, headBase, head})
				if tt.compareErr != nil {
					return false, "", tt.compareErr
				}
				if !tt.compareSame {
					return false, "statements differ", nil
				}
				return true, "", nil
			})
			if tt.noComparer {
				compareStatements = nil
			}
			c := &schemaChangeComparer{
				ic:                  &InstallationClient{logger: slog.New(slog.DiscardHandler)},
				repo:                "octocat/hello-world",
				commits:             [4]string{"approved-sha", "approved-base-sha", "head-sha", "head-base-sha"},
				baseContentReviewed: tt.baseContentReviewed,
				compareStatements:   compareStatements,
				// Every side's content is already read, so the test makes
				// no GitHub requests.
				blobText: map[string]string{
					"approved": "approved content", "approved-base": "approved base content",
					"head": "head content", "head-base": "head base content", "base": "base content",
				},
			}
			if tt.budgetSpent {
				c.statementComparedPaths = make([]string, maxSchemaStatementComparisons)
			}
			budgetBefore := len(c.statementComparedPaths)

			same, err := c.sameStatements(t.Context(), schemaChangeItem{path: tt.path, sides: tt.sides, resolved: true})
			require.NoError(t, err)
			assert.Equal(t, tt.wantSame, same)
			if !tt.wantCompared {
				assert.Empty(t, compared, "an ineligible file is never handed to the dialect")
				assert.Len(t, c.statementComparedPaths, budgetBefore)
				return
			}
			require.Len(t, compared, 1)
			want := [4]string{
				c.blobText[tt.sides[sideApprovedBase].entry.SHA], c.blobText[tt.sides[sideApproved].entry.SHA],
				c.blobText[tt.sides[sideHeadBase].entry.SHA], c.blobText[tt.sides[sideHead].entry.SHA],
			}
			assert.Equal(t, want, compared[0], "the dialect gets each side's content in base, commit order")
			if tt.wantSame {
				assert.Equal(t, []string{tt.path}, c.statementComparedPaths, "a proved file uses up part of the budget")
			} else {
				assert.Empty(t, c.statementComparedPaths)
			}
		})
	}
}

// A comparison cut short because the evaluation itself was cancelled proves
// nothing about the file, so it is an error rather than a changed verdict,
// and it uses none of the comparison budget.
func TestSameStatementsCancelledEvaluation(t *testing.T) {
	file := func(sha string) gitSide {
		return gitSide{entry: TreeEntry{Mode: "100644", Type: "blob", SHA: sha}, found: true}
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c := &schemaChangeComparer{
		ic:                  &InstallationClient{logger: slog.New(slog.DiscardHandler)},
		repo:                "octocat/hello-world",
		commits:             [4]string{"approved-sha", "approved-base-sha", "head-sha", "head-base-sha"},
		baseContentReviewed: true,
		compareStatements: func(ctx context.Context, _, _, _, _ string) (bool, string, error) {
			cancel()
			return false, "", ctx.Err()
		},
		blobText: map[string]string{
			"approved": "approved content", "approved-base": "approved base content",
			"head": "head content", "head-base": "head base content",
		},
	}
	item := schemaChangeItem{
		path:     "schema/db/orders.sql",
		sides:    [4]gitSide{file("approved"), file("approved-base"), file("head"), file("head-base")},
		resolved: true,
	}

	same, err := c.sameStatements(ctx, item)
	require.ErrorIs(t, err, context.Canceled)
	assert.ErrorContains(t, err, "evaluation cancelled")
	assert.False(t, same)
	assert.Empty(t, c.statementComparedPaths)
}
