package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// The review-required comment leads with the database's own operators in
// their own section, then lists the broader authorized principals in a
// separate fallback section, so the author knows who to ping first.
func TestRenderReviewRequired(t *testing.T) {
	data := ReviewGateData{
		Database:          "payments",
		Environment:       "staging",
		RequestedBy:       "alice",
		OperatorReviewers: []string{"org/payments-operators"},
		OtherReviewers:    []string{"bob", "org/dba-team"},
		PRAuthor:          "alice",
	}

	result := RenderReviewRequired(data)

	assert.Contains(t, result, "## Review Required")
	assert.Contains(t, result, "`payments`")
	assert.Contains(t, result, "`staging`")
	assert.Contains(t, result, "@alice")
	assert.Contains(t, result, "approval from an authorized reviewer")
	assert.Contains(t, result, "**Operators of `payments`**:\n- @org/payments-operators")
	assert.Contains(t, result, "**Other authorized reviewers**:\n- @bob\n- @org/dba-team")
	assert.Contains(t, result, "Request a review from anyone listed above")
	assert.Contains(t, result, "schemabot apply -e staging")
	assert.NotContains(t, result, "earlier commit", "no approval on an earlier commit means no explanation line")
}

// An authorized reviewer whose approval was given on an earlier commit sees
// why that approval, still visible on the PR, does not satisfy the gate: the
// PR's schema change is different at the latest commit.
func TestRenderReviewRequired_ChangedApprovers(t *testing.T) {
	result := RenderReviewRequired(ReviewGateData{
		Database:          "payments",
		Environment:       "staging",
		RequestedBy:       "alice",
		OperatorReviewers: []string{"org/payments-operators"},
		OtherReviewers:    []string{"bob", "carol"},
		PRAuthor:          "alice",
		ChangedApprovers:  []string{"bob", "carol"},
	})

	assert.Contains(t, result, "\nApprovals on an earlier commit no longer count because this PR's schema change is different now: @bob, @carol. Ask for an approval of the latest commit.\n")
	assert.NotContains(t, result, "This PR targets", "a PR whose base branch is unknown is not told it targets another branch")
	assert.Less(t, strings.Index(result, "Approvals on an earlier commit"), strings.Index(result, "**Operators of `payments`**"),
		"the explanation precedes the reviewer lists")
}

// A PR targeting a branch other than the default has approvals carried only
// where its base content is unchanged, so the comment says that changes to its
// base branch count, and the author knows the difference may not be theirs.
func TestRenderReviewRequired_ChangedApproversOnOtherBranch(t *testing.T) {
	data := ReviewGateData{
		Database:         "payments",
		Environment:      "staging",
		RequestedBy:      "alice",
		OtherReviewers:   []string{"bob"},
		PRAuthor:         "alice",
		ChangedApprovers: []string{"bob"},
		BaseRef:          "release-1.2",
		DefaultBranch:    "main",
	}

	result := RenderReviewRequired(data)
	assert.Contains(t, result, "\nApprovals on an earlier commit no longer count because this PR's schema change is different now: @bob. This PR targets `release-1.2`, not `main`, so changes that reach `release-1.2` count as a change too. Ask for an approval of the latest commit.\n")

	data.BaseRef = "main"
	assert.NotContains(t, RenderReviewRequired(data), "This PR targets", "a PR targeting the default branch gets no branch sentence")
}

// An authorized reviewer approved an earlier commit the review gate could not
// compare with the head. The gate error leads with what unblocks the apply, an
// approval of the latest commit, names the approval that did not count, and
// ends with who can approve and what to run.
func TestRenderReviewGateError_UncomparedApprovers(t *testing.T) {
	result := RenderReviewGateError(ReviewGateData{
		Database:            "payments",
		Environment:         "staging",
		RequestedBy:         "alice",
		OperatorReviewers:   []string{"org/payments-operators"},
		OtherReviewers:      []string{"carol"},
		PRAuthor:            "alice",
		ChangedApprovers:    []string{"dave"},
		UncomparedApprovers: []string{"bob"},
	})

	assert.True(t, strings.HasPrefix(result, "## ❌ Review Gate Error\n\n**Database**: `payments` | **Environment**: `staging`\n"))
	assert.Contains(t, result, "\n**This apply needs an approval of the latest commit.** @bob approved an earlier commit. An earlier approval normally still counts when the PR's schema change has not changed since, but SchemaBot hit an error checking that for this PR, so that approval does not count. Operators can find the error in the server logs.\n")
	assert.Contains(t, result, "\nApprovals on an earlier commit no longer count because this PR's schema change is different now: @dave.")
	assert.True(t, strings.HasSuffix(result, "**Operators of `payments`**:\n- @org/payments-operators\n\n**Other authorized reviewers**:\n- @carol\n\n### Next steps\n1. Ask anyone listed above to approve the latest commit\n2. Once approved, run `schemabot apply -e staging` again\n"), result)
}

// A database with no operator principals falls back to a single flat list —
// no empty operators section and no "other" framing.
func TestRenderReviewRequired_NoOperators(t *testing.T) {
	data := ReviewGateData{
		Database:       "payments",
		Environment:    "staging",
		RequestedBy:    "alice",
		OtherReviewers: []string{"bob", "org/dba-team"},
		PRAuthor:       "alice",
	}

	result := RenderReviewRequired(data)

	assert.Contains(t, result, "**Authorized reviewers**:\n- @bob\n- @org/dba-team")
	assert.NotContains(t, result, "Operators of")
	assert.NotContains(t, result, "Other authorized reviewers")
	assert.Contains(t, result, "Request a review from anyone listed above")
}

// Operators without any broader fallback principals render only the
// operators section.
func TestRenderReviewRequired_OperatorsOnly(t *testing.T) {
	data := ReviewGateData{
		Database:          "payments",
		Environment:       "staging",
		RequestedBy:       "alice",
		OperatorReviewers: []string{"org/payments-operators"},
		PRAuthor:          "alice",
	}

	result := RenderReviewRequired(data)

	assert.Contains(t, result, "**Operators of `payments`**:\n- @org/payments-operators")
	assert.NotContains(t, result, "Other authorized reviewers")
	assert.NotContains(t, result, "**Authorized reviewers**:")
	assert.Contains(t, result, "Request a review from anyone listed above")
}

func TestRenderReviewRequired_NoOwners(t *testing.T) {
	data := ReviewGateData{
		Database:    "payments",
		Environment: "production",
		RequestedBy: "alice",
		PRAuthor:    "alice",
	}

	result := RenderReviewRequired(data)

	assert.Contains(t, result, "## Review Required")
	assert.Contains(t, result, "approval from an authorized reviewer")
	assert.Contains(t, result, "Request a review from a database operator or admin")
	assert.NotContains(t, result, "Authorized reviewers")
}

func TestRenderReviewRequired_NoAuthor(t *testing.T) {
	data := ReviewGateData{
		Database:          "payments",
		Environment:       "staging",
		RequestedBy:       "alice",
		OperatorReviewers: []string{"bob"},
	}

	result := RenderReviewRequired(data)

	assert.Contains(t, result, "@bob")
}
