package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/schema"
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

// A reviewer who is not an authorized reviewer for the database approved the
// PR. The comment says that approval does not count, and since the listed
// reviewers include a team, it offers adding the approver to one as the other
// way to unblock the apply.
func TestRenderReviewRequired_UnauthorizedApprover(t *testing.T) {
	result := RenderReviewRequired(ReviewGateData{
		Database:              "payments",
		Environment:           "production",
		RequestedBy:           "alice",
		OperatorReviewers:     []string{"org/payments-operators"},
		OtherReviewers:        []string{"org/dba-team"},
		PRAuthor:              "alice",
		UnauthorizedApprovers: []string{"dave"},
	})

	assert.Contains(t, result, "\nSchema changes require approval from an authorized reviewer before applying.\n")
	assert.Contains(t, result, "\n@dave approved, but isn't an authorized reviewer for `payments`, so that approval doesn't count.\n")
	assert.Contains(t, result, "\n### Next steps\n1. Request a review from anyone listed above, or add @dave to one of the teams listed above\n2. Then run `schemabot apply -e production` again\n")
	assert.NotContains(t, result, "Once approved", "the second step also follows adding the approver to a team")
	assertBefore(t, result, "@dave approved", "**Operators of `payments`**",
		"the explanation precedes the reviewer lists")
}

// Several unauthorized reviewers approved, and every listed reviewer is a
// user. A user cannot be added to another user, so the comment only asks for
// a review from someone listed.
func TestRenderReviewRequired_UnauthorizedApproversWithoutTeams(t *testing.T) {
	result := RenderReviewRequired(ReviewGateData{
		Database:              "payments",
		Environment:           "staging",
		RequestedBy:           "alice",
		OtherReviewers:        []string{"bob", "carol"},
		PRAuthor:              "alice",
		UnauthorizedApprovers: []string{"dave", "erin"},
	})

	assert.Contains(t, result, "\n@dave and @erin approved, but aren't authorized reviewers for `payments`, so those approvals don't count.\n")
	assert.Contains(t, result, "\n### Next steps\n1. Request a review from anyone listed above\n2. Once approved, run `schemabot apply -e staging` again\n")
	assert.NotContains(t, result, "teams listed above")
}

// Several unauthorized reviewers approved and a team is listed, so the next
// step offers adding every one of them to it.
func TestRenderReviewRequired_UnauthorizedApproversWithTeam(t *testing.T) {
	result := RenderReviewRequired(ReviewGateData{
		Database:              "payments",
		Environment:           "staging",
		RequestedBy:           "alice",
		OperatorReviewers:     []string{"org/payments-operators"},
		PRAuthor:              "alice",
		UnauthorizedApprovers: []string{"dave", "erin", "frank"},
	})

	assert.Contains(t, result, "\n@dave, @erin and @frank approved, but aren't authorized reviewers for `payments`, so those approvals don't count.\n")
	assert.Contains(t, result, "\n1. Request a review from anyone listed above, or add @dave, @erin and @frank to one of the teams listed above\n2. Then run `schemabot apply -e staging` again\n")
}

// An unauthorized reviewer approved an earlier commit, so joining a team is
// not enough on its own: the next step also asks them to approve the latest
// commit.
func TestRenderReviewRequired_UnauthorizedEarlierApprover(t *testing.T) {
	result := RenderReviewRequired(ReviewGateData{
		Database:                     "payments",
		Environment:                  "staging",
		RequestedBy:                  "alice",
		OperatorReviewers:            []string{"org/payments-operators"},
		PRAuthor:                     "alice",
		UnauthorizedApprovers:        []string{"dave"},
		UnauthorizedEarlierApprovers: []string{"dave"},
	})

	assert.Contains(t, result, "\n1. Request a review from anyone listed above, or add @dave to one of the teams listed above and ask them to approve the latest commit\n2. Then run `schemabot apply -e staging` again\n")
}

// One unauthorized reviewer approved the head and another an earlier commit.
// Each gets the remedy that unblocks the apply for them.
func TestRenderReviewRequired_UnauthorizedHeadAndEarlierApprovers(t *testing.T) {
	result := RenderReviewRequired(ReviewGateData{
		Database:                     "payments",
		Environment:                  "staging",
		RequestedBy:                  "alice",
		OperatorReviewers:            []string{"org/payments-operators"},
		PRAuthor:                     "alice",
		UnauthorizedApprovers:        []string{"dave", "erin"},
		UnauthorizedEarlierApprovers: []string{"erin"},
	})

	assert.Contains(t, result, "\n@dave and @erin approved, but aren't authorized reviewers for `payments`, so those approvals don't count.\n")
	assert.Contains(t, result, "\n1. Request a review from anyone listed above, or add @dave to one of the teams listed above, or add @erin to one of them and ask them to approve the latest commit\n2. Then run `schemabot apply -e staging` again\n")
}

// One reviewer approved an earlier commit whose schema change differs, and
// another approved without being authorized. Both are named with their own
// reason, and the next step leads with the reviewer who knows the change.
func TestRenderReviewRequired_UnauthorizedAndChangedApprovers(t *testing.T) {
	result := RenderReviewRequired(ReviewGateData{
		Database:              "payments",
		Environment:           "staging",
		RequestedBy:           "alice",
		OperatorReviewers:     []string{"org/payments-operators"},
		OtherReviewers:        []string{"bob"},
		PRAuthor:              "alice",
		ChangedApprovers:      []string{"bob"},
		UnauthorizedApprovers: []string{"dave"},
	})

	assert.Contains(t, result, "\n@dave approved, but isn't an authorized reviewer for `payments`, so that approval doesn't count.\n")
	assert.Contains(t, result, "\n@bob approved an earlier commit, but this PR's schema change is different now. That approval no longer counts, so the latest commit needs a new approval.\n")
	assert.Contains(t, result, "\n1. Ask @bob or anyone listed above to approve the latest commit, or add @dave to one of the teams listed above\n2. Then run `schemabot apply -e staging` again\n")
	assert.NotContains(t, result, "Schema changes require approval", "the earlier-commit explanation replaces the generic line")
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

	assert.Contains(t, result, "\n@bob and @carol approved earlier commits, but this PR's schema change is different now. Those approvals no longer count, so the latest commit needs a new approval.\n")
	assert.Contains(t, result, "\n1. Ask @bob, @carol or anyone listed above to approve the latest commit\n",
		"the reviewers who already know the change lead the next step")
	assert.NotContains(t, result, "Schema changes require approval", "the explanation replaces the generic line")
	assert.NotContains(t, result, "This PR targets", "a PR whose base branch is unknown is not told it targets another branch")
	assertBefore(t, result, "approved earlier commits", "**Operators of `payments`**",
		"the explanation precedes the reviewer lists")
}

// The comment names the schema files whose change differs since the earlier
// approvals, so the author sees what needs a fresh review.
func TestRenderReviewRequired_ChangedPaths(t *testing.T) {
	data := ReviewGateData{
		Database:         "payments",
		Environment:      "staging",
		RequestedBy:      "alice",
		OtherReviewers:   []string{"bob"},
		PRAuthor:         "alice",
		ChangedApprovers: []string{"bob"},
		ChangedPaths:     []string{"schema/payments/overrides.sql", "schema/payments/votes.sql"},
	}

	assert.Contains(t, RenderReviewRequired(data), "\n@bob approved an earlier commit, but this PR's changes to `schema/payments/overrides.sql` and `schema/payments/votes.sql` are different now. "+
		"That approval no longer counts, so the latest commit needs a new approval.\n")

	data.ChangedPaths = []string{"schema/payments/votes.sql"}
	assert.Contains(t, RenderReviewRequired(data), "\n@bob approved an earlier commit, but this PR's change to `schema/payments/votes.sql` is different now. ")

	data.ChangedPaths = nil
	data.ChangedBasePaths = []string{"schema/payments/overrides.sql", "schema/payments/votes.sql"}
	assert.Contains(t, RenderReviewRequired(data), "\n@bob approved an earlier commit. `schema/payments/overrides.sql` and `schema/payments/votes.sql` have not changed since then, "+
		"but the base branch they build on has, so applying them now would run different DDL. That approval no longer counts, so the latest commit needs a new approval.\n")
	data.ChangedPaths = []string{"schema/payments/overrides.sql", "schema/payments/votes.sql"}

	// Reviewers can approve different commits, so the same file can have
	// changed contents for one approval and only a changed base for another.
	data.OtherReviewers = []string{"bob", "carol"}
	data.ChangedApprovers = []string{"bob", "carol"}
	data.ChangedBasePaths = []string{"schema/payments/overrides.sql"}
	assert.Contains(t, RenderReviewRequired(data), "\n@bob and @carol approved earlier commits, but this PR's changes to `schema/payments/overrides.sql` and `schema/payments/votes.sql` are different now. "+
		"`schema/payments/overrides.sql` has not changed since then, but the base branch it builds on has, so applying it now would run different DDL. "+
		"Those approvals no longer count, so the latest commit needs a new approval.\n")
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
	assert.Contains(t, result, "\n@bob approved an earlier commit, but this PR's schema change is different now. This PR targets `release-1.2`, not `main`, so changes that reach `release-1.2` also count as changes to this PR. That approval no longer counts, so the latest commit needs a new approval.\n")

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
	assert.Contains(t, result, "\n**This apply needs an approval of the latest commit.** @bob approved an earlier commit. An earlier approval still counts when the PR's schema change is the same at the latest commit, but SchemaBot hit an error checking that, so that approval does not count. Operators can find the error in the server logs.\n")
	assert.Contains(t, result, "\n@dave approved an earlier commit, but this PR's schema change is different now. That approval no longer counts, so the latest commit needs a new approval.\n")
	assert.True(t, strings.HasSuffix(result, "**Operators of `payments`**:\n- @org/payments-operators\n\n**Other authorized reviewers**:\n- @carol\n\n### Next steps\n1. Ask @bob, @dave or anyone listed above to approve the latest commit\n2. Once approved, run `schemabot apply -e staging` again\n"), result)

	result = RenderReviewGateError(ReviewGateData{
		Database:            "payments",
		Environment:         "staging",
		UncomparedApprovers: []string{"bob", "erin"},
	})
	assert.Contains(t, result, "@bob and @erin approved earlier commits. ")
	assert.Contains(t, result, ", so those approvals do not count. ")
	assert.Contains(t, result, "\n1. Ask @bob, @erin or another authorized reviewer to approve the latest commit\n")
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

// The DDL each commit runs for a changed schema file is folded into a
// collapsed block below the explanation, so the reader can expand it to see
// what changed. A commit that runs no DDL for the file says so instead of
// rendering an empty block.
func TestRenderReviewRequired_ChangedStatements(t *testing.T) {
	result := RenderReviewRequired(ReviewGateData{
		Database:          "payments",
		Environment:       "staging",
		RequestedBy:       "alice",
		OperatorReviewers: []string{"org/payments-operators"},
		PRAuthor:          "alice",
		ChangedApprovers:  []string{"bob"},
		ChangedPaths:      []string{"schema/payments/refunds.sql"},
		ChangedStatements: []ChangedStatements{{
			Path:        "schema/payments/refunds.sql",
			Dialect:     schema.DialectMySQL,
			ApprovedSHA: "1111111aaaaaaa",
			HeadSHA:     "2222222bbbbbbb",
			Approved:    []string{"ALTER TABLE `refunds` ADD COLUMN `memo` text"},
		}},
	})

	assert.Contains(t, result, "\n<details>\n<summary>How <code>schema/payments/refunds.sql</code> changed since the approval</summary>\n\n"+
		"Approved at `1111111`:\n\n```sql\nALTER TABLE `refunds` ADD COLUMN `memo` text;\n```\n\n"+
		"Latest commit `2222222` runs no DDL for this file.\n\n</details>\n")
	assertBefore(t, result, "That approval no longer counts, so the latest commit needs a new approval.", "<details>",
		"the explanation precedes the collapsed DDL")
	assertBefore(t, result, "</details>", "**Operators of `payments`**",
		"the collapsed DDL precedes the reviewer lists")
}

// A path and DDL taken from the repository cannot break out of the collapsed
// block: the path is HTML-escaped inside <summary>, and the fence is longer
// than any backtick run in the DDL.
func TestRenderReviewRequired_ChangedStatementsHostileText(t *testing.T) {
	result := RenderReviewRequired(ReviewGateData{
		Database:         "payments",
		Environment:      "staging",
		ChangedApprovers: []string{"bob"},
		ChangedStatements: []ChangedStatements{{
			Path:        "schema/</summary><b>x</b>.sql",
			Dialect:     schema.DialectMySQL,
			ApprovedSHA: "1111111",
			HeadSHA:     "2222222",
			Approved:    []string{"ALTER TABLE `t` ADD COLUMN `c` int COMMENT '```'"},
			Head:        []string{"ALTER TABLE `t` ADD COLUMN `c` bigint"},
		}},
	})

	assert.Contains(t, result, "<summary>How <code>schema/&lt;/summary&gt;&lt;b&gt;x&lt;/b&gt;.sql</code> changed since the approval</summary>")
	assert.Contains(t, result, "````sql\nALTER TABLE `t` ADD COLUMN `c` int COMMENT '```';\n````\n")
}

// The review gate error comment folds the DDL the same way, for an approval
// that could be compared and no longer matches.
func TestRenderReviewGateError_ChangedStatements(t *testing.T) {
	result := RenderReviewGateError(ReviewGateData{
		Database:            "payments",
		Environment:         "staging",
		ChangedApprovers:    []string{"bob"},
		UncomparedApprovers: []string{"carol"},
		ChangedStatements: []ChangedStatements{{
			Path:        "schema/payments/refunds.sql",
			Dialect:     schema.DialectMySQL,
			ApprovedSHA: "1111111",
			HeadSHA:     "2222222",
			Approved:    []string{"ALTER TABLE `refunds` ADD COLUMN `memo` text"},
			Head:        []string{"ALTER TABLE `refunds` ADD COLUMN `note` text"},
		}},
	})

	assert.Contains(t, result, "Approved at `1111111`:\n\n```sql\nALTER TABLE `refunds` ADD COLUMN `memo` text;\n```\n")
	assert.Contains(t, result, "Latest commit `2222222`:\n\n```sql\nALTER TABLE `refunds` ADD COLUMN `note` text;\n```\n")
}

// assertBefore asserts that s contains both first and second, with first
// appearing before second.
func assertBefore(t *testing.T, s, first, second, msg string) {
	t.Helper()
	i, j := strings.Index(s, first), strings.Index(s, second)
	require.GreaterOrEqual(t, i, 0, "%s: %q not found", msg, first)
	require.GreaterOrEqual(t, j, 0, "%s: %q not found", msg, second)
	assert.Less(t, i, j, msg)
}
