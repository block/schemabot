package templates

import (
	"fmt"
	"html"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/glyph"
	"github.com/block/schemabot/pkg/schema"
)

// ReviewGateData contains data for rendering review gate PR comments.
type ReviewGateData struct {
	Database    string
	Environment string
	RequestedBy string
	// OperatorReviewers are the database's own operator principals — the
	// reviewers the author should ping first, shown in their own section.
	OperatorReviewers []string
	// OtherReviewers are the broader principals whose approval also satisfies
	// the gate: admins, repo admins, and codeowners.
	OtherReviewers []string
	PRAuthor       string
	// ChangedApprovers are authorized reviewers whose approval was given on
	// an earlier commit at which the PR's schema change differed from the head.
	ChangedApprovers []string
	// ChangedPaths are the schema inputs whose change differs between those
	// approvals and the head.
	ChangedPaths []string
	// ChangedBasePaths have unchanged approved/head content but different
	// merge-base versions, so they need a base-aware explanation.
	ChangedBasePaths []string
	// ChangedStatements show, for changed schema files, the DDL the approved
	// commit runs beside the DDL the head runs, so the reader can see what
	// changed without opening either commit.
	ChangedStatements []ChangedStatements
	// UncomparedApprovers are authorized reviewers whose approval was given
	// on an earlier commit the review gate could not compare with the head.
	UncomparedApprovers []string
	// UnauthorizedApprovers approved the PR but are not authorized reviewers
	// for the database, so their approvals do not count.
	UnauthorizedApprovers []string
	// UnauthorizedEarlierApprovers are the UnauthorizedApprovers whose
	// approval was given on an earlier commit, so joining a team is not
	// enough on its own: they also need to approve the latest commit.
	UnauthorizedEarlierApprovers []string
	// BaseRef is the PR's base branch and DefaultBranch the repository's
	// default branch; either may be empty when unknown.
	BaseRef       string
	DefaultBranch string
}

// ChangedStatements is the DDL a schema file's change runs at an approved
// commit and at the head.
type ChangedStatements struct {
	Path        string
	Dialect     schema.Dialect
	ApprovedSHA string
	HeadSHA     string
	// Approved and Head are the statements each commit runs for the file;
	// empty means the commit runs none.
	Approved []string
	Head     []string
}

// RenderReviewRequired renders a PR comment when the review gate blocks an apply.
// The database's own operators lead in their own section so the author knows
// who to ping first; the broader principals follow as an explicit fallback.
func RenderReviewRequired(data ReviewGateData) string {
	return renderWithinCommentLimit(changedStatementBlocks(data), 0, func(budget *ddlBlockBudget) string {
		var sb strings.Builder

		sb.WriteString("## Review Required\n\n")
		writeDBEnvLine(&sb, data.Database, data.Environment)
		writeRequesterOrTimestamp(&sb, data.RequestedBy)

		if len(data.ChangedApprovers) == 0 {
			sb.WriteString("\nSchema changes require approval from an authorized reviewer before applying.\n")
		}
		writeUnauthorizedApprovers(&sb, data)
		writeChangedApprovers(&sb, data)
		writeChangedStatements(&sb, data.ChangedStatements, budget)
		writeReviewersAndNextSteps(&sb, data)

		return sb.String()
	})
}

// RenderReviewGateError renders the PR comment for a review gate that blocks
// an apply because it could not check an approval given on an earlier commit
// against the head. The remedy is the same as for a missing approval — an
// approval of the latest commit needs no check — so the comment names who can
// give it and what to run next.
func RenderReviewGateError(data ReviewGateData) string {
	return renderWithinCommentLimit(changedStatementBlocks(data), 0, func(budget *ddlBlockBudget) string {
		var sb strings.Builder

		sb.WriteString("## " + glyph.Failed + " Review Gate Error\n\n")
		writeDBEnvLine(&sb, data.Database, data.Environment)
		writeRequesterOrTimestamp(&sb, data.RequestedBy)

		writeUncomparedApprovers(&sb, data.UncomparedApprovers)
		writeUnauthorizedApprovers(&sb, data)
		writeChangedApprovers(&sb, data)
		writeChangedStatements(&sb, data.ChangedStatements, budget)
		writeReviewersAndNextSteps(&sb, data)

		return sb.String()
	})
}

// writeUncomparedApprovers says who approved an earlier commit the gate could
// not compare with the head, and why that approval does not count.
func writeUncomparedApprovers(sb *strings.Builder, approvers []string) {
	commits, approvals := "an earlier commit", "that approval does"
	if len(approvers) > 1 {
		commits, approvals = "earlier commits", "those approvals do"
	}
	fmt.Fprintf(sb, "\n**This apply needs an approval of the latest commit.** %s approved %s. An earlier approval still counts when the PR's schema change is the same at the latest commit, but SchemaBot hit an error checking that, so %s not count. Operators can find the error in the server logs.\n",
		joinWithAnd(mentions(approvers)), commits, approvals)
}

// writeUnauthorizedApprovers says who approved the PR without being an
// authorized reviewer for the database, so the reader knows why an approval
// they can see on the PR does not count.
func writeUnauthorizedApprovers(sb *strings.Builder, data ReviewGateData) {
	if len(data.UnauthorizedApprovers) == 0 {
		return
	}
	verb, approvals := "isn't an authorized reviewer", "that approval doesn't"
	if len(data.UnauthorizedApprovers) > 1 {
		verb, approvals = "aren't authorized reviewers", "those approvals don't"
	}
	fmt.Fprintf(sb, "\n%s approved, but %s for `%s`, so %s count.\n",
		joinWithAnd(mentions(data.UnauthorizedApprovers)), verb, data.Database, approvals)
}

// writeChangedApprovers says who approved an earlier commit, what changed
// since, and that those approvals no longer count. A PR that targets a branch
// other than the default also learns that changes reaching its base branch
// count as a change, since only default branch content is trusted to have had
// its own review.
func writeChangedApprovers(sb *strings.Builder, data ReviewGateData) {
	if len(data.ChangedApprovers) == 0 {
		return
	}
	plural := len(data.ChangedApprovers) > 1
	commits, approvals, count := "an earlier commit", "That approval", "counts"
	if plural {
		commits, approvals, count = "earlier commits", "Those approvals", "count"
	}
	who := joinWithAnd(mentions(data.ChangedApprovers))

	fmt.Fprintf(sb, "\n%s approved %s", who, commits)
	switch {
	case len(data.ChangedPaths) > 0:
		fmt.Fprintf(sb, ", but %s.", changedPathsClause(data.ChangedPaths))
	case len(data.ChangedBasePaths) == 0:
		sb.WriteString(", but this PR's schema change is different now.")
	default:
		sb.WriteString(".")
	}
	if len(data.ChangedBasePaths) > 0 {
		fmt.Fprintf(sb, " %s", changedBaseClause(data.ChangedBasePaths))
	}
	if targetsOtherThanDefaultBranch(data) {
		fmt.Fprintf(sb, " This PR targets `%s`, not `%s`, so changes that reach `%s` also count as changes to this PR.",
			data.BaseRef, data.DefaultBranch, data.BaseRef)
	}
	fmt.Fprintf(sb, " %s no longer %s, so the latest commit needs a new approval.\n", approvals, count)
}

// changedPathsClause says the PR changes paths differently than it did when
// it was approved.
func changedPathsClause(paths []string) string {
	if len(paths) == 1 {
		return "this PR's change to " + inlineCodeList(paths)[0] + " is different now"
	}
	return "this PR's changes to " + joinWithAnd(inlineCodeList(paths)) + " are different now"
}

// changedBaseClause says paths are unchanged since the approval but now build
// on a different version of the base branch, so they run different DDL.
func changedBaseClause(paths []string) string {
	names := joinWithAnd(inlineCodeList(paths))
	if len(paths) == 1 {
		return names + " has not changed since then, but the base branch it builds on has, so applying it now would run different DDL."
	}
	return names + " have not changed since then, but the base branch they build on has, so applying them now would run different DDL."
}

// changedStatementBlocks counts the DDL blocks writeChangedStatements takes
// from the budget: one per commit that runs DDL for a changed file.
func changedStatementBlocks(data ReviewGateData) int {
	blocks := 0
	for _, change := range data.ChangedStatements {
		for _, stmts := range [][]string{change.Approved, change.Head} {
			if len(stmts) > 0 {
				blocks++
			}
		}
	}
	return blocks
}

// writeChangedStatements folds, per changed schema file, the DDL the approved
// commit runs and the DDL the head runs into a collapsed block, so the reader
// can expand it to see exactly what differs.
func writeChangedStatements(sb *strings.Builder, changes []ChangedStatements, budget *ddlBlockBudget) {
	for _, change := range changes {
		// GitHub renders <summary> content as HTML, not markdown, so the
		// path goes in a <code> tag and is escaped as HTML.
		fmt.Fprintf(sb, "\n<details>\n<summary>How <code>%s</code> changed since the approval</summary>\n\n",
			html.EscapeString(flattenIdentifier(change.Path)))
		writeCommitStatements(sb, "Approved at "+inlineCode(shortSHA(change.ApprovedSHA)), change.Dialect, change.Approved, budget)
		sb.WriteString("\n")
		writeCommitStatements(sb, "Latest commit "+inlineCode(shortSHA(change.HeadSHA)), change.Dialect, change.Head, budget)
		sb.WriteString("\n</details>\n")
	}
}

// writeCommitStatements writes the DDL one commit runs for a schema file
// under label, or says it runs none.
func writeCommitStatements(sb *strings.Builder, label string, dialect schema.Dialect, stmts []string, budget *ddlBlockBudget) {
	if len(stmts) == 0 {
		fmt.Fprintf(sb, "%s runs no DDL for this file.\n", label)
		return
	}
	fmt.Fprintf(sb, "%s:\n\n", label)
	formatted := make([]string, 0, len(stmts))
	for _, stmt := range stmts {
		formatted = append(formatted, ddl.FormatDDLForDialect(dialect, stmt))
	}
	writeSQLFencedBlocks(sb, formatted, budget)
}

// targetsOtherThanDefaultBranch reports whether the PR is known to target a
// branch other than the repository's default branch.
func targetsOtherThanDefaultBranch(data ReviewGateData) bool {
	return data.BaseRef != "" && data.DefaultBranch != "" && data.BaseRef != data.DefaultBranch
}

// writeReviewersAndNextSteps lists who can approve, the database's own
// operators first, and the steps that unblock the apply.
func writeReviewersAndNextSteps(sb *strings.Builder, data ReviewGateData) {
	hasOperators := len(data.OperatorReviewers) > 0
	hasOthers := len(data.OtherReviewers) > 0

	if hasOperators {
		fmt.Fprintf(sb, "\n**Operators of `%s`**:\n", data.Database)
		writeMentionList(sb, data.OperatorReviewers)
		if hasOthers {
			sb.WriteString("\n**Other authorized reviewers**:\n")
			writeMentionList(sb, data.OtherReviewers)
		}
	} else if hasOthers {
		sb.WriteString("\n**Authorized reviewers**:\n")
		writeMentionList(sb, data.OtherReviewers)
	}

	sb.WriteString("\n### Next steps\n")
	earlierApprovers := slices.Concat(data.UncomparedApprovers, data.ChangedApprovers)
	var step string
	switch {
	case len(earlierApprovers) > 0 && (hasOperators || hasOthers):
		// Whoever approved an earlier commit already knows the change, so
		// they lead.
		step = fmt.Sprintf("Ask %s to approve the latest commit", joinWithOr(append(mentions(earlierApprovers), "anyone listed above")))
	case len(earlierApprovers) > 0:
		step = fmt.Sprintf("Ask %s to approve the latest commit", joinWithOr(append(mentions(earlierApprovers), "another authorized reviewer")))
	case hasOperators || hasOthers:
		step = "Request a review from anyone listed above"
	default:
		step = "Request a review from a database operator or admin"
	}
	// A reviewer who approved without being authorized may belong in one of
	// the database's operator teams; membership is checked on every apply, so
	// joining one lets an approval of the latest commit count. Admin and
	// CODEOWNERS teams are never offered, since joining one grants approval
	// beyond this database. Someone who approved an earlier commit is also
	// asked to approve the latest one.
	operatorTeams := teamsIn(data.OperatorReviewers)
	addToTeam := len(data.UnauthorizedApprovers) > 0 && len(operatorTeams) > 0
	if addToTeam {
		var headApprovers []string
		for _, approver := range data.UnauthorizedApprovers {
			if !slices.Contains(data.UnauthorizedEarlierApprovers, approver) {
				headApprovers = append(headApprovers, approver)
			}
		}
		teams := joinWithOr(mentions(operatorTeams))
		if len(headApprovers) > 0 {
			step += fmt.Sprintf(", or add %s to %s", joinWithAnd(mentions(headApprovers)), teams)
			teams = "it"
			if len(operatorTeams) > 1 {
				teams = "one of them"
			}
		}
		if len(data.UnauthorizedEarlierApprovers) > 0 {
			step += fmt.Sprintf(", or add %s to %s and ask them to approve the latest commit", joinWithAnd(mentions(data.UnauthorizedEarlierApprovers)), teams)
		}
	}
	fmt.Fprintf(sb, "1. %s\n", step)
	if addToTeam {
		fmt.Fprintf(sb, "2. Then run `schemabot apply -e %s` again\n", data.Environment)
		return
	}
	fmt.Fprintf(sb, "2. Once approved, run `schemabot apply -e %s` again\n", data.Environment)
}

// teamsIn returns the GitHub teams ("org/team") among principals, leaving out
// user logins.
func teamsIn(principals []string) []string {
	var teams []string
	for _, principal := range principals {
		if strings.Contains(principal, "/") {
			teams = append(teams, principal)
		}
	}
	return teams
}

// writeMentionList renders teams ("org/team") and user logins as a bulleted
// list of @-mentions. The review-required and command-not-authorized comments
// both list who can unblock the reader this way.
func writeMentionList(sb *strings.Builder, logins []string) {
	for _, login := range logins {
		fmt.Fprintf(sb, "- @%s\n", login)
	}
}

// mentions renders each login as an @-mention.
func mentions(logins []string) []string {
	out := make([]string, 0, len(logins))
	for _, login := range logins {
		out = append(out, "@"+login)
	}
	return out
}

// joinWithOr joins items as prose: "a", "a or b", "a, b or c".
func joinWithOr(items []string) string {
	if len(items) <= 1 {
		return strings.Join(items, "")
	}
	return strings.Join(items[:len(items)-1], ", ") + " or " + items[len(items)-1]
}
