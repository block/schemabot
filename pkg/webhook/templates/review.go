package templates

import (
	"fmt"
	"html"
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

		sb.WriteString("\nSchema changes require approval from an authorized reviewer before applying.\n")
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

		fmt.Fprintf(&sb, "\n**This apply needs an approval of the latest commit.** %s approved an earlier commit. An earlier approval normally still counts when the PR's schema change has not changed since, but SchemaBot hit an error checking that for this PR, so that approval does not count. Operators can find the error in the server logs.\n",
			mentionList(data.UncomparedApprovers))
		writeChangedApprovers(&sb, data)
		writeChangedStatements(&sb, data.ChangedStatements, budget)
		writeReviewersAndNextSteps(&sb, data)

		return sb.String()
	})
}

// writeChangedApprovers says which approvals no longer count. A PR that
// targets a branch other than the default also learns that changes reaching
// its base branch count as a change, since only default branch content is
// trusted to have had its own review.
func writeChangedApprovers(sb *strings.Builder, data ReviewGateData) {
	if len(data.ChangedApprovers) == 0 {
		return
	}
	fmt.Fprintf(sb, "\nApprovals on an earlier commit no longer count because this PR's schema change is different now: %s.",
		mentionList(data.ChangedApprovers))
	if len(data.ChangedPaths) > 0 {
		fmt.Fprintf(sb, " Changed since the approval: %s.", strings.Join(inlineCodeList(data.ChangedPaths), ", "))
	}
	if len(data.ChangedBasePaths) > 0 {
		fmt.Fprintf(sb, " Compared with some earlier approvals, file contents are unchanged, but their merge-base versions differ: %s.", strings.Join(inlineCodeList(data.ChangedBasePaths), ", "))
	}
	if targetsOtherThanDefaultBranch(data) {
		fmt.Fprintf(sb, " This PR targets `%s`, not `%s`, so changes that reach `%s` count as a change too.",
			data.BaseRef, data.DefaultBranch, data.BaseRef)
	}
	sb.WriteString(" Ask for an approval of the latest commit.\n")
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
	switch {
	case len(data.UncomparedApprovers) > 0 && (hasOperators || hasOthers):
		sb.WriteString("1. Ask anyone listed above to approve the latest commit\n")
	case len(data.UncomparedApprovers) > 0:
		sb.WriteString("1. Ask a database operator or admin to approve the latest commit\n")
	case hasOperators || hasOthers:
		sb.WriteString("1. Request a review from anyone listed above\n")
	default:
		sb.WriteString("1. Request a review from a database operator or admin\n")
	}
	fmt.Fprintf(sb, "2. Once approved, run `schemabot apply -e %s` again\n", data.Environment)
}

// writeMentionList renders teams ("org/team") and user logins as a bulleted
// list of @-mentions. The review-required and command-not-authorized comments
// both list who can unblock the reader this way.
func writeMentionList(sb *strings.Builder, logins []string) {
	for _, login := range logins {
		fmt.Fprintf(sb, "- @%s\n", login)
	}
}

// mentionList renders logins as comma-separated @-mentions.
func mentionList(logins []string) string {
	mentions := make([]string, 0, len(logins))
	for _, login := range logins {
		mentions = append(mentions, "@"+login)
	}
	return strings.Join(mentions, ", ")
}
