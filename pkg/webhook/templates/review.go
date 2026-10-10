package templates

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/glyph"
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
	// UncomparedApprovers are authorized reviewers whose approval was given
	// on an earlier commit the review gate could not compare with the head.
	UncomparedApprovers []string
}

// RenderReviewRequired renders a PR comment when the review gate blocks an apply.
// The database's own operators lead in their own section so the author knows
// who to ping first; the broader principals follow as an explicit fallback.
func RenderReviewRequired(data ReviewGateData) string {
	var sb strings.Builder

	sb.WriteString("## Review Required\n\n")
	writeDBEnvLine(&sb, data.Database, data.Environment)
	writeRequesterOrTimestamp(&sb, data.RequestedBy)

	sb.WriteString("\nSchema changes require approval from an authorized reviewer before applying.\n")
	writeChangedApprovers(&sb, data.ChangedApprovers)
	writeReviewersAndNextSteps(&sb, data)

	return sb.String()
}

// RenderReviewGateError renders the PR comment for a review gate that blocks
// an apply because it could not check an approval given on an earlier commit
// against the head. The remedy is the same as for a missing approval — an
// approval of the latest commit needs no check — so the comment names who can
// give it and what to run next.
func RenderReviewGateError(data ReviewGateData) string {
	var sb strings.Builder

	sb.WriteString("## " + glyph.Failed + " Review Gate Error\n\n")
	writeDBEnvLine(&sb, data.Database, data.Environment)
	writeRequesterOrTimestamp(&sb, data.RequestedBy)

	fmt.Fprintf(&sb, "\n**This apply needs an approval of the latest commit.** %s approved an earlier commit. An earlier approval normally still counts when the PR's schema change has not changed since, but SchemaBot hit an error checking that for this PR, so that approval does not count. Operators can find the error in the server logs.\n",
		mentionList(data.UncomparedApprovers))
	writeChangedApprovers(&sb, data.ChangedApprovers)
	writeReviewersAndNextSteps(&sb, data)

	return sb.String()
}

func writeChangedApprovers(sb *strings.Builder, changedApprovers []string) {
	if len(changedApprovers) == 0 {
		return
	}
	fmt.Fprintf(sb, "\nApprovals on an earlier commit no longer count because this PR's schema change is different now: %s. Ask for an approval of the latest commit.\n",
		mentionList(changedApprovers))
}

// writeReviewersAndNextSteps lists who can approve, the database's own
// operators first, and the steps that unblock the apply.
func writeReviewersAndNextSteps(sb *strings.Builder, data ReviewGateData) {
	hasOperators := len(data.OperatorReviewers) > 0
	hasOthers := len(data.OtherReviewers) > 0

	if hasOperators {
		fmt.Fprintf(sb, "\n**Operators of `%s`**:\n", data.Database)
		writeReviewerList(sb, data.OperatorReviewers)
		if hasOthers {
			sb.WriteString("\n**Other authorized reviewers**:\n")
			writeReviewerList(sb, data.OtherReviewers)
		}
	} else if hasOthers {
		sb.WriteString("\n**Authorized reviewers**:\n")
		writeReviewerList(sb, data.OtherReviewers)
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

func writeReviewerList(sb *strings.Builder, reviewers []string) {
	for _, reviewer := range reviewers {
		fmt.Fprintf(sb, "- @%s\n", reviewer)
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
