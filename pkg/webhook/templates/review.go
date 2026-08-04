package templates

import (
	"fmt"
	"strings"
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

	hasOperators := len(data.OperatorReviewers) > 0
	hasOthers := len(data.OtherReviewers) > 0

	if hasOperators {
		fmt.Fprintf(&sb, "\n**Operators of `%s`**:\n", data.Database)
		writeReviewerList(&sb, data.OperatorReviewers)
		if hasOthers {
			sb.WriteString("\n**Other authorized reviewers**:\n")
			writeReviewerList(&sb, data.OtherReviewers)
		}
	} else if hasOthers {
		sb.WriteString("\n**Authorized reviewers**:\n")
		writeReviewerList(&sb, data.OtherReviewers)
	}

	sb.WriteString("\n### Next steps\n")
	switch {
	case hasOperators:
		sb.WriteString("1. Request a review from the operators above — any authorized reviewer can approve\n")
	case hasOthers:
		sb.WriteString("1. Request a review from an authorized reviewer above\n")
	default:
		sb.WriteString("1. Request a review from a database operator or admin\n")
	}
	fmt.Fprintf(&sb, "2. Once approved, run `schemabot apply -e %s` again\n", data.Environment)

	return sb.String()
}

func writeReviewerList(sb *strings.Builder, reviewers []string) {
	for _, reviewer := range reviewers {
		fmt.Fprintf(sb, "- @%s\n", reviewer)
	}
}
