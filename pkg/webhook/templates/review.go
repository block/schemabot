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
	Reviewers   []string
	PRAuthor    string
}

// RenderReviewRequired renders a PR comment when the review gate blocks an apply.
func RenderReviewRequired(data ReviewGateData) string {
	var sb strings.Builder

	sb.WriteString("## Review Required\n\n")
	writeDBEnvLine(&sb, data.Database, data.Environment)
	writeRequesterOrTimestamp(&sb, data.RequestedBy)

	if len(data.Reviewers) > 0 {
		sb.WriteString("\nSchema changes require approval from an authorized reviewer before applying.\n")
		sb.WriteString("\n**Authorized reviewers**:\n")
		for _, reviewer := range data.Reviewers {
			fmt.Fprintf(&sb, "- @%s\n", reviewer)
		}
	} else {
		sb.WriteString("\nSchema changes require approval from an authorized reviewer before applying.\n")
	}

	sb.WriteString("\n### Next steps\n")
	if len(data.Reviewers) > 0 {
		sb.WriteString("1. Request a review from an authorized reviewer above\n")
	} else {
		sb.WriteString("1. Request a review from a database operator or admin\n")
	}
	fmt.Fprintf(&sb, "2. Once approved, run `schemabot apply -e %s` again\n", data.Environment)

	return sb.String()
}
