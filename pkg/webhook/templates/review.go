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
	Owners      []string
	PRAuthor    string
}

// RenderReviewRequired renders a PR comment when the review gate blocks an apply.
func RenderReviewRequired(data ReviewGateData) string {
	var sb strings.Builder

	sb.WriteString("## Review Required\n\n")
	writeDBEnvLine(&sb, data.Database, data.Environment)
	writeRequesterOrTimestamp(&sb, data.RequestedBy)

	if len(data.Owners) > 0 {
		sb.WriteString("\nSchema changes require approval from a code owner before applying.\n")
		sb.WriteString("\n**Code owners** (from CODEOWNERS):\n")
		for _, owner := range data.Owners {
			fmt.Fprintf(&sb, "- @%s\n", owner)
		}
	} else {
		sb.WriteString("\nSchema changes require at least one approval before applying.\n")
	}

	sb.WriteString("\n### Next steps\n")
	if len(data.Owners) > 0 {
		sb.WriteString("1. Request a review from a code owner above\n")
	} else {
		sb.WriteString("1. Request a review from a teammate\n")
	}
	fmt.Fprintf(&sb, "2. Once approved, run `schemabot apply -e %s` again\n", data.Environment)

	return sb.String()
}
